//! Independent, authenticated invitations. A guest listener is not a room server.
//! Only endpoints advertised by approved room members may poll an invitation; each
//! recipient gets its own fresh return pass, never another guest's credentials.
use std::{
    collections::HashMap,
    sync::Arc,
    time::{Duration, Instant},
};

use super::bot_share::{BotPayload, Notice, Request as BotRequest, Room, Shares};
use futures_util::{stream, StreamExt};
use iroh::{endpoint::presets, Endpoint, EndpointAddr, EndpointId};
use listen_protocol::RecoveryPeer;
use serde::{Deserialize, Serialize};
use tokio::sync::{Mutex, Semaphore};

const ALPN: &[u8] = b"limusic-recovery/1";
const REQUEST_LIMIT: usize = 8192;
const INVITE_LIFETIME: Duration = Duration::from_secs(600);

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub struct Offer {
    pub server_url: String,
    pub room_code: String,
    pub guest_token: String,
}

#[derive(Serialize, Deserialize)]
struct Request {
    epoch: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    bot: Option<BotRequest>,
}

struct Context {
    peers: Vec<RecoveryPeer>,
    offers: HashMap<String, Offer>, // Endpoint public key -> recipient-specific invitation.
    expires: Instant,
}

impl Context {
    fn offer_for(&self, remote_id: &str) -> Option<Offer> {
        if Instant::now() >= self.expires || !self.peers.iter().any(|p| p.endpoint_id == remote_id)
        {
            return None;
        }
        self.offers.get(remote_id).cloned()
    }
}

struct Listener {
    endpoint: Endpoint,
    task: tokio::task::JoinHandle<()>,
}
impl Drop for Listener {
    fn drop(&mut self) {
        self.task.abort();
    }
}

#[derive(Default)]
pub struct RecoveryNetwork {
    listener: Mutex<Option<Listener>>,
    contexts: Arc<Mutex<HashMap<String, Context>>>,
    bots: Arc<Mutex<Shares>>,
}

impl RecoveryNetwork {
    pub async fn ensure_listener(&self) -> Result<String, String> {
        let mut listener = self.listener.lock().await;
        if listener.is_none() {
            // Ephemeral identity: no hosting identity or database is shared with guests.
            let endpoint = Endpoint::builder(presets::N0)
                .alpns(vec![ALPN.to_vec()])
                .bind()
                .await
                .map_err(|e| e.to_string())?;
            let task = serve(endpoint.clone(), self.contexts.clone(), self.bots.clone());
            *listener = Some(Listener { endpoint, task });
        }
        Ok(listener.as_ref().unwrap().endpoint.id().to_string())
    }

    pub async fn clear(&self) {
        self.contexts.lock().await.clear();
        self.set_bot_room(None).await;
    }

    pub async fn set_bot_room(&self, room: Option<Room>) {
        self.bots.lock().await.set_room(room);
    }

    pub async fn announce_bot(&self, bot: BotPayload) -> Result<usize, String> {
        let endpoint = self.bot_endpoint().await?;
        let (epoch, offers) = self
            .bots
            .lock()
            .await
            .offer(bot)
            .ok_or("There are no eligible room members, or too many offers are pending.")?;
        let offered = stream::iter(offers.into_iter().map(|(peer, bot)| {
            let endpoint = endpoint.clone();
            let epoch = epoch.clone();
            async move {
                self.bot_request(&endpoint, peer, epoch, bot).await.ok()
                    == Some(serde_json::Value::Bool(true))
            }
        }))
        .buffer_unordered(8)
        .filter(|sent| std::future::ready(*sent))
        .count()
        .await;
        Ok(offered)
    }

    pub async fn bot_notices(&self) -> Vec<Notice> {
        self.bots.lock().await.notices()
    }

    pub async fn dismiss_bot(&self, id: &str) {
        self.bots.lock().await.dismiss(id);
    }

    pub async fn accept_bot(&self, id: String) -> Result<BotPayload, String> {
        let endpoint = self.bot_endpoint().await?;
        let (epoch, peer) =
            self.bots.lock().await.origin(&id).ok_or("The bot offer is no longer available.")?;
        let reply = self
            .bot_request(
                &endpoint,
                peer.clone(),
                epoch.clone(),
                BotRequest::Accept { id: id.clone() },
            )
            .await?;
        if self.bots.lock().await.origin(&id) != Some((epoch, peer)) {
            return Err("The room changed while accepting the bot.".into());
        }
        serde_json::from_value(reply).map_err(|_| "The bot offer is no longer available.".into())
    }

    async fn bot_endpoint(&self) -> Result<Endpoint, String> {
        self.listener
            .lock()
            .await
            .as_ref()
            .map(|listener| listener.endpoint.clone())
            .ok_or_else(|| "The room's peer connection is not ready.".into())
    }

    async fn bot_request(
        &self,
        endpoint: &Endpoint,
        peer: String,
        epoch: String,
        bot: BotRequest,
    ) -> Result<serde_json::Value, String> {
        let id: EndpointId = peer.parse().map_err(|_| "Invalid room endpoint.")?;
        tokio::time::timeout(
            Duration::from_secs(6),
            exchange(endpoint, EndpointAddr::new(id), Request { epoch, bot: Some(bot) }),
        )
        .await
        .map_err(|_| "The room member did not respond.")?
        .map_err(|_| "Could not reach the room member.".into())
    }

    pub async fn publish(
        &self,
        epoch: String,
        peers: Vec<RecoveryPeer>,
        offers: HashMap<String, Offer>,
    ) {
        let mut contexts = self.contexts.lock().await;
        contexts.retain(|_, c| c.expires > Instant::now());
        // Bound successive recoveries while allowing previous members time to accept.
        if contexts.len() >= 4 {
            contexts.clear();
        }
        contexts
            .insert(epoch, Context { peers, offers, expires: Instant::now() + INVITE_LIFETIME });
    }

    pub async fn poll(&self, epoch: &str, peers: Vec<RecoveryPeer>) -> Vec<(RecoveryPeer, Offer)> {
        let endpoint = {
            let listener = self.listener.lock().await;
            let Some(listener) = listener.as_ref() else { return vec![] };
            listener.endpoint.clone()
        };
        let requests = stream::iter(peers.into_iter().map(|peer| {
            let endpoint = endpoint.clone();
            let epoch = epoch.to_string();
            async move {
                let id: EndpointId = peer.endpoint_id.parse().ok()?;
                let offer = tokio::time::timeout(
                    Duration::from_secs(6),
                    request(&endpoint, EndpointAddr::new(id), epoch),
                )
                .await
                .ok()?
                .ok()??;
                // Invitations are only for encrypted Limusic rooms, with bounded fresh passes.
                let address = offer.server_url.strip_prefix("limusic-peer://")?;
                address.parse::<EndpointId>().ok()?;
                if offer.room_code.is_empty()
                    || offer.room_code.len() > 32
                    || offer.guest_token.len() != 64
                    || !offer.guest_token.bytes().all(|b| b.is_ascii_hexdigit())
                {
                    return None;
                }
                Some((peer, offer))
            }
        }))
        .buffer_unordered(4);
        requests.filter_map(|result| async { result }).collect().await
    }
}

async fn request(
    endpoint: &Endpoint,
    address: EndpointAddr,
    epoch: String,
) -> anyhow::Result<Option<Offer>> {
    Ok(serde_json::from_value(exchange(endpoint, address, Request { epoch, bot: None }).await?)?)
}

async fn exchange(
    endpoint: &Endpoint,
    address: EndpointAddr,
    request: Request,
) -> anyhow::Result<serde_json::Value> {
    let connection = endpoint.connect(address, ALPN).await?;
    let (mut send, mut recv) = connection.open_bi().await?;
    send.write_all(&serde_json::to_vec(&request)?).await?;
    send.finish()?;
    let bytes = recv.read_to_end(REQUEST_LIMIT).await?;
    let offer = serde_json::from_slice(&bytes)?;
    connection.close(0u32.into(), b"received");
    Ok(offer)
}

fn serve(
    endpoint: Endpoint,
    contexts: Arc<Mutex<HashMap<String, Context>>>,
    bots: Arc<Mutex<Shares>>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let slots = Arc::new(Semaphore::new(16));
        let mut tasks = tokio::task::JoinSet::new();
        loop {
            tokio::select! {
                Some(_) = tasks.join_next(), if !tasks.is_empty() => {},
                incoming = endpoint.accept() => {
                    let Some(incoming) = incoming else { break };
                    let Ok(permit) = slots.clone().try_acquire_owned() else { incoming.refuse(); continue };
                    let contexts = contexts.clone();
                    let bots = bots.clone();
                    tasks.spawn(async move {
                        let _permit = permit;
                        let _ = tokio::time::timeout(Duration::from_secs(8), async {
                            let connection = incoming.await?;
                            let remote = connection.remote_id().to_string();
                            let (mut send, mut recv) = connection.accept_bi().await?;
                            let bytes = recv.read_to_end(REQUEST_LIMIT).await?;
                            let request: Request = serde_json::from_slice(&bytes)?;
                            let reply = if let Some(bot) = request.bot {
                                bots.lock().await.receive(&remote, &request.epoch, bot)
                            } else {
                                serde_json::json!(contexts.lock().await.get(&request.epoch).and_then(|c| c.offer_for(&remote)))
                            };
                            send.write_all(&serde_json::to_vec(&reply)?).await?;
                            send.finish()?;
                            connection.closed().await;
                            Ok::<(), anyhow::Error>(())
                        }).await;
                    });
                }
            }
        }
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    async fn local() -> Endpoint {
        Endpoint::builder(presets::Minimal)
            .clear_ip_transports()
            .bind_addr("127.0.0.1:0")
            .unwrap()
            .alpns(vec![ALPN.to_vec()])
            .bind()
            .await
            .unwrap()
    }

    #[tokio::test]
    async fn invitations_survive_original_server_loss_and_are_recipient_scoped() {
        // Local endpoints only: no original room server or discovery service participates.
        let successor = local().await;
        let guest = local().await;
        let outsider = local().await;
        let offer = Offer {
            server_url: "limusic-peer://new-host".into(),
            room_code: "NEWROOM".into(),
            guest_token: "a".repeat(64),
        };
        let context = Context {
            peers: vec![RecoveryPeer {
                user_id: "guest".into(),
                username: "Guest".into(),
                endpoint_id: guest.id().to_string(),
                is_host: false,
            }],
            offers: HashMap::from([(guest.id().to_string(), offer.clone())]),
            expires: Instant::now() + INVITE_LIFETIME,
        };
        let contexts = Arc::new(Mutex::new(HashMap::from([("old-session".into(), context)])));
        let task =
            serve(successor.clone(), contexts.clone(), Arc::new(Mutex::new(Shares::default())));
        assert_eq!(
            request(&guest, successor.addr(), "old-session".into()).await.unwrap(),
            Some(offer)
        );
        assert_eq!(request(&outsider, successor.addr(), "old-session".into()).await.unwrap(), None);
        assert_eq!(request(&guest, successor.addr(), "other-session".into()).await.unwrap(), None);
        contexts.lock().await.get_mut("old-session").unwrap().expires = Instant::now();
        assert_eq!(request(&guest, successor.addr(), "old-session".into()).await.unwrap(), None);
        task.abort();
        successor.close().await;
        guest.close().await;
        outsider.close().await;
    }

    #[cfg(windows)]
    #[tokio::test]
    async fn bot_offer_only_releases_token_to_matching_member_on_accept() {
        let host = local().await;
        let guest = local().await;
        let outsider = local().await;
        let host_id = host.id().to_string();
        let guest_id = guest.id().to_string();
        let host_shares = Arc::new(Mutex::new(Shares::default()));
        let guest_shares = Arc::new(Mutex::new(Shares::default()));
        let epoch = "share-session".to_owned();
        host_shares.lock().await.set_room(Some(Room {
            epoch: epoch.clone(),
            host: None,
            guests: [guest_id].into(),
        }));
        guest_shares.lock().await.set_room(Some(Room {
            epoch: epoch.clone(),
            host: Some((host_id, "Room host".into())),
            guests: Default::default(),
        }));
        let host_task = serve(host.clone(), Arc::default(), host_shares.clone());
        let guest_task = serve(guest.clone(), Arc::default(), guest_shares.clone());
        let (_, mut offers) = host_shares
            .lock()
            .await
            .offer(BotPayload {
                id: "123".into(),
                name: "Music bot".into(),
                channel: "456".into(),
                token: "test.token".into(),
            })
            .unwrap();
        let (_, offer) = offers.pop().unwrap();
        let encode = |bot| Request { epoch: epoch.clone(), bot: Some(bot) };
        assert!(!serde_json::to_string(&offer).unwrap().contains("test.token"));
        assert!(exchange(&outsider, guest.addr(), encode(offer.clone())).await.unwrap().is_null());
        assert_eq!(
            exchange(&host, guest.addr(), encode(offer)).await.unwrap(),
            serde_json::Value::Bool(true)
        );
        let notice = guest_shares.lock().await.notices().pop().unwrap();
        assert!(!serde_json::to_string(&notice).unwrap().contains("test.token"));
        let accept = BotRequest::Accept { id: notice.id };
        assert!(exchange(&outsider, host.addr(), encode(accept.clone())).await.unwrap().is_null());
        assert_eq!(
            exchange(&guest, host.addr(), encode(accept.clone())).await.unwrap()["token"],
            "test.token"
        );
        host_shares.lock().await.set_room(None);
        assert!(exchange(&guest, host.addr(), encode(accept)).await.unwrap().is_null());
        host_task.abort();
        guest_task.abort();
        host.close().await;
        guest.close().await;
        outsider.close().await;
    }
}
