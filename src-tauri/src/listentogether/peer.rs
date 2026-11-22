//! An encrypted peer transport for the existing Listen Together protocol.
//! Only the owning app can create/reopen rooms through its loopback relay.
use std::{
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
    time::Duration,
};

use futures_util::{Sink, SinkExt, Stream, StreamExt};
use iroh::{endpoint::presets, Endpoint, EndpointAddr, EndpointId, SecretKey};
use tokio::{
    io::{AsyncRead, AsyncWrite, ReadBuf},
    sync::{Mutex, Semaphore},
};
use tokio_tungstenite::{
    tungstenite::{Error, Message},
    WebSocketStream,
};

const ALPN: &[u8] = b"limusic-listen/1";
const PREFIX: &str = "limusic-peer://";
const SECRET_SETTING: &str = "lt_peer_secret";

pub trait RoomSocket:
    Stream<Item = Result<Message, Error>> + Sink<Message, Error = Error> + Unpin + Send
{
}
impl<T> RoomSocket for T where
    T: Stream<Item = Result<Message, Error>> + Sink<Message, Error = Error> + Unpin + Send
{
}
pub type Socket = Box<dyn RoomSocket>;

pub fn is_peer(url: &str) -> bool {
    url.starts_with(PREFIX)
}

fn peer_id(url: &str) -> Result<EndpointId, String> {
    url.strip_prefix(PREFIX)
        .ok_or("Invalid friend address")?
        .parse()
        .map_err(|_| "Invalid friend identity".into())
}

fn load_secret(db: &crate::db::Db) -> Result<Option<SecretKey>, String> {
    db.get_setting(SECRET_SETTING)
        .map(|json| {
            let bytes: [u8; 32] = serde_json::from_str(&json).map_err(|_| {
                "The saved hosting identity is damaged. Restore your app data backup.".to_string()
            })?;
            Ok(SecretKey::from_bytes(&bytes))
        })
        .transpose()
}

struct Hosted {
    endpoint: Endpoint,
    port: u16,
    task: tokio::task::JoinHandle<()>,
}
impl Drop for Hosted {
    fn drop(&mut self) {
        self.task.abort();
    }
}

pub struct PeerNetwork {
    db: Arc<crate::db::Db>,
    hosted: Mutex<Option<Hosted>>,
    client: Mutex<Option<Endpoint>>,
}

impl PeerNetwork {
    pub fn new(db: Arc<crate::db::Db>) -> Self {
        Self { db, hosted: Mutex::new(None), client: Mutex::new(None) }
    }

    pub fn owns(&self, url: &str) -> Result<bool, String> {
        Ok(load_secret(&self.db)?.is_some_and(|key| format!("{PREFIX}{}", key.public()) == url))
    }

    pub async fn host(&self, port: u16) -> Result<String, String> {
        let mut hosted = self.hosted.lock().await;
        if let Some(host) = hosted.as_ref() {
            if host.port == port {
                return Ok(format!("{PREFIX}{}", host.endpoint.id()));
            }
        }
        if let Some(old) = hosted.take() {
            old.endpoint.close().await;
        }
        let secret = match load_secret(&self.db)? {
            Some(key) => key,
            None => {
                let bytes: [u8; 32] = rand::random();
                self.db.set_setting_checked(
                    SECRET_SETTING,
                    &serde_json::to_string(&bytes).map_err(|e| e.to_string())?,
                )?;
                SecretKey::from_bytes(&bytes)
            }
        };
        let endpoint = Endpoint::builder(presets::N0)
            .secret_key(secret)
            .alpns(vec![ALPN.to_vec()])
            .bind()
            .await
            .map_err(|e| e.to_string())?;
        if tokio::time::timeout(Duration::from_secs(25), endpoint.online()).await.is_err() {
            endpoint.close().await;
            return Err("Could not reach the connection service. Check your internet connection and try Host again.".into());
        }
        let address = format!("{PREFIX}{}", endpoint.id());
        let task = serve(endpoint.clone(), port);
        *hosted = Some(Hosted { endpoint, port, task });
        Ok(address)
    }

    pub async fn connect(&self, url: &str) -> Result<Socket, String> {
        let id = peer_id(url)?;
        {
            let hosted = self.hosted.lock().await;
            if let Some(host) = hosted.as_ref().filter(|host| host.endpoint.id() == id) {
                let (socket, _) =
                    tokio_tungstenite::connect_async(format!("ws://127.0.0.1:{}/ws", host.port))
                        .await
                        .map_err(|e| e.to_string())?;
                return Ok(Box::new(socket));
            }
        }
        let endpoint = {
            let mut client = self.client.lock().await;
            if client.is_none() {
                *client = Some(Endpoint::bind(presets::N0).await.map_err(|e| e.to_string())?);
            }
            client.as_ref().unwrap().clone()
        };
        tokio::time::timeout(
            Duration::from_secs(35),
            connect_endpoint(&endpoint, EndpointAddr::new(id)),
        )
        .await
        .map_err(|_| {
            "Your friend is offline or the connection service is unavailable.".to_string()
        })?
    }
}

// Retain the QUIC connection for exactly as long as its WebSocket.
struct PeerIo {
    io: tokio::io::Join<iroh::endpoint::RecvStream, iroh::endpoint::SendStream>,
    connection: iroh::endpoint::Connection,
}
impl Drop for PeerIo {
    fn drop(&mut self) {
        self.connection.close(0u32.into(), b"closed");
    }
}
impl AsyncRead for PeerIo {
    fn poll_read(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.io).poll_read(cx, buf)
    }
}
impl AsyncWrite for PeerIo {
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        Pin::new(&mut self.io).poll_write(cx, buf)
    }
    fn poll_flush(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.io).poll_flush(cx)
    }
    fn poll_shutdown(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.io).poll_shutdown(cx)
    }
}

async fn connect_endpoint(endpoint: &Endpoint, address: EndpointAddr) -> Result<Socket, String> {
    let connection = endpoint.connect(address, ALPN).await.map_err(|e| e.to_string())?;
    let (send, recv) = connection.open_bi().await.map_err(|e| e.to_string())?;
    let io = PeerIo { io: tokio::io::join(recv, send), connection };
    let (socket, _) =
        tokio_tungstenite::client_async_with_config("ws://limusic-peer/ws", io, Some(ws_config()))
            .await
            .map_err(|e| e.to_string())?;
    Ok(Box::new(socket))
}

fn ws_config() -> tokio_tungstenite::tungstenite::protocol::WebSocketConfig {
    tokio_tungstenite::tungstenite::protocol::WebSocketConfig {
        max_message_size: Some(listen_protocol::MAX_ROOM_MESSAGE),
        max_frame_size: Some(listen_protocol::MAX_ROOM_MESSAGE),
        ..Default::default()
    }
}

fn remote_message_allowed(message: &Message) -> bool {
    match message {
        Message::Text(text) => serde_json::from_str::<listen_protocol::ClientMessage>(text)
            .is_ok_and(|m| {
                !matches!(
                    m,
                    listen_protocol::ClientMessage::CreateRoom { .. }
                        | listen_protocol::ClientMessage::OpenSavedRoom { .. }
                        | listen_protocol::ClientMessage::SaveRoom
                )
            }),
        Message::Ping(_) | Message::Pong(_) | Message::Close(_) => true,
        _ => false,
    }
}

fn serve(endpoint: Endpoint, port: u16) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let slots = Arc::new(Semaphore::new(64));
        let mut sessions = tokio::task::JoinSet::new();
        loop {
            tokio::select! {
                Some(_) = sessions.join_next(), if !sessions.is_empty() => {},
                incoming = endpoint.accept() => {
                    let Some(incoming) = incoming else { break };
                    let Ok(permit) = slots.clone().try_acquire_owned() else { incoming.refuse(); continue };
                    sessions.spawn(async move {
                        let _permit = permit;
                        // Bound incomplete handshakes so they cannot exhaust all peer slots.
                        let handshake = async {
                            let connection = incoming.await.map_err(|e| e.to_string())?;
                            let (send, recv) = connection.accept_bi().await.map_err(|e| e.to_string())?;
                            let io = PeerIo { io: tokio::io::join(recv, send), connection };
                            tokio_tungstenite::accept_async_with_config(io, Some(ws_config())).await.map_err(|e| e.to_string())
                        };
                        if let Ok(Ok(socket)) = tokio::time::timeout(Duration::from_secs(15), handshake).await {
                            let _ = bridge(socket, port).await;
                        }
                    });
                }
            }
        }
    })
}

async fn bridge(mut peer: WebSocketStream<PeerIo>, port: u16) -> Result<(), Error> {
    let (mut local, _) =
        tokio_tungstenite::connect_async(format!("ws://127.0.0.1:{port}/ws")).await?;
    loop {
        tokio::select! {
            frame = peer.next() => match frame {
                Some(Ok(frame)) if remote_message_allowed(&frame) => local.send(frame).await?,
                _ => break,
            },
            frame = local.next() => match frame {
                Some(Ok(frame)) => peer.send(frame).await?,
                _ => break,
            },
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use listen_protocol::{ClientMessage as C, ServerMessage as S};

    async fn send(socket: &mut Socket, message: C) {
        socket.send(Message::Text(message.to_json())).await.unwrap();
    }
    async fn receive(socket: &mut Socket, matches: fn(&S) -> bool) -> S {
        tokio::time::timeout(Duration::from_secs(15), async {
            while let Some(frame) = socket.next().await {
                if let Message::Text(text) = frame.unwrap() {
                    let message: S = serde_json::from_str(&text).unwrap();
                    if matches(&message) {
                        return message;
                    }
                }
            }
            panic!("connection closed before expected response")
        })
        .await
        .expect("protocol response timed out")
    }

    #[cfg(windows)]
    async fn exercise_session(public: bool) {
        let folder =
            std::env::temp_dir().join(format!("limusic-peer-test-{}", rand::random::<u64>()));
        std::fs::create_dir_all(&folder).unwrap();
        let db = Arc::new(crate::db::Db::open(&folder.join("app.sqlite")).unwrap());
        let network = PeerNetwork::new(db.clone());
        let relay = super::super::local_relay::LocalRelay::new(
            std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"))
                .join("../target/debug/limusic-sync.exe"),
            folder.join("relay"),
        );
        assert!(!relay.running(), "guests and normal startup do not run a local relay");
        assert!(load_secret(&db).unwrap().is_none());
        let port = relay.ensure_any_port().unwrap();
        let bytes: [u8; 32] = rand::random();
        db.set_setting_checked(SECRET_SETTING, &serde_json::to_string(&bytes).unwrap()).unwrap();
        let secret = load_secret(&db).unwrap().unwrap();
        let endpoint = if public {
            Endpoint::builder(presets::N0)
                .clear_ip_transports()
                .secret_key(secret.clone())
                .alpns(vec![ALPN.to_vec()])
                .bind()
                .await
                .unwrap()
        } else {
            Endpoint::builder(presets::Minimal)
                .clear_ip_transports()
                .bind_addr("127.0.0.1:0")
                .unwrap()
                .secret_key(secret.clone())
                .alpns(vec![ALPN.to_vec()])
                .bind()
                .await
                .unwrap()
        };
        if public {
            tokio::time::timeout(Duration::from_secs(30), endpoint.online())
                .await
                .expect("public relay unavailable");
        }
        let address = format!("{PREFIX}{}", endpoint.id());
        assert!(network.owns(&address).unwrap());
        let task = serve(endpoint.clone(), port);
        *network.hosted.lock().await = Some(Hosted { endpoint: endpoint.clone(), port, task });
        let mut host = network.connect(&address).await.unwrap();
        send(&mut host, C::CreateRoom { username: "Host".into() }).await;
        receive(&mut host, |m| matches!(m, S::RoomCreated { .. })).await;
        send(&mut host, C::SaveRoom).await;
        let S::RoomSaved { room_code, owner_token } =
            receive(&mut host, |m| matches!(m, S::RoomSaved { .. })).await
        else {
            unreachable!()
        };
        let guest_endpoint = if public {
            Endpoint::builder(presets::N0).clear_ip_transports().bind().await.unwrap()
        } else {
            Endpoint::builder(presets::Minimal)
                .clear_ip_transports()
                .bind_addr("127.0.0.1:0")
                .unwrap()
                .bind()
                .await
                .unwrap()
        };
        let target = if public { EndpointAddr::new(endpoint.id()) } else { endpoint.addr() };
        let mut probe = tokio::time::timeout(
            Duration::from_secs(45),
            connect_endpoint(&guest_endpoint, target.clone()),
        )
        .await
        .unwrap()
        .unwrap();
        assert!(super::super::probe_room(&mut probe, &room_code).await.unwrap());
        assert!(!super::super::probe_room(&mut probe, "NOT-OPEN").await.unwrap());
        drop(probe);
        let mut guest = tokio::time::timeout(
            Duration::from_secs(45),
            connect_endpoint(&guest_endpoint, target),
        )
        .await
        .unwrap()
        .unwrap();
        send(&mut guest, C::JoinRoom { room_code: room_code.clone(), username: "Guest".into() })
            .await;
        let S::JoinRequest { user_id, .. } =
            receive(&mut host, |m| matches!(m, S::JoinRequest { .. })).await
        else {
            unreachable!()
        };
        send(&mut host, C::ApproveJoin { user_id }).await;
        let S::JoinApproved { guest_token: Some(guest_token), .. } =
            receive(&mut guest, |m| matches!(m, S::JoinApproved { .. })).await
        else {
            panic!("approval must include a private return credential")
        };
        send(
            &mut host,
            C::Playback(listen_protocol::Playback::at(listen_protocol::PlaybackKind::Seek, 12345)),
        )
        .await;
        let S::SyncPlayback(playback) =
            receive(&mut guest, |m| matches!(m, S::SyncPlayback(_))).await
        else {
            unreachable!()
        };
        assert_eq!(playback.position_ms, 12345);
        // Even a connected, approved guest cannot invoke the private host API.
        send(&mut guest, C::CreateRoom { username: "attacker".into() }).await;
        tokio::time::timeout(Duration::from_secs(5), async {
            while let Some(frame) = guest.next().await {
                if frame.is_err() {
                    break;
                }
            }
        })
        .await
        .expect("forbidden guest command did not close the connection");
        drop(guest);
        drop(host);
        network.hosted.lock().await.take();
        endpoint.close().await;
        relay.stop();
        // Full relay restart and identity reload: no stale IP/port or new invite.
        let reopened = crate::db::Db::open(&folder.join("app.sqlite")).unwrap();
        assert_eq!(load_secret(&reopened).unwrap().unwrap().public(), secret.public());
        let new_port = relay.ensure_any_port().unwrap();
        let restarted = if public {
            Endpoint::builder(presets::N0)
                .clear_ip_transports()
                .secret_key(load_secret(&reopened).unwrap().unwrap())
                .alpns(vec![ALPN.to_vec()])
                .bind()
                .await
                .unwrap()
        } else {
            Endpoint::builder(presets::Minimal)
                .clear_ip_transports()
                .bind_addr("127.0.0.1:0")
                .unwrap()
                .secret_key(load_secret(&reopened).unwrap().unwrap())
                .alpns(vec![ALPN.to_vec()])
                .bind()
                .await
                .unwrap()
        };
        if public {
            tokio::time::timeout(Duration::from_secs(30), restarted.online()).await.unwrap();
        }
        assert_eq!(restarted.id(), peer_id(&address).unwrap());
        let task = serve(restarted.clone(), new_port);
        *network.hosted.lock().await =
            Some(Hosted { endpoint: restarted.clone(), port: new_port, task });
        let mut host = network.connect(&address).await.unwrap();
        send(
            &mut host,
            C::OpenSavedRoom { room_code: room_code.clone(), owner_token, username: "Host".into() },
        )
        .await;
        receive(&mut host, |m| matches!(m, S::RoomCreated { .. })).await;
        let target = if public { EndpointAddr::new(restarted.id()) } else { restarted.addr() };
        let mut guest = tokio::time::timeout(
            Duration::from_secs(45),
            connect_endpoint(&guest_endpoint, target),
        )
        .await
        .unwrap()
        .unwrap();
        assert!(super::super::probe_room(&mut guest, &room_code).await.unwrap());
        send(&mut guest, C::JoinSavedRoom { room_code, username: "Guest".into(), guest_token })
            .await;
        receive(&mut guest, |m| matches!(m, S::JoinApproved { .. })).await;
        drop(guest);
        drop(host);
        network.hosted.lock().await.take();
        restarted.close().await;
        guest_endpoint.close().await;
        relay.stop();
        drop(reopened);
        drop(network);
        drop(db);
        std::fs::remove_dir_all(&folder).unwrap();
    }

    #[cfg(windows)]
    #[tokio::test]
    #[ignore = "requires a built local relay; run scripts/tests/peer.check.ps1"]
    async fn local_peer_approval_sync_and_restart() {
        exercise_session(false).await;
    }

    #[cfg(windows)]
    #[tokio::test]
    #[ignore = "uses public Iroh discovery and relay service; run scripts/tests/peer.check.ps1 -Live"]
    async fn public_relay_discovery_approval_sync_and_restart() {
        exercise_session(true).await;
    }

    #[test]
    fn remote_peers_cannot_create_or_reopen_rooms() {
        use listen_protocol::ClientMessage as C;
        for message in [
            C::CreateRoom { username: "outsider".into() },
            C::SaveRoom,
            C::OpenSavedRoom {
                room_code: "ABCDEF".into(),
                owner_token: "secret".into(),
                username: "outsider".into(),
            },
        ] {
            assert!(!remote_message_allowed(&Message::Text(message.to_json())));
        }
        assert!(remote_message_allowed(&Message::Text(
            C::JoinRoom { room_code: "ABCDEF".into(), username: "guest".into() }.to_json()
        )));
        assert!(!remote_message_allowed(&Message::Text("invalid".into())));
    }
}
