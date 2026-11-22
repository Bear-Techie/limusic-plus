use super::*;
use listen_protocol::RecoveryPeer;
use std::{collections::HashMap, time::Instant};

#[derive(Clone)]
pub(super) struct RecoveryContext {
    epoch: String,
    peers: Vec<RecoveryPeer>,
    my_id: String,
    username: String,
    lost_at: Option<Instant>,
}

#[derive(Clone)]
pub(super) struct RecoveryInvitation {
    epoch: String,
    peer: RecoveryPeer,
    offer: recovery::Offer,
}

pub(super) enum RecoveryTransition {
    Host { source: RecoveryContext, state: RoomState, grants: HashMap<String, String> },
    Join { source: RecoveryContext },
}

impl LtSession {
    pub(super) async fn clear_recovery(&self) {
        self.recovery_context.lock().await.take();
        self.recovery_invitation.lock().await.take();
        self.recovery_transition.lock().await.take();
        self.recovery.clear().await;
        let mut inner = self.inner.lock().await;
        inner.recovery_disconnected = false;
        inner.recovery_inviter = None;
        inner.recovery_busy = false;
    }

    pub(super) async fn register_recovery(&self, supported: bool) {
        let (server, member) = {
            let inner = self.inner.lock().await;
            (inner.server_url.clone(), inner.my_id.clone())
        };
        if !supported || !peer::is_peer(&server) {
            return;
        }
        match self.recovery.ensure_listener().await {
            Ok(endpoint_id) => {
                if self.inner.lock().await.my_id == member {
                    self.send(ClientMessage::RegisterRecovery { endpoint_id }).await;
                }
            }
            Err(error) => {
                self.emit_notice(&format!("Room joined, but recovery is unavailable: {error}"))
                    .await
            }
        }
    }

    pub(super) async fn update_recovery_peers(&self, epoch: String, peers: Vec<RecoveryPeer>) {
        let inner = self.inner.lock().await;
        let Some(me) = inner.my_id.clone() else { return };
        let Some(user) = inner.users.iter().find(|u| u.user_id == me) else { return };
        // Ignore malformed or foreign rosters, including ones sent before our own registration.
        if epoch.len() > 128
            || peers.len() > 50
            || !peers.iter().any(|p| p.user_id == me)
            || peers.iter().any(|p| {
                p.endpoint_id.parse::<iroh::EndpointId>().is_err()
                    || !inner.users.iter().any(|u| u.user_id == p.user_id)
            })
        {
            return;
        }
        let context = RecoveryContext {
            epoch,
            peers,
            my_id: me,
            username: user.username.clone(),
            lost_at: None,
        };
        *self.recovery_context.lock().await = Some(context);
        drop(inner);
        self.refresh_bot_room().await;
    }

    pub(super) async fn refresh_bot_room(&self) {
        let inner = self.inner.lock().await;
        let context = self.recovery_context.lock().await;
        let room = context.as_ref().filter(|context| inner.status_connected
            && inner.my_id.as_deref() == Some(context.my_id.as_str())
            && peer::is_peer(&inner.server_url)).map(|context| {
            let host = context.peers.iter().find(|p| inner.users.iter().any(|u|
                u.user_id == p.user_id && u.is_host && u.is_connected));
            bot_share::Room {
                epoch: context.epoch.clone(),
                host: host.filter(|p| p.user_id != context.my_id)
                    .map(|p| (p.endpoint_id.clone(), p.username.clone())),
                guests: context.peers.iter().filter(|p| inner.role == Role::Host
                    && p.user_id != context.my_id
                    && inner.users.iter().any(|u| u.user_id == p.user_id && u.is_connected))
                    .map(|p| p.endpoint_id.clone()).collect(),
            }
        });
        self.recovery.set_bot_room(room).await;
    }

    pub(crate) async fn share_bot(&self, bot: bot_share::BotPayload) -> Result<usize, String> {
        let inner = self.inner.lock().await;
        if !inner.status_connected || inner.role != Role::Host || !peer::is_peer(&inner.server_url) {
            return Err("Only the host of a connected Plus room can share a bot.".into());
        }
        drop(inner);
        self.recovery.announce_bot(bot).await
    }

    pub(crate) async fn bot_notices(&self) -> Vec<bot_share::Notice> {
        if !self.inner.lock().await.status_connected {
            return Vec::new();
        }
        self.recovery.bot_notices().await
    }

    pub(crate) async fn dismiss_bot(&self, id: &str) {
        self.recovery.dismiss_bot(id).await;
    }

    pub(crate) async fn accept_bot(&self, id: String) -> Result<bot_share::BotPayload, String> {
        let generation = self.gen.load(Ordering::SeqCst);
        if !self.inner.lock().await.status_connected {
            return Err("The room is no longer connected.".into());
        }
        let bot = self.recovery.accept_bot(id).await?;
        if self.gen.load(Ordering::SeqCst) != generation || !self.inner.lock().await.status_connected {
            return Err("The room changed while accepting the bot.".into());
        }
        Ok(bot)
    }

    pub(super) async fn recovery_tick(&self) {
        let generation = self.gen.load(Ordering::SeqCst);
        let (connected, busy) = {
            let inner = self.inner.lock().await;
            (inner.status_connected && !inner.requesting, inner.recovery_busy)
        };
        self.refresh_bot_room().await;
        if busy {
            return;
        }
        let context = {
            let mut guard = self.recovery_context.lock().await;
            let Some(context) = guard.as_mut() else { return };
            if connected {
                context.lost_at = None;
                None
            } else {
                let lost = *context.lost_at.get_or_insert_with(Instant::now);
                (lost.elapsed() >= Duration::from_secs(8)).then(|| context.clone())
            }
        };
        let Some(context) = context else {
            let changed = {
                let mut inner = self.inner.lock().await;
                if self.gen.load(Ordering::SeqCst) != generation {
                    return;
                }
                let changed = inner.recovery_disconnected || inner.recovery_inviter.is_some();
                inner.recovery_disconnected = false;
                inner.recovery_inviter = None;
                changed
            };
            if connected {
                self.recovery_invitation.lock().await.take();
            }
            if changed {
                self.emit_state().await;
            }
            return;
        };
        {
            let mut inner = self.inner.lock().await;
            if self.gen.load(Ordering::SeqCst) != generation || inner.status_connected {
                return;
            }
            if !inner.recovery_disconnected {
                inner.recovery_disconnected = true;
                drop(inner);
                self.emit_state().await;
            }
        }
        let peers = context.peers.iter().filter(|p| p.user_id != context.my_id).cloned().collect();
        let mut offers = self.recovery.poll(&context.epoch, peers).await;
        // Prefer the previous playback host if multiple members volunteer simultaneously.
        offers.sort_by(|(a, _), (b, _)| b.is_host.cmp(&a.is_host).then(a.user_id.cmp(&b.user_id)));
        if self.gen.load(Ordering::SeqCst) != generation {
            return;
        }
        let mut inner = self.inner.lock().await;
        if self.gen.load(Ordering::SeqCst) != generation || inner.status_connected || inner.recovery_busy {
            return;
        }
        let inviter = offers.first().map(|(p, _)| p.username.clone());
        let changed = inviter != inner.recovery_inviter;
        inner.recovery_inviter = inviter;
        drop(inner);
        *self.recovery_invitation.lock().await = offers
            .into_iter()
            .next()
            .map(|(peer, offer)| RecoveryInvitation { epoch: context.epoch, peer, offer });
        if changed {
            self.emit_state().await;
        }
    }

    /// The click authorizes hosting on this PC and inviting this session's remaining members.
    pub async fn host_recovery(self: &Arc<Self>, mut state: RoomState) -> Result<(), String> {
        let _action =
            self.recovery_action.try_lock().map_err(|_| "Recovery is already in progress.")?;
        if self.app.config().identifier != "com.limusic.plus" || !self.local_relay.available() {
            return Err("Hosting a replacement room requires the Windows Plus package.".into());
        }
        if self.inner.lock().await.status_connected {
            return Err("The room is connected again. No replacement is needed.".into());
        }
        let source = self
            .recovery_context
            .lock()
            .await
            .clone()
            .ok_or("There are no recovery contacts for this session.")?;
        let generation = self.gen.load(Ordering::SeqCst);
        self.inner.lock().await.recovery_busy = true;
        self.emit_state().await;
        // Start the server before touching membership. A bind/discovery failure keeps the old room.
        let result = self.start_peer_host().await;
        let (address, port) = match result {
            Ok(value) => value,
            Err(error) => {
                self.inner.lock().await.recovery_busy = false;
                self.emit_state().await;
                return Err(error);
            }
        };
        if self.gen.load(Ordering::SeqCst) != generation || self.inner.lock().await.status_connected
        {
            self.inner.lock().await.recovery_busy = false;
            self.emit_state().await;
            return Err(
                "The room changed while preparing recovery. Please try again if needed.".into()
            );
        }
        state.current_track = state.current_track.filter(shareable);
        state.queue.retain(shareable);
        state.queue.truncate(listen_protocol::MAX_SHARED_QUEUE);
        let grants = source
            .peers
            .iter()
            .filter(|p| p.user_id != source.my_id)
            .map(|p| (p.endpoint_id.clone(), fresh_pass()))
            .collect();
        let username = source.username.clone();
        *self.recovery_transition.lock().await =
            Some(RecoveryTransition::Host { source, state, grants });
        self.set_server_url(address).await;
        // A fresh room avoids overwriting any already-open saved room owned by this PC.
        self.start(ClientMessage::CreateRoom { username }, Some(port)).await;
        Ok(())
    }

    /// Consent is required even when the invitation is from the previous playback host.
    pub async fn accept_recovery(self: &Arc<Self>) -> Result<(), String> {
        let _action =
            self.recovery_action.try_lock().map_err(|_| "Recovery is already in progress.")?;
        let invitation = self
            .recovery_invitation
            .lock()
            .await
            .clone()
            .ok_or("That invitation is no longer available.")?;
        let source =
            self.recovery_context.lock().await.clone().ok_or("The original session has ended.")?;
        let generation = self.gen.load(Ordering::SeqCst);
        if self.inner.lock().await.status_connected
            || source.epoch != invitation.epoch
            || !source.peers.iter().any(|p| p.endpoint_id == invitation.peer.endpoint_id)
        {
            return Err("The room changed. Please check the current invitation.".into());
        }
        // Verify the replacement is accepting members before leaving our recovery context.
        let mut socket = tokio::time::timeout(
            Duration::from_secs(10),
            self.connect_room(&invitation.offer.server_url),
        )
        .await
        .map_err(|_| "The replacement room could not be reached.")??;
        let open = tokio::time::timeout(
            Duration::from_secs(5),
            probe_room(&mut socket, &invitation.offer.room_code),
        )
        .await
        .map_err(|_| "The replacement room did not respond.")??;
        if !open {
            return Err("The replacement room has closed.".into());
        }
        if self.gen.load(Ordering::SeqCst) != generation || self.inner.lock().await.status_connected
        {
            return Err("The original room changed or reconnected.".into());
        }
        self.inner.lock().await.recovery_busy = true;
        let username = source.username.clone();
        *self.recovery_transition.lock().await = Some(RecoveryTransition::Join { source });
        self.set_server_url(invitation.offer.server_url).await;
        self.start(
            ClientMessage::JoinSavedRoom {
                room_code: invitation.offer.room_code,
                username,
                guest_token: invitation.offer.guest_token,
            },
            None,
        )
        .await;
        Ok(())
    }

    pub(super) async fn recovery_seed(&self) -> Option<(RoomState, Vec<String>)> {
        match self.recovery_transition.lock().await.as_ref() {
            Some(RecoveryTransition::Host { state, grants, .. }) => {
                Some((state.clone(), grants.values().cloned().collect()))
            }
            _ => None,
        }
    }

    pub(super) async fn publish_recovery(&self, room_code: String) {
        let server_url = {
            let inner = self.inner.lock().await;
            if inner.room_code.as_ref() != Some(&room_code) {
                return;
            }
            inner.server_url.clone()
        };
        if let Some(RecoveryTransition::Host { source, grants, .. }) =
            self.recovery_transition.lock().await.take()
        {
            let offers = grants
                .into_iter()
                .map(|(id, guest_token)| {
                    (
                        id,
                        recovery::Offer {
                            server_url: server_url.clone(),
                            room_code: room_code.clone(),
                            guest_token,
                        },
                    )
                })
                .collect();
            self.recovery.publish(source.epoch, source.peers, offers).await;
            // Seed again from the live player so setup time doesn't rewind the new host.
            let _ = self.sync_tx.send(SyncCommand::HostSeed);
        }
        self.inner.lock().await.recovery_busy = false;
        self.emit_notice("Replacement room ready. Your friends can choose to join.").await;
    }

    pub(super) async fn restore_recovery_context(&self) -> bool {
        let transition = self.recovery_transition.lock().await.take();
        if let Some(transition) = transition {
            let source = match transition {
                RecoveryTransition::Host { source, .. } | RecoveryTransition::Join { source } => {
                    source
                }
            };
            *self.recovery_context.lock().await = Some(source);
            true
        } else {
            false
        }
    }
}

fn fresh_pass() -> String {
    use rand::RngCore;
    let mut bytes = [0u8; 32];
    rand::thread_rng().fill_bytes(&mut bytes);
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}
