//! Listen Together client. Owns the WebSocket connection to a self-hosted `limusic-sync` server
//! (context/19), the room state, and reconnection. It knows nothing about mpv or the orchestrator:
//! playback for guests is driven through an mpsc channel of [`SyncCommand`]s that a bridge task
//! (in `lib.rs`) applies to `AppState`. The host's playback is broadcast by `AppState` calling
//! [`LtSession::broadcast_playback`].
//!
//! No back-reference to `AppState` (avoids an Arc cycle): everything the connection needs is the
//! `AppHandle` (to emit UI events) and the sync channel.

pub(crate) mod bot_share;
mod local_relay;
mod peer;
mod recovery;
mod recovery_session;
use recovery_session::{RecoveryContext, RecoveryInvitation, RecoveryTransition};

use std::net::SocketAddr;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Duration;

use futures_util::{SinkExt, StreamExt};
use listen_protocol::{
    ClientMessage, Playback, PlaybackKind, RoomState, ServerMessage, Suggestion, Track, User,
};
use tauri::{AppHandle, Emitter};
use tokio::net::TcpStream;
use tokio::sync::{mpsc, Mutex};
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::{Error as WsError, Message};
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};

/// Playback intents the connection hands to the bridge task to apply to the local player.
#[derive(Debug, Clone)]
pub enum SyncCommand {
    /// Full state (after join / reconnect / request-sync): resolve current track, seek to live
    /// position, set play/pause, mirror the queue.
    ApplyState(RoomState),
    /// `track` is `None` when the host is on something a guest can't play (a local file).
    ChangeTrack {
        track: Option<Track>,
        position_ms: i64,
        playing: bool,
        queue: Vec<Track>,
    },
    Play {
        position_ms: i64,
    },
    Pause {
        position_ms: i64,
    },
    Seek {
        position_ms: i64,
    },
    /// Guest: mirror the host's upcoming queue (a guest added/the host removed a track).
    SyncQueue {
        queue: Vec<Track>,
    },
    /// Host: a guest's track to insert at the guest boundary (auto-approved suggestion,
    /// `queued_by` already stamped with the guest's name).
    GuestAdd {
        track: Track,
    },
    /// We just became host of a freshly-created room — seed the room with our current now-playing.
    HostSeed,
    /// We left / were kicked / became host — stop applying remote playback.
    Release,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Role {
    None,
    Host,
    Guest,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Status {
    Disconnected,
    Connecting,
    Connected,
}

impl Status {
    fn as_str(self) -> &'static str {
        match self {
            Status::Disconnected => "disconnected",
            Status::Connecting => "connecting",
            Status::Connected => "connected",
        }
    }
}

/// The server a user gets when they have set none of their own. Kept out of the UI on purpose:
/// the settings field seeds empty, `snapshot_of` reports it empty, and an invite minted on it is a
/// bare room code, so nobody has to see (or retype) somebody else's hostname to listen together.
/// An empty `server_url` means "this one", resolved at connect time in [`LtSession::run`], so
/// clearing the field is how a self-hoster comes back to the default.
const DEFAULT_SERVER: &str = "wss://fedora-1.tail9c4985.ts.net/ws";

#[derive(Default)]
struct Inner {
    recovery_disconnected: bool,
    recovery_inviter: Option<String>,
    recovery_busy: bool,
    hosted_relay_port: Option<u16>,
    supports_saved_rooms: bool,
    unsaved_owner_token: Option<String>,
    saved_room: bool,
    status_connected: bool,
    connecting: bool,
    /// We've asked to create/join and are waiting for the room to materialize (host approval).
    /// Drives the guest's "waiting" UI and blocks duplicate requests.
    requesting: bool,
    role: Role,
    server_url: String,
    room_code: Option<String>,
    my_id: Option<String>,
    session_token: Option<String>,
    users: Vec<User>,
    current_track: Option<Track>,
    queue: Vec<Track>,
    pending_joins: Vec<(String, String)>, // (user_id, username)
    suggestions: Vec<Suggestion>,
    outbound: Option<mpsc::UnboundedSender<ClientMessage>>,
}

impl Default for Role {
    fn default() -> Self {
        Role::None
    }
}

impl Inner {
    fn status(&self) -> Status {
        if self.status_connected {
            Status::Connected
        } else if self.connecting {
            Status::Connecting
        } else {
            Status::Disconnected
        }
    }

    /// Clear all room membership (leave / kicked / fatal). Keeps `server_url`.
    fn reset_room(&mut self) {
        self.role = Role::None;
        self.hosted_relay_port = None;
        self.unsaved_owner_token = None;
        self.saved_room = false;
        self.room_code = None;
        self.my_id = None;
        self.session_token = None;
        self.users.clear();
        self.current_track = None;
        self.queue.clear();
        self.pending_joins.clear();
        self.suggestions.clear();
        self.outbound = None;
        self.status_connected = false;
        self.connecting = false;
        self.requesting = false;
    }

    fn absorb(&mut self, state: &RoomState) {
        self.supports_saved_rooms = state.supports_saved_rooms;
        self.saved_room = state.saved;
        self.room_code = Some(state.room_code.clone());
        self.users = state.users.clone();
        self.current_track = state.current_track.clone();
        self.queue = state.queue.clone();
    }
}

pub struct LtSession {
    recovery: recovery::RecoveryNetwork,
    recovery_context: Mutex<Option<RecoveryContext>>,
    recovery_invitation: Mutex<Option<RecoveryInvitation>>,
    recovery_transition: Mutex<Option<RecoveryTransition>>,
    recovery_action: Mutex<()>,
    peer: peer::PeerNetwork,
    local_relay: Arc<local_relay::LocalRelay>,
    db: Arc<crate::db::Db>,
    saved: Mutex<Vec<SavedRoom>>,
    saved_load_error: Option<String>,
    hosting_action: Mutex<()>,
    app: AppHandle,
    inner: Arc<Mutex<Inner>>,
    sync_tx: mpsc::UnboundedSender<SyncCommand>,
    /// Bumped to cancel the running connection loop (leave / new connection).
    gen: AtomicU64,
}

#[derive(Clone, serde::Serialize, serde::Deserialize)]
struct SavedRoom {
    #[serde(default)]
    recovery_room: bool,
    #[serde(default)]
    last_used_ms: i64,
    #[serde(default)]
    name: String,
    #[serde(default)]
    favorite: bool,
    #[serde(default)]
    host_name: String,
    #[serde(default)]
    local_relay_port: Option<u16>,
    server_url: String,
    room_code: String,
    username: String,
    owner_token: Option<String>,
}

impl SavedRoom {
    fn relay_to_start(&self) -> Option<u16> {
        self.local_relay_port.or_else(|| {
            self.owner_token.as_ref().and_then(|_| local_relay::loopback_port(&self.server_url))
        })
    }

    fn view(&self) -> serde_json::Value {
        serde_json::json!({
            "serverUrl": self.server_url, "roomCode": self.room_code,
            "username": self.username, "owner": self.owner_token.is_some(),
            "hostName": self.host_name,
            "name": self.name, "favorite": self.favorite,
            "lastUsedMs": self.last_used_ms,
        })
    }
}

fn upsert_saved_room(saved: &mut Vec<SavedRoom>, record: SavedRoom) {
    if let Some(old) = saved
        .iter_mut()
        .find(|r| r.server_url == record.server_url && r.room_code == record.room_code)
    {
        let port = record.local_relay_port.or(old.local_relay_port);
        let host_name =
            if old.host_name.is_empty() { record.host_name.clone() } else { old.host_name.clone() };
        let token = record.owner_token.or_else(|| old.owner_token.clone());
        *old = SavedRoom {
            recovery_room: old.recovery_room || record.recovery_room,
            last_used_ms: old.last_used_ms.max(record.last_used_ms),
            name: old.name.clone(),
            favorite: old.favorite,
            owner_token: token,
            local_relay_port: port,
            host_name,
            ..record
        };
    } else {
        saved.push(record);
    }
}

#[derive(serde::Serialize, serde::Deserialize)]
struct GuestPass {
    server_url: String,
    room_code: String,
    token: String,
}

fn canonical_server(url: &str) -> &str {
    if url.is_empty() {
        DEFAULT_SERVER
    } else {
        url
    }
}

fn guest_pass(db: &crate::db::Db, server: &str, code: &str) -> Option<String> {
    let records: Vec<GuestPass> = serde_json::from_str(&db.get_setting("lt_guest_passes")?).ok()?;
    records
        .into_iter()
        .find(|r| r.server_url == canonical_server(server) && r.room_code == code)
        .map(|r| r.token)
}

fn remember_guest_pass(
    db: &crate::db::Db,
    server: &str,
    code: &str,
    token: String,
) -> Result<(), String> {
    let mut records: Vec<GuestPass> = db
        .get_setting("lt_guest_passes")
        .map(|json| {
            serde_json::from_str(&json).map_err(|_| {
                "Saved guest permissions could not be read; existing data was kept.".to_string()
            })
        })
        .transpose()?
        .unwrap_or_default();
    let server_url = canonical_server(server).to_string();
    records.retain(|r| r.server_url != server_url || r.room_code != code);
    records.push(GuestPass { server_url, room_code: code.to_string(), token });
    db.set_setting_checked(
        "lt_guest_passes",
        &serde_json::to_string(&records).map_err(|e| e.to_string())?,
    )
}

fn load_saved_rooms(db: &crate::db::Db) -> Result<Vec<SavedRoom>, String> {
    db.get_setting("lt_saved_rooms")
        .map(|s| {
            let records: Vec<SavedRoom> = serde_json::from_str(&s).map_err(|e| e.to_string())?;
            let mut hosts = Vec::new();
            for record in records {
                upsert_saved_room(&mut hosts, record);
            }
            Ok(hosts)
        })
        .unwrap_or_else(|| Ok(Vec::new()))
}

// Keep the normal Host destination independent of shortcuts and recovery rooms.
// Never replace a damaged or mismatched identity by silently creating another room.
fn own_room(
    db: &crate::db::Db,
    saved: &[SavedRoom],
    address: &str,
) -> Result<Option<SavedRoom>, String> {
    if let Some(json) = db.get_setting("lt_own_room") {
        let room: SavedRoom = serde_json::from_str(&json)
            .map_err(|_| "Your own room could not be read. Restore your app data backup.")?;
        if room.server_url != address || room.owner_token.is_none() {
            return Err("Your own room does not match this hosting identity. Restore your app data backup.".into());
        }
        return Ok(Some(room));
    }
    // Older installations kept ownership only in the saved list. Its first owned
    // room is the original; later recovery rooms must not take its place.
    let room = saved
        .iter()
        .find(|r| r.server_url == address && r.owner_token.is_some() && !r.recovery_room)
        .cloned();
    if let Some(room) = &room {
        db.set_setting_checked(
            "lt_own_room",
            &serde_json::to_string(room).map_err(|e| e.to_string())?,
        )?;
    }
    Ok(room)
}

fn now_ms() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|duration| duration.as_millis() as i64)
        .unwrap_or_default()
}

async fn probe_room(socket: &mut peer::Socket, code: &str) -> Result<bool, String> {
    socket
        .send(Message::Text(ClientMessage::CheckRoom { room_code: code.to_string() }.to_json()))
        .await
        .map_err(|e| e.to_string())?;
    while let Some(frame) = socket.next().await {
        let frame = frame.map_err(|e| e.to_string())?;
        if let Message::Text(text) = frame {
            match serde_json::from_str::<ServerMessage>(&text).map_err(|e| e.to_string())? {
                ServerMessage::RoomAvailability { room_code, open } if room_code == code => {
                    return Ok(open)
                }
                ServerMessage::Pong => {}
                _ => return Err("Relay does not support availability checks".into()),
            }
        }
    }
    Err("Connection closed during availability check".into())
}

impl LtSession {
    /// Create the session. Returns the receiver the bridge task drains to drive guest playback.
    pub fn new(
        app: AppHandle,
        server_url: String,
        db: Arc<crate::db::Db>,
    ) -> (Arc<Self>, mpsc::UnboundedReceiver<SyncCommand>) {
        // rustls needs a process-wide crypto provider before the first `wss://` handshake.
        let _ = rustls::crypto::ring::default_provider().install_default();
        let (sync_tx, sync_rx) = mpsc::unbounded_channel();
        let inner = Inner { server_url, ..Inner::default() };
        let loaded = load_saved_rooms(&db);
        let saved_load_error = loaded.as_ref().err().cloned();
        let s = Arc::new(LtSession {
            recovery: recovery::RecoveryNetwork::default(),
            recovery_context: Mutex::new(None),
            recovery_invitation: Mutex::new(None),
            recovery_transition: Mutex::new(None),
            recovery_action: Mutex::new(()),
            peer: peer::PeerNetwork::new(db.clone()),
            local_relay: Arc::new(local_relay::LocalRelay::bundled()),
            saved: Mutex::new(loaded.unwrap_or_default()),
            saved_load_error,
            hosting_action: Mutex::new(()),
            db,
            app,
            inner: Arc::new(Mutex::new(inner)),
            sync_tx,
            gen: AtomicU64::new(0),
        });
        let weak = Arc::downgrade(&s);
        tauri::async_runtime::spawn(async move {
            loop {
                tokio::time::sleep(Duration::from_secs(3)).await;
                let Some(session) = weak.upgrade() else { break };
                session.recovery_tick().await;
            }
        });
        (s, sync_rx)
    }

    // --- public API (called by commands + AppState) -----------------------------------------

    async fn remember_room(&self, owner_token: Option<String>) -> Result<(), String> {
        if self.saved_load_error.is_some() {
            return Err("Saved room data could not be read; repair it before saving.".into());
        }
        let recovery_room = matches!(
            *self.recovery_transition.lock().await,
            Some(RecoveryTransition::Host { .. })
        );
        let primary = owner_token.is_some()
            && self.app.config().identifier == "com.limusic.plus"
            && !recovery_room;
        let inner = self.inner.lock().await;
        let room_code = inner.room_code.clone().ok_or("Join a room first")?;
        let username = inner
            .users
            .iter()
            .find(|u| Some(&u.user_id) == inner.my_id.as_ref())
            .map(|u| u.username.clone())
            .ok_or("Room identity unavailable")?;
        // Pin the actual relay, including today's built-in default. A future default-server
        // change must never send this reservation or its owner credential to another relay.
        let server_url = if inner.server_url.is_empty() {
            DEFAULT_SERVER.to_string()
        } else {
            inner.server_url.clone()
        };
        let record = SavedRoom {
            recovery_room,
            last_used_ms: now_ms(),
            name: String::new(),
            favorite: false,
            host_name: inner
                .users
                .iter()
                .find(|u| u.is_host)
                .map(|u| u.username.clone())
                .unwrap_or_else(|| server_url.clone()),
            local_relay_port: inner.hosted_relay_port.filter(|_| inner.role == Role::Host),
            server_url,
            room_code,
            username,
            owner_token,
        };
        let mut saved = self.saved.lock().await;
        let mut next = saved.clone();
        upsert_saved_room(&mut next, record.clone());
        if primary && peer::is_peer(&record.server_url) {
            // Persist ownership before updating the removable shortcut list.
            own_room(&self.db, &next, &record.server_url)?;
        }
        self.db.set_setting_checked(
            "lt_saved_rooms",
            &serde_json::to_string(&next).map_err(|e| e.to_string())?,
        )?;
        *saved = next;
        Ok(())
    }

    pub async fn save_room(&self) -> Result<(), String> {
        if self.saved_load_error.is_some() {
            return Err("Saved room data could not be read; repair it before saving.".into());
        }
        let inner = self.inner.lock().await;
        if inner.role == Role::None || !inner.status_connected {
            return Err("Connect to a room before saving.".into());
        }
        if !inner.saved_room {
            if inner.role == Role::Host && inner.supports_saved_rooms {
                drop(inner);
                self.send(ClientMessage::SaveRoom).await;
                return Ok(());
            }
            return Err("This session needs an updated relay with saved rooms enabled.".into());
        }
        let token = inner.unsaved_owner_token.clone();
        drop(inner);
        self.remember_room(token).await?;
        self.emit_notice("Host saved on this device.").await;
        self.emit_state().await;
        Ok(())
    }

    pub async fn forget_room(&self, server: String, code: String) -> Result<(), String> {
        if self.saved_load_error.is_some() {
            return Err("Saved room data could not be read; repair it before saving.".into());
        }
        let mut saved = self.saved.lock().await;
        if saved.iter().any(|r| {
            r.server_url == server && r.room_code == code && r.owner_token.is_some()
        }) {
            return Err("Your own rooms are permanent and cannot be forgotten.".into());
        }
        let next: Vec<_> = saved
            .iter()
            .filter(|r| r.server_url != server || r.room_code != code)
            .cloned()
            .collect();
        self.db.set_setting_checked(
            "lt_saved_rooms",
            &serde_json::to_string(&next).map_err(|e| e.to_string())?,
        )?;
        *saved = next;
        drop(saved);
        self.emit_state().await;
        Ok(())
    }

    /// Inspect only saved friend rooms, without joining or starting a local relay.
    pub async fn check_saved_room(&self, server: String, code: String) -> &'static str {
        if !self
            .saved
            .lock()
            .await
            .iter()
            .any(|r| r.server_url == server && r.room_code == code && r.owner_token.is_none())
        {
            return "unknown";
        }
        let result = tokio::time::timeout(Duration::from_secs(8), async {
            let mut socket = self.connect_room(canonical_server(&server)).await?;
            probe_room(&mut socket, &code).await
        })
        .await;
        match result {
            Ok(Ok(true)) => "open",
            Ok(Ok(false)) => "closed",
            _ => "unknown",
        }
    }

    async fn record_recent_use(&self) {
        let inner = self.inner.lock().await;
        let Some(code) = inner.room_code.as_ref() else { return };
        let mut saved = self.saved.lock().await;
        let mut next = saved.clone();
        let Some(record) = next
            .iter_mut()
            .find(|r| r.room_code == *code && r.server_url == canonical_server(&inner.server_url))
        else {
            return;
        };
        record.last_used_ms = now_ms();
        drop(inner);
        if let Ok(json) = serde_json::to_string(&next) {
            if self.db.set_setting_checked("lt_saved_rooms", &json).is_ok() {
                *saved = next;
            }
        }
    }

    pub async fn update_saved_room(
        &self,
        server: String,
        code: String,
        name: Option<String>,
        favorite: Option<bool>,
    ) -> Result<(), String> {
        if self.saved_load_error.is_some() {
            return Err("Saved rooms could not be read; existing data was kept.".into());
        }
        let mut saved = self.saved.lock().await;
        let mut next = saved.clone();
        let room = next
            .iter_mut()
            .find(|r| r.server_url == server && r.room_code == code)
            .ok_or("Saved room not found")?;
        if let Some(name) = name {
            let name = name.trim();
            if name.chars().count() > 80 || name.chars().any(char::is_control) {
                return Err("Choose a room name of at most 80 characters.".into());
            }
            room.name = name.to_string();
        }
        if let Some(favorite) = favorite {
            room.favorite = favorite;
        }
        self.db.set_setting_checked(
            "lt_saved_rooms",
            &serde_json::to_string(&next).map_err(|e| e.to_string())?,
        )?;
        *saved = next;
        drop(saved);
        self.emit_state().await;
        Ok(())
    }

    fn guest_join(&self, server: &str, room_code: String, username: String) -> ClientMessage {
        match guest_pass(&self.db, server, &room_code) {
            Some(guest_token) => ClientMessage::JoinSavedRoom { room_code, username, guest_token },
            None => ClientMessage::JoinRoom { room_code, username },
        }
    }

    pub async fn open_saved(self: &Arc<Self>, server: String, code: String) -> Result<(), String> {
        let _action = self.hosting_action.try_lock().map_err(|_| "A room is already opening.")?;
        let inner = self.inner.lock().await;
        if inner.role != Role::None || inner.requesting {
            return Err("Leave the current room first.".into());
        }
        drop(inner);
        let record = self
            .saved
            .lock()
            .await
            .iter()
            .find(|r| r.server_url == server && r.room_code == code)
            .cloned()
            .ok_or("Saved room not found")?;
        let relay_port = if peer::is_peer(&record.server_url) {
            if record.owner_token.is_some() {
                if !self.peer.owns(&record.server_url)? {
                    return Err("This hosting identity belongs to another installation. Join it as a guest instead.".into());
                }
                Some(self.start_peer_host().await?.1)
            } else {
                None
            }
        } else {
            let port = record.relay_to_start();
            if let Some(port) = port {
                self.ensure_local_relay(port).await?;
            }
            port
        };
        self.set_server_url(record.server_url.clone()).await;
        let message = match record.owner_token {
            Some(owner_token) => ClientMessage::OpenSavedRoom {
                room_code: record.room_code,
                owner_token,
                username: record.username,
            },
            None => self.guest_join(&record.server_url, record.room_code, record.username),
        };
        self.start(message, relay_port).await;
        Ok(())
    }

    pub async fn set_server_url(&self, url: String) {
        self.inner.lock().await.server_url = url;
        self.emit_state().await;
    }

    /// True when we're in a room in any role.
    pub async fn in_room(&self) -> bool {
        self.inner.lock().await.role != Role::None
    }

    /// True when we're a guest in a room — the caller should block local playback control.
    pub async fn is_guest(&self) -> bool {
        self.inner.lock().await.role == Role::Guest
    }

    pub async fn is_host(&self) -> bool {
        self.inner.lock().await.role == Role::Host
    }

    /// Our own display name in the room (None outside one) — stamps the host's queue adds.
    pub async fn my_username(&self) -> Option<String> {
        let inner = self.inner.lock().await;
        let me = inner.my_id.as_deref()?;
        inner.users.iter().find(|u| u.user_id == me).map(|u| u.username.clone())
    }

    /// Host: broadcast a playback action to the room. No-op unless we're the host and connected.
    pub async fn broadcast_playback(&self, mut p: Playback) {
        keep_shareable(&mut p);
        let inner = self.inner.lock().await;
        if inner.role != Role::Host {
            return;
        }
        if let Some(tx) = &inner.outbound {
            let _ = tx.send(ClientMessage::Playback(p));
        }
    }

    /// Send any client message on the current connection (best-effort).
    async fn send(&self, msg: ClientMessage) {
        if let Some(tx) = &self.inner.lock().await.outbound {
            let _ = tx.send(msg);
        }
    }

    async fn ensure_local_relay(&self, port: u16) -> Result<(), String> {
        let relay = self.local_relay.clone();
        tokio::task::spawn_blocking(move || relay.ensure_running(port))
            .await
            .map_err(|e| e.to_string())?
    }

    pub fn stop_local_relay(&self) {
        self.local_relay.stop();
    }

    async fn start_peer_host(&self) -> Result<(String, u16), String> {
        let relay = self.local_relay.clone();
        let port = tokio::task::spawn_blocking(move || relay.ensure_any_port())
            .await
            .map_err(|e| e.to_string())??;
        match self.peer.host(port).await {
            Ok(address) => Ok((address, port)),
            Err(error) => {
                self.local_relay.stop();
                Err(error)
            }
        }
    }

    pub async fn create_room(
        self: &Arc<Self>,
        username: String,
        run_local_relay: bool,
    ) -> Result<(), String> {
        let _action = self.hosting_action.try_lock().map_err(|_| "A room is already opening.")?;
        if self.app.config().identifier == "com.limusic.plus" {
            if self.saved_load_error.is_some() {
                return Err("Saved rooms could not be read. Restore your app data before hosting.".into());
            }
            if self.in_room().await || self.inner.lock().await.requesting {
                return Err("Leave the current room first.".into());
            }
            let (address, port) = self.start_peer_host().await?;
            let existing = own_room(&self.db, self.saved.lock().await.as_slice(), &address)?;
            if let Some(record) = existing {
                self.set_server_url(address).await;
                self.start(
                    ClientMessage::OpenSavedRoom {
                        room_code: record.room_code,
                        owner_token: record.owner_token.unwrap(),
                        username,
                    },
                    Some(port),
                )
                .await;
            } else {
                self.set_server_url(address).await;
                self.start(ClientMessage::CreateRoom { username }, Some(port)).await;
            }
            return Ok(());
        }
        let port = if run_local_relay {
            let port = local_relay::hosting_port(&self.inner.lock().await.server_url);
            self.ensure_local_relay(port).await?;
            Some(port)
        } else {
            None
        };
        self.start(ClientMessage::CreateRoom { username }, port).await;
        Ok(())
    }

    pub async fn join_room(self: &Arc<Self>, code: String, username: String) {
        let server = self.inner.lock().await.server_url.clone();
        self.start(self.guest_join(&server, code.trim().to_uppercase(), username), None).await;
    }

    pub async fn approve_join(&self, user_id: String) {
        self.send(ClientMessage::ApproveJoin { user_id }).await;
    }
    pub async fn reject_join(&self, user_id: String) {
        // Drop it from our local pending list immediately (optimistic).
        {
            let mut inner = self.inner.lock().await;
            inner.pending_joins.retain(|(id, _)| id != &user_id);
        }
        self.send(ClientMessage::RejectJoin { user_id }).await;
        self.emit_state().await;
    }
    pub async fn kick(&self, user_id: String) {
        self.send(ClientMessage::KickUser { user_id }).await;
    }
    pub async fn transfer_host(&self, user_id: String) {
        self.send(ClientMessage::TransferHost { user_id }).await;
    }
    pub async fn suggest(&self, track: Track) {
        if !shareable(&track) {
            return;
        }
        self.send(ClientMessage::Suggest { track }).await;
    }
    /// Host: approve a suggestion. Returns its track so the caller can add it to the real playback
    /// queue (the host owns the queue; the server just notifies the suggester).
    pub async fn approve_suggestion(&self, id: String) -> Option<Track> {
        let track = {
            let mut inner = self.inner.lock().await;
            let idx = inner.suggestions.iter().position(|s| s.id == id);
            idx.map(|i| inner.suggestions.remove(i).track)
        };
        self.send(ClientMessage::ApproveSuggestion { id }).await;
        self.emit_state().await;
        track
    }
    pub async fn reject_suggestion(&self, id: String) {
        {
            let mut inner = self.inner.lock().await;
            inner.suggestions.retain(|s| s.id != id);
        }
        self.send(ClientMessage::RejectSuggestion { id }).await;
        self.emit_state().await;
    }
    pub async fn request_sync(&self) {
        self.send(ClientMessage::RequestSync).await;
    }

    /// Leave the room and tear down the connection.
    pub async fn leave(&self) {
        {
            let mut inner = self.inner.lock().await;
            if let Some(outbound) = &inner.outbound {
                let _ = outbound.send(ClientMessage::LeaveRoom);
            }
            // Invalidate recovery before resetting the UI so a tick from the old room
            // cannot reopen its dialog while the app is processing a window close.
            self.gen.fetch_add(1, Ordering::SeqCst);
            self.recovery_context.lock().await.take();
            inner.reset_room();
        }
        self.clear_recovery().await;
        let _ = self.sync_tx.send(SyncCommand::Release);
        self.emit_state().await;
    }

    /// A snapshot of the client-side LT state for the UI.
    pub async fn snapshot(&self) -> serde_json::Value {
        let inner = self.inner.lock().await;
        let mut snapshot = Self::snapshot_of(&inner);
        snapshot["savedRooms"] =
            serde_json::Value::Array(self.saved.lock().await.iter().map(SavedRoom::view).collect());
        snapshot["savedRoomsError"] = self.saved_load_error.is_some().into();
        snapshot["localRelayAvailable"] = self.local_relay.available().into();
        snapshot["localRelayRunning"] = self.local_relay.running().into();
        snapshot["peerHosting"] = (self.app.config().identifier == "com.limusic.plus").into();
        snapshot
    }

    // --- connection loop --------------------------------------------------------------------

    async fn start(self: &Arc<Self>, initial: ClientMessage, hosted_relay_port: Option<u16>) {
        let recovering = self.recovery_transition.lock().await.is_some();
        if !recovering {
            self.clear_recovery().await;
        }
        // Cancel any existing connection, reset, then spawn a fresh loop.
        self.gen.fetch_add(1, Ordering::SeqCst);
        {
            let mut inner = self.inner.lock().await;
            inner.reset_room();
            inner.hosted_relay_port = hosted_relay_port;
            inner.connecting = true;
            inner.requesting = true; // waiting for the room to materialize
        }
        self.emit_state().await;
        let gen = self.gen.load(Ordering::SeqCst);
        let me = self.clone();
        tokio::spawn(async move {
            me.run(gen, initial).await;
        });
    }

    async fn connect_room(&self, url: &str) -> Result<peer::Socket, String> {
        if peer::is_peer(url) {
            self.peer.connect(url).await
        } else {
            connect_ws(url)
                .await
                .map(|socket| Box::new(socket) as peer::Socket)
                .map_err(|e| e.to_string())
        }
    }

    async fn run(self: Arc<Self>, gen: u64, initial: ClientMessage) {
        let mut next_msg = initial;
        let mut attempt: u32 = 0;
        loop {
            if self.gen.load(Ordering::SeqCst) != gen {
                return;
            }
            let mut url = self.inner.lock().await.server_url.clone();
            if url.is_empty() {
                url = DEFAULT_SERVER.to_string();
            }
            {
                let mut inner = self.inner.lock().await;
                inner.connecting = true;
                inner.status_connected = false;
            }
            self.emit_state().await;

            match self.connect_room(&url).await {
                Ok(ws) => {
                    // A connect that outlived its cancellation must not touch shared state:
                    // `leave()` and a second Host/Join click bump `gen`, but neither can interrupt
                    // an in-flight connect. Without this check a late arrival overwrites the live
                    // connection's `outbound` sender and re-sends its stale CreateRoom/JoinRoom on
                    // a second socket.
                    if self.gen.load(Ordering::SeqCst) != gen {
                        return;
                    }
                    attempt = 0;
                    let (mut sink, mut read) = ws.split();
                    let (otx, mut orx) = mpsc::unbounded_channel::<ClientMessage>();
                    {
                        let mut inner = self.inner.lock().await;
                        inner.outbound = Some(otx.clone());
                        inner.connecting = false;
                        inner.status_connected = true;
                    }
                    let _ = otx.send(next_msg.clone());
                    self.emit_state().await;

                    // Writer: drain outbound → socket.
                    let writer = tokio::spawn(async move {
                        while let Some(m) = orx.recv().await {
                            if sink.send(Message::Text(m.to_json())).await.is_err() {
                                break;
                            }
                        }
                    });
                    // App-level keepalive.
                    let ping = {
                        let tx = otx.clone();
                        tokio::spawn(async move {
                            loop {
                                tokio::time::sleep(Duration::from_secs(25)).await;
                                if tx.send(ClientMessage::Ping).is_err() {
                                    break;
                                }
                            }
                        })
                    };

                    let mut last_heard = std::time::Instant::now();
                    loop {
                        // Poll the cancel generation even when idle: `leave()` bumps it but the
                        // server doesn't close the socket on LeaveRoom, so we'd otherwise park here.
                        let next = tokio::select! {
                            biased;
                            next = read.next() => next,
                            _ = tokio::time::sleep(Duration::from_millis(500)) => {
                                if self.gen.load(Ordering::SeqCst) != gen {
                                    break;
                                }
                                // The server answers every ping, so this much silence is a dead
                                // socket (sleep/resume, a network switch) that would otherwise
                                // sit here, looking connected, until TCP gives up minutes later.
                                if last_heard.elapsed() > SILENCE_LIMIT {
                                    tracing::warn!("listen-together: server went silent, reconnecting");
                                    break;
                                }
                                continue;
                            }
                        };
                        if self.gen.load(Ordering::SeqCst) != gen {
                            break;
                        }
                        let Some(next) = next else { break }; // stream ended
                        last_heard = std::time::Instant::now();
                        match next {
                            Ok(Message::Text(t)) => {
                                match serde_json::from_str::<ServerMessage>(&t) {
                                    Ok(sm) => {
                                        if self.handle(sm).await {
                                            break; // fatal (kicked / rejected) — stop reading
                                        }
                                    }
                                    Err(e) => tracing::debug!(error = %e, "bad server message"),
                                }
                            }
                            Ok(Message::Close(_)) | Err(_) => break,
                            _ => {}
                        }
                    }
                    ping.abort();
                    // Only if we still own the session: a newer connection may have replaced us.
                    if self.gen.load(Ordering::SeqCst) == gen {
                        self.inner.lock().await.outbound = None;
                    }
                    // Let the writer send what is queued and stop once the last sender is gone.
                    // `leave()` queues LeaveRoom right before cancelling us, and aborting could
                    // lose it: the server then took the leave for a dropped socket, kept the room
                    // up and handed the host role on instead of closing it. Bounded, because a
                    // half-open socket can block a write.
                    drop(otx);
                    let mut writer = writer;
                    if tokio::time::timeout(Duration::from_secs(2), &mut writer).await.is_err() {
                        writer.abort();
                    }
                }
                Err(e) => {
                    tracing::warn!(error = %e, "listen-together connect failed");
                }
            }

            if self.gen.load(Ordering::SeqCst) != gen {
                return;
            }
            // Reconnect only if we were in a room (have a token). Fresh create/join that never
            // landed just fails.
            let token = self.inner.lock().await.session_token.clone();
            let Some(t) = token else {
                self.close_locally("Couldn't reach the Listen Together server.").await;
                return;
            };
            attempt += 1;
            if attempt > 15 && self.recovery_context.lock().await.is_none() {
                self.close_locally("Lost connection to the room.").await;
                return;
            }
            {
                let mut inner = self.inner.lock().await;
                inner.status_connected = false;
                inner.connecting = true;
            }
            self.refresh_bot_room().await;
            self.emit_state().await;
            next_msg = ClientMessage::Reconnect { session_token: t };
            tokio::time::sleep(backoff_delay(attempt)).await;
        }
    }

    /// Handle one server message. Returns `true` if the connection should close (fatal).
    async fn handle(self: &Arc<Self>, mut sm: ServerMessage) -> bool {
        sanitize(&mut sm);
        match sm {
            ServerMessage::RecoveryPeers { epoch, peers } => {
                self.update_recovery_peers(epoch, peers).await;
            }
            ServerMessage::RecoveryReady { room_code } => {
                self.publish_recovery(room_code).await;
            }
            ServerMessage::RoomAvailability { .. } => return false,
            ServerMessage::RoomSaved { room_code, owner_token } => {
                if self.inner.lock().await.room_code.as_ref() == Some(&room_code) {
                    self.inner.lock().await.unsaved_owner_token = Some(owner_token.clone());
                    if let Err(e) = self.remember_room(Some(owner_token)).await {
                        self.emit_notice(&format!("Could not save room on this device: {e}")).await;
                    } else {
                        self.emit_notice("Room saved on this device.").await;
                    }
                }
            }
            ServerMessage::Pong => return false,

            ServerMessage::RoomCreated { room_code: _, user_id, session_token, state } => {
                let reserve = self.app.config().identifier == "com.limusic.plus"
                    && state.supports_saved_rooms
                    && !state.saved;
                {
                    let mut inner = self.inner.lock().await;
                    inner.role = Role::Host;
                    inner.requesting = false;
                    inner.my_id = Some(user_id);
                    inner.session_token = Some(session_token);
                    inner.absorb(&state);
                }
                // Seed the room with whatever we're currently playing.
                let recovering = self.recovery_seed().await;
                if recovering.is_none() {
                    let _ = self.sync_tx.send(SyncCommand::HostSeed);
                }
                if reserve {
                    self.send(ClientMessage::SaveRoom).await;
                }
                if let Some((state, guest_tokens)) = recovering {
                    self.send(ClientMessage::SeedRecovery { state, guest_tokens }).await;
                }
                self.register_recovery(state.supports_recovery).await;
                self.record_recent_use().await;
            }

            ServerMessage::JoinRequest { user_id, username } => {
                let is_new = {
                    let mut inner = self.inner.lock().await;
                    let new = !inner.pending_joins.iter().any(|(id, _)| id == &user_id);
                    if new {
                        inner.pending_joins.push((user_id, username.clone()));
                    }
                    new
                };
                if is_new {
                    self.emit_notice(&format!("{username} wants to join")).await;
                }
            }

            ServerMessage::JoinApproved { user_id, session_token, guest_token, state } => {
                let supports_recovery = state.supports_recovery;
                if let Some(token) = guest_token {
                    let server = self.inner.lock().await.server_url.clone();
                    if let Err(error) =
                        remember_guest_pass(&self.db, &server, &state.room_code, token)
                    {
                        self.emit_notice(&format!(
                            "Joined, but could not remember approval: {error}"
                        ))
                        .await;
                    }
                }
                {
                    let mut inner = self.inner.lock().await;
                    inner.role = if state.host_id == user_id { Role::Host } else { Role::Guest };
                    inner.requesting = false;
                    inner.my_id = Some(user_id);
                    inner.session_token = Some(session_token);
                    inner.absorb(&state);
                }
                let _ = self.sync_tx.send(SyncCommand::ApplyState(state));
                let recovered = self.recovery_transition.lock().await.take().is_some();
                self.inner.lock().await.recovery_busy = false;
                if recovered {
                    if let Err(error) = self.remember_room(None).await {
                        self.emit_notice(&format!("Joined the replacement room, but could not save its shortcut: {error}")).await;
                    }
                }
                self.register_recovery(supports_recovery).await;
                self.record_recent_use().await;
            }

            ServerMessage::JoinRejected { reason } => {
                self.emit_notice(&reason).await;
                self.close_locally("").await;
                return true;
            }

            ServerMessage::Reconnected { user_id, is_host, state } => {
                let supports_recovery = state.supports_recovery;
                {
                    let mut inner = self.inner.lock().await;
                    inner.role = if is_host { Role::Host } else { Role::Guest };
                    inner.requesting = false;
                    inner.my_id = Some(user_id);
                    inner.absorb(&state);
                }
                if !is_host {
                    let _ = self.sync_tx.send(SyncCommand::ApplyState(state));
                }
                self.record_recent_use().await;
                self.register_recovery(supports_recovery).await;
            }

            ServerMessage::UserJoined { user } => {
                let mut inner = self.inner.lock().await;
                inner.pending_joins.retain(|(id, _)| id != &user.user_id);
                if !inner.users.iter().any(|u| u.user_id == user.user_id) {
                    inner.users.push(user);
                }
            }
            ServerMessage::UserLeft { user_id } => {
                let mut inner = self.inner.lock().await;
                inner.users.retain(|u| u.user_id != user_id);
                // Also covers a joiner who vanished while still pending: without this the host's
                // request list keeps a row that approving can never clear.
                inner.pending_joins.retain(|(id, _)| id != &user_id);
                drop(inner);
                self.refresh_bot_room().await;
            }
            ServerMessage::UserDisconnected { user_id } => {
                let mut inner = self.inner.lock().await;
                if let Some(u) = inner.users.iter_mut().find(|u| u.user_id == user_id) {
                    u.is_connected = false;
                }
                drop(inner);
                self.refresh_bot_room().await;
            }
            ServerMessage::UserReconnected { user_id } => {
                let mut inner = self.inner.lock().await;
                if let Some(u) = inner.users.iter_mut().find(|u| u.user_id == user_id) {
                    u.is_connected = true;
                }
                drop(inner);
                self.refresh_bot_room().await;
            }

            ServerMessage::HostChanged { host_id } => {
                let became_host = {
                    let mut inner = self.inner.lock().await;
                    for u in &mut inner.users {
                        u.is_host = u.user_id == host_id;
                    }
                    let me = inner.my_id.clone();
                    let became = me.as_deref() == Some(host_id.as_str());
                    inner.role = if became { Role::Host } else { Role::Guest };
                    if !became {
                        // The server re-sends these to the new host; ours can't be acted on now.
                        inner.pending_joins.clear();
                        inner.suggestions.clear();
                    }
                    became
                };
                self.refresh_bot_room().await;
                if became_host {
                    // We now control playback locally — stop applying remote sync.
                    let _ = self.sync_tx.send(SyncCommand::Release);
                    self.emit_notice("You're the host now.").await;
                }
            }

            ServerMessage::SyncPlayback(p) => {
                // Only guests apply; the host originated it (server excludes us anyway).
                if self.inner.lock().await.role == Role::Guest {
                    self.apply_playback(p).await;
                }
            }

            ServerMessage::SyncState { state } => {
                {
                    let mut inner = self.inner.lock().await;
                    inner.absorb(&state);
                }
                if self.inner.lock().await.role == Role::Guest {
                    let _ = self.sync_tx.send(SyncCommand::ApplyState(state));
                }
            }

            ServerMessage::SuggestionReceived { suggestion } => {
                // Spotify-Jam-style: guest adds go straight into the queue — no host approval
                // step. Stamp who added it, tell the server it's handled (notifies the guest),
                // and hand the track to the bridge to insert at the guest boundary.
                let mut track = suggestion.track;
                if !shareable(&track) {
                    self.send(ClientMessage::RejectSuggestion { id: suggestion.id }).await;
                    return false;
                }
                track.queued_by = Some(suggestion.from_username.clone());
                let title = track.title.clone();
                self.send(ClientMessage::ApproveSuggestion { id: suggestion.id }).await;
                let _ = self.sync_tx.send(SyncCommand::GuestAdd { track });
                self.emit_notice(&format!("{} added {title}", suggestion.from_username)).await;
            }
            ServerMessage::SuggestionApproved { .. } => {
                self.emit_notice("Added to the session queue.").await;
            }
            ServerMessage::SuggestionRejected { reason, .. } => {
                self.emit_notice(&reason).await;
            }

            ServerMessage::Kicked { reason } => {
                self.emit_notice(&reason).await;
                self.close_locally("").await;
                return true;
            }
            ServerMessage::Error { code, message } => {
                self.emit_notice(&message).await;
                // Any error that arrives before we're actually in a room means create/join failed:
                // close instead of leaving the UI spinning on "waiting for the host". Once in a
                // room, only a dead session is fatal (`not_host` etc. are just refusals).
                let in_room = self.inner.lock().await.role != Role::None;
                if code == "session_expired" && self.recovery_context.lock().await.is_some() {
                    self.gen.fetch_add(1, Ordering::SeqCst);
                    let mut inner = self.inner.lock().await;
                    inner.status_connected = false;
                    inner.connecting = false;
                    inner.outbound = None;
                    drop(inner);
                    self.emit_state().await;
                    return true;
                }
                if code == "recovery_failed" {
                    self.send(ClientMessage::LeaveRoom).await;
                    self.close_locally("").await;
                    return true;
                }
                if !in_room || code == "session_expired" {
                    self.close_locally("").await;
                    return true;
                }
            }
        }
        self.emit_state().await;
        false
    }

    /// Translate a broadcast playback action into a bridge command (guest side).
    async fn apply_playback(&self, p: Playback) {
        // Keep our mirror of current track/queue current so the UI reflects it.
        {
            let mut inner = self.inner.lock().await;
            match p.kind {
                PlaybackKind::ChangeTrack => {
                    inner.current_track = p.track.clone();
                    if let Some(q) = &p.queue {
                        inner.queue = q.clone();
                    }
                }
                PlaybackKind::SyncQueue => {
                    if let Some(q) = &p.queue {
                        inner.queue = q.clone();
                    }
                }
                _ => {}
            }
        }
        let cmd = match p.kind {
            PlaybackKind::Play => Some(SyncCommand::Play { position_ms: p.position_ms }),
            PlaybackKind::Pause => Some(SyncCommand::Pause { position_ms: p.position_ms }),
            PlaybackKind::Seek => Some(SyncCommand::Seek { position_ms: p.position_ms }),
            PlaybackKind::ChangeTrack => Some(SyncCommand::ChangeTrack {
                track: p.track,
                position_ms: p.position_ms,
                playing: p.playing,
                queue: p.queue.unwrap_or_default(),
            }),
            PlaybackKind::SyncQueue => p.queue.map(|queue| SyncCommand::SyncQueue { queue }),
            PlaybackKind::SetVolume => None,
        };
        if let Some(cmd) = cmd {
            let _ = self.sync_tx.send(cmd);
        }
    }

    /// Tear down room membership locally (no server round-trip). `notice` optional.
    async fn close_locally(&self, notice: &str) {
        if !notice.is_empty() {
            self.emit_notice(notice).await;
        }
        self.gen.fetch_add(1, Ordering::SeqCst);
        if !self.restore_recovery_context().await {
            self.clear_recovery().await;
        }
        self.inner.lock().await.reset_room();
        self.inner.lock().await.recovery_busy = false;
        let _ = self.sync_tx.send(SyncCommand::Release);
        self.emit_state().await;
    }

    // --- UI events --------------------------------------------------------------------------

    fn snapshot_of(inner: &Inner) -> serde_json::Value {
        let role = match inner.role {
            Role::None => "none",
            Role::Host => "host",
            Role::Guest => "guest",
        };
        serde_json::json!({
            "recoveryDisconnected": inner.recovery_disconnected,
            "recoveryInviter": inner.recovery_inviter,
            "recoveryBusy": inner.recovery_busy,
            "status": inner.status().as_str(),
            "role": role,
            "requesting": inner.requesting,
            "roomCode": inner.room_code,
            "savedRoom": inner.saved_room,
            "myId": inner.my_id,
            "serverUrl": inner.server_url,
            // Sent so the settings panel can show what "the default" actually is once the user
            // opens it. `serverUrl` stays empty until they pick their own, which is what keeps the
            // address off every other screen.
            "defaultServerUrl": DEFAULT_SERVER,
            "users": inner.users,
            "currentTrack": inner.current_track,
            "queue": inner.queue,
            "pendingJoins": inner.pending_joins.iter()
                .map(|(id, name)| serde_json::json!({ "userId": id, "username": name }))
                .collect::<Vec<_>>(),
            "suggestions": inner.suggestions,
        })
    }

    async fn emit_state(&self) {
        let snap = self.snapshot().await;
        let _ = self.app.emit("lt-state", snap);
    }

    async fn emit_notice(&self, msg: &str) {
        let _ = self.app.emit("lt-notice", msg);
    }
}

/// Whether a track may cross the wire: only a YouTube video id. Ids come from the relay and from
/// every other member of the room, and the player resolves a `LOCAL:<path>` id straight to a file,
/// so a peer could point us at any path. On Windows a UNC path (`\\host\share`) makes the OS
/// connect to that host and offer the user's NTLM credentials. Outbound, the same rule keeps our
/// local paths, and the username in them, off the relay; guests couldn't play the file anyway.
fn shareable(t: &Track) -> bool {
    t.id.len() == 11 && t.id.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
}

fn keep_shareable(p: &mut Playback) {
    p.track = p.track.take().filter(shareable);
    if let Some(q) = &mut p.queue {
        q.retain(shareable);
    }
}

/// Drop every track that isn't [`shareable`] before anything reads the message. Suggestions are
/// checked where they're handled, so a bad one can be rejected rather than left pending. No
/// wildcard arm: a new message that carries tracks has to be decided on here.
fn sanitize(sm: &mut ServerMessage) {
    match sm {
        ServerMessage::RecoveryPeers { .. } | ServerMessage::RecoveryReady { .. } => {},
        ServerMessage::RoomAvailability { .. } => {}
        ServerMessage::RoomCreated { state, .. }
        | ServerMessage::JoinApproved { state, .. }
        | ServerMessage::Reconnected { state, .. }
        | ServerMessage::SyncState { state } => {
            state.current_track = state.current_track.take().filter(shareable);
            state.queue.retain(shareable);
        }
        ServerMessage::SyncPlayback(p) => keep_shareable(p),
        ServerMessage::SuggestionReceived { .. }
        | ServerMessage::RoomSaved { .. }
        | ServerMessage::Pong
        | ServerMessage::JoinRequest { .. }
        | ServerMessage::JoinRejected { .. }
        | ServerMessage::UserJoined { .. }
        | ServerMessage::UserLeft { .. }
        | ServerMessage::UserDisconnected { .. }
        | ServerMessage::UserReconnected { .. }
        | ServerMessage::HostChanged { .. }
        | ServerMessage::SuggestionApproved { .. }
        | ServerMessage::SuggestionRejected { .. }
        | ServerMessage::Kicked { .. }
        | ServerMessage::Error { .. } => {}
    }
}

/// How long one address gets to complete a TCP handshake before we move to the next.
const CONNECT_TIMEOUT: Duration = Duration::from_secs(4);

/// No frame from the server for this long means the connection is dead. Pings go out every 25s
/// and each gets a Pong, so this is two missed answers plus slack.
const SILENCE_LIMIT: Duration = Duration::from_secs(60);

/// Open the WebSocket, giving every resolved address its own short deadline.
///
/// `tokio_tungstenite::connect_async` hands `host:port` straight to `TcpStream::connect`, which
/// walks the resolved addresses one at a time and waits out the OS SYN timeout (~127s on Linux) on
/// each dead one. Tailscale Funnel publishes three AAAA and three A records for a node, and the
/// AAAA ingress is a black hole from some networks; glibc sorts IPv6 first, so a host with working
/// IPv6 burned six minutes on dead addresses before ever trying the IPv4 that works. Same reason a
/// join request never reached the host: the socket carrying it was still in a SYN retry.
async fn connect_ws(url: &str) -> Result<WebSocketStream<MaybeTlsStream<TcpStream>>, WsError> {
    let req = url.into_client_request()?;
    let uri = req.uri().clone();
    let host = uri
        .host()
        .ok_or(WsError::Url(tokio_tungstenite::tungstenite::error::UrlError::NoHostName))?;
    let port = uri.port_u16().unwrap_or(if uri.scheme_str() == Some("ws") { 80 } else { 443 });

    let addrs: Vec<SocketAddr> = tokio::net::lookup_host((host, port)).await?.collect();
    let socket = connect_first(&addrs).await.map_err(WsError::Io)?;

    let (ws, _) = tokio_tungstenite::client_async_tls_with_config(req, socket, None, None).await?;
    Ok(ws)
}

/// First address that completes a handshake within [`CONNECT_TIMEOUT`], in order.
async fn connect_first(addrs: &[SocketAddr]) -> std::io::Result<TcpStream> {
    let mut last: Option<std::io::Error> = None;
    for &addr in addrs {
        match tokio::time::timeout(CONNECT_TIMEOUT, TcpStream::connect(addr)).await {
            Ok(Ok(s)) => return Ok(s),
            Ok(Err(e)) => {
                tracing::debug!(%addr, error = %e, "listen-together: address refused");
                last = Some(e);
            }
            Err(_) => {
                tracing::debug!(%addr, "listen-together: address timed out");
                last = Some(std::io::Error::new(
                    std::io::ErrorKind::TimedOut,
                    format!("{addr} did not answer in {CONNECT_TIMEOUT:?}"),
                ));
            }
        }
    }
    Err(last.unwrap_or_else(|| {
        std::io::Error::new(std::io::ErrorKind::AddrNotAvailable, "host resolved to nothing")
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn own_room_stays_pinned_across_restart_shortcut_changes_and_recovery() {
        let path = std::env::temp_dir().join(format!("limusic-own-{}.sqlite", rand::random::<u64>()));
        let original: SavedRoom = serde_json::from_str(r#"{"server_url":"limusic-peer://me","room_code":"ORIGINAL","username":"owner","owner_token":"private"}"#).unwrap();
        let recovery = SavedRoom {
            recovery_room: true,
            room_code: "RECOVERY".into(),
            ..original.clone()
        };
        {
            let db = crate::db::Db::open(&path).unwrap();
            assert!(own_room(&db, &[recovery.clone()], &original.server_url).unwrap().is_none());
            let rooms = vec![recovery.clone(), original.clone()];
            let pinned = own_room(&db, &rooms, &original.server_url).unwrap().unwrap();
            assert_eq!(pinned.room_code, "ORIGINAL");
            let mut rooms = vec![recovery.clone()];
            upsert_saved_room(&mut rooms, SavedRoom { recovery_room: false, ..recovery.clone() });
            assert!(rooms[0].recovery_room, "saving again must preserve recovery provenance");
        }
        {
            let db = crate::db::Db::open(&path).unwrap();
            let pinned = own_room(&db, &[recovery], &original.server_url).unwrap().unwrap();
            assert_eq!(pinned.room_code, "ORIGINAL");
            assert_eq!(pinned.owner_token, original.owner_token);
            assert!(own_room(&db, &[], "limusic-peer://someone-else").is_err());
            db.set_setting_checked("lt_own_room", "broken").unwrap();
            assert!(own_room(&db, &[original.clone()], &original.server_url).is_err());
            assert_eq!(db.get_setting("lt_own_room").as_deref(), Some("broken"));
        }
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn guest_permission_is_private_scoped_and_survives_app_restart() {
        let path =
            std::env::temp_dir().join(format!("limusic-guest-{}.sqlite", rand::random::<u64>()));
        {
            let db = crate::db::Db::open(&path).unwrap();
            remember_guest_pass(&db, "", "ROOM", "secret".into()).unwrap();
        }
        {
            let db = crate::db::Db::open(&path).unwrap();
            assert_eq!(guest_pass(&db, DEFAULT_SERVER, "ROOM").as_deref(), Some("secret"));
            assert!(guest_pass(&db, "wss://another-relay/ws", "ROOM").is_none());
            assert!(guest_pass(&db, DEFAULT_SERVER, "OTHER").is_none());
            remember_guest_pass(&db, "", "ROOM", "replacement".into()).unwrap();
            assert_eq!(guest_pass(&db, "", "ROOM").as_deref(), Some("replacement"));
            assert!(
                load_saved_rooms(&db).unwrap().is_empty(),
                "approval does not force a saved shortcut"
            );
            db.set_setting_checked("lt_guest_passes", "broken").unwrap();
            assert!(remember_guest_pass(&db, "", "ROOM", "lost".into()).is_err());
            assert_eq!(db.get_setting("lt_guest_passes").as_deref(), Some("broken"));
        }
        std::fs::remove_file(path).unwrap();
    }

    #[test]
    fn guests_never_start_a_relay_and_saved_hosts_keep_their_local_choice() {
        let old_json = r#"{"server_url":"ws://127.0.0.1:8080/ws","room_code":"ABCDEFGH","username":"owner","owner_token":"secret"}"#;
        let old: SavedRoom = serde_json::from_str(old_json).unwrap();
        assert_eq!(old.relay_to_start(), Some(8080), "old locally-owned rooms still reconnect");
        let guest = SavedRoom { owner_token: None, ..old.clone() };
        assert_eq!(
            guest.relay_to_start(),
            None,
            "a guest's localhost bookmark must not start a relay"
        );
        let public = SavedRoom {
            server_url: "wss://my-tunnel.example/ws".into(),
            local_relay_port: Some(8080),
            ..old.clone()
        };
        assert_eq!(
            public.relay_to_start(),
            Some(8080),
            "a public tunnel still needs its owner's local relay"
        );
        let remote = SavedRoom { local_relay_port: None, ..public.clone() };
        assert_eq!(
            remote.relay_to_start(),
            None,
            "rooms on someone else's server need no local relay"
        );
        let mut saved = vec![public];
        upsert_saved_room(
            &mut saved,
            SavedRoom { owner_token: None, local_relay_port: None, ..remote },
        );
        assert_eq!(
            saved[0].relay_to_start(),
            Some(8080),
            "saving a guest view preserves existing owner metadata"
        );
    }

    #[test]
    fn saved_rooms_preserve_credentials_and_relay_identity_across_app_storage() {
        let path =
            std::env::temp_dir().join(format!("limusic-saved-{}.sqlite", rand::random::<u64>()));
        {
            let db = crate::db::Db::open(&path).unwrap();
            let mut rooms = Vec::new();
            let record = SavedRoom {
                recovery_room: false,
                last_used_ms: 123,
                name: "Friday music".into(),
                favorite: true,
                host_name: "Friend".into(),
                local_relay_port: Some(8080),
                server_url: "wss://relay-a/ws".into(),
                room_code: "ABCDEFGH".into(),
                username: "owner".into(),
                owner_token: Some("private-owner-token".into()),
            };
            upsert_saved_room(&mut rooms, record.clone());
            upsert_saved_room(
                &mut rooms,
                SavedRoom {
                    last_used_ms: 0,
                    username: "new name".into(),
                    owner_token: None,
                    ..record.clone()
                },
            );
            upsert_saved_room(
                &mut rooms,
                SavedRoom { server_url: "wss://relay-b/ws".into(), owner_token: None, ..record },
            );
            upsert_saved_room(
                &mut rooms,
                SavedRoom {
                    recovery_room: false,
                    last_used_ms: 0,
                    name: String::new(),
                    favorite: false,
                    server_url: "wss://relay-a/ws".into(),
                    room_code: "ZZZZZZZZ".into(),
                    username: "guest".into(),
                    host_name: "Another friend".into(),
                    owner_token: None,
                    local_relay_port: None,
                },
            );
            db.set_setting_checked("lt_saved_rooms", &serde_json::to_string(&rooms).unwrap())
                .unwrap();
        }
        {
            let db = crate::db::Db::open(&path).unwrap();
            let rooms = load_saved_rooms(&db).unwrap();
            assert_eq!(
                rooms.len(),
                3,
                "friends on the same relay and the same code on another relay stay separate"
            );
            assert_eq!(rooms[0].username, "new name");
            assert_eq!(rooms[0].last_used_ms, 123);
            assert_eq!(rooms[0].name, "Friday music");
            assert!(rooms[0].favorite);
            assert_eq!(rooms[0].owner_token.as_deref(), Some("private-owner-token"));
            assert!(rooms[1].owner_token.is_none());
            let view = rooms[0].view();
            assert_eq!(view["owner"], true);
            assert!(!view.to_string().contains("private-owner-token"));
            assert!(view.get("owner_token").is_none());
            db.set_setting_checked("lt_saved_rooms", "broken JSON").unwrap();
            assert!(
                load_saved_rooms(&db).is_err(),
                "unreadable credentials must not be treated as an empty list"
            );
        }
        std::fs::remove_file(path).unwrap();
    }

    fn track(id: &str) -> Track {
        Track {
            id: id.into(),
            title: String::new(),
            artist: String::new(),
            thumbnail: None,
            duration_ms: 0,
            queued_by: None,
        }
    }

    #[test]
    fn only_video_ids_cross_the_wire() {
        let mut p = Playback::new(PlaybackKind::ChangeTrack);
        p.track = Some(track(r"LOCAL:\\evil\share\a.mp3"));
        p.queue =
            Some(vec![track("dQw4w9WgXcQ"), track("LOCAL:/home/me/a.mp3"), track("dQw4w9WgXc")]);
        let mut sm = ServerMessage::SyncPlayback(p.clone());
        sanitize(&mut sm);
        let ServerMessage::SyncPlayback(got) = sm else { unreachable!() };
        assert!(got.track.is_none());
        assert_eq!(
            got.queue.unwrap().iter().map(|t| t.id.as_str()).collect::<Vec<_>>(),
            ["dQw4w9WgXcQ"]
        );

        // Outbound runs the same filter.
        keep_shareable(&mut p);
        assert!(p.track.is_none());
        assert_eq!(p.queue.unwrap().len(), 1);

        assert!(!shareable(&track("../../etc/pas")));
        assert!(shareable(&track("a-B_c123456")));
    }

    /// The bug: a dead address ahead of a live one used to cost a full OS SYN timeout (~127s on
    /// Linux, six minutes across Funnel's three AAAA records). It must now fall through fast.
    #[tokio::test]
    async fn dead_address_does_not_block_the_live_one() {
        let live = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let live_addr = live.local_addr().unwrap();
        // 198.51.100.0/24 is TEST-NET-2: routed nowhere, so this either hangs (→ our timeout) or
        // errors out, which is exactly the fall-through we want to exercise.
        let dead: SocketAddr = "198.51.100.1:443".parse().unwrap();

        let t = std::time::Instant::now();
        let s = connect_first(&[dead, live_addr]).await.expect("should reach the live address");
        assert_eq!(s.peer_addr().unwrap(), live_addr);
        assert!(
            t.elapsed() < CONNECT_TIMEOUT * 2,
            "fell through in {:?}, expected under {:?}",
            t.elapsed(),
            CONNECT_TIMEOUT * 2
        );
    }

    #[tokio::test]
    async fn no_addresses_is_an_error_not_a_hang() {
        assert!(connect_first(&[]).await.is_err());
    }
}

/// Exponential backoff, capped at 32s (context/19 Part B §1: the client's real ceiling).
fn backoff_delay(attempt: u32) -> Duration {
    let shift = attempt.clamp(1, 6) - 1; // 0..=5 → 1,2,4,8,16,32
    Duration::from_secs(1u64 << shift)
}
