//! Limusic "Listen Together" sync server. A self-hosted WebSocket hub that relays small playback
//! control messages between a host and guests — audio never touches it; every client streams
//! direct from YouTube (context/19). Host-authoritative, server-extrapolated position.
//!
//! Plain `ws://` on `0.0.0.0:PORT` (default 8080). TLS is terminated by whatever you front it with
//! (Tailscale Funnel / Cloudflare Tunnel), so there's none here.
//!
//! ponytail: one global `Mutex<HashMap<room, Room>>` and unbounded per-client channels — right for
//! a personal server (tens of users). If it ever needs to scale, shard by room / add backpressure.

use std::collections::{HashMap, HashSet};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use futures_util::{SinkExt, StreamExt};
use listen_protocol::{
    ClientMessage, PlaybackKind, RoomState, ServerMessage, Suggestion, Track, User,
};
use rand::Rng;
use tokio::net::{TcpListener, TcpStream};
use tokio::sync::{mpsc, Mutex};
use tokio_tungstenite::tungstenite::Message;

/// How long a dropped participant's slot (and session token) survives for reconnection.
const RECONNECT_GRACE: Duration = Duration::from_secs(120);
/// How long a dropped host keeps the role before it moves to someone connected. Long enough to
/// ride out a network blip (the client reconnects within seconds), short enough that a room isn't
/// left without anyone driving it.
const HOST_GRACE: Duration = Duration::from_secs(20);
/// How often expired slots and hosts are swept. Bounds how late a handoff can be.
const SWEEP_EVERY: Duration = Duration::from_secs(5);
const MAX_USERS_PER_ROOM: usize = 50;
/// Rooms the server holds at once. The default server is public, and every room is memory.
const MAX_ROOMS: usize = 1000;
const MAX_QUEUE: usize = listen_protocol::MAX_SHARED_QUEUE;
const MAX_MESSAGE: usize = listen_protocol::MAX_ROOM_MESSAGE;
/// A connection that sends nothing for this long is dropped. Clients ping every 25s.
const IDLE_LIMIT: Duration = Duration::from_secs(90);
/// Room code alphabet — no `I`/`O` to avoid confusion (context/19 §2.2).
const CODE_ALPHABET: &[u8] = b"1234567890QWERTYUPASDFGHJKLZXCVBNM";

type Tx = mpsc::UnboundedSender<ServerMessage>;

struct Peer {
    guest_token: Option<String>,
    username: String,
    tx: Tx,
    connected: bool,
    session_token: String,
    disconnected_at: Option<Instant>,
}

/// A joiner awaiting host approval (not yet a room member).
struct Pending {
    owner: bool,
    username: String,
    tx: Tx,
}

struct Room {
    recovery_epoch: String,
    recovery_endpoints: HashMap<String, String>,
    approved_guests: HashSet<String>,
    saved: bool,
    owner_id: Option<String>,
    host_id: String,
    peers: HashMap<String, Peer>,
    pending: HashMap<String, Pending>,
    suggestions: HashMap<String, Suggestion>,
    current_track: Option<Track>,
    is_playing: bool,
    position_ms: i64,
    last_update_ms: i64,
    volume: f64,
    queue: Vec<Track>,
}

impl Room {
    fn admit(
        &mut self,
        code: &str,
        user_id: String,
        username: String,
        tx: Tx,
        guest_token: String,
    ) {
        let returning = self.peers.contains_key(&user_id);
        let session_token = gen_token();
        self.peers.insert(
            user_id.clone(),
            Peer {
                guest_token: Some(guest_token.clone()),
                username: username.clone(),
                tx: tx.clone(),
                connected: true,
                session_token: session_token.clone(),
                disconnected_at: None,
            },
        );
        let now = now_ms();
        let mut state = self.wire_state(code);
        state.position_ms = state.live_position_ms(now);
        state.last_update_ms = now;
        let _ = tx.send(ServerMessage::JoinApproved {
            user_id: user_id.clone(),
            session_token,
            guest_token: Some(guest_token),
            state,
        });
        if returning {
            self.broadcast(
                &ServerMessage::UserReconnected { user_id: user_id.clone() },
                Some(&user_id),
            );
            if self.is_host(&user_id) {
                self.brief_host();
            }
            return;
        }
        self.broadcast(
            &ServerMessage::UserJoined {
                user: User {
                    user_id: user_id.clone(),
                    username,
                    is_host: false,
                    is_connected: true,
                },
            },
            Some(&user_id),
        );
    }
    fn wire_users(&self) -> Vec<User> {
        self.peers
            .iter()
            .map(|(id, p)| User {
                user_id: id.clone(),
                username: p.username.clone(),
                is_host: *id == self.host_id,
                is_connected: p.connected,
            })
            .collect()
    }

    fn wire_state(&self, code: &str) -> RoomState {
        RoomState {
            supports_recovery: true,
            supports_saved_rooms: true,
            saved: self.saved,
            room_code: code.to_string(),
            host_id: self.host_id.clone(),
            users: self.wire_users(),
            current_track: self.current_track.clone(),
            is_playing: self.is_playing,
            position_ms: self.position_ms,
            last_update_ms: self.last_update_ms,
            volume: self.volume,
            queue: self.queue.clone(),
        }
    }

    /// Send to every connected peer except `except` (dropped receivers are ignored — the cleanup
    /// path removes them).
    fn broadcast(&self, msg: &ServerMessage, except: Option<&str>) {
        for (id, p) in &self.peers {
            if p.connected && Some(id.as_str()) != except {
                let _ = p.tx.send(msg.clone());
            }
        }
    }

    fn send_to(&self, uid: &str, msg: ServerMessage) {
        if let Some(p) = self.peers.get(uid) {
            let _ = p.tx.send(msg);
        }
    }

    fn is_host(&self, uid: &str) -> bool {
        self.host_id == uid
    }

    /// Pick any connected non-host peer to promote (for host handoff). None if nobody's around.
    fn any_connected_other(&self, besides: &str) -> Option<String> {
        self.peers
            .iter()
            .find(|(id, p)| p.connected && id.as_str() != besides)
            .map(|(id, _)| id.clone())
    }

    /// Hand the host role to `id` and tell everyone.
    fn set_host(&mut self, id: String) {
        self.host_id = id.clone();
        self.broadcast(&ServerMessage::HostChanged { host_id: id }, None);
        self.brief_host();
        self.broadcast_recovery();
    }

    fn broadcast_recovery(&self) {
        if self.recovery_endpoints.is_empty() { return; }
        let peers = self.recovery_endpoints.iter().filter_map(|(id, endpoint)| {
            let p = self.peers.get(id)?;
            Some(listen_protocol::RecoveryPeer {
                user_id: id.clone(), username: p.username.clone(),
                endpoint_id: endpoint.clone(), is_host: self.host_id == *id,
            })
        }).collect();
        self.broadcast(&ServerMessage::RecoveryPeers { epoch: self.recovery_epoch.clone(), peers }, None);
    }

    /// (Re)send the host everything only a host can act on. Join requests and guest adds are sent
    /// to whoever was host when they arrived, so a host change, or a host whose socket was down,
    /// would otherwise leave a joiner waiting forever and drop the add without a word. The client
    /// ignores a join request it already lists, and a re-sent add lands on its earlier copy.
    fn brief_host(&self) {
        for (id, p) in &self.pending {
            self.send_to(
                &self.host_id,
                ServerMessage::JoinRequest { user_id: id.clone(), username: p.username.clone() },
            );
        }
        for s in self.suggestions.values() {
            self.send_to(
                &self.host_id,
                ServerMessage::SuggestionReceived { suggestion: s.clone() },
            );
        }
    }

    /// A joiner gave up (cancelled, or their socket dropped) before the host answered. Without
    /// this the host keeps a request row that approving can never clear.
    fn drop_pending(&mut self, id: &str) -> bool {
        if self.pending.remove(id).is_none() {
            return false;
        }
        self.send_to(&self.host_id, ServerMessage::UserLeft { user_id: id.to_string() });
        true
    }

    /// Is there a host who can actually answer a join request right now?
    fn host_is_reachable(&self) -> bool {
        self.peers.get(&self.host_id).map(|p| p.connected).unwrap_or(false)
    }
}

#[derive(Default)]
struct Server {
    rooms: Mutex<HashMap<String, Room>>,
    // SQLite commits the reservation before acknowledging it. Never falls back to an empty DB
    // on an open/read error: losing reservations would allow existing codes to be reassigned.
    saved: std::sync::Mutex<Option<rusqlite::Connection>>,
}

fn now_ms() -> i64 {
    SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_millis() as i64).unwrap_or(0)
}

fn gen_code() -> String {
    let mut rng = rand::thread_rng();
    (0..8).map(|_| CODE_ALPHABET[rng.gen_range(0..CODE_ALPHABET.len())] as char).collect()
}

fn gen_user_id() -> String {
    let nanos = SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_nanos()).unwrap_or(0);
    format!("user_{}_{}", nanos, rand::thread_rng().gen_range(0..10000))
}

fn gen_token() -> String {
    let bytes: [u8; 16] = rand::thread_rng().gen();
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn err(code: &str, message: &str) -> ServerMessage {
    ServerMessage::Error { code: code.into(), message: message.into() }
}

impl Server {
    fn with_store(path: &std::path::Path) -> rusqlite::Result<Self> {
        let db = rusqlite::Connection::open(path)?;
        db.execute_batch(
            "CREATE TABLE IF NOT EXISTS saved_rooms (
            code TEXT PRIMARY KEY, token TEXT NOT NULL);
             CREATE TABLE IF NOT EXISTS approved_guests (
             room_code TEXT NOT NULL, token TEXT NOT NULL,
             PRIMARY KEY(room_code, token))",
        )?;
        Ok(Self { rooms: Mutex::new(HashMap::new()), saved: std::sync::Mutex::new(Some(db)) })
    }

    fn reservation(&self, code: &str) -> rusqlite::Result<Option<String>> {
        use rusqlite::OptionalExtension;
        match self.saved.lock().unwrap().as_ref() {
            Some(db) => db
                .query_row("SELECT token FROM saved_rooms WHERE code = ?1", [code], |r| r.get(0))
                .optional(),
            None => Ok(None),
        }
    }

    fn approved_guests(&self, code: &str) -> rusqlite::Result<HashSet<String>> {
        let store = self.saved.lock().unwrap();
        let Some(db) = store.as_ref() else { return Ok(HashSet::new()) };
        let mut query = db.prepare("SELECT token FROM approved_guests WHERE room_code = ?1")?;
        let result = query.query_map([code], |r| r.get(0))?.collect();
        result
    }

    fn persist_guest(&self, code: &str, token: &str, remove: bool) -> rusqlite::Result<()> {
        let store = self.saved.lock().unwrap();
        if let Some(db) = store.as_ref() {
            if remove {
                db.execute(
                    "DELETE FROM approved_guests WHERE room_code = ?1 AND token = ?2",
                    [code, token],
                )?;
            } else {
                db.execute(
                    "INSERT OR IGNORE INTO approved_guests(room_code, token) VALUES(?1, ?2)",
                    [code, token],
                )?;
            }
        }
        Ok(())
    }

    /// Handle one decoded message. `uid`/`room_code` track this connection's identity and are
    /// updated in place as it creates/joins/reconnects.
    async fn dispatch(
        &self,
        mut cm: ClientMessage,
        tx: &Tx,
        uid: &mut Option<String>,
        room_code: &mut Option<String>,
    ) {
        // One identity per connection. The client opens a fresh socket for every create, join and
        // reconnect; a second one here would orphan the first: its slot stays `connected` and
        // nothing ever closes that room.
        if uid.is_some()
            && matches!(
                cm,
                ClientMessage::CreateRoom { .. }
                    | ClientMessage::JoinRoom { .. }
                    | ClientMessage::JoinSavedRoom { .. }
                    | ClientMessage::Reconnect { .. }
                    | ClientMessage::OpenSavedRoom { .. }
            )
        {
            let _ = tx.send(err("already_in_room", "Leave this room first."));
            return;
        }
        let mut reopen_code = None;
        let mut owner_join = false;
        let mut returning_guest = None;
        if let ClientMessage::JoinSavedRoom { room_code, username, guest_token } = cm {
            returning_guest = Some(guest_token);
            cm = ClientMessage::JoinRoom { room_code, username };
        }
        if let ClientMessage::OpenSavedRoom { room_code: code, owner_token, username } = cm {
            match self.reservation(&code) {
                Ok(Some(token)) if token == owner_token => {}
                _ => {
                    let _ = tx.send(err(
                        "saved_room_unavailable",
                        "Saved room unavailable or owner credential invalid.",
                    ));
                    return;
                }
            }
            let rooms = self.rooms.lock().await;
            cm = if let Some(room) = rooms.get(&code) {
                if let Some(peer) = room.owner_id.as_ref().and_then(|id| room.peers.get(id)) {
                    ClientMessage::Reconnect { session_token: peer.session_token.clone() }
                } else {
                    // Leaving or being kicked removes membership. Ownership never bypasses the
                    // current host's approval or takes their role away.
                    owner_join = true;
                    ClientMessage::JoinRoom { room_code: code, username }
                }
            } else {
                reopen_code = Some(code);
                ClientMessage::CreateRoom { username }
            };
        }
        match cm {
            ClientMessage::CheckRoom { room_code } => {
                let code = room_code.trim().to_uppercase();
                let open = self.rooms.lock().await.get(&code).is_some_and(Room::host_is_reachable);
                let _ = tx.send(ServerMessage::RoomAvailability { room_code: code, open });
            }
            ClientMessage::OpenSavedRoom { .. } | ClientMessage::JoinSavedRoom { .. } => {
                unreachable!()
            }
            ClientMessage::SaveRoom => {
                let (Some(me), Some(code)) = (uid.as_ref(), room_code.as_ref()) else { return };
                let mut rooms = self.rooms.lock().await;
                let Some(room) = rooms.get_mut(code) else { return };
                if !room.is_host(me) {
                    let _ = tx.send(err("not_host", "Only the host can save this room."));
                    return;
                }
                if room.saved {
                    // Safe retry if the acknowledgement was lost. A successor host cannot obtain
                    // the owner's credential.
                    if room.owner_id.as_ref() == Some(me) {
                        if let Ok(Some(token)) = self.reservation(code) {
                            let _ = tx.send(ServerMessage::RoomSaved {
                                room_code: code.clone(),
                                owner_token: token,
                            });
                        }
                    } else {
                        let _ = tx.send(err("already_saved", "This room already has an owner."));
                    }
                    return;
                }
                let token = gen_token();
                let store = self.saved.lock().unwrap();
                let result = store.as_ref().ok_or(()).and_then(|db| {
                    let count: i64 = db
                        .query_row("SELECT COUNT(*) FROM saved_rooms", [], |r| r.get(0))
                        .map_err(|_| ())?;
                    if count >= MAX_ROOMS as i64 {
                        return Err(());
                    }
                    let transaction = db.unchecked_transaction().map_err(|_| ())?;
                    transaction.execute(
                        "INSERT INTO saved_rooms(code, token) VALUES(?1, ?2)",
                        [code, &token],
                    )
                    .map_err(|_| ())?;
                    for guest_token in &room.approved_guests {
                        transaction.execute("INSERT OR IGNORE INTO approved_guests(room_code, token) VALUES(?1, ?2)", [code, guest_token]).map_err(|_| ())?;
                    }
                    transaction.commit().map_err(|_| ())
                });
                if result.is_err() {
                    let _ = tx.send(err("save_failed", "The relay could not persist this room. Update/configure the relay and try again."));
                    return;
                }
                room.saved = true;
                room.owner_id = Some(me.clone());
                let _ = tx
                    .send(ServerMessage::RoomSaved { room_code: code.clone(), owner_token: token });
                room.broadcast(&ServerMessage::SyncState { state: room.wire_state(code) }, None);
            }
            ClientMessage::Ping => {
                let _ = tx.send(ServerMessage::Pong);
            }

            ClientMessage::CreateRoom { username } => {
                let mut rooms = self.rooms.lock().await;
                if rooms.len() >= MAX_ROOMS {
                    let _ = tx.send(err("server_full", "The server is full, try again later."));
                    return;
                }
                let code = if let Some(code) = reopen_code {
                    if rooms.contains_key(&code) {
                        let _ =
                            tx.send(err("room_busy", "Room opened elsewhere; reconnect again."));
                        return;
                    }
                    code
                } else {
                    loop {
                        let c = gen_code();
                        if !rooms.contains_key(&c) {
                            match self.reservation(&c) {
                                Ok(None) => break c,
                                Ok(Some(_)) => continue,
                                Err(_) => {
                                    let _ = tx.send(err(
                                        "storage_failed",
                                        "The relay room registry is unavailable.",
                                    ));
                                    return;
                                }
                            }
                        }
                    }
                };
                let user_id = gen_user_id();
                let token = gen_token();
                let mut peers = HashMap::new();
                peers.insert(
                    user_id.clone(),
                    Peer {
                        guest_token: None,
                        username: sanitize(&username),
                        tx: tx.clone(),
                        connected: true,
                        session_token: token.clone(),
                        disconnected_at: None,
                    },
                );
                let room = Room {
                    recovery_epoch: gen_token(),
                    recovery_endpoints: HashMap::new(),
                    approved_guests: match self.approved_guests(&code) {
                        Ok(guests) => guests,
                        Err(_) => {
                            let _ = tx.send(err(
                                "storage_failed",
                                "The relay approval registry is unavailable.",
                            ));
                            return;
                        }
                    },
                    saved: matches!(self.reservation(&code), Ok(Some(_))),
                    owner_id: Some(user_id.clone()),
                    host_id: user_id.clone(),
                    peers,
                    pending: HashMap::new(),
                    suggestions: HashMap::new(),
                    current_track: None,
                    is_playing: false,
                    position_ms: 0,
                    last_update_ms: 0,
                    volume: 1.0,
                    queue: Vec::new(),
                };
                let state = room.wire_state(&code);
                rooms.insert(code.clone(), room);
                *uid = Some(user_id.clone());
                *room_code = Some(code.clone());
                let _ = tx.send(ServerMessage::RoomCreated {
                    room_code: code,
                    user_id,
                    session_token: token,
                    state,
                });
            }

            ClientMessage::JoinRoom { room_code: code, username } => {
                let code = code.trim().to_uppercase();
                let mut rooms = self.rooms.lock().await;
                let Some(room) = rooms.get_mut(&code) else {
                    let _ = tx.send(err("room_not_found", "No room with that code."));
                    return;
                };
                if let Some(token) =
                    returning_guest.filter(|token| room.approved_guests.contains(token))
                {
                    // Replace this guest's stale socket/slot, rather than accumulating duplicates.
                    let previous = room
                        .peers
                        .iter()
                        .find(|(_, peer)| peer.guest_token.as_ref() == Some(&token))
                        .map(|(id, _)| id.clone());
                    if previous.is_none()
                        && room.peers.len() + room.pending.len() >= MAX_USERS_PER_ROOM
                    {
                        let _ = tx.send(err("room_full", "That room is full."));
                        return;
                    }
                    let user_id = previous.unwrap_or_else(gen_user_id);
                    room.admit(&code, user_id.clone(), sanitize(&username), tx.clone(), token);
                    *uid = Some(user_id);
                    *room_code = Some(code);
                    return;
                }
                // Only new guests need a reachable host to answer a request.
                if !room.host_is_reachable() {
                    let _ =
                        tx.send(err("host_unavailable", "The host isn't in that room right now."));
                    return;
                }
                if room.peers.len() + room.pending.len() >= MAX_USERS_PER_ROOM {
                    let _ = tx.send(err("room_full", "That room is full."));
                    return;
                }
                let user_id = gen_user_id();
                room.pending.insert(
                    user_id.clone(),
                    Pending { owner: owner_join, username: sanitize(&username), tx: tx.clone() },
                );
                let host_id = room.host_id.clone();
                let uname = room.pending[&user_id].username.clone();
                room.send_to(
                    &host_id,
                    ServerMessage::JoinRequest { user_id: user_id.clone(), username: uname },
                );
                *uid = Some(user_id);
                *room_code = Some(code);
            }

            ClientMessage::ApproveJoin { user_id: joiner } => {
                let (Some(me), Some(code)) = (uid.clone(), room_code.clone()) else { return };
                let mut rooms = self.rooms.lock().await;
                let Some(room) = rooms.get_mut(&code) else { return };
                if !room.is_host(&me) {
                    let _ = tx.send(err("not_host", "Only the host can approve joins."));
                    return;
                }
                if !room.pending.contains_key(&joiner) {
                    return;
                }
                // Commit permission before acknowledging approval. Never promise durable access
                // when the relay could not save it.
                if room.approved_guests.len() >= 1024 {
                    let _ =
                        tx.send(err("approval_limit", "Too many remembered guests in this room."));
                    return;
                }
                let guest_token = gen_token();
                if room.saved && self.persist_guest(&code, &guest_token, false).is_err() {
                    let _ = tx.send(err(
                        "storage_failed",
                        "Could not remember this approval. Try again.",
                    ));
                    return;
                }
                room.approved_guests.insert(guest_token.clone());
                let p = room.pending.remove(&joiner).unwrap();
                if p.owner {
                    room.owner_id = Some(joiner.clone());
                }
                room.admit(&code, joiner, p.username, p.tx, guest_token);
            }

            ClientMessage::RejectJoin { user_id: joiner } => {
                let (Some(me), Some(code)) = (uid.clone(), room_code.clone()) else { return };
                let mut rooms = self.rooms.lock().await;
                let Some(room) = rooms.get_mut(&code) else { return };
                if !room.is_host(&me) {
                    return;
                }
                if let Some(p) = room.pending.remove(&joiner) {
                    let _ = p.tx.send(ServerMessage::JoinRejected {
                        reason: "The host declined your request.".into(),
                    });
                }
            }

            ClientMessage::Playback(mut p) => {
                let (Some(me), Some(code)) = (uid.clone(), room_code.clone()) else { return };
                let mut rooms = self.rooms.lock().await;
                let Some(room) = rooms.get_mut(&code) else { return };
                if !room.is_host(&me) {
                    let _ = tx.send(err("not_host", "Only the host controls playback."));
                    return;
                }
                if let Some(q) = p.queue.as_mut() {
                    q.truncate(MAX_QUEUE);
                }
                let now = now_ms();
                match p.kind {
                    PlaybackKind::Play => {
                        room.is_playing = true;
                        room.position_ms = p.position_ms;
                        room.last_update_ms = now;
                        p.server_time_ms = now; // clients offset their start by this
                    }
                    PlaybackKind::Pause => {
                        room.is_playing = false;
                        room.position_ms = p.position_ms;
                        room.last_update_ms = now;
                    }
                    PlaybackKind::Seek => {
                        room.position_ms = p.position_ms;
                        room.last_update_ms = now;
                    }
                    PlaybackKind::ChangeTrack => {
                        room.current_track = p.track.clone();
                        room.position_ms = p.position_ms;
                        room.is_playing = p.playing;
                        room.last_update_ms = now;
                        if let Some(q) = &p.queue {
                            room.queue = q.clone();
                        }
                    }
                    PlaybackKind::SyncQueue => {
                        if let Some(q) = &p.queue {
                            room.queue = q.clone();
                        }
                    }
                    PlaybackKind::SetVolume => {
                        room.volume = p.volume;
                    }
                }
                // Host already applied it locally; broadcast to guests only (no echo).
                room.broadcast(&ServerMessage::SyncPlayback(p), Some(&me));
            }

            ClientMessage::RequestSync => {
                let (Some(me), Some(code)) = (uid.clone(), room_code.clone()) else { return };
                let rooms = self.rooms.lock().await;
                let Some(room) = rooms.get(&code) else { return };
                if !room.peers.contains_key(&me) {
                    return; // a joiner still awaiting approval doesn't get the room's state
                }
                // Normalize to the live position so the requester seeks to the right spot.
                let now = now_ms();
                let mut state = room.wire_state(&code);
                state.position_ms = state.live_position_ms(now);
                state.last_update_ms = now;
                let _ = tx.send(ServerMessage::SyncState { state });
            }

            ClientMessage::Suggest { track } => {
                let (Some(me), Some(code)) = (uid.clone(), room_code.clone()) else { return };
                let mut rooms = self.rooms.lock().await;
                let Some(room) = rooms.get_mut(&code) else { return };
                if room.is_host(&me) {
                    return; // host adds tracks directly
                }
                let Some(peer) = room.peers.get(&me) else { return };
                let suggestion = Suggestion {
                    id: gen_token(),
                    from_user_id: me.clone(),
                    from_username: peer.username.clone(),
                    track,
                };
                room.suggestions.insert(suggestion.id.clone(), suggestion.clone());
                let host_id = room.host_id.clone();
                room.send_to(&host_id, ServerMessage::SuggestionReceived { suggestion });
            }

            ClientMessage::ApproveSuggestion { id } => {
                let (Some(me), Some(code)) = (uid.clone(), room_code.clone()) else { return };
                let mut rooms = self.rooms.lock().await;
                let Some(room) = rooms.get_mut(&code) else { return };
                if !room.is_host(&me) {
                    return;
                }
                // The host client does the actual queueing (it owns the playback queue) and will
                // broadcast a SyncQueue; here we just drop the pending suggestion and notify the
                // suggester.
                if let Some(s) = room.suggestions.remove(&id) {
                    room.send_to(&s.from_user_id, ServerMessage::SuggestionApproved { id });
                }
            }

            ClientMessage::RejectSuggestion { id } => {
                let (Some(me), Some(code)) = (uid.clone(), room_code.clone()) else { return };
                let mut rooms = self.rooms.lock().await;
                let Some(room) = rooms.get_mut(&code) else { return };
                if !room.is_host(&me) {
                    return;
                }
                if let Some(s) = room.suggestions.remove(&id) {
                    room.send_to(
                        &s.from_user_id,
                        ServerMessage::SuggestionRejected {
                            id,
                            reason: "The host declined your suggestion.".into(),
                        },
                    );
                }
            }

            ClientMessage::KickUser { user_id: target } => {
                let (Some(me), Some(code)) = (uid.clone(), room_code.clone()) else { return };
                let mut rooms = self.rooms.lock().await;
                let Some(room) = rooms.get_mut(&code) else { return };
                if !room.is_host(&me) || target == me {
                    return;
                }
                if let Some(token) = room.peers.get(&target).and_then(|p| p.guest_token.clone()) {
                    if room.saved && self.persist_guest(&code, &token, true).is_err() {
                        let _ = tx.send(err(
                            "storage_failed",
                            "Could not revoke this guest's access. Try again.",
                        ));
                        return;
                    }
                    room.approved_guests.remove(&token);
                }
                if let Some(p) = room.peers.remove(&target) {
                    room.recovery_endpoints.remove(&target);
                    let _ = p.tx.send(ServerMessage::Kicked {
                        reason: "The host removed you from the room.".into(),
                    });
                    room.broadcast(&ServerMessage::UserLeft { user_id: target }, None);
                    room.broadcast_recovery();
                }
            }

            ClientMessage::TransferHost { user_id: target } => {
                let (Some(me), Some(code)) = (uid.clone(), room_code.clone()) else { return };
                let mut rooms = self.rooms.lock().await;
                let Some(room) = rooms.get_mut(&code) else { return };
                // Not to someone whose socket is down: the room would have a host nobody can
                // reach, and joins are refused until they come back or their slot expires.
                if !room.is_host(&me) || !room.peers.get(&target).is_some_and(|p| p.connected) {
                    return;
                }
                room.set_host(target);
            }

            ClientMessage::Reconnect { session_token } => {
                let mut rooms = self.rooms.lock().await;
                // Find the room + user owning this token.
                let found = rooms.iter_mut().find_map(|(code, room)| {
                    room.peers
                        .iter()
                        .find(|(_, p)| p.session_token == session_token)
                        .map(|(id, _)| (code.clone(), id.clone()))
                });
                let Some((code, user_id)) = found else {
                    let _ = tx.send(err("session_expired", "Your session has expired."));
                    return;
                };
                let room = rooms.get_mut(&code).unwrap();
                {
                    let peer = room.peers.get_mut(&user_id).unwrap();
                    peer.tx = tx.clone();
                    peer.connected = true;
                    peer.disconnected_at = None;
                }
                let is_host = room.is_host(&user_id);
                let now = now_ms();
                let mut state = room.wire_state(&code);
                state.position_ms = state.live_position_ms(now);
                state.last_update_ms = now;
                let _ = tx.send(ServerMessage::Reconnected {
                    user_id: user_id.clone(),
                    is_host,
                    state,
                });
                room.broadcast(
                    &ServerMessage::UserReconnected { user_id: user_id.clone() },
                    Some(&user_id),
                );
                if is_host {
                    room.brief_host();
                }
                *uid = Some(user_id);
                *room_code = Some(code);
            }

            ClientMessage::LeaveRoom => {
                if let (Some(me), Some(code)) = (uid.take(), room_code.take()) {
                    let mut rooms = self.rooms.lock().await;
                    self.remove_member(&mut rooms, &code, &me, /*graceful=*/ true);
                }
            }
            ClientMessage::RegisterRecovery { endpoint_id } => {
                let endpoint_id = endpoint_id.to_ascii_lowercase();
                let (Some(me), Some(code)) = (uid.as_ref(), room_code.as_ref()) else { return };
                let mut rooms = self.rooms.lock().await;
                let Some(room) = rooms.get_mut(code) else { return };
                if !room.peers.get(me).is_some_and(|p| p.connected && p.tx.same_channel(tx)) {
                    return; // Pending joiners cannot discover or advertise member endpoints.
                }
                if endpoint_id.len() != 64 || !endpoint_id.bytes().all(|b| b.is_ascii_hexdigit())
                    || room.recovery_endpoints.iter().any(|(id, e)| id != me && e == &endpoint_id)
                {
                    let _ = tx.send(err("invalid_recovery_endpoint", "Invalid recovery address."));
                    return;
                }
                room.recovery_endpoints.insert(me.clone(), endpoint_id);
                room.broadcast_recovery();
            }
            ClientMessage::SeedRecovery { state, guest_tokens } => {
                let (Some(me), Some(code)) = (uid.as_ref(), room_code.as_ref()) else { return };
                let mut rooms = self.rooms.lock().await;
                let Some(room) = rooms.get_mut(code) else { return };
                if !room.is_host(me) || room.peers.len() != 1 {
                    let _ = tx.send(err("recovery_failed", "Only an empty room can receive a recovered playlist."));
                    return;
                }
                if guest_tokens.len() >= MAX_USERS_PER_ROOM || guest_tokens.iter().any(|t|
                    t.len() != 64 || !t.bytes().all(|b| b.is_ascii_hexdigit())) {
                    let _ = tx.send(err("recovery_failed", "Invalid recovery permissions."));
                    return;
                }
                // Commit every new pass together before sending any invitations.
                let result = (|| -> rusqlite::Result<()> {
                    let store = self.saved.lock().unwrap();
                    if let Some(db) = store.as_ref().filter(|_| room.saved) {
                        let transaction = db.unchecked_transaction()?;
                        for token in &guest_tokens {
                            transaction.execute("INSERT OR IGNORE INTO approved_guests(room_code, token) VALUES(?1, ?2)", [code, token])?;
                        }
                        transaction.commit()?;
                    }
                    Ok(())
                })();
                if result.is_err() {
                    let _ = tx.send(err("recovery_failed", "Could not save recovery permissions. Try again."));
                    return;
                }
                room.approved_guests.extend(guest_tokens);
                let now = now_ms();
                room.position_ms = state.live_position_ms(now).max(0);
                room.current_track = state.current_track;
                room.queue = state.queue.into_iter().take(MAX_QUEUE).collect();
                room.is_playing = state.is_playing;
                room.last_update_ms = now;
                let _ = tx.send(ServerMessage::RecoveryReady { room_code: code.clone() });
            }
        }
    }

    /// Remove a member entirely (explicit leave, or reconnect grace expired). Handles host handoff
    /// and empty-room bookkeeping. `graceful` distinguishes an explicit leave (broadcast UserLeft)
    /// from a grace-expiry sweep (already announced as disconnected).
    fn remove_member(
        &self,
        rooms: &mut HashMap<String, Room>,
        code: &str,
        me: &str,
        graceful: bool,
    ) {
        let Some(room) = rooms.get_mut(code) else { return };
        room.drop_pending(me);
        room.recovery_endpoints.remove(me);
        if room.peers.remove(me).is_some() && graceful {
            room.broadcast(&ServerMessage::UserLeft { user_id: me.to_string() }, None);
        }
        // Host left → hand off to any connected peer.
        if room.host_id == me {
            if let Some(next) = room.any_connected_other(me) {
                room.set_host(next);
            }
        }
        // Last member gone: nobody holds a session token for this room anymore, so it can never be
        // re-entered — only trap joiners. Close it, and turn away anyone still knocking.
        if room.peers.is_empty() {
            for (_, p) in room.pending.drain() {
                let _ = p.tx.send(ServerMessage::JoinRejected {
                    reason: "The host closed the session.".into(),
                });
            }
            rooms.remove(code);
            tracing::info!(room = %code, "room closed (last member left)");
        } else {
            room.broadcast_recovery();
        }
    }

    /// A socket dropped without leaving. Keep the slot for reconnection. A host keeps the role for
    /// [`HOST_GRACE`] before `cleanup` hands it on: handing it off at once moved it to a random
    /// guest on every network blip, and the host came back as a guest whose own playback was then
    /// overridden. Still bounded, so Metrolist's up-to-15-min dead zone (context/19 §4.7) stays
    /// fixed.
    async fn handle_disconnect(&self, uid: Option<String>, room_code: Option<String>, tx: &Tx) {
        let (Some(me), Some(code)) = (uid, room_code) else { return };
        let mut rooms = self.rooms.lock().await;
        let Some(room) = rooms.get_mut(&code) else { return };
        if room.drop_pending(&me) {
            return;
        }
        let Some(peer) = room.peers.get_mut(&me) else { return };
        // The user already reconnected on another socket, and this is the old one finally timing
        // out. Marking them disconnected would hand off their host role and, after the grace,
        // delete a live member who then silently stops getting the room's messages.
        if !peer.tx.same_channel(tx) {
            return;
        }
        peer.connected = false;
        peer.disconnected_at = Some(Instant::now());
        room.broadcast(&ServerMessage::UserDisconnected { user_id: me.clone() }, Some(&me));
    }

    /// Sweep expired reconnection slots and empty rooms, and move the host role off a host who
    /// has been gone longer than [`HOST_GRACE`].
    async fn cleanup(&self) {
        let mut rooms = self.rooms.lock().await;
        let now = Instant::now();
        let codes: Vec<String> = rooms.keys().cloned().collect();
        for code in codes {
            {
                let room = rooms.get_mut(&code).unwrap();
                let host_gone = room
                    .peers
                    .get(&room.host_id)
                    .and_then(|p| p.disconnected_at)
                    .is_some_and(|t| now - t > HOST_GRACE);
                if host_gone {
                    let host = room.host_id.clone();
                    if let Some(next) = room.any_connected_other(&host) {
                        room.set_host(next);
                    }
                }
            }
            // Collect peers past the grace window, then remove them via the shared path.
            let expired: Vec<String> = {
                let room = &rooms[&code];
                room.peers
                    .iter()
                    .filter(|(_, p)| {
                        !p.connected
                            && p.disconnected_at.map(|t| now - t > RECONNECT_GRACE).unwrap_or(false)
                    })
                    .map(|(id, _)| id.clone())
                    .collect()
            };
            for id in expired {
                // Drops the room too once the last slot expires (see `remove_member`).
                self.remove_member(&mut rooms, &code, &id, /*graceful=*/ false);
            }
        }
    }
}

/// Trim + cap a user-supplied string (username), stripping control chars.
fn sanitize(s: &str) -> String {
    let cleaned: String = s.trim().chars().filter(|c| !c.is_control()).take(50).collect();
    if cleaned.is_empty() {
        "Guest".to_string()
    } else {
        cleaned
    }
}

async fn handle_conn(stream: TcpStream, server: Arc<Server>) {
    let config = tokio_tungstenite::tungstenite::protocol::WebSocketConfig {
        max_message_size: Some(MAX_MESSAGE),
        max_frame_size: Some(MAX_MESSAGE),
        ..Default::default()
    };
    let ws = match tokio_tungstenite::accept_async_with_config(stream, Some(config)).await {
        Ok(ws) => ws,
        Err(e) => {
            tracing::debug!(error = %e, "ws handshake failed");
            return;
        }
    };
    let (mut sink, mut read) = ws.split();
    let (tx, mut rx) = mpsc::unbounded_channel::<ServerMessage>();

    // Writer task: drain outbound messages to the socket.
    let writer = tokio::spawn(async move {
        while let Some(msg) = rx.recv().await {
            if sink.send(Message::Text(msg.to_json())).await.is_err() {
                break;
            }
        }
    });

    let mut uid: Option<String> = None;
    let mut room_code: Option<String> = None;

    // Clients ping every 25s, so this much silence is a dead socket. Without a limit a half-open
    // one (the client slept or changed networks) holds its slot forever.
    while let Ok(Some(next)) = tokio::time::timeout(IDLE_LIMIT, read.next()).await {
        match next {
            Ok(Message::Text(t)) => match serde_json::from_str::<ClientMessage>(&t) {
                Ok(cm) => server.dispatch(cm, &tx, &mut uid, &mut room_code).await,
                Err(e) => tracing::debug!(error = %e, "bad client message"),
            },
            Ok(Message::Close(_)) | Err(_) => break,
            _ => {} // ping/pong/binary ignored (tungstenite auto-pongs)
        }
    }

    server.handle_disconnect(uid, room_code, &tx).await;
    writer.abort();
}

#[tokio::main]
async fn main() {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "info,sync_server=info".into()),
        )
        .init();

    let port: u16 = std::env::var("PORT").ok().and_then(|p| p.parse().ok()).unwrap_or(8080);
    let bind_ip = std::env::var("BIND_ADDRESS")
        .unwrap_or_else(|_| "0.0.0.0".into())
        .parse::<std::net::IpAddr>()
        .expect("valid BIND_ADDRESS");
    let addr = std::net::SocketAddr::new(bind_ip, port);
    let listener = TcpListener::bind(&addr).await.expect("bind");
    tracing::info!(%addr, "limusic-sync listening (plain ws — front with Tailscale/Cloudflare for wss)");

    let store_path =
        std::env::var_os("LIMUSIC_ROOMS_DB").unwrap_or_else(|| "saved-rooms.sqlite3".into());
    let server = Arc::new(
        Server::with_store(std::path::Path::new(&store_path)).expect("open saved room registry"),
    );

    // Background cleanup sweep.
    {
        let server = server.clone();
        tokio::spawn(async move {
            loop {
                tokio::time::sleep(SWEEP_EVERY).await;
                server.cleanup().await;
            }
        });
    }

    loop {
        match listener.accept().await {
            Ok((stream, _)) => {
                let server = server.clone();
                tokio::spawn(handle_conn(stream, server));
            }
            Err(e) => tracing::warn!(error = %e, "accept failed"),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use listen_protocol::Playback;

    #[tokio::test]
    async fn recovery_contacts_are_members_only_and_follow_transfers_and_kicks() {
        let server = Server::default();
        let (mut host, mut guest, _) = room_with_guest(&server).await;
        let code = host.code.clone().unwrap();
        host.send(&server, ClientMessage::RegisterRecovery { endpoint_id: "a".repeat(64) }).await;
        guest.send(&server, ClientMessage::RegisterRecovery { endpoint_id: "b".repeat(64) }).await;
        let mut pending = Conn::new();
        pending.send(&server, ClientMessage::JoinRoom { room_code: code.clone(), username: "Pending".into() }).await;
        host.drain(); guest.drain();
        pending.send(&server, ClientMessage::RegisterRecovery { endpoint_id: "c".repeat(64) }).await;
        assert!(pending.drain().is_empty(), "pending joiners never receive recovery contacts");
        assert_eq!(server.rooms.lock().await[&code].recovery_endpoints.len(), 2);
        // Duplicate endpoints cannot alias an existing member.
        guest.send(&server, ClientMessage::RegisterRecovery { endpoint_id: "A".repeat(64) }).await;
        assert!(matches!(guest.drain().as_slice(), [ServerMessage::Error { .. }]));
        host.send(&server, ClientMessage::TransferHost { user_id: guest.id() }).await;
        let messages = guest.drain();
        assert!(messages.iter().any(|m| matches!(m, ServerMessage::RecoveryPeers { peers, .. }
            if peers.len() == 2 && peers.iter().any(|p| p.user_id == guest.id() && p.is_host))));
        guest.send(&server, ClientMessage::KickUser { user_id: host.id() }).await;
        assert!(guest.drain().iter().any(|m| matches!(m, ServerMessage::RecoveryPeers { peers, .. }
            if peers.len() == 1 && peers[0].user_id == guest.id())));
        assert!(!server.rooms.lock().await[&code].recovery_endpoints.contains_key(&host.id()));
    }

    fn recovery_playlist() -> RoomState {
        let track = |n: usize| Track {
            id: format!("{n:011}"), title: format!("Track {n}"), artist: "Artist".into(),
            thumbnail: None, duration_ms: 240_000, queued_by: None,
        };
        RoomState {
            current_track: Some(track(0)), queue: (1..=137).map(track).collect(),
            position_ms: 32_100, is_playing: false, ..Default::default()
        }
    }

    #[tokio::test]
    async fn recovery_preserves_long_playlist_and_fresh_permissions_survive_restart() {
        let path = std::env::temp_dir().join(format!("limusic-recovery-{}.sqlite3", gen_token()));
        let guest_token = "d".repeat(64);
        let (code, owner_token);
        {
            let server = Server::with_store(&path).unwrap();
            let mut host = Conn::new();
            host.send(&server, ClientMessage::CreateRoom { username: "Successor".into() }).await;
            code = host.code.clone().unwrap();
            host.drain();
            owner_token = save_host(&server, &mut host).await;
            host.send(&server, ClientMessage::SeedRecovery { state: recovery_playlist(), guest_tokens: vec![guest_token.clone()] }).await;
            assert!(matches!(host.drain().as_slice(), [ServerMessage::RecoveryReady { .. }]));
            let mut guest = Conn::new();
            guest.send(&server, ClientMessage::JoinSavedRoom {
                room_code: code.clone(), username: "Guest".into(), guest_token: guest_token.clone(),
            }).await;
            let messages = guest.drain();
            let state = messages.iter().find_map(|m| if let ServerMessage::JoinApproved { state, .. } = m { Some(state) } else { None }).unwrap();
            assert_eq!(state.queue, recovery_playlist().queue, "more than fifty upcoming tracks survive");
            assert_eq!(state.current_track, recovery_playlist().current_track);
            assert_eq!(state.position_ms, 32_100);
            assert!(!state.is_playing, "paused playback stays paused");
            // A guest cannot overwrite playback or issue recovery permissions.
            guest.send(&server, ClientMessage::SeedRecovery { state: RoomState::default(), guest_tokens: vec!["e".repeat(64)] }).await;
            assert!(matches!(guest.drain().as_slice(), [ServerMessage::Error { .. }]));
            assert_eq!(server.rooms.lock().await[&code].queue.len(), 137);
        }
        {
            let server = Server::with_store(&path).unwrap();
            let mut host = Conn::new();
            host.send(&server, ClientMessage::OpenSavedRoom { room_code: code.clone(), owner_token, username: "Successor".into() }).await;
            host.drain();
            let mut guest = Conn::new();
            guest.send(&server, ClientMessage::JoinSavedRoom { room_code: code.clone(), username: "Guest".into(), guest_token }).await;
            assert!(matches!(guest.drain().as_slice(), [ServerMessage::JoinApproved { .. }]));
            let mut outsider = Conn::new();
            outsider.send(&server, ClientMessage::JoinSavedRoom { room_code: code.clone(), username: "Guest".into(), guest_token: "f".repeat(64) }).await;
            assert!(outsider.drain().is_empty(), "names and unknown passes cannot bypass approval");
        }
        std::fs::remove_file(path).unwrap();
    }

    #[tokio::test]
    async fn recovery_storage_failure_does_not_publish_invites_or_replace_state() {
        let path = std::env::temp_dir().join(format!("limusic-recovery-failure-{}.sqlite3", gen_token()));
        {
            let server = Server::with_store(&path).unwrap();
            let mut host = Conn::new();
            host.send(&server, ClientMessage::CreateRoom { username: "Host".into() }).await;
            host.drain();
            save_host(&server, &mut host).await;
            server.saved.lock().unwrap().as_ref().unwrap().execute("DROP TABLE approved_guests", []).unwrap();
            host.send(&server, ClientMessage::SeedRecovery { state: recovery_playlist(), guest_tokens: vec!["d".repeat(64)] }).await;
            assert!(matches!(host.drain().as_slice(), [ServerMessage::Error { code, .. }] if code == "recovery_failed"));
            let rooms = server.rooms.lock().await;
            let room = &rooms[host.code.as_ref().unwrap()];
            assert!(room.approved_guests.is_empty());
            assert!(room.current_track.is_none() && room.queue.is_empty());
        }
        std::fs::remove_file(path).unwrap();
    }

    #[tokio::test]
    async fn availability_checks_are_read_only_and_require_an_open_host() {
        let server = Server::default();
        let (mut host, mut guest, _) = room_with_guest(&server).await;
        let code = host.code.clone().unwrap();
        let mut probe = Conn::new();
        for _ in 0..3 {
            probe.send(&server, ClientMessage::CheckRoom { room_code: code.clone() }).await;
            assert!(matches!(
                probe.drain().as_slice(),
                [ServerMessage::RoomAvailability { open: true, .. }]
            ));
        }
        assert!(probe.uid.is_none() && probe.code.is_none(), "a probe never becomes a member");
        assert!(
            host.drain().is_empty() && guest.drain().is_empty(),
            "no approval or member notifications"
        );
        assert_eq!(server.rooms.lock().await[&code].peers.len(), 2);
        assert!(server.rooms.lock().await[&code].pending.is_empty());
        server.handle_disconnect(host.uid.clone(), host.code.clone(), &host.tx).await;
        probe.send(&server, ClientMessage::CheckRoom { room_code: code }).await;
        assert!(matches!(
            probe.drain().as_slice(),
            [ServerMessage::RoomAvailability { open: false, .. }]
        ));
        probe.send(&server, ClientMessage::CheckRoom { room_code: "NOT-OPEN".into() }).await;
        assert!(matches!(
            probe.drain().as_slice(),
            [ServerMessage::RoomAvailability { open: false, .. }]
        ));
        assert_eq!(server.rooms.lock().await.len(), 1, "a probe cannot reopen a room");
    }

    #[tokio::test]
    async fn remembered_guest_reattaches_grace_slot_and_transferred_host_role() {
        let server = Server::default();
        let (mut host, guest, _) = room_with_guest(&server).await;
        let code = guest.code.clone().unwrap();
        let id = guest.id();
        let pass = server.rooms.lock().await[&code].peers[&id].guest_token.clone().unwrap();
        host.send(&server, ClientMessage::TransferHost { user_id: id.clone() }).await;
        server.handle_disconnect(guest.uid.clone(), guest.code.clone(), &guest.tx).await;
        assert!(!server.rooms.lock().await[&code].host_is_reachable());
        let mut returning = Conn::new();
        returning
            .send(
                &server,
                ClientMessage::JoinSavedRoom {
                    room_code: code.clone(),
                    username: "guest".into(),
                    guest_token: pass,
                },
            )
            .await;
        assert!(
            matches!(returning.drain().as_slice(), [ServerMessage::JoinApproved { user_id, state, .. }] if user_id == &id && state.host_id == id)
        );
        assert_eq!(server.rooms.lock().await[&code].peers.len(), 2, "no duplicate grace slot");
        server.handle_disconnect(guest.uid, guest.code, &guest.tx).await;
        assert!(
            server.rooms.lock().await[&code].peers[&id].connected,
            "old socket cannot disconnect the returning guest"
        );
    }

    #[tokio::test]
    async fn approval_storage_failure_keeps_request_pending() {
        let server = Server::with_store(std::path::Path::new(":memory:")).unwrap();
        let mut host = Conn::new();
        host.send(&server, ClientMessage::CreateRoom { username: "host".into() }).await;
        save_host(&server, &mut host).await;
        let mut guest = Conn::new();
        guest
            .send(
                &server,
                ClientMessage::JoinRoom {
                    room_code: host.code.clone().unwrap(),
                    username: "guest".into(),
                },
            )
            .await;
        host.drain();
        server
            .saved
            .lock()
            .unwrap()
            .as_ref()
            .unwrap()
            .execute_batch("DROP TABLE approved_guests")
            .unwrap();
        host.send(&server, ClientMessage::ApproveJoin { user_id: guest.id() }).await;
        assert!(guest.drain().is_empty());
        assert!(
            matches!(host.drain().as_slice(), [ServerMessage::Error { code, .. }] if code == "storage_failed")
        );
        let rooms = server.rooms.lock().await;
        let room = &rooms[host.code.as_ref().unwrap()];
        assert!(room.pending.contains_key(&guest.id()));
        assert!(room.approved_guests.is_empty());
    }

    #[tokio::test]
    async fn remembered_approval_survives_restart_is_scoped_and_kick_revokes_it() {
        let path = std::env::temp_dir().join(format!("limusic-approvals-{}.sqlite3", gen_token()));
        let (code, owner_token, guest_token);
        {
            let server = Server::with_store(&path).unwrap();
            let mut host = Conn::new();
            host.send(&server, ClientMessage::CreateRoom { username: "host".into() }).await;
            code = host.code.clone().unwrap();
            let mut guest = Conn::new();
            guest
                .send(
                    &server,
                    ClientMessage::JoinRoom { room_code: code.clone(), username: "guest".into() },
                )
                .await;
            assert!(guest.drain().is_empty());
            host.send(&server, ClientMessage::ApproveJoin { user_id: guest.id() }).await;
            guest_token = guest
                .drain()
                .into_iter()
                .find_map(|m| match m {
                    ServerMessage::JoinApproved { guest_token, .. } => guest_token,
                    _ => None,
                })
                .unwrap();
            guest.send(&server, ClientMessage::LeaveRoom).await;
            host.drain();
            let mut returning = Conn::new();
            returning
                .send(
                    &server,
                    ClientMessage::JoinSavedRoom {
                        room_code: code.clone(),
                        username: "renamed guest".into(),
                        guest_token: guest_token.clone(),
                    },
                )
                .await;
            assert!(matches!(returning.drain().as_slice(), [ServerMessage::JoinApproved { .. }]));
            assert!(!host.drain().iter().any(|m| matches!(m, ServerMessage::JoinRequest { .. })));
            assert!(!server.rooms.lock().await[&code].is_host(&returning.id()));
            // Saving an already active room also persists approvals issued before it was saved.
            owner_token = save_host(&server, &mut host).await;
        }
        {
            let server = Server::with_store(&path).unwrap();
            let mut offline = Conn::new();
            offline
                .send(
                    &server,
                    ClientMessage::JoinSavedRoom {
                        room_code: code.clone(),
                        username: "guest".into(),
                        guest_token: guest_token.clone(),
                    },
                )
                .await;
            assert!(
                matches!(offline.drain().as_slice(), [ServerMessage::Error { code, .. }] if code == "room_not_found")
            );
            assert!(server.rooms.lock().await.is_empty(), "a credential cannot reopen a room");
            let mut host = Conn::new();
            host.send(
                &server,
                ClientMessage::OpenSavedRoom {
                    room_code: code.clone(),
                    owner_token: owner_token.clone(),
                    username: "host".into(),
                },
            )
            .await;
            host.drain();
            let mut guest = Conn::new();
            guest
                .send(
                    &server,
                    ClientMessage::JoinSavedRoom {
                        room_code: code.clone(),
                        username: "guest".into(),
                        guest_token: guest_token.clone(),
                    },
                )
                .await;
            assert!(matches!(guest.drain().as_slice(), [ServerMessage::JoinApproved { .. }]));
            assert!(!host.drain().iter().any(|m| matches!(m, ServerMessage::JoinRequest { .. })));
            let mut other_host = Conn::new();
            other_host.send(&server, ClientMessage::CreateRoom { username: "other".into() }).await;
            let mut wrong_room = Conn::new();
            wrong_room
                .send(
                    &server,
                    ClientMessage::JoinSavedRoom {
                        room_code: other_host.code.clone().unwrap(),
                        username: "guest".into(),
                        guest_token: guest_token.clone(),
                    },
                )
                .await;
            assert!(wrong_room.drain().is_empty(), "approval is scoped to its room");
            assert!(other_host
                .drain()
                .iter()
                .any(|m| matches!(m, ServerMessage::JoinRequest { .. })));
            let mut imposter = Conn::new();
            imposter
                .send(
                    &server,
                    ClientMessage::JoinSavedRoom {
                        room_code: code.clone(),
                        username: "guest".into(),
                        guest_token: "wrong".into(),
                    },
                )
                .await;
            assert!(imposter.drain().is_empty(), "display name does not grant access");
            assert!(host.drain().iter().any(|m| matches!(m, ServerMessage::JoinRequest { .. })));
            host.send(&server, ClientMessage::KickUser { user_id: guest.id() }).await;
            assert!(matches!(guest.drain().as_slice(), [ServerMessage::Kicked { .. }]));
        }
        {
            let server = Server::with_store(&path).unwrap();
            let mut host = Conn::new();
            host.send(
                &server,
                ClientMessage::OpenSavedRoom {
                    room_code: code.clone(),
                    owner_token,
                    username: "host".into(),
                },
            )
            .await;
            let mut guest = Conn::new();
            guest
                .send(
                    &server,
                    ClientMessage::JoinSavedRoom {
                        room_code: code,
                        username: "guest".into(),
                        guest_token,
                    },
                )
                .await;
            assert!(
                guest.drain().is_empty(),
                "kicking revokes remembered permission across restarts"
            );
            assert!(host.drain().iter().any(|m| matches!(m, ServerMessage::JoinRequest { .. })));
            host.send(&server, ClientMessage::ApproveJoin { user_id: guest.id() }).await;
            assert!(matches!(
                guest.drain().as_slice(),
                [ServerMessage::JoinApproved { guest_token: Some(_), .. }]
            ));
        }
        std::fs::remove_file(path).unwrap();
    }

    async fn save_host(server: &Server, host: &mut Conn) -> String {
        host.drain();
        host.send(server, ClientMessage::SaveRoom).await;
        host.drain()
            .into_iter()
            .find_map(|m| match m {
                ServerMessage::RoomSaved { owner_token, .. } => Some(owner_token),
                _ => None,
            })
            .expect("room reservation committed")
    }

    #[tokio::test]
    async fn saved_identity_survives_last_leave_and_relay_restart() {
        let path = std::env::temp_dir().join(format!("limusic-rooms-{}.sqlite3", gen_token()));
        let code;
        let token;
        {
            let server = Server::with_store(&path).unwrap();
            let mut host = Conn::new();
            host.send(&server, ClientMessage::CreateRoom { username: "owner".into() }).await;
            code = host.code.clone().unwrap();
            token = save_host(&server, &mut host).await;
            host.send(&server, ClientMessage::LeaveRoom).await;
            assert!(server.rooms.lock().await.is_empty());
            assert_eq!(server.reservation(&code).unwrap(), Some(token.clone()));
        }
        {
            let server = Server::with_store(&path).unwrap();
            let mut intruder = Conn::new();
            intruder
                .send(
                    &server,
                    ClientMessage::OpenSavedRoom {
                        room_code: code.clone(),
                        owner_token: "wrong".into(),
                        username: "bad".into(),
                    },
                )
                .await;
            assert!(matches!(intruder.drain().as_slice(), [ServerMessage::Error { .. }]));
            assert!(server.rooms.lock().await.is_empty());

            let mut owner = Conn::new();
            owner
                .send(
                    &server,
                    ClientMessage::OpenSavedRoom {
                        room_code: code.clone(),
                        owner_token: token,
                        username: "owner".into(),
                    },
                )
                .await;
            let got = owner.drain();
            assert!(
                matches!(got.as_slice(), [ServerMessage::RoomCreated { state, .. }] if state.saved && state.room_code == code && state.current_track.is_none())
            );

            // The stable code works for guests again, but they still need approval.
            let mut guest = Conn::new();
            guest
                .send(
                    &server,
                    ClientMessage::JoinRoom { room_code: code, username: "guest".into() },
                )
                .await;
            assert!(guest.drain().is_empty());
            owner.send(&server, ClientMessage::ApproveJoin { user_id: guest.id() }).await;
            assert!(matches!(guest.drain().as_slice(), [ServerMessage::JoinApproved { .. }]));
        }
        std::fs::remove_file(path).unwrap();
    }

    #[tokio::test]
    async fn saved_owner_reconnects_without_stealing_host_or_bypassing_approval() {
        let server = Server::with_store(std::path::Path::new(":memory:")).unwrap();
        let (mut host, mut guest, _) = room_with_guest(&server).await;
        let token = save_host(&server, &mut host).await;
        let code = host.code.clone().unwrap();
        host.send(&server, ClientMessage::TransferHost { user_id: guest.id() }).await;
        let mut back = Conn::new();
        back.send(
            &server,
            ClientMessage::OpenSavedRoom {
                room_code: code.clone(),
                owner_token: token.clone(),
                username: "owner".into(),
            },
        )
        .await;
        assert!(matches!(
            back.drain().as_slice(),
            [ServerMessage::Reconnected { is_host: false, .. }]
        ));
        back.send(&server, ClientMessage::LeaveRoom).await;
        guest.drain();

        let mut returning = Conn::new();
        returning
            .send(
                &server,
                ClientMessage::OpenSavedRoom {
                    room_code: code.clone(),
                    owner_token: token.clone(),
                    username: "owner".into(),
                },
            )
            .await;
        assert!(returning.drain().is_empty(), "explicit leave requires approval again");
        assert!(matches!(guest.drain().as_slice(), [ServerMessage::JoinRequest { .. }]));
        guest.send(&server, ClientMessage::ApproveJoin { user_id: returning.id() }).await;
        returning.drain();
        let mut again = Conn::new();
        again
            .send(
                &server,
                ClientMessage::OpenSavedRoom {
                    room_code: code,
                    owner_token: token,
                    username: "owner".into(),
                },
            )
            .await;
        assert!(matches!(
            again.drain().as_slice(),
            [ServerMessage::Reconnected { is_host: false, .. }]
        ));
        assert_eq!(server.rooms.lock().await[&guest.code.clone().unwrap()].host_id, guest.id());
        guest.send(&server, ClientMessage::SaveRoom).await;
        assert!(
            !guest.drain().iter().any(|m| matches!(m, ServerMessage::RoomSaved { .. })),
            "successor must not receive owner token"
        );
    }

    #[tokio::test]
    async fn failed_storage_does_not_claim_room_is_saved() {
        let server = Server::default(); // no durable store
        let mut host = Conn::new();
        host.send(&server, ClientMessage::CreateRoom { username: "host".into() }).await;
        host.drain();
        host.send(&server, ClientMessage::SaveRoom).await;
        assert!(
            matches!(host.drain().as_slice(), [ServerMessage::Error { code, .. }] if code == "save_failed")
        );
        assert!(!server.rooms.lock().await[&host.code.unwrap()].saved);
    }

    fn dummy_tx() -> (Tx, mpsc::UnboundedReceiver<ServerMessage>) {
        mpsc::unbounded_channel()
    }

    /// One connection as the dispatcher sees it: its channel plus the identity it has taken on.
    struct Conn {
        tx: Tx,
        rx: mpsc::UnboundedReceiver<ServerMessage>,
        uid: Option<String>,
        code: Option<String>,
    }

    impl Conn {
        fn new() -> Self {
            let (tx, rx) = dummy_tx();
            Conn { tx, rx, uid: None, code: None }
        }
        async fn send(&mut self, server: &Server, m: ClientMessage) {
            server.dispatch(m, &self.tx, &mut self.uid, &mut self.code).await;
        }
        fn drain(&mut self) -> Vec<ServerMessage> {
            std::iter::from_fn(|| self.rx.try_recv().ok()).collect()
        }
        fn id(&self) -> String {
            self.uid.clone().unwrap()
        }
    }

    /// A room with an approved guest. Returns (host, guest, guest's session token).
    async fn room_with_guest(server: &Server) -> (Conn, Conn, String) {
        let mut host = Conn::new();
        host.send(server, ClientMessage::CreateRoom { username: "host".into() }).await;
        let mut guest = Conn::new();
        let room_code = host.code.clone().unwrap();
        guest.send(server, ClientMessage::JoinRoom { room_code, username: "guest".into() }).await;
        host.send(server, ClientMessage::ApproveJoin { user_id: guest.id() }).await;
        let token = guest
            .drain()
            .into_iter()
            .find_map(|m| match m {
                ServerMessage::JoinApproved { session_token, .. } => Some(session_token),
                _ => None,
            })
            .expect("guest approved");
        host.drain();
        (host, guest, token)
    }

    /// A join request and a guest add are sent to the host at the time. When the role moves, the
    /// new host has to get them, and a joiner who gives up has to disappear from the host's list.
    #[tokio::test]
    async fn pending_requests_follow_the_host() {
        let server = Server::default();
        let (mut host, mut guest, _) = room_with_guest(&server).await;
        let mut joiner = Conn::new();
        let room_code = host.code.clone().unwrap();
        joiner.send(&server, ClientMessage::JoinRoom { room_code, username: "j".into() }).await;
        host.send(&server, ClientMessage::TransferHost { user_id: guest.id() }).await;
        let got = guest.drain();
        assert!(got.iter().any(|m| matches!(m, ServerMessage::JoinRequest { user_id, .. } if *user_id == joiner.id())), "{got:?}");

        let track = Track {
            id: "vid".into(),
            title: "t".into(),
            artist: "a".into(),
            thumbnail: None,
            duration_ms: 0,
            queued_by: None,
        };
        host.send(&server, ClientMessage::Suggest { track }).await;
        guest.drain();
        // The new host's socket drops before answering; the add must reach it on reconnect.
        let token = server.rooms.lock().await[&guest.code.clone().unwrap()].peers[&guest.id()]
            .session_token
            .clone();
        server.handle_disconnect(guest.uid.clone(), guest.code.clone(), &guest.tx).await;
        let mut back = Conn::new();
        back.send(&server, ClientMessage::Reconnect { session_token: token }).await;
        let got = back.drain();
        assert!(
            got.iter().any(|m| matches!(m, ServerMessage::SuggestionReceived { .. })),
            "{got:?}"
        );

        let joiner_id = joiner.id();
        joiner.send(&server, ClientMessage::LeaveRoom).await;
        let got = back.drain();
        assert!(
            matches!(&got[..], [ServerMessage::UserLeft { user_id }] if *user_id == joiner_id),
            "{got:?}"
        );
    }

    /// A second create on one socket used to make a second room and orphan the first, which then
    /// lived forever with a host marked connected.
    #[tokio::test]
    async fn one_room_per_connection() {
        let server = Server::default();
        let mut c = Conn::new();
        c.send(&server, ClientMessage::CreateRoom { username: "a".into() }).await;
        c.send(&server, ClientMessage::CreateRoom { username: "a".into() }).await;
        assert_eq!(server.rooms.lock().await.len(), 1);
        let got = c.drain();
        assert!(
            matches!(got.last(), Some(ServerMessage::Error { code, .. }) if code == "already_in_room"),
            "{got:?}"
        );
    }

    /// A user who reconnected on a new socket must not be marked gone when the old socket finally
    /// times out, or they lose the host role and, after the grace, their place in the room.
    #[tokio::test]
    async fn old_socket_closing_after_a_reconnect_is_ignored() {
        let server = Server::default();
        let (_host, guest, token) = room_with_guest(&server).await;
        let mut fresh = Conn::new();
        fresh.send(&server, ClientMessage::Reconnect { session_token: token }).await;
        assert!(matches!(fresh.drain()[..], [ServerMessage::Reconnected { .. }]));

        server.handle_disconnect(guest.uid.clone(), guest.code.clone(), &guest.tx).await;
        let rooms = server.rooms.lock().await;
        assert!(rooms[&guest.code.unwrap()].peers[&guest.uid.unwrap()].connected);
    }

    #[tokio::test]
    async fn host_handoff_on_disconnect() {
        let server = Server::default();
        let (htx, _hrx) = dummy_tx();
        let (gtx, _grx) = dummy_tx();

        // Host creates a room.
        let (mut huid, mut hcode) = (None, None);
        server
            .dispatch(
                ClientMessage::CreateRoom { username: "host".into() },
                &htx,
                &mut huid,
                &mut hcode,
            )
            .await;
        let code = hcode.clone().unwrap();
        let host_id = huid.clone().unwrap();

        // Guest joins + host approves.
        let (mut guid, mut gcode) = (None, None);
        server
            .dispatch(
                ClientMessage::JoinRoom { room_code: code.clone(), username: "guest".into() },
                &gtx,
                &mut guid,
                &mut gcode,
            )
            .await;
        let guest_id = guid.clone().unwrap();
        server
            .dispatch(
                ClientMessage::ApproveJoin { user_id: guest_id.clone() },
                &htx,
                &mut huid,
                &mut hcode,
            )
            .await;

        // Host drops: a blip keeps the role, so a reconnect within the grace finds it unchanged.
        server.handle_disconnect(Some(host_id.clone()), Some(code.clone()), &htx).await;
        server.cleanup().await;
        assert_eq!(server.rooms.lock().await[&code].host_id, host_id, "handed off on a blip");

        // Past the grace the role must move to the connected guest (no dead zone).
        server
            .rooms
            .lock()
            .await
            .get_mut(&code)
            .unwrap()
            .peers
            .get_mut(&host_id)
            .unwrap()
            .disconnected_at = Some(Instant::now() - HOST_GRACE - Duration::from_secs(1));
        server.cleanup().await;
        let rooms = server.rooms.lock().await;
        assert_eq!(rooms[&code].host_id, guest_id, "host should hand off to the connected guest");
    }

    /// Full happy path over a real TCP/WebSocket socket: create → join → approve → host broadcast
    /// reaches the guest. Exercises the accept loop, framing, and JSON serde end to end.
    #[tokio::test]
    async fn end_to_end_over_socket() {
        use futures_util::{SinkExt, StreamExt};
        use tokio::net::TcpStream;
        use tokio_tungstenite::{connect_async, MaybeTlsStream, WebSocketStream};

        type Ws = WebSocketStream<MaybeTlsStream<TcpStream>>;
        async fn recv(ws: &mut Ws) -> ServerMessage {
            loop {
                if let Message::Text(t) = ws.next().await.unwrap().unwrap() {
                    return serde_json::from_str(&t).unwrap();
                }
            }
        }
        async fn send(ws: &mut Ws, m: ClientMessage) {
            ws.send(Message::Text(m.to_json())).await.unwrap();
        }

        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let server = Arc::new(Server::default());
        {
            let server = server.clone();
            tokio::spawn(async move {
                while let Ok((stream, _)) = listener.accept().await {
                    tokio::spawn(handle_conn(stream, server.clone()));
                }
            });
        }
        let url = format!("ws://{addr}");

        let (mut host, _) = connect_async(&url).await.unwrap();
        send(&mut host, ClientMessage::CreateRoom { username: "host".into() }).await;
        let code = match recv(&mut host).await {
            ServerMessage::RoomCreated { room_code, .. } => room_code,
            m => panic!("expected RoomCreated, got {m:?}"),
        };

        let (mut guest, _) = connect_async(&url).await.unwrap();
        send(&mut guest, ClientMessage::JoinRoom { room_code: code, username: "guest".into() })
            .await;
        let guest_id = match recv(&mut host).await {
            ServerMessage::JoinRequest { user_id, .. } => user_id,
            m => panic!("expected JoinRequest, got {m:?}"),
        };
        send(&mut host, ClientMessage::ApproveJoin { user_id: guest_id }).await;
        assert!(matches!(recv(&mut guest).await, ServerMessage::JoinApproved { .. }));

        // Host broadcasts a track change → the guest must receive it as SyncPlayback.
        let mut p = Playback::new(PlaybackKind::ChangeTrack);
        p.track = Some(Track {
            id: "vid".into(),
            title: "t".into(),
            artist: "a".into(),
            thumbnail: None,
            duration_ms: 0,
            queued_by: None,
        });
        p.playing = true;
        send(&mut host, ClientMessage::Playback(p)).await;
        match recv(&mut guest).await {
            ServerMessage::SyncPlayback(pb) => assert_eq!(pb.kind, PlaybackKind::ChangeTrack),
            m => panic!("expected SyncPlayback, got {m:?}"),
        }
    }

    #[tokio::test]
    async fn guest_playback_is_rejected() {
        let server = Server::default();
        let (htx, _hrx) = dummy_tx();
        let (gtx, mut grx) = dummy_tx();
        let (mut huid, mut hcode) = (None, None);
        server
            .dispatch(
                ClientMessage::CreateRoom { username: "host".into() },
                &htx,
                &mut huid,
                &mut hcode,
            )
            .await;
        let code = hcode.clone().unwrap();
        let (mut guid, mut gcode) = (None, None);
        server
            .dispatch(
                ClientMessage::JoinRoom { room_code: code.clone(), username: "g".into() },
                &gtx,
                &mut guid,
                &mut gcode,
            )
            .await;
        server
            .dispatch(
                ClientMessage::ApproveJoin { user_id: guid.clone().unwrap() },
                &htx,
                &mut huid,
                &mut hcode,
            )
            .await;
        // drain the JoinApproved
        while grx.try_recv().is_ok() {}
        // Guest tries to control playback → must get a not_host error, not a broadcast.
        server
            .dispatch(
                ClientMessage::Playback(Playback::at(PlaybackKind::Pause, 0)),
                &gtx,
                &mut guid,
                &mut gcode,
            )
            .await;
        let msg = grx.try_recv().expect("guest gets a reply");
        assert!(matches!(msg, ServerMessage::Error { code, .. } if code == "not_host"));
    }

    /// The host makes a room, copies the code and leaves: the room must be gone, and anyone
    /// joining with that code must be told so rather than parked on "waiting for the host".
    #[tokio::test]
    async fn room_closes_when_the_last_member_leaves() {
        let server = Server::default();
        let (htx, _hrx) = dummy_tx();
        let (mut huid, mut hcode) = (None, None);
        server
            .dispatch(
                ClientMessage::CreateRoom { username: "host".into() },
                &htx,
                &mut huid,
                &mut hcode,
            )
            .await;
        let code = hcode.clone().unwrap();
        server.dispatch(ClientMessage::LeaveRoom, &htx, &mut huid, &mut hcode).await;
        assert!(!server.rooms.lock().await.contains_key(&code), "room outlived its last member");

        let (jtx, mut jrx) = dummy_tx();
        let (mut juid, mut jcode) = (None, None);
        server
            .dispatch(
                ClientMessage::JoinRoom { room_code: code, username: "joiner".into() },
                &jtx,
                &mut juid,
                &mut jcode,
            )
            .await;
        let msg = jrx.try_recv().expect("joiner must get an answer, not silence");
        assert!(matches!(msg, ServerMessage::Error { code, .. } if code == "room_not_found"));
    }

    /// Same symptom, other route: the host's socket dropped and their slot is still in its
    /// reconnect grace window, so the room exists but nobody can approve a join.
    #[tokio::test]
    async fn join_is_refused_while_the_host_is_offline() {
        let server = Server::default();
        let (htx, _hrx) = dummy_tx();
        let (mut huid, mut hcode) = (None, None);
        server
            .dispatch(
                ClientMessage::CreateRoom { username: "host".into() },
                &htx,
                &mut huid,
                &mut hcode,
            )
            .await;
        let code = hcode.clone().unwrap();
        server.handle_disconnect(huid.clone(), hcode.clone(), &htx).await;

        let (jtx, mut jrx) = dummy_tx();
        let (mut juid, mut jcode) = (None, None);
        server
            .dispatch(
                ClientMessage::JoinRoom { room_code: code, username: "joiner".into() },
                &jtx,
                &mut juid,
                &mut jcode,
            )
            .await;
        let msg = jrx.try_recv().expect("joiner must get an answer, not silence");
        assert!(matches!(msg, ServerMessage::Error { code, .. } if code == "host_unavailable"));
    }

    /// A joiner still knocking when the host closes the room gets turned away, not stranded.
    #[tokio::test]
    async fn pending_joiner_is_told_when_the_host_closes_the_room() {
        let server = Server::default();
        let (htx, _hrx) = dummy_tx();
        let (mut huid, mut hcode) = (None, None);
        server
            .dispatch(
                ClientMessage::CreateRoom { username: "host".into() },
                &htx,
                &mut huid,
                &mut hcode,
            )
            .await;
        let code = hcode.clone().unwrap();

        let (jtx, mut jrx) = dummy_tx();
        let (mut juid, mut jcode) = (None, None);
        server
            .dispatch(
                ClientMessage::JoinRoom { room_code: code.clone(), username: "joiner".into() },
                &jtx,
                &mut juid,
                &mut jcode,
            )
            .await;
        server.dispatch(ClientMessage::LeaveRoom, &htx, &mut huid, &mut hcode).await;
        let msg = jrx.try_recv().expect("pending joiner must be told the room is gone");
        assert!(matches!(msg, ServerMessage::JoinRejected { .. }));
        assert!(!server.rooms.lock().await.contains_key(&code));
    }
}
