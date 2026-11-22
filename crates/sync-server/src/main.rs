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
    ClientMessage, Playback, PlaybackKind, RoomState, ServerMessage, Suggestion, Track, User,
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
/// Device link rooms held at once. Counted apart from [`MAX_ROOMS`], so linked desktops can never
/// lock anyone out of Listen Together. ponytail: a link room holds up to a 50-track queue
/// (~40 KiB) and a [`MAX_LINK_STATUS`] status, so the worst case is ~210 MB on the owner's laptop;
/// raise it only with a bigger host.
const MAX_LINK_ROOMS: usize = 2000;
/// Largest `link_status` kept. The player's queue window (~120 rows) is about half of it.
const MAX_LINK_STATUS: usize = 64 * 1024;
const MAX_LINK_DEVICES: usize = 8;
/// A link room with no device connected is forgotten after this. Its devices just join again.
const LINK_IDLE_TTL: Duration = Duration::from_secs(12 * 60 * 60);
/// How long a pairing code can be claimed.
const PAIR_TTL: Duration = Duration::from_secs(120);
/// Pairing claims one connection may make, right or wrong.
const MAX_PAIR_CLAIMS: u8 = 3;
/// Link rooms live in the same map as Listen Together rooms, under this prefix plus the secret.
/// No room code contains `:`, so the two can't collide.
const LINK_PREFIX: &str = "link:";
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
    /// In a link room: the device that is playing, or played last (`""` before anyone has).
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
    /// A device link room: keyed by a secret, no approval step, host = the playing device.
    link: bool,
    /// Link rooms: since when no device has been connected (set by the sweep).
    idle_since: Option<Instant>,
    /// Link rooms: the live pairing code and when it expires. One per room.
    pair: Option<(String, Instant)>,
    /// Link rooms: the player's last `link_status` (its queue), kept unread for devices that join.
    status: Option<serde_json::Value>,
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

    fn empty(link: bool) -> Room {
        Room {
            recovery_epoch: gen_token(),
            recovery_endpoints: HashMap::new(),
            approved_guests: HashSet::new(),
            saved: false,
            owner_id: None,
            host_id: String::new(),
            peers: HashMap::new(),
            pending: HashMap::new(),
            suggestions: HashMap::new(),
            current_track: None,
            is_playing: false,
            position_ms: 0,
            last_update_ms: 0,
            volume: 1.0,
            queue: Vec::new(),
            link,
            idle_since: None,
            pair: None,
            status: None,
        }
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
            supports_recovery: !self.link,
            supports_saved_rooms: !self.link,
            saved: self.saved,
            // A link room's key holds its secret: never echo it.
            room_code: if self.link { String::new() } else { code.to_string() },
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
        // A new player's queue is its own, which it sends next.
        if self.host_id != id {
            self.status = None;
        }
        self.host_id = id.clone();
        self.broadcast(&ServerMessage::HostChanged { host_id: id }, None);
        self.brief_host();
        self.broadcast_recovery();
    }

    fn broadcast_recovery(&self) {
        if self.recovery_endpoints.is_empty() {
            return;
        }
        let peers = self
            .recovery_endpoints
            .iter()
            .filter_map(|(id, endpoint)| {
                let p = self.peers.get(id)?;
                Some(listen_protocol::RecoveryPeer {
                    user_id: id.clone(),
                    username: p.username.clone(),
                    endpoint_id: endpoint.clone(),
                    is_host: self.host_id == *id,
                })
            })
            .collect();
        self.broadcast(
            &ServerMessage::RecoveryPeers { epoch: self.recovery_epoch.clone(), peers },
            None,
        );
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

    /// Stop the clock where the music is now: the position becomes the live one, paused.
    fn freeze(&mut self) {
        let now = now_ms();
        self.position_ms = RoomState {
            is_playing: self.is_playing,
            position_ms: self.position_ms,
            last_update_ms: self.last_update_ms,
            ..Default::default()
        }
        .live_position_ms(now);
        self.is_playing = false;
        self.last_update_ms = now;
    }

    /// Link rooms: the player is gone for good (unlinked or left), so nobody is playing.
    fn clear_host(&mut self) {
        self.host_id.clear();
        self.freeze();
    }

    /// The pairing code, if it hasn't expired.
    fn live_pair(&self, now: Instant) -> Option<&str> {
        self.pair.as_ref().filter(|(_, until)| *until > now).map(|(c, _)| c.as_str())
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

fn gen_code(len: usize) -> String {
    let mut rng = rand::thread_rng();
    (0..len).map(|_| CODE_ALPHABET[rng.gen_range(0..CODE_ALPHABET.len())] as char).collect()
}

fn gen_user_id() -> String {
    let nanos = SystemTime::now().duration_since(UNIX_EPOCH).map(|d| d.as_nanos()).unwrap_or(0);
    format!("user_{}_{}", nanos, rand::thread_rng().gen_range(0..10000))
}

fn gen_token() -> String {
    let bytes: [u8; 16] = rand::thread_rng().gen();
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// A link secret: 32 random bytes in base64url without padding is 43 chars; allow up to 64.
fn valid_secret(s: &str) -> bool {
    (43..=64).contains(&s.len())
        && s.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'-' || b == b'_')
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
    /// updated in place as it creates/joins/reconnects. `claims` counts its pairing claims.
    async fn dispatch(
        &self,
        mut cm: ClientMessage,
        tx: &Tx,
        uid: &mut Option<String>,
        room_code: &mut Option<String>,
        claims: &mut u8,
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
                    | ClientMessage::JoinLink { .. }
                    | ClientMessage::PairClaim { .. }
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
                if room.link {
                    let _ =
                        tx.send(err("wrong_room_type", "Device links cannot be saved as rooms."));
                    return;
                }
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
                if rooms.values().filter(|r| !r.link).count() >= MAX_ROOMS {
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
                        let c = gen_code(8);
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
                    ..Room::empty(false)
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
                let Some(room) = rooms.get_mut(&code).filter(|r| !r.link) else {
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
                    if !room.link {
                        let _ = tx.send(err("not_host", "Only the host controls playback."));
                        return;
                    }
                    // Device link: a device that starts playing takes the room over, and the old
                    // player pauses when it hears `host_changed`. Anything else from a device
                    // that isn't playing is dropped without a word.
                    let takeover = p.kind == PlaybackKind::ChangeTrack && p.playing;
                    if !takeover || !room.peers.contains_key(&me) {
                        return;
                    }
                    room.set_host(me.clone());
                }
                if let Some(q) = p.queue.as_mut() {
                    q.truncate(if room.link { listen_protocol::MAX_LINK_QUEUE } else { MAX_QUEUE });
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
                // In a link room this is Unlink, and any linked device may do it.
                let allowed =
                    if room.link { room.peers.contains_key(&me) } else { room.is_host(&me) };
                if !allowed || target == me {
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
                    let reason =
                        if room.link { "unlinked" } else { "The host removed you from the room." };
                    let _ = p.tx.send(ServerMessage::Kicked { reason: reason.into() });
                    if room.host_id == target {
                        room.clear_host();
                    }
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
                // Find the room + user owning this token. Listen Together rooms only: a link
                // device's slot is reached by its secret, never by a token.
                let found = rooms.iter_mut().filter(|(_, r)| !r.link).find_map(|(code, room)| {
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
                if room.link {
                    return;
                }
                if !room.peers.get(me).is_some_and(|p| p.connected && p.tx.same_channel(tx)) {
                    return; // Pending joiners cannot discover or advertise member endpoints.
                }
                if endpoint_id.len() != 64
                    || !endpoint_id.bytes().all(|b| b.is_ascii_hexdigit())
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
                if room.link || !room.is_host(me) || room.peers.len() != 1 {
                    let _ = tx.send(err(
                        "recovery_failed",
                        "Only an empty room can receive a recovered playlist.",
                    ));
                    return;
                }
                if guest_tokens.len() >= MAX_USERS_PER_ROOM
                    || guest_tokens
                        .iter()
                        .any(|t| t.len() != 64 || !t.bytes().all(|b| b.is_ascii_hexdigit()))
                {
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
                    let _ = tx.send(err(
                        "recovery_failed",
                        "Could not save recovery permissions. Try again.",
                    ));
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

            ClientMessage::JoinLink { secret, device_id, name } => {
                if !valid_secret(&secret) {
                    let _ = tx.send(err("bad_secret", "That link isn't valid."));
                    return;
                }
                let key = format!("{LINK_PREFIX}{secret}");
                let device_id = sanitize(&device_id);
                let name = sanitize(&name);
                let mut rooms = self.rooms.lock().await;
                if !rooms.contains_key(&key) {
                    if rooms.values().filter(|r| r.link).count() >= MAX_LINK_ROOMS {
                        let _ = tx.send(err("server_full", "The server is full, try again later."));
                        return;
                    }
                    rooms.insert(key.clone(), Room::empty(true));
                }
                let room = rooms.get_mut(&key).unwrap();
                if let Some(peer) = room.peers.get_mut(&device_id) {
                    // The same device on a new socket: the old one's close is ignored later
                    // (`handle_disconnect` checks the channel).
                    peer.tx = tx.clone();
                    peer.connected = true;
                    peer.disconnected_at = None;
                    peer.username = name;
                    room.broadcast(
                        &ServerMessage::UserReconnected { user_id: device_id.clone() },
                        Some(&device_id),
                    );
                } else {
                    if room.peers.len() >= MAX_LINK_DEVICES {
                        let _ = tx.send(err("room_full", "Too many devices are linked."));
                        return;
                    }
                    room.peers.insert(
                        device_id.clone(),
                        Peer {
                            guest_token: None,
                            username: name.clone(),
                            tx: tx.clone(),
                            connected: true,
                            // Never sent, and `Reconnect` skips link rooms; an empty token here
                            // would let `reconnect{session_token:""}` take this slot.
                            session_token: gen_token(),
                            disconnected_at: None,
                        },
                    );
                    let user = User {
                        user_id: device_id.clone(),
                        username: name,
                        is_host: room.is_host(&device_id),
                        is_connected: true,
                    };
                    room.broadcast(&ServerMessage::UserJoined { user }, Some(&device_id));
                }
                let now = now_ms();
                let mut state = room.wire_state(&key);
                state.position_ms = state.live_position_ms(now);
                state.last_update_ms = now;
                let _ = tx.send(ServerMessage::LinkJoined { state, status: room.status.clone() });
                *uid = Some(device_id);
                *room_code = Some(key);
            }

            ClientMessage::Remote { action, to } => {
                let (Some(me), Some(code)) = (uid.clone(), room_code.clone()) else { return };
                let rooms = self.rooms.lock().await;
                let Some(room) = rooms.get(&code) else { return };
                if !room.link || !room.peers.contains_key(&me) {
                    return;
                }
                // Forwarded, not applied (nor read): the device runs it and publishes the result.
                let named = to.is_some();
                let target = to.unwrap_or_else(|| room.host_id.clone());
                if target == me || !room.peers.get(&target).is_some_and(|p| p.connected) {
                    let _ = tx.send(if named {
                        err("no_device", "That device isn't online.")
                    } else {
                        err("no_player", "No other device is playing right now.")
                    });
                    return;
                }
                room.send_to(&target, ServerMessage::Remote { from: me, action });
            }

            ClientMessage::LinkStatus { status } => {
                let (Some(me), Some(code)) = (uid.clone(), room_code.clone()) else { return };
                let mut rooms = self.rooms.lock().await;
                let Some(room) = rooms.get_mut(&code) else { return };
                // Only the player's queue is the room's; a takeover's status follows its track.
                if !room.link || !room.is_host(&me) || !room.peers.contains_key(&me) {
                    return;
                }
                if status.to_string().len() > MAX_LINK_STATUS {
                    let _ = tx.send(err("too_large", "The queue is too big to share."));
                    return;
                }
                room.status = Some(status.clone());
                room.broadcast(&ServerMessage::LinkStatus { status }, Some(&me));
            }

            ClientMessage::PairStart => {
                let (Some(me), Some(code)) = (uid.clone(), room_code.clone()) else { return };
                let mut rooms = self.rooms.lock().await;
                if !rooms.get(&code).is_some_and(|r| r.link && r.peers.contains_key(&me)) {
                    return;
                }
                let now = Instant::now();
                let pair = loop {
                    let c = gen_code(6);
                    if !rooms.values().any(|r| r.live_pair(now) == Some(c.as_str())) {
                        break c;
                    }
                };
                // A new code replaces the room's old one.
                rooms.get_mut(&code).unwrap().pair = Some((pair.clone(), now + PAIR_TTL));
                let _ = tx.send(ServerMessage::PairCode {
                    code: pair,
                    expires_in_s: PAIR_TTL.as_secs() as u32,
                });
            }

            ClientMessage::PairClaim { code } => {
                // ponytail: per-connection limit only; Tailscale Funnel gives every client the same IP, so a global claim rate is the upgrade if anyone brute-forces codes.
                if *claims >= MAX_PAIR_CLAIMS {
                    return;
                }
                *claims += 1;
                let code = code.trim().to_uppercase();
                let now = Instant::now();
                let mut rooms = self.rooms.lock().await;
                match rooms.iter_mut().find(|(_, r)| r.live_pair(now) == Some(code.as_str())) {
                    Some((key, room)) => {
                        room.pair = None; // single use
                        let secret = key[LINK_PREFIX.len()..].to_string();
                        let _ = tx.send(ServerMessage::Paired { secret });
                    }
                    None => {
                        let _ = tx.send(err("pair_code_invalid", "That code is wrong or expired."));
                    }
                }
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
        // Link rooms: no host handoff (the other devices aren't playing), and the room goes once
        // no device is left in it.
        if room.link {
            if room.peers.remove(me).is_some() {
                room.broadcast(&ServerMessage::UserLeft { user_id: me.to_string() }, None);
            }
            if room.host_id == me {
                room.clear_host();
            }
            if room.peers.is_empty() {
                rooms.remove(code);
            }
            return;
        }
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
        // A link player that drops stops the music where it is. No `HOST_GRACE` handoff in link
        // rooms: the other devices aren't playing.
        if room.link && room.is_host(&me) && room.is_playing {
            room.freeze();
            let pause = Playback::at(PlaybackKind::Pause, room.position_ms);
            room.broadcast(&ServerMessage::SyncPlayback(pause), Some(&me));
        }
    }

    /// Sweep expired reconnection slots and empty rooms, and move the host role off a host who
    /// has been gone longer than [`HOST_GRACE`]. Link rooms instead keep their devices (the list
    /// is the user's devices), drop expired pairing codes, and go after [`LINK_IDLE_TTL`] with no
    /// device connected.
    async fn cleanup(&self, now: Instant) {
        let mut rooms = self.rooms.lock().await;
        let codes: Vec<String> = rooms.keys().cloned().collect();
        for code in codes {
            if rooms[&code].link {
                let room = rooms.get_mut(&code).unwrap();
                if room.live_pair(now).is_none() {
                    room.pair = None;
                }
                if room.peers.values().any(|p| p.connected) {
                    room.idle_since = None;
                } else if now - *room.idle_since.get_or_insert(now) > LINK_IDLE_TTL {
                    rooms.remove(&code);
                    tracing::info!("link room closed (idle)");
                }
                continue;
            }
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
    let mut claims = 0u8;

    // Clients ping every 25s, so this much silence is a dead socket. Without a limit a half-open
    // one (the client slept or changed networks) holds its slot forever.
    while let Ok(Some(next)) = tokio::time::timeout(IDLE_LIMIT, read.next()).await {
        match next {
            Ok(Message::Text(t)) => match serde_json::from_str::<ClientMessage>(&t) {
                Ok(cm) => server.dispatch(cm, &tx, &mut uid, &mut room_code, &mut claims).await,
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
                server.cleanup(Instant::now()).await;
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
        pending
            .send(
                &server,
                ClientMessage::JoinRoom { room_code: code.clone(), username: "Pending".into() },
            )
            .await;
        host.drain();
        guest.drain();
        pending
            .send(&server, ClientMessage::RegisterRecovery { endpoint_id: "c".repeat(64) })
            .await;
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
        assert!(guest
            .drain()
            .iter()
            .any(|m| matches!(m, ServerMessage::RecoveryPeers { peers, .. }
            if peers.len() == 1 && peers[0].user_id == guest.id())));
        assert!(!server.rooms.lock().await[&code].recovery_endpoints.contains_key(&host.id()));
    }

    fn recovery_playlist() -> RoomState {
        let track = |n: usize| Track {
            id: format!("{n:011}"),
            title: format!("Track {n}"),
            artist: "Artist".into(),
            thumbnail: None,
            duration_ms: 240_000,
            queued_by: None,
        };
        RoomState {
            current_track: Some(track(0)),
            queue: (1..=137).map(track).collect(),
            position_ms: 32_100,
            is_playing: false,
            ..Default::default()
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
            host.send(
                &server,
                ClientMessage::SeedRecovery {
                    state: recovery_playlist(),
                    guest_tokens: vec![guest_token.clone()],
                },
            )
            .await;
            assert!(matches!(host.drain().as_slice(), [ServerMessage::RecoveryReady { .. }]));
            let mut guest = Conn::new();
            guest
                .send(
                    &server,
                    ClientMessage::JoinSavedRoom {
                        room_code: code.clone(),
                        username: "Guest".into(),
                        guest_token: guest_token.clone(),
                    },
                )
                .await;
            let messages = guest.drain();
            let state = messages
                .iter()
                .find_map(|m| {
                    if let ServerMessage::JoinApproved { state, .. } = m {
                        Some(state)
                    } else {
                        None
                    }
                })
                .unwrap();
            assert_eq!(
                state.queue,
                recovery_playlist().queue,
                "more than fifty upcoming tracks survive"
            );
            assert_eq!(state.current_track, recovery_playlist().current_track);
            assert_eq!(state.position_ms, 32_100);
            assert!(!state.is_playing, "paused playback stays paused");
            // A guest cannot overwrite playback or issue recovery permissions.
            guest
                .send(
                    &server,
                    ClientMessage::SeedRecovery {
                        state: RoomState::default(),
                        guest_tokens: vec!["e".repeat(64)],
                    },
                )
                .await;
            assert!(matches!(guest.drain().as_slice(), [ServerMessage::Error { .. }]));
            assert_eq!(server.rooms.lock().await[&code].queue.len(), 137);
        }
        {
            let server = Server::with_store(&path).unwrap();
            let mut host = Conn::new();
            host.send(
                &server,
                ClientMessage::OpenSavedRoom {
                    room_code: code.clone(),
                    owner_token,
                    username: "Successor".into(),
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
                        username: "Guest".into(),
                        guest_token,
                    },
                )
                .await;
            assert!(matches!(guest.drain().as_slice(), [ServerMessage::JoinApproved { .. }]));
            let mut outsider = Conn::new();
            outsider
                .send(
                    &server,
                    ClientMessage::JoinSavedRoom {
                        room_code: code.clone(),
                        username: "Guest".into(),
                        guest_token: "f".repeat(64),
                    },
                )
                .await;
            assert!(outsider.drain().is_empty(), "names and unknown passes cannot bypass approval");
        }
        std::fs::remove_file(path).unwrap();
    }

    #[tokio::test]
    async fn recovery_storage_failure_does_not_publish_invites_or_replace_state() {
        let path =
            std::env::temp_dir().join(format!("limusic-recovery-failure-{}.sqlite3", gen_token()));
        {
            let server = Server::with_store(&path).unwrap();
            let mut host = Conn::new();
            host.send(&server, ClientMessage::CreateRoom { username: "Host".into() }).await;
            host.drain();
            save_host(&server, &mut host).await;
            server
                .saved
                .lock()
                .unwrap()
                .as_ref()
                .unwrap()
                .execute("DROP TABLE approved_guests", [])
                .unwrap();
            host.send(
                &server,
                ClientMessage::SeedRecovery {
                    state: recovery_playlist(),
                    guest_tokens: vec!["d".repeat(64)],
                },
            )
            .await;
            assert!(
                matches!(host.drain().as_slice(), [ServerMessage::Error { code, .. }] if code == "recovery_failed")
            );
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
        claims: u8,
    }

    impl Conn {
        fn new() -> Self {
            let (tx, rx) = dummy_tx();
            Conn { tx, rx, uid: None, code: None, claims: 0 }
        }
        async fn send(&mut self, server: &Server, m: ClientMessage) {
            server.dispatch(m, &self.tx, &mut self.uid, &mut self.code, &mut self.claims).await;
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
                &mut 0,
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
                &mut 0,
            )
            .await;
        let guest_id = guid.clone().unwrap();
        server
            .dispatch(
                ClientMessage::ApproveJoin { user_id: guest_id.clone() },
                &htx,
                &mut huid,
                &mut hcode,
                &mut 0,
            )
            .await;

        // Host drops: a blip keeps the role, so a reconnect within the grace finds it unchanged.
        server.handle_disconnect(Some(host_id.clone()), Some(code.clone()), &htx).await;
        server.cleanup(Instant::now()).await;
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
        server.cleanup(Instant::now()).await;
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
                &mut 0,
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
                &mut 0,
            )
            .await;
        server
            .dispatch(
                ClientMessage::ApproveJoin { user_id: guid.clone().unwrap() },
                &htx,
                &mut huid,
                &mut hcode,
                &mut 0,
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
                &mut 0,
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
                &mut 0,
            )
            .await;
        let code = hcode.clone().unwrap();
        server.dispatch(ClientMessage::LeaveRoom, &htx, &mut huid, &mut hcode, &mut 0).await;
        assert!(!server.rooms.lock().await.contains_key(&code), "room outlived its last member");

        let (jtx, mut jrx) = dummy_tx();
        let (mut juid, mut jcode) = (None, None);
        server
            .dispatch(
                ClientMessage::JoinRoom { room_code: code, username: "joiner".into() },
                &jtx,
                &mut juid,
                &mut jcode,
                &mut 0,
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
                &mut 0,
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
                &mut 0,
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
                &mut 0,
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
                &mut 0,
            )
            .await;
        server.dispatch(ClientMessage::LeaveRoom, &htx, &mut huid, &mut hcode, &mut 0).await;
        let msg = jrx.try_recv().expect("pending joiner must be told the room is gone");
        assert!(matches!(msg, ServerMessage::JoinRejected { .. }));
        assert!(!server.rooms.lock().await.contains_key(&code));
    }

    // Device link

    const SECRET: &str = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQ";
    const KEY: &str = "link:abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQ";

    async fn link(server: &Server, device: &str) -> Conn {
        let mut c = Conn::new();
        let (secret, device_id, name) = (SECRET.into(), device.into(), device.into());
        c.send(server, ClientMessage::JoinLink { secret, device_id, name }).await;
        c
    }

    /// Two linked devices with their join chatter drained.
    async fn two_devices(server: &Server) -> (Conn, Conn) {
        let mut a = link(server, "a").await;
        let mut b = link(server, "b").await;
        a.drain();
        b.drain();
        (a, b)
    }

    fn play(id: &str) -> ClientMessage {
        let mut p = Playback::at(PlaybackKind::ChangeTrack, 1000);
        p.track = Some(Track {
            id: id.into(),
            title: "t".into(),
            artist: "a".into(),
            thumbnail: None,
            duration_ms: 0,
            queued_by: None,
        });
        p.playing = true;
        ClientMessage::Playback(p)
    }

    fn error_code(got: &[ServerMessage]) -> Option<&str> {
        got.iter().find_map(|m| match m {
            ServerMessage::Error { code, .. } => Some(code.as_str()),
            _ => None,
        })
    }

    #[tokio::test]
    async fn device_links_cannot_use_saved_room_storage() {
        let server = Server::default();
        let mut device = link(&server, "desktop").await;
        let messages = device.drain();
        assert!(matches!(messages.as_slice(), [ServerMessage::LinkJoined { state, .. }]
            if !state.supports_saved_rooms && !state.supports_recovery));
        device.send(&server, play("track")).await;
        device.drain();
        device.send(&server, ClientMessage::SaveRoom).await;
        assert_eq!(error_code(&device.drain()), Some("wrong_room_type"));
    }

    #[tokio::test]
    async fn link_join_creates_the_room_and_lists_devices() {
        let server = Server::default();
        let mut a = link(&server, "a").await;
        match &a.drain()[..] {
            [ServerMessage::LinkJoined { state, .. }] => {
                assert_eq!(state.room_code, "", "the secret must not be echoed");
                assert_eq!(state.host_id, "");
                assert_eq!(state.users.len(), 1);
            }
            got => panic!("{got:?}"),
        }
        let mut b = link(&server, "b").await;
        match &b.drain()[..] {
            [ServerMessage::LinkJoined { state, .. }] => {
                let mut ids: Vec<_> = state.users.iter().map(|u| u.user_id.as_str()).collect();
                ids.sort();
                assert_eq!(ids, ["a", "b"]);
            }
            got => panic!("{got:?}"),
        }
        let got = a.drain();
        assert!(
            matches!(&got[..], [ServerMessage::UserJoined { user }] if user.user_id == "b"),
            "{got:?}"
        );

        let mut bad = Conn::new();
        let (device_id, name) = ("c".into(), "c".into());
        bad.send(&server, ClientMessage::JoinLink { secret: "short".into(), device_id, name })
            .await;
        assert_eq!(error_code(&bad.drain()), Some("bad_secret"));
        assert!(bad.uid.is_none());

        // A link room can't be entered as a Listen Together room.
        let mut lt = Conn::new();
        let room_code = KEY.to_string();
        lt.send(&server, ClientMessage::JoinRoom { room_code, username: "x".into() }).await;
        assert_eq!(error_code(&lt.drain()), Some("room_not_found"));
    }

    #[tokio::test]
    async fn rejoining_with_the_same_device_id_replaces_the_slot() {
        let server = Server::default();
        let (a, mut b) = two_devices(&server).await;
        let mut a2 = link(&server, "a").await;
        assert!(matches!(a2.drain()[..], [ServerMessage::LinkJoined { .. }]));
        let got = b.drain();
        assert!(
            matches!(&got[..], [ServerMessage::UserReconnected { user_id }] if user_id == "a"),
            "{got:?}"
        );
        // The old socket closing late must not mark the device gone.
        server.handle_disconnect(a.uid.clone(), a.code.clone(), &a.tx).await;
        let rooms = server.rooms.lock().await;
        assert_eq!(rooms[KEY].peers.len(), 2);
        assert!(rooms[KEY].peers["a"].connected);
    }

    #[tokio::test]
    async fn playing_takes_the_room_over() {
        let server = Server::default();
        let (mut a, mut b) = two_devices(&server).await;
        a.send(&server, play("one")).await;
        let got = b.drain();
        assert!(
            matches!(&got[..], [ServerMessage::HostChanged { host_id }, ServerMessage::SyncPlayback(p)]
                if host_id == "a" && p.kind == PlaybackKind::ChangeTrack),
            "{got:?}"
        );
        assert!(
            matches!(&a.drain()[..], [ServerMessage::HostChanged { host_id }] if host_id == "a")
        );

        b.send(&server, play("two")).await;
        let got = a.drain();
        assert!(
            matches!(&got[..], [ServerMessage::HostChanged { host_id }, ServerMessage::SyncPlayback(_)] if host_id == "b"),
            "{got:?}"
        );
        let rooms = server.rooms.lock().await;
        assert_eq!(rooms[KEY].host_id, "b");
        assert_eq!(rooms[KEY].current_track.as_ref().unwrap().id, "two");
    }

    #[tokio::test]
    async fn a_device_that_isnt_playing_cant_change_the_room() {
        let server = Server::default();
        let (mut a, mut b) = two_devices(&server).await;
        a.send(&server, play("one")).await;
        a.drain();
        b.drain();

        let ClientMessage::Playback(mut paused) = play("two") else { unreachable!() };
        paused.playing = false;
        b.send(&server, ClientMessage::Playback(paused)).await;
        b.send(&server, ClientMessage::Playback(Playback::at(PlaybackKind::Pause, 0))).await;
        assert!(a.drain().is_empty());
        assert!(b.drain().is_empty(), "dropped silently, no not_host");
        let rooms = server.rooms.lock().await;
        assert_eq!(rooms[KEY].host_id, "a");
        assert!(rooms[KEY].is_playing);
        assert_eq!(rooms[KEY].current_track.as_ref().unwrap().id, "one");
    }

    #[tokio::test]
    async fn remote_reaches_only_the_player() {
        let server = Server::default();
        let (mut a, mut b) = two_devices(&server).await;
        let mut c = link(&server, "c").await;
        c.drain();
        let next =
            || ClientMessage::Remote { action: serde_json::json!({"kind": "next"}), to: None };

        // Nobody has played yet.
        b.send(&server, next()).await;
        assert_eq!(error_code(&b.drain()), Some("no_player"));

        a.send(&server, play("one")).await;
        a.drain();
        b.drain();
        c.drain();
        b.send(&server, next()).await;
        let got = a.drain();
        assert!(
            matches!(&got[..], [ServerMessage::Remote { from, action }] if from == "b" && action["kind"] == "next"),
            "{got:?}"
        );
        assert!(b.drain().is_empty());
        assert!(c.drain().is_empty());

        // The player can't remote itself.
        a.send(&server, next()).await;
        assert_eq!(error_code(&a.drain()), Some("no_player"));

        server.handle_disconnect(a.uid.clone(), a.code.clone(), &a.tx).await;
        b.drain();
        b.send(&server, next()).await;
        assert_eq!(error_code(&b.drain()), Some("no_player"));
    }

    #[tokio::test]
    async fn remote_to_a_named_device() {
        let server = Server::default();
        let (mut a, mut b) = two_devices(&server).await;
        let take_over = |to: &str| ClientMessage::Remote {
            action: serde_json::json!({"kind": "take_over"}),
            to: Some(to.into()),
        };
        a.send(&server, play("one")).await;
        a.drain();
        b.drain();
        // The player asks a device that isn't playing to take over.
        a.send(&server, take_over("b")).await;
        let got = b.drain();
        assert!(
            matches!(&got[..], [ServerMessage::Remote { from, action }] if from == "a" && action["kind"] == "take_over"),
            "{got:?}"
        );
        assert!(a.drain().is_empty());
        // Not to itself, nor to a device that is offline or unknown.
        a.send(&server, take_over("a")).await;
        assert_eq!(error_code(&a.drain()), Some("no_device"));
        a.send(&server, take_over("z")).await;
        assert_eq!(error_code(&a.drain()), Some("no_device"));
        server.handle_disconnect(b.uid.clone(), b.code.clone(), &b.tx).await;
        a.drain();
        a.send(&server, take_over("b")).await;
        assert_eq!(error_code(&a.drain()), Some("no_device"));
    }

    #[tokio::test]
    async fn link_status_is_the_players_and_kept_for_joiners() {
        let server = Server::default();
        let (mut a, mut b) = two_devices(&server).await;
        let status =
            |n: u64| ClientMessage::LinkStatus { status: serde_json::json!({"current": n}) };

        // Nobody plays yet: nobody's queue is the room's.
        a.send(&server, status(0)).await;
        assert!(b.drain().is_empty());

        a.send(&server, play("one")).await;
        a.drain();
        b.drain();
        a.send(&server, status(1)).await;
        let got = b.drain();
        assert!(
            matches!(&got[..], [ServerMessage::LinkStatus { status }] if status["current"] == 1),
            "{got:?}"
        );
        assert!(a.drain().is_empty(), "not echoed to the player");
        // A device that isn't playing can't overwrite it.
        b.send(&server, status(9)).await;
        assert!(a.drain().is_empty());

        // A device that joins gets it with the room.
        let mut c = link(&server, "c").await;
        match &c.drain()[..] {
            [ServerMessage::LinkJoined { status: Some(s), .. }] => assert_eq!(s["current"], 1),
            got => panic!("{got:?}"),
        }

        // Too big is refused and the last one stands.
        let big = "x".repeat(MAX_LINK_STATUS);
        a.send(&server, ClientMessage::LinkStatus { status: serde_json::json!({"pad": big}) })
            .await;
        assert_eq!(error_code(&a.drain()), Some("too_large"));
        assert_eq!(server.rooms.lock().await[KEY].status.as_ref().unwrap()["current"], 1);

        // A takeover drops the old player's queue.
        b.send(&server, play("two")).await;
        assert!(server.rooms.lock().await[KEY].status.is_none());
    }

    async fn pair_code(server: &Server, c: &mut Conn) -> String {
        c.send(server, ClientMessage::PairStart).await;
        match &c.drain()[..] {
            [ServerMessage::PairCode { code, expires_in_s: 120 }] => {
                assert_eq!(code.len(), 6);
                code.clone()
            }
            got => panic!("{got:?}"),
        }
    }

    async fn claim(server: &Server, c: &mut Conn, code: &str) -> Vec<ServerMessage> {
        c.send(server, ClientMessage::PairClaim { code: code.into() }).await;
        c.drain()
    }

    #[tokio::test]
    async fn pair_codes_are_single_use_and_expire() {
        let server = Server::default();
        let mut a = link(&server, "a").await;
        a.drain();

        // Not a link member: no code.
        let mut outsider = Conn::new();
        outsider.send(&server, ClientMessage::PairStart).await;
        assert!(outsider.drain().is_empty());

        let code = pair_code(&server, &mut a).await;
        let got = claim(&server, &mut Conn::new(), &format!("  {} ", code.to_lowercase())).await;
        assert!(
            matches!(&got[..], [ServerMessage::Paired { secret }] if secret == SECRET),
            "{got:?}"
        );
        let got = claim(&server, &mut Conn::new(), &code).await;
        assert_eq!(error_code(&got), Some("pair_code_invalid"), "single use");

        // A new code replaces the old one.
        let old = pair_code(&server, &mut a).await;
        let new = pair_code(&server, &mut a).await;
        if old != new {
            let got = claim(&server, &mut Conn::new(), &old).await;
            assert_eq!(error_code(&got), Some("pair_code_invalid"));
        }

        // Expired.
        server.rooms.lock().await.get_mut(KEY).unwrap().pair.as_mut().unwrap().1 =
            Instant::now() - Duration::from_secs(1);
        let got = claim(&server, &mut Conn::new(), &new).await;
        assert_eq!(error_code(&got), Some("pair_code_invalid"));

        // Three claims per connection, then silence, even for the right code.
        let code = pair_code(&server, &mut a).await;
        let mut guesser = Conn::new();
        for _ in 0..3 {
            let got = claim(&server, &mut guesser, "AAAAAA").await;
            assert_eq!(error_code(&got), Some("pair_code_invalid"));
        }
        assert!(claim(&server, &mut guesser, &code).await.is_empty());

        // Only from a connection that isn't in a room.
        let got = claim(&server, &mut a, &code).await;
        assert_eq!(error_code(&got), Some("already_in_room"));
    }

    #[tokio::test]
    async fn a_player_that_drops_freezes_the_position() {
        let server = Server::default();
        let (mut a, mut b) = two_devices(&server).await;
        a.send(&server, play("one")).await;
        b.drain();
        // Pretend it has been playing for 5 s since it reported 1000 ms.
        server.rooms.lock().await.get_mut(KEY).unwrap().last_update_ms = now_ms() - 5000;

        server.handle_disconnect(a.uid.clone(), a.code.clone(), &a.tx).await;
        let got = b.drain();
        let [ServerMessage::UserDisconnected { .. }, ServerMessage::SyncPlayback(p)] = &got[..]
        else {
            panic!("{got:?}")
        };
        assert_eq!(p.kind, PlaybackKind::Pause);
        assert!((6000..7000).contains(&p.position_ms), "{}", p.position_ms);
        let rooms = server.rooms.lock().await;
        let room = &rooms[KEY];
        assert!(!room.is_playing);
        assert_eq!(room.position_ms, p.position_ms);
        assert_eq!(room.host_id, "a", "still the last player");
    }

    #[tokio::test]
    async fn an_empty_token_reconnect_cant_reach_a_link_device() {
        let server = Server::default();
        let _a = link(&server, "a").await;
        let token = server.rooms.lock().await[KEY].peers["a"].session_token.clone();
        assert!(!token.is_empty());
        for session_token in [String::new(), token] {
            let mut c = Conn::new();
            c.send(&server, ClientMessage::Reconnect { session_token }).await;
            assert_eq!(error_code(&c.drain()), Some("session_expired"));
            assert!(c.uid.is_none());
        }
    }

    #[tokio::test]
    async fn unlink_and_leave() {
        let server = Server::default();
        let (mut a, mut b) = two_devices(&server).await;
        a.send(&server, play("one")).await;
        a.drain();
        b.drain();
        b.send(&server, ClientMessage::KickUser { user_id: "a".into() }).await;
        assert!(
            matches!(&a.drain()[..], [ServerMessage::Kicked { reason }] if reason == "unlinked")
        );
        assert!(matches!(&b.drain()[..], [ServerMessage::UserLeft { user_id }] if user_id == "a"));
        {
            let rooms = server.rooms.lock().await;
            assert_eq!(rooms[KEY].host_id, "");
            assert!(!rooms[KEY].is_playing);
        }
        // The unlinked socket can't take the room back over.
        a.send(&server, play("two")).await;
        assert_eq!(server.rooms.lock().await[KEY].host_id, "");

        b.send(&server, ClientMessage::LeaveRoom).await;
        assert!(!server.rooms.lock().await.contains_key(KEY), "empty link room deleted");
    }

    #[tokio::test]
    async fn caps_count_link_rooms_apart() {
        let server = Server::default();
        {
            let mut rooms = server.rooms.lock().await;
            for i in 0..MAX_LINK_ROOMS {
                rooms.insert(format!("{LINK_PREFIX}{i}"), Room::empty(true));
            }
        }
        // MAX_ROOMS ignores link rooms.
        let mut host = Conn::new();
        host.send(&server, ClientMessage::CreateRoom { username: "h".into() }).await;
        assert!(matches!(host.drain()[..], [ServerMessage::RoomCreated { .. }]));
        // ...and a new link room is refused at its own cap.
        let mut c = link(&server, "a").await;
        assert_eq!(error_code(&c.drain()), Some("server_full"));

        server.rooms.lock().await.clear();
        for i in 0..MAX_LINK_DEVICES {
            link(&server, &i.to_string()).await;
        }
        let mut extra = link(&server, "extra").await;
        assert_eq!(error_code(&extra.drain()), Some("room_full"));
    }

    #[tokio::test]
    async fn a_link_room_outlives_its_devices_until_the_ttl() {
        let server = Server::default();
        let mut a = link(&server, "a").await;
        a.drain();
        pair_code(&server, &mut a).await;
        server.cleanup(Instant::now() + PAIR_TTL + Duration::from_secs(1)).await;
        assert!(server.rooms.lock().await[KEY].pair.is_none(), "expired code swept");

        server.handle_disconnect(a.uid.clone(), a.code.clone(), &a.tx).await;
        let t0 = Instant::now();
        server.cleanup(t0).await;
        // Way past RECONNECT_GRACE and HOST_GRACE: the device stays listed.
        server.cleanup(t0 + LINK_IDLE_TTL - Duration::from_secs(1)).await;
        assert_eq!(server.rooms.lock().await[KEY].peers.len(), 1);

        server.cleanup(t0 + LINK_IDLE_TTL + Duration::from_secs(1)).await;
        assert!(!server.rooms.lock().await.contains_key(KEY));
    }
}
