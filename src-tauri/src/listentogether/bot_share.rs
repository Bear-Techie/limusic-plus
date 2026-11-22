//! Credential offers use the authenticated peer connection. Announcements carry no token.
use serde::{Deserialize, Serialize};
use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant};

const LIFETIME: Duration = Duration::from_secs(300);

#[derive(Clone, Serialize, Deserialize)]
pub(crate) struct BotPayload {
    pub id: String,
    pub name: String,
    pub channel: String,
    pub token: String,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub(super) enum Request {
    Offer { id: String, bot_id: String, name: String },
    Accept { id: String },
}

#[derive(Clone, Serialize)]
pub(crate) struct Notice {
    pub id: String,
    pub name: String,
    pub sender: String,
    #[serde(skip)]
    bot_id: String,
}

pub(super) struct Room {
    pub epoch: String,
    // On guests, the authenticated endpoint of the current room host.
    pub host: Option<(String, String)>,
    // On the host, only connected, approved guests with registered endpoints.
    pub guests: HashSet<String>,
}

#[derive(Default)]
pub(super) struct Shares {
    pub room: Option<Room>,
    grants: HashMap<String, (String, BotPayload, Instant)>,
    inbox: HashMap<String, (Notice, Instant)>,
}

impl Shares {
    pub fn set_room(&mut self, room: Option<Room>) {
        let same = self.room.as_ref().zip(room.as_ref()).is_some_and(|(old, new)| {
            old.epoch == new.epoch
                && old.host.as_ref().map(|h| &h.0) == new.host.as_ref().map(|h| &h.0)
        });
        if !same {
            self.grants.clear();
            self.inbox.clear();
        }
        self.room = room;
        self.prune();
    }

    fn prune(&mut self) {
        let now = Instant::now();
        self.inbox.retain(|_, (_, until)| *until > now);
        self.grants.retain(|_, (peer, _, until)| {
            *until > now && self.room.as_ref().is_some_and(|room| room.guests.contains(peer))
        });
    }

    pub fn offer(&mut self, bot: BotPayload) -> Option<(String, Vec<(String, Request)>)> {
        self.prune();
        let room = self.room.as_ref()?;
        if room.guests.is_empty() {
            return None;
        }
        let mut requests = Vec::new();
        // A fresh offer for this bot supersedes any previously unanswered offers.
        self.grants.retain(|_, (_, old, _)| old.id != bot.id);
        if self.grants.len() + room.guests.len() > 200 {
            return None;
        }
        for peer in &room.guests {
            let id = format!("{:032x}", rand::random::<u128>());
            self.grants.insert(id.clone(), (peer.clone(), bot.clone(), Instant::now() + LIFETIME));
            requests.push((
                peer.clone(),
                Request::Offer { id, bot_id: bot.id.clone(), name: bot.name.clone() },
            ));
        }
        Some((room.epoch.clone(), requests))
    }

    pub fn receive(&mut self, remote: &str, epoch: &str, request: Request) -> serde_json::Value {
        self.prune();
        let Some(room) = self.room.as_ref().filter(|room| room.epoch == epoch) else {
            return serde_json::Value::Null;
        };
        match request {
            Request::Offer { id, bot_id, name } => {
                if !cfg!(windows) {
                    return serde_json::Value::Null;
                }
                let Some((host, sender)) = &room.host else { return serde_json::Value::Null };
                if host != remote
                    || id.len() != 32
                    || !id.bytes().all(|b| b.is_ascii_hexdigit())
                    || bot_id.is_empty()
                    || bot_id.len() > 20
                    || !bot_id.bytes().all(|b| b.is_ascii_digit())
                    || name.trim().is_empty()
                    || name.len() > 160
                {
                    return serde_json::Value::Null;
                }
                self.inbox.retain(|_, (notice, _)| notice.bot_id != bot_id);
                if self.inbox.len() >= 16 {
                    return serde_json::Value::Null;
                }
                self.inbox.insert(
                    id.clone(),
                    (
                        Notice { id, bot_id, name, sender: sender.clone() },
                        Instant::now() + LIFETIME,
                    ),
                );
                serde_json::Value::Bool(true)
            }
            Request::Accept { id } => self
                .grants
                .get(&id)
                .filter(|(peer, _, _)| peer == remote)
                .map_or(serde_json::Value::Null, |(_, bot, _)| serde_json::json!(bot)),
        }
    }

    pub fn notices(&mut self) -> Vec<Notice> {
        self.prune();
        self.inbox.values().map(|(notice, _)| notice.clone()).collect()
    }

    pub fn origin(&mut self, id: &str) -> Option<(String, String)> {
        self.prune();
        self.inbox.get(id)?;
        let room = self.room.as_ref()?;
        Some((room.epoch.clone(), room.host.as_ref()?.0.clone()))
    }

    pub fn dismiss(&mut self, id: &str) {
        self.inbox.remove(id);
    }
}
