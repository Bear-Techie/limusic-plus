//! Opt-in broadcast of Limusic's output to a single Discord voice channel.

use crate::state::AppState;
use std::sync::Arc;
use tauri::State;

#[cfg(windows)]
mod credentials;
#[cfg(windows)]
mod pcm_capture;
#[cfg(windows)]
mod profile;
#[cfg(windows)]
mod worker;

#[derive(Default)]
pub struct Broadcast {
    #[cfg(windows)]
    session: std::sync::Mutex<Option<worker::Session>>,
}

impl Broadcast {
    pub fn stop(&self) {
        #[cfg(windows)]
        if let Some(session) = self.session.lock().unwrap().as_ref() {
            session.stop();
        }
    }
}

#[tauri::command]
pub fn voice_status(state: State<'_, Broadcast>) -> &'static str {
    #[cfg(windows)]
    return state.session.lock().unwrap().as_ref().map_or_else(
        || if worker::Runtime::find().is_some() { "idle" } else { "not_installed" },
        worker::Session::status,
    );
    #[cfg(not(windows))]
    {
        let _ = state;
        "unsupported"
    }
}

#[tauri::command]
pub fn voice_stop(state: State<'_, Broadcast>) {
    state.stop();
}

#[tauri::command]
pub fn voice_list_bots() -> Result<Vec<serde_json::Value>, &'static str> {
    #[cfg(windows)]
    return credentials::list().map(|bots| bots.iter().map(credentials::Bot::public).collect());
    #[cfg(not(windows))]
    Ok(Vec::new())
}

#[tauri::command]
pub fn voice_save_bot(
    id: String,
    name: String,
    channel: String,
    channel_name: String,
    token: String,
) -> Result<(), &'static str> {
    #[cfg(windows)]
    return credentials::save(&credentials::Bot { id, name, channel, channel_name, token });
    #[cfg(not(windows))]
    {
        let _ = (id, name, channel, channel_name, token);
        Err("unsupported")
    }
}

#[tauri::command]
pub fn voice_forget_bot(id: String) -> Result<(), &'static str> {
    #[cfg(windows)]
    return credentials::forget(&id);
    #[cfg(not(windows))]
    {
        let _ = id;
        Err("unsupported")
    }
}

#[tauri::command]
pub async fn voice_discover(
    token: Option<String>,
    bot_id: Option<String>,
) -> Result<serde_json::Value, &'static str> {
    #[cfg(windows)]
    {
        let runtime = worker::Runtime::find().ok_or("not_installed")?;
        tauri::async_runtime::spawn_blocking(move || {
            let token = match token {
                Some(token) => credentials::clean_token(&token)?.to_owned(),
                None => credentials::load(bot_id.as_deref().ok_or("token")?)?.ok_or("token")?.token,
            };
            runtime.discover(token)
        })
        .await
        .map_err(|_| "failed")?
    }
    #[cfg(not(windows))]
    {
        let _ = (token, bot_id);
        Err("unsupported")
    }
}

#[tauri::command]
pub async fn voice_apply_profile(
    token: Option<String>,
    bot_id: Option<String>,
    name: String,
) -> Result<serde_json::Value, &'static str> {
    #[cfg(windows)]
    {
        let token = match token {
            Some(token) => credentials::clean_token(&token)?.to_owned(),
            None => credentials::load(bot_id.as_deref().ok_or("token")?)?.ok_or("token")?.token,
        };
        profile::apply(&token, &name).await
    }
    #[cfg(not(windows))]
    {
        let _ = (token, bot_id, name);
        Err("unsupported")
    }
}

#[tauri::command]
pub fn voice_start(
    state: State<'_, Broadcast>,
    app: State<'_, Arc<AppState>>,
    bot_id: String,
    channel: String,
    channel_name: Option<String>,
) -> Result<(), &'static str> {
    #[cfg(windows)]
    {
        let mut session = state.session.lock().unwrap();
        if session.as_ref().is_some_and(worker::Session::running) {
            return Err("busy");
        }
        let runtime = worker::Runtime::find().ok_or("not_installed")?;
        let mut bot = credentials::load(&bot_id)?.ok_or("token")?;
        if channel.parse::<u64>().ok().filter(|id| *id != 0).is_none() {
            return Err("channel");
        }
        let channel_name = channel_name.filter(|name| !name.is_empty()).unwrap_or_else(|| {
            if bot.channel == channel { bot.channel_name.clone() } else { String::new() }
        });
        if bot.id != "legacy" && (bot.channel != channel || bot.channel_name != channel_name) {
            bot.channel = channel.clone();
            bot.channel_name = channel_name;
            credentials::save(&bot)?;
        }
        *session = Some(worker::Session::start(
            runtime,
            bot.token,
            channel,
            app.player.broadcast_source(),
        )?);
        Ok(())
    }
    #[cfg(not(windows))]
    {
        let _ = (state, app, bot_id, channel, channel_name);
        Err("unsupported")
    }
}

#[tauri::command]
pub async fn voice_share_bot(state: State<'_, Arc<AppState>>, id: String) -> Result<usize, String> {
    #[cfg(windows)]
    {
        let mut bot = credentials::load(&id)?.ok_or("That bot is no longer saved.")?;
        if bot.id == "legacy" {
            let runtime = worker::Runtime::find().ok_or("Install Discord broadcasting first.")?;
            let token = bot.token.clone();
            let catalog = tauri::async_runtime::spawn_blocking(move || runtime.discover(token))
                .await
                .map_err(|_| "Could not verify the saved bot.")?
                .map_err(|_| {
                    "Could not verify the saved bot. Check the token in Discord settings."
                })?;
            bot.id = catalog["id"].as_str().ok_or("Invalid Discord bot ID.")?.into();
            bot.name = catalog["name"].as_str().ok_or("Invalid Discord bot name.")?.into();
            bot.channel = state.db.get_setting("discord_voice_channel").unwrap_or_default();
            credentials::save(&bot)?;
        }
        state
            .lt
            .share_bot(crate::listentogether::bot_share::BotPayload {
                id: bot.id,
                name: bot.name,
                channel: bot.channel,
                token: bot.token,
            })
            .await
    }
    #[cfg(not(windows))]
    {
        let _ = (state, id);
        Err("Discord broadcasting is unavailable on this platform.".into())
    }
}

#[tauri::command]
pub async fn voice_bot_offers(
    state: State<'_, Arc<AppState>>,
) -> Result<Vec<crate::listentogether::bot_share::Notice>, String> {
    #[cfg(windows)]
    return Ok(state.lt.bot_notices().await);
    #[cfg(not(windows))]
    {
        let _ = state;
        Ok(Vec::new())
    }
}

#[tauri::command]
pub async fn voice_respond_offer(
    state: State<'_, Arc<AppState>>,
    id: String,
    accept: bool,
) -> Result<(), String> {
    #[cfg(windows)]
    {
        if accept {
            let bot = state.lt.accept_bot(id.clone()).await?;
            if bot.name.trim().is_empty()
                || bot.name.len() > 160
                || (!bot.channel.is_empty() && bot.channel.parse::<u64>().is_err())
            {
                return Err("The bot offer contains invalid details.".into());
            }
            let (channel, channel_name) = credentials::load(&bot.id)?
                .filter(|saved| !saved.channel.is_empty())
                .map(|saved| (saved.channel, saved.channel_name))
                .unwrap_or((bot.channel, String::new()));
            credentials::save(&credentials::Bot {
                id: bot.id,
                name: bot.name,
                channel,
                channel_name,
                token: bot.token,
            })?;
        }
        state.lt.dismiss_bot(&id).await;
        Ok(())
    }
    #[cfg(not(windows))]
    {
        let _ = (state, id, accept);
        Err("Discord broadcasting is unavailable on this platform.".into())
    }
}
