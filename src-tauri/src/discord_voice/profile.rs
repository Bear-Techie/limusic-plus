use base64::Engine as _;
use reqwest::header::{AUTHORIZATION, CONTENT_TYPE, HeaderValue, USER_AGENT};
use reqwest::{Response, StatusCode};
use serde_json::{Value, json};
use std::time::Duration;

const DISCORD_API: &str = "https://discord.com/api/v10";
const LIMUSIC_ICON: &[u8] = include_bytes!("../../icons/icon.png");

pub(super) async fn apply(token: &str, name: &str) -> Result<Value, &'static str> {
    let name = name.trim();
    if !(2..=32).contains(&name.chars().count()) {
        return Err("profile_name");
    }
    let mut auth = HeaderValue::from_str(&format!("Bot {token}")).map_err(|_| "token")?;
    auth.set_sensitive(true);
    let icon = format!(
        "data:image/png;base64,{}",
        base64::engine::general_purpose::STANDARD.encode(LIMUSIC_ICON)
    );

    let bot = patch("/users/@me", &auth, json!({ "username": name, "avatar": icon })).await?;
    let bot: Value = serde_json::from_slice(&bot.bytes().await.map_err(|_| "connection")?)
        .map_err(|_| "connection")?;
    let actual_name = bot.get("username").and_then(Value::as_str).unwrap_or(name);

    // The bot profile and its application are separate Discord resources.
    let application_error = patch(
        "/applications/@me",
        &auth,
        json!({ "icon": icon, "tags": ["Limusic", "Music", "Shared"] }),
    )
    .await
    .err();
    Ok(json!({ "name": actual_name, "application_error": application_error }))
}

async fn patch(path: &str, auth: &HeaderValue, body: Value) -> Result<Response, &'static str> {
    let response = crate::http::client()
        .patch(format!("{DISCORD_API}{path}"))
        .timeout(Duration::from_secs(10))
        .header(AUTHORIZATION, auth.clone())
        .header(CONTENT_TYPE, "application/json")
        .header(
            USER_AGENT,
            concat!(
                "DiscordBot (https://github.com/SimoHypers/limusic, ",
                env!("CARGO_PKG_VERSION"),
                ") LimusicPlus"
            ),
        )
        .body(body.to_string())
        .send()
        .await
        .map_err(|_| "connection")?;
    match response.status() {
        status if status.is_success() => Ok(response),
        StatusCode::UNAUTHORIZED => Err("token"),
        StatusCode::TOO_MANY_REQUESTS => Err("profile_rate_limit"),
        status if status.is_server_error() => Err("connection"),
        _ => Err("profile_rejected"),
    }
}
