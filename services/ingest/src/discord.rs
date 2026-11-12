//! Minimal Discord Gateway client that mirrors Lanyard's presence pipeline.
//!
//! It connects as a bot, requests the privileged GUILD_PRESENCES/GUILD_MEMBERS
//! intents, normalizes every presence into Lanyard's JSON shape and writes it to
//! Postgres/Redis. The Elixir gateway serves the same data over REST/WebSocket.

use std::{sync::Arc, time::Duration};

use anyhow::{Context, Result, bail};
use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use tokio::time::{Instant, MissedTickBehavior};
use tokio_tungstenite::{connect_async, tungstenite::Message};

use crate::App;

const GATEWAY_URL: &str = "wss://gateway.discord.gg/?v=10&encoding=json";

// GUILDS | GUILD_MEMBERS | GUILD_PRESENCES. Presences are privileged and must be
// enabled in the Discord developer portal for the bot application.
const INTENTS: u64 = (1 << 0) | (1 << 1) | (1 << 8);

const MAX_BACKOFF_SECS: u64 = 60;

#[derive(Default)]
struct GatewayState {
    session_id: Option<String>,
    resume_url: Option<String>,
    seq: Option<u64>,
}

/// Discord snowflakes are decimal u64 values. Validating them keeps them safe to
/// use as Redis key suffixes and URL path segments.
pub fn valid_snowflake(value: &str) -> bool {
    !value.is_empty()
        && value.len() <= 20
        && value.bytes().all(|b| b.is_ascii_digit())
        && value != "0"
        && value.parse::<u64>().is_ok()
}

pub fn spawn(app: Arc<App>) {
    tokio::spawn(async move {
        let mut state = GatewayState::default();
        let mut backoff = 1u64;

        loop {
            match run_once(&app, &mut state).await {
                Ok(()) => {
                    tracing::warn!("discord gateway closed, reconnecting");
                    backoff = 1;
                }
                Err(err) => {
                    tracing::warn!(error = %err, "discord gateway error, reconnecting");
                }
            }

            tokio::time::sleep(Duration::from_secs(backoff)).await;
            backoff = (backoff.saturating_mul(2)).min(MAX_BACKOFF_SECS);
        }
    });
}

async fn run_once(app: &App, state: &mut GatewayState) -> Result<()> {
    let discord = app.config.discord.as_ref().context("discord config missing")?;

    let url = match (&state.session_id, &state.resume_url) {
        (Some(_), Some(resume)) if resume.starts_with("wss://") => {
            format!("{resume}?v=10&encoding=json")
        }
        _ => GATEWAY_URL.to_owned(),
    };

    let (socket, _) = connect_async(&url).await.context("discord gateway connect failed")?;
    let (mut write, mut read) = socket.split();

    // First frame must be Hello (op 10). Anything else is a protocol violation.
    let hello = tokio::time::timeout(Duration::from_secs(15), read.next())
        .await
        .context("timed out waiting for discord Hello")?
        .context("discord gateway closed before Hello")??;
    let hello: Value = parse_text(hello)?;
    let heartbeat_ms = hello
        .pointer("/d/heartbeat_interval")
        .and_then(Value::as_u64)
        .filter(|ms| (1_000..=120_000).contains(ms))
        .context("invalid discord heartbeat_interval")?;

    let mut heartbeat = tokio::time::interval(Duration::from_millis(heartbeat_ms));
    heartbeat.set_missed_tick_behavior(MissedTickBehavior::Delay);
    heartbeat.tick().await; // consume the immediate first tick

    if let Some(session_id) = &state.session_id {
        let payload = json!({
            "op": 6,
            "d": {
                "token": discord.bot_token,
                "session_id": session_id,
                "seq": state.seq,
            }
        });
        write.send(Message::Text(payload.to_string().into())).await?;
        tracing::info!("discord gateway resuming");
    } else {
        let payload = json!({
            "op": 2,
            "d": {
                "token": discord.bot_token,
                "intents": INTENTS,
                "properties": { "os": "linux", "browser": "taggle", "device": "taggle" },
            }
        });
        write.send(Message::Text(payload.to_string().into())).await?;
        tracing::info!("discord gateway identifying");
    }

    let mut last_ack = Instant::now();

    loop {
        tokio::select! {
            frame = read.next() => {
                let Some(frame) = frame else { return Ok(()) };
                let frame = frame?;

                match frame {
                    Message::Text(text) => {
                        let value: Value = match serde_json::from_str(&text) {
                            Ok(v) => v,
                            Err(err) => {
                                tracing::warn!(error = %err, "discord gateway sent invalid json");
                                continue;
                            }
                        };

                        if let Some(seq) = value.get("s").and_then(Value::as_u64) {
                            state.seq = Some(seq);
                        }

                        match value.get("op").and_then(Value::as_u64) {
                            Some(11) => last_ack = Instant::now(),
                            Some(1) => {
                                let payload = json!({ "op": 1, "d": state.seq });
                                write.send(Message::Text(payload.to_string().into())).await?;
                            }
                            Some(7) => return Ok(()), // reconnect, keeping session for resume
                            Some(9) => {
                                tracing::warn!("discord gateway rejected session; identifying again");
                                state.session_id = None;
                                state.resume_url = None;
                                state.seq = None;
                                bail!("discord invalid session");
                            }
                            Some(0) => {
                                handle_dispatch(app, state, &value).await?;
                            }
                            _ => {}
                        }
                    }
                    Message::Binary(_) => {}
                    Message::Ping(payload) => write.send(Message::Pong(payload)).await?,
                    Message::Close(_) => return Ok(()),
                    Message::Pong(_) | Message::Frame(_) => {}
                }
            }
            _ = heartbeat.tick() => {
                // If the gateway stops acking, assume the socket is half-open.
                if last_ack.elapsed() > Duration::from_millis(heartbeat_ms).saturating_mul(2) {
                    bail!("discord heartbeat ack timed out");
                }
                let payload = json!({ "op": 1, "d": state.seq });
                write.send(Message::Text(payload.to_string().into())).await?;
            }
        }
    }
}

fn parse_text(frame: Message) -> Result<Value> {
    match frame {
        Message::Text(text) => Ok(serde_json::from_str(&text)?),
        _ => bail!("expected a text frame from discord"),
    }
}

async fn handle_dispatch(app: &App, state: &mut GatewayState, value: &Value) -> Result<()> {
    let Some(event) = value.get("t").and_then(Value::as_str) else { return Ok(()) };
    let data = value.get("d").unwrap_or(&Value::Null);

    match event {
        "READY" => {
            state.session_id = data.get("session_id").and_then(Value::as_str).map(str::to_owned);
            state.resume_url = data.get("resume_gateway_url").and_then(Value::as_str).map(str::to_owned);
            tracing::info!("discord gateway ready");
        }
        "RESUMED" => tracing::info!("discord gateway session resumed"),
        "GUILD_CREATE" => {
            if !guild_allowed(app, data.get("id").and_then(Value::as_str)) {
                return Ok(());
            }
            let members: std::collections::HashMap<&str, &Value> = data
                .get("members")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
                .filter_map(|m| m.pointer("/user/id").and_then(Value::as_str).map(|id| (id, m)))
                .collect();

            for presence in data.get("presences").and_then(Value::as_array).into_iter().flatten() {
                let member = presence
                    .pointer("/user/id")
                    .and_then(Value::as_str)
                    .and_then(|id| members.get(id).copied());
                store_presence(app, presence, member).await;
            }
        }
        "PRESENCE_UPDATE" => {
            if !guild_allowed(app, data.get("guild_id").and_then(Value::as_str)) {
                return Ok(());
            }
            store_presence(app, data, None).await;
        }
        "GUILD_MEMBER_REMOVE" => {
            if !guild_allowed(app, data.get("guild_id").and_then(Value::as_str)) {
                return Ok(());
            }
            if let Some(user_id) = data.pointer("/user/id").and_then(Value::as_str)
                && let Err(err) = app.store.delete_discord_presence(user_id).await
            {
                tracing::warn!(error = %err, user_id, "failed deleting discord presence");
            }
        }
        _ => {}
    }
    Ok(())
}

fn guild_allowed(app: &App, guild_id: Option<&str>) -> bool {
    let Some(discord) = app.config.discord.as_ref() else { return false };
    if discord.guild_ids.is_empty() {
        return true;
    }
    guild_id
        .and_then(|id| id.parse::<u64>().ok())
        .is_some_and(|id| discord.guild_ids.contains(&id))
}

async fn store_presence(app: &App, data: &Value, member: Option<&Value>) {
    let Some((user_id, presence)) = normalize_presence(data, member) else { return };
    if let Err(err) = app.store.upsert_discord_presence(&user_id, &presence).await {
        tracing::warn!(error = %err, user_id, "failed storing discord presence");
    }
}

/// Converts a raw Discord presence into Lanyard's response shape.
pub fn normalize_presence(data: &Value, member: Option<&Value>) -> Option<(String, Value)> {
    // GUILD_CREATE presences may carry only a partial user object, so fall back
    // to the guild member payload for the missing profile fields.
    let member_user = member.and_then(|m| m.get("user"));
    let user = data.get("user").or(member_user)?;
    let user_id = user.get("id").and_then(Value::as_str)?.to_owned();
    if !valid_snowflake(&user_id) {
        return None;
    }

    let field = |key: &str| {
        user.get(key)
            .or_else(|| member_user.and_then(|m| m.get(key)))
            .cloned()
            .unwrap_or(Value::Null)
    };

    let client_status = data.get("client_status").and_then(Value::as_object);
    let active = |platform: &str| {
        client_status
            .and_then(|m| m.get(platform))
            .and_then(Value::as_str)
            .is_some_and(|status| status != "offline")
    };

    let activities = data
        .get("activities")
        .and_then(Value::as_array)
        .cloned()
        .unwrap_or_default();
    let spotify_activity = activities
        .iter()
        .find(|activity| activity.get("name").and_then(Value::as_str) == Some("Spotify"));

    let discord_user = json!({
        "id": user_id,
        "username": field("username"),
        "discriminator": non_null(field("discriminator"), json!("0")),
        "avatar": field("avatar"),
        "global_name": field("global_name"),
        "public_flags": non_null(field("public_flags"), json!(0)),
        "bot": non_null(field("bot"), json!(false)),
    });

    let presence = json!({
        "discord_user": discord_user,
        "discord_status": data.get("status").and_then(Value::as_str).unwrap_or("offline"),
        "activities": activities,
        "active_on_discord_desktop": active("desktop"),
        "active_on_discord_mobile": active("mobile"),
        "active_on_discord_web": active("web"),
        "listening_to_spotify": spotify_activity.is_some(),
        "spotify": spotify_activity.map(build_spotify),
        "kv": {},
    });

    Some((user_id, presence))
}

fn non_null(value: Value, fallback: Value) -> Value {
    if value.is_null() { fallback } else { value }
}

fn build_spotify(activity: &Value) -> Value {
    let timestamps = activity.get("timestamps").and_then(Value::as_object);

    let album_art_url = activity
        .pointer("/assets/large_image")
        .and_then(Value::as_str)
        .map(|image| image.strip_prefix("spotify:").unwrap_or(image))
        .map(|image| format!("https://i.scdn.co/image/{image}"))
        .unwrap_or_default();

    json!({
        "track_id": activity.get("sync_id").cloned().unwrap_or(Value::Null),
        "timestamps": {
            "start": timestamps.and_then(|t| t.get("start")).cloned().unwrap_or(Value::Null),
            "end": timestamps.and_then(|t| t.get("end")).cloned().unwrap_or(Value::Null),
        },
        "song": activity.get("details").cloned().unwrap_or(Value::Null),
        "artist": activity.get("state").cloned().unwrap_or(Value::Null),
        "album": activity.pointer("/assets/large_text").cloned().unwrap_or(Value::Null),
        "album_art_url": album_art_url,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn validates_snowflakes() {
        assert!(valid_snowflake("123456789012345678"));
        assert!(!valid_snowflake(""));
        assert!(!valid_snowflake("0"));
        assert!(!valid_snowflake("123abc"));
        assert!(!valid_snowflake("../etc/passwd"));
        assert!(!valid_snowflake("123456789012345678901"));
    }

    #[test]
    fn normalizes_presence_and_spotify() {
        let raw = json!({
            "user": {
                "id": "123456789012345678",
                "username": "example-user",
                "discriminator": "0001",
                "avatar": "a_0123456789abcdef0123456789abcdef",
                "public_flags": 0
            },
            "status": "online",
            "client_status": { "desktop": "online", "mobile": "idle" },
            "activities": [{
                "name": "Spotify",
                "type": 2,
                "sync_id": "0123456789abcdefghijkl",
                "details": "Night Drive",
                "state": "Example Artist",
                "timestamps": { "start": 1615529820677i64, "end": 1615530068733i64 },
                "assets": {
                    "large_image": "spotify:0123456789abcdef0123456789abcdef",
                    "large_text": "Night Drive"
                }
            }]
        });

        let (id, presence) = normalize_presence(&raw, None).unwrap();
        assert_eq!(id, "123456789012345678");
        assert_eq!(presence["discord_status"], "online");
        assert_eq!(presence["active_on_discord_desktop"], true);
        assert_eq!(presence["active_on_discord_mobile"], true);
        assert_eq!(presence["active_on_discord_web"], false);
        assert_eq!(presence["listening_to_spotify"], true);
        assert_eq!(presence["spotify"]["song"], "Night Drive");
        assert_eq!(
            presence["spotify"]["album_art_url"],
            "https://i.scdn.co/image/0123456789abcdef0123456789abcdef"
        );
    }

    #[test]
    fn uses_member_when_presence_has_no_user() {
        let presence = json!({ "status": "idle", "activities": [] });
        let member = json!({ "user": { "id": "123456789", "username": "member" } });
        let (id, normalized) = normalize_presence(&presence, Some(&member)).unwrap();
        assert_eq!(id, "123456789");
        assert_eq!(normalized["discord_user"]["username"], "member");
        assert_eq!(normalized["listening_to_spotify"], false);
        assert!(normalized["spotify"].is_null());
    }

    #[test]
    fn fills_partial_presence_user_from_member() {
        // GUILD_CREATE presences can arrive with just the user id.
        let presence = json!({ "user": { "id": "123456789" }, "status": "online", "activities": [] });
        let member = json!({
            "user": {
                "id": "123456789",
                "username": "member",
                "discriminator": "0",
                "global_name": "Member",
                "bot": true
            }
        });

        let (_, normalized) = normalize_presence(&presence, Some(&member)).unwrap();
        assert_eq!(normalized["discord_user"]["username"], "member");
        assert_eq!(normalized["discord_user"]["global_name"], "Member");
        assert_eq!(normalized["discord_user"]["discriminator"], "0");
        assert_eq!(normalized["discord_user"]["bot"], true);
    }
}
