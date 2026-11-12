//! Minimal Discord Gateway client that mirrors Lanyard's presence pipeline
//! and handles the `/connect` slash command for linking GitHub accounts.
//!
//! It connects as a bot, requests the privileged GUILD_PRESENCES/GUILD_MEMBERS
//! intents, normalizes every presence into Lanyard's JSON shape and writes it to
//! Postgres/Redis. The Elixir gateway serves the same data over REST/WebSocket.

use std::{
    collections::{HashMap, HashSet},
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use anyhow::{Context, Result, bail};
use futures_util::{SinkExt, StreamExt};
use serde::Deserialize;
use serde_json::{Value, json};
use tokio::{
    task::AbortHandle,
    time::{Instant, MissedTickBehavior},
};
use tokio_tungstenite::{connect_async, tungstenite::Message};

use crate::App;

const GATEWAY_URL: &str = "wss://gateway.discord.gg/?v=10&encoding=json";
const DISCORD_API: &str = "https://discord.com/api/v10";

// GUILDS | GUILD_MEMBERS | GUILD_PRESENCES. Presences are privileged and must be
// enabled in the Discord developer portal for the bot application.
const INTENTS: u64 = (1 << 0) | (1 << 1) | (1 << 8);

const MAX_BACKOFF_SECS: u64 = 60;
const PRESENCE_SHARDS: usize = 8;
const PRESENCE_QUEUE_CAP: usize = 4096;
const COLOR_BLURPLE: u32 = 0x5865F2;
const COLOR_GREEN: u32 = 0x22C55E;
const COLOR_RED: u32 = 0xEF4444;

enum PresenceJob {
    Upsert(String, Value),
    Purge(String),
}

struct BotContext {
    http: reqwest::Client,
    application_id: Mutex<Option<String>>,
    registered_guilds: Mutex<HashSet<String>>,
    guild_members: Mutex<HashMap<String, HashSet<String>>>,
    guild_online: Mutex<HashMap<String, HashSet<String>>>,
    member_users: Mutex<HashMap<String, Value>>,
    pending_connects: Mutex<HashMap<String, (u64, AbortHandle)>>,
    connect_cooldowns: Mutex<HashMap<String, Instant>>,
    presence_txs: Vec<tokio::sync::mpsc::Sender<PresenceJob>>,
    next_task_id: AtomicU64,
}

impl BotContext {
    fn new(app: &Arc<App>) -> Result<Self> {
        let http = reqwest::Client::builder()
            .user_agent("taggle-ingest")
            .timeout(Duration::from_secs(15))
            .build()?;

        let mut presence_txs = Vec::with_capacity(PRESENCE_SHARDS);
        for _ in 0..PRESENCE_SHARDS {
            let (tx, mut rx) = tokio::sync::mpsc::channel::<PresenceJob>(PRESENCE_QUEUE_CAP);
            let app_worker = Arc::clone(app);
            tokio::spawn(async move {
                while let Some(job) = rx.recv().await {
                    match job {
                        PresenceJob::Upsert(user_id, presence) => {
                            if let Err(err) = app_worker
                                .store
                                .upsert_discord_presence(&user_id, &presence)
                                .await
                            {
                                tracing::warn!(error = %err, user_id, "failed storing discord presence");
                            }
                        }
                        PresenceJob::Purge(user_id) => {
                            if let Err(err) = app_worker.store.purge_discord_user(&user_id).await {
                                tracing::warn!(error = %err, user_id, "failed purging discord user");
                            } else {
                                tracing::info!(
                                    user_id,
                                    "purged data for user not in monitored discord server"
                                );
                            }
                        }
                    }
                }
            });
            presence_txs.push(tx);
        }

        Ok(Self {
            http,
            application_id: Mutex::new(None),
            registered_guilds: Mutex::new(HashSet::new()),
            guild_members: Mutex::new(HashMap::new()),
            guild_online: Mutex::new(HashMap::new()),
            member_users: Mutex::new(HashMap::new()),
            pending_connects: Mutex::new(HashMap::new()),
            connect_cooldowns: Mutex::new(HashMap::new()),
            presence_txs,
            next_task_id: AtomicU64::new(1),
        })
    }

    fn cache_user(&self, user: &Value) {
        let Some(id) = user.get("id").and_then(Value::as_str) else {
            return;
        };
        if !valid_snowflake(id) || user.get("username").and_then(Value::as_str).is_none() {
            return;
        }
        let mut map = self.member_users.lock().unwrap();
        if map.len() > 20_000 && !map.contains_key(id) {
            return;
        }
        map.insert(id.to_owned(), user.clone());
    }

    fn cached_member(&self, user_id: &str) -> Option<Value> {
        self.member_users
            .lock()
            .unwrap()
            .get(user_id)
            .cloned()
            .map(|user| json!({ "user": user }))
    }

    fn shard_for(user_id: &str) -> usize {
        let mut h: usize = 5381;
        for b in user_id.as_bytes() {
            h = h.wrapping_mul(33).wrapping_add(*b as usize);
        }
        h % PRESENCE_SHARDS
    }

    fn enqueue_presence(&self, app: &App, user_id: String, presence: Value) {
        let idx = Self::shard_for(&user_id);
        if self.presence_txs[idx]
            .try_send(PresenceJob::Upsert(user_id, presence))
            .is_err()
        {
            app.metrics
                .discord_presence_dropped
                .fetch_add(1, Ordering::Relaxed);
            tracing::warn!("discord presence queue full; dropping stale update");
        } else {
            app.metrics
                .discord_presence_updates
                .fetch_add(1, Ordering::Relaxed);
        }
    }

    fn enqueue_purge(&self, user_id: String) {
        let idx = Self::shard_for(&user_id);
        let tx = self.presence_txs[idx].clone();
        if let Err(tokio::sync::mpsc::error::TrySendError::Full(job)) =
            tx.try_send(PresenceJob::Purge(user_id))
        {
            tokio::spawn(async move {
                let _ = tx.send(job).await;
            });
        }
    }

    fn allow_connect_click(&self, user_id: &str) -> bool {
        let now = Instant::now();
        let mut map = self.connect_cooldowns.lock().unwrap();
        if map.len() > 1000 {
            map.retain(|_, ts| now.duration_since(*ts) < Duration::from_secs(30));
        }
        if map
            .get(user_id)
            .is_some_and(|prev| now.duration_since(*prev) < Duration::from_secs(2))
        {
            return false;
        }
        map.insert(user_id.to_owned(), now);
        true
    }

    fn set_guild_members(&self, guild_id: &str, members: HashSet<String>) {
        self.guild_members
            .lock()
            .unwrap()
            .insert(guild_id.to_owned(), members);
    }

    fn set_guild_online(&self, guild_id: &str, online: HashSet<String>) {
        self.guild_online
            .lock()
            .unwrap()
            .insert(guild_id.to_owned(), online);
    }

    fn update_guild_online(&self, guild_id: &str, user_id: &str, status: &str) {
        let mut map = self.guild_online.lock().unwrap();
        let set = map.entry(guild_id.to_owned()).or_default();
        if status == "offline" {
            set.remove(user_id);
        } else {
            set.insert(user_id.to_owned());
        }
    }

    fn all_online_users(&self) -> Vec<String> {
        let map = self.guild_online.lock().unwrap();
        let mut out = HashSet::new();
        for set in map.values() {
            out.extend(set.iter().cloned());
        }
        out.into_iter().collect()
    }

    fn remove_guild(&self, guild_id: &str) {
        self.guild_members.lock().unwrap().remove(guild_id);
        self.guild_online.lock().unwrap().remove(guild_id);
        self.registered_guilds.lock().unwrap().remove(guild_id);
    }

    fn add_guild_member(&self, guild_id: &str, user_id: &str) {
        self.guild_members
            .lock()
            .unwrap()
            .entry(guild_id.to_owned())
            .or_default()
            .insert(user_id.to_owned());
    }

    fn remove_guild_member(&self, guild_id: &str, user_id: &str) {
        if let Some(set) = self.guild_members.lock().unwrap().get_mut(guild_id) {
            set.remove(user_id);
        }
        if let Some(set) = self.guild_online.lock().unwrap().get_mut(guild_id) {
            set.remove(user_id);
        }
    }

    fn is_cached_member_anywhere(&self, user_id: &str) -> bool {
        self.guild_members
            .lock()
            .unwrap()
            .values()
            .any(|set| set.contains(user_id))
    }

    fn known_guild_ids(&self) -> Vec<String> {
        self.guild_members.lock().unwrap().keys().cloned().collect()
    }

    fn cancel_pending(&self, user_id: &str) {
        if let Some((_, handle)) = self.pending_connects.lock().unwrap().remove(user_id) {
            handle.abort();
        }
    }

    fn register_pending(&self, user_id: &str, task_id: u64, handle: AbortHandle) {
        if let Some((_, prev)) = self
            .pending_connects
            .lock()
            .unwrap()
            .insert(user_id.to_owned(), (task_id, handle))
        {
            prev.abort();
        }
    }

    fn clear_pending_if_match(&self, user_id: &str, task_id: u64) {
        let mut map = self.pending_connects.lock().unwrap();
        if map.get(user_id).is_some_and(|(id, _)| *id == task_id) {
            map.remove(user_id);
        }
    }
}

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
        && !value.starts_with('0')
        && value.bytes().all(|b| b.is_ascii_digit())
        && value.parse::<u64>().is_ok()
}

fn is_discord_gateway_url(url: &str) -> bool {
    let Some(rest) = url.strip_prefix("wss://") else {
        return false;
    };
    let host_port = rest.split(['/', '?', '#']).next().unwrap_or("");
    if host_port.is_empty() || host_port.contains('@') {
        return false;
    }
    let host = host_port.split(':').next().unwrap_or("");
    let host_lower = host.to_ascii_lowercase();
    host_lower == "discord.gg" || host_lower.ends_with(".discord.gg")
}

pub fn spawn(app: Arc<App>) {
    tokio::spawn(async move {
        let ctx = Arc::new(BotContext::new(&app).expect("failed to build Discord HTTP client"));
        let mut state = GatewayState::default();
        let mut backoff = 1u64;

        loop {
            let was_resuming = state.session_id.is_some();
            app.metrics
                .discord_reconnects
                .fetch_add(1, Ordering::Relaxed);
            let res = run_once(&app, &ctx, &mut state).await;
            app.metrics.discord_connected.store(0, Ordering::Relaxed);
            match res {
                Ok(()) => {
                    tracing::warn!("discord gateway closed, reconnecting");
                    backoff = 1;
                }
                Err(err) => {
                    tracing::warn!(error = %err, "discord gateway error, reconnecting");
                    if was_resuming {
                        state.session_id = None;
                        state.resume_url = None;
                        state.seq = None;
                    }
                }
            }

            tokio::time::sleep(Duration::from_secs(backoff)).await;
            backoff = (backoff.saturating_mul(2)).min(MAX_BACKOFF_SECS);
        }
    });
}

async fn run_once(app: &Arc<App>, ctx: &Arc<BotContext>, state: &mut GatewayState) -> Result<()> {
    let discord = app
        .config
        .discord
        .as_ref()
        .context("discord config missing")?;

    let url = match (&state.session_id, &state.resume_url) {
        (Some(_), Some(resume)) if is_discord_gateway_url(resume) => {
            format!("{resume}?v=10&encoding=json")
        }
        _ => GATEWAY_URL.to_owned(),
    };

    let (socket, _) = connect_async(&url)
        .await
        .context("discord gateway connect failed")?;
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
        write
            .send(Message::Text(payload.to_string().into()))
            .await?;
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
        write
            .send(Message::Text(payload.to_string().into()))
            .await?;
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
                                handle_dispatch(app, ctx, state, &value).await?;
                            }
                            _ => {}
                        }
                    }
                    Message::Binary(_) => {}
                    Message::Ping(payload) => write.send(Message::Pong(payload)).await?,
                    Message::Close(frame) => {
                        if let Some(code) = frame.map(|f| u16::from(f.code))
                            && matches!(code, 4003 | 4004 | 4007 | 4009 | 4010..=4014)
                        {
                            state.session_id = None;
                            state.resume_url = None;
                            state.seq = None;
                        }
                        return Ok(());
                    }
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

async fn handle_dispatch(
    app: &Arc<App>,
    ctx: &Arc<BotContext>,
    state: &mut GatewayState,
    value: &Value,
) -> Result<()> {
    let Some(event) = value.get("t").and_then(Value::as_str) else {
        return Ok(());
    };
    let data = value.get("d").unwrap_or(&Value::Null);

    match event {
        "READY" => {
            app.metrics.discord_connected.store(1, Ordering::Relaxed);
            state.session_id = data
                .get("session_id")
                .and_then(Value::as_str)
                .map(str::to_owned);
            state.resume_url = data
                .get("resume_gateway_url")
                .and_then(Value::as_str)
                .map(str::to_owned);
            if let Some(app_id) = data.pointer("/application/id").and_then(Value::as_str) {
                *ctx.application_id.lock().unwrap() = Some(app_id.to_owned());
                spawn_command_registration(app, ctx, app_id.to_owned());
            }
            tracing::info!("discord gateway ready");
        }
        "RESUMED" => {
            app.metrics.discord_connected.store(1, Ordering::Relaxed);
            tracing::info!("discord gateway session resumed");
        }
        "GUILD_CREATE" => {
            let Some(guild_id) = data.get("id").and_then(Value::as_str) else {
                return Ok(());
            };
            if !guild_allowed(app, Some(guild_id)) {
                return Ok(());
            }
            if app
                .config
                .discord
                .as_ref()
                .is_some_and(|d| d.guild_ids.is_empty())
            {
                let app_id = ctx.application_id.lock().unwrap().clone();
                if let Some(app_id) = app_id {
                    ensure_guild_commands(app, ctx, app_id, guild_id.to_owned());
                }
            }

            let members: std::collections::HashMap<&str, &Value> = data
                .get("members")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
                .filter_map(|m| {
                    if let Some(u) = m.get("user") {
                        ctx.cache_user(u);
                    }
                    m.pointer("/user/id")
                        .and_then(Value::as_str)
                        .map(|id| (id, m))
                })
                .collect();

            let mut member_ids: HashSet<String> =
                members.keys().map(|id| (*id).to_owned()).collect();
            let mut online_ids: HashSet<String> = HashSet::new();
            for presence in data
                .get("presences")
                .and_then(Value::as_array)
                .into_iter()
                .flatten()
            {
                if let Some(u) = presence.get("user") {
                    ctx.cache_user(u);
                }
                if let Some(id) = presence.pointer("/user/id").and_then(Value::as_str) {
                    member_ids.insert(id.to_owned());
                    if presence.get("status").and_then(Value::as_str) != Some("offline") {
                        online_ids.insert(id.to_owned());
                    }
                }
                let member_from_guild = presence
                    .pointer("/user/id")
                    .and_then(Value::as_str)
                    .and_then(|id| members.get(id).copied());
                let cached_fallback = if member_from_guild.is_none() {
                    presence
                        .pointer("/user/id")
                        .and_then(Value::as_str)
                        .and_then(|id| ctx.cached_member(id))
                } else {
                    None
                };
                if let Some((uid, normalized)) =
                    normalize_presence(presence, member_from_guild.or(cached_fallback.as_ref()))
                {
                    ctx.enqueue_presence(app, uid, normalized);
                }
            }

            let member_count = data.get("member_count").and_then(Value::as_u64);
            let complete_snapshot = member_count.is_some_and(|c| c as usize <= member_ids.len());
            let guild_member_list: Vec<String> = member_ids.iter().cloned().collect();
            ctx.set_guild_members(guild_id, member_ids);
            ctx.set_guild_online(guild_id, online_ids);
            let all_online = ctx.all_online_users();

            let app_bg = Arc::clone(app);
            let ctx_bg = Arc::clone(ctx);
            let gid_bg = guild_id.to_owned();
            tokio::spawn(async move {
                reconcile_guild_members(&app_bg, &ctx_bg, &gid_bg, complete_snapshot).await;
                if let Err(err) = app_bg
                    .store
                    .reconcile_offline_presences(&guild_member_list, &all_online)
                    .await
                {
                    tracing::warn!(error = %err, guild_id = %gid_bg, "failed reconciling offline presences");
                }
            });
        }
        "PRESENCE_UPDATE" => {
            let guild_id = data.get("guild_id").and_then(Value::as_str);
            if !guild_allowed(app, guild_id) {
                return Ok(());
            }
            if let Some(u) = data.get("user") {
                ctx.cache_user(u);
            }
            let status = data
                .get("status")
                .and_then(Value::as_str)
                .unwrap_or("offline");
            let uid = data.pointer("/user/id").and_then(Value::as_str);
            if let (Some(gid), Some(user_id)) = (guild_id, uid) {
                ctx.add_guild_member(gid, user_id);
                ctx.update_guild_online(gid, user_id, status);
            }
            let cached = uid.and_then(|id| ctx.cached_member(id));
            if let Some((uid, normalized)) = normalize_presence(data, cached.as_ref()) {
                ctx.enqueue_presence(app, uid, normalized);
            }
        }
        "GUILD_MEMBER_ADD" | "GUILD_MEMBER_UPDATE" => {
            let Some(guild_id) = data.get("guild_id").and_then(Value::as_str) else {
                return Ok(());
            };
            if !guild_allowed(app, Some(guild_id)) {
                return Ok(());
            }
            if let Some(u) = data.get("user") {
                ctx.cache_user(u);
            }
            if let Some(user_id) = data.pointer("/user/id").and_then(Value::as_str) {
                ctx.add_guild_member(guild_id, user_id);
            }
        }
        "GUILD_MEMBER_REMOVE" => {
            let Some(guild_id) = data.get("guild_id").and_then(Value::as_str) else {
                return Ok(());
            };
            if !guild_allowed(app, Some(guild_id)) {
                return Ok(());
            }
            if let Some(user_id) = data.pointer("/user/id").and_then(Value::as_str) {
                ctx.remove_guild_member(guild_id, user_id);
                let app_bg = Arc::clone(app);
                let ctx_bg = Arc::clone(ctx);
                let uid = user_id.to_owned();
                tokio::spawn(async move {
                    if !is_user_in_monitored_guild(&app_bg, &ctx_bg, &uid).await
                        && !ctx_bg.is_cached_member_anywhere(&uid)
                    {
                        ctx_bg.cancel_pending(&uid);
                        ctx_bg.enqueue_purge(uid);
                    }
                });
            }
        }
        "GUILD_DELETE" => {
            let Some(guild_id) = data.get("id").and_then(Value::as_str) else {
                return Ok(());
            };
            if !guild_allowed(app, Some(guild_id)) {
                return Ok(());
            }
            if data.get("unavailable").and_then(Value::as_bool) != Some(true) {
                ctx.remove_guild(guild_id);
                let app_bg = Arc::clone(app);
                let ctx_bg = Arc::clone(ctx);
                let gid_bg = guild_id.to_owned();
                tokio::spawn(async move {
                    reconcile_guild_members(&app_bg, &ctx_bg, &gid_bg, false).await;
                });
            }
        }
        "INTERACTION_CREATE" => {
            let app = Arc::clone(app);
            let ctx = Arc::clone(ctx);
            let payload = data.clone();
            tokio::spawn(async move {
                if let Err(err) = handle_interaction(&app, &ctx, &payload).await {
                    tracing::warn!(error = %err, "discord interaction failed");
                }
            });
        }
        _ => {}
    }
    Ok(())
}

fn commands_payload() -> Value {
    json!([
        {
            "name": "connect",
            "description": "Connect your GitHub account",
            "type": 1
        },
        {
            "name": "apikey",
            "description": "Generate or rotate your Taggle KV API key",
            "type": 1
        },
        {
            "name": "status",
            "description": "Show your Taggle monitoring status and API endpoints",
            "type": 1
        },
        {
            "name": "kv",
            "description": "Manage your Discord KV key-value pairs",
            "type": 1,
            "options": [
                {
                    "name": "set",
                    "description": "Set a KV pair",
                    "type": 1,
                    "options": [
                        { "name": "key", "description": "Key ([A-Za-z0-9_])", "type": 3, "required": true },
                        { "name": "value", "description": "Value string", "type": 3, "required": true }
                    ]
                },
                {
                    "name": "delete",
                    "description": "Delete a KV key",
                    "type": 1,
                    "options": [
                        { "name": "key", "description": "Key to delete", "type": 3, "required": true }
                    ]
                },
                {
                    "name": "list",
                    "description": "List your current Discord KV pairs",
                    "type": 1
                }
            ]
        }
    ])
}

fn spawn_command_registration(app: &Arc<App>, ctx: &Arc<BotContext>, app_id: String) {
    let Some(discord) = app.config.discord.as_ref() else {
        return;
    };
    let bot_token = discord.bot_token.clone();
    let guild_ids = discord.guild_ids.clone();
    let ctx = Arc::clone(ctx);

    tokio::spawn(async move {
        // Clear any stale global commands so guild-scoped commands never appear twice.
        if let Err(err) = put_commands(
            &ctx.http,
            &bot_token,
            &format!("{DISCORD_API}/applications/{app_id}/commands"),
            &json!([]),
        )
        .await
        {
            tracing::warn!(error = %err, "failed clearing global discord commands");
        }

        let payload = commands_payload();
        for guild_id in guild_ids {
            let gid = guild_id.to_string();
            ctx.registered_guilds.lock().unwrap().insert(gid.clone());
            let url = format!("{DISCORD_API}/applications/{app_id}/guilds/{gid}/commands");
            if let Err(err) = put_commands(&ctx.http, &bot_token, &url, &payload).await {
                tracing::warn!(error = %err, guild_id = %gid, "failed registering guild slash commands");
            } else {
                tracing::info!(guild_id = %gid, "registered Taggle slash commands");
            }
        }
    });
}

fn ensure_guild_commands(app: &Arc<App>, ctx: &Arc<BotContext>, app_id: String, guild_id: String) {
    {
        let mut reg = ctx.registered_guilds.lock().unwrap();
        if !reg.insert(guild_id.clone()) {
            return;
        }
    }
    let Some(discord) = app.config.discord.as_ref() else {
        return;
    };
    let bot_token = discord.bot_token.clone();
    let ctx = Arc::clone(ctx);

    tokio::spawn(async move {
        let url = format!("{DISCORD_API}/applications/{app_id}/guilds/{guild_id}/commands");
        if let Err(err) = put_commands(&ctx.http, &bot_token, &url, &commands_payload()).await {
            ctx.registered_guilds.lock().unwrap().remove(&guild_id);
            tracing::warn!(error = %err, %guild_id, "failed registering guild slash commands");
        } else {
            tracing::info!(%guild_id, "registered Taggle slash commands");
        }
    });
}

async fn put_commands(
    http: &reqwest::Client,
    bot_token: &str,
    url: &str,
    body: &Value,
) -> Result<()> {
    let resp = http
        .put(url)
        .header("Authorization", format!("Bot {bot_token}"))
        .json(body)
        .send()
        .await
        .map_err(|err| anyhow::anyhow!("discord request failed: {}", err.without_url()))?;
    if !resp.status().is_success() {
        let status = resp.status();
        let text: String = resp
            .text()
            .await
            .unwrap_or_default()
            .chars()
            .take(300)
            .collect();
        bail!("discord command registration failed ({status}): {text}");
    }
    Ok(())
}

fn valid_interaction_token(token: &str) -> bool {
    !token.is_empty()
        && token.len() <= 512
        && !token
            .chars()
            .any(|c| c.is_whitespace() || matches!(c, '/' | '?' | '#'))
}

fn interaction_user_id(data: &Value) -> Option<&str> {
    data.pointer("/member/user/id")
        .or_else(|| data.pointer("/user/id"))
        .and_then(Value::as_str)
        .filter(|id| valid_snowflake(id))
}

fn interaction_user_meta(data: &Value) -> (Option<&str>, Option<&str>, Option<&str>) {
    let user = data.pointer("/member/user").or_else(|| data.get("user"));
    let username = user.and_then(|u| u.get("username")).and_then(Value::as_str);
    let global_name = user
        .and_then(|u| u.get("global_name"))
        .and_then(Value::as_str);
    let avatar = user
        .and_then(|u| u.get("avatar"))
        .and_then(Value::as_str)
        .filter(|a| valid_avatar_hash(a));
    (username, global_name, avatar)
}

async fn handle_interaction(app: &Arc<App>, ctx: &Arc<BotContext>, data: &Value) -> Result<()> {
    let Some(id) = data
        .get("id")
        .and_then(Value::as_str)
        .filter(|id| valid_snowflake(id))
    else {
        return Ok(());
    };
    let Some(token) = data
        .get("token")
        .and_then(Value::as_str)
        .filter(|t| valid_interaction_token(t))
    else {
        return Ok(());
    };
    let Some(kind) = data.get("type").and_then(Value::as_u64) else {
        return Ok(());
    };
    let Some(user_id) = interaction_user_id(data).map(str::to_owned) else {
        return Ok(());
    };

    app.metrics
        .discord_interactions
        .fetch_add(1, Ordering::Relaxed);

    if let Some(guild_id) = data.get("guild_id").and_then(Value::as_str) {
        if !guild_allowed(app, Some(guild_id)) {
            return send_interaction_callback(
                &ctx.http,
                id,
                token,
                4,
                Some(ephemeral_data(error_message(
                    "This server is not monitored by Taggle.",
                    false,
                ))),
            )
            .await;
        }
        ctx.add_guild_member(guild_id, &user_id);
    } else if !is_user_in_monitored_guild(app, ctx, &user_id).await {
        return send_interaction_callback(
            &ctx.http,
            id,
            token,
            4,
            Some(ephemeral_data(error_message(
                "You must be a member of a monitored Discord server to use Taggle commands.",
                false,
            ))),
        )
        .await;
    }

    match kind {
        // APPLICATION_COMMAND
        2 => {
            let cmd = data
                .pointer("/data/name")
                .and_then(Value::as_str)
                .unwrap_or("");
            match cmd {
                "connect" => {
                    let existing = app
                        .store
                        .get_discord_github_link(&user_id)
                        .await
                        .unwrap_or(None);
                    let msg = connect_prompt_message(existing.as_deref());
                    send_interaction_callback(&ctx.http, id, token, 4, Some(ephemeral_data(msg)))
                        .await?;
                }
                "apikey" => {
                    let existing = app
                        .store
                        .get_discord_github_link(&user_id)
                        .await
                        .unwrap_or(None);
                    let msg = apikey_prompt_message(existing.as_deref());
                    send_interaction_callback(&ctx.http, id, token, 4, Some(ephemeral_data(msg)))
                        .await?;
                }
                "status" => {
                    let existing = app
                        .store
                        .get_discord_github_link(&user_id)
                        .await
                        .unwrap_or(None);
                    let kv_count = app
                        .store
                        .list_discord_kv(&user_id)
                        .await
                        .map(|v| v.len())
                        .unwrap_or(0);
                    let msg = status_message(&user_id, existing.as_deref(), kv_count);
                    send_interaction_callback(&ctx.http, id, token, 4, Some(ephemeral_data(msg)))
                        .await?;
                }
                "kv" => {
                    let msg = handle_kv_command(app, &user_id, data).await;
                    send_interaction_callback(&ctx.http, id, token, 4, Some(ephemeral_data(msg)))
                        .await?;
                }
                _ => {}
            }
        }
        // MESSAGE_COMPONENT (button click)
        3 => {
            let custom_id = data
                .pointer("/data/custom_id")
                .and_then(Value::as_str)
                .unwrap_or("");
            match custom_id {
                "connect_github" => {
                    // Ack immediately (type 6 = DEFERRED_UPDATE_MESSAGE) so Discord's 3s window never expires.
                    send_interaction_callback(&ctx.http, id, token, 6, None).await?;
                    if !ctx.allow_connect_click(&user_id) {
                        return Ok(());
                    }
                    start_github_device_flow(app, ctx, &user_id, token).await?;
                }
                "cancel_github_connect" => {
                    ctx.cancel_pending(&user_id);
                    let existing = app
                        .store
                        .get_discord_github_link(&user_id)
                        .await
                        .unwrap_or(None);
                    let msg = connect_prompt_message(existing.as_deref());
                    send_interaction_callback(&ctx.http, id, token, 7, Some(msg)).await?;
                }
                "disconnect_github" => {
                    ctx.cancel_pending(&user_id);
                    app.store.unlink_discord_github(&user_id).await?;
                    let msg = connect_prompt_message(None);
                    send_interaction_callback(&ctx.http, id, token, 7, Some(msg)).await?;
                }
                "rotate_discord_key" => {
                    if !ctx.allow_connect_click(&user_id) {
                        return Ok(());
                    }
                    let (username, global_name, avatar) = interaction_user_meta(data);
                    let key = app
                        .store
                        .rotate_discord_api_key(&user_id, username, global_name, avatar)
                        .await?;
                    let msg = issued_apikey_message(
                        "Discord KV",
                        &format!("/v1/discord/users/{user_id}/kv"),
                        &key,
                    );
                    send_interaction_callback(&ctx.http, id, token, 4, Some(ephemeral_data(msg)))
                        .await?;
                }
                "rotate_github_key" => {
                    if !ctx.allow_connect_click(&user_id) {
                        return Ok(());
                    }
                    let msg = match app.store.rotate_github_api_key(&user_id).await? {
                        Some((login, key)) => issued_apikey_message(
                            "GitHub KV",
                            &format!("/v1/github/users/{login}/kv"),
                            &key,
                        ),
                        None => {
                            error_message("Connect a GitHub account first with `/connect`.", false)
                        }
                    };
                    send_interaction_callback(&ctx.http, id, token, 4, Some(ephemeral_data(msg)))
                        .await?;
                }
                _ => {}
            }
        }
        _ => {}
    }

    Ok(())
}

async fn handle_kv_command(app: &App, user_id: &str, data: &Value) -> Value {
    let sub = data.pointer("/data/options/0");
    let sub_name = sub
        .and_then(|s| s.get("name"))
        .and_then(Value::as_str)
        .unwrap_or("");
    let opt = |name: &str| -> Option<&str> {
        sub.and_then(|s| s.get("options"))
            .and_then(Value::as_array)
            .and_then(|opts| {
                opts.iter()
                    .find(|o| o.get("name").and_then(Value::as_str) == Some(name))
                    .and_then(|o| o.get("value"))
                    .and_then(Value::as_str)
            })
    };

    match sub_name {
        "set" => {
            let (Some(k), Some(v)) = (opt("key"), opt("value")) else {
                return error_message("Provide both `key` and `value`.", false);
            };
            match app.store.set_discord_kv(user_id, k, v).await {
                Ok(()) => json!({
                    "embeds": [{
                        "title": "KV Updated",
                        "description": format!("Set `{k}`."),
                        "color": COLOR_GREEN,
                    }]
                }),
                Err(err) => error_message(&format!("Could not set KV: {err}"), false),
            }
        }
        "delete" => {
            let Some(k) = opt("key") else {
                return error_message("Provide `key`.", false);
            };
            match app.store.delete_discord_kv(user_id, k).await {
                Ok(true) => json!({
                    "embeds": [{
                        "title": "KV Deleted",
                        "description": format!("Removed `{k}`."),
                        "color": COLOR_GREEN,
                    }]
                }),
                Ok(false) => json!({
                    "embeds": [{
                        "title": "KV Not Found",
                        "description": format!("Key `{k}` was not set."),
                        "color": COLOR_BLURPLE,
                    }]
                }),
                Err(err) => error_message(&format!("Could not delete KV: {err}"), false),
            }
        }
        "list" => match app.store.list_discord_kv(user_id).await {
            Ok(pairs) if pairs.is_empty() => json!({
                "embeds": [{
                    "title": "Discord KV",
                    "description": "No KV pairs stored yet. Use `/kv set` or your API key.",
                    "color": COLOR_BLURPLE,
                }]
            }),
            Ok(pairs) => {
                let mut lines = String::new();
                for (k, v) in pairs.iter().take(20) {
                    let preview: String = v.chars().take(60).collect();
                    let suffix = if v.chars().count() > 60 { "…" } else { "" };
                    lines.push_str(&format!("`{k}` = `{preview}{suffix}`\n"));
                }
                if pairs.len() > 20 {
                    lines.push_str(&format!("\n…and {} more", pairs.len() - 20));
                }
                json!({
                    "embeds": [{
                        "title": format!("Discord KV ({} keys)", pairs.len()),
                        "description": lines,
                        "color": COLOR_BLURPLE,
                    }]
                })
            }
            Err(err) => error_message(&format!("Could not load KV: {err}"), false),
        },
        _ => error_message("Unknown `/kv` subcommand.", false),
    }
}

#[derive(Deserialize)]
struct DeviceCodeResponse {
    device_code: Option<String>,
    user_code: Option<String>,
    verification_uri: Option<String>,
    expires_in: Option<u64>,
    interval: Option<u64>,
    error: Option<String>,
    error_description: Option<String>,
}

#[derive(Deserialize)]
struct AccessTokenResponse {
    access_token: Option<String>,
    error: Option<String>,
    error_description: Option<String>,
    interval: Option<u64>,
}

async fn start_github_device_flow(
    app: &Arc<App>,
    ctx: &Arc<BotContext>,
    discord_user_id: &str,
    interaction_token: &str,
) -> Result<()> {
    let app_id = ctx
        .application_id
        .lock()
        .unwrap()
        .clone()
        .context("missing discord application_id")?;

    let Some(client_id) = app.config.github_client_id.clone() else {
        let msg = error_message(
            "GitHub OAuth is not configured (`GITHUB_APP_CLIENT_ID` is missing).",
            false,
        );
        return edit_original_interaction(&ctx.http, &app_id, interaction_token, &msg).await;
    };

    let resp = ctx
        .http
        .post("https://github.com/login/device/code")
        .header("Accept", "application/json")
        .json(&json!({ "client_id": client_id, "scope": "read:user" }))
        .send()
        .await?;

    let body_text = resp.text().await.unwrap_or_default();
    let parsed: DeviceCodeResponse =
        serde_json::from_str(&body_text).unwrap_or(DeviceCodeResponse {
            device_code: None,
            user_code: None,
            verification_uri: None,
            expires_in: None,
            interval: None,
            error: Some("invalid_response".into()),
            error_description: Some(body_text.chars().take(300).collect()),
        });

    if let Some(err) = parsed.error {
        let detail = if err == "device_flow_disabled" {
            "Enable **Device Flow** in your GitHub App settings to connect through Discord."
                .to_owned()
        } else {
            parsed.error_description.unwrap_or(err)
        };
        let msg = error_message(&detail, true);
        return edit_original_interaction(&ctx.http, &app_id, interaction_token, &msg).await;
    }

    let (Some(device_code), Some(user_code)) = (parsed.device_code, parsed.user_code) else {
        let msg = error_message("GitHub did not return a device verification code.", true);
        return edit_original_interaction(&ctx.http, &app_id, interaction_token, &msg).await;
    };

    let verification_uri = parsed
        .verification_uri
        .filter(|uri| uri.starts_with("https://github.com/"))
        .unwrap_or_else(|| "https://github.com/login/device".to_owned());
    let expires_in = parsed.expires_in.unwrap_or(900).min(880);
    let interval = parsed.interval.unwrap_or(5).max(5);

    let prompt = device_flow_message(&user_code, &verification_uri);
    edit_original_interaction(&ctx.http, &app_id, interaction_token, &prompt).await?;

    let task_id = ctx.next_task_id.fetch_add(1, Ordering::Relaxed);
    let app_bg = Arc::clone(app);
    let ctx_bg = Arc::clone(ctx);
    let user_id_bg = discord_user_id.to_owned();
    let token_bg = interaction_token.to_owned();

    let join = tokio::spawn(async move {
        let outcome = poll_github_device_token(
            &app_bg,
            &ctx_bg,
            &app_id,
            &user_id_bg,
            &token_bg,
            &client_id,
            &device_code,
            expires_in,
            interval,
        )
        .await;

        if let Err(err) = outcome {
            tracing::warn!(error = %err, discord_user_id = %user_id_bg, "github device flow failed");
            let msg = error_message(
                "Could not complete GitHub authorization. Please try again.",
                true,
            );
            let _ = edit_original_interaction(&ctx_bg.http, &app_id, &token_bg, &msg).await;
        }
        ctx_bg.clear_pending_if_match(&user_id_bg, task_id);
    });

    ctx.register_pending(discord_user_id, task_id, join.abort_handle());
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn poll_github_device_token(
    app: &Arc<App>,
    ctx: &Arc<BotContext>,
    app_id: &str,
    discord_user_id: &str,
    interaction_token: &str,
    client_id: &str,
    device_code: &str,
    expires_in: u64,
    mut interval: u64,
) -> Result<()> {
    let deadline = Instant::now() + Duration::from_secs(expires_in);
    let mut transient_failures: u32 = 0;

    loop {
        tokio::time::sleep(Duration::from_secs(interval)).await;
        if Instant::now() >= deadline {
            let msg = error_message(
                "GitHub authorization code expired. Click below to try again.",
                true,
            );
            edit_original_interaction(&ctx.http, app_id, interaction_token, &msg).await?;
            return Ok(());
        }

        let resp = match ctx
            .http
            .post("https://github.com/login/oauth/access_token")
            .header("Accept", "application/json")
            .json(&json!({
                "client_id": client_id,
                "device_code": device_code,
                "grant_type": "urn:ietf:params:oauth:grant-type:device_code",
            }))
            .send()
            .await
        {
            Ok(resp) => resp,
            Err(err) => {
                transient_failures += 1;
                if transient_failures > 5 {
                    return Err(err.into());
                }
                tracing::debug!(error = %err.without_url(), transient_failures, "transient github poll network error");
                continue;
            }
        };

        let body: AccessTokenResponse = match resp.json().await {
            Ok(body) => {
                transient_failures = 0;
                body
            }
            Err(err) => {
                transient_failures += 1;
                if transient_failures > 5 {
                    return Err(err.into());
                }
                tracing::debug!(error = %err, transient_failures, "transient github poll decode error");
                continue;
            }
        };
        if let Some(access_token) = body.access_token {
            return complete_github_connection(
                app,
                ctx,
                app_id,
                discord_user_id,
                interaction_token,
                &access_token,
            )
            .await;
        }

        match body.error.as_deref() {
            Some("authorization_pending") => continue,
            Some("slow_down") => {
                interval = body.interval.unwrap_or(interval + 5).max(interval + 5);
            }
            Some("expired_token") => {
                let msg = error_message(
                    "GitHub authorization code expired. Click below to try again.",
                    true,
                );
                edit_original_interaction(&ctx.http, app_id, interaction_token, &msg).await?;
                return Ok(());
            }
            Some("access_denied") => {
                let msg = error_message("GitHub authorization was cancelled.", true);
                edit_original_interaction(&ctx.http, app_id, interaction_token, &msg).await?;
                return Ok(());
            }
            Some(other) => {
                let detail = body.error_description.unwrap_or_else(|| other.to_owned());
                let msg = error_message(&detail, true);
                edit_original_interaction(&ctx.http, app_id, interaction_token, &msg).await?;
                return Ok(());
            }
            None => continue,
        }
    }
}

async fn complete_github_connection(
    app: &Arc<App>,
    ctx: &Arc<BotContext>,
    app_id: &str,
    discord_user_id: &str,
    interaction_token: &str,
    access_token: &str,
) -> Result<()> {
    let resp = ctx
        .http
        .get("https://api.github.com/user")
        .bearer_auth(access_token)
        .header("Accept", "application/vnd.github+json")
        .send()
        .await?;

    if !resp.status().is_success() {
        bail!("github /user failed: {}", resp.status());
    }

    let gh_user: Value = resp.json().await?;
    let gh_id = gh_user
        .get("id")
        .and_then(Value::as_i64)
        .context("missing github user id")?;
    let gh_login = gh_user
        .get("login")
        .and_then(Value::as_str)
        .context("missing github login")?;
    let gh_name = gh_user.get("name").and_then(Value::as_str);
    let gh_avatar = gh_user.get("avatar_url").and_then(Value::as_str);

    if !is_user_in_monitored_guild(app, ctx, discord_user_id).await {
        let msg = error_message(
            "You must remain in the monitored Discord server to connect GitHub.",
            false,
        );
        edit_original_interaction(&ctx.http, app_id, interaction_token, &msg).await?;
        return Ok(());
    }

    let login_lower = app
        .store
        .upsert_github_user_oauth(gh_id, gh_login, gh_name, gh_avatar)
        .await?;
    app.store
        .link_discord_github(discord_user_id, gh_id, &login_lower)
        .await?;

    if app.github.is_some()
        && let Err(err) = crate::sync::sync_user(app, &login_lower, Some(access_token)).await
    {
        tracing::warn!(error = %err, login = %login_lower, "initial github sync after /connect failed");
    }

    let msg = connected_message(&login_lower, gh_avatar);
    edit_original_interaction(&ctx.http, app_id, interaction_token, &msg).await?;
    tracing::info!(discord_user_id, github_login = %login_lower, "github account connected via discord");
    Ok(())
}

async fn reconcile_guild_members(
    app: &Arc<App>,
    ctx: &Arc<BotContext>,
    _guild_id: &str,
    complete_snapshot: bool,
) {
    let tracked = match app.store.list_tracked_discord_user_ids().await {
        Ok(ids) => ids,
        Err(err) => {
            tracing::warn!(error = %err, "failed listing tracked discord users for reconciliation");
            return;
        }
    };

    let single_configured_guild = app
        .config
        .discord
        .as_ref()
        .is_some_and(|d| d.guild_ids.len() <= 1 && ctx.known_guild_ids().len() <= 1);

    for uid in tracked {
        if ctx.is_cached_member_anywhere(&uid) {
            continue;
        }
        let still_member = if complete_snapshot && single_configured_guild {
            false
        } else {
            is_user_in_monitored_guild(app, ctx, &uid).await
        };

        if !still_member {
            ctx.cancel_pending(&uid);
            ctx.enqueue_purge(uid);
        }
    }
}

async fn is_user_in_monitored_guild(app: &App, ctx: &BotContext, user_id: &str) -> bool {
    if ctx.is_cached_member_anywhere(user_id) {
        return true;
    }
    let Some(discord) = app.config.discord.as_ref() else {
        return false;
    };

    let mut guild_ids: Vec<String> = if discord.guild_ids.is_empty() {
        ctx.known_guild_ids()
    } else {
        discord.guild_ids.iter().map(ToString::to_string).collect()
    };

    if guild_ids.is_empty() {
        // Startup before GUILD_CREATE: ask Discord REST API which guilds the bot belongs to.
        if let Ok(resp) = ctx
            .http
            .get(format!("{DISCORD_API}/users/@me/guilds"))
            .header("Authorization", format!("Bot {}", discord.bot_token))
            .send()
            .await
            && resp.status().is_success()
            && let Ok(guilds) = resp.json::<Vec<Value>>().await
        {
            guild_ids = guilds
                .into_iter()
                .filter_map(|g| {
                    g.get("id")
                        .and_then(Value::as_str)
                        .filter(|id| valid_snowflake(id))
                        .map(str::to_owned)
                })
                .collect();
        }
        if guild_ids.is_empty() {
            return false;
        }
    }

    for gid in guild_ids {
        let url = format!("{DISCORD_API}/guilds/{gid}/members/{user_id}");
        match ctx
            .http
            .get(&url)
            .header("Authorization", format!("Bot {}", discord.bot_token))
            .send()
            .await
        {
            Ok(resp) if resp.status().is_success() => {
                ctx.add_guild_member(&gid, user_id);
                return true;
            }
            Ok(resp) if resp.status() == reqwest::StatusCode::NOT_FOUND => continue,
            Ok(resp) => {
                tracing::warn!(status = %resp.status(), guild_id = %gid, user_id, "unexpected discord member check status");
                return true;
            }
            Err(err) => {
                tracing::warn!(error = %err.without_url(), guild_id = %gid, user_id, "discord member check failed");
                return true;
            }
        }
    }

    false
}

async fn send_interaction_callback(
    http: &reqwest::Client,
    interaction_id: &str,
    interaction_token: &str,
    kind: u64,
    data: Option<Value>,
) -> Result<()> {
    let url = format!("{DISCORD_API}/interactions/{interaction_id}/{interaction_token}/callback");
    let body = match data {
        Some(d) => json!({ "type": kind, "data": d }),
        None => json!({ "type": kind }),
    };
    let resp = http
        .post(url)
        .json(&body)
        .send()
        .await
        .map_err(|err| anyhow::anyhow!("discord request failed: {}", err.without_url()))?;
    if !resp.status().is_success() {
        let status = resp.status();
        let text: String = resp
            .text()
            .await
            .unwrap_or_default()
            .chars()
            .take(300)
            .collect();
        bail!("discord interaction callback failed ({status}): {text}");
    }
    Ok(())
}

async fn edit_original_interaction(
    http: &reqwest::Client,
    app_id: &str,
    interaction_token: &str,
    data: &Value,
) -> Result<()> {
    let url = format!("{DISCORD_API}/webhooks/{app_id}/{interaction_token}/messages/@original");
    let resp = http
        .patch(url)
        .json(data)
        .send()
        .await
        .map_err(|err| anyhow::anyhow!("discord request failed: {}", err.without_url()))?;
    if !resp.status().is_success() {
        let status = resp.status();
        let text: String = resp
            .text()
            .await
            .unwrap_or_default()
            .chars()
            .take(300)
            .collect();
        bail!("discord interaction edit failed ({status}): {text}");
    }
    Ok(())
}

fn ephemeral_data(mut data: Value) -> Value {
    if let Some(obj) = data.as_object_mut() {
        obj.insert("flags".into(), json!(64));
    }
    data
}

fn connect_prompt_message(existing_login: Option<&str>) -> Value {
    match existing_login {
        Some(login) => json!({
            "embeds": [{
                "title": "Connected",
                "description": format!("GitHub account **@{login}** is connected."),
                "color": COLOR_GREEN,
            }],
            "components": [{
                "type": 1,
                "components": [
                    {
                        "type": 2,
                        "style": 3,
                        "custom_id": "github_connected",
                        "label": "Connected",
                        "disabled": true,
                    },
                    {
                        "type": 2,
                        "style": 2,
                        "custom_id": "connect_github",
                        "label": "Reconnect",
                    },
                    {
                        "type": 2,
                        "style": 4,
                        "custom_id": "disconnect_github",
                        "label": "Disconnect",
                    }
                ]
            }]
        }),
        None => json!({
            "embeds": [{
                "title": "GitHub",
                "description": "Click the button below to connect your GitHub account.",
                "color": COLOR_BLURPLE,
            }],
            "components": [{
                "type": 1,
                "components": [{
                    "type": 2,
                    "style": 1,
                    "custom_id": "connect_github",
                    "label": "GitHub",
                }]
            }]
        }),
    }
}

fn device_flow_message(user_code: &str, verification_uri: &str) -> Value {
    json!({
        "embeds": [{
            "title": "Connect GitHub",
            "description": format!(
                "1. Copy your code: **`{user_code}`**\n2. Click **Open GitHub** below and enter the code.\n\nWaiting for authorization..."
            ),
            "color": COLOR_BLURPLE,
            "footer": { "text": "Expires in 15 minutes" }
        }],
        "components": [{
            "type": 1,
            "components": [
                {
                    "type": 2,
                    "style": 5,
                    "label": "Open GitHub",
                    "url": verification_uri,
                },
                {
                    "type": 2,
                    "style": 4,
                    "custom_id": "cancel_github_connect",
                    "label": "Cancel",
                }
            ]
        }]
    })
}

fn connected_message(login: &str, avatar_url: Option<&str>) -> Value {
    let mut embed = json!({
        "title": "Connected",
        "description": format!("GitHub account **@{login}** is connected."),
        "color": COLOR_GREEN,
    });
    if let Some(url) = avatar_url
        && !url.is_empty()
    {
        embed["thumbnail"] = json!({ "url": url });
    }

    json!({
        "embeds": [embed],
        "components": [{
            "type": 1,
            "components": [
                {
                    "type": 2,
                    "style": 3,
                    "custom_id": "github_connected",
                    "label": "Connected",
                    "disabled": true,
                },
                {
                    "type": 2,
                    "style": 4,
                    "custom_id": "disconnect_github",
                    "label": "Disconnect",
                }
            ]
        }]
    })
}

fn error_message(message: &str, retry: bool) -> Value {
    let components = if retry {
        json!([{
            "type": 1,
            "components": [{
                "type": 2,
                "style": 1,
                "custom_id": "connect_github",
                "label": "Try Again",
            }]
        }])
    } else {
        json!([])
    };

    json!({
        "embeds": [{
            "title": "Taggle Error",
            "description": message,
            "color": COLOR_RED,
        }],
        "components": components,
    })
}

fn apikey_prompt_message(existing_github: Option<&str>) -> Value {
    let mut buttons = vec![json!({
        "type": 2,
        "style": 1,
        "custom_id": "rotate_discord_key",
        "label": "Discord KV Key",
    })];
    if existing_github.is_some() {
        buttons.push(json!({
            "type": 2,
            "style": 2,
            "custom_id": "rotate_github_key",
            "label": "GitHub KV Key",
        }));
    }

    let desc = match existing_github {
        Some(login) => format!(
            "Generate a bearer API key (`tggl_...`) for updating your KV store.\n\n• **Discord KV** is ready.\n• **GitHub KV** is linked to **@{login}**.\n\nGenerating a new key immediately replaces the previous one."
        ),
        None => "Generate a bearer API key (`tggl_...`) for updating your Discord KV store.\n\nConnect GitHub via `/connect` to also issue a GitHub KV key.\n\nGenerating a new key immediately replaces the previous one.".to_owned(),
    };

    json!({
        "embeds": [{
            "title": "API Keys",
            "description": desc,
            "color": COLOR_BLURPLE,
        }],
        "components": [{
            "type": 1,
            "components": buttons,
        }]
    })
}

fn issued_apikey_message(scope: &str, endpoint_path: &str, raw_key: &str) -> Value {
    json!({
        "embeds": [{
            "title": format!("{scope} API Key Issued"),
            "description": format!(
                "Copy this key now — **it will not be shown again** (only its SHA-256 hash is stored).\n\n```\n{raw_key}\n```\n**Example:**\n```http\nPUT {endpoint_path}/currently_building\nAuthorization: Bearer {raw_key}\n{{\"value\": \"taggle\"}}\n```"
            ),
            "color": COLOR_GREEN,
        }]
    })
}

fn status_message(user_id: &str, github_login: Option<&str>, kv_count: usize) -> Value {
    let gh_line = match github_login {
        Some(login) => format!("**Connected:** `@{login}` (`/v1/github/users/{login}`)"),
        None => "Not connected (run `/connect` to link GitHub)".to_owned(),
    };

    json!({
        "embeds": [{
            "title": "Taggle Status",
            "color": COLOR_BLURPLE,
            "fields": [
                { "name": "Discord Presence", "value": format!("Monitored (`/v1/discord/users/{user_id}`)\nKV pairs: `{kv_count}`"), "inline": false },
                { "name": "GitHub Activity", "value": gh_line, "inline": false },
                { "name": "Unified Profile & SVG", "value": format!("Profile JSON: `/v1/profiles/{user_id}`\nREADME Card: `/v1/profiles/{user_id}/card.svg`\nBadge: `/v1/users/{user_id}/badge.svg`"), "inline": false },
            ]
        }]
    })
}

fn guild_allowed(app: &App, guild_id: Option<&str>) -> bool {
    let Some(discord) = app.config.discord.as_ref() else {
        return false;
    };
    if discord.guild_ids.is_empty() {
        return true;
    }
    guild_id
        .and_then(|id| id.parse::<u64>().ok())
        .is_some_and(|id| discord.guild_ids.contains(&id))
}

fn valid_avatar_hash(avatar: &str) -> bool {
    !avatar.is_empty()
        && avatar.len() <= 64
        && avatar
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_')
}

fn sanitize_activity(raw: &Value) -> Option<Value> {
    let obj = raw.as_object()?;
    let mut clean = serde_json::Map::new();
    const ALLOWED_KEYS: &[&str] = &[
        "id",
        "name",
        "type",
        "details",
        "state",
        "timestamps",
        "assets",
        "sync_id",
        "application_id",
        "flags",
        "created_at",
        "emoji",
        "party",
        "url",
    ];
    for key in ALLOWED_KEYS {
        if let Some(val) = obj.get(*key) {
            let v = match val {
                Value::String(s) => Value::String(crate::normalize::truncate_chars(s, 512)),
                other => other.clone(),
            };
            clean.insert((*key).to_owned(), v);
        }
    }
    Some(Value::Object(clean))
}

fn is_coding_editor(name: &str) -> bool {
    matches!(
        name.to_ascii_lowercase().as_str(),
        "visual studio code"
            | "code"
            | "cursor"
            | "zed"
            | "neovim"
            | "nvim"
            | "intellij idea"
            | "webstorm"
            | "pycharm"
            | "rustrover"
            | "clion"
            | "goland"
            | "phpstorm"
            | "rubymine"
            | "datagrip"
            | "android studio"
            | "xcode"
            | "sublime text"
            | "emacs"
            | "helix"
    )
}

fn build_coding(activity: &Value) -> Value {
    let timestamps = activity.get("timestamps").and_then(Value::as_object);
    json!({
        "editor": activity.get("name").cloned().unwrap_or(Value::Null),
        "details": activity.get("details").cloned().unwrap_or(Value::Null),
        "state": activity.get("state").cloned().unwrap_or(Value::Null),
        "timestamps": {
            "start": timestamps.and_then(|t| t.get("start")).cloned().unwrap_or(Value::Null),
            "end": timestamps.and_then(|t| t.get("end")).cloned().unwrap_or(Value::Null),
        },
    })
}

/// Converts a raw Discord presence into Taggle's Lanyard-compatible response shape.
pub fn normalize_presence(data: &Value, member: Option<&Value>) -> Option<(String, Value)> {
    // GUILD_CREATE presences may carry only a partial user object, so fall back
    // to the guild member payload for the missing profile fields.
    let member_user = member.and_then(|m| m.get("user"));
    let user = data.get("user").or(member_user)?;
    let user_id = user.get("id").and_then(Value::as_str)?.to_owned();
    if !valid_snowflake(&user_id) {
        return None;
    }

    let has_full_user = user.get("username").and_then(Value::as_str).is_some();
    let field = |key: &str| {
        user.get(key)
            .filter(|v| has_full_user || !v.is_null())
            .or_else(|| member_user.and_then(|m| m.get(key)))
            .cloned()
            .unwrap_or(Value::Null)
    };

    let avatar = match field("avatar") {
        Value::String(s) if valid_avatar_hash(&s) => Value::String(s),
        _ => Value::Null,
    };

    let client_status = data.get("client_status").and_then(Value::as_object);
    let active = |platform: &str| {
        client_status
            .and_then(|m| m.get(platform))
            .and_then(Value::as_str)
            .is_some_and(|status| status != "offline")
    };

    let activities: Vec<Value> = data
        .get("activities")
        .and_then(Value::as_array)
        .into_iter()
        .flatten()
        .take(20)
        .filter_map(sanitize_activity)
        .collect();
    let spotify_activity = activities
        .iter()
        .find(|activity| activity.get("name").and_then(Value::as_str) == Some("Spotify"));
    let coding_activity = activities.iter().find(|activity| {
        activity
            .get("name")
            .and_then(Value::as_str)
            .is_some_and(is_coding_editor)
    });

    let discord_user = json!({
        "id": user_id,
        "username": field("username"),
        "discriminator": non_null(field("discriminator"), json!("0")),
        "avatar": avatar,
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
        "coding": coding_activity.map(build_coding),
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
        .filter(|image| {
            !image.is_empty()
                && image.len() <= 128
                && image.bytes().all(|b| b.is_ascii_alphanumeric())
        })
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
        assert!(!valid_snowflake("0123"));
        assert!(!valid_snowflake("123abc"));
        assert!(!valid_snowflake("../etc/passwd"));
        assert!(!valid_snowflake("123456789012345678901"));
        assert!(is_discord_gateway_url("wss://gateway.discord.gg"));
        assert!(is_discord_gateway_url(
            "wss://gateway-us-east1-b.discord.gg/"
        ));
        assert!(!is_discord_gateway_url("ws://gateway.discord.gg"));
        assert!(!is_discord_gateway_url(
            "wss://evil.example/gateway.discord.gg"
        ));
        assert!(!is_discord_gateway_url("wss://user@gateway.discord.gg"));
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

        let evil = json!({
            "user": { "id": "123456789012345678", "username": "u" },
            "activities": [{
                "name": "Spotify",
                "assets": { "large_image": "spotify:../../evil?x=1" }
            }]
        });
        let (_, evil_presence) = normalize_presence(&evil, None).unwrap();
        assert_eq!(evil_presence["spotify"]["album_art_url"], "");
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
        let presence =
            json!({ "user": { "id": "123456789" }, "status": "online", "activities": [] });
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

    #[test]
    fn extracts_interaction_user_id_from_guild_or_dm() {
        let guild_ix = json!({ "member": { "user": { "id": "123456789012345678" } } });
        let dm_ix = json!({ "user": { "id": "987654321098765432" } });
        let bad_ix = json!({ "user": { "id": "not-a-snowflake" } });

        assert_eq!(interaction_user_id(&guild_ix), Some("123456789012345678"));
        assert_eq!(interaction_user_id(&dm_ix), Some("987654321098765432"));
        assert_eq!(interaction_user_id(&bad_ix), None);
    }

    #[test]
    fn builds_connect_and_green_connected_messages() {
        let prompt = connect_prompt_message(None);
        assert_eq!(prompt["components"][0]["components"][0]["label"], "GitHub");
        assert_eq!(
            prompt["components"][0]["components"][0]["custom_id"],
            "connect_github"
        );

        let device = device_flow_message("ABCD-1234", "https://github.com/login/device");
        assert!(
            device["embeds"][0]["description"]
                .as_str()
                .unwrap()
                .contains("ABCD-1234")
        );
        assert_eq!(device["components"][0]["components"][0]["style"], 5);
        assert_eq!(
            device["components"][0]["components"][0]["url"],
            "https://github.com/login/device"
        );

        let connected =
            connected_message("octocat", Some("https://avatars.githubusercontent.com/u/1"));
        assert_eq!(connected["embeds"][0]["title"], "Connected");
        assert_eq!(connected["embeds"][0]["color"], COLOR_GREEN);
        assert!(
            connected["embeds"][0]["description"]
                .as_str()
                .unwrap()
                .contains("@octocat")
        );
        assert_eq!(connected["components"][0]["components"][0]["style"], 3);
        assert_eq!(
            connected["components"][0]["components"][0]["label"],
            "Connected"
        );
        assert_eq!(
            connected["components"][0]["components"][0]["disabled"],
            true
        );
    }

    #[test]
    fn sanitizes_activities_extracts_coding_and_validates_tokens() {
        assert!(valid_interaction_token("abc123_def-ghi"));
        assert!(!valid_interaction_token(""));
        assert!(!valid_interaction_token("bad/token"));
        assert!(!valid_interaction_token("bad?token=1"));
        assert!(valid_avatar_hash("a_0123456789abcdef"));
        assert!(!valid_avatar_hash("../evil"));

        let raw = json!({
            "user": { "id": "123456789012345678", "username": "dev", "avatar": "../../evil" },
            "status": "online",
            "activities": [{
                "name": "Neovim",
                "type": 0,
                "details": "Editing router.ex",
                "state": "Workspace: taggle",
                "session_id": "secret-session-id-should-strip"
            }]
        });

        let (_, normalized) = normalize_presence(&raw, None).unwrap();
        assert!(normalized["discord_user"]["avatar"].is_null());
        assert!(normalized["activities"][0].get("session_id").is_none());
        assert_eq!(normalized["coding"]["editor"], "Neovim");
        assert_eq!(normalized["coding"]["details"], "Editing router.ex");
        assert_eq!(normalized["coding"]["state"], "Workspace: taggle");

        let cmds = commands_payload();
        let names: Vec<&str> = cmds
            .as_array()
            .unwrap()
            .iter()
            .filter_map(|c| c["name"].as_str())
            .collect();
        assert_eq!(names, vec!["connect", "apikey", "status", "kv"]);

        let apikey_msg = apikey_prompt_message(Some("octocat"));
        assert_eq!(
            apikey_msg["components"][0]["components"]
                .as_array()
                .unwrap()
                .len(),
            2
        );
        let status_msg = status_message("123456789012345678", Some("octocat"), 3);
        assert!(
            status_msg["embeds"][0]["fields"][1]["value"]
                .as_str()
                .unwrap()
                .contains("@octocat")
        );
    }

    #[test]
    fn partial_presence_update_preserves_cached_avatar_and_profile() {
        let cached_member = json!({
            "user": {
                "id": "490201225921298463",
                "username": "varpsec",
                "global_name": "Varp",
                "avatar": "cb89941afc2aad170afadb740325c9a9"
            }
        });
        let partial_presence = json!({
            "user": { "id": "490201225921298463", "avatar": null },
            "status": "dnd",
            "activities": []
        });

        let (_, normalized) = normalize_presence(&partial_presence, Some(&cached_member)).unwrap();
        assert_eq!(normalized["discord_status"], "dnd");
        assert_eq!(normalized["discord_user"]["username"], "varpsec");
        assert_eq!(normalized["discord_user"]["global_name"], "Varp");
        assert_eq!(
            normalized["discord_user"]["avatar"],
            "cb89941afc2aad170afadb740325c9a9"
        );
    }
}
