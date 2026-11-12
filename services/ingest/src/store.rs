use std::io::Read;

use anyhow::Result;
use chrono::{DateTime, Duration, Utc};
use redis::AsyncCommands;
use serde_json::{Map, Value, json};
use sha2::{Digest, Sha256};
use sqlx::{PgPool, Row};

use crate::normalize::merge;

const LIST_LIMIT: i64 = 30;
const MAX_DISCORD_KV_PAIRS: i64 = 512;
const MAX_KV_VALUE_BYTES: usize = 30_000;

const DISCORD_MERGE_LUA: &str = r#"
local presence_key = KEYS[1]
local kv_key = KEYS[2]
local state_key = KEYS[3]
local events_channel = ARGV[1]
local presence_json = ARGV[2]
local fallback_kv_json = ARGV[3]

redis.call('SET', presence_key, presence_json)
redis.call('SET', kv_key, fallback_kv_json, 'NX')

local kv_raw = redis.call('GET', kv_key)
if not kv_raw or kv_raw == '' then
  kv_raw = '{}'
end

local trimmed = presence_json:match('^%s*(.-)%s*$')
local merged
if trimmed == '{}' then
  merged = '{"kv":' .. kv_raw .. '}'
else
  merged = string.sub(trimmed, 1, -2) .. ',"kv":' .. kv_raw .. '}'
end

local prev = redis.call('GET', state_key)
if prev ~= merged then
  redis.call('SET', state_key, merged)
  redis.call('PUBLISH', events_channel, merged)
end
return 1
"#;

const DISCORD_KV_MERGE_LUA: &str = r#"
local presence_key = KEYS[1]
local kv_key = KEYS[2]
local state_key = KEYS[3]
local events_channel = ARGV[1]
local kv_json = ARGV[2]

redis.call('SET', kv_key, kv_json)
local p_raw = redis.call('GET', presence_key)
if not p_raw or p_raw == '' then
  return 0
end

local trimmed = p_raw:match('^%s*(.-)%s*$')
local merged
if trimmed == '{}' then
  merged = '{"kv":' .. kv_json .. '}'
else
  merged = string.sub(trimmed, 1, -2) .. ',"kv":' .. kv_json .. '}'
end

local prev = redis.call('GET', state_key)
if prev ~= merged then
  redis.call('SET', state_key, merged)
  redis.call('PUBLISH', events_channel, merged)
end
return 1
"#;

/// Validates a GitHub username (`[A-Za-z0-9]`, internal `-`, max 39 chars).
pub fn valid_github_login(login: &str) -> bool {
    let bytes = login.as_bytes();
    !bytes.is_empty()
        && bytes.len() <= 39
        && bytes
            .iter()
            .all(|b| b.is_ascii_alphanumeric() || *b == b'-')
        && !login.starts_with('-')
        && !login.ends_with('-')
}

#[derive(Clone)]
pub struct Store {
    pub pg: PgPool,
    pub redis: redis::aio::ConnectionManager,
}

impl Store {
    pub async fn connect(database_url: &str, redis_url: &str) -> Result<Self> {
        let pg = sqlx::postgres::PgPoolOptions::new()
            .max_connections(10)
            .acquire_timeout(std::time::Duration::from_secs(5))
            .connect(database_url)
            .await?;
        sqlx::migrate!("../../migrations").run(&pg).await?;
        let redis = redis::aio::ConnectionManager::new(redis::Client::open(redis_url)?).await?;
        Ok(Self { pg, redis })
    }

    /// `true` the first time a delivery id is seen (SET NX with 24h TTL).
    pub async fn first_delivery(&self, delivery_id: &str) -> Result<bool> {
        let mut r = self.redis.clone();
        let set: Option<String> = redis::cmd("SET")
            .arg(format!("taggle:delivery:{delivery_id}"))
            .arg(1)
            .arg("NX")
            .arg("EX")
            .arg(86_400)
            .query_async(&mut r)
            .await?;
        Ok(set.is_some())
    }

    pub async fn upsert_user(&self, id: i64, login: &str, avatar_url: Option<&str>) -> Result<()> {
        sqlx::query(
            "INSERT INTO users (id, login, avatar_url) VALUES ($1, $2, $3)
             ON CONFLICT (id) DO UPDATE SET login = EXCLUDED.login, avatar_url = EXCLUDED.avatar_url",
        )
        .bind(id)
        .bind(login.to_lowercase())
        .bind(avatar_url)
        .execute(&self.pg)
        .await?;
        Ok(())
    }

    pub async fn upsert_github_user_oauth(
        &self,
        id: i64,
        login: &str,
        name: Option<&str>,
        avatar_url: Option<&str>,
    ) -> Result<String> {
        let login_lower = login.trim().to_lowercase();
        if !valid_github_login(&login_lower) {
            anyhow::bail!("invalid GitHub login");
        }
        sqlx::query(
            "INSERT INTO users (id, login, name, avatar_url) VALUES ($1, $2, $3, $4)
             ON CONFLICT (id) DO UPDATE
               SET login = EXCLUDED.login,
                   name = COALESCE(EXCLUDED.name, users.name),
                   avatar_url = EXCLUDED.avatar_url",
        )
        .bind(id)
        .bind(&login_lower)
        .bind(name)
        .bind(avatar_url)
        .execute(&self.pg)
        .await?;

        let initial = json!({
            "user": {
                "id": id,
                "username": login_lower,
                "name": name,
                "avatar_url": avatar_url,
                "profile_url": format!("https://github.com/{login_lower}"),
            },
            "last_active_at": null,
            "commits": [],
            "pull_requests": [],
            "issues": [],
            "kv": {},
        });

        let res = sqlx::query(
            "INSERT INTO snapshots (login, data) VALUES ($1, $2) ON CONFLICT (login) DO NOTHING",
        )
        .bind(&login_lower)
        .bind(&initial)
        .execute(&self.pg)
        .await?;

        if res.rows_affected() == 1 {
            let mut r = self.redis.clone();
            let _: () = r
                .set(format!("taggle:state:{login_lower}"), initial.to_string())
                .await?;
        }

        Ok(login_lower)
    }

    pub async fn link_discord_github(
        &self,
        discord_user_id: &str,
        github_id: i64,
        github_login: &str,
    ) -> Result<()> {
        if !crate::discord::valid_snowflake(discord_user_id) {
            anyhow::bail!("invalid Discord user id");
        }
        let login_lower = github_login.trim().to_lowercase();
        if !valid_github_login(&login_lower) {
            anyhow::bail!("invalid GitHub login");
        }
        let prev_login = self.get_discord_github_link(discord_user_id).await?;

        sqlx::query(
            "INSERT INTO discord_github_links (discord_user_id, github_id, github_login)
             VALUES ($1, $2, $3)
             ON CONFLICT (discord_user_id) DO UPDATE
               SET github_id = EXCLUDED.github_id,
                   github_login = EXCLUDED.github_login,
                   connected_at = now()",
        )
        .bind(discord_user_id)
        .bind(github_id)
        .bind(&login_lower)
        .execute(&self.pg)
        .await?;

        if let Some(prev) = prev_login
            && prev != login_lower
            && !self.is_github_connected(&prev).await?
        {
            self.purge_github_user(&prev).await?;
        }
        Ok(())
    }

    pub async fn get_discord_github_link(&self, discord_user_id: &str) -> Result<Option<String>> {
        if !crate::discord::valid_snowflake(discord_user_id) {
            return Ok(None);
        }
        Ok(sqlx::query_scalar(
            "SELECT github_login FROM discord_github_links WHERE discord_user_id = $1",
        )
        .bind(discord_user_id)
        .fetch_optional(&self.pg)
        .await?)
    }

    pub async fn is_github_connected(&self, login: &str) -> Result<bool> {
        let login_lower = login.trim().to_lowercase();
        if login_lower.is_empty() {
            return Ok(false);
        }
        Ok(sqlx::query_scalar(
            "SELECT EXISTS(SELECT 1 FROM discord_github_links WHERE github_login = $1)",
        )
        .bind(login_lower)
        .fetch_one(&self.pg)
        .await?)
    }

    pub async fn list_connected_github_logins(&self) -> Result<Vec<String>> {
        Ok(sqlx::query_scalar(
            "SELECT DISTINCT github_login FROM discord_github_links ORDER BY github_login LIMIT 200",
        )
        .fetch_all(&self.pg)
        .await?)
    }

    pub async fn list_tracked_discord_user_ids(&self) -> Result<Vec<String>> {
        Ok(sqlx::query_scalar(
            "SELECT user_id FROM discord_presences
             UNION
             SELECT id FROM discord_users
             UNION
             SELECT user_id FROM discord_kv
             UNION
             SELECT discord_user_id FROM discord_github_links",
        )
        .fetch_all(&self.pg)
        .await?)
    }

    pub async fn unlink_discord_github(&self, discord_user_id: &str) -> Result<()> {
        if !crate::discord::valid_snowflake(discord_user_id) {
            return Ok(());
        }
        let prev_login = self.get_discord_github_link(discord_user_id).await?;
        sqlx::query("DELETE FROM discord_github_links WHERE discord_user_id = $1")
            .bind(discord_user_id)
            .execute(&self.pg)
            .await?;

        if let Some(login) = prev_login
            && !self.is_github_connected(&login).await?
        {
            self.purge_github_user(&login).await?;
        }
        Ok(())
    }

    pub async fn purge_github_user(&self, login: &str) -> Result<()> {
        let login_lower = login.trim().to_lowercase();
        if login_lower.is_empty() {
            return Ok(());
        }
        let mut tx = self.pg.begin().await?;
        sqlx::query("DELETE FROM activities WHERE login = $1")
            .bind(&login_lower)
            .execute(&mut *tx)
            .await?;
        sqlx::query("DELETE FROM snapshots WHERE login = $1")
            .bind(&login_lower)
            .execute(&mut *tx)
            .await?;
        sqlx::query("DELETE FROM user_kv WHERE login = $1")
            .bind(&login_lower)
            .execute(&mut *tx)
            .await?;
        sqlx::query("DELETE FROM installations WHERE account_login = $1")
            .bind(&login_lower)
            .execute(&mut *tx)
            .await?;
        sqlx::query("DELETE FROM users WHERE login = $1")
            .bind(&login_lower)
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;

        let mut r = self.redis.clone();
        let _: () = r
            .del(&[
                format!("taggle:state:{login_lower}"),
                format!("taggle:sync_cooldown:{login_lower}"),
                format!("taggle:wh_recent:{login_lower}"),
            ])
            .await?;
        Ok(())
    }

    pub async fn purge_unlinked_github_users(&self) -> Result<()> {
        let unlinked: Vec<String> = sqlx::query_scalar(
            "SELECT login FROM (
                SELECT login FROM users
                UNION
                SELECT login FROM snapshots
                UNION
                SELECT DISTINCT login FROM activities
                UNION
                SELECT DISTINCT login FROM user_kv
                UNION
                SELECT DISTINCT account_login AS login FROM installations
            ) t
            WHERE NOT EXISTS (SELECT 1 FROM discord_github_links d WHERE d.github_login = t.login)",
        )
        .fetch_all(&self.pg)
        .await?;

        for login in unlinked {
            self.purge_github_user(&login).await?;
        }
        Ok(())
    }

    /// Prunes old `activities` rows beyond `keep_per_kind` per (login, kind) to bound table size.
    pub async fn prune_old_activities(&self, keep_per_kind: i64) -> Result<u64> {
        let res = sqlx::query(
            "DELETE FROM activities a
             USING (
                SELECT ctid, ROW_NUMBER() OVER (PARTITION BY login, kind ORDER BY occurred_at DESC) AS rn
                FROM activities
             ) ranked
             WHERE a.ctid = ranked.ctid AND ranked.rn > $1",
        )
        .bind(keep_per_kind)
        .execute(&self.pg)
        .await?;
        Ok(res.rows_affected())
    }

    /// Removes all Discord and linked GitHub data for a user who left the server.
    pub async fn purge_discord_user(&self, user_id: &str) -> Result<()> {
        if !crate::discord::valid_snowflake(user_id) {
            return Ok(());
        }
        let linked_login = self.get_discord_github_link(user_id).await?;

        let mut tx = self.pg.begin().await?;
        sqlx::query("DELETE FROM discord_presences WHERE user_id = $1")
            .bind(user_id)
            .execute(&mut *tx)
            .await?;
        sqlx::query("DELETE FROM discord_users WHERE id = $1")
            .bind(user_id)
            .execute(&mut *tx)
            .await?;
        sqlx::query("DELETE FROM discord_kv WHERE user_id = $1")
            .bind(user_id)
            .execute(&mut *tx)
            .await?;
        sqlx::query("DELETE FROM discord_github_links WHERE discord_user_id = $1")
            .bind(user_id)
            .execute(&mut *tx)
            .await?;
        tx.commit().await?;

        let mut r = self.redis.clone();
        let _: () = r
            .del(&[
                format!("taggle:discord:state:{user_id}"),
                format!("taggle:discord:presence:{user_id}"),
                format!("taggle:discord:kv:{user_id}"),
            ])
            .await?;

        if let Some(login) = linked_login
            && !self.is_github_connected(&login).await?
        {
            self.purge_github_user(&login).await?;
        }
        Ok(())
    }

    pub async fn health_check(&self) -> Result<()> {
        sqlx::query("SELECT 1").execute(&self.pg).await?;
        let mut r = self.redis.clone();
        let _: String = redis::cmd("PING").query_async(&mut r).await?;
        Ok(())
    }

    pub async fn get_etag(&self, url: &str) -> Option<String> {
        let key = format!("taggle:etag:{}", sha256_hex(url));
        let mut r = self.redis.clone();
        r.get(&key).await.ok().flatten()
    }

    pub async fn set_etag(&self, url: &str, etag: &str) {
        let key = format!("taggle:etag:{}", sha256_hex(url));
        let mut r = self.redis.clone();
        let _: redis::RedisResult<()> = r.set_ex(&key, etag, 86_400).await;
    }

    pub async fn mark_webhook_seen(&self, login: &str) {
        let login_lower = login.trim().to_lowercase();
        if login_lower.is_empty() {
            return;
        }
        let mut r = self.redis.clone();
        let _: redis::RedisResult<()> = r
            .set_ex(format!("taggle:wh_recent:{login_lower}"), "1", 300)
            .await;
    }

    pub async fn recent_webhook(&self, login: &str) -> bool {
        let login_lower = login.trim().to_lowercase();
        if login_lower.is_empty() {
            return false;
        }
        let mut r = self.redis.clone();
        r.exists(format!("taggle:wh_recent:{login_lower}"))
            .await
            .unwrap_or(false)
    }

    pub async fn upsert_installation(&self, id: i64, login: &str) -> Result<()> {
        sqlx::query(
            "INSERT INTO installations (id, account_login) VALUES ($1, $2)
             ON CONFLICT (id) DO UPDATE SET account_login = EXCLUDED.account_login",
        )
        .bind(id)
        .bind(login.to_lowercase())
        .execute(&self.pg)
        .await?;
        Ok(())
    }

    pub async fn has_activities(&self, login: &str) -> Result<bool> {
        Ok(
            sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM activities WHERE login = $1)")
                .bind(login)
                .fetch_one(&self.pg)
                .await?,
        )
    }

    pub async fn set_app_slug(&self, slug: &str) -> Result<()> {
        let mut r = self.redis.clone();
        let _: () = r.set("taggle:app_slug", slug).await?;
        Ok(())
    }

    pub async fn login_for_installation(&self, id: i64) -> Result<Option<String>> {
        Ok(
            sqlx::query_scalar("SELECT account_login FROM installations WHERE id = $1")
                .bind(id)
                .fetch_optional(&self.pg)
                .await?,
        )
    }

    /// Removes an installation. If it was the user's last one, wipes their GitHub data and cache.
    pub async fn remove_installation(&self, id: i64) -> Result<Option<String>> {
        let Some(login) = self.login_for_installation(id).await? else {
            return Ok(None);
        };
        sqlx::query("DELETE FROM installations WHERE id = $1")
            .bind(id)
            .execute(&self.pg)
            .await?;
        let remaining: i64 =
            sqlx::query_scalar("SELECT count(*) FROM installations WHERE account_login = $1")
                .bind(&login)
                .fetch_one(&self.pg)
                .await?;
        if remaining == 0 {
            if !self.is_github_connected(&login).await? {
                self.purge_github_user(&login).await?;
            } else {
                // Keep the user's /connect session and KV, but drop private repo activities
                // owned by the removed installation so they never linger after uninstall.
                sqlx::query(
                    "DELETE FROM activities WHERE login = $1 AND COALESCE((data->>'private')::boolean, false)",
                )
                .bind(&login)
                .execute(&self.pg)
                .await?;
                self.rebuild_snapshot(&login).await?;
            }
        }
        Ok(Some(login))
    }

    pub async fn upsert_activity(
        &self,
        login: &str,
        kind: &str,
        key: &str,
        defaults: &Value,
        base: &Value,
        at: DateTime<Utc>,
    ) -> Result<()> {
        // First insert gets defaults+base; later events only refresh `base`, so enrichment survives.
        sqlx::query(
            "INSERT INTO activities (login, kind, key, data, occurred_at) VALUES ($1, $2, $3, $4, $5)
             ON CONFLICT (login, kind, key) DO UPDATE
               SET data = activities.data || $6, occurred_at = EXCLUDED.occurred_at",
        )
        .bind(login)
        .bind(kind)
        .bind(key)
        .bind(merge(defaults, base))
        .bind(at)
        .bind(base)
        .execute(&self.pg)
        .await?;
        Ok(())
    }

    pub async fn patch_activity(
        &self,
        login: &str,
        kind: &str,
        key: &str,
        patch: &Value,
    ) -> Result<()> {
        sqlx::query(
            "UPDATE activities SET data = data || $4 WHERE login = $1 AND kind = $2 AND key = $3",
        )
        .bind(login)
        .bind(kind)
        .bind(key)
        .bind(patch)
        .execute(&self.pg)
        .await?;
        Ok(())
    }

    pub async fn patch_pr_deployment(
        &self,
        login: &str,
        repo: &str,
        branch: &str,
        status: &str,
    ) -> Result<bool> {
        let prefix = format!("{repo}#%");
        let patch = json!({ "deployment_status": status });
        let res = sqlx::query(
            "UPDATE activities SET data = data || $4
             WHERE login = $1 AND kind = 'pull_request' AND key LIKE $2
               AND data->>'branch' = $3 AND data->>'state' = 'open'",
        )
        .bind(login)
        .bind(prefix)
        .bind(branch)
        .bind(patch)
        .execute(&self.pg)
        .await?;
        Ok(res.rows_affected() > 0)
    }

    /// Rebuilds the full snapshot, persists it (Postgres + Redis) and publishes it to gateways.
    pub async fn rebuild_snapshot(&self, login: &str) -> Result<()> {
        if !self.is_github_connected(login).await? {
            return Ok(());
        }

        let mut tx = self.pg.begin().await?;
        sqlx::query("SELECT pg_advisory_xact_lock(hashtext('ghkv:' || $1))")
            .bind(login)
            .execute(&mut *tx)
            .await?;

        let Some(user) = sqlx::query("SELECT id, name, avatar_url FROM users WHERE login = $1")
            .bind(login)
            .fetch_optional(&mut *tx)
            .await?
        else {
            return Ok(()); // not monitored
        };
        let id: i64 = user.get("id");
        let name: Option<String> = user.get("name");
        let avatar: Option<String> = user.get("avatar_url");

        let last_active: Option<DateTime<Utc>> =
            sqlx::query_scalar("SELECT max(occurred_at) FROM activities WHERE login = $1")
                .bind(login)
                .fetch_one(&mut *tx)
                .await?;

        let kv_rows = sqlx::query("SELECT key, value FROM user_kv WHERE login = $1")
            .bind(login)
            .fetch_all(&mut *tx)
            .await?;
        let kv: Map<String, Value> = kv_rows
            .iter()
            .map(|r| {
                (
                    r.get::<String, _>("key"),
                    json!(r.get::<String, _>("value")),
                )
            })
            .collect();

        let commits: Vec<Value> = sqlx::query_scalar(
            "SELECT data FROM activities WHERE login = $1 AND kind = 'commit' ORDER BY occurred_at DESC LIMIT $2",
        )
        .bind(login)
        .bind(LIST_LIMIT)
        .fetch_all(&mut *tx)
        .await?;

        let pull_requests: Vec<Value> = sqlx::query_scalar(
            "SELECT data FROM activities WHERE login = $1 AND kind = 'pull_request' ORDER BY occurred_at DESC LIMIT $2",
        )
        .bind(login)
        .bind(LIST_LIMIT)
        .fetch_all(&mut *tx)
        .await?;

        let issues: Vec<Value> = sqlx::query_scalar(
            "SELECT data FROM activities WHERE login = $1 AND kind = 'issue' ORDER BY occurred_at DESC LIMIT $2",
        )
        .bind(login)
        .bind(LIST_LIMIT)
        .fetch_all(&mut *tx)
        .await?;

        let summary = build_github_summary(&commits, &pull_requests, &issues);

        let snapshot = json!({
            "user": {
                "id": id,
                "username": login,
                "name": name,
                "avatar_url": avatar,
                "profile_url": format!("https://github.com/{login}"),
            },
            "last_active_at": last_active,
            "commits": commits,
            "pull_requests": pull_requests,
            "issues": issues,
            "summary": summary,
            "kv": kv,
        });

        let payload = snapshot.to_string();
        let mut r = self.redis.clone();
        let state_key = format!("taggle:state:{login}");
        let prev: Option<String> = r.get(&state_key).await.unwrap_or(None);
        if prev.as_deref() == Some(payload.as_str()) {
            return Ok(());
        }

        let res = sqlx::query(
            "INSERT INTO snapshots (login, data)
             SELECT $1, $2
             WHERE EXISTS (SELECT 1 FROM discord_github_links WHERE github_login = $1)
             ON CONFLICT (login) DO UPDATE SET data = EXCLUDED.data, updated_at = now()",
        )
        .bind(login)
        .bind(&snapshot)
        .execute(&mut *tx)
        .await?;

        if res.rows_affected() == 0 {
            return Ok(());
        }

        tx.commit().await?;

        let _: () = r.set(&state_key, &payload).await?;
        let _: () = r
            .publish(format!("taggle:events:{login}"), &payload)
            .await?;
        Ok(())
    }

    // ---------------------------------------------------------------------
    // Discord presence (Taggle native pipeline, Lanyard-compatible JSON)
    // ---------------------------------------------------------------------

    async fn discord_kv_on<'e, E>(&self, exec: E, user_id: &str) -> Result<Map<String, Value>>
    where
        E: sqlx::Executor<'e, Database = sqlx::Postgres>,
    {
        let rows = sqlx::query("SELECT key, value FROM discord_kv WHERE user_id = $1")
            .bind(user_id)
            .fetch_all(exec)
            .await?;
        Ok(rows
            .iter()
            .map(|r| {
                (
                    r.get::<String, _>("key"),
                    json!(r.get::<String, _>("value")),
                )
            })
            .collect())
    }

    pub async fn list_discord_kv(&self, user_id: &str) -> Result<Vec<(String, String)>> {
        if !crate::discord::valid_snowflake(user_id) {
            return Ok(vec![]);
        }
        let rows =
            sqlx::query("SELECT key, value FROM discord_kv WHERE user_id = $1 ORDER BY key ASC")
                .bind(user_id)
                .fetch_all(&self.pg)
                .await?;
        Ok(rows
            .into_iter()
            .map(|r| (r.get::<String, _>("key"), r.get::<String, _>("value")))
            .collect())
    }

    pub async fn upsert_discord_user(
        &self,
        user_id: &str,
        username: Option<&str>,
        global_name: Option<&str>,
        avatar: Option<&str>,
    ) -> Result<()> {
        if !crate::discord::valid_snowflake(user_id) {
            anyhow::bail!("invalid Discord user id");
        }
        sqlx::query(
            "INSERT INTO discord_users (id, username, global_name, avatar)
             VALUES ($1, $2, $3, $4)
             ON CONFLICT (id) DO UPDATE
               SET username = COALESCE(EXCLUDED.username, discord_users.username),
                   global_name = COALESCE(EXCLUDED.global_name, discord_users.global_name),
                   avatar = COALESCE(EXCLUDED.avatar, discord_users.avatar)",
        )
        .bind(user_id)
        .bind(username)
        .bind(global_name)
        .bind(avatar)
        .execute(&self.pg)
        .await?;
        Ok(())
    }

    pub async fn rotate_discord_api_key(
        &self,
        user_id: &str,
        username: Option<&str>,
        global_name: Option<&str>,
        avatar: Option<&str>,
    ) -> Result<String> {
        self.upsert_discord_user(user_id, username, global_name, avatar)
            .await?;
        let token = generate_api_key()?;
        let hash = sha256_hex(&token);
        sqlx::query("UPDATE discord_users SET api_key_hash = $1 WHERE id = $2")
            .bind(hash)
            .bind(user_id)
            .execute(&self.pg)
            .await?;
        Ok(token)
    }

    pub async fn rotate_github_api_key(
        &self,
        discord_user_id: &str,
    ) -> Result<Option<(String, String)>> {
        let Some(login) = self.get_discord_github_link(discord_user_id).await? else {
            return Ok(None);
        };
        let token = generate_api_key()?;
        let hash = sha256_hex(&token);
        let res = sqlx::query("UPDATE users SET api_key_hash = $1 WHERE login = $2")
            .bind(hash)
            .bind(&login)
            .execute(&self.pg)
            .await?;
        if res.rows_affected() == 0 {
            return Ok(None);
        }
        Ok(Some((login, token)))
    }

    pub async fn set_discord_kv(&self, user_id: &str, key: &str, value: &str) -> Result<()> {
        if !crate::discord::valid_snowflake(user_id) {
            anyhow::bail!("invalid_id");
        }
        if !valid_discord_kv_key(key) {
            anyhow::bail!("invalid_key");
        }
        if value.len() > MAX_KV_VALUE_BYTES {
            anyhow::bail!("value_too_large");
        }

        let mut tx = self.pg.begin().await?;
        sqlx::query("SELECT pg_advisory_xact_lock(hashtext('dcstate:' || $1))")
            .bind(user_id)
            .execute(&mut *tx)
            .await?;

        sqlx::query(
            "INSERT INTO discord_kv (user_id, key, value) VALUES ($1, $2, $3)
             ON CONFLICT (user_id, key) DO UPDATE SET value = EXCLUDED.value",
        )
        .bind(user_id)
        .bind(key)
        .bind(value)
        .execute(&mut *tx)
        .await?;

        let total: i64 = sqlx::query_scalar("SELECT count(*) FROM discord_kv WHERE user_id = $1")
            .bind(user_id)
            .fetch_one(&mut *tx)
            .await?;
        if total > MAX_DISCORD_KV_PAIRS {
            anyhow::bail!("too_many_keys");
        }

        let kv = self.discord_kv_on(&mut *tx, user_id).await?;
        let kv_val = Value::Object(kv);
        sqlx::query(
            "UPDATE discord_presences
             SET data = jsonb_set(data, '{kv}', $2::jsonb, true), updated_at = now()
             WHERE user_id = $1",
        )
        .bind(user_id)
        .bind(&kv_val)
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;

        let kv_json = kv_val.to_string();
        let mut r = self.redis.clone();
        let _: i32 = redis::Script::new(DISCORD_KV_MERGE_LUA)
            .key(format!("taggle:discord:presence:{user_id}"))
            .key(format!("taggle:discord:kv:{user_id}"))
            .key(format!("taggle:discord:state:{user_id}"))
            .arg(format!("taggle:discord:events:{user_id}"))
            .arg(&kv_json)
            .invoke_async(&mut r)
            .await?;

        Ok(())
    }

    pub async fn delete_discord_kv(&self, user_id: &str, key: &str) -> Result<bool> {
        if !crate::discord::valid_snowflake(user_id) {
            anyhow::bail!("invalid_id");
        }
        if !valid_discord_kv_key(key) {
            anyhow::bail!("invalid_key");
        }

        let mut tx = self.pg.begin().await?;
        sqlx::query("SELECT pg_advisory_xact_lock(hashtext('dcstate:' || $1))")
            .bind(user_id)
            .execute(&mut *tx)
            .await?;

        let res = sqlx::query("DELETE FROM discord_kv WHERE user_id = $1 AND key = $2")
            .bind(user_id)
            .bind(key)
            .execute(&mut *tx)
            .await?;
        let deleted = res.rows_affected() > 0;

        let kv = self.discord_kv_on(&mut *tx, user_id).await?;
        let kv_val = Value::Object(kv);
        sqlx::query(
            "UPDATE discord_presences
             SET data = jsonb_set(data, '{kv}', $2::jsonb, true), updated_at = now()
             WHERE user_id = $1",
        )
        .bind(user_id)
        .bind(&kv_val)
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;

        let kv_json = kv_val.to_string();
        let mut r = self.redis.clone();
        let _: i32 = redis::Script::new(DISCORD_KV_MERGE_LUA)
            .key(format!("taggle:discord:presence:{user_id}"))
            .key(format!("taggle:discord:kv:{user_id}"))
            .key(format!("taggle:discord:state:{user_id}"))
            .arg(format!("taggle:discord:events:{user_id}"))
            .arg(&kv_json)
            .invoke_async(&mut r)
            .await?;

        Ok(deleted)
    }

    /// Writes a normalized presence without racing against concurrent KV updates.
    /// No-op when the presence payload did not change (presence events are noisy).
    pub async fn upsert_discord_presence(&self, user_id: &str, presence: &Value) -> Result<()> {
        if !crate::discord::valid_snowflake(user_id) {
            anyhow::bail!("refusing to store invalid Discord user id");
        }

        let mut presence_no_kv = presence.clone();
        if let Some(obj) = presence_no_kv.as_object_mut() {
            obj.remove("kv");
        }
        let presence_json = presence_no_kv.to_string();

        let mut r = self.redis.clone();
        let presence_key = format!("taggle:discord:presence:{user_id}");
        let state_key = format!("taggle:discord:state:{user_id}");
        let (prev_presence, has_state): (Option<String>, bool) = redis::pipe()
            .get(&presence_key)
            .exists(&state_key)
            .query_async(&mut r)
            .await
            .unwrap_or((None, false));

        if has_state && prev_presence.as_deref() == Some(presence_json.as_str()) {
            return Ok(());
        }

        let mut tx = self.pg.begin().await?;
        sqlx::query("SELECT pg_advisory_xact_lock(hashtext('dcstate:' || $1))")
            .bind(user_id)
            .execute(&mut *tx)
            .await?;

        let kv = self.discord_kv_on(&mut *tx, user_id).await?;
        let fallback_kv_json = Value::Object(kv.clone()).to_string();

        let mut pg_data = presence_no_kv;
        if let Some(obj) = pg_data.as_object_mut() {
            obj.insert("kv".into(), Value::Object(kv));
        }

        sqlx::query(
            "INSERT INTO discord_presences (user_id, data) VALUES ($1, $2)
             ON CONFLICT (user_id) DO UPDATE SET data = EXCLUDED.data, updated_at = now()",
        )
        .bind(user_id)
        .bind(&pg_data)
        .execute(&mut *tx)
        .await?;
        tx.commit().await?;

        let _: i32 = redis::Script::new(DISCORD_MERGE_LUA)
            .key(&presence_key)
            .key(format!("taggle:discord:kv:{user_id}"))
            .key(&state_key)
            .arg(format!("taggle:discord:events:{user_id}"))
            .arg(&presence_json)
            .arg(&fallback_kv_json)
            .invoke_async(&mut r)
            .await?;

        Ok(())
    }

    /// Marks previously non-offline members as offline when a GUILD_CREATE snapshot omits them.
    pub async fn reconcile_offline_presences(
        &self,
        member_ids: &[String],
        online_ids: &[String],
    ) -> Result<usize> {
        if member_ids.is_empty() {
            return Ok(0);
        }
        let rows = sqlx::query(
            "SELECT user_id, data
             FROM discord_presences
             WHERE user_id = ANY($1::text[])
               AND NOT (user_id = ANY($2::text[]))
               AND COALESCE(data->>'discord_status', 'offline') <> 'offline'",
        )
        .bind(member_ids)
        .bind(online_ids)
        .fetch_all(&self.pg)
        .await?;

        let mut transitioned = 0usize;
        for row in rows {
            let user_id: String = row.get("user_id");
            let mut data: Value = row.get("data");
            if let Some(obj) = data.as_object_mut() {
                obj.insert("discord_status".into(), json!("offline"));
                obj.insert("activities".into(), json!([]));
                obj.insert("active_on_discord_desktop".into(), json!(false));
                obj.insert("active_on_discord_mobile".into(), json!(false));
                obj.insert("active_on_discord_web".into(), json!(false));
                obj.insert("listening_to_spotify".into(), json!(false));
                obj.insert("spotify".into(), Value::Null);
                obj.insert("coding".into(), Value::Null);
            }
            self.upsert_discord_presence(&user_id, &data).await?;
            transitioned += 1;
        }
        Ok(transitioned)
    }

    pub async fn delete_discord_presence(&self, user_id: &str) -> Result<()> {
        if !crate::discord::valid_snowflake(user_id) {
            return Ok(());
        }
        sqlx::query("DELETE FROM discord_presences WHERE user_id = $1")
            .bind(user_id)
            .execute(&self.pg)
            .await?;
        let mut r = self.redis.clone();
        let _: () = r
            .del(&[
                format!("taggle:discord:state:{user_id}"),
                format!("taggle:discord:presence:{user_id}"),
                format!("taggle:discord:kv:{user_id}"),
            ])
            .await?;
        Ok(())
    }
}

pub fn valid_discord_kv_key(key: &str) -> bool {
    !key.is_empty()
        && key.len() <= 255
        && key.bytes().all(|b| b.is_ascii_alphanumeric() || b == b'_')
}

pub fn sha256_hex(input: &str) -> String {
    let mut hasher = Sha256::new();
    hasher.update(input.as_bytes());
    hex::encode(hasher.finalize())
}

fn generate_api_key() -> Result<String> {
    let mut bytes = [0u8; 32];
    let mut f = std::fs::File::open("/dev/urandom")?;
    f.read_exact(&mut bytes)?;
    Ok(format!("tggl_{}", base64_url_nopad(&bytes)))
}

fn base64_url_nopad(input: &[u8]) -> String {
    const TABLE: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
    let mut out = String::with_capacity((input.len() * 4).div_ceil(3));
    for chunk in input.chunks(3) {
        let b0 = chunk[0] as u32;
        let b1 = if chunk.len() > 1 { chunk[1] as u32 } else { 0 };
        let b2 = if chunk.len() > 2 { chunk[2] as u32 } else { 0 };
        let n = (b0 << 16) | (b1 << 8) | b2;
        out.push(TABLE[((n >> 18) & 0x3f) as usize] as char);
        out.push(TABLE[((n >> 12) & 0x3f) as usize] as char);
        if chunk.len() > 1 {
            out.push(TABLE[((n >> 6) & 0x3f) as usize] as char);
        }
        if chunk.len() > 2 {
            out.push(TABLE[(n & 0x3f) as usize] as char);
        }
    }
    out
}

fn build_github_summary(commits: &[Value], pull_requests: &[Value], issues: &[Value]) -> Value {
    let cutoff = Utc::now() - Duration::hours(24);
    let mut commits_last_24h = 0usize;
    let mut repos = Vec::new();
    let mut seen = std::collections::HashSet::new();

    for c in commits {
        if let Some(ts) = c.get("timestamp").and_then(Value::as_str)
            && let Ok(dt) = DateTime::parse_from_rfc3339(ts)
            && dt.with_timezone(&Utc) >= cutoff
        {
            commits_last_24h += 1;
        }
        if !c.get("private").and_then(Value::as_bool).unwrap_or(false)
            && let Some(repo) = c.get("repo").and_then(Value::as_str)
            && seen.insert(repo.to_owned())
            && repos.len() < 5
        {
            repos.push(repo.to_owned());
        }
    }

    let mut open_pull_requests = 0usize;
    for pr in pull_requests {
        if pr.get("state").and_then(Value::as_str) == Some("open") {
            open_pull_requests += 1;
        }
        if !pr.get("private").and_then(Value::as_bool).unwrap_or(false)
            && let Some(repo) = pr.get("repo").and_then(Value::as_str)
            && seen.insert(repo.to_owned())
            && repos.len() < 5
        {
            repos.push(repo.to_owned());
        }
    }

    let mut open_issues = 0usize;
    for issue in issues {
        if issue.get("state").and_then(Value::as_str) == Some("open") {
            open_issues += 1;
        }
        if !issue
            .get("private")
            .and_then(Value::as_bool)
            .unwrap_or(false)
            && let Some(repo) = issue.get("repo").and_then(Value::as_str)
            && seen.insert(repo.to_owned())
            && repos.len() < 5
        {
            repos.push(repo.to_owned());
        }
    }

    json!({
        "commits_count": commits.len(),
        "commits_last_24h": commits_last_24h,
        "pull_requests_count": pull_requests.len(),
        "open_pull_requests": open_pull_requests,
        "issues_count": issues.len(),
        "open_issues": open_issues,
        "active_repos": repos,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn generates_valid_api_keys_and_validates_kv_keys() {
        let key1 = generate_api_key().unwrap();
        let key2 = generate_api_key().unwrap();
        assert!(key1.starts_with("tggl_"));
        assert_eq!(key1.len(), 5 + 43);
        assert_ne!(key1, key2);
        assert_eq!(sha256_hex("hello").len(), 64);

        assert!(valid_discord_kv_key("location_1"));
        assert!(!valid_discord_kv_key(""));
        assert!(!valid_discord_kv_key("bad.key"));
        assert!(!valid_discord_kv_key(&"a".repeat(256)));
    }

    #[test]
    fn github_summary_never_leaks_private_repos() {
        let commits = vec![
            json!({ "repo": "user/pub", "private": false, "timestamp": Utc::now().to_rfc3339() }),
            json!({ "repo": "user/secret", "private": true, "timestamp": Utc::now().to_rfc3339() }),
        ];
        let prs = vec![json!({ "repo": null, "private": true, "state": "open" })];
        let issues = vec![json!({ "repo": "user/pub2", "private": false, "state": "closed" })];

        let summary = build_github_summary(&commits, &prs, &issues);
        assert_eq!(summary["commits_count"], 2);
        assert_eq!(summary["commits_last_24h"], 2);
        assert_eq!(summary["open_pull_requests"], 1);
        assert_eq!(summary["open_issues"], 0);
        let repos = summary["active_repos"].as_array().unwrap();
        assert_eq!(repos, &vec![json!("user/pub"), json!("user/pub2")]);
    }
}
