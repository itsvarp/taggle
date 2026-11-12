use anyhow::Result;
use chrono::{DateTime, Utc};
use redis::AsyncCommands;
use serde_json::{Map, Value, json};
use sqlx::{PgPool, Row};

use crate::normalize::merge;

const LIST_LIMIT: i64 = 30;

/// Validates a GitHub username (`[A-Za-z0-9]`, internal `-`, max 39 chars).
pub fn valid_github_login(login: &str) -> bool {
    let bytes = login.as_bytes();
    !bytes.is_empty()
        && bytes.len() <= 39
        && bytes.iter().all(|b| b.is_ascii_alphanumeric() || *b == b'-')
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
            let _: () = r.set(format!("taggle:state:{login_lower}"), initial.to_string()).await?;
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
        sqlx::query("DELETE FROM activities WHERE login = $1").bind(&login_lower).execute(&mut *tx).await?;
        sqlx::query("DELETE FROM snapshots WHERE login = $1").bind(&login_lower).execute(&mut *tx).await?;
        sqlx::query("DELETE FROM user_kv WHERE login = $1").bind(&login_lower).execute(&mut *tx).await?;
        sqlx::query("DELETE FROM installations WHERE account_login = $1").bind(&login_lower).execute(&mut *tx).await?;
        sqlx::query("DELETE FROM users WHERE login = $1").bind(&login_lower).execute(&mut *tx).await?;
        tx.commit().await?;

        let mut r = self.redis.clone();
        let _: () = r
            .del(&[
                format!("taggle:state:{login_lower}"),
                format!("taggle:sync_cooldown:{login_lower}"),
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
        sqlx::query("DELETE FROM discord_presences WHERE user_id = $1").bind(user_id).execute(&mut *tx).await?;
        sqlx::query("DELETE FROM discord_users WHERE id = $1").bind(user_id).execute(&mut *tx).await?;
        sqlx::query("DELETE FROM discord_kv WHERE user_id = $1").bind(user_id).execute(&mut *tx).await?;
        sqlx::query("DELETE FROM discord_github_links WHERE discord_user_id = $1").bind(user_id).execute(&mut *tx).await?;
        tx.commit().await?;

        let mut r = self.redis.clone();
        let _: () = r.del(format!("taggle:discord:state:{user_id}")).await?;

        if let Some(login) = linked_login
            && !self.is_github_connected(&login).await?
        {
            self.purge_github_user(&login).await?;
        }
        Ok(())
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
        Ok(sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM activities WHERE login = $1)")
            .bind(login)
            .fetch_one(&self.pg)
            .await?)
    }

    pub async fn set_app_slug(&self, slug: &str) -> Result<()> {
        let mut r = self.redis.clone();
        let _: () = r.set("taggle:app_slug", slug).await?;
        Ok(())
    }

    pub async fn login_for_installation(&self, id: i64) -> Result<Option<String>> {
        Ok(sqlx::query_scalar("SELECT account_login FROM installations WHERE id = $1")
            .bind(id)
            .fetch_optional(&self.pg)
            .await?)
    }

    /// Removes an installation. If it was the user's last one, wipes their GitHub data and cache.
    pub async fn remove_installation(&self, id: i64) -> Result<Option<String>> {
        let Some(login) = self.login_for_installation(id).await? else { return Ok(None) };
        sqlx::query("DELETE FROM installations WHERE id = $1").bind(id).execute(&self.pg).await?;
        let remaining: i64 = sqlx::query_scalar("SELECT count(*) FROM installations WHERE account_login = $1")
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

    pub async fn patch_activity(&self, login: &str, kind: &str, key: &str, patch: &Value) -> Result<()> {
        sqlx::query("UPDATE activities SET data = data || $4 WHERE login = $1 AND kind = $2 AND key = $3")
            .bind(login)
            .bind(kind)
            .bind(key)
            .bind(patch)
            .execute(&self.pg)
            .await?;
        Ok(())
    }

    pub async fn patch_pr_deployment(&self, login: &str, repo: &str, branch: &str, status: &str) -> Result<bool> {
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

    async fn recent(&self, login: &str, kind: &str) -> Result<Vec<Value>> {
        Ok(sqlx::query_scalar("SELECT data FROM activities WHERE login = $1 AND kind = $2 ORDER BY occurred_at DESC LIMIT $3")
            .bind(login)
            .bind(kind)
            .bind(LIST_LIMIT)
            .fetch_all(&self.pg)
            .await?)
    }

    /// Rebuilds the full snapshot, persists it (Postgres + Redis) and publishes it to gateways.
    pub async fn rebuild_snapshot(&self, login: &str) -> Result<()> {
        if !self.is_github_connected(login).await? {
            return Ok(());
        }
        let Some(user) = sqlx::query("SELECT id, name, avatar_url FROM users WHERE login = $1")
            .bind(login)
            .fetch_optional(&self.pg)
            .await?
        else {
            return Ok(()); // not monitored
        };
        let id: i64 = user.get("id");
        let name: Option<String> = user.get("name");
        let avatar: Option<String> = user.get("avatar_url");

        let last_active: Option<DateTime<Utc>> =
            sqlx::query_scalar("SELECT max(occurred_at) FROM activities WHERE login = $1").bind(login).fetch_one(&self.pg).await?;

        let kv = self.user_kv(login).await?;

        let snapshot = json!({
            "user": {
                "id": id,
                "username": login,
                "name": name,
                "avatar_url": avatar,
                "profile_url": format!("https://github.com/{login}"),
            },
            "last_active_at": last_active,
            "commits": self.recent(login, "commit").await?,
            "pull_requests": self.recent(login, "pull_request").await?,
            "issues": self.recent(login, "issue").await?,
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
        .execute(&self.pg)
        .await?;

        if res.rows_affected() == 0 {
            return Ok(());
        }

        let _: () = r.set(&state_key, &payload).await?;
        let _: () = r.publish(format!("taggle:events:{login}"), &payload).await?;
        Ok(())
    }

    async fn user_kv(&self, login: &str) -> Result<Map<String, Value>> {
        let rows = sqlx::query("SELECT key, value FROM user_kv WHERE login = $1").bind(login).fetch_all(&self.pg).await?;
        Ok(rows.iter().map(|r| (r.get::<String, _>("key"), json!(r.get::<String, _>("value")))).collect())
    }

    // ---------------------------------------------------------------------
    // Discord presence (Lanyard-compatible)
    // ---------------------------------------------------------------------

    async fn discord_kv(&self, user_id: &str) -> Result<Map<String, Value>> {
        let rows = sqlx::query("SELECT key, value FROM discord_kv WHERE user_id = $1")
            .bind(user_id)
            .fetch_all(&self.pg)
            .await?;
        Ok(rows.iter().map(|r| (r.get::<String, _>("key"), json!(r.get::<String, _>("value")))).collect())
    }

    /// Writes a normalized presence, merging the user's KV map, then publishes it.
    /// No-op when the payload did not change (presence events are noisy).
    pub async fn upsert_discord_presence(&self, user_id: &str, presence: &Value) -> Result<()> {
        if !crate::discord::valid_snowflake(user_id) {
            anyhow::bail!("refusing to store invalid Discord user id");
        }

        let mut data = presence.clone();
        let kv = self.discord_kv(user_id).await?;
        if let Some(obj) = data.as_object_mut() {
            obj.insert("kv".into(), Value::Object(kv));
        }

        let payload = data.to_string();
        let mut r = self.redis.clone();
        let state_key = format!("taggle:discord:state:{user_id}");
        let prev: Option<String> = r.get(&state_key).await.unwrap_or(None);
        if prev.as_deref() == Some(payload.as_str()) {
            return Ok(());
        }

        sqlx::query(
            "INSERT INTO discord_presences (user_id, data) VALUES ($1, $2)
             ON CONFLICT (user_id) DO UPDATE SET data = EXCLUDED.data, updated_at = now()",
        )
        .bind(user_id)
        .bind(&data)
        .execute(&self.pg)
        .await?;

        let _: () = r.set(&state_key, &payload).await?;
        let _: () = r.publish(format!("taggle:discord:events:{user_id}"), &payload).await?;
        Ok(())
    }

    pub async fn delete_discord_presence(&self, user_id: &str) -> Result<()> {
        if !crate::discord::valid_snowflake(user_id) {
            return Ok(());
        }
        sqlx::query("DELETE FROM discord_presences WHERE user_id = $1").bind(user_id).execute(&self.pg).await?;
        let mut r = self.redis.clone();
        let _: () = r.del(format!("taggle:discord:state:{user_id}")).await?;
        Ok(())
    }
}
