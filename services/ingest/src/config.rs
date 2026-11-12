use anyhow::{Context, Result, bail};

/// GitHub App credentials. Optional so Taggle can run Discord-only.
pub struct GitHubConfig {
    pub webhook_secret: String,
    pub app_id: String,
    pub private_key: String,
}

/// Discord bot credentials. Optional so Taggle can run GitHub-only.
pub struct DiscordConfig {
    pub bot_token: String,
    pub guild_ids: Vec<u64>,
}

pub struct Config {
    pub database_url: String,
    pub redis_url: String,
    pub port: u16,
    pub github: Option<GitHubConfig>,
    pub discord: Option<DiscordConfig>,
}

impl Config {
    /// Secrets (GITHUB_*, DISCORD_*) are injected by `infisical run`; nothing is read from disk.
    /// Each integration is independently optional: both, GitHub-only, or Discord-only all work.
    pub fn from_env() -> Result<Self> {
        let non_empty = |k: &str| std::env::var(k).ok().filter(|v| !v.is_empty());

        let github = match non_empty("GITHUB_APP_ID") {
            Some(app_id) => Some(GitHubConfig {
                webhook_secret: non_empty("GITHUB_WEBHOOK_SECRET")
                    .context("GITHUB_APP_ID is set but GITHUB_WEBHOOK_SECRET is missing")?,
                app_id,
                private_key: non_empty("GITHUB_APP_PRIVATE_KEY")
                    .context("GITHUB_APP_ID is set but GITHUB_APP_PRIVATE_KEY is missing")?,
            }),
            None => None,
        };

        let discord = match non_empty("DISCORD_BOT_TOKEN") {
            Some(bot_token) => {
                let guild_ids = parse_guild_ids(non_empty("DISCORD_GUILD_IDS").as_deref())?;
                Some(DiscordConfig { bot_token, guild_ids })
            }
            None => None,
        };

        Ok(Self {
            database_url: std::env::var("DATABASE_URL")
                .unwrap_or_else(|_| "postgres://taggle:taggle@localhost:5433/taggle".into()),
            redis_url: std::env::var("REDIS_URL").unwrap_or_else(|_| "redis://localhost:6380".into()),
            port: std::env::var("INGEST_PORT").ok().and_then(|p| p.parse().ok()).unwrap_or(4001),
            github,
            discord,
        })
    }
}

/// Parses `DISCORD_GUILD_IDS` as a comma/space separated list of snowflakes.
fn parse_guild_ids(raw: Option<&str>) -> Result<Vec<u64>> {
    let Some(raw) = raw else { return Ok(Vec::new()) };

    let mut ids = Vec::new();
    for part in raw.split(|c: char| c == ',' || c.is_whitespace()) {
        let part = part.trim();
        if part.is_empty() {
            continue;
        }
        let id: u64 = part
            .parse()
            .with_context(|| format!("invalid Discord guild id {part:?} in DISCORD_GUILD_IDS"))?;
        if id == 0 {
            bail!("invalid Discord guild id 0 in DISCORD_GUILD_IDS");
        }
        ids.push(id);
    }
    ids.sort_unstable();
    ids.dedup();
    Ok(ids)
}

#[cfg(test)]
mod tests {
    use super::parse_guild_ids;

    #[test]
    fn parses_guild_ids() {
        assert_eq!(parse_guild_ids(None).unwrap(), Vec::<u64>::new());
        assert_eq!(parse_guild_ids(Some(" 1,2 3, 3 ")).unwrap(), vec![1, 2, 3]);
        assert!(parse_guild_ids(Some("1,nope")).is_err());
        assert!(parse_guild_ids(Some("0")).is_err());
    }
}
