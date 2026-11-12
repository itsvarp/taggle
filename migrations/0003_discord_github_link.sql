-- Links a Discord user id to the GitHub account connected via the Discord bot (/connect).
CREATE TABLE IF NOT EXISTS discord_github_links (
    discord_user_id TEXT PRIMARY KEY,
    github_id       BIGINT NOT NULL,
    github_login    TEXT   NOT NULL,
    connected_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS discord_github_links_login_idx ON discord_github_links (github_login);
