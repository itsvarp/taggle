-- Discord presence support. The Rust ingest service writes presences from the
-- Discord gateway; the Elixir gateway serves them and owns OAuth users/KV.

-- Latest normalized Lanyard-style presence per Discord user id.
CREATE TABLE IF NOT EXISTS discord_presences (
    user_id    TEXT PRIMARY KEY,             -- Discord snowflake (digits only)
    data       JSONB       NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Discord accounts that signed in through OAuth. Used to issue API keys for KV.
CREATE TABLE IF NOT EXISTS discord_users (
    id           TEXT PRIMARY KEY,           -- Discord snowflake
    username     TEXT,
    global_name  TEXT,
    avatar       TEXT,
    api_key_hash TEXT,                       -- sha256 hex of the user's API key
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS discord_kv (
    user_id TEXT NOT NULL,
    key     TEXT NOT NULL,
    value   TEXT NOT NULL,
    PRIMARY KEY (user_id, key)
);
