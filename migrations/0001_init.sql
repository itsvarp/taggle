-- Taggle schema. Owned/applied by services/ingest at startup; gateway only reads/writes
-- users.api_key_hash and user_kv.

CREATE TABLE IF NOT EXISTS users (
    id           BIGINT PRIMARY KEY,              -- GitHub account id
    login        TEXT   NOT NULL UNIQUE,          -- lowercase
    name         TEXT,
    avatar_url   TEXT,
    api_key_hash TEXT,                            -- sha256 hex of the user's API key
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS installations (
    id            BIGINT PRIMARY KEY,             -- GitHub App installation id
    account_login TEXT   NOT NULL,                -- lowercase, FK-ish to users.login
    created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS installations_login_idx ON installations (account_login);

-- One row per commit / pull request / issue, attributed to the installation owner.
CREATE TABLE IF NOT EXISTS activities (
    login       TEXT        NOT NULL,
    kind        TEXT        NOT NULL CHECK (kind IN ('commit', 'pull_request', 'issue')),
    key         TEXT        NOT NULL,             -- repo@sha | repo#number
    data        JSONB       NOT NULL,
    occurred_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (login, kind, key)
);
CREATE INDEX IF NOT EXISTS activities_recent_idx ON activities (login, kind, occurred_at DESC);

CREATE TABLE IF NOT EXISTS user_kv (
    login TEXT NOT NULL,
    key   TEXT NOT NULL,
    value TEXT NOT NULL,
    PRIMARY KEY (login, key)
);

-- Durable copy of the Redis snapshot; gateway reads it on a Redis miss (read-through).
CREATE TABLE IF NOT EXISTS snapshots (
    login      TEXT PRIMARY KEY,
    data       JSONB       NOT NULL,
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
