-- Performance indexes for hot queries in gateway and ingest.

CREATE INDEX IF NOT EXISTS discord_presences_updated_idx
    ON discord_presences (updated_at DESC);

CREATE INDEX IF NOT EXISTS activities_login_occurred_idx
    ON activities (login, occurred_at DESC);
