# Taggle Gateway

Elixir service that exposes Taggle over REST + WebSocket.

- Discord presences: `GET /v1/discord/users/:id`, `GET /v1/users/:id`
- GitHub activity: `GET /v1/github/users/:login`, `GET /v1/users/:login`
- WebSocket: `/socket` (`INIT_STATE`, `PRESENCE_UPDATE`, `ACTIVITY_UPDATE`)
- OAuth: `/auth/github`, `/auth/discord`
- KV + API keys: portal at `/`

Run locally:

```bash
cd services/gateway
infisical run --env dev --path /github-app --domain http://localhost:8080 -- mise exec -- mix run --no-halt
```

Environment:

| Variable | Required | Purpose |
|---|---|---|
| `DATABASE_URL` | yes | Postgres |
| `REDIS_URL` | yes | Redis |
| `SESSION_SECRET` | production | signs session cookies |
| `GITHUB_APP_CLIENT_ID` / `_SECRET` | for GitHub OAuth | GitHub sign-in |
| `GITHUB_APP_SLUG` | optional | portal install link |
| `DISCORD_CLIENT_ID` / `_SECRET` | for Discord KV | Discord sign-in |
| `DISCORD_INVITE_URL` | optional | portal invite link |
| `INGEST_URL` | optional | manual GitHub sync target |

The gateway never receives `DISCORD_BOT_TOKEN`; that belongs to `services/ingest`.
