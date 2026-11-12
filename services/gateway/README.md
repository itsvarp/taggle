# Taggle Gateway

Elixir service that exposes Taggle over REST, SVG embeds, self-hosted JS SDK, Prometheus metrics, and WebSocket.

- Discord presences: `GET /v1/discord/users/:id`, `GET /v1/users/:id`
- GitHub activity: `GET /v1/github/users/:login`, `GET /v1/users/:login`
- Unified profile: `GET /v1/profiles/:id` (resolves Discord ID or GitHub login to `{discord, github}`)
- Pure-vector SVG embeds: `GET /v1/users/:id/card.svg`, `GET /v1/profiles/:id/card.svg`, `GET /v1/users/:id/badge.svg`
- Self-hosted Web Component & SDK: `GET /taggle.js` (`TaggleClient` + `<taggle-card>`)
- WebSocket: `/socket` (`INIT_STATE`, `PRESENCE_UPDATE`, `ACTIVITY_UPDATE`)
- Observability: `GET /health` (Postgres + Redis check), `GET /metrics` (Prometheus)
- Account linking, API keys, and KV management happen via Discord slash commands (`/connect`, `/apikey`, `/kv`, `/status`) handled by `services/ingest`, plus Bearer-authenticated KV REST endpoints (`/v1/*/kv`).

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
| `PORT` | optional | HTTP/WS listen port (default `4000`) |
| `RATE_LIMIT_READ_RPM` | optional | Per-IP read/WS requests per minute (default `120`) |
| `RATE_LIMIT_KV_RPM` | optional | Per-IP KV write requests per minute (default `30`) |
| `TRUST_PROXY` | optional | Set `true` behind a trusted reverse proxy to read `x-forwarded-for` |
| `METRICS_TOKEN` | optional | If set, `GET /metrics` requires `Authorization: Bearer <token>` |

The gateway never receives `DISCORD_BOT_TOKEN`; that belongs to `services/ingest`.
