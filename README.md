# Taggle

Inspired by the [Lanyard](https://github.com/Phineas/lanyard) project.

**Your Discord presence and GitHub activity, in one small API.**

Taggle watches a Discord server and a GitHub account, keeps the latest state in
Postgres and Redis, and serves it as plain JSON over REST and WebSocket. It's
meant for the small things: a "listening to" card on a portfolio, a status line
in a README, a dashboard that shows what you shipped today.

Discord and GitHub are independent. Run Taggle with both integrations, with only
Discord, or with only GitHub. If a set of credentials is missing, that half
simply stays off; it doesn't stop the other one from working.

Taggle is self-hosted. There is no shared public instance and no account to
create. You run it, point it at your Discord server and your GitHub account, and
decide who can reach it.

## Quick start

```bash
git clone https://github.com/itsvarp/taggle.git
cd taggle

# Start Postgres, Redis, the gateway and the ingest worker.
docker compose up -d --build

curl http://localhost:4000/health
# {"success":true}
```

With no credentials the stack still starts and serves `/health`, which is handy
for checking that everything is wired up. Add a Discord bot token, a GitHub App,
or both to make it useful.

| Endpoint | What it is |
|---|---|
| `http://localhost:4000/v1/users/:id` | REST API (auto-detects Discord ID or GitHub login) |
| `http://localhost:4000/v1/profiles/:id` | Unified `{discord, github}` profile endpoint |
| `http://localhost:4000/v1/users/:id/card.svg` | Built-in pure-vector SVG status card (`/badge.svg` for pill badge) |
| `http://localhost:4000/taggle.js` | Self-hosted `TaggleClient` & `<taggle-card>` Web Component |
| `http://localhost:4000/socket` | WebSocket (`INIT_STATE`, `PRESENCE_UPDATE`, `ACTIVITY_UPDATE`) |
| `http://localhost:4000/health` & `/metrics` | Gateway Postgres+Redis health & Prometheus metrics |
| `http://localhost:4001/health` & `/metrics` | Ingest Postgres+Redis health & Prometheus metrics |

Account linking (`/connect`), API keys (`/apikey`), KV (`/kv`), and status checks
(`/status`) work directly via Discord bot slash commands.

The compose file binds every port to `127.0.0.1`, so nothing is exposed to the
network by default. In production, put a reverse proxy in front of the gateway.

## How it works

```text
Discord Gateway ──ws──▶  ingest (Rust)  ──▶  Postgres + Redis
GitHub webhooks  ─────▶  ingest (Rust)  ──▶  Postgres + Redis
                                                   │
                          gateway (Elixir) ◀───────┘
                                │
                          REST + WebSocket
```

`ingest` holds the Discord bot connection and receives GitHub webhooks. It
normalizes both sources and writes them to Postgres and Redis. `gateway` only
reads that state and serves it publicly; it never sees the Discord bot token.

Redis is hot state and pub/sub. Postgres is the durable copy. If Redis is
flushed, the gateway rebuilds the response from Postgres on the next read.

## Reading Discord presence

The unified route picks the source by looking at the id:

```http
GET /v1/users/:id
GET /v1/profiles/:id
```

All digits means a Discord snowflake. Anything else is treated as a GitHub
login. A GitHub username that happens to be entirely numeric is ambiguous and
resolves to Discord; use the explicit route for those. `/v1/profiles/:id`
returns `{ "discord": ..., "github": ... }` when accounts are linked via
Discord `/connect`. You can also be explicit:

```http
GET /v1/discord/users/:user_id
GET /v1/github/users/:login
```

A response looks like this (all identifiers below are made up):

```json
{
  "success": true,
  "data": {
    "discord_user": {
      "id": "123456789012345678",
      "username": "example-user",
      "discriminator": "0",
      "avatar": "a_0123456789abcdef0123456789abcdef",
      "global_name": "Example",
      "public_flags": 0,
      "bot": false
    },
    "discord_status": "online",
    "activities": [
      {
        "id": "spotify:1",
        "name": "Spotify",
        "type": 2,
        "details": "Night Drive",
        "state": "Example Artist",
        "sync_id": "0123456789abcdefghijkl",
        "timestamps": { "start": 1760000000000, "end": 1760000240000 },
        "assets": {
          "large_image": "spotify:0123456789abcdef0123456789abcdef",
          "large_text": "Night Drive"
        }
      }
    ],
    "active_on_discord_desktop": true,
    "active_on_discord_mobile": false,
    "active_on_discord_web": false,
    "listening_to_spotify": true,
    "spotify": {
      "track_id": "0123456789abcdefghijkl",
      "song": "Night Drive",
      "artist": "Example Artist",
      "album": "Night Drive",
      "album_art_url": "https://cdn.example.com/album-art.jpg",
      "timestamps": { "start": 1760000000000, "end": 1760000240000 }
    },
    "kv": {}
  }
}
```

The fields map directly to what Discord sends:

- `discord_status` is `online`, `idle`, `dnd`, or `offline`.
- `activities` keeps Discord's activity objects. The `type` values are the usual
  ones: `0` playing, `1` streaming, `2` listening, `3` watching, `4` custom
  status, `5` competing. Most widgets skip type `4`.
- `listening_to_spotify` and `spotify` are convenience fields built from the
  Spotify activity, so you don't have to parse `activities` yourself.
- `coding` is a convenience object (e.g. `{"editor", "file", "workspace"}`)
  when an IDE Rich Presence (VS Code, Neovim, Cursor, Zed, JetBrains, etc.) is active.
- `active_on_discord_desktop`, `active_on_discord_mobile` and
  `active_on_discord_web` tell you where the person is online.
- `kv` is the user's key-value store, described below.

An id that isn't monitored returns `404`:

```json
{
  "success": false,
  "error": {
    "code": "user_not_monitored",
    "message": "User is not monitored by Taggle."
  }
}
```

### What "monitored" means for Discord

Taggle does not ask people to log in. The bot sits in a Discord server, and any
member whose presence the bot can see is readable through the API. Joining the
server is the whole setup.

A few consequences worth knowing:

- Discord doesn't send presences for invisible members, and on startup it only
  reports members who are currently online.
- When someone leaves the server, their stored presence is removed.
- The bot can monitor several servers. If `DISCORD_GUILD_IDS` is empty, every
  server the bot is in is included.

## Reading GitHub activity

```http
GET /v1/github/users/:login
```

```json
{
  "success": true,
  "data": {
    "user": {
      "id": 1234567,
      "username": "example-user",
      "name": "Example User",
      "avatar_url": "https://github.com/example-user.png",
      "profile_url": "https://github.com/example-user"
    },
    "last_active_at": "2026-01-15T12:34:56Z",
    "commits": [
      {
        "sha": "a1b2c3d",
        "message": "feat: add websocket heartbeat",
        "repo": "example-user/demo-repo",
        "branch": "main",
        "private": false,
        "author": {
          "username": "example-user",
          "avatar_url": "https://github.com/example-user.png"
        },
        "url": "https://github.com/example-user/demo-repo/commit/a1b2c3d",
        "timestamp": "2026-01-15T12:34:56Z"
      }
    ],
    "pull_requests": [
      {
        "id": 42,
        "number": 42,
        "title": "feat: reconnect after gateway close",
        "repo": "example-user/demo-repo",
        "branch": "feat/reconnect",
        "state": "open",
        "draft": false,
        "private": false,
        "checks_status": "pending",
        "deployment_status": "building",
        "review_threads": { "resolved": 2, "total": 4 },
        "participants": [
          {
            "username": "example-user",
            "avatar_url": "https://github.com/example-user.png"
          }
        ],
        "author": {
          "username": "example-user",
          "avatar_url": "https://github.com/example-user.png"
        },
        "url": "https://github.com/example-user/demo-repo/pull/42",
        "updated_at": "2026-01-15T11:50:00Z"
      }
    ],
    "issues": [
      {
        "number": 7,
        "title": "Presence disappears after reconnect",
        "repo": "example-user/demo-repo",
        "state": "open",
        "labels": [
          { "name": "bug", "color": "d73a4a" }
        ],
        "comments": 3,
        "private": false,
        "author": {
          "username": "example-user",
          "avatar_url": "https://github.com/example-user.png"
        },
        "url": "https://github.com/example-user/demo-repo/issues/7",
        "updated_at": "2026-01-14T09:20:00Z"
      }
    ],
    "kv": {
      "currently_building": "taggle"
    }
  }
}
```

`commits`, `pull_requests` and `issues` are trimmed to the most recent entries.
You can filter them with query parameters:

| Parameter | Example | Notes |
|---|---|---|
| `types` | `commits,pull_requests` | Any of `commits`, `pull_requests`, `issues` |
| `repos` | `example-user/demo-repo` | Comma-separated `owner/repo` list |
| `limit` | `10` | Caps each list; `1` to `100` |

```http
GET /v1/github/users/example-user?types=commits&repos=example-user/demo-repo&limit=5
```

### Private repositories

Private data is opt-in at the GitHub App level: Taggle only sees repositories
you selected when installing the app.

Even then, private events are masked in the public response. They keep their
type and timestamp, but `repo`, `title`, `message`, `branch` and `url` are
`null`, and labels and participants are empty. That way a public widget can say
"pushed to a private repo" without leaking the name or content.

### How GitHub data stays fresh

If you configure a GitHub App webhook, pushes and pull requests show up almost
immediately. Without webhooks (or for public commits), `ingest` runs an adaptive
ETag-backed poller (`SYNC_INTERVAL_SECS`, default `60`, minimum `5`) backed by
Redis L2 ETag caching, automatically skipping redundant polls right after fresh webhooks.

## KV store

Every monitored account has a small public key-value store. It's for the extra
bits your widget needs: a timezone, a "currently building" line, a link to a
project, whatever. It shows up under `kv` in the same REST response, and updates
are pushed over the WebSocket.

To write to it you need an API key:

1. Join a monitored Discord server and run `/apikey` (ephemeral response — only you see it).
2. Optionally run `/connect <github_username>` to link your GitHub account.
3. Creating a new key rotates and replaces the old one; only a SHA-256 hash is stored. You can also run `/kv set`, `/kv delete`, and `/kv list` directly in Discord.

Send it as a bearer token:

```http
Authorization: Bearer tggl_...
```

Keys are prefixed with `tggl_` so they're easy to spot in logs and secret
scanners.

### Discord KV

```http
PUT    /v1/discord/users/:user_id/kv/:key
PATCH  /v1/discord/users/:user_id/kv
DELETE /v1/discord/users/:user_id/kv/:key
```

`PUT` takes either a raw string body or `{"value": "..."}`. `PATCH` takes a JSON
object of several pairs:

```http
PATCH /v1/discord/users/123456789012345678/kv
Authorization: Bearer tggl_...
Content-Type: application/json

{
  "location": "somewhere",
  "currently_building": "taggle"
}
```

Rules:

| Rule | Value |
|---|---|
| Key | `[A-Za-z0-9_]{1,255}` |
| Value | string, number or boolean, up to 30,000 characters |
| Total pairs | 512 per user |

### GitHub KV

```http
PUT    /v1/github/users/:login/kv/:key
PATCH  /v1/github/users/:login/kv
DELETE /v1/github/users/:login/kv/:key
```

Same shape, with slightly different limits:

| Rule | Value |
|---|---|
| Key | `[a-zA-Z0-9._-]{1,64}` |
| Value | string, number or boolean, up to 30,000 characters |
| Pairs per `PATCH` | 50 |

The compatibility routes under `/v1/users/:id/kv/...` work too, and pick the
source by the same digits rule.

KV writes are rebroadcast right away: Discord changes publish
`PRESENCE_UPDATE`, GitHub changes publish `ACTIVITY_UPDATE`.

## Quicklinks

Lanyard-style avatar redirects are available for Discord:

```http
GET /:discord_id.png
```

```html
<img src="https://taggle.example.com/123456789012345678.png" alt="" />
```

The server looks up the avatar and redirects to Discord's CDN. `png`, `gif`,
`webp`, `jpg` and `jpeg` are accepted. GitHub snapshots already include
`avatar_url`, so there's no separate quicklink route for them.

## Embeddable SVG cards, badges & browser SDK

Taggle serves built-in, zero-external-dependency SVG widgets directly from the gateway:

```http
GET /v1/users/:id/card.svg?theme=dark&accent=58a6ff&compact=false&hide_github=false
GET /v1/profiles/:id/card.svg
GET /v1/users/:id/badge.svg
```

- Supported themes: `dark` (default), `light`, `midnight`, `catppuccin`.
- Pure vector (built-in monogram avatar, zero third-party image proxies or external URLs), strict XML escaping, and `Content-Security-Policy: default-src 'none'; style-src 'unsafe-inline'`.

For live WebSocket widgets in web pages without external libraries, embed Taggle's self-hosted Web Component & SDK:

```html
<script type="module" src="https://taggle.example.com/taggle.js"></script>
<taggle-card subject="123456789012345678" theme="dark"></taggle-card>
```

Or use `TaggleClient` programmatically:

```js
import { TaggleClient } from "https://taggle.example.com/taggle.js";
const client = new TaggleClient({ ids: ["123456789012345678", "octocat"] });
client.on("update", (data) => console.log(data));
```

## WebSocket

```text
wss://taggle.example.com/socket
```

Aliases: `/v1/socket`, `/v1/discord/socket`, `/v1/github/socket`. One socket
can carry Discord and GitHub subscriptions at the same time.

When you connect, the server sends Hello:

```json
{ "op": 1, "d": { "heartbeat_interval": 30000 } }
```

Then send an Initialize payload:

```json
{ "op": 2, "d": { "subscribe_to_id": "123456789012345678" } }
```

```json
{ "op": 2, "d": { "subscribe_to_ids": ["123456789012345678", "example-user"] } }
```

```json
{ "op": 2, "d": { "subscribe_to_all": true } }
```

GitHub subscriptions accept an optional filter, which is applied to both the
initial snapshot and later updates:

```json
{
  "op": 2,
  "d": {
    "subscribe_to_id": "example-user",
    "filter": { "types": ["commits"], "limit": 5 }
  }
}
```

### Opcodes

| Opcode | Direction | Name | Payload `d` |
|---|---|---|---|
| `1` | Server → Client | Hello | `{"heartbeat_interval": 30000}` |
| `2` | Client → Server | Initialize | `subscribe_to_id`, `subscribe_to_ids`, `subscribe_to_all`, optional `filter` |
| `3` | Both | Heartbeat | Client sends `{}`; server replies `{}` |
| `4` | Client → Server | Unsubscribe | `unsubscribe_from_id`, `unsubscribe_from_ids`, `unsubscribe_from_all` |
| `0` | Server → Client | Event | `t` is `INIT_STATE`, `PRESENCE_UPDATE` or `ACTIVITY_UPDATE` |

Send a heartbeat every 30 seconds:

```json
{ "op": 3 }
```

The server answers with `{"op":3}`. If it doesn't hear from you for 60 seconds,
it closes the socket with code `4004`.

Re-sending `op: 2` replaces your previous subscriptions.

### Events

```json
{ "op": 0, "t": "INIT_STATE", "d": { "...": "presence or snapshot" } }
```

```json
{ "op": 0, "t": "PRESENCE_UPDATE", "d": { "...": "presence" } }
```

```json
{ "op": 0, "t": "ACTIVITY_UPDATE", "d": { "...": "github snapshot" } }
```

`INIT_STATE` behaves differently depending on how many ids you sent:

- One id: `d` is that presence or snapshot, or `null` if nothing is known yet.
- Several ids: `d` is a map keyed by the id you sent; unknown ids are omitted.
- `subscribe_to_all`: `d` is a map of every monitored Discord presence.

`PRESENCE_UPDATE` is sent for Discord changes. `ACTIVITY_UPDATE` is sent for
GitHub changes, already filtered if you set a filter.

### Errors and limits

Bad input doesn't disconnect you. You get an error event instead:

```json
{ "op": 0, "t": "ERROR", "d": { "code": "invalid_payload" } }
```

| Code | Meaning |
|---|---|
| `unknown_opcode` | The `op` isn't one of the supported values |
| `requires_data_object` | `op: 2` or `op: 4` arrived without a `d` object |
| `invalid_payload` | Bad JSON, an invalid id, or an empty subscription |
| `rate_limited` | Inbound frame rate limit exceeded (closes with `4008` on sustained flood) |

Limits:

- 20 explicit ids per socket.
- 500 presences for `subscribe_to_all`.
- 1 MiB per inbound text frame.
- 40 inbound frames / 10s soft limit (100 frames / 10s hard disconnect with code `4008`).

## Self-hosting

### What you need

- Docker and Docker Compose.
- A domain and TLS if the API will be public.
- A Discord application (for Discord) and/or a GitHub App (for GitHub).

You don't need both. A Discord-only deployment is just a bot token and a server.

### Configuration

Taggle reads everything from environment variables. In compose, these are read
from the shell that starts the stack, so export them first or inject them with
your secrets manager.

| Variable | Used by | Notes |
|---|---|---|
| `PORT` | gateway | Default `4000`. |
| `DATABASE_URL` | both | Defaults to the compose Postgres. |
| `REDIS_URL` | both | Defaults to the compose Redis. |
| `INGEST_PORT` | ingest | Default `4001`. |
| `SYNC_INTERVAL_SECS` | ingest | GitHub adaptive polling interval. Default `60`, minimum `5`. Skips redundant polling right after fresh webhooks. |
| `RATE_LIMIT_READ_RPM` | gateway | Per-IP REST/SVG/WS requests per minute. Default `120`. |
| `RATE_LIMIT_KV_RPM` | gateway | Per-IP KV write requests per minute. Default `30`. |
| `TRUST_PROXY` | gateway | Set `true` behind a reverse proxy so rate limits inspect `x-forwarded-for`. |
| `METRICS_TOKEN` | gateway | Optional bearer token guarding `GET /metrics`. |

Discord:

| Variable | Notes |
|---|---|
| `DISCORD_BOT_TOKEN` | Bot token (`ingest` only). Presence isn't monitored without it. |
| `DISCORD_APP_ID` | Optional override for slash command registration (`/connect`, `/apikey`, `/kv`, `/status`). |
| `DISCORD_GUILD_IDS` | Comma-separated server ids to monitor and register guild commands on. Empty means every server the bot is in. |

GitHub:

| Variable | Notes |
|---|---|
| `GITHUB_APP_ID` | App id. |
| `GITHUB_APP_PRIVATE_KEY` | App private key in PEM format. Literal `\n` escapes are accepted. |
| `GITHUB_WEBHOOK_SECRET` | Secret you set on the GitHub App webhook. Required when the app is configured. |

### Discord bot

1. Create an application at
   <https://discord.com/developers/applications>.
2. Open **Bot** and enable **Presence Intent** and **Server Members Intent**
   under Privileged Gateway Intents. Presence does not arrive without them.
3. Invite the bot to your server with the `bot` and `applications.commands` scopes.
4. Set `DISCORD_BOT_TOKEN`. Set `DISCORD_GUILD_IDS` if the bot is in more than
   one server and you only want one monitored (also speeds up slash command registration).

Members can use `/connect <github_username>`, `/apikey`, `/kv`, and `/status` directly in Discord.

### GitHub App

1. Create an app under **Settings → Developer settings → GitHub Apps**.
2. Set **Webhook URL** to
   `${BASE_URL}/webhooks/github` and pick a secret. The webhook has to reach the
   ingest worker; proxy that one path to port `4001`.
3. Grant read-only repository permissions: **Metadata**, **Contents**, **Pull
   requests**, **Issues**, **Checks**, **Commit statuses**, **Deployments**.
4. Subscribe to events: **Push**, **Pull request**, **Pull request review**,
   **Pull request review thread**, **Pull request review comment**, **Issues**,
   **Issue comment**, **Check run**, **Deployment status**, **Installation**,
   **Installation repositories**.
5. Set `GITHUB_APP_ID`, `GITHUB_APP_PRIVATE_KEY`, and `GITHUB_WEBHOOK_SECRET`.
6. Install the app on your account or organization, then run `/connect <github_username>` in Discord.

Without webhooks, Taggle still works via adaptive ETag polling (`SYNC_INTERVAL_SECS`).

### Start the stack

```bash
# Discord (optional)
export DISCORD_BOT_TOKEN="..."
export DISCORD_GUILD_IDS="..."

# GitHub (optional)
export GITHUB_APP_ID="..."
export GITHUB_APP_PRIVATE_KEY="$(cat your-app.private-key.pem)"
export GITHUB_WEBHOOK_SECRET="..."

docker compose up -d --build
docker compose logs -f
```

Then terminate TLS in front of `127.0.0.1:4000` and make sure your proxy
forwards WebSocket upgrades on `/socket`. If you use GitHub webhooks, forward
`/webhooks/github` to `127.0.0.1:4001` as well.

The compose stack also exposes Postgres on `127.0.0.1:5433` and Redis on
`127.0.0.1:6380` for local development. Don't publish those.

### Discord-only

Leave every `GITHUB_*` variable empty. The GitHub routes return
`user_not_monitored`, and the Discord side works normally.

### GitHub-only

Leave every `DISCORD_*` variable empty. `subscribe_to_all` returns an empty map;
GitHub routes work normally.

## Security notes

- The Discord bot token only exists in `ingest`. The gateway never receives it.
- GitHub webhook signatures are checked with a constant-time HMAC comparison,
  deliveries are deduplicated by validated delivery id, and processor concurrency
  sheds load with `503` when saturated.
- Discord ids, interaction tokens, avatar hashes, and GitHub logins/repo slugs
  are strictly validated before DB/Redis use; Discord `activities` strip raw `session_id`s.
- All SQL is parameterized and presence/KV state writes serialize cleanly with Postgres
  advisory locks + atomic Redis Lua string-splice merges.
- API keys use `tggl_` + 32 CSPRNG bytes. Tokens fail a fast format check before
  database lookup; only a SHA-256 hash is stored, and comparisons use
  `Plug.Crypto.secure_compare/2`.
- Built-in Redis/ETS fixed-window rate limiting guards all read and KV routes (`429 rate_limited`),
  and WebSocket sockets enforce per-connection inbound rate limits (`ERROR rate_limited` / close `4008`).
- Built-in SVG cards/badges are pure-vector (no external images/fonts), XML-escaped,
  and served with `Content-Security-Policy: default-src 'none'; style-src 'unsafe-inline'`.
- Prometheus `/metrics` is available on both `gateway` (`4000`) and `ingest` (`4001`),
  and `GET /health` verifies live Postgres + Redis connectivity.
- CORS is open for public reads. Authenticated writes use a bearer token, not a
  cookie, so a browser on another origin can't write on your behalf.

### Presence is public

Taggle is a broadcast tool. Anyone who can reach the API can read every presence
it stores, and `subscribe_to_all` returns the whole set at once. That's the same
model Lanyard uses, and it's fine when the server is a small community that
knows the data is public. If that's not true for your server, narrow
`DISCORD_GUILD_IDS`, keep the API private, or put an allowlist in front of it.

## Development

```bash
make up-db        # Postgres + Redis only
make test         # cargo test + mix test
```

Start the services with the environment variables from the configuration
section. The Makefile also has `make ingest` and `make gateway` targets for a
local setup that injects secrets with Infisical.

The repository layout:

```text
services/ingest      Rust: Discord Gateway, slash commands, GitHub webhooks & sync, /metrics
services/gateway     Elixir: REST, WebSocket, KV, SVG embeds, /taggle.js, rate limit, /metrics
sdk/                 First-party browser SDK & <taggle-card> Web Component
migrations/          Postgres schema & indexes
```

`mix test` expects the local Postgres and Redis from `make up-db` to be running.

## Limitations

A few things are intentionally simple:

- Presence is only available for members the bot can see. Invisible members
  don't appear; on startup `GUILD_CREATE`, previously cached members absent from
  the initial online list are reconciled to `offline`.
- GitHub updates without webhooks are delayed by the polling interval.
- There's no per-user authentication for reads. The API is public by design, or
  private because you put it behind an internal network or allowlist.
- Built-in rate limiting enforces per-IP limits (`RATE_LIMIT_READ_RPM`,
  `RATE_LIMIT_KV_RPM`) and WebSocket frame limits; add edge protection at your
  reverse proxy if exposed directly to untrusted traffic.

---

Taggle is not affiliated with Discord, GitHub, Spotify, or Lanyard.
