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
| `http://localhost:4000/` | Portal: connect accounts, create API keys |
| `http://localhost:4000/v1/...` | REST API |
| `http://localhost:4000/socket` | WebSocket |
| `http://localhost:4001/health` | Ingest worker health |

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
```

All digits means a Discord snowflake. Anything else is treated as a GitHub
login. A GitHub username that happens to be entirely numeric is ambiguous and
resolves to Discord; use the explicit route for those. You can also be explicit:

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
immediately. Without webhooks, `ingest` polls on an interval
(`SYNC_INTERVAL_SECS`, default `180`, minimum `30`). Polling is enough for a
portfolio; webhooks are better if you want a live feed.

## KV store

Every monitored account has a small public key-value store. It's for the extra
bits your widget needs: a timezone, a "currently building" line, a link to a
project, whatever. It shows up under `kv` in the same REST response, and updates
are pushed over the WebSocket.

To write to it you need an API key:

1. Open the portal at `/`.
2. Connect Discord and/or GitHub.
3. Press **Create API key**. The key is shown once; only a SHA-256 hash is
   stored. Creating a new key replaces the old one.

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

Limits:

- 20 explicit ids per socket.
- 500 presences for `subscribe_to_all`.
- 1 MiB per inbound text frame.

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
| `BASE_URL` | gateway | Public URL, e.g. `https://taggle.example.com`. Used for OAuth redirects and cookies. |
| `SESSION_SECRET` | gateway | At least 16 characters. Set it in production, or sessions reset on restart. |
| `PORT` | gateway | Default `4000`. |
| `DATABASE_URL` | both | Defaults to the compose Postgres. |
| `REDIS_URL` | both | Defaults to the compose Redis. |
| `INGEST_URL` | gateway | Internal URL of the ingest worker. |
| `INGEST_PORT` | ingest | Default `4001`. |
| `SYNC_INTERVAL_SECS` | ingest | GitHub polling interval. Default `180`, minimum `30`. |

Discord:

| Variable | Notes |
|---|---|
| `DISCORD_BOT_TOKEN` | Bot token. Presence isn't monitored without it. |
| `DISCORD_GUILD_IDS` | Comma-separated server ids to monitor. Empty means every server the bot is in. |
| `DISCORD_CLIENT_ID` | OAuth client id. Only needed for Discord KV API keys. |
| `DISCORD_CLIENT_SECRET` | OAuth client secret. Only needed for Discord KV API keys. |
| `DISCORD_INVITE_URL` | Optional invite link shown in the portal. |

GitHub:

| Variable | Notes |
|---|---|
| `GITHUB_APP_ID` | App id. |
| `GITHUB_APP_PRIVATE_KEY` | App private key in PEM format. Literal `\n` escapes are accepted. |
| `GITHUB_WEBHOOK_SECRET` | Secret you set on the GitHub App webhook. Required when the app is configured. |
| `GITHUB_APP_CLIENT_ID` | OAuth client id, for the portal's Connect GitHub flow. |
| `GITHUB_APP_CLIENT_SECRET` | OAuth client secret. |
| `GITHUB_APP_SLUG` | Optional. Used to build the app install link. |

### Discord bot

1. Create an application at
   <https://discord.com/developers/applications>.
2. Open **Bot** and enable **Presence Intent** and **Server Members Intent**
   under Privileged Gateway Intents. Presence does not arrive without them.
3. Invite the bot to your server with the `bot` scope. It doesn't need
   moderation permissions; it only needs to be a member.
4. Set `DISCORD_BOT_TOKEN`. Set `DISCORD_GUILD_IDS` if the bot is in more than
   one server and you only want one monitored.
5. For KV API keys, add `${BASE_URL}/auth/discord/callback` under **OAuth2 →
   Redirects** and set the client id and secret.

That's it. Members don't need to authorize anything; joining the server is
enough.

### GitHub App

1. Create an app under **Settings → Developer settings → GitHub Apps**.
2. Set **Homepage URL** to `${BASE_URL}`.
3. Set **Callback URL** to `${BASE_URL}/auth/github/callback`.
4. If you want instant updates, set **Webhook URL** to
   `${BASE_URL}/webhooks/github` and pick a secret. The webhook has to reach the
   ingest worker; proxy that one path to port `4001`.
5. Grant read-only repository permissions: **Metadata**, **Contents**, **Pull
   requests**, **Issues**, **Checks**, **Commit statuses**, **Deployments**.
6. Subscribe to events: **Push**, **Pull request**, **Pull request review**,
   **Pull request review thread**, **Pull request review comment**, **Issues**,
   **Issue comment**, **Check run**, **Deployment status**, **Installation**,
   **Installation repositories**.
7. Set `GITHUB_APP_ID`, `GITHUB_APP_PRIVATE_KEY`, `GITHUB_WEBHOOK_SECRET`,
   `GITHUB_APP_CLIENT_ID`, `GITHUB_APP_CLIENT_SECRET` and, optionally,
   `GITHUB_APP_SLUG`.
8. Install the app on your account or organization, then open the portal and
   connect GitHub.

Without webhooks, Taggle still works; it just polls every few minutes.

### Start the stack

```bash
export BASE_URL=https://taggle.example.com
export SESSION_SECRET="$(openssl rand -hex 32)"

# Discord (optional)
export DISCORD_BOT_TOKEN="..."
export DISCORD_GUILD_IDS="..."
export DISCORD_CLIENT_ID="..."
export DISCORD_CLIENT_SECRET="..."

# GitHub (optional)
export GITHUB_APP_ID="..."
export GITHUB_APP_PRIVATE_KEY="$(cat your-app.private-key.pem)"
export GITHUB_WEBHOOK_SECRET="..."
export GITHUB_APP_CLIENT_ID="..."
export GITHUB_APP_CLIENT_SECRET="..."

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
  and deliveries are deduplicated by delivery id.
- Discord ids and GitHub logins are validated before being used in Redis keys.
  SQL is parameterized.
- API keys are `tggl_` plus 32 random bytes. Only a SHA-256 hash is stored, and
  comparisons use `Plug.Crypto.secure_compare/2`.
- OAuth `state` values are single-use with a 10-minute TTL.
- Session cookies are `HttpOnly`, `SameSite=Lax`, and `Secure` over HTTPS.
- CORS is open for public reads. Authenticated writes use a bearer token, not a
  cookie, so a browser on another origin can't write on your behalf.
- There is no built-in rate limiting. If the API is public, add a limit at your
  reverse proxy.

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

The two services live in:

```text
services/ingest      Rust: Discord Gateway, GitHub webhooks, normalization
services/gateway     Elixir: REST, WebSocket, OAuth, KV, portal
migrations/          Postgres schema
```

`mix test` expects the local Postgres and Redis from `make up-db` to be running.

## Limitations

A few things are intentionally simple:

- Presence is only available for members the bot can see. Invisible members
  don't appear, and on startup only currently online members are reported.
- After a bot restart, Discord only sends the presences that are online at that
  moment. An entry can stay "online" until the next presence event corrects it.
- GitHub updates without webhooks are delayed by the polling interval.
- There's no per-user authentication for reads. The API is public by design, or
  private because you put it behind something that isn't.
- Rate limiting is left to the proxy.

---

Taggle is not affiliated with Discord, GitHub, Spotify, or Lanyard.
