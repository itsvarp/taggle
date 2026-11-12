# Taggle Protocol & API Reference

Taggle serves two independent sources over one REST + WebSocket API:

- **Discord presence** — Lanyard-compatible response shape, opcodes and event names.
- **GitHub activity** — commits, pull requests and issues.

You can consume one source or both. Nothing requires the other integration to be
configured.

Base URL (local dev): `http://localhost:4000`

## Routing rules

| Route | Source |
|---|---|
| `GET /v1/users/:id` | All digits → Discord snowflake; otherwise GitHub login |
| `GET /v1/discord/users/:user_id` | Discord, explicit |
| `GET /v1/github/users/:login` | GitHub, explicit |
| `GET /:discord_id.png` | Lanyard avatar quicklink (redirect) |

A GitHub username that is entirely numeric is ambiguous and resolves to Discord.
Use the explicit `/v1/github/users/...` route for those accounts.

---

## Discord REST API

### `GET /v1/discord/users/:user_id`

Returns the latest Lanyard-style presence for a monitored Discord account.

```json
{
  "success": true,
  "data": {
    "discord_user": {
      "id": "123456789012345678",
      "username": "example-user",
      "discriminator": "0001",
      "avatar": "a_0123456789abcdef0123456789abcdef",
      "global_name": "Example",
      "public_flags": 0,
      "bot": false
    },
    "discord_status": "online",
    "activities": [
      {
        "name": "Spotify",
        "type": 2,
        "details": "Night Drive",
        "state": "Example Artist",
        "timestamps": { "start": 1615529820677, "end": 1615530068733 }
      }
    ],
    "active_on_discord_desktop": true,
    "active_on_discord_mobile": false,
    "active_on_discord_web": false,
    "listening_to_spotify": true,
    "spotify": {
      "track_id": "0123456789abcdefghijkl",
      "timestamps": { "start": 1615529820677, "end": 1615530068733 },
      "song": "Night Drive",
      "artist": "Example Artist",
      "album_art_url": "https://i.scdn.co/image/0123456789abcdef0123456789abcdef",
      "album": "Night Drive"
    },
    "kv": {
      "location": "somewhere"
    }
  }
}
```

Unknown or not-monitored id:

```json
{ "success": false, "error": { "code": "user_not_monitored", "message": "User is not monitored by Taggle." } }
```

### Discord KV (authenticated)

Generate a key at `/` after `Connect Discord`. Keys are shown once; only a
SHA-256 hash is stored.

Rules (Lanyard-compatible): key `[A-Za-z0-9_]{1,255}`, value string ≤ 30,000
characters, max 512 pairs per user.

- `PUT /v1/discord/users/:user_id/kv/:key` — body `{"value":"..."}` or raw string
- `PATCH /v1/discord/users/:user_id/kv` — JSON object of key/value pairs
- `DELETE /v1/discord/users/:user_id/kv/:key`

Header: `Authorization: Bearer tggl_...` (raw key also accepted). KV writes
immediately republish `PRESENCE_UPDATE` to WebSocket clients.

---

## GitHub REST API

### `GET /v1/github/users/:login`

Query parameters (all optional):

- `types`: comma-separated `commits`, `pull_requests`, `issues`
- `repos`: comma-separated `owner/repo` filter
- `limit`: integer `1..100` capping each activity list

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
    "last_active_at": "2026-11-11T23:02:14Z",
    "commits": [
      {
        "sha": "a1b2c3d",
        "message": "feat: add websocket heartbeat",
        "repo": "example-user/demo-repo",
        "branch": "main",
        "private": false,
        "author": { "username": "example-user", "avatar_url": "https://github.com/example-user.png" },
        "url": "https://github.com/example-user/demo-repo/commit/a1b2c3d",
        "timestamp": "2026-11-11T23:02:14Z"
      }
    ],
    "pull_requests": [],
    "issues": [],
    "kv": { "currently_building": "taggle" }
  }
}
```

Private repository events have `private: true`, with `repo`, `title`, `message`,
`branch` and `url` set to `null`, and `labels`/`participants` emptied.

Not monitored → same `404 user_not_monitored` shape as Discord.

### GitHub KV (authenticated)

Same shape as Discord KV, but keys may also contain `.` and `-` (max 64), and
updates publish `ACTIVITY_UPDATE`.

- `PUT /v1/github/users/:login/kv/:key`
- `PATCH /v1/github/users/:login/kv`
- `DELETE /v1/github/users/:login/kv/:key`

Compatibility routes without the `/github` prefix (`/v1/users/:id/kv/...`) pick
the source by the same digits rule.

---

## WebSocket Protocol

Endpoint: `ws://localhost:4000/socket` (aliases: `/v1/socket`,
`/v1/github/socket`, `/v1/discord/socket`). One socket handles both sources.

| Opcode | Direction | Name | Payload `d` |
|---|---|---|---|
| `1` | Server → Client | Hello | `{"heartbeat_interval": 30000}` |
| `2` | Client → Server | Initialize | `{"subscribe_to_id": "..."}`, `{"subscribe_to_ids": [...]}` or `{"subscribe_to_all": true}`; optional `"filter"` for GitHub |
| `3` | Client ↔ Server | Heartbeat | Client sends `{"op":3}` every interval; server replies `{"op":3}` |
| `4` | Client → Server | Unsubscribe | `{"unsubscribe_from_id":"..."}`, `{"unsubscribe_from_ids":[...]}` or `{"unsubscribe_from_all":true}` |
| `0` | Server → Client | Event | `t: INIT_STATE \| PRESENCE_UPDATE \| ACTIVITY_UPDATE` |

### Examples

Subscribe to one Discord user:

```json
{ "op": 2, "d": { "subscribe_to_id": "123456789012345678" } }
```

Subscribe to a GitHub user with a filter:

```json
{ "op": 2, "d": { "subscribe_to_id": "example-user", "filter": { "types": ["commits"], "limit": 5 } } }
```

Subscribe to both at once:

```json
{ "op": 2, "d": { "subscribe_to_ids": ["123456789012345678", "example-user"] } }
```

Subscribe to every monitored Discord presence (capped at 500):

```json
{ "op": 2, "d": { "subscribe_to_all": true } }
```

### Events

`INIT_STATE` for a single id contains that presence/snapshot directly. For
multiple ids it contains a map keyed by the id sent by the client; ids that are
not monitored are omitted. `subscribe_to_all` returns a map of all presences.

```json
{ "op": 0, "t": "INIT_STATE", "d": { "123456789012345678": { "...": "presence" } } }
{ "op": 0, "t": "PRESENCE_UPDATE", "d": { "...": "presence" } }
{ "op": 0, "t": "ACTIVITY_UPDATE", "d": { "...": "github snapshot" } }
```

Invalid payloads receive an event instead of a disconnect:

```json
{ "op": 0, "t": "ERROR", "d": { "code": "invalid_payload" } }
```

Error codes: `unknown_opcode`, `requires_data_object`, `invalid_payload`.
Heartbeat timeout closes the socket with code `4004`.

Limits: 20 explicit subscriptions per socket, 500 presences for
`subscribe_to_all`, 1 MiB inbound text frames.

---

## Direct Discord gateway notes

The bot must be in the monitored server. Discord only emits presence events for
members whose presence the bot can see, which requires the privileged
`GUILD_PRESENCES` intent and `GUILD_MEMBERS` for member metadata. `DISCORD_GUILD_IDS`
optionally restricts monitoring to specific guilds; empty means every guild the
bot is in.

Presence is stored as a normalized snapshot. When a member leaves a monitored
guild, Taggle removes their presence so a stale "online" state is not served.

---

## Security notes

- Bot token stays in the ingest service and is never sent to the gateway or logs.
- GitHub webhook signatures use constant-time HMAC verification and delivery-id
  deduplication.
- Discord ids and GitHub logins are validated before becoming Redis key
  suffixes; all SQL is parameterized.
- API keys are `tggl_` + 32 random bytes. Only a SHA-256 hash is stored and
  compared with `Plug.Crypto.secure_compare/2`.
- OAuth `state` values are single-use (`GETDEL`) with a 10-minute TTL.
- Session cookies are `HttpOnly`, `SameSite=Lax`, and `Secure` over HTTPS.
