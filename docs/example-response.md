# Example Responses

## Discord presence

Endpoint:

```http
GET /v1/discord/users/123456789012345678
# or the unified route: GET /v1/users/123456789012345678
```

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
        "type": 2,
        "timestamps": { "start": 1615529820677, "end": 1615530068733 },
        "sync_id": "0123456789abcdefghijkl",
        "state": "Example Artist",
        "party": { "id": "spotify:123456789012345678" },
        "name": "Spotify",
        "id": "spotify:1",
        "flags": 48,
        "details": "Night Drive",
        "created_at": 1615529838051,
        "assets": {
          "large_text": "Night Drive",
          "large_image": "spotify:0123456789abcdef0123456789abcdef"
        }
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

## GitHub activity

Endpoint:

```http
GET /v1/github/users/example-user
# or the unified route: GET /v1/users/example-user
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
    "last_active_at": "2026-11-12T00:42:44Z",
    "commits": [
      {
        "sha": "1a714b2",
        "message": "Update project name in README.md",
        "repo": "example-user/demo-repo",
        "branch": "main",
        "private": false,
        "author": {
          "username": "example-user",
          "avatar_url": "https://github.com/example-user.png"
        },
        "url": "https://github.com/example-user/demo-repo/commit/1a714b215c0af46a400f5849e19681b9a1f2e117",
        "timestamp": "2026-11-12T00:42:44Z"
      }
    ],
    "pull_requests": [
      {
        "id": 42,
        "number": 42,
        "title": "feat: add websocket reconnect jitter",
        "repo": "example-user/demo-repo",
        "branch": "feat/ws-jitter",
        "state": "open",
        "draft": false,
        "private": false,
        "checks_status": "pending",
        "deployment_status": "building",
        "review_threads": { "resolved": 2, "total": 4 },
        "participants": [{ "username": "example-user", "avatar_url": "https://github.com/example-user.png" }],
        "author": { "username": "example-user", "avatar_url": "https://github.com/example-user.png" },
        "url": "https://github.com/example-user/demo-repo/pull/42",
        "updated_at": "2026-11-11T22:50:00Z"
      }
    ],
    "issues": [
      {
        "number": 7,
        "title": "Presence disappears after reconnect",
        "repo": "example-org/example-project",
        "state": "closed",
        "labels": [
          { "name": "bug", "color": "d73a4a" }
        ],
        "comments": 5,
        "private": false,
        "author": { "username": "example-user", "avatar_url": "https://github.com/example-user.png" },
        "url": "https://github.com/example-org/example-project/issues/7",
        "updated_at": "2026-10-26T10:26:09Z"
      }
    ],
    "kv": {}
  }
}
```
