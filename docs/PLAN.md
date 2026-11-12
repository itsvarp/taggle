# Taggle — Plan

Taggle, iki bağımsız gerçek zamanlı kaynağı aynı REST + WebSocket API üzerinden
sunan bir servistir:

- **Discord presence** — Lanyard ile aynı JSON şeması, opcode'lar ve event adları.
- **GitHub aktivitesi** — commit / PR / issue akışı.

Kullanıcı üç moddan birini seçebilir: yalnız Discord, yalnız GitHub veya ikisi
birlikte. İki entegrasyon birbirine bağımlı değildir; ilgili secret'lar yoksa o
entegrasyon sessizce devre dışı kalır.

## Mimari

```text
Discord Gateway ──WS──> [Rust: ingest] ──> Postgres + Redis (presence)
GitHub ──webhook──────> [Rust: ingest] ──> Postgres + Redis (activity)
                                              │
Siteler <──REST/WS── [Elixir: gateway] <──────┘
```

| Bileşen | Teknoloji | Sorumluluk |
|---|---|---|
| `services/ingest` | Rust (axum, tokio, sqlx, redis, reqwest, tokio-tungstenite) | GitHub webhook imza doğrulama, Discord Gateway botu, normalize, Postgres yazma, Redis `SET` + `PUBLISH` |
| `services/gateway` | Elixir (Bandit + Plug + WebSock, Ecto/Postgrex, Redix) | REST, WebSocket, GitHub + Discord OAuth, KV, portal |
| Redis | — | `taggle:state:{login}`, `taggle:discord:state:{id}`, `taggle:events:{login}`, `taggle:discord:events:{id}`, delivery dedupe |
| Postgres | — | users, installations, activities, snapshots, discord_presences, discord_users, discord_kv |
| Infisical (local) | — | Tüm uygulama secret'ları |

Servisler birbirine HTTP atmaz (yalnızca gateway → ingest manuel sync için); köprü
Redis + Postgres'tir. Redis boşsa gateway Postgres'ten snapshot/presence derleyip
geri yazar (read-through).

## API sözleşmesi

### Unified (önerilen)

- `GET /v1/users/:id` — yalnız rakamsa Discord snowflake, değilse GitHub login.
- `GET /v1/github/users/:login` — açık GitHub.
- `GET /v1/discord/users/:user_id` — açık Discord.
- `GET /:discord_id.png` — Lanyard quicklink (avatar).

### Discord (Lanyard uyumlu)

Yanıt:

```json
{
  "success": true,
  "data": {
    "discord_user": { "id": "...", "username": "...", "discriminator": "0", "avatar": "...", "global_name": "..." },
    "discord_status": "online",
    "activities": [],
    "active_on_discord_desktop": true,
    "active_on_discord_mobile": false,
    "active_on_discord_web": false,
    "listening_to_spotify": false,
    "spotify": null,
    "kv": {}
  }
}
```

Bağlı olmayan kullanıcı → `404 { success:false, error:{ code:"user_not_monitored" } }`.

### GitHub

`GET /v1/github/users/:login?repos=&types=&limit=` →
`{ "success": true, "data": { user, last_active_at, commits[], pull_requests[], issues[], kv } }`

Private repo: varsayılan **gizli**; kullanıcı açarsa maskeli (`repo`/`message` yok,
sadece zaman + tür).

### WebSocket

`ws://localhost:4000/socket` (aynı socket hem Discord hem GitHub aboneliklerini taşır).

| Opcode | Yön | Ad | `d` |
|---|---|---|---|
| `1` | S → C | Hello | `{heartbeat_interval}` |
| `2` | C → S | Initialize | `{subscribe_to_id}` / `{subscribe_to_ids}` / `{subscribe_to_all}`, opsiyonel `filter` |
| `3` | C ↔ S | Heartbeat | `{}` |
| `4` | C → S | Unsubscribe | `{unsubscribe_from_id(s)}` / `{unsubscribe_from_all}` |
| `0` | S → C | Event | `t: INIT_STATE \| PRESENCE_UPDATE \| ACTIVITY_UPDATE` |

Discord id'leri rakam, GitHub login'leri harf içerir. Karışık abonelik desteklenir;
`subscribe_to_all` yalnız Discord presencelerini kapsar.

### Discord KV

Lanyard kuralları: anahtar `[A-Za-z0-9_]{1,255}`, değer string ≤ 30.000 karakter,
kullanıcı başına ≤ 512 çift.

- `PUT /v1/discord/users/:id/kv/:key`
- `PATCH /v1/discord/users/:id/kv`
- `DELETE /v1/discord/users/:id/kv/:key`

`Authorization: Bearer tggl_...` gerekir. Anahtar yalnızca Discord OAuth ile
(`/auth/discord`) alınabilir; hash olarak saklanır.

## Discord bot ayarları

1. <https://discord.com/developers/applications> üzerinden bot oluştur.
2. **Privileged Gateway Intents** → `PRESENCE INTENT` ve `SERVER MEMBERS INTENT` açık.
3. Botu izlenecek sunucuya davet et (presence intent'i olmadan presence gelmez).
4. OAuth2 → Redirects: `http://localhost:4000/auth/discord/callback` (prod'u ekle).
5. Secret'lar: `DISCORD_BOT_TOKEN`, `DISCORD_CLIENT_ID`, `DISCORD_CLIENT_SECRET`,
   opsiyonel `DISCORD_GUILD_IDS`, `DISCORD_INVITE_URL`.

## GitHub App ayarları (local geliştirme)

| Alan | Değer |
|---|---|
| Homepage URL | `http://localhost:4000` |
| Callback URL | `http://localhost:4000/auth/github/callback` |
| Setup URL (opsiyonel) | `http://localhost:4000/setup` + "Redirect on update" |
| Webhook URL | Public tünel gerekir: `https://<tünel>/webhooks/github` → `localhost:4001` |
| Webhook secret | `openssl rand -hex 32` |

**Repository permissions (Read-only):** Metadata, Contents, Pull requests, Issues, Checks, Commit statuses, Deployments.
**Subscribe to events:** Push, Pull request, Pull request review, Pull request review thread, Pull request review comment, Issues, Issue comment, Check run, Deployment status, Installation, Installation repositories.

Portlar: gateway `4000`, ingest `4001`, Infisical `8080`.

## Secret yönetimi (Infisical)

- Proje `taggle`, env `dev`, path `/github-app` (ad tarihsel; Discord secret'larını
  da aynı path'e koyabilirsin): `GITHUB_APP_*`, `GITHUB_WEBHOOK_SECRET`,
  `DISCORD_BOT_TOKEN`, `DISCORD_CLIENT_ID`, `DISCORD_CLIENT_SECRET`.
- Servisler `infisical run --env dev --path /github-app -- <komut>` ile başlar;
  secret diske yazılmaz.
- `infra/infisical/.env` yalnızca Infisical'ın kendi anahtarlarını içerir
  (chmod 600, gitignored). **`INFISICAL_ENCRYPTION_KEY` kaybolursa saklı
  secret'lar okunamaz — yedekle.**

## Güvenlik notları

- Discord bot token'ı yalnızca ingest'e verilir; gateway'e verilmez ve loglanmaz.
- Webhook HMAC sabit zamanlı karşılaştırılır; delivery id ile dedupe yapılır.
- Discord id'leri `^[0-9]{1,20}$`, GitHub login'leri güvenli karakter seti ile
  doğrulanır; Redis key'lerine kullanıcı girdisi ham eklenmez.
- Tüm SQL parametreli; API anahtarları `tggl_` + 32 byte rastgele, yalnız SHA-256
  hash'i saklanır ve `secure_compare` ile doğrulanır.
- OAuth `state` değerleri Redis'te tek kullanımlık (`GETDEL`) ve 10 dk TTL.
- Session cookie: `HttpOnly`, `SameSite=Lax`, HTTPS'te `Secure`.
- KV anahtar/değer boyutları ve çift sayısı sınırlıdır.
- CORS `*` yalnız public okuma içindir; session cookie'leri cross-origin
  kullanılmaz (API anahtarı header'da taşınır).

## Fazlar

1. **Altyapı:** Infisical + Taggle Postgres/Redis compose, migrations, monorepo iskeleti.
2. **Ingest (Rust):** GitHub webhook + HMAC + dedupe; Discord Gateway + presence; normalizer; Postgres/Redis.
3. **Gateway (Elixir):** REST, WebSocket, Redis pub/sub fan-out, read-through cache.
4. **Auth & ayarlar:** GitHub OAuth, Discord OAuth, installation akışı, KV, API key.
5. **Sertleştirme:** rate limit, ETag polling fallback (opsiyonel), metrikler, deploy.
