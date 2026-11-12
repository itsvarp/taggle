defmodule Taggle.FeaturesTest do
  use ExUnit.Case, async: false
  import Plug.Test
  import Plug.Conn

  alias Taggle.Gateway.{
    AvatarCache,
    DiscordAuth,
    DiscordStore,
    GitHubAuth,
    GitHubStore,
    Router,
    Socket
  }

  @opts Router.init([])

  setup do
    login = "feat" <> Integer.to_string(System.unique_integer([:positive]))
    id = System.unique_integer([:positive])
    discord_id = "8" <> Integer.to_string(System.unique_integer([:positive]))

    on_exit(fn ->
      System.delete_env("METRICS_TOKEN")
      System.delete_env("RATE_LIMIT_READ_RPM")
      System.delete_env("TRUST_PROXY")

      Postgrex.query!(
        :pg,
        "DELETE FROM discord_github_links WHERE discord_user_id = $1 OR github_login = $2",
        [discord_id, login]
      )

      Postgrex.query!(:pg, "DELETE FROM snapshots WHERE login = $1", [login])
      Postgrex.query!(:pg, "DELETE FROM user_kv WHERE login = $1", [login])
      Postgrex.query!(:pg, "DELETE FROM installations WHERE account_login = $1", [login])
      Postgrex.query!(:pg, "DELETE FROM users WHERE login = $1", [login])
      Postgrex.query!(:pg, "DELETE FROM discord_presences WHERE user_id = $1", [discord_id])
      Postgrex.query!(:pg, "DELETE FROM discord_users WHERE id = $1", [discord_id])
      Postgrex.query!(:pg, "DELETE FROM discord_kv WHERE user_id = $1", [discord_id])
      Redix.command(:redix, ["DEL", "taggle:state:" <> login])
      Redix.command(:redix, ["DEL", DiscordStore.presence_key(discord_id)])
      Redix.command(:redix, ["DEL", DiscordStore.state_key(discord_id)])
      Redix.command(:redix, ["DEL", DiscordStore.kv_key(discord_id)])
    end)

    %{login: login, id: id, discord_id: discord_id}
  end

  defp seed_linked_profile!(%{login: login, id: id, discord_id: discord_id}, extra \\ %{}) do
    GitHubStore.upsert_user_from_oauth(%{
      id: id,
      login: login,
      name: "Feature User",
      avatar_url: nil
    })

    Postgrex.query!(
      :pg,
      "INSERT INTO discord_github_links (discord_user_id, github_id, github_login) VALUES ($1, $2, $3)",
      [discord_id, id, login]
    )

    presence =
      %{
        "discord_user" => %{
          "id" => discord_id,
          "username" => "feat_user",
          "global_name" => extra["global_name"] || "Feat User",
          "discriminator" => "0",
          "avatar" => nil,
          "public_flags" => 0
        },
        "discord_status" => "online",
        "activities" => extra["activities"] || [],
        "coding" =>
          extra["coding"] || %{"editor" => "Neovim", "file" => "router.ex", "workspace" => "oen"},
        "active_on_discord_desktop" => true,
        "active_on_discord_mobile" => false,
        "active_on_discord_web" => false,
        "listening_to_spotify" => false,
        "spotify" => nil,
        "kv" => %{}
      }

    Postgrex.query!(
      :pg,
      "INSERT INTO discord_presences (user_id, data) VALUES ($1, $2)",
      [discord_id, presence]
    )

    Postgrex.query!(
      :pg,
      "INSERT INTO discord_users (id, username) VALUES ($1, $2)",
      [discord_id, "feat_user"]
    )

    Redix.command!(:redix, [
      "SET",
      DiscordStore.presence_key(discord_id),
      JSON.encode!(Map.delete(presence, "kv"))
    ])

    Redix.command!(:redix, ["SET", DiscordStore.state_key(discord_id), JSON.encode!(presence)])
    presence
  end

  test "unified /v1/profiles/:id resolves via Discord ID and GitHub login", ctx do
    seed_linked_profile!(ctx)

    dc_conn = conn(:get, "/v1/profiles/" <> ctx.discord_id) |> Router.call(@opts)
    assert dc_conn.status == 200
    dc_body = JSON.decode!(dc_conn.resp_body)
    assert dc_body["success"] == true
    assert dc_body["data"]["discord"]["discord_user"]["id"] == ctx.discord_id
    assert dc_body["data"]["github"]["user"]["username"] == ctx.login

    gh_conn = conn(:get, "/v1/profiles/" <> ctx.login) |> Router.call(@opts)
    assert gh_conn.status == 200
    gh_body = JSON.decode!(gh_conn.resp_body)
    assert gh_body["data"]["discord"]["discord_status"] == "online"
    assert gh_body["data"]["github"]["user"]["username"] == ctx.login

    missing = conn(:get, "/v1/profiles/nonexistentuser999") |> Router.call(@opts)
    assert missing.status == 404
  end

  test "SVG card & badge embed safe cached avatar, are CSP-protected, and XML-escape XSS payloads",
       ctx do
    seed_linked_profile!(ctx, %{
      "global_name" => "<script>alert('xss')</script> & User",
      "coding" => %{"editor" => "VSCode", "file" => "<img onerror=alert(1)>"}
    })

    # Tiny 1x1 transparent PNG data URI seeded in AvatarCache (outbound HTTP disabled in test env).
    png_uri =
      "data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAQAAAC1HAwCAAAAC0lEQVR42mNkYAAAAAYAAjCB0C8AAAAASUVORK5CYII="

    assert :ok == AvatarCache.put(AvatarCache.cache_key_for_github(ctx.login), png_uri)
    assert {:error, :invalid_data_uri} == AvatarCache.put("bad", "https://evil.example/a.png")

    card =
      conn(
        :get,
        "/v1/users/#{ctx.discord_id}/card.svg?theme=catppuccin&accent=javascript:alert(1)"
      )
      |> Router.call(@opts)

    assert card.status == 200
    assert get_resp_header(card, "content-type") == ["image/svg+xml; charset=utf-8"]

    assert get_resp_header(card, "content-security-policy") == [
             "default-src 'none'; img-src data:; style-src 'unsafe-inline'"
           ]

    refute String.contains?(card.resp_body, "<script")
    refute String.contains?(card.resp_body, "javascript:")
    refute String.contains?(card.resp_body, "<img")
    assert String.contains?(card.resp_body, "<image href=\"#{png_uri}\"")
    refute String.contains?(card.resp_body, "<image href=\"http")
    assert String.contains?(card.resp_body, "&lt;script&gt;")

    profile_card =
      conn(:get, "/v1/profiles/#{ctx.login}/card.svg?compact=true&avatar=false")
      |> Router.call(@opts)

    assert profile_card.status == 200
    assert String.contains?(profile_card.resp_body, ~s(height="96"))
    refute String.contains?(profile_card.resp_body, "<image")

    badge =
      conn(:get, "/v1/users/#{ctx.discord_id}/badge.svg?accent=00ff88") |> Router.call(@opts)

    assert badge.status == 200
    assert String.contains?(badge.resp_body, "#00ff88")
    assert String.contains?(badge.resp_body, "<image href=\"#{png_uri}\"")
  end

  test "self-hosted /taggle.js serves first-party Web Component SDK" do
    js = conn(:get, "/taggle.js") |> Router.call(@opts)
    assert js.status == 200
    assert get_resp_header(js, "content-type") == ["application/javascript; charset=utf-8"]
    assert String.contains?(js.resp_body, "export class TaggleClient")
    assert String.contains?(js.resp_body, "taggle-card")
  end

  test "/metrics exposes Prometheus counters and respects METRICS_TOKEN" do
    open_conn = conn(:get, "/metrics") |> Router.call(@opts)
    assert open_conn.status == 200
    assert String.contains?(open_conn.resp_body, "taggle_gateway_http_requests_total")

    System.put_env("METRICS_TOKEN", "secret-prom-token")

    unauth = conn(:get, "/metrics") |> Router.call(@opts)
    assert unauth.status == 401

    wrong =
      conn(:get, "/metrics")
      |> put_req_header("authorization", "Bearer wrong")
      |> Router.call(@opts)

    assert wrong.status == 401

    authed =
      conn(:get, "/metrics")
      |> put_req_header("authorization", "Bearer secret-prom-token")
      |> Router.call(@opts)

    assert authed.status == 200
    assert String.contains?(authed.resp_body, "taggle_gateway_ws_connections_active")
  end

  test "HTTP rate limiter enforces limit and sets retry-after" do
    System.put_env("RATE_LIMIT_READ_RPM", "2")
    System.put_env("TRUST_PROXY", "true")
    ip = "198.51.100." <> Integer.to_string(rem(System.unique_integer([:positive]), 200) + 10)

    r1 =
      conn(:get, "/v1/users/nobody_rl")
      |> put_req_header("x-forwarded-for", ip)
      |> Router.call(@opts)

    assert r1.status == 404
    assert get_resp_header(r1, "x-ratelimit-limit") == ["2"]
    assert get_resp_header(r1, "x-ratelimit-remaining") == ["1"]

    r2 =
      conn(:get, "/v1/users/nobody_rl")
      |> put_req_header("x-forwarded-for", ip)
      |> Router.call(@opts)

    assert r2.status == 404
    assert get_resp_header(r2, "x-ratelimit-remaining") == ["0"]

    r3 =
      conn(:get, "/v1/users/nobody_rl")
      |> put_req_header("x-forwarded-for", ip)
      |> Router.call(@opts)

    assert r3.status == 429
    assert JSON.decode!(r3.resp_body)["error"]["code"] == "rate_limited"
    assert get_resp_header(r3, "retry-after") != []
  end

  test "Auth token pre-check, avatar validation, 30KB KV limit, and Lua empty-array preservation",
       ctx do
    seed_linked_profile!(ctx)
    {:ok, api_key} = DiscordStore.rotate_api_key(ctx.discord_id)

    assert String.starts_with?(api_key, "tggl_")
    assert GitHubAuth.valid_token_shape?(api_key)
    refute GitHubAuth.valid_token_shape?("bad_prefix_1234567890")
    refute DiscordStore.valid_api_key?(ctx.discord_id, "not_a_taggle_token")

    assert DiscordAuth.avatar_url(%{id: "123", avatar: "../../evil"}) =~
             "https://cdn.discordapp.com/embed/avatars/"

    # Empty `activities: []` must stay an array `[]` after Redis Lua KV merge (never `{}`).
    assert {:ok, %{"theme" => "midnight"}} =
             DiscordStore.put_kv(ctx.discord_id, %{"theme" => "midnight"})

    {:ok, raw} = Redix.command(:redix, ["GET", DiscordStore.state_key(ctx.discord_id)])
    decoded = JSON.decode!(raw)
    assert decoded["activities"] == []
    assert decoded["coding"]["editor"] == "Neovim"
    assert decoded["kv"] == %{"theme" => "midnight"}

    oversized = String.duplicate("x", 30_001)

    too_big =
      conn(:put, "/v1/discord/users/#{ctx.discord_id}/kv/bio", JSON.encode!(%{value: oversized}))
      |> put_req_header("content-type", "application/json")
      |> put_req_header("authorization", "Bearer " <> api_key)
      |> Router.call(@opts)

    assert too_big.status == 400
    assert JSON.decode!(too_big.resp_body)["error"]["code"] == "value_too_large"
  end

  test "WebSocket Socket handles batch INIT_STATE, safe decode, and inbound rate limiting", ctx do
    seed_linked_profile!(ctx)

    {:push, {:text, hello}, state} = Socket.init([])
    assert %{"op" => 1, "d" => %{"heartbeat_interval" => _}} = JSON.decode!(hello)

    init_msg =
      JSON.encode!(%{op: 2, d: %{subscribe_to_ids: [ctx.discord_id, ctx.login, ctx.login]}})

    {:push, {:text, init_frame}, state} = Socket.handle_in({init_msg, [opcode: :text]}, state)
    init_decoded = JSON.decode!(init_frame)
    assert init_decoded["t"] == "INIT_STATE"
    assert init_decoded["d"][ctx.discord_id]["discord_status"] == "online"
    assert init_decoded["d"][ctx.login]["user"]["username"] == ctx.login

    # Filtered ACTIVITY_UPDATE with corrupt JSON payload does not crash Socket.
    filtered_state = %{state | filter: %{"types" => ["commits"]}}

    assert {:ok, ^filtered_state} =
             Socket.handle_info({:activity_update, ctx.login, "{bad-json"}, filtered_state)

    # Heartbeat ack works cleanly.
    hb_msg = JSON.encode!(%{op: 3})
    {:push, {:text, ack}, state} = Socket.handle_in({hb_msg, [opcode: :text]}, state)
    assert JSON.decode!(ack) == %{"op" => 3}

    # Unsubscribe removes subjects.
    unsub_msg = JSON.encode!(%{op: 4, d: %{unsubscribe_from_id: ctx.login}})
    {:ok, state} = Socket.handle_in({unsub_msg, [opcode: :text]}, state)
    refute MapSet.member?(state.subs, "gh:" <> ctx.login)

    # Inbound soft & hard rate limit protection.
    hot_state = %{state | msg_count: 40, msg_window_start: System.monotonic_time(:millisecond)}
    {:push, {:text, err_frame}, _} = Socket.handle_in({hb_msg, [opcode: :text]}, hot_state)

    assert JSON.decode!(err_frame) == %{
             "op" => 0,
             "t" => "ERROR",
             "d" => %{"code" => "rate_limited"}
           }

    abuse_state = %{state | msg_count: 100, msg_window_start: System.monotonic_time(:millisecond)}

    assert {:stop, :normal, {4008, "rate limit exceeded"}, _} =
             Socket.handle_in({hb_msg, [opcode: :text]}, abuse_state)
  end
end
