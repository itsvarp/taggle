defmodule Taggle.GatewayTest do
  use ExUnit.Case, async: false
  import Plug.Test
  import Plug.Conn

  alias Taggle.Gateway.{DiscordAuth, DiscordStore, Filter, GitHubStore, Ids, Router}

  @opts Router.init([])

  describe "Ids" do
    test "detects Discord snowflakes and GitHub logins" do
      assert Ids.source("123456789012345678") == :discord
      assert Ids.source("octocat") == :github
      assert Ids.source("Example-User") == :github
      assert Ids.source("bad/../id") == :invalid
      assert Ids.source("-nope") == :invalid
      assert Ids.source("0") == :invalid
      assert Ids.source("0123") == :invalid
      assert Ids.discord_id?("12345678901234567890")
      refute Ids.discord_id?("123456789012345678901")
      refute Ids.discord_id?("99999999999999999999")
    end

    test "builds Discord CDN avatar URLs" do
      assert DiscordAuth.avatar_url(%{id: "123", avatar: "abc"}) ==
               "https://cdn.discordapp.com/avatars/123/abc.png"

      assert DiscordAuth.avatar_url(%{id: "123", avatar: nil}) =~
               "https://cdn.discordapp.com/embed/avatars/"
    end
  end

  describe "Filter.apply/2" do
    test "filters by types, repos and limit" do
      snap = %{
        "commits" => [
          %{"repo" => "a/1", "sha" => "1"},
          %{"repo" => "b/2", "sha" => "2"},
          %{"repo" => "a/1", "sha" => "3"}
        ],
        "pull_requests" => [%{"repo" => "a/1", "number" => 1}],
        "issues" => [%{"repo" => "b/2", "number" => 9}]
      }

      filtered = Filter.apply(snap, %{"types" => "commits", "repos" => "a/1", "limit" => "1"})
      assert length(filtered["commits"]) == 1
      assert hd(filtered["commits"])["sha"] == "1"
      assert filtered["pull_requests"] == []
      assert filtered["issues"] == []

      ws_filtered = Filter.apply(snap, %{"types" => ["issues"], "limit" => 5})
      assert ws_filtered["commits"] == []
      assert length(ws_filtered["issues"]) == 1
    end
  end

  describe "Router & Store integration" do
    setup do
      login = "testuser" <> Integer.to_string(System.unique_integer([:positive]))
      id = System.unique_integer([:positive])
      discord_id = "9" <> Integer.to_string(System.unique_integer([:positive]))

      on_exit(fn ->
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
        Redix.command(:redix, ["DEL", DiscordStore.state_key(discord_id)])
      end)

      %{login: login, id: id, discord_id: discord_id}
    end

    test "health, root, and unknown user return expected JSON" do
      conn = conn(:get, "/health") |> Router.call(@opts)
      assert conn.status == 200
      assert JSON.decode!(conn.resp_body) == %{"success" => true}

      conn = conn(:get, "/") |> Router.call(@opts)
      assert conn.status == 404
      assert JSON.decode!(conn.resp_body)["error"]["code"] == "not_found"

      conn = conn(:get, "/auth/github") |> Router.call(@opts)
      assert conn.status == 404
      assert JSON.decode!(conn.resp_body)["error"]["code"] == "not_found"

      conn = conn(:get, "/v1/users/definitelynobody999") |> Router.call(@opts)
      assert conn.status == 404

      assert %{"success" => false, "error" => %{"code" => "user_not_monitored"}} =
               JSON.decode!(conn.resp_body)

      conn = conn(:get, "/v1/users/bad%2Fid") |> Router.call(@opts)
      assert conn.status == 404
      assert JSON.decode!(conn.resp_body)["error"]["code"] == "not_found"
    end

    test "unlinked GitHub user is not served even if snapshot exists", %{login: login, id: id} do
      GitHubStore.upsert_user_from_oauth(%{id: id, login: login, name: "Test", avatar_url: nil})

      conn = conn(:get, "/v1/github/users/" <> login) |> Router.call(@opts)
      assert conn.status == 404
      assert JSON.decode!(conn.resp_body)["error"]["code"] == "user_not_monitored"
    end

    test "read-through cache restores Redis from Postgres on miss", %{
      login: login,
      id: id,
      discord_id: discord_id
    } do
      GitHubStore.upsert_user_from_oauth(%{id: id, login: login, name: "Test", avatar_url: nil})

      Postgrex.query!(
        :pg,
        "INSERT INTO discord_github_links (discord_user_id, github_id, github_login) VALUES ($1, $2, $3)",
        [discord_id, id, login]
      )

      Redix.command!(:redix, ["DEL", "taggle:state:" <> login])

      conn = conn(:get, "/v1/users/" <> login) |> Router.call(@opts)
      assert conn.status == 200
      body = JSON.decode!(conn.resp_body)
      assert body["success"] == true
      assert body["data"]["user"]["username"] == login

      assert {:ok, raw} = Redix.command(:redix, ["GET", "taggle:state:" <> login])
      assert is_binary(raw)
    end

    test "GitHub KV update requires API key and broadcasts via Redis pubsub", %{
      login: login,
      id: id,
      discord_id: discord_id
    } do
      GitHubStore.upsert_user_from_oauth(%{id: id, login: login, name: "Test", avatar_url: nil})

      Postgrex.query!(
        :pg,
        "INSERT INTO discord_github_links (discord_user_id, github_id, github_login) VALUES ($1, $2, $3)",
        [discord_id, id, login]
      )

      {:ok, api_key} = GitHubStore.rotate_api_key(login)

      unauth =
        conn(:put, "/v1/users/#{login}/kv/status", JSON.encode!(%{value: "coding"}))
        |> put_req_header("content-type", "application/json")
        |> Router.call(@opts)

      assert unauth.status == 401

      Registry.register(Taggle.Gateway.Subs, "gh:" <> login, nil)

      ok =
        conn(:put, "/v1/users/#{login}/kv/status", JSON.encode!(%{value: "coding"}))
        |> put_req_header("content-type", "application/json")
        |> put_req_header("authorization", "Bearer " <> api_key)
        |> Router.call(@opts)

      assert ok.status == 200
      assert JSON.decode!(ok.resp_body) == %{"success" => true, "data" => %{"status" => "coding"}}

      assert_receive {:activity_update, ^login, payload}, 1_000
      assert JSON.decode!(payload)["kv"] == %{"status" => "coding"}

      patched =
        conn(:patch, "/v1/users/#{login}/kv", JSON.encode!(%{theme: "dark"}))
        |> put_req_header("content-type", "application/json")
        |> put_req_header("authorization", api_key)
        |> Router.call(@opts)

      assert patched.status == 200
      assert JSON.decode!(patched.resp_body)["data"] == %{"status" => "coding", "theme" => "dark"}

      deleted =
        conn(:delete, "/v1/users/#{login}/kv/status")
        |> put_req_header("authorization", api_key)
        |> Router.call(@opts)

      assert deleted.status == 200
      assert JSON.decode!(deleted.resp_body)["data"] == %{"theme" => "dark"}
    end

    test "Discord presence is served from Postgres and repopulates Redis", %{
      discord_id: discord_id
    } do
      presence = %{
        "discord_user" => %{
          "id" => discord_id,
          "username" => "tester",
          "discriminator" => "0",
          "avatar" => nil,
          "public_flags" => 0
        },
        "discord_status" => "online",
        "activities" => [],
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

      Redix.command!(:redix, ["DEL", DiscordStore.state_key(discord_id)])

      conn = conn(:get, "/v1/discord/users/" <> discord_id) |> Router.call(@opts)
      assert conn.status == 200
      body = JSON.decode!(conn.resp_body)
      assert body["success"] == true
      assert body["data"]["discord_status"] == "online"
      assert body["data"]["discord_user"]["username"] == "tester"

      # Unified route resolves numeric ids to Discord.
      conn = conn(:get, "/v1/users/" <> discord_id) |> Router.call(@opts)
      assert conn.status == 200

      assert {:ok, raw} = Redix.command(:redix, ["GET", DiscordStore.state_key(discord_id)])
      assert is_binary(raw)
    end

    test "Discord KV requires an API key and broadcasts PRESENCE_UPDATE", %{
      discord_id: discord_id
    } do
      presence = %{
        "discord_user" => %{"id" => discord_id, "username" => "tester"},
        "discord_status" => "online",
        "activities" => [],
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
        [discord_id, "tester"]
      )

      {:ok, api_key} = DiscordStore.rotate_api_key(discord_id)

      unauth =
        conn(
          :put,
          "/v1/discord/users/#{discord_id}/kv/location",
          JSON.encode!(%{value: "somewhere"})
        )
        |> put_req_header("content-type", "application/json")
        |> Router.call(@opts)

      assert unauth.status == 401

      Registry.register(Taggle.Gateway.Subs, "dc:" <> discord_id, nil)

      ok =
        conn(
          :put,
          "/v1/discord/users/#{discord_id}/kv/location",
          JSON.encode!(%{value: "somewhere"})
        )
        |> put_req_header("content-type", "application/json")
        |> put_req_header("authorization", "Bearer " <> api_key)
        |> Router.call(@opts)

      assert ok.status == 200

      assert JSON.decode!(ok.resp_body) == %{
               "success" => true,
               "data" => %{"location" => "somewhere"}
             }

      assert_receive {:discord_presence_update, payload}, 1_000
      assert JSON.decode!(payload)["kv"] == %{"location" => "somewhere"}

      # Lanyard key rules: no dots in Discord KV keys.
      bad_key =
        conn(:put, "/v1/discord/users/#{discord_id}/kv/bad.key", JSON.encode!(%{value: "x"}))
        |> put_req_header("content-type", "application/json")
        |> put_req_header("authorization", api_key)
        |> Router.call(@opts)

      assert bad_key.status == 400
      assert JSON.decode!(bad_key.resp_body)["error"]["code"] == "invalid_key"
    end

    test "avatar quicklink returns 404 when Discord user is not monitored", %{
      discord_id: discord_id
    } do
      conn = conn(:get, "/#{discord_id}.png") |> Router.call(@opts)
      assert conn.status == 404
    end
  end
end
