defmodule Taggle.Gateway.Router do
  @moduledoc false
  use Plug.Router

  alias Taggle.Gateway.{
    ClientJs,
    DiscordAuth,
    DiscordStore,
    Filter,
    GitHubAuth,
    GitHubStore,
    Ids,
    Metrics,
    RateLimit,
    SvgCard
  }

  plug(:metrics_and_headers)
  plug(Plug.Parsers, parsers: [:json], pass: ["*/*"], json_decoder: JSON, length: 2_000_000)
  plug(:match)
  plug(:dispatch)

  options _ do
    Plug.Conn.send_resp(conn, 204, "")
  end

  get "/health" do
    with {:ok, _} <- Postgrex.query(:pg, "SELECT 1", []),
         {:ok, "PONG"} <- Redix.command(:redix, ["PING"]) do
      send_json(conn, 200, %{success: true})
    else
      _ ->
        send_json(conn, 503, %{
          success: false,
          error: %{code: "unhealthy", message: "Dependencies unavailable."}
        })
    end
  end

  get "/metrics" do
    if metrics_authorized?(conn) do
      conn
      |> Plug.Conn.put_resp_content_type("text/plain; version=0.0.4", "utf-8")
      |> Plug.Conn.send_resp(200, Metrics.render())
    else
      unauthorized(conn)
    end
  end

  get "/taggle.js" do
    conn
    |> Plug.Conn.put_resp_content_type("application/javascript", "utf-8")
    |> Plug.Conn.put_resp_header("cache-control", "public, max-age=60")
    |> Plug.Conn.send_resp(200, ClientJs.body())
  end

  # ---------------------------------------------------------------------
  # Public REST API
  # ---------------------------------------------------------------------

  get "/v1/profiles/:id/card.svg" do
    with_rate_limit(conn, "read", read_rpm(), fn c -> send_svg(c, id, :card) end)
  end

  get "/v1/profiles/:id" do
    with_rate_limit(conn, "read", read_rpm(), fn c -> send_profile(c, id) end)
  end

  get "/v1/users/:id/card.svg" do
    with_rate_limit(conn, "read", read_rpm(), fn c -> send_svg(c, id, :card) end)
  end

  get "/v1/users/:id/badge.svg" do
    with_rate_limit(conn, "read", read_rpm(), fn c -> send_svg(c, id, :badge) end)
  end

  get "/v1/users/:id" do
    with_rate_limit(conn, "read", read_rpm(), fn c -> dispatch_user(c, id) end)
  end

  get "/v1/github/users/:username" do
    with_rate_limit(conn, "read", read_rpm(), fn c ->
      send_github_snapshot(c, Ids.normalize_github(username))
    end)
  end

  get "/v1/discord/users/:user_id" do
    with_rate_limit(conn, "read", read_rpm(), fn c -> send_discord_presence(c, user_id) end)
  end

  # KV: explicit routes plus source-detecting compatibility routes.
  patch "/v1/github/users/:username/kv" do
    with_rate_limit(conn, "kv", write_rpm(), fn c ->
      github_patch_kv(c, Ids.normalize_github(username))
    end)
  end

  put "/v1/github/users/:username/kv/:key" do
    with_rate_limit(conn, "kv", write_rpm(), fn c ->
      github_put_kv(c, Ids.normalize_github(username), key)
    end)
  end

  delete "/v1/github/users/:username/kv/:key" do
    with_rate_limit(conn, "kv", write_rpm(), fn c ->
      github_delete_kv(c, Ids.normalize_github(username), key)
    end)
  end

  patch "/v1/discord/users/:user_id/kv" do
    with_rate_limit(conn, "kv", write_rpm(), fn c -> discord_patch_kv(c, user_id) end)
  end

  put "/v1/discord/users/:user_id/kv/:key" do
    with_rate_limit(conn, "kv", write_rpm(), fn c -> discord_put_kv(c, user_id, key) end)
  end

  delete "/v1/discord/users/:user_id/kv/:key" do
    with_rate_limit(conn, "kv", write_rpm(), fn c -> discord_delete_kv(c, user_id, key) end)
  end

  patch "/v1/users/:id/kv" do
    with_rate_limit(conn, "kv", write_rpm(), fn c ->
      case Ids.source(id) do
        :discord -> discord_patch_kv(c, id)
        :github -> github_patch_kv(c, Ids.normalize_github(id))
        :invalid -> not_found(c)
      end
    end)
  end

  put "/v1/users/:id/kv/:key" do
    with_rate_limit(conn, "kv", write_rpm(), fn c ->
      case Ids.source(id) do
        :discord -> discord_put_kv(c, id, key)
        :github -> github_put_kv(c, Ids.normalize_github(id), key)
        :invalid -> not_found(c)
      end
    end)
  end

  delete "/v1/users/:id/kv/:key" do
    with_rate_limit(conn, "kv", write_rpm(), fn c ->
      case Ids.source(id) do
        :discord -> discord_delete_kv(c, id, key)
        :github -> github_delete_kv(c, Ids.normalize_github(id), key)
        :invalid -> not_found(c)
      end
    end)
  end

  get "/socket" do
    with_rate_limit(conn, "ws", read_rpm(), fn c -> upgrade_socket(c) end)
  end

  get "/v1/socket" do
    with_rate_limit(conn, "ws", read_rpm(), fn c -> upgrade_socket(c) end)
  end

  get "/v1/github/socket" do
    with_rate_limit(conn, "ws", read_rpm(), fn c -> upgrade_socket(c) end)
  end

  get "/v1/discord/socket" do
    with_rate_limit(conn, "ws", read_rpm(), fn c -> upgrade_socket(c) end)
  end

  # Lanyard quicklink: https://.../<discord_id>.png. Keep this after the static
  # routes so it cannot shadow /socket or /health.
  get "/:file" do
    with_rate_limit(conn, "read", read_rpm(), fn c ->
      case String.split(file, ".", parts: 2) do
        [id, ext] when ext in ~w(png gif webp jpg jpeg) ->
          with true <- Ids.discord_id?(id),
               {:ok, %{} = presence} <- DiscordStore.get_presence(id) do
            redirect(c, discord_avatar_url(presence, id, ext))
          else
            _ -> not_found(c)
          end

        _ ->
          not_found(c)
      end
    end)
  end

  match _ do
    not_found(conn)
  end

  # ---------------------------------------------------------------------
  # Helpers
  # ---------------------------------------------------------------------

  defp upgrade_socket(conn) do
    conn
    |> WebSockAdapter.upgrade(Taggle.Gateway.Socket, [],
      timeout: 60_000,
      compress: false,
      max_frame_size: 1_048_576
    )
    |> halt()
  end

  defp dispatch_user(conn, id) do
    case Ids.source(id) do
      :discord -> send_discord_presence(conn, id)
      :github -> send_github_snapshot(conn, Ids.normalize_github(id))
      :invalid -> not_found(conn)
    end
  end

  defp send_profile(conn, id) do
    conn = Plug.Conn.fetch_query_params(conn)

    case resolve_profile(id, conn.query_params) do
      {:ok, profile} ->
        send_json(conn, 200, %{success: true, data: profile})

      :not_found ->
        user_not_monitored(conn)

      :invalid ->
        not_found(conn)

      :unavailable ->
        unavailable(conn)
    end
  end

  defp send_svg(conn, id, kind) do
    conn = Plug.Conn.fetch_query_params(conn)

    case resolve_profile(id, %{}) do
      {:ok, profile} ->
        svg =
          case kind do
            :card -> SvgCard.render_card(profile, conn.query_params)
            :badge -> SvgCard.render_badge(profile, conn.query_params)
          end

        conn
        |> Plug.Conn.put_resp_content_type("image/svg+xml", "utf-8")
        |> Plug.Conn.put_resp_header(
          "content-security-policy",
          "default-src 'none'; img-src data:; style-src 'unsafe-inline'"
        )
        |> Plug.Conn.put_resp_header("cache-control", "public, max-age=10, s-maxage=10")
        |> Plug.Conn.send_resp(200, svg)

      :not_found ->
        user_not_monitored(conn)

      :invalid ->
        not_found(conn)

      :unavailable ->
        unavailable(conn)
    end
  end

  defp resolve_profile(id, query_params) do
    case Ids.source(id) do
      :discord ->
        with {:ok, discord} <- DiscordStore.get_presence(id),
             {:ok, gh_login} <- DiscordStore.linked_github_login(id) do
          github =
            case gh_login do
              login when is_binary(login) ->
                case GitHubStore.get_snapshot(login) do
                  {:ok, snap} when is_map(snap) -> Filter.apply(snap, query_params)
                  _ -> nil
                end

              _ ->
                nil
            end

          if is_nil(discord) and is_nil(github) do
            :not_found
          else
            {:ok, %{discord: discord, github: github}}
          end
        else
          {:error, :invalid_id} -> :invalid
          _ -> :unavailable
        end

      :github ->
        login = Ids.normalize_github(id)

        with {:ok, github} <- GitHubStore.get_snapshot(login),
             {:ok, dc_id} <- GitHubStore.linked_discord_id(login) do
          discord =
            case dc_id do
              uid when is_binary(uid) ->
                case DiscordStore.get_presence(uid) do
                  {:ok, pres} when is_map(pres) -> pres
                  _ -> nil
                end

              _ ->
                nil
            end

          filtered_gh = if is_map(github), do: Filter.apply(github, query_params), else: nil

          if is_nil(discord) and is_nil(filtered_gh) do
            :not_found
          else
            {:ok, %{discord: discord, github: filtered_gh}}
          end
        else
          {:error, :invalid_id} -> :invalid
          _ -> :unavailable
        end

      :invalid ->
        :invalid
    end
  end

  defp send_github_snapshot(conn, login) do
    conn = Plug.Conn.fetch_query_params(conn)

    if has_filter_params?(conn.query_params) do
      case GitHubStore.get_snapshot(login) do
        {:ok, nil} ->
          user_not_monitored(conn)

        {:ok, data} ->
          send_json(conn, 200, %{success: true, data: Filter.apply(data, conn.query_params)})

        {:error, _} ->
          unavailable(conn)
      end
    else
      case GitHubStore.get_snapshot_json(login) do
        {:ok, nil} -> user_not_monitored(conn)
        {:ok, raw} when is_binary(raw) -> send_raw_data_json(conn, raw)
        {:error, _} -> unavailable(conn)
      end
    end
  end

  defp send_discord_presence(conn, user_id) do
    case DiscordStore.get_presence_json(user_id) do
      {:ok, nil} -> user_not_monitored(conn)
      {:ok, raw} when is_binary(raw) -> send_raw_data_json(conn, raw)
      {:error, :invalid_id} -> not_found(conn)
      {:error, _} -> unavailable(conn)
    end
  end

  defp user_not_monitored(conn) do
    send_json(conn, 404, %{
      success: false,
      error: %{code: "user_not_monitored", message: "User is not monitored by Taggle."}
    })
  end

  defp has_filter_params?(params) do
    Map.has_key?(params, "types") or Map.has_key?(params, "repos") or
      Map.has_key?(params, "limit")
  end

  defp send_raw_data_json(conn, raw_json) do
    conn
    |> Plug.Conn.put_resp_content_type("application/json")
    |> Plug.Conn.send_resp(200, "{\"success\":true,\"data\":" <> raw_json <> "}")
  end

  defp github_patch_kv(conn, login) do
    with token when is_binary(token) <- GitHubAuth.extract_api_key(conn),
         true <- GitHubStore.valid_api_key?(login, token),
         pairs when is_map(pairs) and not is_struct(pairs) and not is_map_key(pairs, "_json") <-
           conn.body_params,
         {:ok, kv} <- GitHubStore.put_kv(login, pairs) do
      send_json(conn, 200, %{success: true, data: kv})
    else
      {:error, code} -> bad_request(conn, error_code(code))
      %Plug.Conn.Unfetched{} -> bad_request(conn, "invalid_json")
      %{"_json" => _} -> bad_request(conn, "invalid_json")
      _ -> unauthorized(conn)
    end
  end

  defp github_put_kv(conn, login, key) do
    with token when is_binary(token) <- GitHubAuth.extract_api_key(conn),
         true <- GitHubStore.valid_api_key?(login, token),
         {:ok, value} <- extract_kv_value(conn),
         {:ok, kv} <- GitHubStore.put_kv(login, %{key => value}) do
      send_json(conn, 200, %{success: true, data: kv})
    else
      {:error, code} -> bad_request(conn, error_code(code))
      _ -> unauthorized(conn)
    end
  end

  defp github_delete_kv(conn, login, key) do
    with token when is_binary(token) <- GitHubAuth.extract_api_key(conn),
         true <- GitHubStore.valid_api_key?(login, token),
         {:ok, kv} <- GitHubStore.delete_kv(login, key) do
      send_json(conn, 200, %{success: true, data: kv})
    else
      {:error, code} -> bad_request(conn, error_code(code))
      _ -> unauthorized(conn)
    end
  end

  defp discord_patch_kv(conn, user_id) do
    with token when is_binary(token) <- GitHubAuth.extract_api_key(conn),
         true <- DiscordStore.valid_api_key?(user_id, token),
         pairs when is_map(pairs) and not is_struct(pairs) and not is_map_key(pairs, "_json") <-
           conn.body_params,
         {:ok, kv} <- DiscordStore.put_kv(user_id, pairs) do
      send_json(conn, 200, %{success: true, data: kv})
    else
      {:error, code} -> bad_request(conn, error_code(code))
      %Plug.Conn.Unfetched{} -> bad_request(conn, "invalid_json")
      %{"_json" => _} -> bad_request(conn, "invalid_json")
      _ -> unauthorized(conn)
    end
  end

  defp discord_put_kv(conn, user_id, key) do
    with token when is_binary(token) <- GitHubAuth.extract_api_key(conn),
         true <- DiscordStore.valid_api_key?(user_id, token),
         {:ok, value} <- extract_kv_value(conn),
         {:ok, kv} <- DiscordStore.put_kv(user_id, %{key => value}) do
      send_json(conn, 200, %{success: true, data: kv})
    else
      {:error, code} -> bad_request(conn, error_code(code))
      _ -> unauthorized(conn)
    end
  end

  defp discord_delete_kv(conn, user_id, key) do
    with token when is_binary(token) <- GitHubAuth.extract_api_key(conn),
         true <- DiscordStore.valid_api_key?(user_id, token),
         {:ok, kv} <- DiscordStore.delete_kv(user_id, key) do
      send_json(conn, 200, %{success: true, data: kv})
    else
      {:error, code} -> bad_request(conn, error_code(code))
      _ -> unauthorized(conn)
    end
  end

  defp discord_avatar_url(presence, user_id, ext) do
    avatar =
      case presence do
        %{"discord_user" => %{"avatar" => avatar}} when is_binary(avatar) and avatar != "" ->
          avatar

        _ ->
          nil
      end

    DiscordAuth.avatar_url(%{id: user_id, avatar: avatar})
    |> String.replace_suffix(".png", "." <> ext)
  end

  defp extract_kv_value(%{body_params: %{"value" => v}})
       when is_binary(v) do
    if byte_size(v) <= 30_000, do: {:ok, v}, else: {:error, "value_too_large"}
  end

  defp extract_kv_value(%{body_params: %{"value" => v}})
       when is_number(v) or is_boolean(v),
       do: {:ok, v}

  defp extract_kv_value(%{body_params: %{"_json" => v}})
       when is_binary(v) do
    if byte_size(v) <= 30_000, do: {:ok, v}, else: {:error, "value_too_large"}
  end

  defp extract_kv_value(%{body_params: %{"_json" => v}})
       when is_number(v) or is_boolean(v),
       do: {:ok, v}

  defp extract_kv_value(conn) do
    case Plug.Conn.read_body(conn, length: 30_001) do
      {:ok, raw, _} when byte_size(raw) > 0 and byte_size(raw) <= 30_000 -> {:ok, raw}
      {:ok, raw, _} when byte_size(raw) > 30_000 -> {:error, "value_too_large"}
      {:more, _, _} -> {:error, "value_too_large"}
      _ -> {:error, "missing_value"}
    end
  end

  defp read_rpm, do: RateLimit.limit_rpm("RATE_LIMIT_READ_RPM", 120)
  defp write_rpm, do: RateLimit.limit_rpm("RATE_LIMIT_KV_RPM", 30)

  defp with_rate_limit(conn, bucket, limit, fun) do
    case RateLimit.check(conn, bucket, limit) do
      {:ok, limited_conn} ->
        fun.(limited_conn)

      {:error, :rate_limited, limited_conn} ->
        send_json(limited_conn, 429, %{
          success: false,
          error: %{code: "rate_limited", message: "Too many requests."}
        })
    end
  end

  defp metrics_authorized?(conn) do
    case System.get_env("METRICS_TOKEN") do
      token when is_binary(token) and byte_size(token) > 0 ->
        case Plug.Conn.get_req_header(conn, "authorization") do
          ["Bearer " <> provided] ->
            byte_size(provided) == byte_size(token) and
              Plug.Crypto.secure_compare(provided, token)

          _ ->
            false
        end

      _ ->
        true
    end
  end

  defp unauthorized(conn) do
    send_json(conn, 401, %{
      success: false,
      error: %{code: "unauthorized", message: "Provide a valid API key."}
    })
  end

  defp error_code(code) when is_binary(code), do: code
  defp error_code(code) when is_atom(code), do: Atom.to_string(code)
  defp error_code(_), do: "error"

  defp bad_request(conn, code) do
    send_json(conn, 400, %{success: false, error: %{code: error_code(code)}})
  end

  defp not_found(conn) do
    send_json(conn, 404, %{success: false, error: %{code: "not_found", message: "Not found."}})
  end

  defp unavailable(conn) do
    send_json(conn, 503, %{
      success: false,
      error: %{code: "unavailable", message: "State store unavailable."}
    })
  end

  defp redirect(conn, location) do
    conn
    |> Plug.Conn.put_resp_header("location", location)
    |> Plug.Conn.send_resp(302, "")
  end

  defp metrics_and_headers(conn, _opts) do
    Metrics.inc(:http_requests_total)

    conn
    |> Plug.Conn.put_resp_header("access-control-allow-origin", "*")
    |> Plug.Conn.put_resp_header(
      "access-control-allow-methods",
      "GET, PUT, PATCH, DELETE, OPTIONS"
    )
    |> Plug.Conn.put_resp_header("access-control-allow-headers", "authorization, content-type")
    |> Plug.Conn.put_resp_header("access-control-max-age", "86400")
    |> Plug.Conn.put_resp_header("x-content-type-options", "nosniff")
    |> Plug.Conn.put_resp_header("x-frame-options", "DENY")
    |> Plug.Conn.put_resp_header("referrer-policy", "no-referrer")
    |> Plug.Conn.put_resp_header("cache-control", "no-store")
    |> Plug.Conn.register_before_send(fn resp_conn ->
      cond do
        resp_conn.status >= 500 -> Metrics.inc(:http_errors_5xx_total)
        resp_conn.status >= 400 -> Metrics.inc(:http_errors_4xx_total)
        true -> :ok
      end

      resp_conn
    end)
  end

  defp send_json(conn, status, body) do
    conn
    |> Plug.Conn.put_resp_content_type("application/json")
    |> Plug.Conn.send_resp(status, JSON.encode!(body))
  end
end
