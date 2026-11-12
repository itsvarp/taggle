defmodule Taggle.Gateway.Router do
  @moduledoc false
  use Plug.Router

  alias Taggle.Gateway.{
    DiscordAuth,
    DiscordStore,
    Filter,
    GitHubAuth,
    GitHubStore,
    Ids
  }

  plug :cors
  plug Plug.Parsers, parsers: [:json], pass: ["*/*"], json_decoder: JSON, length: 2_000_000
  plug :match
  plug :dispatch

  options _ do
    Plug.Conn.send_resp(conn, 204, "")
  end

  get "/health" do
    send_json(conn, 200, %{success: true})
  end

  # ---------------------------------------------------------------------
  # Public REST API
  # ---------------------------------------------------------------------

  get "/v1/users/:id" do
    dispatch_user(conn, id)
  end

  get "/v1/github/users/:username" do
    send_github_snapshot(conn, Ids.normalize_github(username))
  end

  get "/v1/discord/users/:user_id" do
    send_discord_presence(conn, user_id)
  end

  # KV: explicit routes plus source-detecting compatibility routes.
  patch "/v1/github/users/:username/kv" do
    github_patch_kv(conn, Ids.normalize_github(username))
  end

  put "/v1/github/users/:username/kv/:key" do
    github_put_kv(conn, Ids.normalize_github(username), key)
  end

  delete "/v1/github/users/:username/kv/:key" do
    github_delete_kv(conn, Ids.normalize_github(username), key)
  end

  patch "/v1/discord/users/:user_id/kv" do
    discord_patch_kv(conn, user_id)
  end

  put "/v1/discord/users/:user_id/kv/:key" do
    discord_put_kv(conn, user_id, key)
  end

  delete "/v1/discord/users/:user_id/kv/:key" do
    discord_delete_kv(conn, user_id, key)
  end

  patch "/v1/users/:id/kv" do
    case Ids.source(id) do
      :discord -> discord_patch_kv(conn, id)
      :github -> github_patch_kv(conn, Ids.normalize_github(id))
      :invalid -> not_found(conn)
    end
  end

  put "/v1/users/:id/kv/:key" do
    case Ids.source(id) do
      :discord -> discord_put_kv(conn, id, key)
      :github -> github_put_kv(conn, Ids.normalize_github(id), key)
      :invalid -> not_found(conn)
    end
  end

  delete "/v1/users/:id/kv/:key" do
    case Ids.source(id) do
      :discord -> discord_delete_kv(conn, id, key)
      :github -> github_delete_kv(conn, Ids.normalize_github(id), key)
      :invalid -> not_found(conn)
    end
  end

  get "/socket" do
    upgrade_socket(conn)
  end

  get "/v1/socket" do
    upgrade_socket(conn)
  end

  get "/v1/github/socket" do
    upgrade_socket(conn)
  end

  get "/v1/discord/socket" do
    upgrade_socket(conn)
  end

  # Lanyard quicklink: https://.../<discord_id>.png. Keep this after the static
  # routes so it cannot shadow /socket or /health.
  get "/:file" do
    case String.split(file, ".", parts: 2) do
      [id, ext] when ext in ~w(png gif webp jpg jpeg) ->
        with true <- Ids.discord_id?(id),
             {:ok, %{} = presence} <- DiscordStore.get_presence(id) do
          redirect(conn, discord_avatar_url(presence, id, ext))
        else
          _ -> not_found(conn)
        end

      _ ->
        not_found(conn)
    end
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
      max_frame_size: 65_536
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

  defp send_github_snapshot(conn, login) do
    case GitHubStore.get_snapshot(login) do
      {:ok, nil} ->
        send_json(conn, 404, %{
          success: false,
          error: %{code: "user_not_monitored", message: "User is not monitored by Taggle."}
        })

      {:ok, data} ->
        conn = Plug.Conn.fetch_query_params(conn)
        send_json(conn, 200, %{success: true, data: Filter.apply(data, conn.query_params)})

      {:error, _} ->
        unavailable(conn)
    end
  end

  defp send_discord_presence(conn, user_id) do
    case DiscordStore.get_presence(user_id) do
      {:ok, nil} ->
        send_json(conn, 404, %{
          success: false,
          error: %{code: "user_not_monitored", message: "User is not monitored by Taggle."}
        })

      {:ok, data} ->
        send_json(conn, 200, %{success: true, data: data})

      {:error, :invalid_id} ->
        not_found(conn)

      {:error, _} ->
        unavailable(conn)
    end
  end

  defp github_patch_kv(conn, login) do
    with token when is_binary(token) <- GitHubAuth.extract_api_key(conn),
         true <- GitHubStore.valid_api_key?(login, token),
         pairs when is_map(pairs) and not is_struct(pairs) and not is_map_key(pairs, "_json") <- conn.body_params,
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
         pairs when is_map(pairs) and not is_struct(pairs) and not is_map_key(pairs, "_json") <- conn.body_params,
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
        %{"discord_user" => %{"avatar" => avatar}} when is_binary(avatar) and avatar != "" -> avatar
        _ -> nil
      end

    DiscordAuth.avatar_url(%{id: user_id, avatar: avatar}) |> String.replace_suffix(".png", "." <> ext)
  end

  defp extract_kv_value(%{body_params: %{"value" => v}})
       when is_binary(v) or is_number(v) or is_boolean(v),
       do: {:ok, v}

  defp extract_kv_value(%{body_params: %{"_json" => v}})
       when is_binary(v) or is_number(v) or is_boolean(v),
       do: {:ok, v}

  defp extract_kv_value(conn) do
    case Plug.Conn.read_body(conn, length: 64_000) do
      {:ok, raw, _} when byte_size(raw) > 0 -> {:ok, raw}
      _ -> {:error, "missing_value"}
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
    send_json(conn, 503, %{success: false, error: %{code: "unavailable", message: "State store unavailable."}})
  end

  defp redirect(conn, location) do
    conn
    |> Plug.Conn.put_resp_header("location", location)
    |> Plug.Conn.send_resp(302, "")
  end

  defp cors(conn, _opts) do
    conn
    |> Plug.Conn.put_resp_header("access-control-allow-origin", "*")
    |> Plug.Conn.put_resp_header("access-control-allow-methods", "GET, PUT, PATCH, DELETE, OPTIONS")
    |> Plug.Conn.put_resp_header("access-control-allow-headers", "authorization, content-type")
    |> Plug.Conn.put_resp_header("x-content-type-options", "nosniff")
    |> Plug.Conn.put_resp_header("cache-control", "no-store")
  end

  defp send_json(conn, status, body) do
    conn
    |> Plug.Conn.put_resp_content_type("application/json")
    |> Plug.Conn.send_resp(status, JSON.encode!(body))
  end
end
