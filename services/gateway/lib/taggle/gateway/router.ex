defmodule Taggle.Gateway.Router do
  @moduledoc false
  use Plug.Router

  require Logger

  alias Taggle.Gateway.{
    DiscordAuth,
    DiscordStore,
    Filter,
    GitHubAuth,
    GitHubStore,
    Ids,
    Portal
  }

  plug :cors
  plug Plug.Parsers, parsers: [:json], pass: ["*/*"], json_decoder: JSON
  plug :fetch_cookies
  plug :match
  plug :dispatch

  options _ do
    Plug.Conn.send_resp(conn, 204, "")
  end

  get "/health" do
    send_json(conn, 200, %{success: true})
  end

  get "/" do
    gh_login =
      case GitHubAuth.verify_session(conn) do
        {:ok, login} -> login
        _ -> nil
      end

    dc_id =
      case DiscordAuth.verify_session(conn) do
        {:ok, id} -> id
        _ -> nil
      end

    gh_user =
      case gh_login && GitHubStore.user_overview(gh_login) do
        {:ok, user} -> user
        _ -> nil
      end

    dc_user = dc_id && DiscordStore.overview(dc_id)

    conn
    |> Plug.Conn.put_resp_content_type("text/html")
    |> Plug.Conn.send_resp(200, Portal.render(gh_user, dc_user, GitHubAuth.base_url()))
  end

  # ---------------------------------------------------------------------
  # GitHub OAuth
  # ---------------------------------------------------------------------

  get "/auth/github" do
    case GitHubAuth.github_authorize_url() do
      {:ok, url} ->
        redirect(conn, url)

      {:error, :missing_client_id} ->
        send_json(conn, 503, %{
          success: false,
          error: %{code: "oauth_not_configured", message: "GitHub OAuth is not configured."}
        })
    end
  end

  get "/auth/github/callback" do
    conn = Plug.Conn.fetch_query_params(conn)
    code = conn.query_params["code"]
    state = conn.query_params["state"]
    installation_id = conn.query_params["installation_id"]

    case GitHubAuth.exchange_code(code, state, installation_id) do
      {:ok, user} ->
        login = GitHubStore.upsert_user_from_oauth(user)
        maybe_record_installation(installation_id, login)
        trigger_ingest_sync(login, user[:access_token])

        conn
        |> Plug.Conn.put_resp_cookie(
          GitHubAuth.session_cookie_name(),
          GitHubAuth.sign_session(login),
          GitHubAuth.session_cookie_opts()
        )
        |> redirect("/")

      {:error, reason} ->
        log_oauth_error("github", reason)
        oauth_failed(conn, "GitHub")
    end
  end

  get "/setup" do
    conn = Plug.Conn.fetch_query_params(conn)

    with {:ok, login} <- GitHubAuth.verify_session(conn) do
      maybe_record_installation(conn.query_params["installation_id"], login)
    end

    redirect(conn, "/")
  end

  post "/auth/logout" do
    conn
    |> Plug.Conn.delete_resp_cookie(GitHubAuth.session_cookie_name())
    |> Plug.Conn.delete_resp_cookie(DiscordAuth.cookie_name())
    |> redirect("/")
  end

  post "/auth/github/logout" do
    conn
    |> Plug.Conn.delete_resp_cookie(GitHubAuth.session_cookie_name())
    |> redirect("/")
  end

  # ---------------------------------------------------------------------
  # Discord OAuth (only needed to obtain a KV API key)
  # ---------------------------------------------------------------------

  get "/auth/discord" do
    case DiscordAuth.authorize_url() do
      {:ok, url} ->
        redirect(conn, url)

      {:error, :not_configured} ->
        send_json(conn, 503, %{
          success: false,
          error: %{code: "oauth_not_configured", message: "Discord OAuth is not configured."}
        })
    end
  end

  get "/auth/discord/callback" do
    conn = Plug.Conn.fetch_query_params(conn)
    code = conn.query_params["code"]
    state = conn.query_params["state"]

    case DiscordAuth.exchange_code(code, state) do
      {:ok, user} ->
        case DiscordStore.upsert_oauth_user(user) do
          {:ok, user_id} ->
            conn
            |> Plug.Conn.put_resp_cookie(
              DiscordAuth.cookie_name(),
              DiscordAuth.sign_session(user_id),
              DiscordAuth.session_cookie_opts()
            )
            |> redirect("/")

          {:error, _} ->
            oauth_failed(conn, "Discord")
        end

      {:error, reason} ->
        log_oauth_error("discord", reason)
        oauth_failed(conn, "Discord")
    end
  end

  post "/auth/discord/logout" do
    conn
    |> Plug.Conn.delete_resp_cookie(DiscordAuth.cookie_name())
    |> redirect("/")
  end

  # ---------------------------------------------------------------------
  # Authenticated management (session cookie)
  # ---------------------------------------------------------------------

  post "/api/me/sync" do
    with {:ok, login} <- GitHubAuth.verify_session(conn) do
      trigger_ingest_sync(login, nil)
      send_json(conn, 200, %{success: true})
    else
      _ -> unauthorized(conn)
    end
  end

  post "/api/me/api-key" do
    with {:ok, login} <- GitHubAuth.verify_session(conn),
         {:ok, key} <- GitHubStore.rotate_api_key(login) do
      send_json(conn, 200, %{success: true, data: %{api_key: key}})
    else
      _ -> unauthorized(conn)
    end
  end

  put "/api/me/kv" do
    with {:ok, login} <- GitHubAuth.verify_session(conn),
         pairs when is_map(pairs) and not is_struct(pairs) <- conn.body_params,
         {:ok, kv} <- GitHubStore.put_kv(login, pairs) do
      send_json(conn, 200, %{success: true, data: kv})
    else
      {:error, code} -> bad_request(conn, error_code(code))
      %Plug.Conn.Unfetched{} -> bad_request(conn, "invalid_json")
      _ -> unauthorized(conn)
    end
  end

  delete "/api/me/kv/:key" do
    with {:ok, login} <- GitHubAuth.verify_session(conn),
         {:ok, kv} <- GitHubStore.delete_kv(login, key) do
      send_json(conn, 200, %{success: true, data: kv})
    else
      {:error, code} -> bad_request(conn, error_code(code))
      _ -> unauthorized(conn)
    end
  end

  post "/api/discord/me/api-key" do
    with {:ok, user_id} <- DiscordAuth.verify_session(conn),
         {:ok, key} <- DiscordStore.rotate_api_key(user_id) do
      send_json(conn, 200, %{success: true, data: %{api_key: key}})
    else
      _ -> unauthorized(conn)
    end
  end

  put "/api/discord/me/kv" do
    with {:ok, user_id} <- DiscordAuth.verify_session(conn),
         pairs when is_map(pairs) and not is_struct(pairs) <- conn.body_params,
         {:ok, kv} <- DiscordStore.put_kv(user_id, pairs) do
      send_json(conn, 200, %{success: true, data: kv})
    else
      {:error, code} -> bad_request(conn, error_code(code))
      %Plug.Conn.Unfetched{} -> bad_request(conn, "invalid_json")
      _ -> unauthorized(conn)
    end
  end

  delete "/api/discord/me/kv/:key" do
    with {:ok, user_id} <- DiscordAuth.verify_session(conn),
         {:ok, kv} <- DiscordStore.delete_kv(user_id, key) do
      send_json(conn, 200, %{success: true, data: kv})
    else
      {:error, code} -> bad_request(conn, error_code(code))
      _ -> unauthorized(conn)
    end
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
        if Ids.discord_id?(id), do: redirect(conn, discord_avatar_url(id, ext)), else: not_found(conn)

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
    |> WebSockAdapter.upgrade(Taggle.Gateway.Socket, [], timeout: 60_000, compress: false)
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
         pairs when is_map(pairs) and not is_struct(pairs) <- conn.body_params,
         {:ok, kv} <- GitHubStore.put_kv(login, pairs) do
      send_json(conn, 200, %{success: true, data: kv})
    else
      {:error, code} -> bad_request(conn, error_code(code))
      %Plug.Conn.Unfetched{} -> bad_request(conn, "invalid_json")
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
         pairs when is_map(pairs) and not is_struct(pairs) <- conn.body_params,
         {:ok, kv} <- DiscordStore.put_kv(user_id, pairs) do
      send_json(conn, 200, %{success: true, data: kv})
    else
      {:error, code} -> bad_request(conn, error_code(code))
      %Plug.Conn.Unfetched{} -> bad_request(conn, "invalid_json")
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

  defp discord_avatar_url(user_id, ext) do
    avatar =
      case DiscordStore.get_presence(user_id) do
        {:ok, %{"discord_user" => %{"avatar" => avatar}}} when is_binary(avatar) and avatar != "" -> avatar
        _ -> nil
      end

    DiscordAuth.avatar_url(%{id: user_id, avatar: avatar}) |> String.replace_suffix(".png", "." <> ext)
  end

  defp maybe_record_installation(nil, _login), do: :ok

  defp maybe_record_installation(id_str, login) do
    case Integer.parse(to_string(id_str)) do
      {id, ""} when id > 0 -> GitHubStore.record_installation(id, login)
      _ -> :ok
    end
  end

  defp trigger_ingest_sync(login, oauth_token) do
    base =
      System.get_env("INGEST_URL", "http://localhost:4001")
      |> String.trim_trailing("/")

    body =
      if is_binary(oauth_token) and oauth_token != "",
        do: %{login: login, oauth_token: oauth_token},
        else: %{login: login}

    try do
      Req.post(base <> "/internal/sync", json: body, receive_timeout: 8_000, retry: false)
    rescue
      _ -> :ok
    end
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

  defp log_oauth_error(provider, reason) do
    detail =
      case reason do
        {:discord_oauth_status, status} -> "http #{status}"
        {:error, %{status: status}} -> "http #{status}"
        %{status: status} -> "http #{status}"
        atom when is_atom(atom) -> to_string(atom)
        _ -> "unexpected error"
      end

    Logger.warning("#{provider} oauth failed: #{detail}")
  end

  defp oauth_failed(conn, provider) do
    send_json(conn, 400, %{
      success: false,
      error: %{code: "oauth_failed", message: "#{provider} sign-in failed. Please try again."}
    })
  end

  defp unauthorized(conn) do
    send_json(conn, 401, %{
      success: false,
      error: %{code: "unauthorized", message: "Sign in or provide a valid API key."}
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
  end

  defp send_json(conn, status, body) do
    conn
    |> Plug.Conn.put_resp_content_type("application/json")
    |> Plug.Conn.send_resp(status, JSON.encode!(body))
  end
end
