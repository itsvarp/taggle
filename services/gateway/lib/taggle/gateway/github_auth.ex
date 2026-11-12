defmodule Taggle.Gateway.GitHubAuth do
  @moduledoc """
  GitHub OAuth flow, signed session cookies and API key extraction.
  """

  @session_cookie "taggle_gh_session"
  @session_salt "taggle-github-session-v1"
  @session_max_age 60 * 60 * 24 * 14

  def session_cookie_name, do: @session_cookie

  def base_url do
    System.get_env("BASE_URL", "http://localhost:" <> System.get_env("PORT", "4000"))
    |> String.trim_trailing("/")
  end

  def app_slug do
    case System.get_env("GITHUB_APP_SLUG") do
      s when is_binary(s) and s != "" ->
        s

      _ ->
        case Redix.command(:redix, ["GET", "taggle:app_slug"]) do
          {:ok, slug} when is_binary(slug) and slug != "" -> slug
          _ -> nil
        end
    end
  end

  def github_authorize_url do
    case System.get_env("GITHUB_APP_CLIENT_ID") do
      client_id when is_binary(client_id) and client_id != "" ->
        state = :crypto.strong_rand_bytes(18) |> Base.url_encode64(padding: false)
        Redix.command!(:redix, ["SET", "taggle:oauth_state:" <> state, "1", "EX", "600"])

        query =
          URI.encode_query(%{
            client_id: client_id,
            redirect_uri: base_url() <> "/auth/github/callback",
            state: state
          })

        {:ok, "https://github.com/login/oauth/authorize?" <> query}

      _ ->
        {:error, :missing_client_id}
    end
  end

  def exchange_code(code, state, installation_id \\ nil)

  def exchange_code(code, state, installation_id) when is_binary(code) do
    with :ok <- verify_oauth_state(state, installation_id),
         client_id when is_binary(client_id) and client_id != "" <- System.get_env("GITHUB_APP_CLIENT_ID"),
         client_secret when is_binary(client_secret) and client_secret != "" <- System.get_env("GITHUB_APP_CLIENT_SECRET"),
         {:ok, %{status: 200, body: %{"access_token" => token}}} when is_binary(token) <-
           Req.post("https://github.com/login/oauth/access_token",
             headers: [{"accept", "application/json"}],
             json: %{
               client_id: client_id,
               client_secret: client_secret,
               code: code,
               redirect_uri: base_url() <> "/auth/github/callback"
             },
             retry: false
           ),
         {:ok, %{status: 200, body: %{"id" => id, "login" => login} = user}} <-
           Req.get("https://api.github.com/user",
             headers: [
               {"authorization", "Bearer " <> token},
               {"accept", "application/vnd.github+json"},
               {"user-agent", "taggle-gateway"}
             ],
             retry: false
           ) do
      {:ok, %{id: id, login: login, name: user["name"], avatar_url: user["avatar_url"], access_token: token}}
    else
      nil -> {:error, :missing_oauth_config}
      {:error, _} = err -> err
      other -> {:error, other}
    end
  end

  def exchange_code(_, _, _), do: {:error, :invalid_params}

  defp verify_oauth_state(state, _installation_id) when is_binary(state) and state != "" do
    case Redix.command(:redix, ["GETDEL", "taggle:oauth_state:" <> state]) do
      {:ok, "1"} -> :ok
      _ -> {:error, :invalid_state}
    end
  end

  # GitHub App installation flow omits `state` when initiated from GitHub's UI.
  defp verify_oauth_state(nil, inst_id) when is_binary(inst_id) and inst_id != "" do
    case Integer.parse(inst_id) do
      {_, ""} -> :ok
      _ -> {:error, :invalid_state}
    end
  end

  defp verify_oauth_state(_, _), do: {:error, :invalid_state}

  def sign_session(login) do
    Plug.Crypto.MessageVerifier.sign(login, session_secret())
  end

  def verify_session(conn) do
    with token when is_binary(token) <- conn.cookies[@session_cookie],
         {:ok, login} <- Plug.Crypto.MessageVerifier.verify(token, session_secret()),
         true <- Taggle.Gateway.Ids.github_login?(login) do
      {:ok, login}
    else
      _ -> :error
    end
  end

  def extract_api_key(conn) do
    case Plug.Conn.get_req_header(conn, "authorization") do
      ["Bearer " <> token] -> String.trim(token)
      ["bearer " <> token] -> String.trim(token)
      [token] -> String.trim(token)
      _ -> nil
    end
  end

  defp session_secret do
    case System.get_env("SESSION_SECRET") do
      s when is_binary(s) and byte_size(s) >= 16 ->
        Plug.Crypto.KeyGenerator.generate(s, @session_salt)

      _ ->
        # Dev fallback: ephemeral key per VM boot when SESSION_SECRET isn't configured.
        case :persistent_term.get({__MODULE__, :secret}, nil) do
          nil ->
            key = :crypto.strong_rand_bytes(32)
            :persistent_term.put({__MODULE__, :secret}, key)
            key

          key ->
            key
        end
    end
  end

  def session_cookie_opts do
    [
      http_only: true,
      same_site: "Lax",
      max_age: @session_max_age,
      secure: String.starts_with?(base_url(), "https://")
    ]
  end
end
