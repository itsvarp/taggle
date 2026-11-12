defmodule Taggle.Gateway.DiscordAuth do
  @moduledoc """
  Discord OAuth2 (`identify`) flow used to issue API keys for Discord KV.

  Presence itself needs no login: joining the monitored Discord server is enough.
  OAuth is only required to prove ownership of a Discord id before creating a KV
  API key.
  """

  @cookie "taggle_dc_session"
  @state_prefix "taggle:dc_oauth_state:"
  @salt "taggle-discord-session-v1"
  @session_max_age 60 * 60 * 24 * 14

  def cookie_name, do: @cookie

  def configured? do
    is_binary(System.get_env("DISCORD_CLIENT_ID")) and System.get_env("DISCORD_CLIENT_ID") != "" and
      is_binary(System.get_env("DISCORD_CLIENT_SECRET")) and System.get_env("DISCORD_CLIENT_SECRET") != ""
  end

  def invite_url do
    case System.get_env("DISCORD_INVITE_URL") do
      url when is_binary(url) and url != "" -> url
      _ -> nil
    end
  end

  def base_url do
    System.get_env("BASE_URL", "http://localhost:" <> System.get_env("PORT", "4000"))
    |> String.trim_trailing("/")
  end

  def redirect_uri, do: System.get_env("DISCORD_REDIRECT_URI") || base_url() <> "/auth/discord/callback"

  def authorize_url do
    with client_id when is_binary(client_id) and client_id != "" <- System.get_env("DISCORD_CLIENT_ID"),
         client_secret when is_binary(client_secret) and client_secret != "" <- System.get_env("DISCORD_CLIENT_SECRET") do
      _ = client_secret
      state = :crypto.strong_rand_bytes(24) |> Base.url_encode64(padding: false)
      Redix.command!(:redix, ["SET", @state_prefix <> state, "1", "EX", "600"])

      query =
        URI.encode_query(%{
          client_id: client_id,
          redirect_uri: redirect_uri(),
          response_type: "code",
          scope: "identify",
          state: state
        })

      {:ok, "https://discord.com/oauth2/authorize?" <> query}
    else
      _ -> {:error, :not_configured}
    end
  end

  def exchange_code(code, state) when is_binary(code) and code != "" do
    with :ok <- verify_state(state),
         client_id when is_binary(client_id) and client_id != "" <- System.get_env("DISCORD_CLIENT_ID"),
         client_secret when is_binary(client_secret) and client_secret != "" <- System.get_env("DISCORD_CLIENT_SECRET"),
         {:ok, %{status: 200, body: %{"access_token" => token}}} when is_binary(token) <-
           Req.post("https://discord.com/api/oauth2/token",
             form: %{
               client_id: client_id,
               client_secret: client_secret,
               grant_type: "authorization_code",
               code: code,
               redirect_uri: redirect_uri()
             },
             headers: [{"accept", "application/json"}],
             retry: false
           ),
         {:ok, %{status: 200, body: %{"id" => id, "username" => username} = user}} <-
           Req.get("https://discord.com/api/users/@me",
             headers: [
               {"authorization", "Bearer " <> token},
               {"accept", "application/json"},
               {"user-agent", "taggle-gateway"}
             ],
             retry: false
           ) do
      {:ok,
       %{
         id: id,
         username: username,
         global_name: user["global_name"],
         avatar: user["avatar"]
       }}
    else
      nil -> {:error, :not_configured}
      {:error, _} = err -> err
      %{status: status} -> {:error, {:discord_oauth_status, status}}
      other -> {:error, other}
    end
  end

  def exchange_code(_, _), do: {:error, :invalid_params}

  defp verify_state(state) when is_binary(state) and state != "" do
    case Redix.command(:redix, ["GETDEL", @state_prefix <> state]) do
      {:ok, "1"} -> :ok
      _ -> {:error, :invalid_state}
    end
  end

  defp verify_state(_), do: {:error, :invalid_state}

  def sign_session(user_id), do: Plug.Crypto.MessageVerifier.sign(user_id, session_secret())

  def verify_session(conn) do
    with token when is_binary(token) <- conn.cookies[@cookie],
         {:ok, user_id} <- Plug.Crypto.MessageVerifier.verify(token, session_secret()),
         true <- Taggle.Gateway.Ids.discord_id?(user_id) do
      {:ok, user_id}
    else
      _ -> :error
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

  def avatar_url(%{id: id, avatar: avatar}) when is_binary(avatar) and avatar != "" do
    "https://cdn.discordapp.com/avatars/#{id}/#{avatar}.png"
  end

  def avatar_url(%{id: id}) do
    index =
      case Integer.parse(to_string(id)) do
        {n, ""} -> rem(div(n, 4_194_304), 6)
        _ -> 0
      end

    "https://cdn.discordapp.com/embed/avatars/#{index}.png"
  end

  defp session_secret do
    case System.get_env("SESSION_SECRET") do
      s when is_binary(s) and byte_size(s) >= 16 ->
        Plug.Crypto.KeyGenerator.generate(s, @salt)

      _ ->
        # Dev fallback: random key per VM boot when SESSION_SECRET isn't configured.
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
end
