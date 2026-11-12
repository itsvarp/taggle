defmodule Taggle.Gateway.AvatarCache do
  @moduledoc """
  Self-hosted, SSRF-safe avatar fetcher and ETS/Redis cache for inline SVG data URIs.
  Only fetches from validated Discord/GitHub avatar URLs and verifies image magic bytes.
  """

  alias Taggle.Gateway.{DiscordAuth, Ids}

  @ets_table :taggle_gateway_avatar_cache
  @ets_ttl_ms 600_000
  @neg_ttl_ms 60_000
  @redis_ttl_secs 600
  @neg_redis_ttl_secs 60
  @max_bytes 150_000

  @data_uri_re ~r/^data:image\/(png|jpeg|gif|webp);base64,[A-Za-z0-9+\/=]+$/

  def init do
    if :ets.whereis(@ets_table) == :undefined do
      :ets.new(@ets_table, [
        :named_table,
        :public,
        :set,
        {:read_concurrency, true},
        {:write_concurrency, true}
      ])
    end

    :ok
  end

  def valid_data_uri?(uri) when is_binary(uri), do: Regex.match?(@data_uri_re, uri)
  def valid_data_uri?(_), do: false

  @doc "Seeds a validated data URI in ETS for deterministic tests or prewarming."
  def put(key, data_uri) when is_binary(key) and is_binary(data_uri) do
    init()

    if valid_data_uri?(data_uri) do
      :ets.insert(@ets_table, {key, data_uri, System.monotonic_time(:millisecond) + @ets_ttl_ms})
      :ok
    else
      {:error, :invalid_data_uri}
    end
  end

  @doc "Resolves a safe `data:image/...;base64,...` URI for a profile, or nil if unavailable."
  def for_profile(%{discord: discord, github: github}), do: resolve(discord, github)
  def for_profile(%{"discord_user" => _} = discord), do: resolve(discord, nil)
  def for_profile(%{"user" => _} = github), do: resolve(nil, github)
  def for_profile(_), do: nil

  defp resolve(discord, github) do
    case build_source(discord, github) do
      nil -> nil
      {cache_key, url} -> get_or_fetch(cache_key, url)
    end
  end

  @doc false
  def cache_key_for_discord(id, avatar) when is_binary(id) do
    if DiscordAuth.valid_avatar?(avatar), do: "dc:#{id}:#{avatar}", else: "dc:#{id}:embed"
  end

  def cache_key_for_github(login) when is_binary(login), do: "gh:#{Ids.normalize_github(login)}"

  defp build_source(discord, github) do
    discord_id = get_in(discord || %{}, ["discord_user", "id"])
    discord_avatar = get_in(discord || %{}, ["discord_user", "avatar"])

    gh_login =
      get_in(github || %{}, ["user", "username"]) || get_in(github || %{}, ["user", "login"])

    cond do
      is_binary(discord_id) and Ids.discord_id?(discord_id) and
          DiscordAuth.valid_avatar?(discord_avatar) ->
        ext = if String.starts_with?(discord_avatar, "a_"), do: "gif", else: "png"
        url = "https://cdn.discordapp.com/avatars/#{discord_id}/#{discord_avatar}.#{ext}?size=128"
        {cache_key_for_discord(discord_id, discord_avatar), url}

      is_binary(gh_login) and Ids.github_login?(gh_login) ->
        norm = Ids.normalize_github(gh_login)
        {cache_key_for_github(norm), "https://github.com/#{norm}.png?size=128"}

      is_binary(discord_id) and Ids.discord_id?(discord_id) ->
        url = DiscordAuth.avatar_url(%{id: discord_id})
        {cache_key_for_discord(discord_id, nil), url}

      true ->
        nil
    end
  end

  defp get_or_fetch(cache_key, url) do
    init()
    now = System.monotonic_time(:millisecond)

    case :ets.lookup(@ets_table, cache_key) do
      [{^cache_key, val, exp}] when exp > now ->
        val

      _ ->
        fetch_from_redis_or_upstream(cache_key, url, now)
    end
  end

  defp fetch_from_redis_or_upstream(cache_key, url, now) do
    redis_key = "taggle:avatar_uri:" <> cache_key

    case redix_get(redis_key) do
      {:ok, ""} ->
        :ets.insert(@ets_table, {cache_key, nil, now + @neg_ttl_ms})
        nil

      {:ok, uri} when is_binary(uri) ->
        if valid_data_uri?(uri) do
          :ets.insert(@ets_table, {cache_key, uri, now + @ets_ttl_ms})
          uri
        else
          nil
        end

      _ ->
        if fetch_enabled?() do
          case fetch_and_encode(url) do
            {:ok, uri} ->
              :ets.insert(@ets_table, {cache_key, uri, now + @ets_ttl_ms})
              redix_set(redis_key, uri, @redis_ttl_secs)
              uri

            :error ->
              :ets.insert(@ets_table, {cache_key, nil, now + @neg_ttl_ms})
              redix_set(redis_key, "", @neg_redis_ttl_secs)
              nil
          end
        else
          nil
        end
    end
  end

  defp fetch_enabled? do
    Application.get_env(:taggle_gateway, :fetch_avatars, true) == true
  end

  defp fetch_and_encode(url) do
    case Req.get(url,
           receive_timeout: 2_000,
           connect_options: [timeout: 1_500],
           max_redirects: 3,
           decode_body: false
         ) do
      {:ok, %Req.Response{status: 200, body: body}}
      when is_binary(body) and byte_size(body) > 0 and byte_size(body) <= @max_bytes ->
        case detect_mime(body) do
          {:ok, mime} -> {:ok, "data:#{mime};base64," <> Base.encode64(body)}
          :error -> :error
        end

      _ ->
        :error
    end
  rescue
    _ -> :error
  end

  defp detect_mime(<<0x89, 0x50, 0x4E, 0x47, 0x0D, 0x0A, 0x1A, 0x0A, _::binary>>),
    do: {:ok, "image/png"}

  defp detect_mime(<<0xFF, 0xD8, 0xFF, _::binary>>), do: {:ok, "image/jpeg"}
  defp detect_mime(<<"GIF87a", _::binary>>), do: {:ok, "image/gif"}
  defp detect_mime(<<"GIF89a", _::binary>>), do: {:ok, "image/gif"}
  defp detect_mime(<<"RIFF", _::binary-size(4), "WEBP", _::binary>>), do: {:ok, "image/webp"}
  defp detect_mime(_), do: :error

  defp redix_get(key) do
    if Process.whereis(:redix), do: Redix.command(:redix, ["GET", key]), else: {:error, :no_redis}
  rescue
    _ -> {:error, :redis_error}
  end

  defp redix_set(key, val, ttl_secs) do
    if Process.whereis(:redix),
      do: Redix.command(:redix, ["SET", key, val, "EX", Integer.to_string(ttl_secs)]),
      else: :ok
  rescue
    _ -> :ok
  end
end
