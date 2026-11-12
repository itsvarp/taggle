defmodule Taggle.Gateway.DiscordStore do
  @moduledoc """
  Discord presence read-through cache, OAuth users, race-free KV and linked lookups.

  The Rust ingest service owns the Discord gateway connection and writes
  pure presence data to `taggle:discord:presence:{id}` + Postgres. This module
  owns `taggle:discord:kv:{id}`, merges both atomically via Redis Lua into
  `taggle:discord:state:{id}`, and serves them over REST/WebSocket.
  """
  require Logger

  alias Taggle.Gateway.{GitHubAuth, Ids, Metrics}

  @api_key_prefix "tggl_"
  @max_kv_pairs 512
  @max_value_bytes 30_000

  @kv_merge_lua """
  local presence_key = KEYS[1]
  local kv_key = KEYS[2]
  local state_key = KEYS[3]
  local events_channel = ARGV[1]
  local kv_json = ARGV[2]

  redis.call('SET', kv_key, kv_json)
  local p_raw = redis.call('GET', presence_key)
  if not p_raw or p_raw == '' then
    return 0
  end

  local trimmed = p_raw:match('^%s*(.-)%s*$')
  local merged
  if trimmed == '{}' then
    merged = '{"kv":' .. kv_json .. '}'
  else
    merged = string.sub(trimmed, 1, -2) .. ',"kv":' .. kv_json .. '}'
  end

  local prev = redis.call('GET', state_key)
  if prev ~= merged then
    redis.call('SET', state_key, merged)
    redis.call('PUBLISH', events_channel, merged)
  end
  return 1
  """

  def presence_key(user_id), do: "taggle:discord:presence:" <> user_id
  def kv_key(user_id), do: "taggle:discord:kv:" <> user_id
  def state_key(user_id), do: "taggle:discord:state:" <> user_id
  def events_key(user_id), do: "taggle:discord:events:" <> user_id

  @doc "Returns decoded presence map (used by quicklinks, SVG cards, profiles, WS init)."
  def get_presence(user_id) do
    with true <- Ids.discord_id?(user_id),
         {:ok, raw} when is_binary(raw) <- get_presence_json(user_id) do
      case JSON.decode(raw) do
        {:ok, decoded} ->
          {:ok, decoded}

        {:error, _} ->
          with {:ok, fresh} when is_binary(fresh) <- read_through_pg_json(user_id, true),
               {:ok, decoded} <- JSON.decode(fresh) do
            {:ok, decoded}
          else
            {:ok, nil} -> {:ok, nil}
            _ -> {:error, :invalid_json}
          end
      end
    else
      false -> {:error, :invalid_id}
      {:ok, nil} -> {:ok, nil}
      {:error, _} = err -> err
    end
  end

  @doc "Fast-path raw JSON binary read from Redis with Postgres read-through."
  def get_presence_json(user_id) do
    if Ids.discord_id?(user_id) do
      key = state_key(user_id)

      case Redix.command(:redix, ["GET", key]) do
        {:ok, raw} when is_binary(raw) ->
          Metrics.inc(:cache_hits_total)
          {:ok, raw}

        {:ok, nil} ->
          Metrics.inc(:cache_misses_total)
          read_through_pg_json(user_id, true)

        {:error, reason} ->
          Metrics.inc(:cache_misses_total)
          Logger.warning("redis discord read failed, falling back to pg: #{inspect(reason)}")
          read_through_pg_json(user_id, false)
      end
    else
      {:error, :invalid_id}
    end
  end

  @doc "Batch-fetches presences for multiple Discord IDs in a single Redis MGET roundtrip."
  def get_presences_batch([]), do: %{}

  def get_presences_batch(user_ids) do
    valid_ids = Enum.filter(user_ids, &Ids.discord_id?/1) |> Enum.uniq()
    if valid_ids == [], do: %{}

    keys = Enum.map(valid_ids, &state_key/1)

    cached =
      case Redix.command(:redix, ["MGET" | keys]) do
        {:ok, values} when is_list(values) ->
          Enum.zip(valid_ids, values)

        _ ->
          Enum.map(valid_ids, &{&1, nil})
      end

    Enum.reduce(cached, %{}, fn
      {id, raw}, acc when is_binary(raw) ->
        Metrics.inc(:cache_hits_total)

        case JSON.decode(raw) do
          {:ok, decoded} -> Map.put(acc, id, decoded)
          _ -> put_if_present(acc, id, get_presence(id))
        end

      {id, nil}, acc ->
        Metrics.inc(:cache_misses_total)
        put_if_present(acc, id, get_presence(id))
    end)
  end

  defp put_if_present(acc, id, {:ok, %{} = map}), do: Map.put(acc, id, map)
  defp put_if_present(acc, _id, _), do: acc

  defp read_through_pg_json(user_id, populate_redis?) do
    Metrics.inc(:pg_fallbacks_total)

    case Postgrex.query(:pg, "SELECT data FROM discord_presences WHERE user_id = $1", [user_id]) do
      {:ok, %Postgrex.Result{rows: [[data]]}} ->
        kv = kv_map(user_id)
        presence_only = Map.delete(data, "kv")
        merged = Map.put(presence_only, "kv", kv)
        raw = JSON.encode!(merged)

        if populate_redis? do
          _ =
            Redix.pipeline(:redix, [
              ["SET", presence_key(user_id), JSON.encode!(presence_only)],
              ["SET", kv_key(user_id), JSON.encode!(kv)],
              ["SET", state_key(user_id), raw]
            ])
        end

        {:ok, raw}

      {:ok, %Postgrex.Result{rows: []}} ->
        {:ok, nil}

      {:error, reason} ->
        {:error, reason}
    end
  end

  def linked_github_login(user_id) do
    if Ids.discord_id?(user_id) do
      case Postgrex.query(
             :pg,
             "SELECT github_login FROM discord_github_links WHERE discord_user_id = $1",
             [user_id]
           ) do
        {:ok, %Postgrex.Result{rows: [[login]]}} -> {:ok, login}
        {:ok, %Postgrex.Result{rows: []}} -> {:ok, nil}
        {:error, reason} -> {:error, reason}
      end
    else
      {:ok, nil}
    end
  end

  @doc "All monitored presences as a `%{user_id => presence}` map, capped for safety."
  def list_presences(limit \\ 500) do
    limit = limit |> max(1) |> min(2_000)

    with {:ok, %Postgrex.Result{rows: rows}} <-
           Postgrex.query(
             :pg,
             "SELECT user_id, data FROM discord_presences ORDER BY updated_at DESC LIMIT $1",
             [limit]
           ),
         {:ok, kv_by_user} <- kv_by_user(Enum.map(rows, &hd/1)) do
      presences =
        Map.new(rows, fn [user_id, data] ->
          {user_id, Map.put(data, "kv", Map.get(kv_by_user, user_id, %{}))}
        end)

      {:ok, presences}
    else
      {:error, reason} -> {:error, reason}
    end
  end

  defp kv_by_user([]), do: {:ok, %{}}

  defp kv_by_user(user_ids) do
    case Postgrex.query(
           :pg,
           "SELECT user_id, key, value FROM discord_kv WHERE user_id = ANY($1::text[])",
           [user_ids]
         ) do
      {:ok, %Postgrex.Result{rows: rows}} ->
        grouped =
          rows
          |> Enum.group_by(&hd/1, fn [_, key, value] -> {key, value} end)
          |> Map.new(fn {user_id, pairs} -> {user_id, Map.new(pairs)} end)

        {:ok, grouped}

      {:error, reason} ->
        {:error, reason}
    end
  end

  def kv_map(user_id) do
    case Postgrex.query(:pg, "SELECT key, value FROM discord_kv WHERE user_id = $1", [user_id]) do
      {:ok, %Postgrex.Result{rows: rows}} ->
        Map.new(rows, fn [k, v] -> {k, v} end)

      {:error, reason} ->
        Logger.warning("discord kv read failed: #{inspect(reason)}")
        %{}
    end
  end

  # ---------------------------------------------------------------------
  # OAuth account + API key
  # ---------------------------------------------------------------------

  def upsert_oauth_user(%{id: id, username: username} = user) when is_binary(id) do
    if Ids.discord_id?(id) do
      Postgrex.query!(
        :pg,
        """
        INSERT INTO discord_users (id, username, global_name, avatar)
        VALUES ($1, $2, $3, $4)
        ON CONFLICT (id) DO UPDATE
          SET username = EXCLUDED.username,
              global_name = COALESCE(EXCLUDED.global_name, discord_users.global_name),
              avatar = EXCLUDED.avatar
        """,
        [id, username, Map.get(user, :global_name), Map.get(user, :avatar)]
      )

      {:ok, id}
    else
      {:error, :invalid_id}
    end
  end

  def rotate_api_key(user_id) when is_binary(user_id) do
    token =
      @api_key_prefix <> (:crypto.strong_rand_bytes(32) |> Base.url_encode64(padding: false))

    hash = sha256_hex(token)

    case Postgrex.query(
           :pg,
           "UPDATE discord_users SET api_key_hash = $1 WHERE id = $2 RETURNING id",
           [hash, user_id]
         ) do
      {:ok, %Postgrex.Result{rows: [[_]]}} -> {:ok, token}
      _ -> :not_found
    end
  end

  def valid_api_key?(user_id, token) when is_binary(token) do
    if Ids.discord_id?(user_id) and GitHubAuth.valid_token_shape?(token) do
      case Postgrex.query(:pg, "SELECT api_key_hash FROM discord_users WHERE id = $1", [user_id]) do
        {:ok, %Postgrex.Result{rows: [[stored]]}} when is_binary(stored) ->
          Plug.Crypto.secure_compare(stored, sha256_hex(token))

        _ ->
          false
      end
    else
      false
    end
  end

  def valid_api_key?(_, _), do: false

  # ---------------------------------------------------------------------
  # KV writes (Lanyard semantics, race-free with ingest presence updates)
  # ---------------------------------------------------------------------

  def put_kv(user_id, pairs) when is_map(pairs) do
    with true <- Ids.discord_id?(user_id),
         :ok <- validate_pairs(pairs) do
      Postgrex.transaction(:pg, fn conn ->
        Postgrex.query!(conn, "SELECT pg_advisory_xact_lock(hashtext('dcstate:' || $1))", [
          user_id
        ])

        for {k, v} <- pairs do
          Postgrex.query!(
            conn,
            """
            INSERT INTO discord_kv (user_id, key, value) VALUES ($1, $2, $3)
            ON CONFLICT (user_id, key) DO UPDATE SET value = EXCLUDED.value
            """,
            [user_id, k, to_string(v)]
          )
        end

        %Postgrex.Result{rows: [[total]]} =
          Postgrex.query!(conn, "SELECT count(*) FROM discord_kv WHERE user_id = $1", [user_id])

        if total > @max_kv_pairs do
          Postgrex.rollback(conn, "too_many_keys")
        end

        collect_kv_update!(conn, user_id)
      end)
      |> finalize_kv_publish(user_id)
    else
      false -> {:error, "invalid_id"}
      {:error, _} = err -> err
    end
  end

  def delete_kv(user_id, key) when is_binary(key) do
    with true <- Ids.discord_id?(user_id),
         true <- valid_key?(key) do
      Postgrex.transaction(:pg, fn conn ->
        Postgrex.query!(conn, "SELECT pg_advisory_xact_lock(hashtext('dcstate:' || $1))", [
          user_id
        ])

        Postgrex.query!(conn, "DELETE FROM discord_kv WHERE user_id = $1 AND key = $2", [
          user_id,
          key
        ])

        collect_kv_update!(conn, user_id)
      end)
      |> finalize_kv_publish(user_id)
    else
      false -> {:error, "invalid_key"}
    end
  end

  def delete_kv(_, _), do: {:error, "invalid_key"}

  defp collect_kv_update!(conn, user_id) do
    %Postgrex.Result{rows: kv_rows} =
      Postgrex.query!(conn, "SELECT key, value FROM discord_kv WHERE user_id = $1", [user_id])

    kv = Map.new(kv_rows, fn [k, v] -> {k, v} end)

    case Postgrex.query!(
           conn,
           """
           UPDATE discord_presences
           SET data = jsonb_set(data, '{kv}', $2::jsonb, true), updated_at = now()
           WHERE user_id = $1
           RETURNING data
           """,
           [user_id, kv]
         ) do
      %Postgrex.Result{rows: [[presence]]} -> {:publish, kv, presence}
      %Postgrex.Result{rows: []} -> {:noop, kv}
    end
  end

  defp finalize_kv_publish({:ok, {:publish, kv, presence}}, user_id) do
    kv_json = JSON.encode!(kv)

    case Redix.command(:redix, [
           "EVAL",
           @kv_merge_lua,
           "3",
           presence_key(user_id),
           kv_key(user_id),
           state_key(user_id),
           events_key(user_id),
           kv_json
         ]) do
      {:ok, 1} ->
        {:ok, kv}

      _ ->
        # Fallback if presence:{id} wasn't seeded in Redis yet (e.g., direct PG insert or Redis flush).
        presence_only = Map.delete(presence, "kv")
        merged = Map.put(presence_only, "kv", kv)
        raw = JSON.encode!(merged)

        _ =
          Redix.pipeline(:redix, [
            ["SET", presence_key(user_id), JSON.encode!(presence_only)],
            ["SET", kv_key(user_id), kv_json],
            ["SET", state_key(user_id), raw],
            ["PUBLISH", events_key(user_id), raw]
          ])

        {:ok, kv}
    end
  end

  defp finalize_kv_publish({:ok, {:noop, kv}}, user_id) do
    _ = Redix.command(:redix, ["SET", kv_key(user_id), JSON.encode!(kv)])
    {:ok, kv}
  end

  defp finalize_kv_publish({:error, reason}, _user_id), do: {:error, reason}

  defp validate_pairs(pairs) when map_size(pairs) > 50, do: {:error, "too_many_keys"}

  defp validate_pairs(pairs) do
    Enum.reduce_while(pairs, :ok, fn {k, v}, :ok ->
      cond do
        not valid_key?(k) ->
          {:halt, {:error, "invalid_key"}}

        not (is_binary(v) or is_number(v) or is_boolean(v)) ->
          {:halt, {:error, "invalid_value"}}

        byte_size(to_string(v)) > @max_value_bytes ->
          {:halt, {:error, "value_too_large"}}

        true ->
          {:cont, :ok}
      end
    end)
  end

  defp valid_key?(key) when is_binary(key), do: Regex.match?(~r/^[A-Za-z0-9_]{1,255}$/, key)
  defp valid_key?(_), do: false

  defp sha256_hex(s), do: :crypto.hash(:sha256, s) |> Base.encode16(case: :lower)
end
