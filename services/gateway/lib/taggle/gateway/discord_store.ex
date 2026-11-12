defmodule Taggle.Gateway.DiscordStore do
  @moduledoc """
  Discord presence read-through cache, OAuth users and Lanyard-style KV.

  The Rust ingest service owns the Discord gateway connection and writes
  presences to `taggle:discord:state:{id}` + Postgres. This module serves them
  to the REST/WebSocket API and manages per-user KV.
  """
  require Logger

  alias Taggle.Gateway.Ids

  @api_key_prefix "tggl_"
  @max_kv_pairs 512
  @max_value_bytes 30_000

  def state_key(user_id), do: "taggle:discord:state:" <> user_id
  def events_key(user_id), do: "taggle:discord:events:" <> user_id

  def get_presence(user_id) do
    with true <- Ids.discord_id?(user_id),
         {:ok, raw} <- get_presence_raw(user_id) do
      case raw do
        nil -> {:ok, nil}
        data -> {:ok, Map.put(data, "kv", kv_map(user_id))}
      end
    else
      false -> {:error, :invalid_id}
      {:error, _} = err -> err
    end
  end

  def get_presence_raw(user_id) do
    key = state_key(user_id)

    case Redix.command(:redix, ["GET", key]) do
      {:ok, raw} when is_binary(raw) ->
        {:ok, JSON.decode!(raw)}

      {:ok, nil} ->
        read_through_pg(user_id, key)

      {:error, reason} ->
        Logger.warning("redis discord read failed, falling back to pg: #{inspect(reason)}")
        read_through_pg(user_id, nil)
    end
  end

  defp read_through_pg(user_id, redis_key) do
    case Postgrex.query(:pg, "SELECT data FROM discord_presences WHERE user_id = $1", [user_id]) do
      {:ok, %Postgrex.Result{rows: [[data]]}} ->
        raw = JSON.encode!(data)
        if redis_key, do: _ = Redix.command(:redix, ["SET", redis_key, raw])
        {:ok, data}

      {:ok, %Postgrex.Result{rows: []}} ->
        {:ok, nil}

      {:error, reason} ->
        {:error, reason}
    end
  end

  @doc "All monitored presences as a `%{user_id => presence}` map, capped for safety."
  def list_presences(limit \\ 500) do
    limit = limit |> max(1) |> min(2_000)

    with {:ok, %Postgrex.Result{rows: rows}} <-
           Postgrex.query(:pg, "SELECT user_id, data FROM discord_presences ORDER BY updated_at DESC LIMIT $1", [limit]),
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
    case Postgrex.query(:pg, "SELECT user_id, key, value FROM discord_kv WHERE user_id = ANY($1::text[])", [user_ids]) do
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
      {:ok, %Postgrex.Result{rows: rows}} -> Map.new(rows, fn [k, v] -> {k, v} end)
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

  def oauth_user(user_id) do
    case Postgrex.query(
           :pg,
           "SELECT id, username, global_name, avatar, api_key_hash IS NOT NULL FROM discord_users WHERE id = $1",
           [user_id]
         ) do
      {:ok, %Postgrex.Result{rows: [[id, username, global_name, avatar, has_key]]}} ->
        {:ok, %{id: id, username: username, global_name: global_name, avatar: avatar, has_api_key: has_key}}

      {:ok, %Postgrex.Result{rows: []}} ->
        :not_found

      {:error, reason} ->
        {:error, reason}
    end
  end

  def rotate_api_key(user_id) when is_binary(user_id) do
    token = @api_key_prefix <> (:crypto.strong_rand_bytes(32) |> Base.url_encode64(padding: false))
    hash = sha256_hex(token)

    case Postgrex.query(:pg, "UPDATE discord_users SET api_key_hash = $1 WHERE id = $2 RETURNING id", [hash, user_id]) do
      {:ok, %Postgrex.Result{rows: [[_]]}} -> {:ok, token}
      _ -> :not_found
    end
  end

  def valid_api_key?(user_id, token) when is_binary(token) and byte_size(token) > 0 do
    case Postgrex.query(:pg, "SELECT api_key_hash FROM discord_users WHERE id = $1", [user_id]) do
      {:ok, %Postgrex.Result{rows: [[stored]]}} when is_binary(stored) ->
        Plug.Crypto.secure_compare(stored, sha256_hex(token))

      _ ->
        false
    end
  end

  def valid_api_key?(_, _), do: false

  # ---------------------------------------------------------------------
  # KV writes (Lanyard semantics)
  # ---------------------------------------------------------------------

  def put_kv(user_id, pairs) when is_map(pairs) do
    with true <- Ids.discord_id?(user_id),
         :ok <- validate_pairs(pairs),
         :ok <- ensure_pair_budget(user_id, pairs) do
      Postgrex.transaction(:pg, fn conn ->
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
      end)
      |> case do
        {:ok, _} -> publish_presence(user_id)
        {:error, reason} -> {:error, reason}
      end
    else
      false -> {:error, "invalid_id"}
      {:error, _} = err -> err
    end
  end

  def delete_kv(user_id, key) when is_binary(key) do
    with true <- Ids.discord_id?(user_id),
         true <- valid_key?(key) do
      Postgrex.query!(:pg, "DELETE FROM discord_kv WHERE user_id = $1 AND key = $2", [user_id, key])
      publish_presence(user_id)
    else
      false -> {:error, "invalid_key"}
    end
  end

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

  defp ensure_pair_budget(user_id, pairs) do
    case Postgrex.query(:pg, "SELECT key FROM discord_kv WHERE user_id = $1", [user_id]) do
      {:ok, %Postgrex.Result{rows: rows}} ->
        existing = MapSet.new(rows, &hd/1)
        total = existing |> MapSet.union(MapSet.new(Map.keys(pairs))) |> MapSet.size()

        if total > @max_kv_pairs, do: {:error, "too_many_keys"}, else: :ok

      {:error, reason} ->
        {:error, reason}
    end
  end

  @doc """
  Re-merges KV into the stored presence and republishes it so WebSocket clients
  receive an updated `PRESENCE_UPDATE` after a KV write.
  """
  def publish_presence(user_id) do
    kv = kv_map(user_id)

    case get_presence_raw(user_id) do
      {:ok, nil} ->
        {:ok, kv}

      {:ok, data} when is_map(data) ->
        data = Map.put(data, "kv", kv)
        raw = JSON.encode!(data)

        _ = Postgrex.query(
          :pg,
          "UPDATE discord_presences SET data = $2, updated_at = now() WHERE user_id = $1",
          [user_id, data]
        )

        _ = Redix.command(:redix, ["SET", state_key(user_id), raw])
        _ = Redix.command(:redix, ["PUBLISH", events_key(user_id), raw])
        {:ok, kv}

      {:error, reason} ->
        {:error, reason}
    end
  end

  def overview(user_id) do
    presence =
      case get_presence(user_id) do
        {:ok, p} -> p
        _ -> nil
      end

    user =
      case oauth_user(user_id) do
        {:ok, u} -> u
        _ -> nil
      end

    %{presence: presence, user: user, kv: kv_map(user_id)}
  end

  defp sha256_hex(s), do: :crypto.hash(:sha256, s) |> Base.encode16(case: :lower)
end
