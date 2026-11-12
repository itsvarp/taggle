defmodule Taggle.Gateway.GitHubStore do
  @moduledoc """
  Redis hot-cache + PostgreSQL read-through store, linked lookups and KV management.
  """
  require Logger

  alias Taggle.Gateway.{GitHubAuth, Ids, Metrics}

  @max_kv_pairs 512
  @max_value_bytes 30_000

  def state_key(login), do: "taggle:state:" <> login

  @doc "Fetches the serialized JSON snapshot from Redis, falling back to Postgres on miss."
  def get_snapshot_raw(login) when is_binary(login) do
    key = state_key(login)

    case Redix.command(:redix, ["GET", key]) do
      {:ok, raw} when is_binary(raw) ->
        Metrics.inc(:cache_hits_total)
        {:ok, raw}

      {:ok, nil} ->
        Metrics.inc(:cache_misses_total)
        read_through_pg(login, key)

      {:error, reason} ->
        Metrics.inc(:cache_misses_total)
        Logger.warning("redis read failed, falling back to pg: #{inspect(reason)}")
        read_through_pg(login, nil)
    end
  end

  def connected?(login) when is_binary(login) do
    case Postgrex.query(
           :pg,
           "SELECT EXISTS(SELECT 1 FROM discord_github_links WHERE github_login = $1)",
           [login]
         ) do
      {:ok, %Postgrex.Result{rows: [[true]]}} -> true
      _ -> false
    end
  end

  def connected?(_), do: false

  def linked_discord_id(login) when is_binary(login) do
    if Ids.github_login?(login) do
      case Postgrex.query(
             :pg,
             "SELECT discord_user_id FROM discord_github_links WHERE github_login = $1 LIMIT 1",
             [login]
           ) do
        {:ok, %Postgrex.Result{rows: [[discord_id]]}} -> {:ok, discord_id}
        {:ok, %Postgrex.Result{rows: []}} -> {:ok, nil}
        {:error, reason} -> {:error, reason}
      end
    else
      {:ok, nil}
    end
  end

  @doc "Fast-path raw JSON binary read for unfiltered REST responses."
  def get_snapshot_json(login) when is_binary(login) do
    if Ids.github_login?(login) and connected?(login) do
      get_snapshot_raw(login)
    else
      {:ok, nil}
    end
  end

  def get_snapshot(login) do
    if Ids.github_login?(login) and connected?(login) do
      case get_snapshot_raw(login) do
        {:ok, nil} ->
          {:ok, nil}

        {:ok, raw} ->
          case JSON.decode(raw) do
            {:ok, decoded} ->
              {:ok, decoded}

            {:error, _} ->
              with {:ok, fresh} when is_binary(fresh) <- read_through_pg(login, state_key(login)),
                   {:ok, decoded} <- JSON.decode(fresh) do
                {:ok, decoded}
              else
                {:ok, nil} -> {:ok, nil}
                _ -> {:error, :invalid_json}
              end
          end

        err ->
          err
      end
    else
      {:ok, nil}
    end
  end

  @doc "Batch-fetches decoded snapshots for multiple GitHub logins using one PG check + Redis MGET."
  def get_snapshots_batch([]), do: %{}

  def get_snapshots_batch(logins) do
    valid_logins = Enum.filter(logins, &Ids.github_login?/1) |> Enum.uniq()
    if valid_logins == [], do: %{}

    connected_logins =
      case Postgrex.query(
             :pg,
             "SELECT DISTINCT github_login FROM discord_github_links WHERE github_login = ANY($1::text[])",
             [valid_logins]
           ) do
        {:ok, %Postgrex.Result{rows: rows}} -> Enum.map(rows, &hd/1)
        _ -> []
      end

    if connected_logins == [] do
      %{}
    else
      keys = Enum.map(connected_logins, &state_key/1)

      cached =
        case Redix.command(:redix, ["MGET" | keys]) do
          {:ok, values} when is_list(values) -> Enum.zip(connected_logins, values)
          _ -> Enum.map(connected_logins, &{&1, nil})
        end

      Enum.reduce(cached, %{}, fn
        {login, raw}, acc when is_binary(raw) ->
          Metrics.inc(:cache_hits_total)

          case JSON.decode(raw) do
            {:ok, decoded} -> Map.put(acc, login, decoded)
            _ -> put_if_present(acc, login, get_snapshot(login))
          end

        {login, nil}, acc ->
          Metrics.inc(:cache_misses_total)
          put_if_present(acc, login, get_snapshot(login))
      end)
    end
  end

  defp put_if_present(acc, login, {:ok, %{} = map}), do: Map.put(acc, login, map)
  defp put_if_present(acc, _login, _), do: acc

  defp read_through_pg(login, redis_key) do
    Metrics.inc(:pg_fallbacks_total)

    case Postgrex.query(:pg, "SELECT data FROM snapshots WHERE login = $1", [login]) do
      {:ok, %Postgrex.Result{rows: [[data]]}} ->
        raw = JSON.encode!(data)
        if redis_key, do: _ = Redix.command(:redix, ["SET", redis_key, raw])
        {:ok, raw}

      {:ok, %Postgrex.Result{rows: []}} ->
        {:ok, nil}

      {:error, reason} ->
        {:error, reason}
    end
  end

  def upsert_user_from_oauth(%{id: id, login: login, name: name, avatar_url: avatar_url}) do
    login = String.downcase(login)
    if not Ids.github_login?(login), do: raise(ArgumentError, "invalid github login")

    Postgrex.query!(
      :pg,
      """
      INSERT INTO users (id, login, name, avatar_url)
      VALUES ($1, $2, $3, $4)
      ON CONFLICT (id) DO UPDATE
        SET login = EXCLUDED.login,
            name = COALESCE(EXCLUDED.name, users.name),
            avatar_url = EXCLUDED.avatar_url
      """,
      [id, login, name, avatar_url]
    )

    ensure_snapshot!(id, login, name, avatar_url)
    login
  end

  def ensure_snapshot!(id, login, name, avatar_url) do
    initial = %{
      "user" => %{
        "id" => id,
        "username" => login,
        "name" => name,
        "avatar_url" => avatar_url,
        "profile_url" => "https://github.com/" <> login
      },
      "last_active_at" => nil,
      "commits" => [],
      "pull_requests" => [],
      "issues" => [],
      "kv" => %{}
    }

    case Postgrex.query(
           :pg,
           "INSERT INTO snapshots (login, data) VALUES ($1, $2) ON CONFLICT (login) DO NOTHING",
           [login, initial]
         ) do
      {:ok, %Postgrex.Result{num_rows: 1}} ->
        raw = JSON.encode!(initial)
        _ = Redix.command(:redix, ["SET", state_key(login), raw])
        :ok

      _ ->
        :ok
    end
  end

  def rotate_api_key(login) do
    if Ids.github_login?(login) do
      token = "tggl_" <> (:crypto.strong_rand_bytes(32) |> Base.url_encode64(padding: false))
      hash = sha256_hex(token)

      case Postgrex.query(:pg, "UPDATE users SET api_key_hash = $1 WHERE login = $2", [
             hash,
             login
           ]) do
        {:ok, %Postgrex.Result{num_rows: 1}} -> {:ok, token}
        _ -> :not_found
      end
    else
      :not_found
    end
  end

  def valid_api_key?(login, token) when is_binary(token) do
    if Ids.github_login?(login) and GitHubAuth.valid_token_shape?(token) and connected?(login) do
      verify_api_key(login, token)
    else
      false
    end
  end

  def valid_api_key?(_, _), do: false

  defp verify_api_key(login, token) do
    case Postgrex.query(:pg, "SELECT api_key_hash FROM users WHERE login = $1", [login]) do
      {:ok, %Postgrex.Result{rows: [[stored]]}} when is_binary(stored) ->
        Plug.Crypto.secure_compare(stored, sha256_hex(token))

      _ ->
        false
    end
  end

  def put_kv(login, pairs) when is_map(pairs) do
    with true <- Ids.github_login?(login),
         :ok <- validate_kv_pairs(pairs) do
      Postgrex.transaction(:pg, fn conn ->
        Postgrex.query!(conn, "SELECT pg_advisory_xact_lock(hashtext('ghkv:' || $1))", [login])

        for {k, v} <- pairs do
          Postgrex.query!(
            conn,
            """
            INSERT INTO user_kv (login, key, value) VALUES ($1, $2, $3)
            ON CONFLICT (login, key) DO UPDATE SET value = EXCLUDED.value
            """,
            [login, k, to_string(v)]
          )
        end

        %Postgrex.Result{rows: [[total]]} =
          Postgrex.query!(conn, "SELECT count(*) FROM user_kv WHERE login = $1", [login])

        if total > @max_kv_pairs do
          Postgrex.rollback(conn, "too_many_keys")
        end

        collect_kv_snapshot!(conn, login)
      end)
      |> finalize_kv_publish(login)
    else
      false -> {:error, "invalid_login"}
      {:error, _} = err -> err
    end
  end

  def delete_kv(login, key) when is_binary(key) do
    with true <- Ids.github_login?(login),
         true <- Regex.match?(~r/^[a-zA-Z0-9._-]{1,64}$/, key) do
      Postgrex.transaction(:pg, fn conn ->
        Postgrex.query!(conn, "SELECT pg_advisory_xact_lock(hashtext('ghkv:' || $1))", [login])
        Postgrex.query!(conn, "DELETE FROM user_kv WHERE login = $1 AND key = $2", [login, key])
        collect_kv_snapshot!(conn, login)
      end)
      |> finalize_kv_publish(login)
    else
      false -> {:error, "invalid_key"}
    end
  end

  def delete_kv(_, _), do: {:error, "invalid_key"}

  defp collect_kv_snapshot!(conn, login) do
    %Postgrex.Result{rows: kv_rows} =
      Postgrex.query!(conn, "SELECT key, value FROM user_kv WHERE login = $1", [login])

    kv = Map.new(kv_rows, fn [k, v] -> {k, v} end)

    case Postgrex.query!(
           conn,
           """
           UPDATE snapshots
           SET data = jsonb_set(data, '{kv}', $2::jsonb, true), updated_at = now()
           WHERE login = $1
           RETURNING data
           """,
           [login, kv]
         ) do
      %Postgrex.Result{rows: [[snapshot]]} ->
        {:publish, kv, JSON.encode!(snapshot)}

      %Postgrex.Result{rows: []} ->
        {:noop, kv}
    end
  end

  defp finalize_kv_publish({:ok, {:publish, kv, raw}}, login) do
    _ = Redix.command(:redix, ["SET", state_key(login), raw])
    _ = Redix.command(:redix, ["PUBLISH", "taggle:events:" <> login, raw])
    {:ok, kv}
  end

  defp finalize_kv_publish({:ok, {:noop, kv}}, _login), do: {:ok, kv}
  defp finalize_kv_publish({:error, reason}, _login), do: {:error, reason}

  defp validate_kv_pairs(pairs) when map_size(pairs) > 50, do: {:error, "too_many_keys"}

  defp validate_kv_pairs(pairs) do
    Enum.reduce_while(pairs, :ok, fn {k, v}, :ok ->
      cond do
        not is_binary(k) or not Regex.match?(~r/^[a-zA-Z0-9._-]{1,64}$/, k) ->
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

  defp sha256_hex(s) do
    :crypto.hash(:sha256, s) |> Base.encode16(case: :lower)
  end
end
