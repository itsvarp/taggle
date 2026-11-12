defmodule Taggle.Gateway.GitHubStore do
  @moduledoc """
  Redis hot-cache + PostgreSQL read-through store and KV management.
  """
  require Logger

  @doc "Fetches the serialized JSON snapshot from Redis, falling back to Postgres on miss."
  def get_snapshot_raw(login) when is_binary(login) do
    key = "taggle:state:" <> login

    case Redix.command(:redix, ["GET", key]) do
      {:ok, raw} when is_binary(raw) ->
        {:ok, raw}

      {:ok, nil} ->
        read_through_pg(login, key)

      {:error, reason} ->
        Logger.warning("redis read failed, falling back to pg: #{inspect(reason)}")
        read_through_pg(login, nil)
    end
  end

  def get_snapshot(login) do
    if Taggle.Gateway.Ids.github_login?(login) do
      case get_snapshot_raw(login) do
        {:ok, nil} -> {:ok, nil}
        {:ok, raw} -> {:ok, JSON.decode!(raw)}
        err -> err
      end
    else
      {:ok, nil}
    end
  end

  defp read_through_pg(login, redis_key) do
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

  def record_installation(installation_id, login) when is_integer(installation_id) do
    login = String.downcase(login)

    Postgrex.query!(
      :pg,
      """
      INSERT INTO installations (id, account_login)
      VALUES ($1, $2)
      ON CONFLICT (id) DO UPDATE SET account_login = EXCLUDED.account_login
      """,
      [installation_id, login]
    )
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
        _ = Redix.command(:redix, ["SET", "taggle:state:" <> login, raw])
        :ok

      _ ->
        :ok
    end
  end

  def user_overview(login) do
    user_row =
      Postgrex.query!(
        :pg,
        "SELECT id, name, avatar_url, api_key_hash IS NOT NULL FROM users WHERE login = $1",
        [login]
      )

    case user_row.rows do
      [[id, name, avatar_url, has_key]] ->
        %Postgrex.Result{rows: [[inst_count]]} =
          Postgrex.query!(
            :pg,
            "SELECT count(*) FROM installations WHERE account_login = $1",
            [login]
          )

        %Postgrex.Result{rows: kv_rows} =
          Postgrex.query!(
            :pg,
            "SELECT key, value FROM user_kv WHERE login = $1 ORDER BY key",
            [login]
          )

        snap =
          case get_snapshot(login) do
            {:ok, s} when is_map(s) -> s
            _ -> %{}
          end

        {:ok,
         %{
           id: id,
           login: login,
           name: name,
           avatar_url: avatar_url,
           has_api_key: has_key,
           installations: inst_count,
           kv: Map.new(kv_rows, fn [k, v] -> {k, v} end),
           commits: Map.get(snap, "commits", []),
           pull_requests: Map.get(snap, "pull_requests", []),
           issues: Map.get(snap, "issues", [])
         }}

      [] ->
        :not_found
    end
  end

  def rotate_api_key(login) do
    if Taggle.Gateway.Ids.github_login?(login) do
      token = "tggl_" <> (:crypto.strong_rand_bytes(32) |> Base.url_encode64(padding: false))
      hash = sha256_hex(token)

      case Postgrex.query!(:pg, "UPDATE users SET api_key_hash = $1 WHERE login = $2", [hash, login]) do
        %Postgrex.Result{num_rows: 1} -> {:ok, token}
        _ -> :not_found
      end
    else
      :not_found
    end
  end

  def valid_api_key?(login, token) when is_binary(token) and byte_size(token) > 0 do
    if Taggle.Gateway.Ids.github_login?(login) do
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
    with true <- Taggle.Gateway.Ids.github_login?(login),
         :ok <- validate_kv_pairs(pairs) do
      Postgrex.transaction(:pg, fn conn ->
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

        sync_kv_snapshot!(conn, login)
      end)
    else
      false -> {:error, "invalid_login"}
      {:error, _} = err -> err
    end
  end

  def delete_kv(login, key) when is_binary(key) do
    with true <- Taggle.Gateway.Ids.github_login?(login),
         true <- Regex.match?(~r/^[a-zA-Z0-9._-]{1,64}$/, key) do
      Postgrex.transaction(:pg, fn conn ->
        Postgrex.query!(conn, "DELETE FROM user_kv WHERE login = $1 AND key = $2", [login, key])
        sync_kv_snapshot!(conn, login)
      end)
    else
      false -> {:error, "invalid_key"}
    end
  end

  defp sync_kv_snapshot!(conn, login) do
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
        raw = JSON.encode!(snapshot)
        _ = Redix.command(:redix, ["SET", "taggle:state:" <> login, raw])
        _ = Redix.command(:redix, ["PUBLISH", "taggle:events:" <> login, raw])
        kv

      %Postgrex.Result{rows: []} ->
        kv
    end
  end

  defp validate_kv_pairs(pairs) when map_size(pairs) > 50, do: {:error, "too_many_keys"}

  defp validate_kv_pairs(pairs) do
    Enum.reduce_while(pairs, :ok, fn {k, v}, :ok ->
      cond do
        not is_binary(k) or not Regex.match?(~r/^[a-zA-Z0-9._-]{1,64}$/, k) ->
          {:halt, {:error, "invalid_key"}}

        not (is_binary(v) or is_number(v) or is_boolean(v)) ->
          {:halt, {:error, "invalid_value"}}

        byte_size(to_string(v)) > 30_000 ->
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
