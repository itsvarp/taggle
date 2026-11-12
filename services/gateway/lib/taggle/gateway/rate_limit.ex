defmodule Taggle.Gateway.RateLimit do
  @moduledoc """
  Built-in fixed-window rate limiter backed by Redis (for multi-node consistency)
  with an automatic local ETS fallback when Redis is unavailable.
  """

  import Plug.Conn
  alias Taggle.Gateway.Metrics

  @ets_table :taggle_gateway_rate_limits

  @redis_lua """
  local current = redis.call('INCR', KEYS[1])
  if current == 1 then
    redis.call('PEXPIRE', KEYS[1], ARGV[1])
  end
  local ttl = redis.call('PTTL', KEYS[1])
  if ttl < 0 then
    redis.call('PEXPIRE', KEYS[1], ARGV[1])
    ttl = tonumber(ARGV[1])
  end
  return {current, ttl}
  """

  def init do
    if :ets.whereis(@ets_table) == :undefined do
      :ets.new(@ets_table, [
        :named_table,
        :public,
        :set,
        {:write_concurrency, true},
        {:read_concurrency, true}
      ])
    end

    :ok
  end

  def limit_rpm(env_key, default) do
    case System.get_env(env_key) do
      nil ->
        default

      "" ->
        default

      raw ->
        case Integer.parse(raw) do
          {n, ""} when n > 0 -> n
          _ -> default
        end
    end
  end

  def check(conn, bucket, limit, window_secs \\ 60) do
    if System.get_env("RATE_LIMIT_DISABLED") == "true" do
      {:ok, conn}
    else
      ip = client_ip(conn)
      do_check(conn, "#{bucket}:#{ip}", limit, window_secs)
    end
  end

  def check_key(bucket, key, limit, window_secs \\ 60) do
    window_ms = window_secs * 1000
    bucket_id = div(System.system_time(:millisecond), window_ms)
    redis_key = "taggle:rl:#{bucket}:#{key}:#{bucket_id}"

    case redis_incr(redis_key, window_ms) do
      {:ok, count, ttl_ms} ->
        evaluate(count, limit, ttl_ms)

      :fallback ->
        ets_incr({bucket, key, bucket_id}, limit, window_ms)
    end
  end

  defp do_check(conn, key, limit, window_secs) do
    case check_key("http", key, limit, window_secs) do
      {:ok, remaining, reset_secs} ->
        conn =
          conn
          |> put_resp_header("x-ratelimit-limit", Integer.to_string(limit))
          |> put_resp_header("x-ratelimit-remaining", Integer.to_string(remaining))
          |> put_resp_header("x-ratelimit-reset", Integer.to_string(reset_secs))

        {:ok, conn}

      {:error, :rate_limited, reset_secs} ->
        Metrics.inc(:rate_limited_total)

        conn =
          conn
          |> put_resp_header("x-ratelimit-limit", Integer.to_string(limit))
          |> put_resp_header("x-ratelimit-remaining", "0")
          |> put_resp_header("x-ratelimit-reset", Integer.to_string(reset_secs))
          |> put_resp_header("retry-after", Integer.to_string(max(reset_secs, 1)))

        {:error, :rate_limited, conn}
    end
  end

  defp redis_incr(key, window_ms) do
    case Redix.command(:redix, ["EVAL", @redis_lua, "1", key, Integer.to_string(window_ms)]) do
      {:ok, [count, ttl_ms]} when is_integer(count) and is_integer(ttl_ms) ->
        {:ok, count, max(ttl_ms, 1)}

      _ ->
        :fallback
    end
  rescue
    _ -> :fallback
  catch
    :exit, _ -> :fallback
  end

  defp ets_incr(ets_key, limit, window_ms) do
    init()

    if :ets.info(@ets_table, :size) > 20_000 do
      :ets.delete_all_objects(@ets_table)
    end

    count = :ets.update_counter(@ets_table, ets_key, {2, 1}, {ets_key, 0})
    now = System.system_time(:millisecond)
    ttl_ms = window_ms - rem(now, window_ms)
    evaluate(count, limit, ttl_ms)
  end

  defp evaluate(count, limit, ttl_ms) do
    reset_secs = max(div(ttl_ms + 999, 1000), 1)

    if count <= limit do
      {:ok, max(limit - count, 0), reset_secs}
    else
      {:error, :rate_limited, reset_secs}
    end
  end

  def client_ip(conn) do
    if System.get_env("TRUST_PROXY") == "true" do
      case get_req_header(conn, "x-forwarded-for") do
        [header | _] ->
          header
          |> String.split(",", parts: 2)
          |> hd()
          |> String.trim()
          |> sanitize_ip()
          |> Kernel.||(peer_ip(conn))

        _ ->
          peer_ip(conn)
      end
    else
      peer_ip(conn)
    end
  end

  defp peer_ip(%{remote_ip: ip}) when is_tuple(ip), do: ip |> :inet.ntoa() |> to_string()
  defp peer_ip(_), do: "unknown"

  defp sanitize_ip(raw) when byte_size(raw) > 0 and byte_size(raw) <= 64 do
    case :inet.parse_address(String.to_charlist(raw)) do
      {:ok, _} -> raw
      _ -> nil
    end
  end

  defp sanitize_ip(_), do: nil
end
