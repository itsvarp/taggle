defmodule Taggle.Gateway.Metrics do
  @moduledoc """
  Lock-free ETS counters and Prometheus text exposition for Taggle Gateway.
  """

  @table :taggle_gateway_metrics

  @counters [
    :http_requests_total,
    :http_errors_4xx_total,
    :http_errors_5xx_total,
    :rate_limited_total,
    :ws_connections_active,
    :ws_connections_total,
    :ws_messages_in_total,
    :ws_messages_out_total,
    :cache_hits_total,
    :cache_misses_total,
    :pg_fallbacks_total,
    :fanout_dispatched_total,
    :fanout_skipped_total
  ]

  def init do
    if :ets.whereis(@table) == :undefined do
      :ets.new(@table, [
        :named_table,
        :public,
        :set,
        {:write_concurrency, true},
        {:read_concurrency, true}
      ])
    end

    for key <- @counters do
      :ets.insert_new(@table, {key, 0})
    end

    :ok
  end

  def inc(key, by \\ 1) when is_atom(key) and is_integer(by) do
    if :ets.whereis(@table) != :undefined do
      :ets.update_counter(@table, key, {2, by}, {key, 0})
    end

    :ok
  end

  def get(key) when is_atom(key) do
    case :ets.whereis(@table) do
      :undefined ->
        0

      _ ->
        case :ets.lookup(@table, key) do
          [{^key, val}] -> val
          _ -> 0
        end
    end
  end

  def render do
    [
      metric(
        "taggle_gateway_http_requests_total",
        "counter",
        "Total HTTP requests handled.",
        :http_requests_total
      ),
      metric(
        "taggle_gateway_http_errors_4xx_total",
        "counter",
        "Total HTTP 4xx responses.",
        :http_errors_4xx_total
      ),
      metric(
        "taggle_gateway_http_errors_5xx_total",
        "counter",
        "Total HTTP 5xx responses.",
        :http_errors_5xx_total
      ),
      metric(
        "taggle_gateway_rate_limited_total",
        "counter",
        "Total requests rejected with 429.",
        :rate_limited_total
      ),
      metric(
        "taggle_gateway_ws_connections_active",
        "gauge",
        "Currently active WebSocket connections.",
        :ws_connections_active
      ),
      metric(
        "taggle_gateway_ws_connections_total",
        "counter",
        "Total WebSocket connections accepted.",
        :ws_connections_total
      ),
      metric(
        "taggle_gateway_ws_messages_in_total",
        "counter",
        "Inbound WebSocket frames processed.",
        :ws_messages_in_total
      ),
      metric(
        "taggle_gateway_ws_messages_out_total",
        "counter",
        "Outbound WebSocket frames pushed.",
        :ws_messages_out_total
      ),
      metric(
        "taggle_gateway_cache_hits_total",
        "counter",
        "Redis state cache hits.",
        :cache_hits_total
      ),
      metric(
        "taggle_gateway_cache_misses_total",
        "counter",
        "Redis state cache misses.",
        :cache_misses_total
      ),
      metric(
        "taggle_gateway_pg_fallbacks_total",
        "counter",
        "Postgres read-through queries executed.",
        :pg_fallbacks_total
      ),
      metric(
        "taggle_gateway_fanout_dispatched_total",
        "counter",
        "Pub/sub events delivered to active subscribers.",
        :fanout_dispatched_total
      ),
      metric(
        "taggle_gateway_fanout_skipped_total",
        "counter",
        "Pub/sub events skipped with zero local subscribers.",
        :fanout_skipped_total
      )
    ]
    |> IO.iodata_to_binary()
  end

  defp metric(name, type, help, key) do
    [
      "# HELP ",
      name,
      " ",
      help,
      "\n",
      "# TYPE ",
      name,
      " ",
      type,
      "\n",
      name,
      " ",
      Integer.to_string(get(key)),
      "\n"
    ]
  end
end
