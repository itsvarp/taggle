defmodule Taggle.Gateway.Socket do
  @moduledoc """
  Lanyard-compatible WebSocket protocol for Discord presences and GitHub activity.

    op 1  Hello        server -> client  %{heartbeat_interval: ms}
    op 2  Initialize   client -> server  %{subscribe_to_id | subscribe_to_ids | subscribe_to_all, filter?}
    op 3  Heartbeat    client -> server  (server answers op 3 ack)
    op 4  Unsubscribe  client -> server  %{unsubscribe_from_id | unsubscribe_from_ids | unsubscribe_from_all}
    op 0  Event        server -> client  t: INIT_STATE | PRESENCE_UPDATE | ACTIVITY_UPDATE

  Ids made only of digits are Discord snowflakes; everything else is a GitHub
  login. A single client can subscribe to both kinds at once.
  """
  @behaviour WebSock

  alias Taggle.Gateway.{DiscordStore, Filter, GitHubStore, Ids, Metrics}

  @heartbeat_ms 30_000
  @max_subs 20
  @init_cooldown_ms 500
  @msg_window_ms 10_000
  @msg_soft_limit 40
  @msg_hard_limit 100
  @all_key "dc:all"

  @impl true
  def init(_args) do
    Metrics.inc(:ws_connections_total)
    Metrics.inc(:ws_connections_active)
    Metrics.inc(:ws_messages_out_total)
    timer = Process.send_after(self(), :heartbeat_timeout, @heartbeat_ms * 2)

    {:push, {:text, encode(%{op: 1, d: %{heartbeat_interval: @heartbeat_ms}})},
     %{
       timer: timer,
       subs: MapSet.new(),
       filter: nil,
       last_init_at: nil,
       msg_window_start: System.monotonic_time(:millisecond),
       msg_count: 0
     }}
  end

  @impl true
  def handle_in({text, [opcode: :text]}, state) do
    Metrics.inc(:ws_messages_in_total)

    case check_inbound_rate(state) do
      {:hard_limit, new_state} ->
        {:stop, :normal, {4008, "rate limit exceeded"}, new_state}

      {:soft_limit, new_state} ->
        push_error(new_state, "rate_limited")

      {:ok, new_state} ->
        handle_text_frame(text, new_state)
    end
  end

  def handle_in(_, state), do: {:ok, state}

  defp handle_text_frame(text, state) do
    case JSON.decode(text) do
      {:ok, %{"op" => 3}} ->
        push_text(encode(%{op: 3}), reset_timer(state))

      {:ok, %{"op" => 2, "d" => d}} when is_map(d) ->
        subscribe(d, state)

      {:ok, %{"op" => 4, "d" => d}} when is_map(d) ->
        {:ok, unsubscribe(d, state)}

      {:ok, %{"op" => 2}} ->
        push_error(state, "requires_data_object")

      {:ok, %{"op" => 4}} ->
        push_error(state, "requires_data_object")

      {:ok, _} ->
        push_error(state, "unknown_opcode")

      _ ->
        push_error(state, "invalid_payload")
    end
  end

  @impl true
  def handle_info({:activity_update, login, payload}, %{subs: subs} = state)
      when is_binary(payload) do
    if MapSet.member?(subs, "gh:" <> login) do
      if state.filter do
        case JSON.decode(payload) do
          {:ok, decoded} ->
            data = Filter.apply(decoded, state.filter)
            push_text(encode(%{op: 0, t: "ACTIVITY_UPDATE", d: data}), state)

          {:error, _} ->
            {:ok, state}
        end
      else
        # `payload` is already serialized JSON from ingest/store; wrap without decode+encode.
        push_text("{\"op\":0,\"t\":\"ACTIVITY_UPDATE\",\"d\":" <> payload <> "}", state)
      end
    else
      {:ok, state}
    end
  end

  def handle_info({:discord_presence_update, payload}, state) when is_binary(payload) do
    push_text("{\"op\":0,\"t\":\"PRESENCE_UPDATE\",\"d\":" <> payload <> "}", state)
  end

  def handle_info(:heartbeat_timeout, state),
    do: {:stop, :normal, {4004, "heartbeat timeout"}, state}

  def handle_info(_, state), do: {:ok, state}

  @impl true
  def terminate(_reason, _state) do
    Metrics.inc(:ws_connections_active, -1)
    :ok
  end

  defp subscribe(d, state) do
    now_ms = System.monotonic_time(:millisecond)

    if is_integer(state.last_init_at) and now_ms - state.last_init_at < @init_cooldown_ms do
      push_error(state, "rate_limited")
    else
      do_subscribe(d, %{state | last_init_at: now_ms})
    end
  end

  defp do_subscribe(d, state) do
    all? = d["subscribe_to_all"] === true

    ids =
      case d do
        %{"subscribe_to_ids" => list} when is_list(list) -> list
        %{"subscribe_to_id" => id} when is_binary(id) -> [id]
        _ -> []
      end
      |> Enum.filter(&is_binary/1)
      |> Enum.map(&String.trim/1)
      |> Enum.reject(&(&1 == ""))
      |> Enum.uniq()
      |> Enum.take(@max_subs)

    {keys, subjects, invalid?} = classify(ids)

    cond do
      invalid? ->
        push_error(state, "invalid_payload")

      keys == [] and not all? ->
        push_error(state, "invalid_payload")

      true ->
        # Re-initializing replaces previous subscriptions.
        unregister_all(state.subs)

        keys = if all?, do: MapSet.put(keys, @all_key), else: keys
        for key <- keys, do: Registry.register(Taggle.Gateway.Subs, key, nil)

        filter =
          if is_map(d["filter"]),
            do: Map.take(d["filter"], ["types", "repos", "limit"]),
            else: nil

        init_state =
          if all? do
            case DiscordStore.list_presences() do
              {:ok, presences} -> presences
              _ -> %{}
            end
          else
            build_init_state(subjects, filter)
          end

        push_text(encode(%{op: 0, t: "INIT_STATE", d: init_state}), %{
          state
          | subs: keys,
            filter: filter
        })
    end
  end

  defp classify(ids) do
    Enum.reduce(ids, {MapSet.new(), [], false}, fn id, {keys, subjects, invalid?} ->
      case Ids.source(id) do
        :discord ->
          {MapSet.put(keys, "dc:" <> id), [{:discord, id} | subjects], invalid?}

        :github ->
          login = Ids.normalize_github(id)
          {MapSet.put(keys, "gh:" <> login), [{:github, login, id} | subjects], invalid?}

        :invalid ->
          {keys, subjects, true}
      end
    end)
    |> then(fn {keys, subjects, invalid?} -> {keys, Enum.reverse(subjects), invalid?} end)
  end

  defp build_init_state([single], filter) do
    case single do
      {:discord, id} ->
        case DiscordStore.get_presence(id) do
          {:ok, presence} -> presence
          _ -> nil
        end

      {:github, login, _raw} ->
        case GitHubStore.get_snapshot(login) do
          {:ok, snapshot} -> Filter.apply(snapshot, filter)
          _ -> nil
        end
    end
  end

  defp build_init_state(subjects, filter) do
    dc_ids = for {:discord, id} <- subjects, do: id
    gh_logins = for {:github, login, _raw} <- subjects, do: login

    presences = DiscordStore.get_presences_batch(dc_ids)
    snapshots = GitHubStore.get_snapshots_batch(gh_logins)

    Enum.reduce(subjects, %{}, fn
      {:discord, id}, acc ->
        case Map.get(presences, id) do
          nil -> acc
          p -> Map.put(acc, id, p)
        end

      {:github, login, raw}, acc ->
        case Map.get(snapshots, login) do
          nil -> acc
          s -> Map.put(acc, raw, Filter.apply(s, filter))
        end
    end)
  end

  defp unsubscribe(d, state) do
    all? = d["unsubscribe_from_all"] === true

    ids =
      case d do
        %{"unsubscribe_from_ids" => list} when is_list(list) -> list
        %{"unsubscribe_from_id" => id} when is_binary(id) -> [id]
        _ -> []
      end
      |> Enum.filter(&is_binary/1)
      |> Enum.map(&String.trim/1)

    keys =
      ids
      |> Enum.map(fn id ->
        case Ids.source(id) do
          :discord -> "dc:" <> id
          :github -> "gh:" <> Ids.normalize_github(id)
          :invalid -> nil
        end
      end)
      |> Enum.reject(&is_nil/1)
      |> MapSet.new()

    keys = if all?, do: MapSet.put(keys, @all_key), else: keys

    # If the client had only `subscribe_to_all`, unsubscribing from all removes it.
    keys =
      if all?, do: MapSet.union(keys, MapSet.filter(state.subs, &(&1 == @all_key))), else: keys

    for key <- keys, do: Registry.unregister(Taggle.Gateway.Subs, key)

    %{state | subs: MapSet.difference(state.subs, keys)}
  end

  defp unregister_all(subs) do
    for key <- subs, do: Registry.unregister(Taggle.Gateway.Subs, key)
  end

  defp push_error(state, code) do
    push_text(encode(%{op: 0, t: "ERROR", d: %{code: code}}), state)
  end

  defp push_text(frame, state) do
    Metrics.inc(:ws_messages_out_total)
    {:push, {:text, frame}, state}
  end

  defp check_inbound_rate(state) do
    now = System.monotonic_time(:millisecond)

    {window_start, count} =
      if now - state.msg_window_start >= @msg_window_ms do
        {now, 1}
      else
        {state.msg_window_start, state.msg_count + 1}
      end

    new_state = %{state | msg_window_start: window_start, msg_count: count}

    cond do
      count > @msg_hard_limit -> {:hard_limit, new_state}
      count > @msg_soft_limit -> {:soft_limit, new_state}
      true -> {:ok, new_state}
    end
  end

  defp reset_timer(%{timer: t} = state) do
    Process.cancel_timer(t)
    %{state | timer: Process.send_after(self(), :heartbeat_timeout, @heartbeat_ms * 2)}
  end

  defp encode(term), do: JSON.encode!(term)
end
