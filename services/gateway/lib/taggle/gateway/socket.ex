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

  alias Taggle.Gateway.{DiscordStore, Filter, GitHubStore, Ids}

  @heartbeat_ms 30_000
  @max_subs 20
  @all_key "dc:all"

  @impl true
  def init(_args) do
    timer = Process.send_after(self(), :heartbeat_timeout, @heartbeat_ms * 2)

    {:push, {:text, encode(%{op: 1, d: %{heartbeat_interval: @heartbeat_ms}})},
     %{timer: timer, subs: MapSet.new(), filter: nil}}
  end

  @impl true
  def handle_in({text, [opcode: :text]}, state) do
    case JSON.decode(text) do
      {:ok, %{"op" => 3}} ->
        {:push, {:text, encode(%{op: 3})}, reset_timer(state)}

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

  def handle_in(_, state), do: {:ok, state}

  @impl true
  def handle_info({:activity_update, login, payload}, %{subs: subs} = state) do
    if MapSet.member?(subs, "gh:" <> login) do
      data =
        if state.filter, do: payload |> JSON.decode!() |> Filter.apply(state.filter), else: JSON.decode!(payload)

      {:push, {:text, encode(%{op: 0, t: "ACTIVITY_UPDATE", d: data})}, state}
    else
      {:ok, state}
    end
  end

  def handle_info({:discord_presence_update, payload}, state) do
    {:push, {:text, encode(%{op: 0, t: "PRESENCE_UPDATE", d: JSON.decode!(payload)})}, state}
  end

  def handle_info(:heartbeat_timeout, state), do: {:stop, :normal, {4004, "heartbeat timeout"}, state}
  def handle_info(_, state), do: {:ok, state}

  @impl true
  def terminate(_reason, _state), do: :ok

  defp subscribe(d, state) do
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

        filter = if is_map(d["filter"]), do: d["filter"], else: nil

        init_state =
          if all? do
            case DiscordStore.list_presences() do
              {:ok, presences} -> presences
              _ -> %{}
            end
          else
            build_init_state(subjects, ids, filter)
          end

        {:push, {:text, encode(%{op: 0, t: "INIT_STATE", d: init_state})},
         %{state | subs: keys, filter: filter}}
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

  defp build_init_state(subjects, ids, filter) do
    single? = length(ids) == 1

    if single? do
      case hd(subjects) do
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
    else
      Map.new(subjects, fn
        {:discord, id} ->
          presence =
            case DiscordStore.get_presence(id) do
              {:ok, p} -> p
              _ -> nil
            end

          {id, presence}

        {:github, login, raw} ->
          snapshot =
            case GitHubStore.get_snapshot(login) do
              {:ok, s} -> Filter.apply(s, filter)
              _ -> nil
            end

          {raw, snapshot}
      end)
      |> Enum.reject(fn {_id, value} -> is_nil(value) end)
      |> Map.new()
    end
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
    keys = if all?, do: MapSet.union(keys, MapSet.filter(state.subs, &(&1 == @all_key))), else: keys

    for key <- keys, do: Registry.unregister(Taggle.Gateway.Subs, key)

    %{state | subs: MapSet.difference(state.subs, keys)}
  end

  defp unregister_all(subs) do
    for key <- subs, do: Registry.unregister(Taggle.Gateway.Subs, key)
  end

  defp push_error(state, code) do
    {:push, {:text, encode(%{op: 0, t: "ERROR", d: %{code: code}})}, state}
  end

  defp reset_timer(%{timer: t} = state) do
    Process.cancel_timer(t)
    %{state | timer: Process.send_after(self(), :heartbeat_timeout, @heartbeat_ms * 2)}
  end

  defp encode(term), do: JSON.encode!(term)
end
