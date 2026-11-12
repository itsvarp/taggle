defmodule Taggle.Gateway.DiscordFanout do
  @moduledoc """
  Listens to `taggle:discord:events:*` on Redis and forwards each presence to
  WebSocket clients subscribed to that Discord id or to every presence.
  """
  use GenServer
  require Logger

  @pattern "taggle:discord:events:*"
  @prefix "taggle:discord:events:"

  def start_link(opts), do: GenServer.start_link(__MODULE__, opts, name: __MODULE__)

  @impl true
  def init(opts) do
    {:ok, pubsub} = Redix.PubSub.start_link(opts[:redis_url], sync_connect: false)
    {:ok, ref} = Redix.PubSub.psubscribe(pubsub, @pattern, self())
    {:ok, %{pubsub: pubsub, ref: ref}}
  end

  @impl true
  def handle_info(
        {:redix_pubsub, _pid, _ref, :pmessage, %{channel: @prefix <> user_id, payload: payload}},
        state
      ) do
    entries =
      Registry.lookup(Taggle.Gateway.Subs, "dc:" <> user_id) ++
        Registry.lookup(Taggle.Gateway.Subs, "dc:all")

    unique_entries = Enum.uniq_by(entries, fn {pid, _} -> pid end)

    if unique_entries == [] do
      Taggle.Gateway.Metrics.inc(:fanout_skipped_total)
    else
      Enum.each(unique_entries, fn {pid, _} -> send(pid, {:discord_presence_update, payload}) end)
      Taggle.Gateway.Metrics.inc(:fanout_dispatched_total, length(unique_entries))
    end

    {:noreply, state}
  end

  def handle_info({:redix_pubsub, _pid, _ref, type, _}, state)
      when type in [:psubscribed, :disconnected] do
    if type == :disconnected,
      do: Logger.warning("redis discord pubsub disconnected, redix will reconnect")

    {:noreply, state}
  end

  def handle_info(_msg, state), do: {:noreply, state}
end
