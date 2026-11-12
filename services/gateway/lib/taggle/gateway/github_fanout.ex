defmodule Taggle.Gateway.GitHubFanout do
  @moduledoc """
  Listens to `taggle:events:*` on Redis (published by the Rust ingest service)
  and forwards each payload to every WebSocket subscribed to that login.
  """
  use GenServer
  require Logger

  @pattern "taggle:events:*"
  @prefix "taggle:events:"

  def start_link(opts), do: GenServer.start_link(__MODULE__, opts, name: __MODULE__)

  @impl true
  def init(opts) do
    {:ok, pubsub} = Redix.PubSub.start_link(opts[:redis_url], sync_connect: false)
    {:ok, ref} = Redix.PubSub.psubscribe(pubsub, @pattern, self())
    {:ok, %{pubsub: pubsub, ref: ref}}
  end

  @impl true
  def handle_info(
        {:redix_pubsub, _pid, _ref, :pmessage, %{channel: @prefix <> login, payload: payload}},
        state
      ) do
    with true <- Taggle.Gateway.Ids.github_login?(login),
         entries when entries != [] <- Registry.lookup(Taggle.Gateway.Subs, "gh:" <> login),
         true <- Taggle.Gateway.GitHubStore.connected?(login) do
      for {pid, _} <- entries, do: send(pid, {:activity_update, login, payload})
      Taggle.Gateway.Metrics.inc(:fanout_dispatched_total, length(entries))
    else
      _ ->
        Taggle.Gateway.Metrics.inc(:fanout_skipped_total)
    end

    {:noreply, state}
  end

  def handle_info({:redix_pubsub, _pid, _ref, type, _}, state)
      when type in [:psubscribed, :disconnected] do
    if type == :disconnected,
      do: Logger.warning("redis pubsub disconnected, redix will reconnect")

    {:noreply, state}
  end

  def handle_info(_msg, state), do: {:noreply, state}
end
