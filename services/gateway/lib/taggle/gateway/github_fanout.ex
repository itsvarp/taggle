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
  def handle_info({:redix_pubsub, _pid, _ref, :pmessage, %{channel: @prefix <> login, payload: payload}}, state) do
    if Taggle.Gateway.GitHubStore.connected?(login) do
      Registry.dispatch(Taggle.Gateway.Subs, "gh:" <> login, fn entries ->
        for {pid, _} <- entries, do: send(pid, {:activity_update, login, payload})
      end)
    end

    {:noreply, state}
  end

  def handle_info({:redix_pubsub, _pid, _ref, type, _}, state)
      when type in [:psubscribed, :disconnected] do
    if type == :disconnected, do: Logger.warning("redis pubsub disconnected, redix will reconnect")
    {:noreply, state}
  end

  def handle_info(_msg, state), do: {:noreply, state}
end
