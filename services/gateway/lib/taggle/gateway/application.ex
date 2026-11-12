defmodule Taggle.Gateway.Application do
  @moduledoc false
  use Application

  @impl true
  def start(_type, _args) do
    redis_url = System.get_env("REDIS_URL", "redis://localhost:6380")
    database_url = System.get_env("DATABASE_URL", "postgres://taggle:taggle@localhost:5433/taggle")
    port = String.to_integer(System.get_env("PORT", "4000"))

    children = [
      # One entry per WebSocket per subscribed login; used for fan-out.
      {Registry, keys: :duplicate, name: Taggle.Gateway.Subs, partitions: System.schedulers_online()},
      {Postgrex, [name: :pg, pool_size: 10] ++ parse_pg_url(database_url)},
      {Redix, {redis_url, [name: :redix]}},
      {Taggle.Gateway.GitHubFanout, redis_url: redis_url},
      {Taggle.Gateway.DiscordFanout, redis_url: redis_url},
      {Bandit, plug: Taggle.Gateway.Router, scheme: :http, port: port}
    ]

    Supervisor.start_link(children, strategy: :one_for_one, name: Taggle.Gateway.Supervisor)
  end

  defp parse_pg_url(url) do
    uri = URI.parse(url)
    [username, password] = String.split(uri.userinfo || "taggle:taggle", ":", parts: 2)

    [
      hostname: uri.host || "localhost",
      port: uri.port || 5432,
      username: username,
      password: password,
      database: String.trim_leading(uri.path || "/taggle", "/")
    ]
  end
end
