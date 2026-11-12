defmodule Taggle.Gateway.MixProject do
  use Mix.Project

  def project do
    [
      app: :taggle_gateway,
      version: "0.1.0",
      elixir: "~> 1.20",
      start_permanent: Mix.env() == :prod,
      deps: deps()
    ]
  end

  def application do
    [
      extra_applications: [:logger],
      mod: {Taggle.Gateway.Application, []}
    ]
  end

  # Bandit + Plug + WebSock is exactly the stack Phoenix runs on, so moving to
  # Phoenix later is additive (add :phoenix, mount this router/socket in an Endpoint).
  defp deps do
    [
      {:bandit, "~> 1.14"},
      {:plug, "~> 1.20"},
      {:websock_adapter, "~> 0.7"},
      {:redix, "~> 1.10"},
      {:postgrex, "~> 0.22"},
      {:req, "~> 0.7"}
    ]
  end
end
