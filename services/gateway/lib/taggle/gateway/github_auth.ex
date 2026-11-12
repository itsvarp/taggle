defmodule Taggle.Gateway.GitHubAuth do
  @moduledoc """
  API key extraction for GitHub KV HTTP endpoints.
  """

  def extract_api_key(conn) do
    case Plug.Conn.get_req_header(conn, "authorization") do
      ["Bearer " <> token] -> String.trim(token)
      ["bearer " <> token] -> String.trim(token)
      [token] -> String.trim(token)
      _ -> nil
    end
  end
end
