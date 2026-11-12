defmodule Taggle.Gateway.GitHubAuth do
  @moduledoc """
  API key extraction and format validation for KV HTTP endpoints.
  """

  def extract_api_key(conn) do
    token =
      case Plug.Conn.get_req_header(conn, "authorization") do
        ["Bearer " <> t] -> String.trim(t)
        ["bearer " <> t] -> String.trim(t)
        [t] -> String.trim(t)
        _ -> nil
      end

    if valid_token_shape?(token), do: token, else: nil
  end

  def valid_token_shape?(token) when is_binary(token) do
    String.starts_with?(token, "tggl_") and byte_size(token) >= 16 and byte_size(token) <= 128 and
      not String.contains?(token, [" ", "\t", "\r", "\n"])
  end

  def valid_token_shape?(_), do: false
end
