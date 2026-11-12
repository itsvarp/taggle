defmodule Taggle.Gateway.ClientJs do
  @moduledoc """
  Serves the self-hosted first-party Taggle browser SDK & <taggle-card> Web Component.
  Reads `/sdk/taggle.js` at compile time so the release remains self-contained.
  """

  @sdk_path Path.expand("assets/taggle.js", __DIR__)
  @external_resource @sdk_path
  @script File.read!(@sdk_path)

  def body, do: @script
end
