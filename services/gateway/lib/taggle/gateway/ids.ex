defmodule Taggle.Gateway.Ids do
  @moduledoc """
  Validation and source detection for public identifiers.

  GitHub logins and Discord snowflakes share the `/v1/users/:id` route. A value
  made only of digits is treated as a Discord id (Lanyard compatibility); every
  other value is treated as a GitHub login. Explicit `/v1/github/...` and
  `/v1/discord/...` routes remove any ambiguity.
  """

  @github ~r/^[A-Za-z0-9](?:[A-Za-z0-9-]{0,37}[A-Za-z0-9])?$/
  @discord ~r/^[0-9]{1,20}$/

  def source(value) when is_binary(value) do
    cond do
      Regex.match?(@discord, value) -> if(discord_id?(value), do: :discord, else: :invalid)
      github_login?(value) -> :github
      true -> :invalid
    end
  end

  def source(_), do: :invalid

  def discord_id?(value) when is_binary(value) do
    value != "0" and Regex.match?(@discord, value) and match?({_, ""}, Integer.parse(value))
  end

  def discord_id?(_), do: false

  def github_login?(value) when is_binary(value), do: Regex.match?(@github, value)
  def github_login?(_), do: false

  def normalize_github(value) when is_binary(value), do: String.downcase(value)
  def normalize_github(_), do: ""
end
