defmodule Taggle.Gateway.DiscordAuth do
  @moduledoc """
  Discord CDN avatar URL helper with strict avatar hash validation.
  """

  @avatar_re ~r/^[A-Za-z0-9_]{1,64}$/

  def valid_avatar?(avatar) when is_binary(avatar), do: Regex.match?(@avatar_re, avatar)
  def valid_avatar?(_), do: false

  def avatar_url(%{id: id, avatar: avatar}) when is_binary(avatar) and avatar != "" do
    if valid_avatar?(avatar) do
      "https://cdn.discordapp.com/avatars/#{id}/#{avatar}.png"
    else
      avatar_url(%{id: id})
    end
  end

  def avatar_url(%{id: id}) do
    index =
      case Integer.parse(to_string(id)) do
        {n, ""} -> rem(div(n, 4_194_304), 6)
        _ -> 0
      end

    "https://cdn.discordapp.com/embed/avatars/#{index}.png"
  end
end
