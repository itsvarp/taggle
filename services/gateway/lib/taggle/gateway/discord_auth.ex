defmodule Taggle.Gateway.DiscordAuth do
  @moduledoc """
  Discord CDN avatar URL helper.
  """

  def avatar_url(%{id: id, avatar: avatar}) when is_binary(avatar) and avatar != "" do
    "https://cdn.discordapp.com/avatars/#{id}/#{avatar}.png"
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
