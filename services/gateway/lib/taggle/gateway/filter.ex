defmodule Taggle.Gateway.Filter do
  @moduledoc "Applies `?types=`, `?repos=` and `?limit=` (or WS `filter`) to a state snapshot."

  @list_keys ~w(commits pull_requests issues)

  def apply(nil, _params), do: nil
  def apply(data, nil), do: data
  def apply(data, params) when map_size(params) == 0, do: data

  def apply(data, params) when is_map(data) and is_map(params) do
    types = to_set(params["types"])
    repos = to_set(params["repos"])
    limit = parse_limit(params["limit"])

    Enum.reduce(@list_keys, data, fn key, acc ->
      cond do
        MapSet.size(types) > 0 and not MapSet.member?(types, key) ->
          Map.put(acc, key, [])

        is_list(acc[key]) ->
          list =
            acc[key]
            |> Enum.filter(&(MapSet.size(repos) == 0 or MapSet.member?(repos, &1["repo"])))
            |> then(&if(limit, do: Enum.take(&1, limit), else: &1))

          Map.put(acc, key, list)

        true ->
          acc
      end
    end)
  end

  defp to_set(nil), do: MapSet.new()

  defp to_set(list) when is_list(list) do
    list
    |> Enum.filter(&is_binary/1)
    |> Enum.map(&String.trim/1)
    |> Enum.reject(&(&1 == ""))
    |> Enum.take(25)
    |> MapSet.new()
  end

  defp to_set(s) when is_binary(s) do
    s
    |> String.split(",", trim: true)
    |> Enum.map(&String.trim/1)
    |> Enum.reject(&(&1 == ""))
    |> Enum.take(25)
    |> MapSet.new()
  end

  defp to_set(_), do: MapSet.new()

  defp parse_limit(n) when is_integer(n) and n > 0, do: min(n, 100)

  defp parse_limit(s) when is_binary(s) do
    case Integer.parse(s) do
      {n, ""} when n > 0 -> min(n, 100)
      _ -> nil
    end
  end

  defp parse_limit(_), do: nil
end
