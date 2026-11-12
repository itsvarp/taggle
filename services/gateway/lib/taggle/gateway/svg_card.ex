defmodule Taggle.Gateway.SvgCard do
  @moduledoc """
  Pure-vector, zero-external-request SVG card and badge renderer for Taggle.
  All text is control-character-stripped and XML-escaped to prevent SVG XSS.
  """

  @themes %{
    "dark" => %{
      bg: "#0d1117",
      panel: "#161b22",
      border: "#30363d",
      text: "#f0f6fc",
      muted: "#8b949e",
      accent: "#58a6ff"
    },
    "light" => %{
      bg: "#ffffff",
      panel: "#f6f8fa",
      border: "#d0d7de",
      text: "#1f2328",
      muted: "#656d76",
      accent: "#0969da"
    },
    "midnight" => %{
      bg: "#090d16",
      panel: "#111827",
      border: "#1e293b",
      text: "#f8fafc",
      muted: "#94a3b8",
      accent: "#38bdf8"
    },
    "catppuccin" => %{
      bg: "#1e1e2e",
      panel: "#181825",
      border: "#45475a",
      text: "#cdd6f4",
      muted: "#a6adc8",
      accent: "#cba6f7"
    }
  }

  @status_colors %{
    "online" => "#23a55a",
    "idle" => "#f0b232",
    "dnd" => "#f23f43",
    "offline" => "#80848e"
  }

  @hex_re ~r/^[0-9a-fA-F]{6}$/

  @doc """
  Renders a rich pure-vector status card SVG (`440x168` or `440x120` when compact).
  """
  def render_card(profile, opts \\ %{}) do
    theme = resolve_theme(opts)
    compact? = truthy?(opts["compact"])
    hide_github? = truthy?(opts["hide_github"])

    {discord, github} = unpack_profile(profile)
    info = extract_summary(discord, github, hide_github?)

    height = if compact?, do: 118, else: 168
    initials = avatar_initials(info.title)
    status_color = Map.get(@status_colors, info.status, @status_colors["offline"])

    github_section =
      if compact? do
        ""
      else
        """
        <rect x="16" y="102" width="408" height="50" rx="8" fill="#{theme.panel}" stroke="#{theme.border}" stroke-width="1"/>
        <circle cx="30" cy="120" r="4" fill="#{theme.accent}"/>
        <text x="42" y="124" font-family="ui-sans-serif, system-ui, -apple-system, sans-serif" font-size="12" font-weight="600" fill="#{theme.text}">#{escape(info.bottom_title, 48)}</text>
        <text x="30" y="141" font-family="ui-monospace, SFMono-Regular, Menlo, monospace" font-size="11" fill="#{theme.muted}">#{escape(info.bottom_sub, 56)}</text>
        """
      end

    """
    <svg xmlns="http://www.w3.org/2000/svg" width="440" height="#{height}" viewBox="0 0 440 #{height}" role="img" aria-label="#{escape(info.title, 40)} status card">
      <rect x="0.5" y="0.5" width="439" height="#{height - 1}" rx="12" fill="#{theme.bg}" stroke="#{theme.border}" stroke-width="1"/>
      <rect x="16" y="18" width="48" height="48" rx="12" fill="#{theme.panel}" stroke="#{theme.accent}" stroke-width="1.5"/>
      <text x="40" y="48" text-anchor="middle" font-family="ui-sans-serif, system-ui, -apple-system, sans-serif" font-size="18" font-weight="700" fill="#{theme.accent}">#{escape(initials, 2)}</text>
      <circle cx="60" cy="62" r="7" fill="#{theme.bg}"/>
      <circle cx="60" cy="62" r="5" fill="#{status_color}"/>
      <text x="78" y="36" font-family="ui-sans-serif, system-ui, -apple-system, sans-serif" font-size="16" font-weight="700" fill="#{theme.text}">#{escape(info.title, 28)}</text>
      <text x="78" y="54" font-family="ui-sans-serif, system-ui, -apple-system, sans-serif" font-size="12" fill="#{theme.muted}">#{escape(info.subtitle, 44)}</text>
      <rect x="338" y="20" width="86" height="22" rx="11" fill="#{theme.panel}" stroke="#{theme.border}" stroke-width="1"/>
      <circle cx="352" cy="31" r="4" fill="#{status_color}"/>
      <text x="362" y="35" font-family="ui-sans-serif, system-ui, -apple-system, sans-serif" font-size="11" font-weight="600" fill="#{theme.text}">#{escape(info.status_label, 10)}</text>
      <text x="16" y="88" font-family="ui-sans-serif, system-ui, -apple-system, sans-serif" font-size="12.5" font-weight="500" fill="#{theme.text}">#{escape(info.primary_line, 56)}</text>
      #{github_section}
    </svg>
    """
    |> String.trim()
  end

  @doc """
  Renders a compact pure-vector status badge SVG pill (`height=28`).
  """
  def render_badge(profile, opts \\ %{}) do
    theme = resolve_theme(opts)
    {discord, github} = unpack_profile(profile)
    info = extract_summary(discord, github, false)
    status_color = Map.get(@status_colors, info.status, @status_colors["offline"])

    label = escape(info.badge_label, 24)
    value = escape(info.badge_value, 40)

    label_w = max(64, String.length(info.badge_label) * 7 + 26)
    value_w = max(72, String.length(info.badge_value) * 6 + 22)
    total_w = label_w + value_w

    """
    <svg xmlns="http://www.w3.org/2000/svg" width="#{total_w}" height="28" viewBox="0 0 #{total_w} 28" role="img" aria-label="#{label}: #{value}">
      <rect x="0.5" y="0.5" width="#{total_w - 1}" height="27" rx="6" fill="#{theme.bg}" stroke="#{theme.border}" stroke-width="1"/>
      <path d="M6 1 H#{label_w} V27 H6 A5 5 0 0 1 1 22 V6 A5 5 0 0 1 6 1 Z" fill="#{theme.panel}"/>
      <circle cx="14" cy="14" r="4" fill="#{status_color}"/>
      <text x="24" y="18" font-family="ui-sans-serif, system-ui, -apple-system, sans-serif" font-size="11" font-weight="600" fill="#{theme.text}">#{label}</text>
      <text x="#{label_w + 10}" y="18" font-family="ui-monospace, SFMono-Regular, Menlo, monospace" font-size="11" fill="#{theme.accent}">#{value}</text>
    </svg>
    """
    |> String.trim()
  end

  defp resolve_theme(opts) do
    theme_name = opts["theme"] || "dark"
    base = Map.get(@themes, theme_name, @themes["dark"])

    accent =
      case opts["accent"] do
        "#" <> hex ->
          if Regex.match?(@hex_re, hex), do: "#" <> String.downcase(hex), else: base.accent

        hex when is_binary(hex) ->
          if Regex.match?(@hex_re, hex), do: "#" <> String.downcase(hex), else: base.accent

        _ ->
          base.accent
      end

    %{base | accent: accent}
  end

  defp unpack_profile(%{discord: d, github: g}), do: {d, g}
  defp unpack_profile(%{"discord_user" => _} = d), do: {d, nil}
  defp unpack_profile(%{"user" => _} = g), do: {nil, g}
  defp unpack_profile(_), do: {nil, nil}

  defp extract_summary(discord, github, hide_github?) do
    user = get_in(discord || %{}, ["discord_user"]) || %{}
    gh_user = get_in(github || %{}, ["user", "login"])

    title =
      user["global_name"] ||
        user["username"] ||
        gh_user ||
        "Taggle User"

    handle_parts =
      [
        if(user["username"], do: "@" <> to_string(user["username"])),
        if(gh_user && !hide_github?, do: "gh/" <> to_string(gh_user))
      ]
      |> Enum.reject(&is_nil/1)

    subtitle =
      case handle_parts do
        [] -> "Live Developer Telemetry"
        parts -> Enum.join(parts, "  ·  ")
      end

    status = to_string((discord && discord["discord_status"]) || "offline")
    status = if Map.has_key?(@status_colors, status), do: status, else: "offline"
    status_label = String.capitalize(status)

    primary_line = pick_primary_activity(discord, github)
    {bottom_title, bottom_sub} = pick_bottom_line(github, discord, hide_github?)

    badge_label = user["username"] || gh_user || "taggle"
    badge_value = pick_badge_value(discord, github, status_label)

    %{
      title: title,
      subtitle: subtitle,
      status: status,
      status_label: status_label,
      primary_line: primary_line,
      bottom_title: bottom_title,
      bottom_sub: bottom_sub,
      badge_label: badge_label,
      badge_value: badge_value
    }
  end

  defp pick_primary_activity(discord, github) do
    cond do
      coding = discord && discord["coding"] ->
        editor = coding["editor"] || "IDE"
        file = coding["file"] || coding["workspace"] || "coding"
        "Coding in #{editor}: #{file}"

      discord && discord["listening_to_spotify"] == true && is_map(discord["spotify"]) ->
        song = discord["spotify"]["song"] || "Music"
        artist = discord["spotify"]["artist"] || "Spotify"
        "Listening: #{song} - #{artist}"

      is_list(discord && discord["activities"]) and discord["activities"] != [] ->
        act = hd(discord["activities"])
        name = act["name"] || "Active"
        details = act["details"] || act["state"]
        if details, do: "#{name}: #{details}", else: "Activity: #{name}"

      is_list(github && github["commits"]) and github["commits"] != [] ->
        c = hd(github["commits"])
        "Latest commit in #{c["repo"]}: #{c["message"]}"

      true ->
        "No active presence right now"
    end
  end

  defp pick_bottom_line(github, _discord, false) when is_map(github) do
    summary = github["summary"] || %{}
    commits = github["commits"] || []
    prs = github["pull_requests"] || []

    cond do
      commits != [] ->
        c = hd(commits)
        {"GitHub · #{c["repo"]}", "#{c["sha"] || "commit"}: #{c["message"]}"}

      prs != [] ->
        pr = hd(prs)
        {"GitHub PR · #{pr["repo"]} ##{pr["number"]}", pr["title"] || "Pull Request"}

      true ->
        total = summary["total_activities"] || 0
        c24 = summary["commits_24h"] || 0
        {"GitHub Activity", "#{total} cached events · #{c24} commits (24h)"}
    end
  end

  defp pick_bottom_line(_github, discord, _hide) do
    kv_count =
      case discord && discord["kv"] do
        kv when is_map(kv) -> map_size(kv)
        _ -> 0
      end

    platform =
      cond do
        discord && discord["active_on_discord_desktop"] -> "Desktop"
        discord && discord["active_on_discord_web"] -> "Web"
        discord && discord["active_on_discord_mobile"] -> "Mobile"
        true -> "Idle"
      end

    {"Taggle Self-Hosted Presence", "Client: #{platform} · #{kv_count} KV entries"}
  end

  defp pick_badge_value(discord, github, status_label) do
    cond do
      coding = discord && discord["coding"] ->
        file = coding["file"] || coding["workspace"] || "coding"
        "#{coding["editor"] || "IDE"}: #{file}"

      discord && discord["listening_to_spotify"] == true && is_map(discord["spotify"]) ->
        "♫ #{discord["spotify"]["song"] || "Spotify"}"

      is_list(github && github["commits"]) and github["commits"] != [] ->
        c = hd(github["commits"])
        "#{c["repo"]} @ #{c["sha"] || "head"}"

      true ->
        status_label
    end
  end

  defp avatar_initials(name) do
    name
    |> to_string()
    |> clean_controls()
    |> String.trim()
    |> String.split(~r/[\s_-]+/, trim: true)
    |> case do
      [] -> "TG"
      [one] -> String.slice(one, 0, 2) |> String.upcase()
      [a, b | _] -> String.upcase(String.slice(a, 0, 1) <> String.slice(b, 0, 1))
    end
  end

  defp escape(value, max_len) do
    value
    |> to_string()
    |> clean_controls()
    |> truncate(max_len)
    |> String.replace("&", "&amp;")
    |> String.replace("<", "&lt;")
    |> String.replace(">", "&gt;")
    |> String.replace("\"", "&quot;")
    |> String.replace("'", "&#39;")
  end

  defp clean_controls(str) do
    str
    |> String.replace(~r/[\x00-\x1F\x7F-\x9F]/u, " ")
    |> String.replace(~r/\s+/u, " ")
    |> String.trim()
  end

  defp truncate(str, max_len) do
    if String.length(str) > max_len do
      String.slice(str, 0, max_len - 1) <> "…"
    else
      str
    end
  end

  defp truthy?(v) when v in ["1", "true", "yes", true], do: true
  defp truthy?(_), do: false
end
