defmodule Taggle.Gateway.SvgCard do
  @moduledoc """
  Minimalist SVG card and badge renderer for Taggle.
  Embeds cached inline `data:image/*` avatars when available (falling back to a
  crisp monogram), and strictly sanitizes + XML-escapes all user text.
  """

  alias Taggle.Gateway.AvatarCache

  @themes %{
    "dark" => %{
      bg: "#0b0d11",
      panel: "#141820",
      border: "#232834",
      text: "#f3f4f6",
      muted: "#8b949e",
      accent: "#60a5fa"
    },
    "light" => %{
      bg: "#ffffff",
      panel: "#f8fafc",
      border: "#e2e8f0",
      text: "#0f172a",
      muted: "#64748b",
      accent: "#2563eb"
    },
    "midnight" => %{
      bg: "#06080f",
      panel: "#0f1524",
      border: "#1e293b",
      text: "#f8fafc",
      muted: "#94a3b8",
      accent: "#38bdf8"
    },
    "catppuccin" => %{
      bg: "#1e1e2e",
      panel: "#181825",
      border: "#313244",
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
  Renders a minimalist status card SVG (`400x142`, or `400x96` when `compact=true`).
  """
  def render_card(profile, opts \\ %{}) do
    theme = resolve_theme(opts)
    compact? = truthy?(opts["compact"])
    hide_github? = truthy?(opts["hide_github"])

    {discord, github} = unpack_profile(profile)
    info = extract_summary(discord, github, hide_github?)
    avatar_uri = resolve_avatar(profile, opts)

    height = if compact?, do: 96, else: 142
    status_color = Map.get(@status_colors, info.status, @status_colors["offline"])
    avatar_markup = render_card_avatar(avatar_uri, info.title, theme)
    activity_icon = render_activity_icon(info.mode, theme.accent, status_color)

    footer_markup =
      if compact? do
        ""
      else
        """
        <line x1="18" y1="92" x2="382" y2="92" stroke="#{theme.border}" stroke-width="1" stroke-opacity="0.8"/>
        <circle cx="22" cy="110" r="3" fill="#{theme.accent}"/>
        <text x="32" y="114" font-family="ui-sans-serif, system-ui, -apple-system, sans-serif" font-size="11.5" font-weight="600" fill="#{theme.text}">#{escape(info.bottom_title, 46)}</text>
        <text x="18" y="130" font-family="ui-monospace, SFMono-Regular, Menlo, monospace" font-size="11" fill="#{theme.muted}">#{escape(info.bottom_sub, 54)}</text>
        """
      end

    """
    <svg xmlns="http://www.w3.org/2000/svg" width="400" height="#{height}" viewBox="0 0 400 #{height}" role="img" aria-label="#{escape(info.title, 40)} status card">
      <defs>
        <clipPath id="taggle-card-av"><rect x="18" y="18" width="56" height="56" rx="14"/></clipPath>
      </defs>
      <rect x="0.5" y="0.5" width="399" height="#{height - 1}" rx="12" fill="#{theme.bg}" stroke="#{theme.border}" stroke-width="1"/>
      #{avatar_markup}
      <circle cx="71" cy="71" r="7" fill="#{theme.bg}"/>
      <circle cx="71" cy="71" r="4.5" fill="#{status_color}"/>
      <text x="86" y="34" font-family="ui-sans-serif, system-ui, -apple-system, sans-serif" font-size="15.5" font-weight="600" fill="#{theme.text}">#{escape(info.title, 24)}</text>
      <text x="382" y="34" text-anchor="end" font-family="ui-monospace, SFMono-Regular, Menlo, monospace" font-size="11" fill="#{theme.muted}">#{escape(info.status, 10)}</text>
      <text x="86" y="51" font-family="ui-sans-serif, system-ui, -apple-system, sans-serif" font-size="11.5" fill="#{theme.muted}">#{escape(info.subtitle, 40)}</text>
      #{activity_icon}
      <text x="102" y="71" font-family="ui-sans-serif, system-ui, -apple-system, sans-serif" font-size="12.5" font-weight="500" fill="#{theme.text}">#{escape(info.primary_line, 44)}</text>
      #{footer_markup}
    </svg>
    """
    |> String.trim()
  end

  @doc """
  Renders a compact minimalist badge SVG pill (`height=30`) with avatar + status + activity.
  """
  def render_badge(profile, opts \\ %{}) do
    theme = resolve_theme(opts)
    {discord, github} = unpack_profile(profile)
    info = extract_summary(discord, github, false)
    avatar_uri = resolve_avatar(profile, opts)
    status_color = Map.get(@status_colors, info.status, @status_colors["offline"])

    raw_label = truncate(clean_controls(info.badge_label), 20)
    raw_value = truncate(clean_controls(info.badge_value), 36)
    label = escape(raw_label, 20)
    value = escape(raw_value, 36)

    label_x = 34
    label_w = approx_px_width(raw_label, 6.7)
    sep_x = label_x + label_w + 10
    value_x = sep_x + 10
    value_w = approx_px_width(raw_value, 6.4)
    total_w = max(148, value_x + value_w + 14)

    avatar_markup = render_badge_avatar(avatar_uri, info.title, theme)

    """
    <svg xmlns="http://www.w3.org/2000/svg" width="#{total_w}" height="30" viewBox="0 0 #{total_w} 30" role="img" aria-label="#{label}: #{value}">
      <defs>
        <clipPath id="taggle-badge-av"><rect x="5" y="5" width="20" height="20" rx="6"/></clipPath>
      </defs>
      <rect x="0.5" y="0.5" width="#{total_w - 1}" height="29" rx="9" fill="#{theme.bg}" stroke="#{theme.border}" stroke-width="1"/>
      #{avatar_markup}
      <circle cx="24" cy="24" r="4" fill="#{theme.bg}"/>
      <circle cx="24" cy="24" r="2.7" fill="#{status_color}"/>
      <text x="#{label_x}" y="19" font-family="ui-sans-serif, system-ui, -apple-system, sans-serif" font-size="11.5" font-weight="600" fill="#{theme.text}">#{label}</text>
      <line x1="#{sep_x}" y1="8" x2="#{sep_x}" y2="22" stroke="#{theme.border}" stroke-width="1"/>
      <text x="#{value_x}" y="19" font-family="ui-monospace, SFMono-Regular, Menlo, monospace" font-size="11" fill="#{theme.accent}">#{value}</text>
    </svg>
    """
    |> String.trim()
  end

  defp resolve_avatar(profile, opts) do
    cond do
      opts["avatar"] == "false" ->
        nil

      AvatarCache.valid_data_uri?(opts["avatar_uri"]) ->
        opts["avatar_uri"]

      true ->
        AvatarCache.for_profile(profile)
    end
  end

  defp render_card_avatar(uri, _title, theme) when is_binary(uri) do
    """
    <rect x="18" y="18" width="56" height="56" rx="14" fill="#{theme.panel}"/>
    <image href="#{uri}" x="18" y="18" width="56" height="56" preserveAspectRatio="xMidYMid slice" clip-path="url(#taggle-card-av)"/>
    <rect x="18" y="18" width="56" height="56" rx="14" fill="none" stroke="#{theme.border}" stroke-width="1"/>
    """
    |> String.trim()
  end

  defp render_card_avatar(nil, title, theme) do
    initials = escape(avatar_initials(title), 2)

    """
    <rect x="18" y="18" width="56" height="56" rx="14" fill="#{theme.panel}" stroke="#{theme.border}" stroke-width="1"/>
    <text x="46" y="52" text-anchor="middle" font-family="ui-sans-serif, system-ui, -apple-system, sans-serif" font-size="18" font-weight="600" fill="#{theme.accent}">#{initials}</text>
    """
    |> String.trim()
  end

  defp render_badge_avatar(uri, _title, theme) when is_binary(uri) do
    """
    <rect x="5" y="5" width="20" height="20" rx="6" fill="#{theme.panel}"/>
    <image href="#{uri}" x="5" y="5" width="20" height="20" preserveAspectRatio="xMidYMid slice" clip-path="url(#taggle-badge-av)"/>
    """
    |> String.trim()
  end

  defp render_badge_avatar(nil, title, theme) do
    initials = escape(avatar_initials(title), 2)

    """
    <rect x="5" y="5" width="20" height="20" rx="6" fill="#{theme.panel}" stroke="#{theme.border}" stroke-width="1"/>
    <text x="15" y="18.5" text-anchor="middle" font-family="ui-sans-serif, system-ui, -apple-system, sans-serif" font-size="9" font-weight="700" fill="#{theme.accent}">#{initials}</text>
    """
    |> String.trim()
  end

  defp render_activity_icon(:coding, accent, _status_color) do
    ~s(<path d="M90 64 L86 67 L90 70 M94 64 L98 67 L94 70" fill="none" stroke="#{accent}" stroke-width="1.5" stroke-linecap="round" stroke-linejoin="round"/>)
  end

  defp render_activity_icon(:spotify, _accent, _status_color) do
    ~s(<rect x="86" y="65" width="2" height="6" rx="1" fill="#23a55a"/><rect x="90" y="62" width="2" height="9" rx="1" fill="#23a55a"/><rect x="94" y="64" width="2" height="7" rx="1" fill="#23a55a"/>)
  end

  defp render_activity_icon(:activity, accent, _status_color) do
    ~s(<circle cx="91" cy="67" r="3.2" fill="#{accent}"/>)
  end

  defp render_activity_icon(_idle, _accent, status_color) do
    ~s(<circle cx="91" cy="67" r="3" fill="#{status_color}"/>)
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

    gh_user =
      get_in(github || %{}, ["user", "username"]) || get_in(github || %{}, ["user", "login"])

    title =
      user["global_name"] ||
        user["username"] ||
        gh_user ||
        "Taggle"

    handle_parts =
      [
        if(user["username"], do: "@" <> to_string(user["username"])),
        if(gh_user && !hide_github?, do: "gh/" <> to_string(gh_user))
      ]
      |> Enum.reject(&is_nil/1)

    subtitle =
      case handle_parts do
        [] -> "self-hosted presence"
        parts -> Enum.join(parts, " · ")
      end

    status = to_string((discord && discord["discord_status"]) || "offline")
    status = if Map.has_key?(@status_colors, status), do: status, else: "offline"
    status_label = String.capitalize(status)

    {mode, primary_line} = pick_primary_activity(discord, github, status_label)
    {bottom_title, bottom_sub} = pick_bottom_line(github, discord, hide_github?)

    badge_label = user["username"] || user["global_name"] || gh_user || "taggle"
    badge_value = pick_badge_value(discord, github, status_label)

    %{
      title: title,
      subtitle: subtitle,
      status: status,
      status_label: status_label,
      mode: mode,
      primary_line: primary_line,
      bottom_title: bottom_title,
      bottom_sub: bottom_sub,
      badge_label: badge_label,
      badge_value: badge_value
    }
  end

  defp pick_primary_activity(discord, github, status_label) do
    cond do
      coding = discord && discord["coding"] ->
        editor = coding["editor"] || "IDE"
        detail = coding["details"] || coding["file"] || coding["state"] || coding["workspace"]
        line = if detail, do: "#{editor} · #{detail}", else: "Coding in #{editor}"
        {:coding, line}

      discord && discord["listening_to_spotify"] == true && is_map(discord["spotify"]) ->
        song = discord["spotify"]["song"] || "Music"
        artist = discord["spotify"]["artist"] || "Spotify"
        {:spotify, "#{song} — #{artist}"}

      is_list(discord && discord["activities"]) and discord["activities"] != [] ->
        act = first_non_custom_activity(discord["activities"])

        if act do
          name = act["name"] || "Active"
          details = act["details"] || act["state"]
          {:activity, if(details, do: "#{name} · #{details}", else: name)}
        else
          {:idle, status_label}
        end

      is_list(github && github["commits"]) and github["commits"] != [] ->
        c = hd(github["commits"])
        {:activity, "#{c["repo"]}: #{c["message"]}"}

      true ->
        {:idle, status_label}
    end
  end

  defp first_non_custom_activity(activities) do
    Enum.find(activities, fn a -> a["type"] != 4 and is_binary(a["name"]) and a["name"] != "" end)
  end

  defp pick_bottom_line(github, _discord, false) when is_map(github) do
    summary = github["summary"] || %{}
    commits = github["commits"] || []
    prs = github["pull_requests"] || []

    cond do
      commits != [] ->
        c = hd(commits)
        repo = c["repo"] || "private repository"
        msg = if c["private"], do: "Private repository commit", else: c["message"] || "Commit"
        {"#{repo} · #{c["sha"] || "commit"}", msg}

      prs != [] ->
        pr = hd(prs)
        repo = pr["repo"] || "private repository"
        title = if pr["private"], do: "Private pull request", else: pr["title"] || "Pull Request"
        {"#{repo} ##{pr["number"]}", title}

      true ->
        total = summary["commits_count"] || summary["total_activities"] || 0
        c24 = summary["commits_last_24h"] || summary["commits_24h"] || 0
        {"GitHub Activity", "#{total} commits · #{c24} in 24h"}
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
        discord && discord["active_on_discord_desktop"] -> "desktop"
        discord && discord["active_on_discord_web"] -> "web"
        discord && discord["active_on_discord_mobile"] -> "mobile"
        true -> "idle"
      end

    {"Taggle Presence", "client: #{platform} · #{kv_count} kv"}
  end

  defp pick_badge_value(discord, github, status_label) do
    cond do
      coding = discord && discord["coding"] ->
        detail = coding["details"] || coding["file"] || coding["state"] || coding["workspace"]

        if detail,
          do: "#{coding["editor"] || "IDE"}: #{detail}",
          else: "#{coding["editor"] || "coding"}"

      discord && discord["listening_to_spotify"] == true && is_map(discord["spotify"]) ->
        "♫ #{discord["spotify"]["song"] || "Spotify"}"

      is_list(discord && discord["activities"]) and
          first_non_custom_activity(discord["activities"]) != nil ->
        act = first_non_custom_activity(discord["activities"])
        act["name"] || status_label

      is_list(github && github["commits"]) and github["commits"] != [] ->
        c = hd(github["commits"])
        repo = c["repo"] || "private"
        "#{repo} @ #{c["sha"] || "head"}"

      true ->
        status_label
    end
  end

  defp approx_px_width(str, base) do
    str
    |> String.graphemes()
    |> Enum.reduce(0.0, fn ch, acc ->
      factor =
        cond do
          ch in ~w(i l I j t f r . , : ; | ! ' `) -> 0.55
          ch in ~w(m w M W @ % _) -> 1.28
          ch == " " -> 0.55
          true -> 1.0
        end

      acc + base * factor
    end)
    |> Float.ceil()
    |> trunc()
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
