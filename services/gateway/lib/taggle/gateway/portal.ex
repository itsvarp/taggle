defmodule Taggle.Gateway.Portal do
  @moduledoc false
  require EEx

  alias Taggle.Gateway.{DiscordAuth, GitHubAuth}

  def render(gh_user, dc_user, base_url) do
    assigns = %{
      gh: gh_user,
      dc: dc_user,
      base_url: base_url,
      ws: ws_url(base_url),
      gh_slug: GitHubAuth.app_slug(),
      gh_configured: configured?("GITHUB_APP_CLIENT_ID", "GITHUB_APP_CLIENT_SECRET"),
      dc_configured: DiscordAuth.configured?(),
      dc_invite: DiscordAuth.invite_url(),
      feed: if(gh_user, do: recent_feed(gh_user), else: []),
      dc_avatar: dc_user && dc_user.user && DiscordAuth.avatar_url(dc_user.user),
      dc_activities: (dc_user && dc_user.presence && dc_user.presence["activities"]) || [],
      dc_spotify: dc_user && dc_user.presence && dc_user.presence["spotify"],
      dc_status: dc_user && dc_user.presence && dc_user.presence["discord_status"]
    }

    render_layout(render_github(assigns) <> render_discord(assigns))
  end

  EEx.function_from_string(:defp, :render_github, ~S"""
  <section class="card">
    <div class="row">
      <div>
        <h2>GitHub</h2>
        <p class="muted">
          <%= if assigns.gh do %>
            Signed in as <strong>@<%= h(assigns.gh.login) %></strong> &middot;
            Commits: <%= length(assigns.gh.commits) %> &middot;
            PRs: <%= length(assigns.gh.pull_requests) %> &middot;
            Issues: <%= length(assigns.gh.issues) %> &middot;
            App installs: <%= assigns.gh.installations %>
          <% else %>
            Optional. Connect GitHub to publish commits, pull requests and issues.
          <% end %>
        </p>
      </div>
      <div class="actions">
        <%= if assigns.gh do %>
          <button class="btn secondary" type="button" onclick="syncNow()">Sync GitHub</button>
          <form method="post" action="/auth/github/logout"><button class="btn secondary" type="submit">Disconnect</button></form>
        <% end %>
      </div>
    </div>

    <%= if assigns.gh do %>
      <%= if assigns.feed == [] do %>
        <p class="muted">No GitHub activity synced yet. Click "Sync GitHub".</p>
      <% else %>
        <table>
          <thead><tr><th>Event</th><th>Repo</th><th>Updated</th></tr></thead>
          <tbody>
            <%= for item <- assigns.feed do %>
              <tr><td><%= h(item.title) %></td><td><code><%= h(item.repo) %></code></td><td class="muted"><%= h(item.at) %></td></tr>
            <% end %>
          </tbody>
        </table>
      <% end %>

      <h3>Endpoints</h3>
      <p><a href="/v1/github/users/<%= h(assigns.gh.login) %>"><code>GET <%= h(assigns.base_url) %>/v1/github/users/<%= h(assigns.gh.login) %></code></a></p>
      <p><code>WS <%= h(assigns.ws) %>/socket</code> &middot; <code>{"op":2,"d":{"subscribe_to_id":"<%= h(assigns.gh.login) %>"}}</code></p>

      <div class="actions" style="margin-top:12px">
        <%= if is_binary(assigns.gh_slug) and assigns.gh_slug != "" do %>
          <a class="btn secondary" href="https://github.com/apps/<%= h(assigns.gh_slug) %>/installations/new">Configure GitHub App</a>
        <% end %>
      </div>

      <h3>GitHub KV</h3>
      <%= if map_size(assigns.gh.kv) == 0 do %>
        <p class="muted">No GitHub KV entries yet.</p>
      <% else %>
        <table>
          <thead><tr><th>Key</th><th>Value</th><th></th></tr></thead>
          <tbody>
            <%= for {k, v} <- assigns.gh.kv do %>
              <tr>
                <td><code><%= h(k) %></code></td>
                <td><code><%= h(v) %></code></td>
                <td><button class="del" onclick="delKv('/api/me/kv', this.dataset.key)" data-key="<%= h(k) %>">Delete</button></td>
              </tr>
            <% end %>
          </tbody>
        </table>
      <% end %>
      <form onsubmit="addKv(event, '/api/me/kv', 'gh-kv-k', 'gh-kv-v')" class="row" style="margin-top:12px;gap:8px">
        <input id="gh-kv-k" placeholder="key" required />
        <input id="gh-kv-v" placeholder="value" required />
        <button class="btn" type="submit">Save</button>
      </form>

      <h3>GitHub API Key</h3>
      <p class="muted">Used only for KV writes. The key is shown once; only a hash is stored.</p>
      <button class="btn secondary" onclick="genKey('/api/me/api-key')">Generate New API Key</button>
      <pre id="key-out" class="key-out" style="display:none"></pre>
    <% else %>
      <%= if assigns.gh_configured do %>
        <a class="btn" href="/auth/github">Connect GitHub</a>
      <% else %>
        <p class="muted">GitHub OAuth is not configured on this instance. You can still use the public read API if an installation is connected.</p>
      <% end %>
    <% end %>
  </section>
  """, [:assigns])

  EEx.function_from_string(:defp, :render_discord, ~S"""
  <section class="card">
    <div class="row">
      <div>
        <h2>Discord</h2>
        <p class="muted">
          <%= if assigns.dc && assigns.dc.user do %>
            Signed in as <strong><%= h(assigns.dc.user.global_name || assigns.dc.user.username || assigns.dc.user.id) %></strong>
            <%= if assigns.dc_status do %> &middot; <span class="status <%= status_class(assigns.dc_status) %>"><%= h(assigns.dc_status) %></span><% end %>
          <% else %>
            Optional. Presence is public; join the monitored server to appear in the API.
          <% end %>
        </p>
      </div>
      <div class="actions">
        <%= if assigns.dc && assigns.dc.user do %>
          <form method="post" action="/auth/discord/logout"><button class="btn secondary" type="submit">Disconnect</button></form>
        <% end %>
      </div>
    </div>

    <%= if assigns.dc && assigns.dc.user do %>
      <%= if assigns.dc.presence do %>
        <div class="presence">
          <img class="avatar" src="<%= h(assigns.dc_avatar) %>" alt="" width="64" height="64" />
          <div>
            <p style="margin:0"><strong>@<%= h(assigns.dc.presence["discord_user"]["username"]) %></strong></p>
            <p class="muted" style="margin:4px 0 0">
              Desktop: <%= yes_no(assigns.dc.presence["active_on_discord_desktop"]) %> &middot;
              Mobile: <%= yes_no(assigns.dc.presence["active_on_discord_mobile"]) %> &middot;
              Web: <%= yes_no(assigns.dc.presence["active_on_discord_web"]) %>
            </p>
          </div>
        </div>

        <%= if assigns.dc_spotify do %>
          <h3>Spotify</h3>
          <div class="spotify">
            <%= if assigns.dc_spotify["album_art_url"] not in [nil, ""] do %>
              <img src="<%= h(assigns.dc_spotify["album_art_url"]) %>" alt="" width="56" height="56" />
            <% end %>
            <div>
              <p style="margin:0"><strong><%= h(assigns.dc_spotify["song"]) %></strong></p>
              <p class="muted" style="margin:4px 0 0"><%= h(assigns.dc_spotify["artist"]) %> &middot; <%= h(assigns.dc_spotify["album"]) %></p>
            </div>
          </div>
        <% end %>

        <h3>Activities</h3>
        <%= if assigns.dc_activities == [] do %>
          <p class="muted">No active activities.</p>
        <% else %>
          <table>
            <thead><tr><th>Activity</th><th>Details</th><th>State</th></tr></thead>
            <tbody>
              <%= for activity <- assigns.dc_activities do %>
                <tr>
                  <td><%= h(activity["name"]) %></td>
                  <td><%= h(activity["details"]) %></td>
                  <td><%= h(activity["state"]) %></td>
                </tr>
              <% end %>
            </tbody>
          </table>
        <% end %>
      <% else %>
        <p class="muted">You are signed in, but no live presence is visible yet. Join the monitored Discord server and make sure the bot can see your presence.</p>
      <% end %>

      <h3>Endpoints</h3>
      <p><a href="/v1/discord/users/<%= h(assigns.dc.user.id) %>"><code>GET <%= h(assigns.base_url) %>/v1/discord/users/<%= h(assigns.dc.user.id) %></code></a></p>
      <p><code>WS <%= h(assigns.ws) %>/socket</code> &middot; <code>{"op":2,"d":{"subscribe_to_id":"<%= h(assigns.dc.user.id) %>"}}</code></p>
      <p><code>GET <%= h(assigns.base_url) %>/<%= h(assigns.dc.user.id) %>.png</code> (quicklink)</p>

      <h3>Discord KV</h3>
      <%= if map_size(assigns.dc.kv) == 0 do %>
        <p class="muted">No Discord KV entries yet.</p>
      <% else %>
        <table>
          <thead><tr><th>Key</th><th>Value</th><th></th></tr></thead>
          <tbody>
            <%= for {k, v} <- assigns.dc.kv do %>
              <tr>
                <td><code><%= h(k) %></code></td>
                <td><code><%= h(v) %></code></td>
                <td><button class="del" onclick="delKv('/api/discord/me/kv', this.dataset.key)" data-key="<%= h(k) %>">Delete</button></td>
              </tr>
            <% end %>
          </tbody>
        </table>
      <% end %>
      <form onsubmit="addKv(event, '/api/discord/me/kv', 'dc-kv-k', 'dc-kv-v')" class="row" style="margin-top:12px;gap:8px">
        <input id="dc-kv-k" placeholder="key (a-z, A-Z, 0-9, _)" required />
        <input id="dc-kv-v" placeholder="value" required />
        <button class="btn" type="submit">Save</button>
      </form>

      <h3>Discord API Key</h3>
      <p class="muted">Used only for KV writes. The key is shown once; only a hash is stored.</p>
      <button class="btn secondary" onclick="genKey('/api/discord/me/api-key')">Generate New API Key</button>
      <pre id="key-out" class="key-out" style="display:none"></pre>
    <% else %>
      <%= if assigns.dc_invite do %>
        <p><a class="btn" href="<%= h(assigns.dc_invite) %>" rel="noreferrer noopener" target="_blank">Join the Discord server</a></p>
      <% end %>
      <%= if assigns.dc_configured do %>
        <p class="muted" style="margin-top:12px">Connect Discord only if you want a KV API key. Presence itself is public.</p>
        <a class="btn secondary" href="/auth/discord">Connect Discord</a>
      <% else %>
        <p class="muted">Discord OAuth is not configured on this instance. Presence and the public API still work.</p>
      <% end %>
    <% end %>
  </section>
  """, [:assigns])

  EEx.function_from_string(:defp, :render_layout, ~S"""
  <!doctype html>
  <html lang="en">
  <head>
    <meta charset="utf-8" />
    <meta name="viewport" content="width=device-width,initial-scale=1" />
    <title>Taggle</title>
    <style>
      body { font-family: ui-sans-serif, system-ui, sans-serif; background:#09090b; color:#fafafa; margin:0; padding:32px 16px; }
      .wrap { max-width:760px; margin:0 auto; display:grid; gap:16px; }
      .hero h1 { margin:0 0 4px; font-size:26px; letter-spacing:-.02em; }
      .card { background:#111113; border:1px solid #27272a; border-radius:10px; padding:20px; }
      .row { display:flex; align-items:flex-start; justify-content:space-between; gap:12px; }
      .actions { display:flex; gap:8px; align-items:center; flex-wrap:wrap; justify-content:flex-end; }
      h1,h2,h3 { margin:0 0 8px; font-size:18px; }
      h3 { font-size:14px; margin-top:20px; }
      .muted { color:#a1a1aa; font-size:14px; margin:0 0 12px; }
      a { color:#60a5fa; text-decoration:none; }
      code, pre { font-family: ui-monospace, monospace; font-size:13px; }
      pre { background:#09090b; border:1px solid #27272a; padding:12px; border-radius:8px; overflow:auto; margin:0; }
      .btn { background:#fafafa; color:#09090b; border:0; border-radius:6px; padding:8px 14px; font-weight:600; cursor:pointer; display:inline-block; font-size:13px; }
      .btn.secondary { background:#27272a; color:#fafafa; }
      .del { background:#7f1d1d; color:#fecaca; border:0; border-radius:4px; padding:4px 8px; cursor:pointer; font-size:12px; }
      input { background:#09090b; color:#fafafa; border:1px solid #27272a; border-radius:6px; padding:8px 10px; flex:1; font-size:13px; }
      table { width:100%; border-collapse:collapse; font-size:13px; }
      th,td { text-align:left; padding:6px 4px; border-bottom:1px solid #27272a; }
      .presence { display:flex; gap:12px; align-items:center; margin:12px 0; }
      .avatar { border-radius:50%; background:#27272a; }
      .spotify { display:flex; gap:12px; align-items:center; }
      .spotify img { border-radius:6px; background:#27272a; }
      .status { text-transform:capitalize; font-weight:600; }
      .status.online { color:#4ade80; }
      .status.idle { color:#facc15; }
      .status.dnd { color:#f87171; }
      .status.offline { color:#a1a1aa; }
      .key-out { margin-top:12px; }
      footer { color:#71717a; font-size:12px; text-align:center; padding-top:8px; }
    </style>
  </head>
  <body>
    <div class="wrap">
      <header class="hero">
        <h1>Taggle</h1>
        <p class="muted">Real-time Discord presence and GitHub activity over REST JSON &amp; WebSocket. Use either source, or both.</p>
      </header>
      <%= body %>
      <footer>Taggle &middot; Discord presence is Lanyard-compatible.</footer>
    </div>
    <script>
      async function syncNow() {
        const btn = document.getElementById('sync-btn');
        if (btn) { btn.textContent = 'Syncing...'; btn.disabled = true; }
        await fetch('/api/me/sync', { method: 'POST' });
        location.reload();
      }
      async function genKey(path) {
        const r = await fetch(path, { method: 'POST' });
        const j = await r.json();
        const el = document.getElementById('key-out');
        el.style.display = 'block';
        el.textContent = j.success ? 'Copy once (not stored in plaintext): ' + j.data.api_key : JSON.stringify(j);
      }
      async function addKv(e, path, keyId, valueId) {
        e.preventDefault();
        const key = document.getElementById(keyId).value;
        const value = document.getElementById(valueId).value;
        const r = await fetch(path, {
          method: 'PUT',
          headers: { 'content-type': 'application/json' },
          body: JSON.stringify({ [key]: value })
        });
        if (r.ok) location.reload();
      }
      async function delKv(path, key) {
        const r = await fetch(path + '/' + encodeURIComponent(key), { method: 'DELETE' });
        if (r.ok) location.reload();
      }
    </script>
  </body>
  </html>
  """, [:body])

  defp recent_feed(user) do
    commits =
      Enum.map(user.commits || [], fn c ->
        %{
          title: if(c["private"], do: "Private commit (#{c["sha"]})", else: c["message"] || c["sha"]),
          repo: if(c["private"], do: "private", else: c["repo"] || ""),
          at: c["timestamp"] || ""
        }
      end)

    prs =
      Enum.map(user.pull_requests || [], fn pr ->
        %{
          title: if(pr["private"], do: "Private PR ##{pr["number"]}", else: "#{pr["title"]} (##{pr["number"]})"),
          repo: if(pr["private"], do: "private", else: pr["repo"] || ""),
          at: pr["updated_at"] || ""
        }
      end)

    issues =
      Enum.map(user.issues || [], fn i ->
        %{
          title: if(i["private"], do: "Private issue ##{i["number"]}", else: "#{i["title"]} (##{i["number"]})"),
          repo: if(i["private"], do: "private", else: i["repo"] || ""),
          at: i["updated_at"] || ""
        }
      end)

    (commits ++ prs ++ issues)
    |> Enum.sort_by(& &1.at, :desc)
    |> Enum.take(8)
  end

  defp configured?(a, b) do
    is_binary(System.get_env(a)) and System.get_env(a) != "" and
      is_binary(System.get_env(b)) and System.get_env(b) != ""
  end

  defp status_class(status) when status in ["online", "idle", "dnd", "offline"], do: status
  defp status_class(_), do: "offline"

  defp yes_no(true), do: "yes"
  defp yes_no(_), do: "no"

  defp ws_url("https://" <> rest), do: "wss://" <> rest
  defp ws_url("http://" <> rest), do: "ws://" <> rest
  defp ws_url(u), do: u

  defp h(nil), do: ""
  defp h(val), do: val |> to_string() |> Plug.HTML.html_escape()
end
