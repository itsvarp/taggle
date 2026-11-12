/**
 * Taggle First-Party Browser Client & <taggle-card> Web Component.
 * Zero external dependencies. 100% self-hosted by Taggle Gateway.
 *
 * Usage:
 *   <script type="module" src="/taggle.js"></script>
 *   <taggle-card id="123456789012345678" theme="dark"></taggle-card>
 *
 * Programmatic SDK:
 *   import { TaggleClient } from "/taggle.js";
 *   const client = new TaggleClient({ ids: ["123456789012345678", "octocat"] });
 *   client.on("update", (data) => console.log(data));
 */
export class TaggleClient {
  constructor(options = {}) {
    const base =
      options.baseUrl ||
      (typeof window !== "undefined" ? window.location.origin : "");
    this.baseUrl = base.replace(/\/+$/, "");
    this.wsUrl =
      options.wsUrl ||
      this.baseUrl.replace(/^http/, "ws") + "/socket";
    this.ids = Array.isArray(options.ids)
      ? options.ids.slice()
      : options.id
      ? [options.id]
      : [];
    this.all = Boolean(options.all);
    this.filter = options.filter || null;
    this.reconnect = options.reconnect !== false;

    this._ws = null;
    this._hb = null;
    this._retryMs = 1000;
    this._closed = false;
    this._listeners = new Map();

    if (options.autoConnect !== false) {
      this.connect();
    }
  }

  on(event, fn) {
    if (!this._listeners.has(event)) this._listeners.set(event, new Set());
    this._listeners.get(event).add(fn);
    return () => this.off(event, fn);
  }

  off(event, fn) {
    this._listeners.get(event)?.delete(fn);
  }

  _emit(event, payload) {
    const set = this._listeners.get(event);
    if (!set) return;
    for (const fn of set) {
      try {
        fn(payload);
      } catch (_) {}
    }
  }

  connect() {
    this._closed = false;
    const ws = new WebSocket(this.wsUrl);
    this._ws = ws;

    ws.addEventListener("message", (ev) => {
      let msg;
      try {
        msg = JSON.parse(ev.data);
      } catch (_) {
        return;
      }

      if (msg.op === 1 && msg.d?.heartbeat_interval) {
        this._retryMs = 1000;
        this._startHeartbeat(msg.d.heartbeat_interval);
        this.subscribe(this.ids, { all: this.all, filter: this.filter });
      } else if (msg.op === 0) {
        if (msg.t === "INIT_STATE") {
          this._emit("init", msg.d);
          this._emit("update", msg.d);
        } else if (msg.t === "PRESENCE_UPDATE" || msg.t === "ACTIVITY_UPDATE") {
          this._emit("update", msg.d);
          this._emit(msg.t, msg.d);
        } else if (msg.t === "ERROR") {
          this._emit("error", msg.d);
        }
      }
    });

    ws.addEventListener("close", () => {
      this._stopHeartbeat();
      this._emit("close", {});
      if (!this._closed && this.reconnect) {
        const delay = this._retryMs;
        this._retryMs = Math.min(this._retryMs * 2, 30000);
        setTimeout(() => {
          if (!this._closed) this.connect();
        }, delay);
      }
    });
  }

  subscribe(ids = [], opts = {}) {
    if (!this._ws || this._ws.readyState !== WebSocket.OPEN) return;
    const d = {};
    if (opts.all) {
      d.subscribe_to_all = true;
    } else if (ids.length === 1) {
      d.subscribe_to_id = String(ids[0]);
    } else if (ids.length > 1) {
      d.subscribe_to_ids = ids.map(String);
    }
    if (opts.filter) d.filter = opts.filter;
    this._ws.send(JSON.stringify({ op: 2, d }));
  }

  unsubscribe(ids = [], opts = {}) {
    if (!this._ws || this._ws.readyState !== WebSocket.OPEN) return;
    const d = {};
    if (opts.all) d.unsubscribe_from_all = true;
    else if (ids.length === 1) d.unsubscribe_from_id = String(ids[0]);
    else if (ids.length > 1) d.unsubscribe_from_ids = ids.map(String);
    this._ws.send(JSON.stringify({ op: 4, d }));
  }

  close() {
    this._closed = true;
    this._stopHeartbeat();
    this._ws?.close();
  }

  _startHeartbeat(ms) {
    this._stopHeartbeat();
    this._hb = setInterval(() => {
      if (this._ws?.readyState === WebSocket.OPEN) {
        this._ws.send(JSON.stringify({ op: 3 }));
      }
    }, ms);
  }

  _stopHeartbeat() {
    if (this._hb) {
      clearInterval(this._hb);
      this._hb = null;
    }
  }
}

const STATUS_COLOR = {
  online: "#23a55a",
  idle: "#f0b232",
  dnd: "#f23f43",
  offline: "#80848e",
};

class TaggleCardElement extends HTMLElement {
  static get observedAttributes() {
    return ["subject", "user-id", "theme", "base-url"];
  }

  constructor() {
    super();
    this.attachShadow({ mode: "open" });
    this._client = null;
    this._data = null;
  }

  connectedCallback() {
    this._render();
    this._connect();
  }

  disconnectedCallback() {
    this._client?.close();
    this._client = null;
  }

  attributeChangedCallback() {
    if (this.isConnected) {
      this._client?.close();
      this._connect();
    }
  }

  _subject() {
    return (
      this.getAttribute("subject") ||
      this.getAttribute("user-id") ||
      ""
    ).trim();
  }

  _baseUrl() {
    const attr = this.getAttribute("base-url");
    if (attr) return attr.replace(/\/+$/, "");
    return typeof window !== "undefined" ? window.location.origin : "";
  }

  async _connect() {
    const id = this._subject();
    if (!id) return;

    const base = this._baseUrl();
    try {
      const res = await fetch(`${base}/v1/profiles/${encodeURIComponent(id)}`);
      if (res.ok) {
        const json = await res.json();
        if (json.success && json.data) {
          this._data = json.data;
          this._render();
        }
      }
    } catch (_) {}

    const subIds = [];
    if (this._data?.discord?.discord_user?.id) {
      subIds.push(this._data.discord.discord_user.id);
    }
    if (this._data?.github?.user?.login) {
      subIds.push(this._data.github.user.login);
    }
    if (subIds.length === 0) subIds.push(id);

    this._client = new TaggleClient({
      baseUrl: base,
      ids: subIds,
    });
    this._client.on("update", (payload) => {
      if (!payload || typeof payload !== "object") return;
      if (!this._data) this._data = { discord: null, github: null };
      if (payload.discord_user) {
        this._data = { ...this._data, discord: payload };
      } else if (payload.user) {
        this._data = { ...this._data, github: payload };
      } else {
        for (const val of Object.values(payload)) {
          if (val?.discord_user) this._data.discord = val;
          if (val?.user) this._data.github = val;
        }
      }
      this._render();
    });
  }

  _render() {
    const root = this.shadowRoot;
    root.textContent = "";

    const dark = (this.getAttribute("theme") || "dark") !== "light";
    const style = document.createElement("style");
    style.textContent = `
      :host { display: inline-block; font-family: ui-sans-serif, system-ui, sans-serif; }
      .card {
        min-width: 280px; max-width: 420px; padding: 14px 16px; border-radius: 12px;
        background: ${dark ? "#0d1117" : "#ffffff"};
        color: ${dark ? "#f0f6fc" : "#1f2328"};
        border: 1px solid ${dark ? "#30363d" : "#d0d7de"};
      }
      .top { display: flex; align-items: center; gap: 10px; }
      .dot { width: 10px; height: 10px; border-radius: 999px; flex: none; }
      .title { font-weight: 700; font-size: 15px; }
      .sub { font-size: 12px; color: ${dark ? "#8b949e" : "#656d76"}; margin-top: 2px; }
      .line { margin-top: 10px; font-size: 13px; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }
    `;
    root.appendChild(style);

    const card = document.createElement("div");
    card.className = "card";

    const dc = this._data?.discord;
    const gh = this._data?.github;
    const status = dc?.discord_status || "offline";
    const name =
      dc?.discord_user?.global_name ||
      dc?.discord_user?.username ||
      gh?.user?.login ||
      this._subject() ||
      "Taggle";

    const top = document.createElement("div");
    top.className = "top";

    const dot = document.createElement("span");
    dot.className = "dot";
    dot.style.background = STATUS_COLOR[status] || STATUS_COLOR.offline;

    const titleWrap = document.createElement("div");
    const title = document.createElement("div");
    title.className = "title";
    title.textContent = String(name);

    const sub = document.createElement("div");
    sub.className = "sub";
    const handles = [];
    if (dc?.discord_user?.username) handles.push(`@${dc.discord_user.username}`);
    if (gh?.user?.login) handles.push(`gh/${gh.user.login}`);
    sub.textContent = handles.join(" · ") || status;

    titleWrap.appendChild(title);
    titleWrap.appendChild(sub);
    top.appendChild(dot);
    top.appendChild(titleWrap);
    card.appendChild(top);

    const line = document.createElement("div");
    line.className = "line";
    if (dc?.coding?.file) {
      line.textContent = `${dc.coding.editor || "IDE"}: ${dc.coding.file}`;
    } else if (dc?.listening_to_spotify && dc?.spotify?.song) {
      line.textContent = `♫ ${dc.spotify.song} — ${dc.spotify.artist || ""}`;
    } else if (dc?.activities?.[0]?.name) {
      const a = dc.activities[0];
      line.textContent = a.details ? `${a.name}: ${a.details}` : a.name;
    } else if (gh?.commits?.[0]) {
      const c = gh.commits[0];
      line.textContent = `${c.repo}: ${c.message}`;
    } else {
      line.textContent = `Status: ${status}`;
    }
    card.appendChild(line);

    root.appendChild(card);
  }
}

if (typeof customElements !== "undefined" && !customElements.get("taggle-card")) {
  customElements.define("taggle-card", TaggleCardElement);
}
