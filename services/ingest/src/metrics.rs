use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};

/// Lock-free atomic metrics for the ingest worker, exposed at `/metrics`.
#[derive(Default)]
pub struct IngestMetrics {
    pub webhooks_accepted: AtomicU64,
    pub webhooks_duplicate: AtomicU64,
    pub webhooks_rejected: AtomicU64,
    pub webhooks_busy: AtomicU64,
    pub github_syncs_ok: AtomicU64,
    pub github_syncs_err: AtomicU64,
    pub github_api_requests: AtomicU64,
    pub github_api_not_modified: AtomicU64,
    pub github_api_rate_limited: AtomicU64,
    pub discord_connected: AtomicI64,
    pub discord_reconnects: AtomicU64,
    pub discord_presence_updates: AtomicU64,
    pub discord_presence_dropped: AtomicU64,
    pub discord_interactions: AtomicU64,
}

impl IngestMetrics {
    pub fn render(&self) -> String {
        let mut out = String::with_capacity(1024);
        let push_counter = |out: &mut String, name: &str, help: &str, val: u64| {
            out.push_str("# HELP ");
            out.push_str(name);
            out.push(' ');
            out.push_str(help);
            out.push('\n');
            out.push_str("# TYPE ");
            out.push_str(name);
            out.push_str(" counter\n");
            out.push_str(name);
            out.push(' ');
            out.push_str(&val.to_string());
            out.push('\n');
        };
        let push_gauge = |out: &mut String, name: &str, help: &str, val: i64| {
            out.push_str("# HELP ");
            out.push_str(name);
            out.push(' ');
            out.push_str(help);
            out.push('\n');
            out.push_str("# TYPE ");
            out.push_str(name);
            out.push_str(" gauge\n");
            out.push_str(name);
            out.push(' ');
            out.push_str(&val.to_string());
            out.push('\n');
        };

        push_counter(
            &mut out,
            "taggle_ingest_webhooks_accepted_total",
            "Accepted GitHub webhook deliveries.",
            self.webhooks_accepted.load(Ordering::Relaxed),
        );
        push_counter(
            &mut out,
            "taggle_ingest_webhooks_duplicate_total",
            "Duplicate GitHub webhook deliveries ignored.",
            self.webhooks_duplicate.load(Ordering::Relaxed),
        );
        push_counter(
            &mut out,
            "taggle_ingest_webhooks_rejected_total",
            "Rejected GitHub webhook requests.",
            self.webhooks_rejected.load(Ordering::Relaxed),
        );
        push_counter(
            &mut out,
            "taggle_ingest_webhooks_busy_total",
            "Webhook requests rejected with 503 due to worker saturation.",
            self.webhooks_busy.load(Ordering::Relaxed),
        );
        push_counter(
            &mut out,
            "taggle_ingest_github_syncs_ok_total",
            "Successful periodic GitHub sync cycles.",
            self.github_syncs_ok.load(Ordering::Relaxed),
        );
        push_counter(
            &mut out,
            "taggle_ingest_github_syncs_err_total",
            "Failed periodic GitHub sync cycles.",
            self.github_syncs_err.load(Ordering::Relaxed),
        );
        push_counter(
            &mut out,
            "taggle_ingest_github_api_requests_total",
            "Outbound GitHub HTTP requests.",
            self.github_api_requests.load(Ordering::Relaxed),
        );
        push_counter(
            &mut out,
            "taggle_ingest_github_api_not_modified_total",
            "GitHub requests served via ETag 304 Not Modified.",
            self.github_api_not_modified.load(Ordering::Relaxed),
        );
        push_counter(
            &mut out,
            "taggle_ingest_github_api_rate_limited_total",
            "GitHub requests returning 403/429 rate limit.",
            self.github_api_rate_limited.load(Ordering::Relaxed),
        );
        push_gauge(
            &mut out,
            "taggle_ingest_discord_connected",
            "1 when Discord gateway is connected, 0 otherwise.",
            self.discord_connected.load(Ordering::Relaxed),
        );
        push_counter(
            &mut out,
            "taggle_ingest_discord_reconnects_total",
            "Discord gateway reconnect attempts.",
            self.discord_reconnects.load(Ordering::Relaxed),
        );
        push_counter(
            &mut out,
            "taggle_ingest_discord_presence_updates_total",
            "Normalized Discord presence updates queued.",
            self.discord_presence_updates.load(Ordering::Relaxed),
        );
        push_counter(
            &mut out,
            "taggle_ingest_discord_presence_dropped_total",
            "Discord presence updates dropped due to full worker queue.",
            self.discord_presence_dropped.load(Ordering::Relaxed),
        );
        push_counter(
            &mut out,
            "taggle_ingest_discord_interactions_total",
            "Discord slash command and component interactions processed.",
            self.discord_interactions.load(Ordering::Relaxed),
        );

        out
    }
}
