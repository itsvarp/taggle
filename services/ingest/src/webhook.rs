use std::sync::Arc;

use anyhow::Result;
use axum::{
    Router,
    body::Bytes,
    extract::{DefaultBodyLimit, State},
    http::{HeaderMap, StatusCode},
    routing::{get, post},
};
use hmac::{Hmac, KeyInit, Mac};
use serde_json::{Value, json};
use sha2::Sha256;
use tokio::sync::Semaphore;

use crate::{App, normalize};

#[derive(Clone)]
struct WebhookState {
    app: Arc<App>,
    jobs: Arc<Semaphore>,
}

pub fn router(app: Arc<App>) -> Router {
    let state = WebhookState {
        app,
        jobs: Arc::new(Semaphore::new(32)),
    };
    Router::new()
        .route("/health", get(|| async { "ok" }))
        .route("/webhooks/github", post(github_webhook))
        .layer(DefaultBodyLimit::max(4 * 1024 * 1024))
        .with_state(state)
}

/// Constant-time check of `X-Hub-Signature-256: sha256=<hex>`.
pub fn verify_signature(secret: &str, body: &[u8], header: &str) -> bool {
    let Some(hex_sig) = header.strip_prefix("sha256=") else { return false };
    let Ok(sig) = hex::decode(hex_sig) else { return false };
    let Ok(mut mac) = Hmac::<Sha256>::new_from_slice(secret.as_bytes()) else { return false };
    mac.update(body);
    mac.verify_slice(&sig).is_ok()
}

async fn github_webhook(State(state): State<WebhookState>, headers: HeaderMap, body: Bytes) -> StatusCode {
    let app = &state.app;
    let Some(github) = app.config.github.as_ref() else {
        return StatusCode::SERVICE_UNAVAILABLE; // Discord-only deployment
    };

    let header = |k: &str| headers.get(k).and_then(|v| v.to_str().ok()).unwrap_or("");

    if !verify_signature(&github.webhook_secret, &body, header("x-hub-signature-256")) {
        return StatusCode::UNAUTHORIZED;
    }
    let (event, delivery) = (header("x-github-event").to_owned(), header("x-github-delivery").to_owned());
    if event.is_empty() || delivery.is_empty() || delivery.len() > 128 {
        return StatusCode::BAD_REQUEST;
    }
    let Ok(payload) = serde_json::from_slice::<Value>(&body) else { return StatusCode::BAD_REQUEST };

    match app.store.first_delivery(&delivery).await {
        Ok(true) => {}
        Ok(false) => return StatusCode::OK, // duplicate redelivery
        Err(e) => {
            tracing::error!(error = %e, "dedupe check failed");
            return StatusCode::INTERNAL_SERVER_ERROR; // GitHub can retry
        }
    }

    // Ack fast; GitHub gives webhooks 10s, while bounding background concurrency.
    let app_bg = Arc::clone(&state.app);
    let jobs = Arc::clone(&state.jobs);
    tokio::spawn(async move {
        let Ok(_permit) = jobs.acquire_owned().await else { return };
        if let Err(e) = process(&app_bg, &event, &payload).await {
            tracing::error!(error = %e, %event, %delivery, "processing failed");
        }
    });
    StatusCode::ACCEPTED
}

async fn process(app: &App, event: &str, p: &Value) -> Result<()> {
    let store = &app.store;
    let installation = p.pointer("/installation/id").and_then(Value::as_i64);

    match event {
        "ping" => return Ok(()),
        "installation" => {
            let (Some(id), Some(login), Some(account_id)) = (
                installation,
                normalize::s(p, "/installation/account/login"),
                p.pointer("/installation/account/id").and_then(Value::as_i64),
            ) else {
                return Ok(());
            };
            match normalize::s(p, "/action") {
                Some("created") | Some("unsuspend") | Some("new_permissions_accepted") => {
                    let login_lower = login.to_lowercase();
                    if !store.is_github_connected(&login_lower).await? {
                        tracing::debug!(login = %login_lower, "ignoring installation for unlinked github user");
                        return Ok(());
                    }
                    store.upsert_user(account_id, login, normalize::s(p, "/installation/account/avatar_url")).await?;
                    store.upsert_installation(id, login).await?;
                    run_backfill(app, id, &login_lower).await?;
                    store.rebuild_snapshot(&login_lower).await?;
                    tracing::info!(login, "installation added");
                }
                Some("deleted") | Some("suspend") => {
                    if let Some(removed_login) = store.remove_installation(id).await?
                        && store.is_github_connected(&removed_login).await?
                    {
                        let _ = crate::sync::sync_user(app, &removed_login, None).await;
                    }
                    tracing::info!(login, "installation removed");
                }
                _ => {}
            }
            return Ok(());
        }
        "installation_repositories" => {
            let Some(id) = installation else { return Ok(()) };
            if normalize::s(p, "/action") == Some("added") {
                if let Some(login) = store.login_for_installation(id).await?
                    && store.is_github_connected(&login).await?
                {
                    run_backfill(app, id, &login).await?;
                    store.rebuild_snapshot(&login).await?;
                }
            }
            return Ok(());
        }
        _ => {}
    }

    let Some(installation) = installation else { return Ok(()) };
    let Some(login) = store.login_for_installation(installation).await? else {
        tracing::debug!(installation, event, "event for unknown installation");
        return Ok(());
    };
    if !store.is_github_connected(&login).await? {
        return Ok(());
    }

    if event == "deployment_status" {
        if let (Some(repo), Some(branch), Some(raw_state)) = (
            normalize::s(p, "/repository/full_name"),
            normalize::s(p, "/deployment/ref"),
            normalize::s(p, "/deployment_status/state"),
        ) {
            let mapped = match raw_state {
                "pending" | "in_progress" | "queued" => "building",
                "success" => "ready",
                "failure" | "error" => "error",
                other => other,
            };
            if store.patch_pr_deployment(&login, repo, branch, mapped).await? {
                store.rebuild_snapshot(&login).await?;
            }
        }
        return Ok(());
    }

    for act in normalize::from_event(event, p) {
        store.upsert_activity(&login, act.kind, &act.key, &act.defaults, &act.base, act.at).await?;
    }

    let private = p.pointer("/repository/private").and_then(Value::as_bool).unwrap_or(false);
    if let Some(github) = app.github.as_ref() {
        for (owner, repo, number) in normalize::pr_targets(event, p) {
            match github.enrich_pr(installation, &owner, &repo, number).await {
                Ok(e) => {
                    let mut patch = json!({
                        "review_threads": e.review_threads,
                        "checks_status": e.checks_status,
                        "participants": if private { json!([]) } else { e.participants },
                    });
                    if e.merged == Some(true) {
                        patch["state"] = json!("merged");
                    }
                    store.patch_activity(&login, "pull_request", &format!("{owner}/{repo}#{number}"), &patch).await?;
                }
                Err(err) => tracing::warn!(error = %err, %owner, %repo, number, "enrichment failed"),
            }
        }
    }

    store.rebuild_snapshot(&login).await
}

async fn run_backfill(app: &App, installation: i64, login: &str) -> Result<()> {
    let Some(github) = app.github.as_ref() else { return Ok(()) };
    match github.backfill_installation(installation).await {
        Ok(acts) => {
            for act in acts {
                app.store.upsert_activity(login, act.kind, &act.key, &act.defaults, &act.base, act.at).await?;
            }
        }
        Err(err) => tracing::warn!(error = %err, installation, login, "initial backfill failed"),
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::verify_signature;
    use hmac::{Hmac, KeyInit, Mac};
    use sha2::Sha256;

    fn sign(secret: &str, body: &[u8]) -> String {
        let mut mac = Hmac::<Sha256>::new_from_slice(secret.as_bytes()).unwrap();
        mac.update(body);
        format!("sha256={}", hex::encode(mac.finalize().into_bytes()))
    }

    #[test]
    fn accepts_valid_and_rejects_invalid() {
        let sig = sign("s3cret", b"{}");
        assert!(verify_signature("s3cret", b"{}", &sig));
        assert!(!verify_signature("other", b"{}", &sig));
        assert!(!verify_signature("s3cret", b"{ }", &sig));
        assert!(!verify_signature("s3cret", b"{}", "sha256=zz"));
        assert!(!verify_signature("s3cret", b"{}", ""));
    }
}
