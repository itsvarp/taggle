use std::sync::Arc;

use anyhow::Result;
use axum::{
    Json, Router,
    body::Bytes,
    extract::State,
    http::{HeaderMap, StatusCode},
    routing::{get, post},
};
use hmac::{Hmac, KeyInit, Mac};
use serde::Deserialize;
use serde_json::{Value, json};
use sha2::Sha256;

use crate::{App, normalize, sync};

#[derive(Deserialize)]
struct SyncReq {
    login: String,
    oauth_token: Option<String>,
}

pub fn router(app: Arc<App>) -> Router {
    Router::new()
        .route("/health", get(|| async { "ok" }))
        .route("/webhooks/github", post(github_webhook))
        .route("/internal/sync", post(manual_sync))
        .with_state(app)
}

async fn manual_sync(State(app): State<Arc<App>>, Json(req): Json<SyncReq>) -> StatusCode {
    if app.config.github.is_none() {
        return StatusCode::SERVICE_UNAVAILABLE;
    }
    let login = req.login.trim().to_lowercase();
    if login.is_empty() {
        return StatusCode::BAD_REQUEST;
    }
    if req.oauth_token.is_none() {
        match app.store.allow_manual_sync(&login, 10).await {
            Ok(true) => {}
            Ok(false) => return StatusCode::OK,
            Err(_) => {}
        }
    }
    match sync::sync_user(&app, &login, req.oauth_token.as_deref()).await {
        Ok(()) => StatusCode::OK,
        Err(err) => {
            tracing::warn!(error = %err, %login, "manual sync failed");
            StatusCode::BAD_GATEWAY
        }
    }
}

/// Constant-time check of `X-Hub-Signature-256: sha256=<hex>`.
pub fn verify_signature(secret: &str, body: &[u8], header: &str) -> bool {
    let Some(hex_sig) = header.strip_prefix("sha256=") else { return false };
    let Ok(sig) = hex::decode(hex_sig) else { return false };
    let Ok(mut mac) = Hmac::<Sha256>::new_from_slice(secret.as_bytes()) else { return false };
    mac.update(body);
    mac.verify_slice(&sig).is_ok()
}

async fn github_webhook(State(app): State<Arc<App>>, headers: HeaderMap, body: Bytes) -> StatusCode {
    let Some(github) = app.config.github.as_ref() else {
        return StatusCode::SERVICE_UNAVAILABLE; // Discord-only deployment
    };

    let header = |k: &str| headers.get(k).and_then(|v| v.to_str().ok()).unwrap_or("");

    if !verify_signature(&github.webhook_secret, &body, header("x-hub-signature-256")) {
        return StatusCode::UNAUTHORIZED;
    }
    let (event, delivery) = (header("x-github-event").to_owned(), header("x-github-delivery").to_owned());
    if event.is_empty() || delivery.is_empty() {
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

    // Ack fast; GitHub gives webhooks 10s.
    tokio::spawn(async move {
        if let Err(e) = process(&app, &event, &payload).await {
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
                    store.upsert_user(account_id, login, normalize::s(p, "/installation/account/avatar_url")).await?;
                    store.upsert_installation(id, login).await?;
                    let login_lower = login.to_lowercase();
                    run_backfill(app, id, &login_lower).await?;
                    store.rebuild_snapshot(&login_lower).await?;
                    tracing::info!(login, "installation added");
                }
                Some("deleted") | Some("suspend") => {
                    store.remove_installation(id).await?;
                    tracing::info!(login, "installation removed");
                }
                _ => {}
            }
            return Ok(());
        }
        "installation_repositories" => {
            let Some(id) = installation else { return Ok(()) };
            if normalize::s(p, "/action") == Some("added") {
                if let Some(login) = store.login_for_installation(id).await? {
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
            store.patch_pr_deployment(&login, repo, branch, mapped).await?;
            store.rebuild_snapshot(&login).await?;
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
