use std::{collections::BTreeSet, sync::Arc, time::Duration};

use anyhow::Result;

use crate::App;

/// Background polling loop: keeps commits, PRs and issues synced even without a webhook tunnel.
pub fn spawn_background(app: Arc<App>) {
    let secs: u64 = std::env::var("SYNC_INTERVAL_SECS")
        .ok()
        .and_then(|s| s.parse().ok())
        .map(|s: u64| s.max(30))
        .unwrap_or(180);

    tokio::spawn(async move {
        if let Err(err) = sync_all(&app).await {
            tracing::warn!(error = %err, "initial github sync failed");
        }
        let mut ticker = tokio::time::interval(Duration::from_secs(secs));
        ticker.tick().await;
        loop {
            ticker.tick().await;
            if let Err(err) = sync_all(&app).await {
                tracing::warn!(error = %err, "periodic github sync failed");
            }
        }
    });
}

pub async fn sync_all(app: &App) -> Result<()> {
    let Some(github) = app.github.as_ref() else { return Ok(()) };

    if let Ok(slug) = github.fetch_app_slug().await {
        if let Err(err) = app.store.set_app_slug(&slug).await {
            tracing::warn!(error = %err, "failed storing app slug");
        }
    }

    let mut dirty = BTreeSet::new();

    match github.list_app_installations().await {
        Ok(installs) => {
            for inst in installs {
                let login_lower = inst.login.to_lowercase();
                if let Err(err) = app
                    .store
                    .upsert_user(inst.account_id, &login_lower, inst.avatar_url.as_deref())
                    .await
                {
                    tracing::warn!(error = %err, login = %login_lower, "upsert_user failed");
                    continue;
                }
                if let Err(err) = app.store.upsert_installation(inst.id, &login_lower).await {
                    tracing::warn!(error = %err, login = %login_lower, "upsert_installation failed");
                    continue;
                }
                match github.backfill_installation(inst.id).await {
                    Ok(acts) => {
                        for act in acts {
                            if let Err(err) = app
                                .store
                                .upsert_activity(&login_lower, act.kind, &act.key, &act.defaults, &act.base, act.at)
                                .await
                            {
                                tracing::warn!(error = %err, key = %act.key, "upsert activity failed");
                            }
                        }
                        dirty.insert(login_lower);
                    }
                    Err(err) => tracing::warn!(error = %err, installation = inst.id, "installation sync failed"),
                }
            }
        }
        Err(err) => tracing::warn!(error = %err, "list_app_installations failed"),
    }

    for login in app.store.list_user_logins().await? {
        let use_etag = app.store.has_activities(&login).await.unwrap_or(false);
        match github.backfill_user_public(&login, None, use_etag).await {
            Ok(acts) => {
                for act in acts {
                    if let Err(err) = app
                        .store
                        .upsert_activity(&login, act.kind, &act.key, &act.defaults, &act.base, act.at)
                        .await
                    {
                        tracing::warn!(error = %err, key = %act.key, "upsert public activity failed");
                    }
                }
                dirty.insert(login);
            }
            Err(err) => tracing::warn!(error = %err, %login, "public sync failed"),
        }
    }

    for login in dirty {
        if let Err(err) = app.store.rebuild_snapshot(&login).await {
            tracing::warn!(error = %err, %login, "rebuild_snapshot failed");
        }
    }

    Ok(())
}

pub async fn sync_user(app: &App, login: &str, oauth_token: Option<&str>) -> Result<()> {
    let Some(github) = app.github.as_ref() else { return Ok(()) };

    let login_lower = login.trim().to_lowercase();
    if login_lower.is_empty() {
        return Ok(());
    }

    if let Ok(installs) = github.list_app_installations().await {
        for inst in installs.into_iter().filter(|i| i.login.eq_ignore_ascii_case(&login_lower)) {
            let _ = app
                .store
                .upsert_user(inst.account_id, &login_lower, inst.avatar_url.as_deref())
                .await;
            let _ = app.store.upsert_installation(inst.id, &login_lower).await;
            if let Ok(acts) = github.backfill_installation(inst.id).await {
                for act in acts {
                    let _ = app
                        .store
                        .upsert_activity(&login_lower, act.kind, &act.key, &act.defaults, &act.base, act.at)
                        .await;
                }
            }
        }
    }

    match github.backfill_user_public(&login_lower, oauth_token, false).await {
        Ok(acts) => {
            for act in acts {
                app.store
                    .upsert_activity(&login_lower, act.kind, &act.key, &act.defaults, &act.base, act.at)
                    .await?;
            }
        }
        Err(err) => tracing::warn!(error = %err, login = %login_lower, "public user sync failed"),
    }

    app.store.rebuild_snapshot(&login_lower).await
}
