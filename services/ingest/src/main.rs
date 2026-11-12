mod config;
mod discord;
mod github;
mod metrics;
mod normalize;
mod store;
mod sync;
mod webhook;

use std::sync::Arc;

use anyhow::Result;

pub struct App {
    pub config: config::Config,
    pub store: store::Store,
    pub github: Option<github::GitHub>,
    pub metrics: Arc<metrics::IngestMetrics>,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt()
        .with_max_level(tracing::Level::INFO)
        .init();

    let config = config::Config::from_env()?;
    let store = store::Store::connect(&config.database_url, &config.redis_url).await?;

    let metrics = Arc::new(metrics::IngestMetrics::default());

    let github = match &config.github {
        Some(gh) => Some(
            github::GitHub::new(&gh.app_id, &gh.private_key)?
                .with_infra(store.clone(), Arc::clone(&metrics)),
        ),
        None => None,
    };

    let port = config.port;
    let github_enabled = config.github.is_some();
    let discord_enabled = config.discord.is_some();
    let app = Arc::new(App {
        config,
        store,
        github,
        metrics,
    });

    if github_enabled {
        sync::spawn_background(Arc::clone(&app));
    } else {
        tracing::info!("GitHub integration disabled (GITHUB_APP_ID not set)");
    }

    if discord_enabled {
        discord::spawn(Arc::clone(&app));
    } else {
        tracing::info!("Discord integration disabled (DISCORD_BOT_TOKEN not set)");
    }

    let listener = tokio::net::TcpListener::bind(("0.0.0.0", port)).await?;
    tracing::info!(
        port,
        github = github_enabled,
        discord = discord_enabled,
        "taggle-ingest listening"
    );
    axum::serve(listener, webhook::router(app))
        .with_graceful_shutdown(async {
            let _ = tokio::signal::ctrl_c().await;
        })
        .await?;
    Ok(())
}
