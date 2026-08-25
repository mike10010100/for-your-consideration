#![forbid(unsafe_code)]

//! `for-your-consideration` binary entrypoint.
//!
//! Orchestrates the multi-signal graph store, recommender engine, Jetstream firehose ingester,
//! and Axum XRPC server with graceful shutdown and structured logging.

use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use compact_str::CompactString;
use for_your_consideration::prelude::*;
use tokio::net::TcpListener;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tracing::{error, info, warn};

/// Default server bind port if not overridden by `PORT` env var.
const DEFAULT_PORT: u16 = 3000;
/// Default server bind host if not overridden by `HOST` env var.
const DEFAULT_HOST: &str = "0.0.0.0";
/// Default feed generator service DID if not overridden by `SERVICE_DID` env var.
const DEFAULT_SERVICE_DID: &str = "did:web:feed.example.com";
/// Default feed generator hostname if not overridden by `HOSTNAME` env var.
const DEFAULT_HOSTNAME: &str = "feed.example.com";
/// Graceful shutdown timeout in seconds before aborting background tasks.
const SHUTDOWN_TIMEOUT_SECS: u64 = 10;

#[tokio::main]
async fn main() -> Result<()> {
    // 0. Install process-wide Rustls crypto provider
    let _ = rustls::crypto::ring::default_provider().install_default();

    // 1. Initialize structured logging
    let _ = tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| tracing_subscriber::EnvFilter::new("info")),
        )
        .try_init();

    info!("Starting for-your-consideration AT Protocol Custom Feed Generator v0.1.0");

    // 2. Read runtime configuration from environment
    let host = std::env::var("HOST").unwrap_or_else(|_| DEFAULT_HOST.to_string());
    let port = std::env::var("PORT")
        .ok()
        .and_then(|p| p.parse::<u16>().ok())
        .unwrap_or(DEFAULT_PORT);
    let service_did =
        std::env::var("SERVICE_DID").unwrap_or_else(|_| DEFAULT_SERVICE_DID.to_string());
    let hostname = std::env::var("HOSTNAME").unwrap_or_else(|_| DEFAULT_HOSTNAME.to_string());
    let jetstream_url =
        std::env::var("JETSTREAM_URL").unwrap_or_else(|_| DEFAULT_JETSTREAM_URL.to_string());
    let enable_ingestion =
        std::env::var("ENABLE_INGESTION").map_or(true, |v| v != "false" && v != "0");

    let bind_addr: SocketAddr = format!("{host}:{port}")
        .parse()
        .map_err(|e| FeedError::Server(format!("Invalid bind address '{host}:{port}': {e}")))?;

    // Snapshot configuration
    let snapshot_path = std::env::var("SNAPSHOT_PATH").map_or_else(
        |_| std::path::PathBuf::from("snapshot.bin"),
        std::path::PathBuf::from,
    );
    let snapshot_interval_secs = std::env::var("SNAPSHOT_INTERVAL_SECS")
        .ok()
        .and_then(|p| p.parse::<u64>().ok())
        .unwrap_or(300);
    let snapshot_config = SnapshotConfig {
        path: snapshot_path,
        interval_secs: snapshot_interval_secs,
    };

    // 3. Initialize core domain services
    let interner = Arc::new(StringInterner::new());
    let graph = Arc::new(GraphStore::new());
    let preferences_store = Arc::new(UserPreferencesStore::new());
    let recommender = Arc::new(Recommender::new(Arc::clone(&interner), Arc::clone(&graph)));
    let snapshot_tracker = Arc::new(SnapshotStatusTracker::new(&snapshot_config));

    // Hydrate snapshot on boot if it exists
    let restored_cursor = if snapshot_config.path.exists() {
        match load_snapshot_with_preferences(
            &snapshot_config.path,
            &interner,
            &graph,
            &preferences_store,
        ) {
            Ok(Some(loaded)) => {
                info!(
                    duration_ms = loaded.load_duration_ms,
                    strings = loaded.header.num_strings,
                    users = loaded.header.num_users,
                    edges = loaded.header.total_forward_edges,
                    preferences = loaded.header.num_preferences,
                    cursor = loaded.header.jetstream_cursor_us,
                    "Hydrated snapshot successfully from '{}'",
                    snapshot_config.path.display()
                );
                snapshot_tracker.record_load(loaded.load_duration_ms);
                if loaded.header.jetstream_cursor_us > 0 {
                    Some(loaded.header.jetstream_cursor_us)
                } else {
                    None
                }
            }
            Ok(None) => {
                info!(
                    "Snapshot file '{}' does not exist; starting with clean state",
                    snapshot_config.path.display()
                );
                None
            }
            Err(err) => {
                warn!(
                    error = %err,
                    "Failed to load snapshot from '{}'; starting with clean state",
                    snapshot_config.path.display()
                );
                None
            }
        }
    } else {
        info!(
            "No snapshot found at '{}'; starting with clean state",
            snapshot_config.path.display()
        );
        None
    };

    // 4. Initialize Jetstream Ingester & Ingestion Tracker with Historical Replay
    let backfill_hours: u64 = std::env::var("REPLAY_HOURS")
        .or_else(|_| std::env::var("BACKFILL_HOURS"))
        .ok()
        .and_then(|h| h.parse::<u64>().ok())
        .unwrap_or(12);

    let now_epoch_secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();

    // Default backfill start: now - backfill_hours
    let backfill_start_us = now_epoch_secs
        .saturating_sub(backfill_hours.saturating_mul(3600))
        .saturating_mul(1_000_000);

    // Oldest safe cursor clamped to the requested replay/backfill window
    let oldest_safe_cursor_us = backfill_start_us;

    let effective_cursor = match restored_cursor {
        Some(cursor) if cursor > 0 => {
            let safe_cursor = cursor.max(oldest_safe_cursor_us);
            if safe_cursor > cursor {
                info!(
                    snapshot_cursor = cursor,
                    clamped_cursor = safe_cursor,
                    backfill_hours = backfill_hours,
                    "Snapshot cursor was older than requested {backfill_hours}h replay window; clamped to {backfill_hours}h ago"
                );
            } else {
                let downtime_secs = now_epoch_secs.saturating_sub(cursor / 1_000_000);
                info!(
                    cursor_us = cursor,
                    downtime_secs = downtime_secs,
                    "Resuming Jetstream ingestion from snapshot cursor (replaying {downtime_secs}s of downtime gap)"
                );
            }
            Some(safe_cursor)
        }
        _ => {
            if backfill_hours > 0 {
                info!(
                    backfill_hours = backfill_hours,
                    cursor_us = backfill_start_us,
                    "Initiating {backfill_hours}-hour historical replay backfill from Jetstream"
                );
                Some(backfill_start_us)
            } else {
                info!("Starting Jetstream ingestion from live stream head");
                None
            }
        }
    };

    let ingester_config = IngesterConfig {
        jetstream_url: CompactString::new(&jetstream_url),
        initial_cursor: effective_cursor,
        ..IngesterConfig::default()
    };
    let ingester =
        JetstreamIngester::new(ingester_config, Arc::clone(&interner), Arc::clone(&graph));
    let ingestion_tracker = Arc::new(IngestionTracker::new(Arc::clone(ingester.stats())));

    let feed_rkey = std::env::var("FEED_RKEY").unwrap_or_else(|_| DEFAULT_FEED_RKEY.to_string());

    // 5. Initialize Axum XRPC server with trackers
    let app_state = AppState::new(
        Arc::clone(&recommender),
        CompactString::new(&service_did),
        CompactString::new(&hostname),
    )
    .with_preferences_store(Arc::clone(&preferences_store))
    .with_feed_rkey(CompactString::new(&feed_rkey))
    .with_snapshot_tracker(Arc::clone(&snapshot_tracker))
    .with_ingestion_tracker(Arc::clone(&ingestion_tracker));

    let router = create_xrpc_router(app_state);
    let listener = TcpListener::bind(bind_addr).await.map_err(FeedError::Io)?;
    info!("XRPC HTTP server bound to http://{bind_addr}");

    // 6. Setup cancellation tokens and JoinSet for task lifecycle management
    let cancel_token = CancellationToken::new();
    let mut tasks = JoinSet::new();

    // Spawn XRPC Server task
    let server_token = cancel_token.clone();
    tasks.spawn(async move {
        if let Err(e) = serve_xrpc(listener, router, server_token).await {
            error!("XRPC server error: {e}");
        }
    });

    // Spawn Jetstream Ingestion task
    if enable_ingestion {
        let ingest_token = cancel_token.clone();
        let ingester_task = ingester.clone();
        tasks.spawn(async move {
            info!("Starting Jetstream real-time firehose consumer...");
            if let Err(e) = ingester_task.run(ingest_token).await {
                error!("Jetstream ingester error: {e}");
            } else {
                info!("Jetstream ingester shut down cleanly.");
            }
        });
    }

    // Spawn Periodic Snapshot Checkpoint task
    let snapshot_interner = Arc::clone(&interner);
    let snapshot_graph = Arc::clone(&graph);
    let snapshot_preferences = Arc::clone(&preferences_store);
    let snapshot_cancel = cancel_token.clone();
    let snapshot_path = snapshot_config.path.clone();
    let snapshot_interval = Duration::from_secs(snapshot_config.interval_secs);
    let snapshot_ingester_stats = Arc::clone(ingester.stats());
    let periodic_snapshot_tracker = Arc::clone(&snapshot_tracker);

    tasks.spawn(async move {
        let mut interval = tokio::time::interval(snapshot_interval);
        interval.tick().await; // first tick fires immediately
        loop {
            tokio::select! {
                () = snapshot_cancel.cancelled() => {
                    info!("Snapshot periodic task received shutdown cancellation");
                    break;
                }
                _ = interval.tick() => {
                    // Periodic memory bounding: Prune edges older than retention window (default: 30 days)
                    let now_secs = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_secs();
                    let retention_days: u64 = std::env::var("RETENTION_DAYS")
                        .ok()
                        .and_then(|d| d.parse::<u64>().ok())
                        .unwrap_or(30);
                    let prune_cutoff = now_secs.saturating_sub(retention_days.saturating_mul(86400));
                    snapshot_graph.prune_older_than(prune_cutoff);

                    tracing::debug!("Triggering periodic snapshot checkpoint");
                    let current_cursor = snapshot_ingester_stats.latest_cursor_us.load(std::sync::atomic::Ordering::Relaxed);
                    let start_save = std::time::Instant::now();
                    match save_snapshot_with_preferences(&snapshot_path, &snapshot_interner, &snapshot_graph, &snapshot_preferences, current_cursor) {
                        Ok(_) => {
                            let duration_ms = start_save.elapsed().as_secs_f64() * 1000.0;
                            let file_size = snapshot_path.metadata().map_or(0, |m| m.len());
                            periodic_snapshot_tracker.record_save(duration_ms, file_size);
                            tracing::debug!("Periodic snapshot checkpoint saved successfully in {duration_ms:.2} ms");
                        }
                        Err(e) => {
                            periodic_snapshot_tracker.record_save_failure(&e.to_string());
                            warn!(error = %e, "Periodic snapshot save failed");
                        }
                    }
                }
            }
        }
    });

    // 7. Wait for shutdown signal (SIGINT / SIGTERM)
    wait_for_shutdown_signal().await;
    info!("Shutdown signal received. Initiating graceful drain...");

    // 8. Trigger cooperative cancellation across all tasks
    cancel_token.cancel();

    // 9. Await task completion with timeout safety
    let shutdown_timeout = Duration::from_secs(SHUTDOWN_TIMEOUT_SECS);
    let drain_result = tokio::time::timeout(shutdown_timeout, async {
        while let Some(res) = tasks.join_next().await {
            if let Err(e) = res {
                warn!("Task failed during shutdown: {e}");
            }
        }
    })
    .await;

    if drain_result.is_err() {
        warn!(
            "Graceful shutdown timed out after {SHUTDOWN_TIMEOUT_SECS}s; aborting remaining tasks."
        );
        tasks.abort_all();
    } else {
        info!("All background tasks shut down cleanly.");
    }

    // 10. Persist final snapshot on graceful shutdown
    let final_cursor = ingester.latest_cursor();
    info!("Persisting final snapshot on graceful shutdown...");
    let start_final = std::time::Instant::now();
    match save_snapshot_with_preferences(
        &snapshot_config.path,
        &interner,
        &graph,
        &preferences_store,
        final_cursor,
    ) {
        Ok(_) => {
            let duration_ms = start_final.elapsed().as_secs_f64() * 1000.0;
            let file_size = snapshot_config.path.metadata().map_or(0, |m| m.len());
            snapshot_tracker.record_save(duration_ms, file_size);
            info!(
                "Final shutdown snapshot saved successfully to '{}' in {duration_ms:.2} ms",
                snapshot_config.path.display()
            );
        }
        Err(e) => {
            snapshot_tracker.record_save_failure(&e.to_string());
            error!(error = %e, "Final shutdown snapshot save failed");
        }
    }

    info!("for-your-consideration shutdown complete. Goodbye!");

    Ok(())
}

/// Waits for OS termination signals (SIGINT / SIGTERM).
async fn wait_for_shutdown_signal() {
    let ctrl_c = async {
        if let Err(err) = tokio::signal::ctrl_c().await {
            error!("Failed to listen for Ctrl+C signal: {err}");
        }
    };

    #[cfg(unix)]
    let terminate = async {
        match tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate()) {
            Ok(mut signal) => {
                signal.recv().await;
            }
            Err(err) => {
                error!("Failed to register SIGTERM listener: {err}");
                std::future::pending::<()>().await;
            }
        }
    };

    #[cfg(not(unix))]
    let terminate = std::future::pending::<()>();

    tokio::select! {
        () = ctrl_c => {
            info!("Received SIGINT (Ctrl+C)");
        }
        () = terminate => {
            info!("Received SIGTERM");
        }
    }
}
