#![forbid(unsafe_code)]
#![deny(
    clippy::all,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::todo,
    clippy::unimplemented,
    missing_docs,
    rust_2018_idioms
)]

//! `for-your-consideration` binary entrypoint.
//!
//! Orchestrates the multi-signal graph store, recommender engine, Jetstream firehose ingester,
//! and Axum XRPC server with graceful shutdown and structured logging.

use std::net::SocketAddr;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Duration;

#[cfg(feature = "jemalloc")]
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

#[cfg(all(feature = "mimalloc", not(feature = "jemalloc")))]
#[global_allocator]
static GLOBAL: mimalloc::MiMalloc = mimalloc::MiMalloc;

use compact_str::CompactString;
use for_your_consideration::prelude::*;
use rand::Rng;
use sha2::Digest;
use tokio::net::TcpListener;
use tokio::sync::Mutex as AsyncMutex;
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
/// Default graph retention in days if not overridden by `RETENTION_DAYS` env var.
const DEFAULT_RETENTION_DAYS: u64 = 7;
/// Default snapshot interval in seconds if not overridden by `SNAPSHOT_INTERVAL_SECS` env var.
const DEFAULT_SNAPSHOT_INTERVAL_SECS: u64 = 14400;
/// Default periodic store pruning interval in seconds if not overridden by `PRUNE_INTERVAL_SECS` env var.
const DEFAULT_PRUNE_INTERVAL_SECS: u64 = 900;

/// Default timeout in seconds for the final shutdown snapshot save. Must fit inside Docker's
/// `stop_grace_period` after the bounded drain; override with `SHUTDOWN_SAVE_TIMEOUT_SECS`.
const DEFAULT_SHUTDOWN_SAVE_TIMEOUT_SECS: u64 = 240;

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

    info!(
        "Starting for-your-consideration AT Protocol Custom Feed Generator v{}",
        env!("CARGO_PKG_VERSION")
    );

    // 2. Read runtime configuration from environment
    let host = std::env::var("HOST").unwrap_or_else(|_| DEFAULT_HOST.to_string());
    let port = std::env::var("PORT")
        .ok()
        .and_then(|p| p.parse::<u16>().ok())
        .unwrap_or(DEFAULT_PORT);
    let service_did =
        std::env::var("SERVICE_DID").unwrap_or_else(|_| DEFAULT_SERVICE_DID.to_string());
    // Prefer FEED_HOSTNAME: the generic HOSTNAME variable is exported by most shells and
    // CI environments (e.g. "MacBook-Pro.local"), which would silently publish a wrong
    // hostname in the DID document and OAuth client metadata.
    let hostname = std::env::var("FEED_HOSTNAME")
        .or_else(|_| std::env::var("HOSTNAME"))
        .unwrap_or_else(|_| DEFAULT_HOSTNAME.to_string());
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
        .unwrap_or(DEFAULT_SNAPSHOT_INTERVAL_SECS);
    let snapshot_config = SnapshotConfig {
        path: snapshot_path,
        interval_secs: snapshot_interval_secs,
    };

    // Final shutdown save timeout (seconds). Must fit inside Docker's `stop_grace_period` with
    // margin for the drain phase; env-configurable so operators can raise it alongside the grace
    // period for very large stores.
    let shutdown_save_timeout_secs = std::env::var("SHUTDOWN_SAVE_TIMEOUT_SECS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .filter(|&v| v > 0)
        .unwrap_or(DEFAULT_SHUTDOWN_SAVE_TIMEOUT_SECS);

    // 3. Initialize core domain services
    let interner = Arc::new(StringInterner::new());
    let graph = Arc::new(GraphStore::new());
    let preferences_store = Arc::new(UserPreferencesStore::new());
    let recommender = Arc::new(Recommender::new(Arc::clone(&interner), Arc::clone(&graph)));
    let snapshot_tracker = Arc::new(SnapshotStatusTracker::new(&snapshot_config));

    let retention_days: u64 = std::env::var("RETENTION_DAYS")
        .ok()
        .and_then(|d| d.parse::<u64>().ok())
        .unwrap_or(DEFAULT_RETENTION_DAYS);

    // 4. Initialize Jetstream Ingester & Ingestion Tracker (cursor updated once snapshot hydration completes)
    let backfill_hours: u64 = std::env::var("REPLAY_HOURS")
        .or_else(|_| std::env::var("BACKFILL_HOURS"))
        .ok()
        .and_then(|h| h.parse::<u64>().ok())
        .unwrap_or(12);

    let parallel_slicing = std::env::var("PARALLEL_SLICING")
        .or_else(|_| std::env::var("PARALLEL_STREAMS"))
        .or_else(|_| std::env::var("ENABLE_PARALLEL_SLICING"))
        .map_or(true, |v| v != "false" && v != "0");

    let jetstream_endpoints: Vec<CompactString> = std::env::var("JETSTREAM_ENDPOINTS").map_or_else(
        |_| {
            if std::env::var("JETSTREAM_URL").is_ok() {
                vec![CompactString::new(&jetstream_url)]
            } else {
                DEFAULT_JETSTREAM_ENDPOINTS
                    .iter()
                    .map(|&ep| CompactString::new(ep))
                    .collect()
            }
        },
        |eps| {
            eps.split(',')
                .map(|s| CompactString::new(s.trim()))
                .filter(|s| !s.is_empty())
                .collect()
        },
    );

    let ingester_config = IngesterConfig {
        jetstream_url: CompactString::new(&jetstream_url),
        jetstream_endpoints,
        parallel_slicing,
        initial_cursor: None,
        ..IngesterConfig::default()
    };

    if ingester_config.parallel_slicing && ingester_config.wanted_collections.len() > 1 {
        info!(
            endpoints = ?ingester_config.jetstream_endpoints,
            collections = ?ingester_config.wanted_collections,
            "Jetstream ingestion configured with parallel multi-stream collection slicing"
        );
    } else {
        info!(
            url = %ingester_config.jetstream_url,
            collections = ?ingester_config.wanted_collections,
            "Jetstream ingestion configured with single-stream fallback"
        );
    }

    let ingester =
        JetstreamIngester::new(ingester_config, Arc::clone(&interner), Arc::clone(&graph));
    let ingestion_tracker = Arc::new(IngestionTracker::new(Arc::clone(ingester.stats())));

    // Resolve optional administrator identity (ADMIN_DID or ADMIN_HANDLE)
    let admin_did = if let Ok(did) = std::env::var("ADMIN_DID") {
        let trimmed = did.trim();
        if trimmed.is_empty() {
            None
        } else {
            Some(CompactString::new(trimmed))
        }
    } else if let Ok(handle) = std::env::var("ADMIN_HANDLE") {
        let trimmed = handle.trim().trim_start_matches('@');
        if trimmed.is_empty() {
            None
        } else if trimmed.starts_with("did:") {
            Some(CompactString::new(trimmed))
        } else {
            match resolve_identity_pds(trimmed).await {
                Ok(identity) => {
                    info!(
                        handle = %trimmed,
                        did = %identity.did,
                        "Resolved admin handle to DID"
                    );
                    Some(identity.did)
                }
                Err(e) => {
                    warn!(
                        handle = %trimmed,
                        error = %e,
                        "Failed to resolve ADMIN_HANDLE to DID; continuing without admin restriction. \
                         Set ADMIN_DID explicitly to skip resolution."
                    );
                    None
                }
            }
        }
    } else {
        None
    };

    if let Some(ref did) = admin_did {
        info!(admin_did = %did, "Feed generator publishing locked to administrator DID");
    }

    let feed_rkey = std::env::var("FEED_RKEY").unwrap_or_else(|_| DEFAULT_FEED_RKEY.to_string());
    // Canonical record URI (at://<publisher-did>/app.bsky.feed.generator/<rkey>). The record
    // lives in the publisher's repo, which the service DID cannot express, so deployments
    // that publish under a specific account should set FEED_URI explicitly.
    let feed_uri = std::env::var("FEED_URI")
        .ok()
        .map(|u| u.trim().to_string())
        .filter(|u| !u.is_empty());

    let (session_secret, is_custom_secret) = std::env::var("SESSION_SECRET").map_or_else(
        |_| {
            // Fail fast in release builds: an ephemeral key silently invalidates every
            // signed session on restart and prevents multi-instance deployments.
            // Development builds fall back to a generated ephemeral key.
            if !cfg!(debug_assertions) {
                return Err(FeedError::Server(
                    "SESSION_SECRET environment variable must be set in release deployments \
                     (generate with: openssl rand -hex 32). Refusing to start with an ephemeral key."
                        .to_string(),
                ));
            }
            let mut key = [0u8; 32];
            rand::rng().fill_bytes(&mut key);
            Ok((key, false))
        },
        |sec| {
            let trimmed = sec.trim();
            if trimmed.is_empty() {
                if !cfg!(debug_assertions) {
                    return Err(FeedError::Server(
                        "SESSION_SECRET environment variable is empty in a release deployment \
                         (generate with: openssl rand -hex 32). Refusing to start."
                            .to_string(),
                    ));
                }
                let mut key = [0u8; 32];
                rand::rng().fill_bytes(&mut key);
                Ok((key, false))
            } else {
                let hash = sha2::Sha256::digest(trimmed.as_bytes());
                let mut key = [0u8; 32];
                key.copy_from_slice(&hash);
                Ok((key, true))
            }
        },
    )?;

    if is_custom_secret {
        info!("Custom HMAC session signing secret loaded from environment");
    } else {
        info!("Generated cryptographically secure ephemeral 256-bit session key for this runtime instance");
    }

    // 5. Initialize Axum XRPC server with trackers
    let mut app_state = AppState::new(
        Arc::clone(&recommender),
        CompactString::new(&service_did),
        CompactString::new(&hostname),
    )
    .with_session_secret(session_secret)
    .with_preferences_store(Arc::clone(&preferences_store))
    .with_feed_rkey(CompactString::new(&feed_rkey))
    .with_admin_did(admin_did)
    .with_snapshot_tracker(Arc::clone(&snapshot_tracker))
    .with_ingestion_tracker(Arc::clone(&ingestion_tracker));

    if let Some(uri) = feed_uri {
        info!(feed_uri = %uri, "describeFeedGenerator will advertise configured FEED_URI");
        app_state = app_state.with_feed_uri(uri);
    }

    // Service Auth JWT validation mode (SEC: ES256K signature verification).
    // Default is legacy payload-only; operators opt into enforcement once their
    // AppView traffic is confirmed to carry signed service tokens.
    let service_auth_mode = std::env::var("SERVICE_AUTH_MODE")
        .ok()
        .map(|v| v.trim().to_ascii_lowercase())
        .map_or(ServiceAuthMode::Legacy, |v| match v.as_str() {
            "enforce" | "strict" | "verify" => {
                info!("Service Auth: enforcing ES256K signature verification on getFeedSkeleton");
                ServiceAuthMode::Enforce
            }
            "legacy" | "off" | "payload-only" => {
                info!("Service Auth: legacy payload-only JWT validation (no signature check)");
                ServiceAuthMode::Legacy
            }
            other => {
                warn!(
                    value = %other,
                    "Unknown SERVICE_AUTH_MODE; defaulting to legacy payload-only validation"
                );
                ServiceAuthMode::Legacy
            }
        });
    app_state = app_state.with_service_auth_mode(service_auth_mode);

    let prune_oauth_store = Arc::clone(&app_state.oauth_store);
    let prune_user_oauth_sessions = Arc::clone(&app_state.user_oauth_sessions);
    let prune_active_users = Arc::clone(&app_state.active_users_tracker);
    let snapshot_oauth_store = Arc::clone(&app_state.oauth_store);
    let snapshot_user_oauth_sessions = Arc::clone(&app_state.user_oauth_sessions);
    let snapshot_active_users = Arc::clone(&app_state.active_users_tracker);
    let router = create_xrpc_router(app_state);
    let listener = TcpListener::bind(bind_addr).await.map_err(FeedError::Io)?;
    info!("XRPC HTTP server bound to http://{bind_addr}");

    // 6. Setup cancellation tokens and JoinSet for task lifecycle management.
    // The HTTP listener uses a dedicated cancellation token so it stays online and continues
    // serving live feed traffic while the final shutdown snapshot is written to disk.
    let cancel_token = CancellationToken::new();
    let http_cancel_token = CancellationToken::new();
    let mut tasks = JoinSet::new();

    // Spawn XRPC Server task immediately so `/healthz` and feed endpoints are available within < 0.5s
    let server_token = http_cancel_token.clone();
    let http_server_handle = tokio::spawn(async move {
        if let Err(e) = serve_xrpc(listener, router, server_token).await {
            error!("XRPC server error: {e}");
        }
    });

    // Serializes background startup hydration, periodic prune/compaction, periodic snapshot,
    // and shutdown snapshot writes so maintenance tasks never contend on graph locks simultaneously
    // or race the `.tmp` path.
    let snapshot_save_slot: Arc<AsyncMutex<()>> = Arc::new(AsyncMutex::new(()));
    let hydration_completed = Arc::new(AtomicBool::new(!snapshot_config.path.exists()));
    let baseline_compacted_strings = Arc::new(AtomicUsize::new(0));

    // Spawn Background Snapshot Hydration, Firehose Ingestion, and Conditional Startup Compaction
    {
        let boot_cancel = cancel_token.clone();
        let boot_save_slot = Arc::clone(&snapshot_save_slot);
        let boot_interner = Arc::clone(&interner);
        let boot_graph = Arc::clone(&graph);
        let boot_preferences = Arc::clone(&preferences_store);
        let boot_snapshot_path = snapshot_config.path.clone();
        let boot_snapshot_tracker = Arc::clone(&snapshot_tracker);
        let boot_hydration_completed = Arc::clone(&hydration_completed);
        let boot_baseline_strings = Arc::clone(&baseline_compacted_strings);
        let boot_ingester = ingester.clone();

        tasks.spawn(async move {
            // Step A: Hydrate snapshot from disk and prune expired edges in background
            let hydrate_interner = Arc::clone(&boot_interner);
            let hydrate_graph = Arc::clone(&boot_graph);
            let hydrate_preferences = Arc::clone(&boot_preferences);
            let hydrate_path = boot_snapshot_path.clone();
            let hydrate_tracker = Arc::clone(&boot_snapshot_tracker);
            let hydrate_slot = Arc::clone(&boot_save_slot);

            let hydrate_res = tokio::task::spawn_blocking(move || {
                let _guard = hydrate_slot.blocking_lock();
                let restored_cursor = if hydrate_path.exists() {
                    match load_snapshot_with_preferences(
                        &hydrate_path,
                        &hydrate_interner,
                        &hydrate_graph,
                        &hydrate_preferences,
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
                                hydrate_path.display()
                            );
                            hydrate_tracker.record_load(loaded.load_duration_ms);
                            if loaded.header.jetstream_cursor_us > 0 {
                                Some(loaded.header.jetstream_cursor_us)
                            } else {
                                None
                            }
                        }
                        Ok(None) => {
                            info!(
                                "Snapshot file '{}' does not exist; starting with clean state",
                                hydrate_path.display()
                            );
                            None
                        }
                        Err(err) => {
                            warn!(
                                error = %err,
                                "Failed to load snapshot from '{}'; starting with clean state",
                                hydrate_path.display()
                            );
                            None
                        }
                    }
                } else {
                    info!(
                        "No snapshot found at '{}'; starting with clean state",
                        hydrate_path.display()
                    );
                    None
                };

                let now_epoch_secs = std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap_or_default()
                    .as_secs();

                if restored_cursor.is_some() {
                    let prune_cutoff =
                        now_epoch_secs.saturating_sub(retention_days.saturating_mul(86400));
                    let prune_start = std::time::Instant::now();
                    hydrate_graph.prune_older_than(prune_cutoff);
                    let prune_stats = hydrate_graph.stats();
                    let prune_duration_ms = prune_start.elapsed().as_secs_f64() * 1000.0;
                    info!(
                        retention_days = retention_days,
                        prune_cutoff = prune_cutoff,
                        edges_retained = prune_stats.total_interactions,
                        users_retained = prune_stats.total_users,
                        duration_ms = prune_duration_ms,
                        "Startup graph retention prune completed successfully before Jetstream ingestion"
                    );
                }

                (restored_cursor, now_epoch_secs)
            })
            .await;

            let (restored_cursor, now_epoch_secs) = match hydrate_res {
                Ok(pair) => pair,
                Err(join_err) => {
                    error!(error = %join_err, "Startup snapshot hydration spawn_blocking join error");
                    return;
                }
            };

            boot_baseline_strings.store(boot_interner.len(), Ordering::Release);
            boot_hydration_completed.store(true, Ordering::Release);

            if boot_cancel.is_cancelled() {
                return;
            }

            // Step B: Spawn conditional background memory compaction ONLY if dead strings exceed
            // hysteresis threshold, without blocking Jetstream firehose catch-up.
            let active_nodes = boot_graph.total_nodes();
            let current_strings = boot_interner.len();
            if restored_cursor.is_some()
                && current_strings > active_nodes.saturating_mul(2).max(100_000)
            {
                let compact_interner = Arc::clone(&boot_interner);
                let compact_graph = Arc::clone(&boot_graph);
                let compact_preferences = Arc::clone(&boot_preferences);
                let compact_slot = Arc::clone(&boot_save_slot);
                let compact_baseline = Arc::clone(&boot_baseline_strings);
                tokio::spawn(async move {
                    let compact_res = tokio::task::spawn_blocking(move || {
                        let _guard = compact_slot.blocking_lock();
                        for_your_consideration::snapshot::compact_memory_stores(
                            &compact_interner,
                            &compact_graph,
                            &compact_preferences,
                        )
                    })
                    .await;
                    if let Ok(compact_stats) = compact_res {
                        compact_baseline.store(compact_stats.strings_after, Ordering::Release);
                        info!(
                            strings_before = compact_stats.strings_before,
                            strings_after = compact_stats.strings_after,
                            duration_ms = compact_stats.duration_ms,
                            "Asynchronous startup memory store compaction completed"
                        );
                    }
                });
            }

            // Step C: Start Jetstream Ingestion immediately
            if enable_ingestion {
                let backfill_start_us = now_epoch_secs
                    .saturating_sub(backfill_hours.saturating_mul(3600))
                    .saturating_mul(1_000_000);
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

                let ingester_task = boot_ingester.with_initial_cursor(effective_cursor);
                info!("Starting Jetstream real-time firehose consumer...");
                if let Err(e) = ingester_task.run(boot_cancel).await {
                    error!("Jetstream ingester error: {e}");
                } else {
                    info!("Jetstream ingester shut down cleanly.");
                }
            }
        });
    }

    // Spawn Periodic Store Pruning task (SEC-06 memory bounding)
    let prune_interner = Arc::clone(&interner);
    let prune_graph = Arc::clone(&graph);
    let prune_preferences = Arc::clone(&preferences_store);
    let prune_impression_store = Arc::clone(&recommender.impression_store);
    let prune_cancel = cancel_token.clone();
    let prune_save_slot = Arc::clone(&snapshot_save_slot);
    let prune_hydration_done = Arc::clone(&hydration_completed);
    let prune_baseline_strings = Arc::clone(&baseline_compacted_strings);
    let prune_interval_secs = std::env::var("PRUNE_INTERVAL_SECS")
        .ok()
        .and_then(|v| v.parse::<u64>().ok())
        .unwrap_or(DEFAULT_PRUNE_INTERVAL_SECS);
    let prune_interval = Duration::from_secs(prune_interval_secs);

    tasks.spawn(async move {
        let mut interval = tokio::time::interval(prune_interval);
        interval.tick().await; // Consume initial immediate tick (startup prune already ran)
        loop {
            tokio::select! {
                () = prune_cancel.cancelled() => {
                    info!("Periodic store pruning task received shutdown cancellation");
                    break;
                }
                _ = interval.tick() => {
                    if !prune_hydration_done.load(Ordering::Acquire) {
                        continue;
                    }
                    let now_secs = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_secs();
                    let retention_days: u64 = std::env::var("RETENTION_DAYS")
                        .ok()
                        .and_then(|d| d.parse::<u64>().ok())
                        .unwrap_or(DEFAULT_RETENTION_DAYS);
                    let prune_cutoff = now_secs.saturating_sub(retention_days.saturating_mul(86400));

                    let p_interner = Arc::clone(&prune_interner);
                    let p_graph = Arc::clone(&prune_graph);
                    let p_preferences = Arc::clone(&prune_preferences);
                    let p_impressions = Arc::clone(&prune_impression_store);
                    let p_oauth = Arc::clone(&prune_oauth_store);
                    let p_sessions = Arc::clone(&prune_user_oauth_sessions);
                    let p_active = Arc::clone(&prune_active_users);
                    let p_slot = Arc::clone(&prune_save_slot);
                    let baseline_strings = prune_baseline_strings.load(Ordering::Acquire);

                    let prune_res = tokio::task::spawn_blocking(move || {
                        let _guard = p_slot.blocking_lock();
                        let prune_start = std::time::Instant::now();
                        p_graph.prune_older_than(prune_cutoff);
                        p_impressions.prune_expired(now_secs);
                        p_oauth.prune_expired(
                            for_your_consideration::auth::DEFAULT_OAUTH_STATE_TTL_SECS,
                            now_secs,
                        );
                        p_sessions.prune_expired(now_secs);
                        p_active.prune_older_than(now_secs.saturating_sub(900));
                        let prune_duration_ms = prune_start.elapsed().as_secs_f64() * 1000.0;
                        tracing::debug!(
                            retention_days = retention_days,
                            prune_cutoff = prune_cutoff,
                            duration_ms = prune_duration_ms,
                            "Periodic memory pruning pass completed"
                        );

                        let active_nodes = p_graph.total_nodes();
                        let current_strings = p_interner.len();
                        let growth_threshold = baseline_strings
                            .saturating_add(baseline_strings / 2)
                            .max(100_000);
                        if current_strings > active_nodes.saturating_mul(2).max(100_000)
                            && current_strings > growth_threshold
                        {
                            let compact_stats =
                                for_your_consideration::snapshot::compact_memory_stores(
                                    &p_interner,
                                    &p_graph,
                                    &p_preferences,
                                );
                            info!(
                                strings_before = compact_stats.strings_before,
                                strings_after = compact_stats.strings_after,
                                duration_ms = compact_stats.duration_ms,
                                "Periodic memory compaction completed"
                            );
                            Some(compact_stats.strings_after)
                        } else {
                            None
                        }
                    })
                    .await;

                    match prune_res {
                        Ok(Some(new_baseline)) => {
                            prune_baseline_strings.store(new_baseline, Ordering::Release);
                        }
                        Ok(None) => {}
                        Err(join_err) => {
                            warn!(error = %join_err, "Periodic prune spawn_blocking join error");
                        }
                    }
                }
            }
        }
    });

    // Spawn Periodic Snapshot Checkpoint and Store Pruning task
    let snapshot_interner = Arc::clone(&interner);
    let snapshot_graph = Arc::clone(&graph);
    let snapshot_preferences = Arc::clone(&preferences_store);
    let snapshot_impression_store = Arc::clone(&recommender.impression_store);
    let snapshot_cancel = cancel_token.clone();
    let snapshot_path = snapshot_config.path.clone();
    let snapshot_interval = Duration::from_secs(snapshot_config.interval_secs);
    let snapshot_ingester_stats = Arc::clone(ingester.stats());
    let periodic_snapshot_tracker = Arc::clone(&snapshot_tracker);
    let periodic_save_slot = Arc::clone(&snapshot_save_slot);
    let snapshot_hydration_done = Arc::clone(&hydration_completed);

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
                    if !snapshot_hydration_done.load(Ordering::Acquire) {
                        continue;
                    }
                    // Periodic memory bounding: Prune stores (SEC-06) inside spawn_blocking
                    let now_secs = std::time::SystemTime::now()
                        .duration_since(std::time::UNIX_EPOCH)
                        .unwrap_or_default()
                        .as_secs();
                    let retention_days: u64 = std::env::var("RETENTION_DAYS")
                        .ok()
                        .and_then(|d| d.parse::<u64>().ok())
                        .unwrap_or(DEFAULT_RETENTION_DAYS);
                    let prune_cutoff = now_secs.saturating_sub(retention_days.saturating_mul(86400));

                    // NOTE: Compaction is intentionally NOT run here. It is an O(data) rebuild that
                    // can take minutes on large stores and would both delay this checkpoint and
                    // block a concurrent shutdown. Dead-string memory reclamation is handled by the
                    // dedicated periodic prune task (`PRUNE_INTERVAL_SECS`), which compacts only when
                    // the interner grows beyond the active-node threshold.

                    tracing::debug!("Triggering periodic snapshot checkpoint");
                    let current_cursor = snapshot_ingester_stats
                        .latest_cursor_us
                        .load(std::sync::atomic::Ordering::Relaxed);
                    let start_save = std::time::Instant::now();
                    let snap_path = snapshot_path.clone();
                    let snap_interner = Arc::clone(&snapshot_interner);
                    let snap_graph = Arc::clone(&snapshot_graph);
                    let snap_preferences = Arc::clone(&snapshot_preferences);
                    let snap_impressions = Arc::clone(&snapshot_impression_store);
                    let snap_oauth = Arc::clone(&snapshot_oauth_store);
                    let snap_sessions = Arc::clone(&snapshot_user_oauth_sessions);
                    let snap_active = Arc::clone(&snapshot_active_users);
                    let save_slot = Arc::clone(&periodic_save_slot);

                    let save_res = tokio::task::spawn_blocking(move || {
                        // Serialize against any concurrent prune/compaction or shutdown save.
                        let _guard = save_slot.blocking_lock();
                        snap_graph.prune_older_than(prune_cutoff);
                        snap_impressions.prune_expired(now_secs);
                        snap_oauth.prune_expired(
                            for_your_consideration::auth::DEFAULT_OAUTH_STATE_TTL_SECS,
                            now_secs,
                        );
                        snap_sessions.prune_expired(now_secs);
                        snap_active.prune_older_than(now_secs.saturating_sub(900));

                        save_snapshot_with_preferences(
                            &snap_path,
                            &snap_interner,
                            &snap_graph,
                            &snap_preferences,
                            current_cursor,
                        )
                    })
                    .await;

                    match save_res {
                        Ok(Ok(_)) => {
                            let duration_ms = start_save.elapsed().as_secs_f64() * 1000.0;
                            let file_size = snapshot_path.metadata().map_or(0, |m| m.len());
                            periodic_snapshot_tracker.record_save(duration_ms, file_size);
                            tracing::debug!(
                                "Periodic snapshot checkpoint saved successfully in {duration_ms:.2} ms"
                            );
                        }
                        Ok(Err(e)) => {
                            periodic_snapshot_tracker.record_save_failure(&e.to_string());
                            warn!(error = %e, "Periodic snapshot save failed");
                        }
                        Err(join_err) => {
                            periodic_snapshot_tracker.record_save_failure(&join_err.to_string());
                            warn!(error = %join_err, "Periodic snapshot spawn_blocking join error");
                        }
                    }
                }
            }
        }
    });

    // 7. Wait for shutdown signal (SIGINT / SIGTERM)
    wait_for_shutdown_signal().await;
    info!("Shutdown signal received. Stopping firehose ingestion while keeping HTTP server online for final snapshot write...");

    // 8. Trigger cooperative cancellation across background ingestion/maintenance tasks
    cancel_token.cancel();

    // 9. Await background task completion with timeout safety
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
            "Graceful shutdown timed out after {SHUTDOWN_TIMEOUT_SECS}s; aborting remaining background tasks."
        );
        tasks.abort_all();
    } else {
        info!("All background ingestion and maintenance tasks shut down cleanly.");
    }

    // 10. Persist final snapshot on graceful shutdown while the HTTP server remains online.
    //
    // Guarded by `hydration_completed` so an early SIGTERM during initial snapshot hydration never
    // overwrites an existing `/data/snapshot.bin` with an unhydrated store.
    if hydration_completed.load(Ordering::Acquire) {
        let final_cursor = ingester.latest_cursor();
        info!("Persisting final snapshot on graceful shutdown (HTTP server still serving live traffic)...");
        let start_final = std::time::Instant::now();
        let snap_path = snapshot_config.path.clone();
        let snap_interner = Arc::clone(&interner);
        let snap_graph = Arc::clone(&graph);
        let snap_preferences = Arc::clone(&preferences_store);
        let save_slot = Arc::clone(&snapshot_save_slot);

        let save_res = tokio::time::timeout(
            Duration::from_secs(shutdown_save_timeout_secs),
            tokio::task::spawn_blocking(move || {
                // Serialize against any in-flight periodic save so the two never race the `.tmp` path.
                let _guard = save_slot.blocking_lock();
                save_snapshot_with_preferences(
                    &snap_path,
                    &snap_interner,
                    &snap_graph,
                    &snap_preferences,
                    final_cursor,
                )
            }),
        )
        .await;

        match save_res {
            Ok(Ok(Ok(_))) => {
                let duration_ms = start_final.elapsed().as_secs_f64() * 1000.0;
                let file_size = snapshot_config.path.metadata().map_or(0, |m| m.len());
                snapshot_tracker.record_save(duration_ms, file_size);
                info!(
                    "Final shutdown snapshot saved successfully to '{}' in {duration_ms:.2} ms",
                    snapshot_config.path.display()
                );
            }
            Ok(Ok(Err(e))) => {
                snapshot_tracker.record_save_failure(&e.to_string());
                error!(error = %e, "Final shutdown snapshot save failed");
            }
            Ok(Err(join_err)) => {
                snapshot_tracker.record_save_failure(&join_err.to_string());
                error!(error = %join_err, "Final shutdown snapshot spawn_blocking join error");
            }
            Err(_) => {
                let msg = format!(
                    "Final shutdown snapshot save exceeded {shutdown_save_timeout_secs}s timeout"
                );
                snapshot_tracker.record_save_failure(&msg);
                error!("{msg}");
            }
        }
    } else {
        warn!(
            "Shutdown signal arrived before initial snapshot hydration completed; skipping final snapshot save to preserve existing snapshot file."
        );
    }

    // 11. Now that the snapshot is safely persisted, shut down the HTTP listener.
    http_cancel_token.cancel();
    let _ = tokio::time::timeout(Duration::from_secs(5), http_server_handle).await;

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
