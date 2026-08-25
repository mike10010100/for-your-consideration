#![forbid(unsafe_code)]

//! Real-time Jetstream WebSocket ingestion pipeline for AT Protocol / Bluesky firehose.
//!
//! # Architecture Overview
//!
//! The ingestion engine connects to Bluesky Jetstream WebSocket endpoints, filters
//! subscriptions by wanted collections, parses events into strongly typed data models,
//! and applies graph mutations to [`GraphStore`] and [`StringInterner`] via a bounded
//! backpressure channel (`tokio::sync::mpsc`).
//!
//! Key components:
//! - **Connection & Reader Task**: Manages WebSocket lifecycle, query parameter construction,
//!   keepalive pings/pongs, and inactivity watchdog.
//! - **Exponential Backoff Engine**: Retries failed connections with exponential backoff and
//!   random jitter (500ms initial, doubling to 30s max, resetting on successful frame receipt).
//! - **Monotonic Cursor Tracker**: Atomic `time_us` preservation, resuming seamlessly via `?cursor=...`.
//! - **Bounded Channel Backpressure**: Bounded `mpsc` queue (10,000–50,000 capacity) preventing OOM
//!   under firehose burst traffic.
//! - **Graph Mutation Consumer**: Worker task draining events, interning strings, and updating graph
//!   relationships in real time.
//! - **Graceful Shutdown**: Coordinated via [`CancellationToken`], sending clean WebSocket close
//!   frames and draining in-flight buffered events with zero data loss.
//!
//! # Example
//!
//! ```rust
//! use for_your_consideration::prelude::*;
//! use std::sync::Arc;
//!
//! let interner = Arc::new(StringInterner::new());
//! let graph = Arc::new(GraphStore::new());
//! let config = IngesterConfig::default();
//! let ingester = JetstreamIngester::new(config, interner, graph);
//!
//! assert_eq!(ingester.latest_cursor(), 0);
//! ```

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use compact_str::CompactString;
use futures_util::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use tokio::sync::mpsc;
use tokio::task::JoinSet;
use tokio::time::Instant;
use tokio_tungstenite::tungstenite::protocol::CloseFrame;
use tokio_tungstenite::tungstenite::Message;
use tokio_util::sync::CancellationToken;
use tracing::{debug, info, trace, warn};

use crate::error::{FeedError, Result};
use crate::graph::GraphStore;
use crate::interner::StringInterner;
use crate::types::{IngestionVelocityInfo, SignalType, BLUESKY_EPOCH_SECS};

/// Default production Jetstream WebSocket endpoint.
pub const DEFAULT_JETSTREAM_URL: &str = "wss://jetstream1.us-east.bsky.network/subscribe";

/// Default capacity for the bounded backpressure event channel (50,000 events).
pub const DEFAULT_CHANNEL_CAPACITY: usize = 50_000;

/// Default inactivity timeout before detecting a hung TCP connection (60 seconds).
pub const DEFAULT_INACTIVITY_TIMEOUT_SECS: u64 = 60;

/// Default initial reconnect backoff delay (500 ms).
pub const DEFAULT_INITIAL_BACKOFF_MS: u64 = 500;

/// Default maximum reconnect backoff delay cap (30 seconds).
pub const DEFAULT_MAX_BACKOFF_SECS: u64 = 30;

/// Default keepalive ping interval (30 seconds).
pub const DEFAULT_PING_INTERVAL_SECS: u64 = 30;

/// Configuration parameters for the Jetstream WebSocket ingester.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IngesterConfig {
    /// Base WebSocket endpoint URL (e.g. `wss://jetstream1.us-east.bsky.network/subscribe`).
    pub jetstream_url: CompactString,
    /// List of NSID collection filters to subscribe to.
    pub wanted_collections: Vec<CompactString>,
    /// Optional starting cursor timestamp in microseconds (`time_us`).
    pub initial_cursor: Option<u64>,
    /// Bounded capacity for the internal mpsc event channel.
    pub channel_capacity: usize,
    /// Initial backoff delay for reconnection attempts (default: 500ms).
    pub initial_backoff: Duration,
    /// Maximum backoff delay cap for reconnection attempts (default: 30s).
    pub max_backoff: Duration,
    /// Inactivity timeout before treating connection as hung (default: 60s).
    pub inactivity_timeout: Duration,
    /// Optional interval for sending keepalive WebSocket pings.
    pub ping_interval: Option<Duration>,
}

/// Alias for [`IngesterConfig`].
pub type JetstreamConfig = IngesterConfig;

impl Default for IngesterConfig {
    fn default() -> Self {
        Self {
            jetstream_url: CompactString::new(DEFAULT_JETSTREAM_URL),
            wanted_collections: vec![
                CompactString::new("app.bsky.feed.like"),
                CompactString::new("app.bsky.feed.repost"),
                CompactString::new("app.bsky.feed.post"),
                CompactString::new("app.bsky.graph.follow"),
            ],
            initial_cursor: None,
            channel_capacity: DEFAULT_CHANNEL_CAPACITY,
            initial_backoff: Duration::from_millis(DEFAULT_INITIAL_BACKOFF_MS),
            max_backoff: Duration::from_secs(DEFAULT_MAX_BACKOFF_SECS),
            inactivity_timeout: Duration::from_secs(DEFAULT_INACTIVITY_TIMEOUT_SECS),
            ping_interval: Some(Duration::from_secs(DEFAULT_PING_INTERVAL_SECS)),
        }
    }
}

impl IngesterConfig {
    /// Creates a new configuration with a custom Jetstream URL and default settings.
    #[must_use]
    pub fn new(jetstream_url: impl Into<CompactString>) -> Self {
        Self {
            jetstream_url: jetstream_url.into(),
            ..Self::default()
        }
    }

    /// Sets the list of wanted collections to subscribe to.
    #[must_use]
    pub fn with_collections(mut self, collections: Vec<CompactString>) -> Self {
        self.wanted_collections = collections;
        self
    }

    /// Sets the initial stream cursor timestamp in microseconds (`time_us`).
    #[must_use]
    pub const fn with_initial_cursor(mut self, cursor: Option<u64>) -> Self {
        self.initial_cursor = cursor;
        self
    }

    /// Sets the bounded channel capacity.
    #[must_use]
    pub fn with_channel_capacity(mut self, capacity: usize) -> Self {
        self.channel_capacity = capacity.max(1);
        self
    }

    /// Sets the exponential reconnect backoff parameters.
    #[must_use]
    pub fn with_backoff(mut self, initial: Duration, max: Duration) -> Self {
        let safe_initial = initial.max(Duration::from_millis(100));
        self.initial_backoff = safe_initial;
        self.max_backoff = max.max(safe_initial);
        self
    }

    /// Sets the inactivity timeout.
    #[must_use]
    pub fn with_inactivity_timeout(mut self, timeout: Duration) -> Self {
        self.inactivity_timeout = timeout.max(Duration::from_secs(10));
        self
    }

    /// Sets the keepalive WebSocket ping interval.
    #[must_use]
    pub const fn with_ping_interval(mut self, ping_interval: Option<Duration>) -> Self {
        self.ping_interval = ping_interval;
        self
    }
}

/// Strongly typed normalized domain event extracted from a Jetstream commit frame.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub enum JetstreamEvent {
    /// Interaction event (Like, Repost, or Quote).
    Interaction {
        /// DID of the interacting user.
        user_did: CompactString,
        /// Canonical AT-URI of the target post.
        post_uri: CompactString,
        /// Algorithmic signal type (Like = 1.0x, Quote = 2.0x, Repost = 3.0x).
        signal: SignalType,
        /// Unix timestamp in seconds.
        timestamp_secs: u64,
    },
    /// Post creation metadata with thread hierarchy references.
    PostMeta {
        /// Canonical AT-URI of the created post.
        post_uri: CompactString,
        /// DID of the author.
        author_did: CompactString,
        /// Canonical AT-URI of the root post if this is a reply.
        root_uri: Option<CompactString>,
        /// Canonical AT-URI of the parent post if this is a reply.
        parent_uri: Option<CompactString>,
        /// Unix timestamp in seconds when the post was created.
        created_at_secs: u64,
    },
    /// Directed follow relationship.
    Follow {
        /// DID of the follower account.
        follower_did: CompactString,
        /// DID of the followed account.
        subject_did: CompactString,
    },
    /// Deletion of a commit (e.g. un-like, un-repost, or un-follow).
    Delete {
        /// DID of the actor performing the deletion.
        did: CompactString,
        /// Collection NSID of the deleted record.
        collection: CompactString,
        /// Record key (rkey) of the deleted record.
        rkey: CompactString,
    },
}

/// Alias for [`JetstreamEvent`].
pub type IngestEvent = JetstreamEvent;

/// Lock-free atomic runtime metrics and health counters for the ingestion pipeline.
#[derive(Debug, Default)]
pub struct IngestionStats {
    /// Total raw events / frames received over the WebSocket stream.
    pub events_received: AtomicU64,
    /// Total events successfully parsed and applied to the in-memory graph.
    pub events_processed: AtomicU64,
    /// Total raw network bytes received over the WebSocket stream.
    pub bytes_received: AtomicU64,
    /// Total number of reconnection attempts triggered.
    pub reconnect_count: AtomicU64,
    /// Highest monotonic Jetstream cursor (`time_us`) processed.
    pub latest_cursor_us: AtomicU64,
    /// Timestamp (unix seconds) of the most recent event or heartbeat.
    pub last_activity_timestamp: AtomicU64,
    /// Initial cursor timestamp (`time_us`) when backfill / hydration began, if any.
    pub initial_cursor_us: AtomicU64,
    /// Real-time wall-clock target timestamp (`time_us`) when backfill was initiated.
    pub backfill_target_cursor_us: AtomicU64,
    /// Real-time wall-clock start timestamp (`time_us`) when ingestion started.
    pub backfill_start_wall_time_us: AtomicU64,
}

impl IngestionStats {
    /// Creates a new [`IngestionStats`] with initial cursor.
    #[must_use]
    pub fn new(initial_cursor: Option<u64>) -> Self {
        let stats = Self::default();
        let now_us = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_micros() as u64;

        stats
            .backfill_start_wall_time_us
            .store(now_us, Ordering::Relaxed);
        if let Some(cur) = initial_cursor {
            stats.latest_cursor_us.store(cur, Ordering::Relaxed);
            stats.initial_cursor_us.store(cur, Ordering::Relaxed);
            stats
                .backfill_target_cursor_us
                .store(now_us, Ordering::Relaxed);
        }
        stats
    }

    /// Returns a frozen point-in-time snapshot of current ingestion metrics.
    #[must_use]
    pub fn snapshot(&self) -> IngestionStatsSnapshot {
        IngestionStatsSnapshot {
            events_received: self.events_received.load(Ordering::Relaxed),
            events_processed: self.events_processed.load(Ordering::Relaxed),
            bytes_received: self.bytes_received.load(Ordering::Relaxed),
            reconnect_count: self.reconnect_count.load(Ordering::Relaxed),
            latest_cursor_us: self.latest_cursor_us.load(Ordering::Relaxed),
            last_activity_timestamp: self.last_activity_timestamp.load(Ordering::Relaxed),
            initial_cursor_us: self.initial_cursor_us.load(Ordering::Relaxed),
            backfill_target_cursor_us: self.backfill_target_cursor_us.load(Ordering::Relaxed),
            backfill_start_wall_time_us: self.backfill_start_wall_time_us.load(Ordering::Relaxed),
        }
    }
}

/// Point-in-time frozen snapshot of ingestion metrics (suitable for `/healthz` or metrics exporters).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
pub struct IngestionStatsSnapshot {
    /// Total raw events received over the WebSocket stream.
    pub events_received: u64,
    /// Total events successfully deserialized and recorded into the graph.
    pub events_processed: u64,
    /// Total raw network bytes received.
    pub bytes_received: u64,
    /// Total number of reconnection attempts triggered.
    pub reconnect_count: u64,
    /// Highest monotonic Jetstream cursor (`time_us`) processed.
    pub latest_cursor_us: u64,
    /// Timestamp (unix seconds) of the most recent event or heartbeat.
    pub last_activity_timestamp: u64,
    /// Initial cursor timestamp (`time_us`) when backfill / hydration began.
    pub initial_cursor_us: u64,
    /// Real-time wall-clock target timestamp (`time_us`) when backfill was initiated.
    pub backfill_target_cursor_us: u64,
    /// Real-time wall-clock start timestamp (`time_us`) when ingestion started.
    pub backfill_start_wall_time_us: u64,
}

/// Thread-safe tracker computing real-time event ingestion velocity and statistics.
#[derive(Debug)]
pub struct IngestionTracker {
    stats: Arc<IngestionStats>,
    sample_window: parking_lot::Mutex<VelocitySample>,
}

#[derive(Debug, Clone)]
struct VelocitySample {
    last_sampled_at: Instant,
    last_processed_count: u64,
    current_velocity: f32,
}

impl Default for IngestionTracker {
    fn default() -> Self {
        Self::new(Arc::new(IngestionStats::default()))
    }
}

impl IngestionTracker {
    /// Creates a new [`IngestionTracker`] wrapping the given [`IngestionStats`].
    #[must_use]
    pub fn new(stats: Arc<IngestionStats>) -> Self {
        let now = Instant::now();
        let initial_processed = stats.events_processed.load(Ordering::Relaxed);
        Self {
            stats,
            sample_window: parking_lot::Mutex::new(VelocitySample {
                last_sampled_at: now,
                last_processed_count: initial_processed,
                current_velocity: 0.0,
            }),
        }
    }

    /// Returns a reference to the underlying [`IngestionStats`].
    #[must_use]
    pub const fn stats(&self) -> &Arc<IngestionStats> {
        &self.stats
    }

    /// Computes and returns instantaneous velocity in events/sec.
    pub fn calculate_velocity(&self) -> f32 {
        let now = Instant::now();
        let current_processed = self.stats.events_processed.load(Ordering::Relaxed);
        let mut sample = self.sample_window.lock();
        let elapsed = now.saturating_duration_since(sample.last_sampled_at);

        if elapsed.as_millis() >= 200 {
            let count_diff = current_processed.saturating_sub(sample.last_processed_count);
            let secs = elapsed.as_secs_f32();
            let velocity = if secs > 0.0 {
                count_diff as f32 / secs
            } else {
                0.0
            };
            sample.last_sampled_at = now;
            sample.last_processed_count = current_processed;
            sample.current_velocity = velocity;
            velocity
        } else {
            sample.current_velocity
        }
    }

    /// Returns a point-in-time [`IngestionVelocityInfo`] snapshot with live velocity.
    #[must_use]
    pub fn get_velocity_info(&self) -> IngestionVelocityInfo {
        let snap = self.stats.snapshot();
        let velocity = self.calculate_velocity();

        let now_secs = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();
        let now_us = now_secs.saturating_mul(1_000_000);

        let latest_cursor_us = snap.latest_cursor_us;
        let cursor_secs = latest_cursor_us / 1_000_000;
        let lag_seconds = now_secs.saturating_sub(cursor_secs);

        let init_cur = snap.initial_cursor_us;
        let target_cur = snap.backfill_target_cursor_us;

        let (progress_percent, eta_seconds, speedup_factor, is_live) =
            if init_cur > 0 && target_cur > init_cur {
                let total_span_us = target_cur.saturating_sub(init_cur);
                let covered_us = latest_cursor_us.saturating_sub(init_cur);
                let raw_progress = (covered_us as f64 / (total_span_us.max(1) as f64)) * 100.0;
                let progress = (raw_progress as f32).clamp(0.0, 100.0);

                let wall_elapsed_secs =
                    (now_us.saturating_sub(snap.backfill_start_wall_time_us) / 1_000_000).max(1);
                let cursor_advanced_secs = covered_us / 1_000_000;
                let speedup = cursor_advanced_secs as f32 / wall_elapsed_secs as f32;

                let live = lag_seconds <= 60 || latest_cursor_us >= target_cur;
                let eta = if live {
                    Some(0)
                } else if speedup > 0.1 {
                    let remaining_secs = target_cur.saturating_sub(latest_cursor_us) / 1_000_000;
                    Some((remaining_secs as f32 / speedup) as u64)
                } else {
                    None
                };

                (progress, eta, speedup, live)
            } else {
                (100.0, None, 1.0, true)
            };

        IngestionVelocityInfo {
            events_received: snap.events_received,
            events_processed: snap.events_processed,
            bytes_received: snap.bytes_received,
            reconnect_count: snap.reconnect_count,
            latest_cursor_us,
            last_activity_timestamp: snap.last_activity_timestamp,
            velocity_events_per_sec: velocity,
            initial_cursor_us: if init_cur > 0 { Some(init_cur) } else { None },
            target_cursor_us: if target_cur > 0 {
                Some(target_cur)
            } else {
                None
            },
            lag_seconds,
            backfill_progress_percent: progress_percent,
            is_live,
            eta_seconds,
            speedup_factor,
        }
    }
}

/// Manages exponential backoff delays with random jitter for reconnect attempts.
#[derive(Debug, Clone)]
pub struct BackoffManager {
    initial_delay: Duration,
    max_delay: Duration,
    current_delay: Duration,
    consecutive_failures: u32,
    jitter_fraction: f64,
}

/// Alias for [`BackoffManager`].
pub type BackoffPolicy = BackoffManager;

impl BackoffManager {
    /// Creates a new [`BackoffManager`].
    #[must_use]
    pub fn new(initial_delay: Duration, max_delay: Duration) -> Self {
        let safe_initial = initial_delay.max(Duration::from_millis(100));
        let safe_max = max_delay.max(safe_initial);
        Self {
            initial_delay: safe_initial,
            max_delay: safe_max,
            current_delay: safe_initial,
            consecutive_failures: 0,
            jitter_fraction: 0.20,
        }
    }

    /// Computes the next backoff duration with exponential doubling and jitter.
    pub fn next_backoff(&mut self) -> Duration {
        let base = self.current_delay;
        self.consecutive_failures = self.consecutive_failures.saturating_add(1);

        // Exponential doubling
        let next_ms = (self.current_delay.as_millis() as u64).saturating_mul(2);
        self.current_delay = Duration::from_millis(next_ms).min(self.max_delay);

        // Pseudo-random jitter in [-20%, +20%] using high-resolution timestamp
        let base_ms = base.as_millis() as u64;
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map_or(123_456_789, |d| u64::from(d.subsec_nanos()));

        let pseudo_rand = (nanos
            .wrapping_mul(6_364_136_223_846_793_005)
            .wrapping_add(u64::from(self.consecutive_failures))
            >> 32) as u32;

        let jitter_range_pct = (self.jitter_fraction * 100.0) as i64;
        let modulo = (jitter_range_pct * 2 + 1) as u32;
        let jitter_offset_pct = i64::from(pseudo_rand % modulo) - jitter_range_pct;

        let jittered_ms = if jitter_offset_pct >= 0 {
            base_ms.saturating_add(base_ms * (jitter_offset_pct as u64) / 100)
        } else {
            base_ms.saturating_sub(base_ms * ((-jitter_offset_pct) as u64) / 100)
        };

        Duration::from_millis(jittered_ms.max(50))
    }

    /// Resets the backoff state to the initial delay.
    pub const fn reset(&mut self) {
        self.current_delay = self.initial_delay;
        self.consecutive_failures = 0;
    }

    /// Returns the number of consecutive failed attempts.
    #[must_use]
    pub const fn consecutive_failures(&self) -> u32 {
        self.consecutive_failures
    }
}

/// Lock-free monotonic cursor tracker.
#[derive(Debug, Default)]
pub struct CursorTracker {
    latest_time_us: AtomicU64,
}

impl CursorTracker {
    /// Creates a new [`CursorTracker`] with an optional initial timestamp.
    #[must_use]
    pub const fn new(initial: Option<u64>) -> Self {
        let val = match initial {
            Some(v) => v,
            None => 0,
        };
        Self {
            latest_time_us: AtomicU64::new(val),
        }
    }

    /// Updates the cursor if `time_us` is greater than the current recorded value.
    pub fn update(&self, time_us: u64) {
        self.latest_time_us.fetch_max(time_us, Ordering::Relaxed);
    }

    /// Returns the current cursor timestamp, or `None` if zero.
    #[must_use]
    pub fn get(&self) -> Option<u64> {
        let val = self.latest_time_us.load(Ordering::Relaxed);
        if val == 0 {
            None
        } else {
            Some(val)
        }
    }

    /// Returns the raw `u64` cursor value.
    #[must_use]
    pub fn get_raw(&self) -> u64 {
        self.latest_time_us.load(Ordering::Relaxed)
    }
}

/// Constructs a WebSocket subscription URL with query parameter collection filters and cursor.
#[must_use]
pub fn build_subscription_url(
    base_url: &str,
    wanted_collections: &[CompactString],
    cursor: Option<u64>,
) -> String {
    let mut url = base_url.to_string();

    // Ensure valid HTTP/WS path exists before appending query parameters
    if let Some(scheme_idx) = url.find("://") {
        let after_scheme = &url[scheme_idx + 3..];
        if !after_scheme.contains('/') && !after_scheme.contains('?') {
            url.push('/');
        }
    } else if !url.contains('/') && !url.contains('?') {
        url.push('/');
    }

    let has_query = url.contains('?');
    let mut query_params: Vec<String> = Vec::new();

    // Add wanted collections if not already present
    if !url.contains("wantedCollections") {
        for col in wanted_collections {
            query_params.push(format!("wantedCollections={col}"));
        }
    }

    // Add cursor if available and non-zero
    if let Some(c) = cursor {
        if c > 0 && !url.contains("cursor=") {
            query_params.push(format!("cursor={c}"));
        }
    }

    if !query_params.is_empty() {
        let separator = if has_query { '&' } else { '?' };
        if !url.ends_with('?') && !url.ends_with('&') {
            url.push(separator);
        }
        url.push_str(&query_params.join("&"));
    }

    url
}

/// Alias for [`build_subscription_url`].
#[must_use]
pub fn build_jetstream_url(
    base_url: &str,
    wanted_collections: &[CompactString],
    cursor: Option<u64>,
) -> String {
    build_subscription_url(base_url, wanted_collections, cursor)
}

/// Raw wire envelope for Jetstream WebSocket messages.
#[derive(Debug, Deserialize)]
pub struct RawJetstreamMessage {
    /// Author DID of the Jetstream event.
    #[serde(default)]
    pub did: CompactString,
    /// Event timestamp in microseconds since unix epoch.
    #[serde(default)]
    pub time_us: u64,
    /// Event kind string (e.g. "commit").
    #[serde(default)]
    pub kind: CompactString,
    /// Commit operation payload if kind is "commit".
    #[serde(default)]
    pub commit: Option<RawJetstreamCommit>,
}

/// Raw wire payload for Jetstream commit operations.
#[derive(Debug, Deserialize)]
pub struct RawJetstreamCommit {
    /// AT Protocol NSID collection (e.g. "app.bsky.feed.like").
    #[serde(default)]
    pub collection: CompactString,
    /// Record key identifier.
    #[serde(default)]
    pub rkey: CompactString,
    /// Commit operation type (e.g. "create", "delete").
    #[serde(default)]
    pub operation: CompactString,
    /// Raw JSON record value if present.
    #[serde(default)]
    pub record: Option<serde_json::Value>,
}

/// Parses a raw Jetstream JSON string into normalized [`JetstreamEvent`]s and timestamp in microseconds.
///
/// Returns `None` if the frame is malformed JSON, not a commit event, an unwanted collection,
/// or missing required fields.
#[must_use]
#[allow(clippy::too_many_lines)]
pub fn parse_jetstream_frame(text: &str) -> Option<(Vec<JetstreamEvent>, u64)> {
    let msg: RawJetstreamMessage = match serde_json::from_str(text) {
        Ok(m) => m,
        Err(_) => return None,
    };

    if msg.kind != "commit" || msg.did.is_empty() {
        return None;
    }

    let commit = msg.commit?;
    let time_us = msg.time_us;
    let fallback_secs = if time_us > 0 {
        (time_us / 1_000_000).max(BLUESKY_EPOCH_SECS)
    } else {
        BLUESKY_EPOCH_SECS
    };

    let is_delete = commit.operation.eq_ignore_ascii_case("delete");

    if is_delete {
        let event = JetstreamEvent::Delete {
            did: msg.did,
            collection: commit.collection,
            rkey: commit.rkey,
        };
        return Some((vec![event], time_us));
    }

    if !commit.operation.eq_ignore_ascii_case("create") {
        return None;
    }

    let record = commit.record?;
    let mut events = Vec::with_capacity(2);

    match commit.collection.as_str() {
        "app.bsky.feed.like" => {
            let post_uri = record
                .get("subject")
                .and_then(|s| s.get("uri").and_then(|u| u.as_str()).or_else(|| s.as_str()))
                .map(CompactString::new)?;

            events.push(JetstreamEvent::Interaction {
                user_did: msg.did,
                post_uri,
                signal: SignalType::Like,
                timestamp_secs: fallback_secs,
            });
        }
        "app.bsky.feed.repost" => {
            let post_uri = record
                .get("subject")
                .and_then(|s| s.get("uri").and_then(|u| u.as_str()).or_else(|| s.as_str()))
                .map(CompactString::new)?;

            events.push(JetstreamEvent::Interaction {
                user_did: msg.did,
                post_uri,
                signal: SignalType::Repost,
                timestamp_secs: fallback_secs,
            });
        }
        "app.bsky.feed.post" => {
            let post_uri = CompactString::new(format!(
                "at://{}/app.bsky.feed.post/{}",
                msg.did, commit.rkey
            ));

            let reply = record.get("reply");
            let root_uri = reply
                .and_then(|r| r.get("root"))
                .and_then(|root| root.get("uri"))
                .and_then(serde_json::Value::as_str)
                .map(CompactString::new);

            let parent_uri = reply
                .and_then(|r| r.get("parent"))
                .and_then(|parent| parent.get("uri"))
                .and_then(serde_json::Value::as_str)
                .map(CompactString::new);

            events.push(JetstreamEvent::PostMeta {
                post_uri,
                author_did: msg.did.clone(),
                root_uri,
                parent_uri,
                created_at_secs: fallback_secs,
            });

            // Check for embedded quote posts
            if let Some(embed) = record.get("embed") {
                let embed_type = embed
                    .get("$type")
                    .and_then(serde_json::Value::as_str)
                    .unwrap_or_default();

                let quote_uri_opt = if embed_type == "app.bsky.embed.record" {
                    embed
                        .get("record")
                        .and_then(|r| r.get("uri"))
                        .and_then(serde_json::Value::as_str)
                } else if embed_type == "app.bsky.embed.recordWithMedia" {
                    embed
                        .get("record")
                        .and_then(|r| r.get("record"))
                        .and_then(|r| r.get("uri"))
                        .and_then(serde_json::Value::as_str)
                } else {
                    None
                };

                if let Some(q_uri) = quote_uri_opt {
                    events.push(JetstreamEvent::Interaction {
                        user_did: msg.did,
                        post_uri: CompactString::new(q_uri),
                        signal: SignalType::Quote,
                        timestamp_secs: fallback_secs,
                    });
                }
            }
        }
        "app.bsky.graph.follow" => {
            let subject_did = record
                .get("subject")
                .and_then(|s| s.as_str().or_else(|| s.get("did").and_then(|d| d.as_str())))
                .map(CompactString::new)?;

            events.push(JetstreamEvent::Follow {
                follower_did: msg.did,
                subject_did,
            });
        }
        _ => return None,
    }

    if events.is_empty() {
        None
    } else {
        Some((events, time_us))
    }
}

/// Parses a Jetstream JSON string into a single primary [`JetstreamEvent`].
///
/// Returns `Ok(None)` if the frame is not a relevant commit or is malformed.
pub fn parse_jetstream_json(json_str: &str) -> Result<Option<JetstreamEvent>> {
    match parse_jetstream_frame(json_str) {
        Some((mut events, _)) => Ok(events.pop()),
        None => Ok(None),
    }
}

/// Applies a single [`JetstreamEvent`] to the in-memory [`GraphStore`] and [`StringInterner`].
pub fn apply_event_to_graph(event: &JetstreamEvent, interner: &StringInterner, graph: &GraphStore) {
    match event {
        JetstreamEvent::Interaction {
            user_did,
            post_uri,
            signal,
            timestamp_secs,
        } => {
            let uid = interner.intern(user_did);
            let pid = interner.intern(post_uri);
            graph.record_interaction(uid, pid, *signal, *timestamp_secs);
        }
        JetstreamEvent::PostMeta {
            post_uri,
            author_did,
            root_uri,
            parent_uri,
            created_at_secs,
        } => {
            let pid = interner.intern(post_uri);
            let aid = interner.intern(author_did);
            let rid = root_uri.as_ref().map(|r| interner.intern(r));
            let paid = parent_uri.as_ref().map(|p| interner.intern(p));
            graph.record_post_meta(pid, aid, rid, paid, *created_at_secs);
        }
        JetstreamEvent::Follow {
            follower_did,
            subject_did,
        } => {
            let fid = interner.intern(follower_did);
            let tid = interner.intern(subject_did);
            graph.record_follow(fid, tid);
        }
        JetstreamEvent::Delete {
            did,
            collection,
            rkey: _,
        } => {
            if let Some(uid) = interner.lookup_id(did) {
                match collection.as_str() {
                    "app.bsky.feed.like" => {
                        let edges = graph.get_user_interactions(uid);
                        for edge in edges {
                            if edge.signal() == SignalType::Like {
                                graph.remove_interaction(uid, edge.target(), SignalType::Like);
                                break;
                            }
                        }
                    }
                    "app.bsky.feed.repost" => {
                        let edges = graph.get_user_interactions(uid);
                        for edge in edges {
                            if edge.signal() == SignalType::Repost {
                                graph.remove_interaction(uid, edge.target(), SignalType::Repost);
                                break;
                            }
                        }
                    }
                    "app.bsky.graph.follow" => {
                        let follows = graph.get_user_follows(uid);
                        if let Some(&target) = follows.first() {
                            graph.remove_follow(uid, target);
                        }
                    }
                    _ => {}
                }
            }
        }
    }
}

/// Real-time Jetstream WebSocket ingestion pipeline client.
#[derive(Debug, Clone)]
pub struct JetstreamIngester {
    config: IngesterConfig,
    interner: Arc<StringInterner>,
    graph: Arc<GraphStore>,
    stats: Arc<IngestionStats>,
}

/// Alias for [`JetstreamIngester`].
pub type JetstreamClient = JetstreamIngester;

impl JetstreamIngester {
    /// Creates a new [`JetstreamIngester`] instance.
    #[must_use]
    pub fn new(
        config: IngesterConfig,
        interner: Arc<StringInterner>,
        graph: Arc<GraphStore>,
    ) -> Self {
        let stats = Arc::new(IngestionStats::new(config.initial_cursor));
        Self {
            config,
            interner,
            graph,
            stats,
        }
    }

    /// Returns a reference to the shared ingestion metrics.
    #[must_use]
    pub const fn stats(&self) -> &Arc<IngestionStats> {
        &self.stats
    }

    /// Returns a snapshot of the current ingestion metrics.
    #[must_use]
    pub fn stats_snapshot(&self) -> IngestionStatsSnapshot {
        self.stats.snapshot()
    }

    /// Returns the latest recorded cursor in microseconds.
    #[must_use]
    pub fn latest_cursor(&self) -> u64 {
        self.stats.latest_cursor_us.load(Ordering::Relaxed)
    }

    /// Returns the current stream cursor (`time_us`).
    #[must_use]
    pub fn current_cursor(&self) -> u64 {
        self.latest_cursor()
    }

    /// Spawns the reader and consumer worker tasks onto the provided [`JoinSet`].
    pub fn start_pipeline(
        &self,
        join_set: &mut JoinSet<Result<()>>,
        cancel_token: CancellationToken,
    ) {
        let ingester = self.clone();
        join_set.spawn(async move { ingester.run(cancel_token).await });
    }

    /// Spawns the ingester lifecycle onto an external [`tokio::task::JoinSet`].
    pub fn spawn(&self, cancel_token: CancellationToken, join_set: &mut JoinSet<Result<()>>) {
        self.start_pipeline(join_set, cancel_token);
    }

    /// Runs the ingester pipeline to completion, listening to `cancel_token` for graceful shutdown.
    pub async fn run(&self, cancel_token: CancellationToken) -> Result<()> {
        if cancel_token.is_cancelled() {
            return Ok(());
        }

        let (tx, rx) = mpsc::channel::<JetstreamEvent>(self.config.channel_capacity);
        let mut join_set = JoinSet::new();

        // 1. Spawn consumer worker task
        let interner = Arc::clone(&self.interner);
        let graph = Arc::clone(&self.graph);
        let stats = Arc::clone(&self.stats);
        let worker_token = cancel_token.child_token();

        join_set.spawn(async move {
            run_consumer_worker(rx, interner, graph, stats, worker_token).await;
            Ok(())
        });

        // 2. Spawn reader reconnect loop task
        let config = self.config.clone();
        let stats_reader = Arc::clone(&self.stats);
        let reader_token = cancel_token.child_token();

        join_set.spawn(async move {
            run_reader_reconnect_loop(config, tx, stats_reader, reader_token).await
        });

        // 3. Await tasks
        while let Some(join_res) = join_set.join_next().await {
            match join_res {
                Ok(task_res) => task_res?,
                Err(join_err) => {
                    if !join_err.is_cancelled() {
                        return Err(FeedError::Ingest(format!(
                            "Ingestion task failed: {join_err}"
                        )));
                    }
                }
            }
        }

        Ok(())
    }

    /// Runs the WebSocket reader loop with reconnect backoff and inactivity watchdog.
    pub async fn run_reader(
        &self,
        tx: mpsc::Sender<JetstreamEvent>,
        cancel: CancellationToken,
    ) -> Result<()> {
        run_reader_reconnect_loop(self.config.clone(), tx, Arc::clone(&self.stats), cancel).await
    }

    /// Runs the consumer worker loop draining [`JetstreamEvent`]s into [`GraphStore`].
    pub async fn run_worker(
        &self,
        rx: mpsc::Receiver<JetstreamEvent>,
        cancel: CancellationToken,
    ) -> Result<()> {
        run_consumer_worker(
            rx,
            Arc::clone(&self.interner),
            Arc::clone(&self.graph),
            Arc::clone(&self.stats),
            cancel,
        )
        .await;
        Ok(())
    }

    /// Dispatches a single [`JetstreamEvent`] to [`StringInterner`] and [`GraphStore`].
    pub fn process_event(&self, event: &JetstreamEvent) {
        apply_event_to_graph(event, &self.interner, &self.graph);
        self.stats.events_processed.fetch_add(1, Ordering::Relaxed);
    }
}

/// Internal loop for the consumer worker task.
#[allow(clippy::iter_with_drain)]
async fn run_consumer_worker(
    mut rx: mpsc::Receiver<JetstreamEvent>,
    interner: Arc<StringInterner>,
    graph: Arc<GraphStore>,
    stats: Arc<IngestionStats>,
    _cancel: CancellationToken,
) {
    debug!("Ingestion consumer worker task started.");

    let mut batch = Vec::with_capacity(512);
    while rx.recv_many(&mut batch, 512).await > 0 {
        let count = batch.len() as u64;
        for event in batch.drain(..) {
            apply_event_to_graph(&event, &interner, &graph);
        }
        stats.events_processed.fetch_add(count, Ordering::Relaxed);
    }

    debug!("Ingestion channel drained and closed successfully.");
}

/// Internal loop for the WebSocket reader with reconnect backoff and watchdog.
#[allow(clippy::too_many_lines)]
async fn run_reader_reconnect_loop(
    config: IngesterConfig,
    tx: mpsc::Sender<JetstreamEvent>,
    stats: Arc<IngestionStats>,
    cancel: CancellationToken,
) -> Result<()> {
    let mut backoff = BackoffManager::new(config.initial_backoff, config.max_backoff);

    while !cancel.is_cancelled() {
        let cursor_val = stats.latest_cursor_us.load(Ordering::Relaxed);
        let cursor_opt = if cursor_val > 0 {
            Some(cursor_val)
        } else {
            None
        };

        let url = build_subscription_url(
            config.jetstream_url.as_str(),
            &config.wanted_collections,
            cursor_opt,
        );

        debug!("Connecting to Jetstream WebSocket: {url}");

        let connect_fut = tokio_tungstenite::connect_async(&url);
        let (mut ws_stream, _) = tokio::select! {
            () = cancel.cancelled() => {
                debug!("Ingestion reader cancelled during connect.");
                break;
            }
            res = connect_fut => {
                match res {
                    Ok(conn) => conn,
                    Err(err) => {
                        stats.reconnect_count.fetch_add(1, Ordering::Relaxed);
                        let delay = backoff.next_backoff();
                        warn!("Jetstream connection to {url} failed: {err}. Retrying in {delay:?}");
                        tokio::select! {
                            () = cancel.cancelled() => break,
                            () = tokio::time::sleep(delay) => continue,
                        }
                    }
                }
            }
        };

        info!("Connected to Jetstream WebSocket: {url}");
        let mut last_activity = Instant::now();
        let mut ping_interval = config
            .ping_interval
            .map(|d| tokio::time::interval_at(Instant::now() + d, d));

        loop {
            let timeout_duration = config.inactivity_timeout;
            let sleep_watchdog = tokio::time::sleep_until(last_activity + timeout_duration);

            tokio::select! {
                () = cancel.cancelled() => {
                    info!("Reader received cancellation signal, cleanly closing WebSocket.");
                    let _ = ws_stream.close(Some(CloseFrame {
                        code: tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode::Normal,
                        reason: "Graceful shutdown".into(),
                    })).await;
                    return Ok(());
                }
                () = sleep_watchdog => {
                    stats.reconnect_count.fetch_add(1, Ordering::Relaxed);
                    warn!("Jetstream inactivity timeout ({timeout_duration:?}) elapsed without frames. Reconnecting.");
                    let _ = ws_stream.close(None).await;
                    break;
                }
                _ = async {
                    if let Some(ref mut interval) = ping_interval {
                        interval.tick().await
                    } else {
                        futures_util::future::pending().await
                    }
                } => {
                    trace!("Sending keepalive WebSocket ping.");
                    if ws_stream.send(Message::Ping(Vec::new())).await.is_err() {
                        warn!("Failed to send keepalive ping, reconnecting.");
                        break;
                    }
                }
                msg_opt = ws_stream.next() => {
                    match msg_opt {
                        Some(Ok(msg)) => {
                            last_activity = Instant::now();
                            let now_secs = std::time::SystemTime::now()
                                .duration_since(std::time::UNIX_EPOCH)
                                .map_or(0, |d| d.as_secs());
                            stats.last_activity_timestamp.store(now_secs, Ordering::Relaxed);

                            match msg {
                                Message::Text(text) => {
                                    stats.events_received.fetch_add(1, Ordering::Relaxed);
                                    stats.bytes_received.fetch_add(text.len() as u64, Ordering::Relaxed);

                                    if let Some((events, time_us)) = parse_jetstream_frame(&text) {
                                        if time_us > 0 {
                                            stats.latest_cursor_us.fetch_max(time_us, Ordering::Relaxed);
                                        }

                                        for event in events {
                                            if tx.send(event).await.is_err() {
                                                info!("Consumer channel closed, terminating reader.");
                                                return Ok(());
                                            }
                                        }

                                        // Valid frame received: reset exponential backoff
                                        backoff.reset();
                                    }
                                }
                                Message::Ping(data) => {
                                    trace!("Received WebSocket Ping, replying with Pong.");
                                    let _ = ws_stream.send(Message::Pong(data)).await;
                                }
                                Message::Pong(_) => {
                                    trace!("Received WebSocket Pong.");
                                }
                                Message::Close(close_frame) => {
                                    stats.reconnect_count.fetch_add(1, Ordering::Relaxed);
                                    warn!("Jetstream server sent Close frame: {close_frame:?}. Reconnecting.");
                                    break;
                                }
                                Message::Binary(bin) => {
                                    stats.bytes_received.fetch_add(bin.len() as u64, Ordering::Relaxed);
                                }
                                Message::Frame(_) => {}
                            }
                        }
                        Some(Err(err)) => {
                            stats.reconnect_count.fetch_add(1, Ordering::Relaxed);
                            warn!("WebSocket stream error: {err}. Reconnecting.");
                            break;
                        }
                        None => {
                            stats.reconnect_count.fetch_add(1, Ordering::Relaxed);
                            warn!("WebSocket stream closed by remote host. Reconnecting.");
                            break;
                        }
                    }
                }
            }
        }

        // Connection dropped or closed, sleep backoff duration before retrying
        let delay = backoff.next_backoff();
        tokio::select! {
            () = cancel.cancelled() => break,
            () = tokio::time::sleep(delay) => {}
        }
    }

    Ok(())
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic, missing_docs)]
mod tests {
    use super::*;
    use crate::types::BLUESKY_EPOCH_SECS;

    #[test]
    fn test_ingester_config_builder() {
        let config = IngesterConfig::new("ws://127.0.0.1:8080/sub")
            .with_collections(vec![CompactString::new("app.bsky.feed.like")])
            .with_initial_cursor(Some(1_700_000_000_000_000))
            .with_channel_capacity(10_000)
            .with_backoff(Duration::from_millis(200), Duration::from_secs(10))
            .with_inactivity_timeout(Duration::from_secs(45))
            .with_ping_interval(Some(Duration::from_secs(15)));

        assert_eq!(config.jetstream_url, "ws://127.0.0.1:8080/sub");
        assert_eq!(config.wanted_collections.len(), 1);
        assert_eq!(config.initial_cursor, Some(1_700_000_000_000_000));
        assert_eq!(config.channel_capacity, 10_000);
        assert_eq!(config.initial_backoff, Duration::from_millis(200));
        assert_eq!(config.max_backoff, Duration::from_secs(10));
        assert_eq!(config.inactivity_timeout, Duration::from_secs(45));
        assert_eq!(config.ping_interval, Some(Duration::from_secs(15)));
    }

    #[test]
    fn test_url_construction_variations() {
        let cols = vec![
            CompactString::new("app.bsky.feed.like"),
            CompactString::new("app.bsky.feed.post"),
        ];

        // 1. Plain base URL
        let url1 = build_subscription_url("wss://jetstream.example.com/subscribe", &cols, None);
        assert_eq!(
            url1,
            "wss://jetstream.example.com/subscribe?wantedCollections=app.bsky.feed.like&wantedCollections=app.bsky.feed.post"
        );

        // 2. Base URL with cursor
        let url2 = build_subscription_url(
            "wss://jetstream.example.com/subscribe",
            &cols,
            Some(1_700_000_000_123_456),
        );
        assert_eq!(
            url2,
            "wss://jetstream.example.com/subscribe?wantedCollections=app.bsky.feed.like&wantedCollections=app.bsky.feed.post&cursor=1700000000123456"
        );

        // 3. Base URL with existing query param
        let url3 = build_subscription_url(
            "wss://jetstream.example.com/subscribe?compress=true",
            &cols,
            Some(1_700_000_000_123_456),
        );
        assert!(url3.starts_with("wss://jetstream.example.com/subscribe?compress=true&"));
        assert!(url3.contains("wantedCollections=app.bsky.feed.like"));
        assert!(url3.contains("cursor=1700000000123456"));

        // 4. Cursor = 0 should be omitted
        let url4 = build_subscription_url("wss://jetstream.example.com/subscribe", &cols, Some(0));
        assert!(!url4.contains("cursor="));
    }

    #[test]
    fn test_parse_like_frame() {
        let json = r#"{
            "did": "did:plc:alice",
            "time_us": 1700000000123456,
            "kind": "commit",
            "commit": {
                "collection": "app.bsky.feed.like",
                "rkey": "3k123",
                "operation": "create",
                "record": {
                    "$type": "app.bsky.feed.like",
                    "subject": { "uri": "at://did:plc:bob/app.bsky.feed.post/post1" }
                }
            }
        }"#;

        let (events, time_us) = parse_jetstream_frame(json).unwrap();
        assert_eq!(time_us, 1_700_000_000_123_456);
        assert_eq!(events.len(), 1);
        match &events[0] {
            JetstreamEvent::Interaction {
                user_did,
                post_uri,
                signal,
                timestamp_secs,
            } => {
                assert_eq!(user_did, "did:plc:alice");
                assert_eq!(post_uri, "at://did:plc:bob/app.bsky.feed.post/post1");
                assert_eq!(*signal, SignalType::Like);
                assert_eq!(*timestamp_secs, 1_700_000_000);
            }
            _ => panic!("Expected Interaction variant"),
        }
    }

    #[test]
    fn test_parse_repost_frame() {
        let json = r#"{
            "did": "did:plc:carol",
            "time_us": 1700000000500000,
            "kind": "commit",
            "commit": {
                "collection": "app.bsky.feed.repost",
                "rkey": "3k456",
                "operation": "create",
                "record": {
                    "$type": "app.bsky.feed.repost",
                    "subject": { "uri": "at://did:plc:dan/app.bsky.feed.post/post2" }
                }
            }
        }"#;

        let (events, time_us) = parse_jetstream_frame(json).unwrap();
        assert_eq!(time_us, 1_700_000_000_500_000);
        assert_eq!(events.len(), 1);
        match &events[0] {
            JetstreamEvent::Interaction {
                user_did,
                post_uri,
                signal,
                timestamp_secs,
            } => {
                assert_eq!(user_did, "did:plc:carol");
                assert_eq!(post_uri, "at://did:plc:dan/app.bsky.feed.post/post2");
                assert_eq!(*signal, SignalType::Repost);
                assert_eq!(*timestamp_secs, 1_700_000_000);
            }
            _ => panic!("Expected Repost variant"),
        }
    }

    #[test]
    fn test_parse_post_frame_with_reply_and_quote() {
        let json = r#"{
            "did": "did:plc:author",
            "time_us": 1700000000000000,
            "kind": "commit",
            "commit": {
                "collection": "app.bsky.feed.post",
                "rkey": "3kpost789",
                "operation": "create",
                "record": {
                    "$type": "app.bsky.feed.post",
                    "text": "Check this out!",
                    "reply": {
                        "root": { "uri": "at://did:plc:root/app.bsky.feed.post/root1" },
                        "parent": { "uri": "at://did:plc:parent/app.bsky.feed.post/parent1" }
                    },
                    "embed": {
                        "$type": "app.bsky.embed.record",
                        "record": { "uri": "at://did:plc:quoted/app.bsky.feed.post/quote1" }
                    }
                }
            }
        }"#;

        let (events, _) = parse_jetstream_frame(json).unwrap();
        assert_eq!(events.len(), 2);

        // 1. PostMeta
        match &events[0] {
            JetstreamEvent::PostMeta {
                post_uri,
                author_did,
                root_uri,
                parent_uri,
                created_at_secs,
            } => {
                assert_eq!(post_uri, "at://did:plc:author/app.bsky.feed.post/3kpost789");
                assert_eq!(author_did, "did:plc:author");
                assert_eq!(
                    root_uri.as_deref(),
                    Some("at://did:plc:root/app.bsky.feed.post/root1")
                );
                assert_eq!(
                    parent_uri.as_deref(),
                    Some("at://did:plc:parent/app.bsky.feed.post/parent1")
                );
                assert_eq!(*created_at_secs, 1_700_000_000);
            }
            _ => panic!("Expected PostMeta variant"),
        }

        // 2. Quote Interaction
        match &events[1] {
            JetstreamEvent::Interaction {
                user_did,
                post_uri,
                signal,
                timestamp_secs,
            } => {
                assert_eq!(user_did, "did:plc:author");
                assert_eq!(post_uri, "at://did:plc:quoted/app.bsky.feed.post/quote1");
                assert_eq!(*signal, SignalType::Quote);
                assert_eq!(*timestamp_secs, 1_700_000_000);
            }
            _ => panic!("Expected Quote Interaction"),
        }
    }

    #[test]
    fn test_parse_follow_frame() {
        let json = r#"{
            "did": "did:plc:follower",
            "time_us": 1700000000000000,
            "kind": "commit",
            "commit": {
                "collection": "app.bsky.graph.follow",
                "rkey": "3kfollow123",
                "operation": "create",
                "record": {
                    "$type": "app.bsky.graph.follow",
                    "subject": "did:plc:followed"
                }
            }
        }"#;

        let (events, _) = parse_jetstream_frame(json).unwrap();
        assert_eq!(events.len(), 1);
        match &events[0] {
            JetstreamEvent::Follow {
                follower_did,
                subject_did,
            } => {
                assert_eq!(follower_did, "did:plc:follower");
                assert_eq!(subject_did, "did:plc:followed");
            }
            _ => panic!("Expected Follow variant"),
        }
    }

    #[test]
    fn test_parse_delete_frame() {
        let json = r#"{
            "did": "did:plc:user",
            "time_us": 1700000000000000,
            "kind": "commit",
            "commit": {
                "collection": "app.bsky.feed.like",
                "rkey": "3kdelete",
                "operation": "delete"
            }
        }"#;

        let (events, _) = parse_jetstream_frame(json).unwrap();
        assert_eq!(events.len(), 1);
        match &events[0] {
            JetstreamEvent::Delete {
                did,
                collection,
                rkey,
            } => {
                assert_eq!(did, "did:plc:user");
                assert_eq!(collection, "app.bsky.feed.like");
                assert_eq!(rkey, "3kdelete");
            }
            _ => panic!("Expected Delete variant"),
        }
    }

    #[test]
    fn test_parse_malformed_or_irrelevant_frames() {
        assert!(parse_jetstream_frame("invalid json").is_none());
        assert!(parse_jetstream_frame("{}").is_none());
        assert!(parse_jetstream_frame(r#"{"kind":"identity","did":"did:plc:foo"}"#).is_none());
        assert!(parse_jetstream_frame(
            r#"{"kind":"commit","did":"did:plc:foo","commit":{"collection":"app.bsky.custom.unknown","operation":"create"}}"#
        ).is_none());
    }

    #[test]
    fn test_backoff_manager_exponential_growth_and_reset() {
        let mut backoff = BackoffManager::new(Duration::from_millis(500), Duration::from_secs(30));

        let b1 = backoff.next_backoff();
        assert!((400..=600).contains(&b1.as_millis()));
        assert_eq!(backoff.consecutive_failures(), 1);

        let b2 = backoff.next_backoff();
        assert!((800..=1200).contains(&b2.as_millis()));
        assert_eq!(backoff.consecutive_failures(), 2);

        // Advance several times to hit 30s cap
        for _ in 0..10 {
            backoff.next_backoff();
        }
        let b_cap = backoff.next_backoff();
        assert!((24_000..=36_000).contains(&b_cap.as_millis()));

        backoff.reset();
        assert_eq!(backoff.consecutive_failures(), 0);
        let b_reset = backoff.next_backoff();
        assert!((400..=600).contains(&b_reset.as_millis()));
    }

    #[test]
    fn test_cursor_tracker_monotonicity() {
        let tracker = CursorTracker::new(Some(100));
        assert_eq!(tracker.get(), Some(100));

        tracker.update(200);
        assert_eq!(tracker.get(), Some(200));

        tracker.update(150); // Out of order should not regress
        assert_eq!(tracker.get(), Some(200));

        tracker.update(300);
        assert_eq!(tracker.get(), Some(300));
    }

    #[test]
    fn test_apply_events_to_graph() {
        let interner = StringInterner::new();
        let graph = GraphStore::new();
        let now = BLUESKY_EPOCH_SECS + 5000;

        // 1. Like
        let like_event = JetstreamEvent::Interaction {
            user_did: CompactString::new("did:plc:alice"),
            post_uri: CompactString::new("at://did:plc:bob/app.bsky.feed.post/1"),
            signal: SignalType::Like,
            timestamp_secs: now,
        };
        apply_event_to_graph(&like_event, &interner, &graph);

        let uid = interner.lookup_id("did:plc:alice").unwrap();
        let pid = interner
            .lookup_id("at://did:plc:bob/app.bsky.feed.post/1")
            .unwrap();

        assert_eq!(graph.get_user_interactions(uid).len(), 1);
        assert_eq!(graph.get_post_interactions(pid).len(), 1);

        // 2. Follow
        let follow_event = JetstreamEvent::Follow {
            follower_did: CompactString::new("did:plc:alice"),
            subject_did: CompactString::new("did:plc:bob"),
        };
        apply_event_to_graph(&follow_event, &interner, &graph);
        let bob_id = interner.lookup_id("did:plc:bob").unwrap();
        assert_eq!(graph.get_user_follows(uid), vec![bob_id]);

        // 3. PostMeta
        let post_event = JetstreamEvent::PostMeta {
            post_uri: CompactString::new("at://did:plc:alice/app.bsky.feed.post/2"),
            author_did: CompactString::new("did:plc:alice"),
            root_uri: None,
            parent_uri: None,
            created_at_secs: now,
        };
        apply_event_to_graph(&post_event, &interner, &graph);
        let p2_id = interner
            .lookup_id("at://did:plc:alice/app.bsky.feed.post/2")
            .unwrap();
        assert!(graph.get_post_meta(p2_id).unwrap().is_root());

        // 4. Delete like
        let delete_like = JetstreamEvent::Delete {
            did: CompactString::new("did:plc:alice"),
            collection: CompactString::new("app.bsky.feed.like"),
            rkey: CompactString::new("3k123"),
        };
        apply_event_to_graph(&delete_like, &interner, &graph);
        assert!(graph.get_user_interactions(uid).is_empty());
    }

    struct TestMockJetstreamServer {
        addr: std::net::SocketAddr,
        event_tx: mpsc::Sender<String>,
        shutdown_tx: tokio::sync::watch::Sender<bool>,
    }

    impl TestMockJetstreamServer {
        async fn start() -> Self {
            let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
            let addr = listener.local_addr().unwrap();
            let (event_tx, event_rx) = mpsc::channel::<String>(100);
            let (shutdown_tx, mut shutdown_rx) = tokio::sync::watch::channel(false);
            let shared_rx = Arc::new(tokio::sync::Mutex::new(event_rx));

            tokio::spawn(async move {
                loop {
                    tokio::select! {
                        _ = shutdown_rx.changed() => {
                            if *shutdown_rx.borrow() {
                                break;
                            }
                        }
                        accept_res = listener.accept() => {
                            if let Ok((stream, _)) = accept_res {
                                let rx = Arc::clone(&shared_rx);
                                let mut shut = shutdown_rx.clone();
                                tokio::spawn(async move {
                                    if let Ok(mut ws) = tokio_tungstenite::accept_async(stream).await {
                                        loop {
                                            tokio::select! {
                                                _ = shut.changed() => {
                                                    if *shut.borrow() {
                                                        let _ = ws.close(None).await;
                                                        break;
                                                    }
                                                }
                                                msg = async {
                                                    let mut guard = rx.lock().await;
                                                    guard.recv().await
                                                } => {
                                                    match msg {
                                                        Some(json) => {
                                                            if ws.send(Message::Text(json)).await.is_err() {
                                                                break;
                                                            }
                                                        }
                                                        None => break,
                                                    }
                                                }
                                            }
                                        }
                                    }
                                });
                            }
                        }
                    }
                }
            });

            Self {
                addr,
                event_tx,
                shutdown_tx,
            }
        }

        fn ws_url(&self) -> String {
            format!("ws://{}", self.addr)
        }

        async fn send_like(&self, user_did: &str, post_uri: &str, time_us: u64) {
            let payload = serde_json::json!({
                "did": user_did,
                "time_us": time_us,
                "kind": "commit",
                "commit": {
                    "collection": "app.bsky.feed.like",
                    "rkey": "3k12345",
                    "operation": "create",
                    "record": {
                        "$type": "app.bsky.feed.like",
                        "subject": {
                            "uri": post_uri,
                            "cid": "bafyreih3..."
                        },
                        "createdAt": "2026-08-21T18:00:00Z"
                    }
                }
            });
            let _ = self.event_tx.send(payload.to_string()).await;
        }

        fn shutdown(&self) {
            let _ = self.shutdown_tx.send(true);
        }
    }

    #[tokio::test]
    async fn test_ingester_pipeline_mock_server_flow() {
        let server = TestMockJetstreamServer::start().await;
        let interner = Arc::new(StringInterner::new());
        let graph = Arc::new(GraphStore::new());

        let config = IngesterConfig::new(server.ws_url())
            .with_channel_capacity(100)
            .with_inactivity_timeout(Duration::from_secs(10));

        let ingester = JetstreamIngester::new(config, Arc::clone(&interner), Arc::clone(&graph));
        let cancel = CancellationToken::new();

        let mut join_set = JoinSet::new();
        ingester.start_pipeline(&mut join_set, cancel.clone());

        // Send a like event
        tokio::time::sleep(Duration::from_millis(50)).await;
        server
            .send_like(
                "did:plc:stream_user",
                "at://did:plc:author/app.bsky.feed.post/stream_post",
                1_700_000_000_000_000,
            )
            .await;

        tokio::time::sleep(Duration::from_millis(100)).await;
        cancel.cancel();

        while let Some(res) = join_set.join_next().await {
            assert!(res.unwrap().is_ok());
        }

        let uid = interner.lookup_id("did:plc:stream_user");
        assert!(uid.is_some());
        let interactions = graph.get_user_interactions(uid.unwrap());
        assert_eq!(interactions.len(), 1);

        let stats = ingester.stats_snapshot();
        assert!(stats.events_received >= 1);
        assert!(stats.events_processed >= 1);
        assert_eq!(stats.latest_cursor_us, 1_700_000_000_000_000);

        server.shutdown();
    }

    #[test]
    fn test_ingestion_tracker_backfill_progress_and_eta() {
        let start_us = 1_700_000_000_000_000;
        let stats = Arc::new(IngestionStats::new(Some(start_us)));
        let tracker = IngestionTracker::new(Arc::clone(&stats));

        // Overwrite target cursor to start_us + 10_000_000_000 (10,000s in future of start)
        let target_us = start_us + 10_000_000_000;
        stats
            .backfill_target_cursor_us
            .store(target_us, Ordering::Relaxed);

        // At start (latest == start) -> progress 0%
        let info = tracker.get_velocity_info();
        assert_eq!(info.initial_cursor_us, Some(start_us));
        assert_eq!(info.target_cursor_us, Some(target_us));
        assert!(info.backfill_progress_percent < 0.1);

        // Advance cursor halfway (5,000s)
        stats
            .latest_cursor_us
            .store(start_us + 5_000_000_000, Ordering::Relaxed);
        let info2 = tracker.get_velocity_info();
        assert!((info2.backfill_progress_percent - 50.0).abs() < 1.0);

        // Advance cursor to target (10,000s)
        stats.latest_cursor_us.store(target_us, Ordering::Relaxed);
        let info3 = tracker.get_velocity_info();
        assert!((info3.backfill_progress_percent - 100.0).abs() < 0.1);
        assert!(info3.is_live);
    }
}
