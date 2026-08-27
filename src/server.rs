#![forbid(unsafe_code)]

//! Axum HTTP XRPC web server exposing AT Protocol custom feed endpoints.
//!
//! # Endpoints
//!
//! - `GET /`: Interactive web dashboard single-page application.
//! - `GET /dashboard`: Alias for the interactive web dashboard.
//! - `GET /xrpc/app.bsky.feed.getFeedSkeleton`: Generates custom feed skeletons for Bluesky viewers.
//! - `GET /.well-known/did.json`: Returns the AT Protocol feed generator DID document.
//! - `GET /healthz`: Health check endpoint reporting graph and memory statistics.
//! - `GET /api/telemetry`: Real-time graph, ingestion, snapshot, and impression telemetry.
//! - `GET /api/taste-twins`: Co-interactor taste-twin discovery and cosine similarity scores.
//! - `GET /api/feed-preview`: Live candidate scoring preview with adjustable dials.
//! - `GET /api/explain`: 3-step graph proof chain explainer.
//!
//! # Architecture
//!
//! The server utilizes Axum on the Tokio asynchronous runtime with non-blocking graph traversal,
//! permissive CORS headers for XRPC browser interoperability, and coordinated graceful shutdown.

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::Instant;

use ahash::AHashMap;
use axum::extract::{Query, State};
use axum::http::header::{ACCEPT, AUTHORIZATION, CONTENT_TYPE};
use axum::http::{HeaderMap, HeaderValue, Method, StatusCode};
use axum::response::{Html, IntoResponse};
use axum::routing::{get, post};
use axum::{Json, Router};
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine;
use compact_str::CompactString;
use parking_lot::RwLock;
use rand::RngCore;
use serde::Deserialize;
use sha2::{Digest, Sha256};
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;
use tower_http::cors::CorsLayer;
use tracing::{error, info};

use crate::auth::{
    authenticate_pds_session_with_secret, build_secure_http_client,
    exchange_oauth_code_with_secret, extract_session_did_from_headers_with_secret,
    generate_pkce_pair, publish_feed_generator_record, resolve_identity_pds, validate_service_jwt,
    DPoPKey, OAuthSessionState, OAuthStateStore, OAuthUserSessionStore,
    DEFAULT_OAUTH_STATE_TTL_SECS, DEFAULT_SESSION_SECRET,
};
use crate::error::{FeedError, Result};
use crate::ingest::IngestionTracker;
use crate::preferences::UserPreferencesStore;
use crate::recommender::Recommender;
use crate::snapshot::SnapshotStatusTracker;
use crate::types::{
    ActiveUsersTelemetryInfo, ApiErrorResponse, ExplainQuery, FeedPreviewQuery, FeedPublishRequest,
    FeedSkeletonResponse, GenericStatusResponse, GraphTelemetryInfo, ImpressionTelemetryInfo,
    InternerTelemetryInfo, LoginRequestBody, MemoryTelemetryInfo, OAuthCallbackRequest,
    OAuthClientMetadata, OAuthLoginQuery, OAuthLoginResponse, PreferencesPayloadDto,
    PreferencesResponseDto, RecommendationDials, SavePreferencesRequestBody, SkeletonFeedPost,
    TasteTwinsQuery, TelemetryResponse, TopicWeights, UserDials, DEFAULT_MIN_LIKES,
    DEFAULT_PAGE_LIMIT, MAX_PAGE_LIMIT,
};

/// Embedded HTML content for the interactive web dashboard single-page application.
pub const DASHBOARD_HTML: &str = include_str!("assets/dashboard.html");

/// Default feed record key if not overridden by `FEED_RKEY` env var.
pub const DEFAULT_FEED_RKEY: &str = "for-your-consideration";

/// Number of parallel lock shards for partitioned active user tracking.
pub const ACTIVE_USERS_SHARDS: usize = 64;

/// Returns the shard index for a viewer identifier string.
#[inline]
#[must_use]
pub fn active_users_shard_idx(viewer: &str) -> usize {
    let mut hasher = crc32fast::Hasher::new();
    hasher.update(viewer.as_bytes());
    (hasher.finalize() as usize) & (ACTIVE_USERS_SHARDS - 1)
}

/// RAII guard that decrements the in-flight request counter when dropped.
#[derive(Debug)]
pub struct InFlightRequestGuard<'a> {
    tracker: &'a ActiveUsersTracker,
}

impl Drop for InFlightRequestGuard<'_> {
    fn drop(&mut self) {
        self.tracker.decrement_in_flight();
    }
}

/// High-concurrency 64-shard partitioned tracker for real-time and sliding-window feed consumers.
#[derive(Debug)]
pub struct ActiveUsersTracker {
    in_flight: AtomicUsize,
    shards: [RwLock<AHashMap<CompactString, u64>>; ACTIVE_USERS_SHARDS],
}

impl Default for ActiveUsersTracker {
    fn default() -> Self {
        Self::new()
    }
}

impl Clone for ActiveUsersTracker {
    fn clone(&self) -> Self {
        let mut new_shards: [RwLock<AHashMap<CompactString, u64>>; ACTIVE_USERS_SHARDS] =
            std::array::from_fn(|_| RwLock::new(AHashMap::new()));
        for (i, shard) in self.shards.iter().enumerate() {
            *new_shards[i].get_mut() = shard.read().clone();
        }
        Self {
            in_flight: AtomicUsize::new(self.in_flight.load(Ordering::SeqCst)),
            shards: new_shards,
        }
    }
}

impl ActiveUsersTracker {
    /// Creates a new partitioned active users tracker.
    #[must_use]
    pub fn new() -> Self {
        Self {
            in_flight: AtomicUsize::new(0),
            shards: std::array::from_fn(|_| RwLock::new(AHashMap::new())),
        }
    }

    /// Records activity for a viewer identifier at a specific timestamp.
    pub fn record_activity(&self, viewer: &str, timestamp_secs: u64) {
        let trimmed = viewer.trim();
        if trimmed.is_empty() {
            return;
        }
        let idx = active_users_shard_idx(trimmed);
        let mut shard = self.shards[idx].write();
        shard.insert(CompactString::new(trimmed), timestamp_secs);
    }

    /// Tracks a request entering the server, incrementing in-flight count and recording viewer activity.
    #[must_use]
    pub fn track_request<'a>(
        &'a self,
        viewer: Option<&str>,
        timestamp_secs: u64,
    ) -> InFlightRequestGuard<'a> {
        self.in_flight.fetch_add(1, Ordering::SeqCst);
        if let Some(v) = viewer {
            self.record_activity(v, timestamp_secs);
        }
        InFlightRequestGuard { tracker: self }
    }

    /// Safely decrements the in-flight request counter.
    pub fn decrement_in_flight(&self) {
        let _ = self
            .in_flight
            .fetch_update(Ordering::SeqCst, Ordering::SeqCst, |val| {
                Some(val.saturating_sub(1))
            });
    }

    /// Returns the number of currently active in-flight requests.
    #[must_use]
    pub fn in_flight_count(&self) -> usize {
        self.in_flight.load(Ordering::SeqCst)
    }

    /// Prunes viewer activity records older than the cutoff timestamp.
    pub fn prune_older_than(&self, cutoff_secs: u64) {
        for shard in &self.shards {
            let mut guard = shard.write();
            guard.retain(|_, &mut ts| ts >= cutoff_secs);
        }
    }

    /// Computes active user metrics across 1m, 5m, 15m sliding windows.
    #[must_use]
    pub fn get_telemetry(&self, now_secs: u64) -> ActiveUsersTelemetryInfo {
        let cutoff_1m = now_secs.saturating_sub(60);
        let cutoff_5m = now_secs.saturating_sub(300);
        let cutoff_15m = now_secs.saturating_sub(900);

        let in_flight_requests = self.in_flight_count();
        let mut concurrent_viewers_1m: usize = 0;
        let mut concurrent_viewers_5m: usize = 0;
        let mut concurrent_viewers_15m: usize = 0;
        let mut total_active_viewers: usize = 0;

        for shard in &self.shards {
            let guard = shard.read();
            total_active_viewers = total_active_viewers.saturating_add(guard.len());
            for &ts in guard.values() {
                if ts >= cutoff_1m {
                    concurrent_viewers_1m = concurrent_viewers_1m.saturating_add(1);
                }
                if ts >= cutoff_5m {
                    concurrent_viewers_5m = concurrent_viewers_5m.saturating_add(1);
                }
                if ts >= cutoff_15m {
                    concurrent_viewers_15m = concurrent_viewers_15m.saturating_add(1);
                }
            }
        }

        ActiveUsersTelemetryInfo {
            in_flight_requests,
            concurrent_viewers_1m,
            concurrent_viewers_5m,
            concurrent_viewers_15m,
            total_active_viewers,
        }
    }
}

/// Application state shared across all HTTP request handlers.
#[derive(Clone)]
pub struct AppState {
    /// Reference to the algorithmic recommendation engine.
    pub recommender: Arc<Recommender>,
    /// Store for user preference dials.
    pub preferences_store: Arc<UserPreferencesStore>,
    /// Store for pending OAuth authorization PKCE session states.
    pub oauth_store: Arc<OAuthStateStore>,
    /// Store for active authenticated user OAuth sessions (tokens + `DPoP` keys).
    pub user_oauth_sessions: Arc<OAuthUserSessionStore>,
    /// Tracker for snapshot persistence status and metrics.
    pub snapshot_tracker: Arc<SnapshotStatusTracker>,
    /// Tracker for real-time Jetstream firehose ingestion velocity and statistics.
    pub ingestion_tracker: Arc<IngestionTracker>,
    /// Tracker for concurrent in-flight and sliding-window active feed consumers.
    pub active_users_tracker: Arc<ActiveUsersTracker>,
    /// AT Protocol service DID (e.g. `did:web:feed.example.com`).
    pub service_did: CompactString,
    /// Fully-qualified hostname serving the feed generator (e.g. `feed.example.com`).
    pub hostname: CompactString,
    /// Feed record key identifier (e.g. `for-your-consideration`).
    pub feed_rkey: CompactString,
    /// Optional administrator DID authorized to publish or modify the official feed generator record.
    pub admin_did: Option<CompactString>,
    /// Server HMAC secret for cryptographically signing and verifying session tokens.
    pub session_secret: [u8; 32],
    /// Server initialization instant for uptime tracking.
    pub start_time: Instant,
}

impl AppState {
    /// Creates a new [`AppState`] instance with default snapshot, ingestion trackers, and OAuth store.
    #[must_use]
    pub fn new(
        recommender: Arc<Recommender>,
        service_did: impl Into<CompactString>,
        hostname: impl Into<CompactString>,
    ) -> Self {
        let session_secret =
            std::env::var("SESSION_SECRET").map_or(*DEFAULT_SESSION_SECRET, |sec| {
                let trimmed = sec.trim();
                if trimmed.is_empty() {
                    *DEFAULT_SESSION_SECRET
                } else {
                    let hash = Sha256::digest(trimmed.as_bytes());
                    let mut key = [0u8; 32];
                    key.copy_from_slice(&hash);
                    key
                }
            });

        Self {
            recommender,
            preferences_store: Arc::new(UserPreferencesStore::new()),
            oauth_store: Arc::new(OAuthStateStore::new()),
            user_oauth_sessions: Arc::new(OAuthUserSessionStore::new()),
            snapshot_tracker: Arc::new(SnapshotStatusTracker::default()),
            ingestion_tracker: Arc::new(IngestionTracker::default()),
            active_users_tracker: Arc::new(ActiveUsersTracker::new()),
            service_did: service_did.into(),
            hostname: hostname.into(),
            feed_rkey: CompactString::new(DEFAULT_FEED_RKEY),
            admin_did: None,
            session_secret,
            start_time: Instant::now(),
        }
    }

    /// Sets a custom session signing secret.
    #[must_use]
    pub const fn with_session_secret(mut self, secret: [u8; 32]) -> Self {
        self.session_secret = secret;
        self
    }

    /// Sets an optional administrator DID authorized to publish the feed generator record.
    #[must_use]
    pub fn with_admin_did(mut self, admin_did: Option<impl Into<CompactString>>) -> Self {
        self.admin_did = admin_did.map(Into::into);
        self
    }

    /// Sets a custom preferences store.
    #[must_use]
    pub fn with_preferences_store(mut self, preferences_store: Arc<UserPreferencesStore>) -> Self {
        self.preferences_store = preferences_store;
        self
    }

    /// Sets a custom OAuth PKCE state store.
    #[must_use]
    pub fn with_oauth_store(mut self, oauth_store: Arc<OAuthStateStore>) -> Self {
        self.oauth_store = oauth_store;
        self
    }

    /// Sets a custom snapshot status tracker.
    #[must_use]
    pub fn with_snapshot_tracker(mut self, tracker: Arc<SnapshotStatusTracker>) -> Self {
        self.snapshot_tracker = tracker;
        self
    }

    /// Sets a custom ingestion tracker.
    #[must_use]
    pub fn with_ingestion_tracker(mut self, tracker: Arc<IngestionTracker>) -> Self {
        self.ingestion_tracker = tracker;
        self
    }

    /// Sets a custom active users tracker.
    #[must_use]
    pub fn with_active_users_tracker(mut self, tracker: Arc<ActiveUsersTracker>) -> Self {
        self.active_users_tracker = tracker;
        self
    }

    /// Sets a custom feed record key.
    #[must_use]
    pub fn with_feed_rkey(mut self, feed_rkey: impl Into<CompactString>) -> Self {
        self.feed_rkey = feed_rkey.into();
        self
    }
}

/// Query parameters for `GET /xrpc/app.bsky.feed.getFeedSkeleton`.
#[derive(Debug, Clone, Deserialize)]
pub struct FeedSkeletonQuery {
    /// Canonical AT-URI of the requested feed generator (required).
    pub feed: Option<String>,
    /// Maximum number of posts to return (optional, default 30, max 100).
    pub limit: Option<usize>,
    /// Opaque pagination cursor (optional).
    pub cursor: Option<String>,
    /// Time-decay freshness dial (e.g. "realtime", "balanced", "weekly").
    pub freshness: Option<String>,
    /// Exploration serendipity dial (e.g. "familiar", "balanced", "`deep_dive`").
    pub discovery: Option<String>,
    /// Whether to include reply posts (e.g. "true", "all") or restrict to root posts only (optional).
    pub replies: Option<String>,
    /// Alternative boolean flag for including replies.
    pub include_replies: Option<bool>,
    /// Minimum engagement floor in likes or preset string ("emerging", "balanced", "curated", or numeric).
    #[serde(alias = "engagement_floor", default)]
    pub min_likes: Option<String>,
    /// Topic bias multiplier for Art category (0.0 to 5.0).
    pub art: Option<f32>,
    /// Topic bias multiplier for Tech category (0.0 to 5.0).
    pub tech: Option<f32>,
    /// Topic bias multiplier for Science category (0.0 to 5.0).
    pub science: Option<f32>,
    /// Topic bias multiplier for News category (0.0 to 5.0).
    pub news: Option<f32>,
    /// Topic bias multiplier for Culture category (0.0 to 5.0).
    pub culture: Option<f32>,
    /// Whether to generate structured explanation traces (optional bool).
    pub explain: Option<bool>,
}

/// Builds the production Axum HTTP router with all endpoints and middleware.
pub fn create_xrpc_router(state: AppState) -> Router {
    let cors = CorsLayer::new()
        .allow_methods([
            Method::GET,
            Method::HEAD,
            Method::OPTIONS,
            Method::POST,
            Method::DELETE,
        ])
        .allow_origin(tower_http::cors::Any)
        .allow_headers([AUTHORIZATION, CONTENT_TYPE, ACCEPT]);

    Router::new()
        .route("/", get(handle_get_dashboard))
        .route("/dashboard", get(handle_get_dashboard))
        .route("/oauth/callback", get(handle_get_dashboard))
        .route(
            "/oauth/client-metadata.json",
            get(handle_get_client_metadata),
        )
        .route("/client-metadata.json", get(handle_get_client_metadata))
        .route(
            "/xrpc/app.bsky.feed.getFeedSkeleton",
            get(handle_get_feed_skeleton),
        )
        .route(
            "/xrpc/app.bsky.feed.describeFeedGenerator",
            get(handle_describe_feed_generator),
        )
        .route("/.well-known/did.json", get(handle_get_did_doc))
        .route("/healthz", get(handle_get_healthz))
        .route("/api/telemetry", get(handle_get_telemetry))
        .route("/api/taste-twins", get(handle_get_taste_twins))
        .route("/api/feed-preview", get(handle_get_feed_preview))
        .route("/api/explain", get(handle_get_explain))
        .route("/api/auth/login", post(handle_post_auth_login))
        .route("/api/oauth/login", get(handle_get_oauth_login))
        .route("/api/oauth/callback", post(handle_post_oauth_callback))
        .route("/api/feed/publish", post(handle_post_feed_publish))
        .route(
            "/api/preferences",
            get(handle_get_preferences)
                .post(handle_post_preferences)
                .delete(handle_delete_preferences),
        )
        .layer(cors)
        .layer(axum::middleware::from_fn(security_headers_middleware))
        .layer(axum::extract::DefaultBodyLimit::max(64 * 1024))
        .with_state(state)
}

/// Middleware attaching standard HTTP defense-in-depth security headers.
async fn security_headers_middleware(
    request: axum::extract::Request,
    next: axum::middleware::Next,
) -> axum::response::Response {
    let mut response = next.run(request).await;
    let headers = response.headers_mut();
    headers.insert(
        axum::http::header::X_CONTENT_TYPE_OPTIONS,
        HeaderValue::from_static("nosniff"),
    );
    headers.insert(
        axum::http::header::X_FRAME_OPTIONS,
        HeaderValue::from_static("SAMEORIGIN"),
    );
    headers.insert(
        axum::http::header::REFERRER_POLICY,
        HeaderValue::from_static("strict-origin-when-cross-origin"),
    );
    headers.insert(
        axum::http::header::CONTENT_SECURITY_POLICY,
        HeaderValue::from_static(
            "default-src 'self'; img-src 'self' data: https:; style-src 'self' 'unsafe-inline'; script-src 'self' 'unsafe-inline'; connect-src 'self' https: wss:; frame-ancestors 'self';",
        ),
    );
    response
}

/// Handler for `GET /` and `GET /dashboard` serving the embedded SPA dashboard.
pub async fn handle_get_dashboard() -> impl IntoResponse {
    Html(DASHBOARD_HTML)
}

/// Handler for `GET /xrpc/app.bsky.feed.getFeedSkeleton`.
pub async fn handle_get_feed_skeleton(
    State(state): State<AppState>,
    headers: HeaderMap,
    Query(query): Query<FeedSkeletonQuery>,
) -> impl IntoResponse {
    let _feed_uri = match query.feed {
        Some(f) if !f.trim().is_empty() => f,
        _ => {
            return (
                StatusCode::BAD_REQUEST,
                [(
                    axum::http::header::CONTENT_TYPE,
                    HeaderValue::from_static("application/json"),
                )],
                Json(serde_json::json!({
                    "error": "InvalidRequest",
                    "message": "Missing required 'feed' parameter"
                })),
            )
                .into_response();
        }
    };

    // 1. Resolve viewer DID from Authorization Bearer JWT (verifying exp and aud)
    let now_secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();

    let viewer_did = headers
        .get(AUTHORIZATION)
        .and_then(|h| h.to_str().ok())
        .and_then(|auth_header| {
            validate_service_jwt(auth_header, Some(state.service_did.as_str()), now_secs)
                .ok()
                .map(|did| did.to_string())
        });

    let _in_flight_guard = state
        .active_users_tracker
        .track_request(viewer_did.as_deref().or(Some("anonymous")), now_secs);

    // 2. Precedence Hierarchy:
    //    1) Explicit HTTP query parameters (?freshness, ?discovery, ?art, etc.)
    //    2) Persisted UserDials (from UserPreferencesStore via viewer DID)
    //    3) System Defaults (UserDials::default())
    let base_dials = if let Some(ref did) = viewer_did {
        state
            .preferences_store
            .get_by_did(&state.recommender.interner, did)
            .unwrap_or_default()
    } else {
        UserDials::default()
    };

    let half_life_secs = match query.freshness.as_deref() {
        Some("realtime" | "fast" | "6h") => 6.0 * 3600.0,
        Some("balanced" | "36h") => 36.0 * 3600.0,
        Some("weekly" | "slow" | "168h") => 168.0 * 3600.0,
        Some(custom) => custom
            .parse::<f32>()
            .unwrap_or(base_dials.freshness_half_life_secs)
            .clamp(3600.0, 604_800.0),
        None => base_dials.freshness_half_life_secs,
    };

    let explore_ratio = match query.discovery.as_deref() {
        Some("familiar" | "low" | "5%") => 0.05,
        Some("balanced" | "med" | "15%") => 0.15,
        Some("deep_dive" | "deepdive" | "high" | "35%") => 0.35,
        Some(custom) => custom
            .parse::<f32>()
            .unwrap_or(base_dials.serendipity_ratio)
            .clamp(0.0, 0.50),
        None => base_dials.serendipity_ratio,
    };

    let topic_weights = TopicWeights {
        art: query
            .art
            .unwrap_or(base_dials.topic_weights.art)
            .clamp(0.0, 5.0),
        tech: query
            .tech
            .unwrap_or(base_dials.topic_weights.tech)
            .clamp(0.0, 5.0),
        science: query
            .science
            .unwrap_or(base_dials.topic_weights.science)
            .clamp(0.0, 5.0),
        news: query
            .news
            .unwrap_or(base_dials.topic_weights.news)
            .clamp(0.0, 5.0),
        culture: query
            .culture
            .unwrap_or(base_dials.topic_weights.culture)
            .clamp(0.0, 5.0),
    };

    let limit = query
        .limit
        .unwrap_or(DEFAULT_PAGE_LIMIT)
        .clamp(1, MAX_PAGE_LIMIT);
    let explain = query.explain.unwrap_or(false);

    let include_replies = match (query.replies.as_deref(), query.include_replies) {
        (Some("true" | "all" | "include" | "1"), _) | (_, Some(true)) => true,
        (Some("false" | "root_only" | "root" | "0"), _) | (_, Some(false)) => false,
        _ => base_dials.include_replies,
    };

    let min_likes = query
        .min_likes
        .as_deref()
        .map_or(base_dials.min_likes, |raw| {
            RecommendationDials::parse_engagement_floor(Some(raw))
        });

    let dials = RecommendationDials {
        half_life_secs,
        explore_ratio,
        topic_weights,
        explain,
        include_replies,
        min_likes,
        limit,
        cursor: query.cursor,
    };

    let now_secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();

    match state
        .recommender
        .recommend(viewer_did.as_deref(), &dials, now_secs)
    {
        Ok(rec) => {
            if let Some(ref did) = viewer_did {
                let post_ids: Vec<u32> = rec.posts.iter().map(|p| p.post_id).collect();
                state
                    .recommender
                    .record_impressions_by_did(Some(did), &post_ids, now_secs);
            }

            let skeleton = FeedSkeletonResponse {
                feed: rec
                    .posts
                    .into_iter()
                    .map(|p| SkeletonFeedPost {
                        post: p.uri,
                        reason: None,
                        feed_context: p.explain,
                    })
                    .collect(),
                cursor: rec.cursor,
            };
            (
                StatusCode::OK,
                [(
                    axum::http::header::CONTENT_TYPE,
                    HeaderValue::from_static("application/json"),
                )],
                Json(skeleton),
            )
                .into_response()
        }
        Err(err) => {
            error!("Error evaluating feed recommendation: {err}");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                [(
                    axum::http::header::CONTENT_TYPE,
                    HeaderValue::from_static("application/json"),
                )],
                Json(serde_json::json!({
                    "error": "InternalServerError",
                    "message": err.to_string()
                })),
            )
                .into_response()
        }
    }
}

/// Handler for `POST /api/auth/login`.
pub async fn handle_post_auth_login(
    State(state): State<AppState>,
    Json(body): Json<LoginRequestBody>,
) -> impl IntoResponse {
    match authenticate_pds_session_with_secret(
        &body.identifier,
        &body.password,
        body.pds_url.as_deref(),
        &state.session_secret,
    )
    .await
    {
        Ok(resp) => (
            StatusCode::OK,
            [(CONTENT_TYPE, HeaderValue::from_static("application/json"))],
            Json(resp),
        )
            .into_response(),
        Err(FeedError::InvalidInput(msg)) => (
            StatusCode::BAD_REQUEST,
            [(CONTENT_TYPE, HeaderValue::from_static("application/json"))],
            Json(ApiErrorResponse::new("InvalidRequest", msg)),
        )
            .into_response(),
        Err(FeedError::Auth(msg)) => (
            StatusCode::UNAUTHORIZED,
            [(CONTENT_TYPE, HeaderValue::from_static("application/json"))],
            Json(ApiErrorResponse::new("AuthenticationFailed", msg)),
        )
            .into_response(),
        Err(err) => (
            StatusCode::BAD_GATEWAY,
            [(CONTENT_TYPE, HeaderValue::from_static("application/json"))],
            Json(ApiErrorResponse::new("PdsError", err.to_string())),
        )
            .into_response(),
    }
}

/// Helper function to percent-encode query parameter values according to RFC 3986.
fn percent_encode_query_param(s: &str) -> String {
    let mut encoded = String::with_capacity(s.len() * 2);
    for b in s.bytes() {
        match b {
            b'a'..=b'z' | b'A'..=b'Z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                encoded.push(b as char);
            }
            _ => {
                use std::fmt::Write;
                let _ = write!(encoded, "%{:02X}", b);
            }
        }
    }
    encoded
}

/// Handler for `GET /oauth/client-metadata.json` and `GET /client-metadata.json`.
pub async fn handle_get_client_metadata(State(state): State<AppState>) -> impl IntoResponse {
    let metadata = OAuthClientMetadata::new_for_host(state.hostname.as_str());
    (
        StatusCode::OK,
        [(
            axum::http::header::CONTENT_TYPE,
            HeaderValue::from_static("application/json"),
        )],
        Json(metadata),
    )
}

/// Handler for `GET /api/oauth/login`.
pub async fn handle_get_oauth_login(
    State(state): State<AppState>,
    Query(query): Query<OAuthLoginQuery>,
) -> impl IntoResponse {
    let Some(handle) = query
        .handle
        .as_deref()
        .map(str::trim)
        .filter(|s| !s.is_empty())
    else {
        return (
            StatusCode::BAD_REQUEST,
            [(
                axum::http::header::CONTENT_TYPE,
                HeaderValue::from_static("application/json"),
            )],
            Json(ApiErrorResponse::new(
                "InvalidRequest",
                "Missing required 'handle' parameter",
            )),
        )
            .into_response();
    };

    let resolved = match resolve_identity_pds(handle).await {
        Ok(res) => res,
        Err(err) => {
            return (
                StatusCode::BAD_REQUEST,
                [(
                    axum::http::header::CONTENT_TYPE,
                    HeaderValue::from_static("application/json"),
                )],
                Json(ApiErrorResponse::new(
                    "IdentityResolutionFailed",
                    err.to_string(),
                )),
            )
                .into_response();
        }
    };

    let pkce = generate_pkce_pair();
    let mut state_bytes = [0u8; 24];
    rand::thread_rng().fill_bytes(&mut state_bytes);
    let state_nonce = URL_SAFE_NO_PAD.encode(state_bytes);

    let is_localhost = state.hostname.starts_with("localhost")
        || state.hostname.starts_with("127.0.0.1")
        || state.hostname.starts_with("0.0.0.0");
    let scheme = if is_localhost { "http" } else { "https" };
    let expected_redirect_uri = format!("{scheme}://{}/oauth/callback", state.hostname);

    let redirect_uri = if let Some(ref req_uri) = query.redirect_uri {
        let trimmed_req = req_uri.trim();
        let server_origin_prefix = format!("{scheme}://{}/", state.hostname);
        let is_valid = trimmed_req == expected_redirect_uri
            || (is_localhost
                && (trimmed_req.starts_with("http://127.0.0.1:")
                    || trimmed_req.starts_with("http://localhost:"))
                && (trimmed_req.ends_with("/oauth/callback") || trimmed_req.contains("/callback")))
            || (trimmed_req.starts_with(&server_origin_prefix)
                && (trimmed_req.ends_with("/oauth/callback")
                    || trimmed_req.ends_with("/callback")
                    || trimmed_req.contains("/oauth/")));
        if !is_valid {
            return (
                StatusCode::BAD_REQUEST,
                [(
                    axum::http::header::CONTENT_TYPE,
                    HeaderValue::from_static("application/json"),
                )],
                Json(ApiErrorResponse::new(
                    "InvalidRedirectUri",
                    format!(
                        "Invalid redirect_uri '{trimmed_req}'. Must match server origin callback whitelist"
                    ),
                )),
            )
                .into_response();
        }
        trimmed_req.to_string()
    } else {
        expected_redirect_uri
    };

    let now_secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();

    let dpop_key = DPoPKey::generate();

    state.oauth_store.insert(
        state_nonce.clone(),
        OAuthSessionState {
            code_verifier: pkce.verifier.clone(),
            handle: resolved.handle.to_string(),
            did: Some(resolved.did.to_string()),
            pds_url: resolved.pds_endpoint,
            token_endpoint: resolved.token_endpoint,
            redirect_uri: redirect_uri.clone(),
            created_at_secs: now_secs,
            dpop_private_key: Some(dpop_key.to_bytes_b64()),
        },
    );

    let client_id = if is_localhost {
        "http://127.0.0.1:3000/oauth/client-metadata.json".to_string()
    } else {
        format!("{scheme}://{}/oauth/client-metadata.json", state.hostname)
    };

    let auth_url = if let Some(ref par_endpoint) = resolved.par_endpoint {
        let http_client = build_secure_http_client();

        let par_form = [
            ("client_id", client_id.as_str()),
            ("response_type", "code"),
            ("redirect_uri", redirect_uri.as_str()),
            ("scope", "atproto transition:generic"),
            ("state", state_nonce.as_str()),
            ("code_challenge", pkce.challenge.as_str()),
            ("code_challenge_method", "S256"),
            ("login_hint", resolved.handle.as_str()),
        ];

        let dpop_proof = dpop_key.create_proof("POST", par_endpoint, None, None).ok();

        let mut req = http_client.post(par_endpoint);
        if let Some(ref proof) = dpop_proof {
            req = req.header("DPoP", proof);
        }

        let mut resp = req.form(&par_form).send().await;

        if let Ok(ref r) = resp {
            if r.status() == StatusCode::BAD_REQUEST || r.status() == StatusCode::UNAUTHORIZED {
                if let Some(nonce_val) = r.headers().get("DPoP-Nonce").and_then(|h| h.to_str().ok())
                {
                    if let Ok(retry_proof) =
                        dpop_key.create_proof("POST", par_endpoint, Some(nonce_val), None)
                    {
                        resp = http_client
                            .post(par_endpoint)
                            .header("DPoP", &retry_proof)
                            .form(&par_form)
                            .send()
                            .await;
                    }
                }
            }
        }

        match resp {
            Ok(resp) if resp.status().is_success() => {
                let json: serde_json::Value = resp.json().await.unwrap_or_default();
                if let Some(request_uri) = json["request_uri"].as_str() {
                    format!(
                        "{}?client_id={}&request_uri={}",
                        resolved.auth_endpoint,
                        percent_encode_query_param(&client_id),
                        percent_encode_query_param(request_uri.trim()),
                    )
                } else if !is_localhost && resolved.require_par {
                    return (
                        StatusCode::BAD_GATEWAY,
                        [(
                            axum::http::header::CONTENT_TYPE,
                            HeaderValue::from_static("application/json"),
                        )],
                        Json(ApiErrorResponse::new(
                            "ParFailed",
                            "Pushed Authorization Request required by PDS authorization server but endpoint response was missing request_uri",
                        )),
                    )
                        .into_response();
                } else {
                    format!(
                        "{}?client_id={}&response_type=code&redirect_uri={}&scope=atproto%20transition:generic&state={}&code_challenge={}&code_challenge_method=S256&login_hint={}",
                        resolved.auth_endpoint,
                        percent_encode_query_param(&client_id),
                        percent_encode_query_param(&redirect_uri),
                        percent_encode_query_param(&state_nonce),
                        percent_encode_query_param(&pkce.challenge),
                        percent_encode_query_param(resolved.handle.as_str()),
                    )
                }
            }
            _ => {
                if !is_localhost && resolved.require_par {
                    return (
                        StatusCode::BAD_GATEWAY,
                        [(
                            axum::http::header::CONTENT_TYPE,
                            HeaderValue::from_static("application/json"),
                        )],
                        Json(ApiErrorResponse::new(
                            "ParFailed",
                            "Pushed Authorization Request required by PDS authorization server but endpoint request failed",
                        )),
                    )
                        .into_response();
                }
                format!(
                    "{}?client_id={}&response_type=code&redirect_uri={}&scope=atproto%20transition:generic&state={}&code_challenge={}&code_challenge_method=S256&login_hint={}",
                    resolved.auth_endpoint,
                    percent_encode_query_param(&client_id),
                    percent_encode_query_param(&redirect_uri),
                    percent_encode_query_param(&state_nonce),
                    percent_encode_query_param(&pkce.challenge),
                    percent_encode_query_param(resolved.handle.as_str()),
                )
            }
        }
    } else {
        format!(
            "{}?client_id={}&response_type=code&redirect_uri={}&scope=atproto%20transition:generic&state={}&code_challenge={}&code_challenge_method=S256&login_hint={}",
            resolved.auth_endpoint,
            percent_encode_query_param(&client_id),
            percent_encode_query_param(&redirect_uri),
            percent_encode_query_param(&state_nonce),
            percent_encode_query_param(&pkce.challenge),
            percent_encode_query_param(resolved.handle.as_str()),
        )
    };

    (
        StatusCode::OK,
        [(
            axum::http::header::CONTENT_TYPE,
            HeaderValue::from_static("application/json"),
        )],
        Json(OAuthLoginResponse {
            status: CompactString::new("ok"),
            authorization_url: auth_url,
            state: state_nonce,
        }),
    )
        .into_response()
}

/// Handler for `POST /api/oauth/callback`.
pub async fn handle_post_oauth_callback(
    State(state): State<AppState>,
    Json(body): Json<OAuthCallbackRequest>,
) -> impl IntoResponse {
    let code = body.code.trim();
    let state_nonce = body.state.trim();

    if code.is_empty() || state_nonce.is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            [(
                axum::http::header::CONTENT_TYPE,
                HeaderValue::from_static("application/json"),
            )],
            Json(ApiErrorResponse::new(
                "InvalidRequest",
                "Missing required 'code' or 'state' parameter",
            )),
        )
            .into_response();
    }

    // Atomically take session state for replay defense
    let Some(session) = state.oauth_store.take(state_nonce) else {
        return (
            StatusCode::BAD_REQUEST,
            [(
                axum::http::header::CONTENT_TYPE,
                HeaderValue::from_static("application/json"),
            )],
            Json(ApiErrorResponse::new(
                "InvalidState",
                "Invalid or already used OAuth state token",
            )),
        )
            .into_response();
    };

    let now_secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();

    if now_secs.saturating_sub(session.created_at_secs) > DEFAULT_OAUTH_STATE_TTL_SECS {
        return (
            StatusCode::BAD_REQUEST,
            [(
                axum::http::header::CONTENT_TYPE,
                HeaderValue::from_static("application/json"),
            )],
            Json(ApiErrorResponse::new(
                "OAuthExpired",
                "OAuth authorization session has expired",
            )),
        )
            .into_response();
    }

    let is_localhost = state.hostname.starts_with("localhost")
        || state.hostname.starts_with("127.0.0.1")
        || state.hostname.starts_with("0.0.0.0");
    let scheme = if is_localhost { "http" } else { "https" };
    let client_id = if is_localhost {
        "http://127.0.0.1:3000/oauth/client-metadata.json".to_string()
    } else {
        format!("{scheme}://{}/oauth/client-metadata.json", state.hostname)
    };

    match exchange_oauth_code_with_secret(code, &session, &client_id, &state.session_secret).await {
        Ok((resp, maybe_oauth_session)) => {
            if let Some(user_session) = maybe_oauth_session {
                state
                    .user_oauth_sessions
                    .insert(user_session.did.clone(), user_session);
            }
            (
                StatusCode::OK,
                [(
                    axum::http::header::CONTENT_TYPE,
                    HeaderValue::from_static("application/json"),
                )],
                Json(resp),
            )
                .into_response()
        }
        Err(FeedError::Auth(msg)) => (
            StatusCode::UNAUTHORIZED,
            [(
                axum::http::header::CONTENT_TYPE,
                HeaderValue::from_static("application/json"),
            )],
            Json(ApiErrorResponse::new("AuthenticationFailed", msg)),
        )
            .into_response(),
        Err(err) => (
            StatusCode::BAD_GATEWAY,
            [(
                axum::http::header::CONTENT_TYPE,
                HeaderValue::from_static("application/json"),
            )],
            Json(ApiErrorResponse::new(
                "TokenExchangeFailed",
                err.to_string(),
            )),
        )
            .into_response(),
    }
}

/// Handler for `POST /api/feed/publish`.
pub async fn handle_post_feed_publish(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(body): Json<FeedPublishRequest>,
) -> impl IntoResponse {
    let Some(viewer_did) =
        extract_session_did_from_headers_with_secret(&headers, &state.session_secret)
    else {
        return (
            StatusCode::UNAUTHORIZED,
            [(
                axum::http::header::CONTENT_TYPE,
                HeaderValue::from_static("application/json"),
            )],
            Json(ApiErrorResponse::new(
                "Unauthorized",
                "Missing or invalid authorization token",
            )),
        )
            .into_response();
    };

    let auth_header = headers
        .get("authorization")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    let token = auth_header
        .strip_prefix("Bearer ")
        .or_else(|| auth_header.strip_prefix("bearer "))
        .or_else(|| auth_header.strip_prefix("BEARER "))
        .unwrap_or(auth_header)
        .trim();

    // Check optional administrator authorization restriction
    if let Some(admin_did) = &state.admin_did {
        if viewer_did.as_str() != admin_did.as_str() {
            return (
                StatusCode::FORBIDDEN,
                [(
                    axum::http::header::CONTENT_TYPE,
                    HeaderValue::from_static("application/json"),
                )],
                Json(ApiErrorResponse::new(
                    "Forbidden",
                    format!(
                        "User '{}' is not authorized to publish this feed generator (restricted to administrator '{}')",
                        viewer_did, admin_did
                    ),
                )),
            )
                .into_response();
        }
    }

    let maybe_oauth_session = state.user_oauth_sessions.get(viewer_did.as_str());

    match publish_feed_generator_record(
        &viewer_did,
        token,
        &body,
        state.service_did.as_str(),
        None,
        maybe_oauth_session.as_ref(),
    )
    .await
    {
        Ok(resp) => (
            StatusCode::OK,
            [(
                axum::http::header::CONTENT_TYPE,
                HeaderValue::from_static("application/json"),
            )],
            Json(resp),
        )
            .into_response(),
        Err(FeedError::InvalidInput(msg)) => (
            StatusCode::BAD_REQUEST,
            [(
                axum::http::header::CONTENT_TYPE,
                HeaderValue::from_static("application/json"),
            )],
            Json(ApiErrorResponse::new("InvalidInput", msg)),
        )
            .into_response(),
        Err(FeedError::Auth(msg)) => (
            StatusCode::UNAUTHORIZED,
            [(
                axum::http::header::CONTENT_TYPE,
                HeaderValue::from_static("application/json"),
            )],
            Json(ApiErrorResponse::new("Unauthorized", msg)),
        )
            .into_response(),
        Err(err) => (
            StatusCode::BAD_GATEWAY,
            [(
                axum::http::header::CONTENT_TYPE,
                HeaderValue::from_static("application/json"),
            )],
            Json(ApiErrorResponse::new("PublishFailed", err.to_string())),
        )
            .into_response(),
    }
}

/// Handler for `GET /api/preferences`.
pub async fn handle_get_preferences(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> impl IntoResponse {
    let Some(viewer_did) =
        extract_session_did_from_headers_with_secret(&headers, &state.session_secret)
    else {
        return (
            StatusCode::UNAUTHORIZED,
            [(CONTENT_TYPE, HeaderValue::from_static("application/json"))],
            Json(ApiErrorResponse::new(
                "Unauthorized",
                "Missing or invalid authorization token",
            )),
        )
            .into_response();
    };

    let saved = state
        .preferences_store
        .get_by_did(&state.recommender.interner, &viewer_did);
    let is_custom = saved.is_some();
    let dials = saved.unwrap_or_default();

    let resp = PreferencesResponseDto {
        did: viewer_did,
        preferences: PreferencesPayloadDto {
            freshness_hours: dials.freshness_half_life_hours(),
            discovery_ratio: dials.discovery_ratio(),
            topic_weights: dials.topic_weights,
            include_replies: dials.include_replies,
            min_likes: dials.min_likes,
        },
        is_custom,
        dials: Some(dials.into()),
    };

    (
        StatusCode::OK,
        [(CONTENT_TYPE, HeaderValue::from_static("application/json"))],
        Json(resp),
    )
        .into_response()
}

/// Handler for `POST /api/preferences`.
pub async fn handle_post_preferences(
    State(state): State<AppState>,
    headers: HeaderMap,
    Json(body): Json<SavePreferencesRequestBody>,
) -> impl IntoResponse {
    let Some(viewer_did) =
        extract_session_did_from_headers_with_secret(&headers, &state.session_secret)
    else {
        return (
            StatusCode::UNAUTHORIZED,
            [(CONTENT_TYPE, HeaderValue::from_static("application/json"))],
            Json(ApiErrorResponse::new(
                "Unauthorized",
                "Missing or invalid authorization token",
            )),
        )
            .into_response();
    };

    let include_replies = body.include_replies.unwrap_or(false);
    let min_likes = body.min_likes.unwrap_or(DEFAULT_MIN_LIKES);
    let dials = UserDials {
        freshness_half_life_secs: body.freshness_hours * 3600.0,
        serendipity_ratio: body.discovery_ratio,
        topic_weights: body.topic_weights.unwrap_or_default(),
        include_replies,
        min_likes,
        updated_at_secs: std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs(),
    };

    if let Err(err) = dials.validate() {
        return (
            StatusCode::BAD_REQUEST,
            [(CONTENT_TYPE, HeaderValue::from_static("application/json"))],
            Json(ApiErrorResponse::new("InvalidInput", err)),
        )
            .into_response();
    }

    state
        .preferences_store
        .set_by_did(&state.recommender.interner, &viewer_did, dials);

    (
        StatusCode::OK,
        [(CONTENT_TYPE, HeaderValue::from_static("application/json"))],
        Json(GenericStatusResponse {
            status: "ok".to_string(),
            message: Some("Preferences saved successfully".to_string()),
            did: Some(viewer_did),
            preferences: Some(PreferencesPayloadDto {
                freshness_hours: dials.freshness_half_life_hours(),
                discovery_ratio: dials.discovery_ratio(),
                topic_weights: dials.topic_weights,
                include_replies: dials.include_replies,
                min_likes: dials.min_likes,
            }),
            dials: Some(dials.into()),
        }),
    )
        .into_response()
}

/// Handler for `DELETE /api/preferences`.
pub async fn handle_delete_preferences(
    State(state): State<AppState>,
    headers: HeaderMap,
) -> impl IntoResponse {
    let Some(viewer_did) =
        extract_session_did_from_headers_with_secret(&headers, &state.session_secret)
    else {
        return (
            StatusCode::UNAUTHORIZED,
            [(CONTENT_TYPE, HeaderValue::from_static("application/json"))],
            Json(ApiErrorResponse::new(
                "Unauthorized",
                "Missing or invalid authorization token",
            )),
        )
            .into_response();
    };

    state
        .preferences_store
        .delete_by_did(&state.recommender.interner, &viewer_did);

    (
        StatusCode::OK,
        [(CONTENT_TYPE, HeaderValue::from_static("application/json"))],
        Json(GenericStatusResponse {
            status: "reset_to_defaults".to_string(),
            message: Some("Preferences reset to system defaults".to_string()),
            did: Some(viewer_did),
            preferences: None,
            dials: None,
        }),
    )
        .into_response()
}

/// Handler for `GET /.well-known/did.json`.
pub async fn handle_get_did_doc(State(state): State<AppState>) -> impl IntoResponse {
    let hostname = state.hostname.as_str().trim_end_matches('/');
    let doc = serde_json::json!({
        "@context": ["https://www.w3.org/ns/did/v1"],
        "id": state.service_did.as_str(),
        "service": [{
            "id": "#bsky_fg",
            "type": "BskyFeedGenerator",
            "serviceEndpoint": format!("https://{hostname}")
        }]
    });

    (
        StatusCode::OK,
        [(
            axum::http::header::CONTENT_TYPE,
            HeaderValue::from_static("application/json"),
        )],
        Json(doc),
    )
}

/// Handler for `GET /xrpc/app.bsky.feed.describeFeedGenerator`.
pub async fn handle_describe_feed_generator(State(state): State<AppState>) -> impl IntoResponse {
    let hostname = state.hostname.as_str().trim_end_matches('/');
    let resp = serde_json::json!({
        "did": state.service_did.as_str(),
        "feeds": [
            {
                "uri": format!("at://{}/app.bsky.feed.generator/{}", state.service_did, state.feed_rkey)
            }
        ],
        "links": {
            "privacyPolicy": format!("https://{hostname}/privacy"),
            "termsOfService": format!("https://{hostname}/terms")
        }
    });

    (
        StatusCode::OK,
        [(
            axum::http::header::CONTENT_TYPE,
            HeaderValue::from_static("application/json"),
        )],
        Json(resp),
    )
}

/// Handler for `GET /healthz`.
pub async fn handle_get_healthz(State(state): State<AppState>) -> impl IntoResponse {
    let stats = state.recommender.graph.get_stats();
    let uptime = state.start_time.elapsed().as_secs();

    let resp = serde_json::json!({
        "status": "ok",
        "nodes": stats.total_users + stats.total_posts,
        "edges": stats.total_interactions,
        "interned_strings": state.recommender.interner.len(),
        "uptime_seconds": uptime
    });

    (
        StatusCode::OK,
        [(
            axum::http::header::CONTENT_TYPE,
            HeaderValue::from_static("application/json"),
        )],
        Json(resp),
    )
}

/// Handler for `GET /api/telemetry`.
pub async fn handle_get_telemetry(State(state): State<AppState>) -> impl IntoResponse {
    let now_secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();

    let stats = state.recommender.graph.get_stats();
    let interner_len = state.recommender.interner.len();
    let uptime = state.start_time.elapsed().as_secs();

    let graph_info = GraphTelemetryInfo {
        total_nodes: stats.total_users + stats.total_posts,
        total_users: stats.total_users,
        total_posts: stats.total_posts,
        total_edges: stats.total_interactions,
        total_follows: stats.total_follows,
        post_metadata_entries: stats.total_metadata_entries,
        active_velocity_posts: stats.active_velocity_posts,
    };

    let interner_info = InternerTelemetryInfo {
        total_interned_strings: interner_len,
    };

    let graph_bytes = state.recommender.graph.estimated_size_bytes();
    let interner_bytes = state.recommender.interner.estimated_size_bytes();
    let impression_bytes = state.recommender.impression_store.estimated_size_bytes();
    let total_estimated_bytes = graph_bytes
        .saturating_add(interner_bytes)
        .saturating_add(impression_bytes);

    let memory_info = MemoryTelemetryInfo {
        graph_bytes,
        interner_bytes,
        impression_bytes,
        total_estimated_bytes,
        formatted_total: crate::types::format_memory_bytes(total_estimated_bytes),
    };

    let snapshot_info = state.snapshot_tracker.get_status();
    let ingestion_info = state.ingestion_tracker.get_velocity_info();

    let impression_info = ImpressionTelemetryInfo {
        total_tracked_viewers: state.recommender.impression_store.total_viewers(),
        hard_suppression_window_secs: crate::recommender::HARD_SUPPRESSION_WINDOW_SECS,
        fatigue_decay_window_secs: crate::recommender::FATIGUE_WINDOW_SECS,
    };

    let active_users_info = state.active_users_tracker.get_telemetry(now_secs);

    let response = TelemetryResponse {
        status: "ok".to_string(),
        uptime_seconds: uptime,
        graph: graph_info,
        interner: interner_info,
        memory: memory_info,
        ingestion: ingestion_info,
        snapshot: snapshot_info,
        impression_store: impression_info,
        active_users: active_users_info,
        admin_did: state.admin_did.as_ref().map(ToString::to_string),
    };

    (
        StatusCode::OK,
        [(
            axum::http::header::CONTENT_TYPE,
            HeaderValue::from_static("application/json"),
        )],
        Json(response),
    )
}

/// Handler for `GET /api/taste-twins`.
pub async fn handle_get_taste_twins(
    State(state): State<AppState>,
    Query(query): Query<TasteTwinsQuery>,
) -> impl IntoResponse {
    let Some(target) = query.target_identifier() else {
        return (
            StatusCode::BAD_REQUEST,
            [(
                axum::http::header::CONTENT_TYPE,
                HeaderValue::from_static("application/json"),
            )],
            Json(ApiErrorResponse::new(
                "InvalidRequest",
                "Missing required 'did' or 'handle' parameter",
            )),
        )
            .into_response();
    };

    let limit = query.limit_or_default();

    match state.recommender.find_taste_twins(target, limit) {
        Ok(twins_resp) => (
            StatusCode::OK,
            [(
                axum::http::header::CONTENT_TYPE,
                HeaderValue::from_static("application/json"),
            )],
            Json(twins_resp),
        )
            .into_response(),
        Err(err) => {
            error!("Error finding taste twins for '{target}': {err}");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                [(
                    axum::http::header::CONTENT_TYPE,
                    HeaderValue::from_static("application/json"),
                )],
                Json(ApiErrorResponse::new(
                    "InternalServerError",
                    err.to_string(),
                )),
            )
                .into_response()
        }
    }
}

/// Handler for `GET /api/feed-preview`.
pub async fn handle_get_feed_preview(
    State(state): State<AppState>,
    Query(query): Query<FeedPreviewQuery>,
) -> impl IntoResponse {
    let now_secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();

    let viewer_opt = query.viewer_identifier();
    let dials = query.to_dials();

    let _in_flight_guard = state
        .active_users_tracker
        .track_request(viewer_opt.or(Some("preview-viewer")), now_secs);

    match state.recommender.recommend_preview(viewer_opt, &dials) {
        Ok(preview_resp) => (
            StatusCode::OK,
            [(
                axum::http::header::CONTENT_TYPE,
                HeaderValue::from_static("application/json"),
            )],
            Json(preview_resp),
        )
            .into_response(),
        Err(err) => {
            error!("Error evaluating feed preview: {err}");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                [(
                    axum::http::header::CONTENT_TYPE,
                    HeaderValue::from_static("application/json"),
                )],
                Json(ApiErrorResponse::new(
                    "InternalServerError",
                    err.to_string(),
                )),
            )
                .into_response()
        }
    }
}

/// Handler for `GET /api/explain`.
pub async fn handle_get_explain(
    State(state): State<AppState>,
    Query(query): Query<ExplainQuery>,
) -> impl IntoResponse {
    let Some(post_uri) = query.post_uri() else {
        return (
            StatusCode::BAD_REQUEST,
            [(
                axum::http::header::CONTENT_TYPE,
                HeaderValue::from_static("application/json"),
            )],
            Json(ApiErrorResponse::new(
                "InvalidRequest",
                "Missing required 'uri' or 'post' parameter",
            )),
        )
            .into_response();
    };

    let viewer = query.viewer_identifier().unwrap_or("");

    match state.recommender.explain_recommendation(viewer, post_uri) {
        Ok(proof_chain) => (
            StatusCode::OK,
            [(
                axum::http::header::CONTENT_TYPE,
                HeaderValue::from_static("application/json"),
            )],
            Json(proof_chain),
        )
            .into_response(),
        Err(err) => {
            error!("Error generating proof chain for post '{post_uri}': {err}");
            (
                StatusCode::INTERNAL_SERVER_ERROR,
                [(
                    axum::http::header::CONTENT_TYPE,
                    HeaderValue::from_static("application/json"),
                )],
                Json(ApiErrorResponse::new(
                    "InternalServerError",
                    err.to_string(),
                )),
            )
                .into_response()
        }
    }
}

/// Runs the Axum XRPC server on the provided TCP listener with graceful cancellation support.
pub async fn serve_xrpc(
    listener: TcpListener,
    router: Router,
    cancel_token: CancellationToken,
) -> Result<()> {
    let local_addr = listener.local_addr().map_err(FeedError::Io)?;
    info!("Axum XRPC server listening on http://{local_addr}");

    axum::serve(listener, router.into_make_service())
        .with_graceful_shutdown(async move {
            cancel_token.cancelled().await;
            info!("Axum XRPC server received cancellation signal, shutting down...");
        })
        .await
        .map_err(FeedError::Io)
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic, missing_docs)]
mod tests {
    use std::sync::Arc;

    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use http_body_util::BodyExt;
    use tower::ServiceExt;

    use super::*;
    use crate::graph::GraphStore;
    use crate::interner::StringInterner;
    use crate::types::{
        FeedPreviewResponse, GraphProofChain, LoginSuccessResponse, PreferencesResponseDto,
        SavePreferencesRequestBody, SignalType, TasteTwinsResponse, TopicWeights,
    };

    fn create_test_state() -> AppState {
        let interner = Arc::new(StringInterner::new());
        let graph = Arc::new(GraphStore::new());

        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        let u1 = interner.intern("did:plc:user1");
        let u2 = interner.intern("did:plc:user2");
        let u3 = interner.intern("did:plc:user3");
        let pid = interner.intern("at://did:plc:author/app.bsky.feed.post/123");
        let aid = interner.intern("did:plc:author");
        graph.record_post_meta(pid, aid, None, None, now);
        graph.record_interaction(u1, pid, SignalType::Like, now);
        graph.record_interaction(u2, pid, SignalType::Like, now);
        graph.record_interaction(u3, pid, SignalType::Like, now);

        let recommender = Arc::new(Recommender::new(interner, graph));
        AppState::new(recommender, "did:web:feed.example.com", "feed.example.com")
    }

    #[tokio::test]
    async fn test_dashboard_root_endpoint() {
        let state = create_test_state();
        let app = create_xrpc_router(state);

        let req = Request::builder().uri("/").body(Body::empty()).unwrap();

        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        assert!(resp
            .headers()
            .get("content-type")
            .unwrap()
            .to_str()
            .unwrap()
            .starts_with("text/html"));

        let body = resp.into_body().collect().await.unwrap().to_bytes();
        let html = String::from_utf8(body.to_vec()).unwrap();
        assert!(html.contains("<!DOCTYPE html>"));
        assert!(html.contains("id=\"telemetry\""));
        assert!(html.contains("id=\"taste-twins\""));
        assert!(html.contains("id=\"dials\""));
        assert!(html.contains("id=\"feed-preview\""));
        assert!(html.contains("id=\"proof-modal\""));
    }

    #[tokio::test]
    async fn test_dashboard_alias_endpoint() {
        let state = create_test_state();
        let app = create_xrpc_router(state);

        let req = Request::builder()
            .uri("/dashboard")
            .body(Body::empty())
            .unwrap();

        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        assert!(resp
            .headers()
            .get("content-type")
            .unwrap()
            .to_str()
            .unwrap()
            .starts_with("text/html"));

        let body = resp.into_body().collect().await.unwrap().to_bytes();
        let html = String::from_utf8(body.to_vec()).unwrap();
        assert!(html.contains("FOR YOUR CONSIDERATION") || html.contains("FYC"));
    }

    #[tokio::test]
    async fn test_healthz_endpoint() {
        let state = create_test_state();
        let app = create_xrpc_router(state);

        let req = Request::builder()
            .uri("/healthz")
            .body(Body::empty())
            .unwrap();

        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);

        let body = resp.into_body().collect().await.unwrap().to_bytes();
        let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(json["status"], "ok");
        assert!(json["nodes"].as_u64().unwrap() > 0);
    }

    #[tokio::test]
    async fn test_did_doc_endpoint() {
        let state = create_test_state();
        let app = create_xrpc_router(state);

        let req = Request::builder()
            .uri("/.well-known/did.json")
            .body(Body::empty())
            .unwrap();

        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);

        let body = resp.into_body().collect().await.unwrap().to_bytes();
        let doc: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(doc["id"], "did:web:feed.example.com");
        assert_eq!(
            doc["service"][0]["serviceEndpoint"],
            "https://feed.example.com"
        );
    }

    #[tokio::test]
    async fn test_get_feed_skeleton_missing_feed_error() {
        let state = create_test_state();
        let app = create_xrpc_router(state);

        let req = Request::builder()
            .uri("/xrpc/app.bsky.feed.getFeedSkeleton")
            .body(Body::empty())
            .unwrap();

        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn test_get_feed_skeleton_valid() {
        let state = create_test_state();
        let app = create_xrpc_router(state);

        let req = Request::builder()
            .uri("/xrpc/app.bsky.feed.getFeedSkeleton?feed=at://did:plc:feed/app.bsky.feed.generator/foryou&limit=10")
            .body(Body::empty())
            .unwrap();

        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);

        let body = resp.into_body().collect().await.unwrap().to_bytes();
        let skeleton: FeedSkeletonResponse = serde_json::from_slice(&body).unwrap();
        assert_eq!(skeleton.feed.len(), 1);
        assert_eq!(
            skeleton.feed[0].post.as_str(),
            "at://did:plc:author/app.bsky.feed.post/123"
        );
    }

    #[tokio::test]
    async fn test_get_feed_skeleton_records_impressions_for_authenticated_viewer() {
        let state = create_test_state();
        let app = create_xrpc_router(state.clone());

        // Construct mock JWT for did:plc:test_viewer
        let header_b64 = "eyJhbGciOiJub25lIn0";
        let payload_b64 = "eyJpc3MiOiJkaWQ6cGxjOnRlc3Rfdmlld2VyIn0";
        let sig_b64 = "c2ln";
        let token = format!("{header_b64}.{payload_b64}.{sig_b64}");

        let req1 = Request::builder()
            .uri("/xrpc/app.bsky.feed.getFeedSkeleton?feed=at://did:plc:feed/app.bsky.feed.generator/foryou&limit=10")
            .header("Authorization", format!("Bearer {token}"))
            .body(Body::empty())
            .unwrap();

        let resp1 = app.clone().oneshot(req1).await.unwrap();
        assert_eq!(resp1.status(), StatusCode::OK);

        let body1 = resp1.into_body().collect().await.unwrap().to_bytes();
        let skeleton1: FeedSkeletonResponse = serde_json::from_slice(&body1).unwrap();
        assert_eq!(skeleton1.feed.len(), 1);

        // Verify impression was recorded in state.recommender.impression_store
        let viewer_id = state.recommender.interner.intern("did:plc:test_viewer");
        let post_id = state
            .recommender
            .interner
            .intern("at://did:plc:author/app.bsky.feed.post/123");
        assert!(state
            .recommender
            .impression_store()
            .contains_impression(viewer_id, post_id));

        // Subsequent immediate request by the same viewer should suppress the seen post (100% hard suppression)
        let req2 = Request::builder()
            .uri("/xrpc/app.bsky.feed.getFeedSkeleton?feed=at://did:plc:feed/app.bsky.feed.generator/foryou&limit=10")
            .header("Authorization", format!("Bearer {token}"))
            .body(Body::empty())
            .unwrap();

        let resp2 = app.oneshot(req2).await.unwrap();
        assert_eq!(resp2.status(), StatusCode::OK);

        let body2 = resp2.into_body().collect().await.unwrap().to_bytes();
        let skeleton2: FeedSkeletonResponse = serde_json::from_slice(&body2).unwrap();
        // Under smooth continuous soft damping, the seen post is softly dampened (0.15x) rather than dropped completely
        assert_eq!(skeleton2.feed.len(), 1);
    }

    #[tokio::test]
    async fn test_api_telemetry_endpoint() {
        let state = create_test_state();
        let app = create_xrpc_router(state);

        let req = Request::builder()
            .uri("/api/telemetry")
            .body(Body::empty())
            .unwrap();

        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
        assert_eq!(
            resp.headers().get("content-type").unwrap(),
            "application/json"
        );

        let body = resp.into_body().collect().await.unwrap().to_bytes();
        let telemetry: TelemetryResponse = serde_json::from_slice(&body).unwrap();
        assert_eq!(telemetry.status, "ok");
        assert!(telemetry.graph.total_nodes > 0);
        assert!(telemetry.interner.total_interned_strings > 0);
    }

    #[tokio::test]
    async fn test_api_taste_twins_missing_param_returns_400() {
        let state = create_test_state();
        let app = create_xrpc_router(state);

        let req = Request::builder()
            .uri("/api/taste-twins")
            .body(Body::empty())
            .unwrap();

        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);

        let body = resp.into_body().collect().await.unwrap().to_bytes();
        let err: ApiErrorResponse = serde_json::from_slice(&body).unwrap();
        assert_eq!(err.error, "InvalidRequest");
    }

    #[tokio::test]
    async fn test_api_taste_twins_valid_query() {
        let state = create_test_state();
        let app = create_xrpc_router(state);

        let req = Request::builder()
            .uri("/api/taste-twins?did=did:plc:test_user&limit=5")
            .body(Body::empty())
            .unwrap();

        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);

        let body = resp.into_body().collect().await.unwrap().to_bytes();
        let twins: TasteTwinsResponse = serde_json::from_slice(&body).unwrap();
        assert_eq!(twins.viewer_did, "did:plc:test_user");
    }

    #[tokio::test]
    async fn test_api_feed_preview_endpoint() {
        let state = create_test_state();
        let app = create_xrpc_router(state);

        let req = Request::builder()
            .uri("/api/feed-preview?viewer=did:plc:test_user&freshness=realtime&art=2.0&tech=0.5&limit=10&explain=true")
            .body(Body::empty())
            .unwrap();

        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);

        let body = resp.into_body().collect().await.unwrap().to_bytes();
        let preview: FeedPreviewResponse = serde_json::from_slice(&body).unwrap();
        assert_eq!(preview.viewer_did, "did:plc:test_user");
    }

    #[tokio::test]
    async fn test_api_explain_missing_uri_returns_400() {
        let state = create_test_state();
        let app = create_xrpc_router(state);

        let req = Request::builder()
            .uri("/api/explain?viewer=did:plc:test_user")
            .body(Body::empty())
            .unwrap();

        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);

        let body = resp.into_body().collect().await.unwrap().to_bytes();
        let err: ApiErrorResponse = serde_json::from_slice(&body).unwrap();
        assert_eq!(err.error, "InvalidRequest");
    }

    #[tokio::test]
    async fn test_api_explain_valid_query() {
        let state = create_test_state();
        let app = create_xrpc_router(state);

        let req = Request::builder()
            .uri("/api/explain?viewer=did:plc:test_user&uri=at://did:plc:author/app.bsky.feed.post/123")
            .body(Body::empty())
            .unwrap();

        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);

        let body = resp.into_body().collect().await.unwrap().to_bytes();
        let chain: GraphProofChain = serde_json::from_slice(&body).unwrap();
        assert_eq!(chain.steps.len(), 3);
    }

    #[tokio::test]
    async fn test_api_auth_login_endpoints() {
        let state = create_test_state();
        let app = create_xrpc_router(state);

        // 1. Empty body/fields -> 400
        let req_empty = Request::builder()
            .method(Method::POST)
            .uri("/api/auth/login")
            .header(CONTENT_TYPE, "application/json")
            .body(Body::from(r#"{"identifier":"","password":""}"#))
            .unwrap();
        let resp_empty = app.clone().oneshot(req_empty).await.unwrap();
        assert_eq!(resp_empty.status(), StatusCode::BAD_REQUEST);

        // 2. Invalid password -> 401
        let req_invalid = Request::builder()
            .method(Method::POST)
            .uri("/api/auth/login")
            .header(CONTENT_TYPE, "application/json")
            .body(Body::from(
                r#"{"identifier":"alice.bsky.social","password":"invalid-password"}"#,
            ))
            .unwrap();
        let resp_invalid = app.clone().oneshot(req_invalid).await.unwrap();
        assert_eq!(resp_invalid.status(), StatusCode::UNAUTHORIZED);

        // 3. Valid credentials -> 200 with token
        let req_valid = Request::builder()
            .method(Method::POST)
            .uri("/api/auth/login")
            .header(CONTENT_TYPE, "application/json")
            .body(Body::from(
                r#"{"identifier":"alice.bsky.social","password":"valid-app-password"}"#,
            ))
            .unwrap();
        let resp_valid = app.oneshot(req_valid).await.unwrap();
        assert_eq!(resp_valid.status(), StatusCode::OK);
        let body = resp_valid.into_body().collect().await.unwrap().to_bytes();
        let login_res: LoginSuccessResponse = serde_json::from_slice(&body).unwrap();
        assert_eq!(login_res.status, "ok");
        assert_eq!(login_res.handle, "alice.bsky.social");
        assert!(!login_res.token.is_empty());
    }

    #[tokio::test]
    async fn test_api_preferences_crud_lifecycle() {
        let state = create_test_state();
        let app = create_xrpc_router(state.clone());
        let token = crate::auth::generate_session_token_signed(
            "did:plc:prefs_user",
            3600,
            &state.session_secret,
        );

        // 1. Unauthenticated GET -> 401
        let req_unauth = Request::builder()
            .method(Method::GET)
            .uri("/api/preferences")
            .body(Body::empty())
            .unwrap();
        let resp_unauth = app.clone().oneshot(req_unauth).await.unwrap();
        assert_eq!(resp_unauth.status(), StatusCode::UNAUTHORIZED);

        // 2. Authenticated GET (defaults) -> 200 is_custom: false
        let req_get_default = Request::builder()
            .method(Method::GET)
            .uri("/api/preferences")
            .header(AUTHORIZATION, format!("Bearer {token}"))
            .body(Body::empty())
            .unwrap();
        let resp_get_default = app.clone().oneshot(req_get_default).await.unwrap();
        assert_eq!(resp_get_default.status(), StatusCode::OK);
        let body = resp_get_default
            .into_body()
            .collect()
            .await
            .unwrap()
            .to_bytes();
        let prefs_resp: PreferencesResponseDto = serde_json::from_slice(&body).unwrap();
        assert!(!prefs_resp.is_custom);
        assert_eq!(prefs_resp.preferences.freshness_hours, 24.0);

        // 3. Authenticated POST -> 200 saves custom dials
        let save_req = SavePreferencesRequestBody {
            freshness_hours: 12.0,
            discovery_ratio: 0.35,
            topic_weights: Some(TopicWeights {
                art: 2.0,
                tech: 3.0,
                science: 1.0,
                news: 0.5,
                culture: 1.5,
            }),
            include_replies: Some(true),
            min_likes: Some(10),
        };
        let req_post = Request::builder()
            .method(Method::POST)
            .uri("/api/preferences")
            .header(AUTHORIZATION, format!("Bearer {token}"))
            .header(CONTENT_TYPE, "application/json")
            .body(Body::from(serde_json::to_vec(&save_req).unwrap()))
            .unwrap();
        let resp_post = app.clone().oneshot(req_post).await.unwrap();
        assert_eq!(resp_post.status(), StatusCode::OK);

        // 4. Authenticated GET -> 200 returns updated custom dials
        let req_get_updated = Request::builder()
            .method(Method::GET)
            .uri("/api/preferences")
            .header(AUTHORIZATION, format!("Bearer {token}"))
            .body(Body::empty())
            .unwrap();
        let resp_get_updated = app.clone().oneshot(req_get_updated).await.unwrap();
        assert_eq!(resp_get_updated.status(), StatusCode::OK);
        let body_up = resp_get_updated
            .into_body()
            .collect()
            .await
            .unwrap()
            .to_bytes();
        let prefs_up: PreferencesResponseDto = serde_json::from_slice(&body_up).unwrap();
        assert!(prefs_up.is_custom);
        assert_eq!(prefs_up.preferences.freshness_hours, 12.0);
        assert_eq!(prefs_up.preferences.discovery_ratio, 0.35);
        assert!(prefs_up.preferences.include_replies);
        assert_eq!(prefs_up.preferences.min_likes, 10);

        // 5. DELETE -> 200 resets to default dials
        let req_del = Request::builder()
            .method(Method::DELETE)
            .uri("/api/preferences")
            .header(AUTHORIZATION, format!("Bearer {token}"))
            .body(Body::empty())
            .unwrap();
        let resp_del = app.clone().oneshot(req_del).await.unwrap();
        assert_eq!(resp_del.status(), StatusCode::OK);

        // 6. GET after reset -> is_custom is false again
        let req_get_after = Request::builder()
            .method(Method::GET)
            .uri("/api/preferences")
            .header(AUTHORIZATION, format!("Bearer {token}"))
            .body(Body::empty())
            .unwrap();
        let resp_get_after = app.clone().oneshot(req_get_after).await.unwrap();
        assert_eq!(resp_get_after.status(), StatusCode::OK);
        let body_after = resp_get_after
            .into_body()
            .collect()
            .await
            .unwrap()
            .to_bytes();
        let prefs_after: PreferencesResponseDto = serde_json::from_slice(&body_after).unwrap();
        assert!(!prefs_after.is_custom);
    }

    #[tokio::test]
    async fn test_api_preferences_boundary_validation() {
        let state = create_test_state();
        let token = crate::auth::generate_session_token_signed(
            "did:plc:bounds_user",
            3600,
            &state.session_secret,
        );
        let app = create_xrpc_router(state);

        // Freshness too low (<1h) -> 400
        let req_low_freshness = Request::builder()
            .method(Method::POST)
            .uri("/api/preferences")
            .header(AUTHORIZATION, format!("Bearer {token}"))
            .header(CONTENT_TYPE, "application/json")
            .body(Body::from(r#"{"freshness_hours":0.5,"discovery_ratio":0.15,"topic_weights":{"art":1.0,"tech":1.0,"science":1.0,"news":1.0,"culture":1.0}}"#))
            .unwrap();
        let resp_low = app.clone().oneshot(req_low_freshness).await.unwrap();
        assert_eq!(resp_low.status(), StatusCode::BAD_REQUEST);

        // Discovery too high (>0.50) -> 400
        let req_high_disc = Request::builder()
            .method(Method::POST)
            .uri("/api/preferences")
            .header(AUTHORIZATION, format!("Bearer {token}"))
            .header(CONTENT_TYPE, "application/json")
            .body(Body::from(r#"{"freshness_hours":24.0,"discovery_ratio":0.75,"topic_weights":{"art":1.0,"tech":1.0,"science":1.0,"news":1.0,"culture":1.0}}"#))
            .unwrap();
        let resp_disc = app.clone().oneshot(req_high_disc).await.unwrap();
        assert_eq!(resp_disc.status(), StatusCode::BAD_REQUEST);

        // Topic weight too high (>5.0x) -> 400
        let req_high_topic = Request::builder()
            .method(Method::POST)
            .uri("/api/preferences")
            .header(AUTHORIZATION, format!("Bearer {token}"))
            .header(CONTENT_TYPE, "application/json")
            .body(Body::from(r#"{"freshness_hours":24.0,"discovery_ratio":0.15,"topic_weights":{"art":6.0,"tech":1.0,"science":1.0,"news":1.0,"culture":1.0}}"#))
            .unwrap();
        let resp_topic = app.oneshot(req_high_topic).await.unwrap();
        assert_eq!(resp_topic.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn test_get_feed_skeleton_service_jwt_validation_and_fallback() {
        let state = create_test_state();
        let app = create_xrpc_router(state.clone());
        let viewer_did = "did:plc:victim_user_123";

        // Save custom dials for victim
        let dials = UserDials {
            freshness_half_life_secs: 6.0 * 3600.0,
            serendipity_ratio: 0.05,
            topic_weights: TopicWeights {
                art: 5.0,
                tech: 0.0,
                science: 0.0,
                news: 0.0,
                culture: 0.0,
            },
            include_replies: false,
            min_likes: DEFAULT_MIN_LIKES,
            updated_at_secs: 0,
        };
        state
            .preferences_store
            .set_by_did(&state.recommender.interner, viewer_did, dials);

        // 1. Expired Service JWT
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_secs();
        let header = URL_SAFE_NO_PAD.encode(r#"{"alg":"ES256K","typ":"JWT"}"#);
        let expired_payload = URL_SAFE_NO_PAD.encode(format!(
            r#"{{"iss":"{viewer_did}","aud":"did:web:feed.example.com","exp":{}}}"#,
            now - 100
        ));
        let expired_jwt = format!("{header}.{expired_payload}.mock_sig");

        let req_expired = Request::builder()
            .uri("/xrpc/app.bsky.feed.getFeedSkeleton?feed=at://did:web:feed.example.com/app.bsky.feed.generator/for-your-consideration")
            .header(AUTHORIZATION, format!("Bearer {expired_jwt}"))
            .body(Body::empty())
            .unwrap();
        let resp_expired = app.clone().oneshot(req_expired).await.unwrap();
        assert_eq!(resp_expired.status(), StatusCode::OK);

        // 2. Mismatched Audience Service JWT
        let wrong_aud_payload = URL_SAFE_NO_PAD.encode(format!(
            r#"{{"iss":"{viewer_did}","aud":"did:web:competitor-feed.com","exp":{}}}"#,
            now + 3600
        ));
        let wrong_aud_jwt = format!("{header}.{wrong_aud_payload}.mock_sig");

        let req_wrong_aud = Request::builder()
            .uri("/xrpc/app.bsky.feed.getFeedSkeleton?feed=at://did:web:feed.example.com/app.bsky.feed.generator/for-your-consideration")
            .header(AUTHORIZATION, format!("Bearer {wrong_aud_jwt}"))
            .body(Body::empty())
            .unwrap();
        let resp_wrong_aud = app.oneshot(req_wrong_aud).await.unwrap();
        assert_eq!(resp_wrong_aud.status(), StatusCode::OK);
    }

    #[test]
    fn test_app_state_session_secret_sha256_derivation() {
        let recommender = Arc::new(Recommender::new(
            Arc::new(crate::interner::StringInterner::new()),
            Arc::new(crate::graph::GraphStore::new()),
        ));

        // When SESSION_SECRET is set
        std::env::set_var("SESSION_SECRET", "custom-secret-key-12345");
        let state = AppState::new(recommender.clone(), "did:web:test", "test.example.com");
        let expected_hash = Sha256::digest(b"custom-secret-key-12345");
        assert_eq!(state.session_secret, expected_hash.as_ref());

        // When SESSION_SECRET is empty string
        std::env::set_var("SESSION_SECRET", "   ");
        let state_empty = AppState::new(recommender, "did:web:test", "test.example.com");
        assert_eq!(state_empty.session_secret, *DEFAULT_SESSION_SECRET);

        std::env::remove_var("SESSION_SECRET");
    }

    #[tokio::test]
    async fn test_oversized_payload_body_limit_rejected() {
        let state = create_test_state();
        let app = create_xrpc_router(state);
        let token = crate::auth::generate_session_token("did:plc:large_user", 3600);

        // 128 KB payload exceeds the 64 KB DefaultBodyLimit
        let large_payload = format!(
            r#"{{"freshness_hours":24.0,"discovery_ratio":0.15,"topic_weights":{{"art":1.0,"tech":1.0,"science":1.0,"news":1.0,"culture":1.0}},"padding":"{}"}}"#,
            "x".repeat(128 * 1024)
        );

        let req_large = Request::builder()
            .method(Method::POST)
            .uri("/api/preferences")
            .header(AUTHORIZATION, format!("Bearer {token}"))
            .header(CONTENT_TYPE, "application/json")
            .body(Body::from(large_payload))
            .unwrap();
        let resp = app.oneshot(req_large).await.unwrap();
        assert_eq!(resp.status(), StatusCode::PAYLOAD_TOO_LARGE);
    }

    #[test]
    fn test_active_users_tracker_in_flight_lifecycle() {
        let tracker = ActiveUsersTracker::new();
        assert_eq!(tracker.in_flight_count(), 0);

        {
            let _g1 = tracker.track_request(Some("did:plc:alice"), 1000);
            assert_eq!(tracker.in_flight_count(), 1);

            {
                let _g2 = tracker.track_request(Some("did:plc:bob"), 1000);
                assert_eq!(tracker.in_flight_count(), 2);
            }
            assert_eq!(tracker.in_flight_count(), 1);
        }
        assert_eq!(tracker.in_flight_count(), 0);

        // Guard drop does not underflow
        tracker.decrement_in_flight();
        assert_eq!(tracker.in_flight_count(), 0);
    }

    #[test]
    fn test_active_users_tracker_sliding_windows_and_pruning() {
        let tracker = ActiveUsersTracker::new();
        let now = 1_000_000u64;

        // Viewer 1: 30s ago (1m, 5m, 15m)
        tracker.record_activity("did:plc:v1", now - 30);
        // Viewer 2: 2m ago (5m, 15m)
        tracker.record_activity("did:plc:v2", now - 120);
        // Viewer 3: 10m ago (15m)
        tracker.record_activity("did:plc:v3", now - 600);
        // Viewer 4: 20m ago (>15m)
        tracker.record_activity("did:plc:v4", now - 1200);

        let telemetry = tracker.get_telemetry(now);
        assert_eq!(telemetry.in_flight_requests, 0);
        assert_eq!(telemetry.concurrent_viewers_1m, 1);
        assert_eq!(telemetry.concurrent_viewers_5m, 2);
        assert_eq!(telemetry.concurrent_viewers_15m, 3);
        assert_eq!(telemetry.total_active_viewers, 4);

        // Pruning older than 15m (now - 900)
        tracker.prune_older_than(now - 900);
        let after_prune = tracker.get_telemetry(now);
        assert_eq!(after_prune.total_active_viewers, 3);
        assert_eq!(after_prune.concurrent_viewers_15m, 3);
    }

    #[test]
    fn test_active_users_tracker_concurrent_stress() {
        use std::sync::Arc;
        let tracker = Arc::new(ActiveUsersTracker::new());
        let mut handles = Vec::new();

        for thread_id in 0..16 {
            let t = Arc::clone(&tracker);
            handles.push(std::thread::spawn(move || {
                for i in 0..100 {
                    let now = 1_700_000 + i;
                    let did = format!("did:plc:user_{thread_id}_{i}");
                    let _guard = t.track_request(Some(&did), now);
                    let _telemetry = t.get_telemetry(now);
                    if i % 20 == 0 {
                        t.prune_older_than(now.saturating_sub(60));
                    }
                }
            }));
        }

        for h in handles {
            h.join().unwrap();
        }

        assert_eq!(tracker.in_flight_count(), 0);
    }
}
