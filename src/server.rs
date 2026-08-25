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

use std::sync::Arc;
use std::time::Instant;

use axum::extract::{Query, State};
use axum::http::header::{ACCEPT, AUTHORIZATION, CONTENT_TYPE};
use axum::http::{HeaderMap, HeaderValue, Method, StatusCode};
use axum::response::{Html, IntoResponse};
use axum::routing::get;
use axum::{Json, Router};
use compact_str::CompactString;
use serde::Deserialize;
use tokio::net::TcpListener;
use tokio_util::sync::CancellationToken;
use tower_http::cors::CorsLayer;
use tracing::{error, info};

use crate::auth::extract_viewer_did_from_headers;
use crate::error::{FeedError, Result};
use crate::ingest::IngestionTracker;
use crate::recommender::Recommender;
use crate::snapshot::SnapshotStatusTracker;
use crate::types::{
    ApiErrorResponse, ExplainQuery, FeedPreviewQuery, FeedSkeletonResponse, GraphTelemetryInfo,
    ImpressionTelemetryInfo, InternerTelemetryInfo, MemoryTelemetryInfo, RecommendationDials,
    SkeletonFeedPost, TasteTwinsQuery, TelemetryResponse,
};

/// Embedded HTML content for the interactive web dashboard single-page application.
pub const DASHBOARD_HTML: &str = include_str!("assets/dashboard.html");

/// Default feed record key if not overridden by `FEED_RKEY` env var.
pub const DEFAULT_FEED_RKEY: &str = "for-your-consideration";

/// Application state shared across all HTTP request handlers.
#[derive(Clone)]
pub struct AppState {
    /// Reference to the algorithmic recommendation engine.
    pub recommender: Arc<Recommender>,
    /// Tracker for snapshot persistence status and metrics.
    pub snapshot_tracker: Arc<SnapshotStatusTracker>,
    /// Tracker for real-time Jetstream firehose ingestion velocity and statistics.
    pub ingestion_tracker: Arc<IngestionTracker>,
    /// AT Protocol service DID (e.g. `did:web:feed.example.com`).
    pub service_did: CompactString,
    /// Fully-qualified hostname serving the feed generator (e.g. `feed.example.com`).
    pub hostname: CompactString,
    /// Feed record key identifier (e.g. `for-your-consideration`).
    pub feed_rkey: CompactString,
    /// Server initialization instant for uptime tracking.
    pub start_time: Instant,
}

impl AppState {
    /// Creates a new [`AppState`] instance with default snapshot and ingestion trackers.
    #[must_use]
    pub fn new(
        recommender: Arc<Recommender>,
        service_did: impl Into<CompactString>,
        hostname: impl Into<CompactString>,
    ) -> Self {
        Self {
            recommender,
            snapshot_tracker: Arc::new(SnapshotStatusTracker::default()),
            ingestion_tracker: Arc::new(IngestionTracker::default()),
            service_did: service_did.into(),
            hostname: hostname.into(),
            feed_rkey: CompactString::new(DEFAULT_FEED_RKEY),
            start_time: Instant::now(),
        }
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
    /// Whether to generate structured explanation traces (optional bool).
    pub explain: Option<bool>,
}

/// Builds the production Axum HTTP router with all endpoints and middleware.
pub fn create_xrpc_router(state: AppState) -> Router {
    let cors = CorsLayer::new()
        .allow_methods([Method::GET, Method::HEAD, Method::OPTIONS])
        .allow_origin(tower_http::cors::Any)
        .allow_headers([AUTHORIZATION, CONTENT_TYPE, ACCEPT]);

    Router::new()
        .route("/", get(handle_get_dashboard))
        .route("/dashboard", get(handle_get_dashboard))
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
        .layer(cors)
        .with_state(state)
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

    let viewer_did = extract_viewer_did_from_headers(&headers);

    let dials = RecommendationDials::from_query(
        query.freshness.as_deref(),
        query.discovery.as_deref(),
        query.explain,
        query.limit,
        query.cursor,
    );

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

    let response = TelemetryResponse {
        status: "ok".to_string(),
        uptime_seconds: uptime,
        graph: graph_info,
        interner: interner_info,
        memory: memory_info,
        ingestion: ingestion_info,
        snapshot: snapshot_info,
        impression_store: impression_info,
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
    let viewer_opt = query.viewer_identifier();
    let dials = query.to_dials();

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
mod tests {
    use std::sync::Arc;

    use axum::body::Body;
    use axum::http::{Request, StatusCode};
    use http_body_util::BodyExt;
    use tower::ServiceExt;

    use super::*;
    use crate::graph::GraphStore;
    use crate::interner::StringInterner;
    use crate::types::{FeedPreviewResponse, GraphProofChain, SignalType, TasteTwinsResponse};

    fn create_test_state() -> AppState {
        let interner = Arc::new(StringInterner::new());
        let graph = Arc::new(GraphStore::new());

        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_secs();

        let uid = interner.intern("did:plc:test_user");
        let pid = interner.intern("at://did:plc:author/app.bsky.feed.post/123");
        let aid = interner.intern("did:plc:author");
        graph.record_post_meta(pid, aid, None, None, now);
        graph.record_interaction(uid, pid, SignalType::Like, now);

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
}
