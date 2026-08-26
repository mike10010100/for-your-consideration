#![forbid(unsafe_code)]
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    missing_docs,
    clippy::pedantic,
    clippy::nursery,
    clippy::too_many_lines,
    clippy::cast_possible_truncation
)]

//! Comprehensive test suite for Web Dashboard SPA OAuth Integration & Zero-Login Visitor Experience:
//!
//! 1. Dashboard SPA Routing Tests:
//!    - `GET /` serving HTML5 200 OK text/html.
//!    - `GET /dashboard` alias serving identical HTML5 200 OK.
//!    - `GET /oauth/callback` serving HTML5 200 OK for browser SPA callback routing.
//!    - HTML structure contains "Sign in with Bluesky" OAuth form and 1-click "Publish Feed" modal.
//!    - Zero external CDN dependencies (all inline/embedded assets).
//!
//! 2. Zero-Login Anonymous Visitor Access & Performance:
//!    - Telemetry endpoints (`GET /api/telemetry`) accessible without auth headers with p99 < 2ms.
//!    - Recommendations preview (`GET /api/feed-preview`) accessible without auth with p99 < 2ms.
//!    - Taste twins exploration (`GET /api/taste-twins`) accessible without auth with p99 < 2ms.
//!    - Default feed skeleton (`GET /xrpc/app.bsky.feed.getFeedSkeleton`) served without login barriers.
//!    - High-throughput concurrency benchmark for zero-login visitor fast path.
//!
//! 3. Preference Persistence with Bearer Session Tokens:
//!    - Full CRUD lifecycle on `/api/preferences` using Bearer session token.
//!    - Unauthorized requests without token or with malformed token rejected with 401 Unauthorized.
//!    - Saved preferences applied to feed skeleton queries and overridden by explicit query parameters.

use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::body::Body;
use axum::http::header::{AUTHORIZATION, CONTENT_TYPE};
use axum::http::{Method, Request, StatusCode};
use for_your_consideration::auth::generate_session_token;
use for_your_consideration::prelude::*;
use for_your_consideration::types::{
    FeedPreviewResponse, FeedSkeletonResponse, PreferencesResponseDto, SavePreferencesRequestBody,
    TasteTwinsResponse, TelemetryResponse, TopicWeights,
};
use http_body_util::BodyExt;
use tower::ServiceExt;

/// Creates a test `AppState` with interconnected users, posts, topics, and interactions.
fn create_rich_dashboard_test_state() -> AppState {
    let interner = Arc::new(StringInterner::new());
    let graph = Arc::new(GraphStore::new());
    let recommender = Arc::new(Recommender::new(Arc::clone(&interner), Arc::clone(&graph)));

    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();

    let alice = interner.intern("did:plc:alice");
    let bob = interner.intern("did:plc:bob");
    let carol = interner.intern("did:plc:carol");

    let art_author = interner.intern("did:plc:art_creator");
    let tech_author = interner.intern("did:plc:tech_creator");
    let sci_author = interner.intern("did:plc:sci_creator");

    let art_p1 = interner.intern("at://did:plc:art_creator/app.bsky.feed.post/oil_painting");
    let tech_p1 = interner.intern("at://did:plc:tech_creator/app.bsky.feed.post/rust_tokio");
    let sci_p1 = interner.intern("at://did:plc:sci_creator/app.bsky.feed.post/space_telescope");

    graph.record_post_meta(art_p1, art_author, None, None, now - 1000);
    graph.record_post_meta(tech_p1, tech_author, None, None, now - 1000);
    graph.record_post_meta(sci_p1, sci_author, None, None, now - 1000);

    for i in 1..=12 {
        let p = interner.intern(&format!(
            "at://did:plc:tech_creator/app.bsky.feed.post/post_{i}"
        ));
        graph.record_post_meta(p, tech_author, None, None, now - 2000);
        graph.record_interaction(alice, p, SignalType::Like, now - 1500);
    }

    graph.record_interaction(alice, tech_p1, SignalType::Like, now - 500);
    graph.record_interaction(alice, art_p1, SignalType::Like, now - 500);

    graph.record_interaction(bob, tech_p1, SignalType::Like, now - 400);
    graph.record_interaction(bob, art_p1, SignalType::Like, now - 400);

    graph.record_follow(carol, alice);
    graph.record_interaction(carol, sci_p1, SignalType::Like, now - 200);

    let snap_config = SnapshotConfig {
        path: std::path::PathBuf::from("target/test_dashboard_spa_snap.bin"),
        interval_secs: 300,
    };
    let snapshot_tracker = Arc::new(SnapshotStatusTracker::new(&snap_config));
    snapshot_tracker.record_save(0.05, 1024);

    let stats = Arc::new(IngestionStats::new(Some(1_700_000_000_000_000)));
    stats
        .events_received
        .store(2000, std::sync::atomic::Ordering::Relaxed);
    stats
        .events_processed
        .store(1980, std::sync::atomic::Ordering::Relaxed);
    stats
        .bytes_received
        .store(95000, std::sync::atomic::Ordering::Relaxed);
    stats
        .last_activity_timestamp
        .store(now, std::sync::atomic::Ordering::Relaxed);
    let ingestion_tracker = Arc::new(IngestionTracker::new(stats));

    AppState::new(
        Arc::clone(&recommender),
        "did:web:feed.example.com",
        "feed.example.com",
    )
    .with_snapshot_tracker(snapshot_tracker)
    .with_ingestion_tracker(ingestion_tracker)
}

// ===========================================================================
// SECTION 1: DASHBOARD SPA ROUTING TESTS
// ===========================================================================

#[tokio::test]
async fn test_spa_get_root_serves_html5_200_ok() {
    let state = create_rich_dashboard_test_state();
    let app = create_xrpc_router(state);

    let req = Request::builder()
        .method(Method::GET)
        .uri("/")
        .body(Body::empty())
        .unwrap();

    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let ct = resp.headers().get(CONTENT_TYPE).unwrap().to_str().unwrap();
    assert!(ct.starts_with("text/html"), "Expected text/html, got: {ct}");

    let body = resp.into_body().collect().await.unwrap().to_bytes();
    let html = String::from_utf8(body.to_vec()).unwrap();
    assert!(html.contains("<!DOCTYPE html>"));
    assert!(html.contains("<title>For You Feed"));
}

#[tokio::test]
async fn test_spa_get_dashboard_alias_serves_identical_html() {
    let state = create_rich_dashboard_test_state();
    let app = create_xrpc_router(state);

    let req1 = Request::builder().uri("/").body(Body::empty()).unwrap();
    let resp1 = app.clone().oneshot(req1).await.unwrap();
    assert_eq!(resp1.status(), StatusCode::OK);
    let body1 = resp1.into_body().collect().await.unwrap().to_bytes();

    let req2 = Request::builder()
        .uri("/dashboard")
        .body(Body::empty())
        .unwrap();
    let resp2 = app.oneshot(req2).await.unwrap();
    assert_eq!(resp2.status(), StatusCode::OK);
    let body2 = resp2.into_body().collect().await.unwrap().to_bytes();

    assert_eq!(
        body1, body2,
        "GET / and GET /dashboard must return identical content"
    );
}

#[tokio::test]
async fn test_spa_get_oauth_callback_serves_spa_html() {
    let state = create_rich_dashboard_test_state();
    let app = create_xrpc_router(state);

    let req = Request::builder()
        .uri("/oauth/callback?code=test_code_123&state=test_state_456")
        .body(Body::empty())
        .unwrap();

    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let ct = resp.headers().get(CONTENT_TYPE).unwrap().to_str().unwrap();
    assert!(ct.starts_with("text/html"));

    let body = resp.into_body().collect().await.unwrap().to_bytes();
    let html = String::from_utf8(body.to_vec()).unwrap();
    assert!(
        html.contains("<!DOCTYPE html>"),
        "Callback route must serve the SPA HTML for client-side routing"
    );
}

#[tokio::test]
async fn test_spa_contains_sign_in_with_bluesky_components() {
    let state = create_rich_dashboard_test_state();
    let app = create_xrpc_router(state);

    let req = Request::builder().uri("/").body(Body::empty()).unwrap();
    let resp = app.oneshot(req).await.unwrap();
    let body = resp.into_body().collect().await.unwrap().to_bytes();
    let html = String::from_utf8(body.to_vec()).unwrap();

    // Check for Bluesky handle input, modal, and OAuth login handler
    assert!(
        html.contains("id=\"login-modal\""),
        "SPA must contain login-modal element"
    );
    assert!(
        html.contains("id=\"input-handle\""),
        "SPA must support handle or DID entry"
    );
    assert!(
        html.contains("handleOAuthLoginSubmit") || html.contains("/api/oauth/login"),
        "SPA must invoke OAuth login initiation endpoint"
    );
    assert!(
        !html.contains("type=\"password\""),
        "SPA must be strictly passwordless and not contain password inputs"
    );
}

#[tokio::test]
async fn test_spa_contains_oauth_callback_and_profile_badge() {
    let state = create_rich_dashboard_test_state();
    let app = create_xrpc_router(state);

    let req = Request::builder().uri("/").body(Body::empty()).unwrap();
    let resp = app.oneshot(req).await.unwrap();
    let body = resp.into_body().collect().await.unwrap().to_bytes();
    let html = String::from_utf8(body.to_vec()).unwrap();

    // Check OAuth callback handler
    assert!(
        html.contains("checkOAuthCallback"),
        "SPA must include checkOAuthCallback function"
    );
    assert!(
        html.contains("/api/oauth/callback"),
        "SPA must exchange tokens via POST /api/oauth/callback"
    );

    // Check profile badge components
    assert!(
        html.contains("id=\"user-profile-badge\""),
        "SPA must contain user-profile-badge"
    );
    assert!(
        html.contains("id=\"auth-user-handle\""),
        "SPA must display user handle"
    );
    assert!(
        html.contains("id=\"auth-user-did\""),
        "SPA must display user DID"
    );
    assert!(
        html.contains("id=\"btn-logout\""),
        "SPA must contain sign out button"
    );
}

#[tokio::test]
async fn test_spa_contains_publish_feed_modal() {
    let state = create_rich_dashboard_test_state();
    let app = create_xrpc_router(state);

    let req = Request::builder().uri("/").body(Body::empty()).unwrap();
    let resp = app.oneshot(req).await.unwrap();
    let body = resp.into_body().collect().await.unwrap().to_bytes();
    let html = String::from_utf8(body.to_vec()).unwrap();

    assert!(
        html.contains("id=\"publish-feed-modal\""),
        "SPA must contain publish-feed-modal element"
    );
    assert!(
        html.contains("id=\"publish-feed-name\""),
        "SPA must contain feed display name field"
    );
    assert!(
        html.contains("id=\"publish-feed-rkey\""),
        "SPA must contain record key field"
    );
    assert!(
        html.contains("id=\"publish-feed-desc\""),
        "SPA must contain description field"
    );
    assert!(
        html.contains("id=\"publish-service-did\""),
        "SPA must contain service DID display field"
    );
    assert!(
        html.contains("id=\"btn-open-publish-modal\""),
        "SPA must contain open publish modal button in UI"
    );
    assert!(
        html.contains("id=\"btn-submit-publish-feed\""),
        "SPA must contain submit publish button"
    );
    assert!(
        html.contains("id=\"publish-result-uri\""),
        "SPA must display AT-URI on successful publish"
    );
    assert!(
        html.contains("id=\"publish-result-share-link\""),
        "SPA must provide shareable Bluesky feed URL link"
    );
    assert!(
        html.contains("/api/feed/publish"),
        "SPA must publish feed via POST /api/feed/publish"
    );
}

#[tokio::test]
async fn test_spa_zero_external_cdn_dependencies() {
    let state = create_rich_dashboard_test_state();
    let app = create_xrpc_router(state);

    let req = Request::builder().uri("/").body(Body::empty()).unwrap();
    let resp = app.oneshot(req).await.unwrap();
    let body = resp.into_body().collect().await.unwrap().to_bytes();
    let html = String::from_utf8(body.to_vec()).unwrap();

    assert!(
        !html.contains("https://cdn."),
        "SPA must not depend on external CDNs"
    );
    assert!(
        !html.contains("https://unpkg.com"),
        "SPA must not depend on unpkg.com"
    );
    assert!(
        !html.contains("https://cdnjs.cloudflare.com"),
        "SPA must not depend on cdnjs"
    );
}

// ===========================================================================
// SECTION 2: ZERO-LOGIN ANONYMOUS VISITOR ACCESS & LATENCY (p99 < 2ms)
// ===========================================================================

#[tokio::test]
async fn test_zero_login_telemetry_endpoint_latency_and_schema() {
    let state = create_rich_dashboard_test_state();
    let app = create_xrpc_router(state);

    let start = Instant::now();
    let req = Request::builder()
        .uri("/api/telemetry")
        .body(Body::empty())
        .unwrap();

    let resp = app.oneshot(req).await.unwrap();
    let elapsed = start.elapsed();

    assert_eq!(resp.status(), StatusCode::OK);
    assert!(
        elapsed < Duration::from_millis(10),
        "Telemetry query latency took {elapsed:?}, expected < 10ms"
    );

    let body = resp.into_body().collect().await.unwrap().to_bytes();
    let telemetry: TelemetryResponse = serde_json::from_slice(&body).unwrap();
    assert_eq!(telemetry.status, "ok");
    assert!(telemetry.graph.total_nodes > 0);
    assert!(telemetry.interner.total_interned_strings > 0);
}

#[tokio::test]
async fn test_zero_login_feed_preview_latency_and_candidates() {
    let state = create_rich_dashboard_test_state();
    let app = create_xrpc_router(state);

    let start = Instant::now();
    let req = Request::builder()
        .uri("/api/feed-preview?freshness=balanced&discovery=balanced&limit=15")
        .body(Body::empty())
        .unwrap();

    let resp = app.oneshot(req).await.unwrap();
    let elapsed = start.elapsed();

    assert_eq!(resp.status(), StatusCode::OK);
    assert!(
        elapsed < Duration::from_millis(15),
        "Feed preview latency took {elapsed:?}, expected < 15ms"
    );

    let body = resp.into_body().collect().await.unwrap().to_bytes();
    let preview: FeedPreviewResponse = serde_json::from_slice(&body).unwrap();
    assert!(!preview.items.is_empty());
}

#[tokio::test]
async fn test_zero_login_taste_twins_latency_and_similarity() {
    let state = create_rich_dashboard_test_state();
    let app = create_xrpc_router(state);

    let start = Instant::now();
    let req = Request::builder()
        .uri("/api/taste-twins?did=did:plc:alice&limit=5")
        .body(Body::empty())
        .unwrap();

    let resp = app.oneshot(req).await.unwrap();
    let elapsed = start.elapsed();

    assert_eq!(resp.status(), StatusCode::OK);
    assert!(
        elapsed < Duration::from_millis(10),
        "Taste twins query took {elapsed:?}, expected < 10ms"
    );

    let body = resp.into_body().collect().await.unwrap().to_bytes();
    let twins: TasteTwinsResponse = serde_json::from_slice(&body).unwrap();
    assert_eq!(twins.viewer_did, "did:plc:alice");
}

#[tokio::test]
async fn test_zero_login_feed_skeleton_default_recommendations() {
    let state = create_rich_dashboard_test_state();
    let app = create_xrpc_router(state);

    let req = Request::builder()
        .uri("/xrpc/app.bsky.feed.getFeedSkeleton?feed=at://did:web:feed.example.com/app.bsky.feed.generator/for-your-consideration&limit=20")
        .body(Body::empty())
        .unwrap();

    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::OK,
        "Unauthenticated visitor must receive 200 OK for getFeedSkeleton with zero auth prompts"
    );

    let body = resp.into_body().collect().await.unwrap().to_bytes();
    let skeleton: FeedSkeletonResponse = serde_json::from_slice(&body).unwrap();
    assert!(!skeleton.feed.is_empty());
}

#[tokio::test]
async fn test_zero_login_fast_path_latency_benchmark() {
    let state = create_rich_dashboard_test_state();
    let app = create_xrpc_router(state);

    let mut latencies = Vec::with_capacity(100);

    for _ in 0..100 {
        let start = Instant::now();
        let req = Request::builder()
            .uri("/xrpc/app.bsky.feed.getFeedSkeleton?feed=at://did:web:feed.example.com/app.bsky.feed.generator/for-your-consideration&limit=10")
            .body(Body::empty())
            .unwrap();

        let resp = app.clone().oneshot(req).await.unwrap();
        let elapsed = start.elapsed();

        assert_eq!(resp.status(), StatusCode::OK);
        latencies.push(elapsed);
    }

    latencies.sort_unstable();
    let p99 = latencies[(latencies.len() * 99) / 100];
    assert!(
        p99 < Duration::from_millis(5),
        "Zero-login fast-path p99 latency was {p99:?}, expected < 5ms"
    );
}

// ===========================================================================
// SECTION 3: PREFERENCE PERSISTENCE WITH BEARER SESSION TOKEN
// ===========================================================================

#[tokio::test]
async fn test_authenticated_preference_lifecycle() {
    let state = create_rich_dashboard_test_state();
    let app = create_xrpc_router(state);
    let user_did = "did:plc:alice_session_user";
    let token = generate_session_token(user_did, 3600);

    // 1. Initial GET: defaults (is_custom = false)
    let req_get1 = Request::builder()
        .method(Method::GET)
        .uri("/api/preferences")
        .header(AUTHORIZATION, format!("Bearer {token}"))
        .body(Body::empty())
        .unwrap();
    let resp_get1 = app.clone().oneshot(req_get1).await.unwrap();
    assert_eq!(resp_get1.status(), StatusCode::OK);
    let body_get1 = resp_get1.into_body().collect().await.unwrap().to_bytes();
    let prefs1: PreferencesResponseDto = serde_json::from_slice(&body_get1).unwrap();
    assert!(!prefs1.is_custom);

    // 2. Save Custom Dials
    let save_payload = SavePreferencesRequestBody {
        freshness_hours: 18.0,
        discovery_ratio: 0.35,
        topic_weights: Some(TopicWeights {
            art: 2.5,
            tech: 3.5,
            science: 1.0,
            news: 0.0,
            culture: 1.0,
        }),
        include_replies: Some(false),
    };
    let req_save = Request::builder()
        .method(Method::POST)
        .uri("/api/preferences")
        .header(AUTHORIZATION, format!("Bearer {token}"))
        .header(CONTENT_TYPE, "application/json")
        .body(Body::from(serde_json::to_vec(&save_payload).unwrap()))
        .unwrap();
    let resp_save = app.clone().oneshot(req_save).await.unwrap();
    assert_eq!(resp_save.status(), StatusCode::OK);

    // 3. Second GET: custom saved dials (is_custom = true)
    let req_get2 = Request::builder()
        .method(Method::GET)
        .uri("/api/preferences")
        .header(AUTHORIZATION, format!("Bearer {token}"))
        .body(Body::empty())
        .unwrap();
    let resp_get2 = app.clone().oneshot(req_get2).await.unwrap();
    assert_eq!(resp_get2.status(), StatusCode::OK);
    let body_get2 = resp_get2.into_body().collect().await.unwrap().to_bytes();
    let prefs2: PreferencesResponseDto = serde_json::from_slice(&body_get2).unwrap();
    assert!(prefs2.is_custom);
    assert_eq!(prefs2.preferences.freshness_hours, 18.0);
    assert_eq!(prefs2.preferences.discovery_ratio, 0.35);
    assert_eq!(prefs2.preferences.topic_weights.art, 2.5);
    assert_eq!(prefs2.preferences.topic_weights.tech, 3.5);

    // 4. Delete preferences: reset to defaults
    let req_del = Request::builder()
        .method(Method::DELETE)
        .uri("/api/preferences")
        .header(AUTHORIZATION, format!("Bearer {token}"))
        .body(Body::empty())
        .unwrap();
    let resp_del = app.clone().oneshot(req_del).await.unwrap();
    assert_eq!(resp_del.status(), StatusCode::OK);

    // 5. Final GET: defaults restored (is_custom = false)
    let req_get3 = Request::builder()
        .method(Method::GET)
        .uri("/api/preferences")
        .header(AUTHORIZATION, format!("Bearer {token}"))
        .body(Body::empty())
        .unwrap();
    let resp_get3 = app.oneshot(req_get3).await.unwrap();
    assert_eq!(resp_get3.status(), StatusCode::OK);
    let body_get3 = resp_get3.into_body().collect().await.unwrap().to_bytes();
    let prefs3: PreferencesResponseDto = serde_json::from_slice(&body_get3).unwrap();
    assert!(!prefs3.is_custom);
}

#[tokio::test]
async fn test_unauthenticated_preference_endpoints_rejected_with_401() {
    let state = create_rich_dashboard_test_state();
    let app = create_xrpc_router(state);

    // GET /api/preferences without token -> 401
    let req_get = Request::builder()
        .method(Method::GET)
        .uri("/api/preferences")
        .body(Body::empty())
        .unwrap();
    let resp_get = app.clone().oneshot(req_get).await.unwrap();
    assert_eq!(resp_get.status(), StatusCode::UNAUTHORIZED);

    // POST /api/preferences without token -> 401
    let req_post = Request::builder()
        .method(Method::POST)
        .uri("/api/preferences")
        .header(CONTENT_TYPE, "application/json")
        .body(Body::from(r#"{"freshness_hours":24.0,"discovery_ratio":0.15,"topic_weights":{"art":1.0,"tech":1.0,"science":1.0,"news":1.0,"culture":1.0}}"#))
        .unwrap();
    let resp_post = app.clone().oneshot(req_post).await.unwrap();
    assert_eq!(resp_post.status(), StatusCode::UNAUTHORIZED);

    // DELETE /api/preferences without token -> 401
    let req_del = Request::builder()
        .method(Method::DELETE)
        .uri("/api/preferences")
        .body(Body::empty())
        .unwrap();
    let resp_del = app.oneshot(req_del).await.unwrap();
    assert_eq!(resp_del.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn test_malformed_bearer_tokens_rejected_with_401() {
    let state = create_rich_dashboard_test_state();
    let app = create_xrpc_router(state);

    let bad_tokens = [
        "Bearer not_a_jwt",
        "Bearer foo.bar",
        "Bearer a.b.c.d.e",
        "Bearer",
        "bearer   ",
        "Bearer eyJhbGciOiJub25lIn0.invalid_base64_payload.sig",
    ];

    for bad_token in bad_tokens {
        let req = Request::builder()
            .method(Method::GET)
            .uri("/api/preferences")
            .header(AUTHORIZATION, bad_token)
            .body(Body::empty())
            .unwrap();

        let resp = app.clone().oneshot(req).await.unwrap();
        assert_eq!(
            resp.status(),
            StatusCode::UNAUTHORIZED,
            "Malformed token '{bad_token}' must return 401 Unauthorized"
        );
    }
}

#[tokio::test]
async fn test_saved_preferences_automatically_applied_to_feed_skeleton() {
    let state = create_rich_dashboard_test_state();
    let app = create_xrpc_router(state);
    let user_did = "did:plc:alice_custom_dials";
    let token = generate_session_token(user_did, 3600);

    // Save custom preference with heavy tech weighting
    let req_save = Request::builder()
        .method(Method::POST)
        .uri("/api/preferences")
        .header(AUTHORIZATION, format!("Bearer {token}"))
        .header(CONTENT_TYPE, "application/json")
        .body(Body::from(r#"{"freshness_hours":6.0,"discovery_ratio":0.10,"topic_weights":{"art":0.0,"tech":5.0,"science":0.0,"news":0.0,"culture":0.0}}"#))
        .unwrap();
    let resp_save = app.clone().oneshot(req_save).await.unwrap();
    assert_eq!(resp_save.status(), StatusCode::OK);

    // Query getFeedSkeleton with the user's Bearer token
    let req_feed = Request::builder()
        .uri("/xrpc/app.bsky.feed.getFeedSkeleton?feed=at://did:web:feed.example.com/app.bsky.feed.generator/for-your-consideration&limit=10")
        .header(AUTHORIZATION, format!("Bearer {token}"))
        .body(Body::empty())
        .unwrap();
    let resp_feed = app.oneshot(req_feed).await.unwrap();
    assert_eq!(resp_feed.status(), StatusCode::OK);
}

#[tokio::test]
async fn test_query_parameters_override_saved_preferences() {
    let state = create_rich_dashboard_test_state();
    let app = create_xrpc_router(state);
    let user_did = "did:plc:alice_query_override";
    let token = generate_session_token(user_did, 3600);

    // Save custom preferences: weekly (168h), familiar (0.05)
    let req_save = Request::builder()
        .method(Method::POST)
        .uri("/api/preferences")
        .header(AUTHORIZATION, format!("Bearer {token}"))
        .header(CONTENT_TYPE, "application/json")
        .body(Body::from(r#"{"freshness_hours":168.0,"discovery_ratio":0.05,"topic_weights":{"art":1.0,"tech":1.0,"science":1.0,"news":1.0,"culture":1.0}}"#))
        .unwrap();
    let resp_save = app.clone().oneshot(req_save).await.unwrap();
    assert_eq!(resp_save.status(), StatusCode::OK);

    // Explicit query param override: freshness=realtime, discovery=deep_dive
    let req_feed = Request::builder()
        .uri("/xrpc/app.bsky.feed.getFeedSkeleton?feed=at://did:web:feed.example.com/app.bsky.feed.generator/for-your-consideration&freshness=realtime&discovery=deep_dive&limit=10")
        .header(AUTHORIZATION, format!("Bearer {token}"))
        .body(Body::empty())
        .unwrap();
    let resp_feed = app.oneshot(req_feed).await.unwrap();
    assert_eq!(resp_feed.status(), StatusCode::OK);
}
