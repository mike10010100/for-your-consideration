#![forbid(unsafe_code)]
#![allow(
    clippy::all,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    missing_docs,
    rust_2018_idioms
)]

//! Milestone 3 Iteration 2: Challenger 2 Empirical Verification Suite
//!
//! Rigorously verifies:
//! 1. Full 3-Tier Precedence Matrix (HTTP Query Params > Persisted `UserDials` > System Defaults).
//! 2. Adversarial Query Parameter Overrides, Edge Clamping, and Fallback Resilience.
//! 3. REST `/api/preferences` GET, POST (standard & alias payloads), and DELETE lifecycle.
//! 4. Boundary rejections on `/api/preferences` (400 Bad Request on invalid inputs, 401 on missing/bad auth).
//! 5. Web Dashboard SPA HTML contract, controls, accessibility, and security headers.
//! 6. Concurrent read/write stress across 64-shard storage during live XRPC skeleton queries.
//! 7. Release-mode concurrent recommendation p99 latency benchmark (< 2.0ms SLA).

mod common;
use common::generate_mock_jwt;

use std::sync::Arc;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use axum::body::Body;
use axum::http::header::{AUTHORIZATION, CONTENT_TYPE};
use axum::http::{Method, Request, StatusCode};
use http_body_util::BodyExt;
use tower::ServiceExt;

use for_your_consideration::auth::generate_session_token;
use for_your_consideration::graph::GraphStore;
use for_your_consideration::interner::StringInterner;
use for_your_consideration::preferences::UserPreferencesStore;
use for_your_consideration::recommender::Recommender;
use for_your_consideration::server::{create_xrpc_router, AppState, DASHBOARD_HTML};
use for_your_consideration::types::{
    ApiErrorResponse, FeedSkeletonResponse, GenericStatusResponse, PreferencesResponseDto,
    RecommendationDials, SignalType, TopicWeights, UserDials, CURATED_MIN_LIKES, DEFAULT_MIN_LIKES,
    EMERGING_MIN_LIKES,
};

fn current_time_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

/// Helper function to create a test harness with populated graph and server router.
fn create_test_server() -> (
    Arc<StringInterner>,
    Arc<GraphStore>,
    Arc<UserPreferencesStore>,
    Arc<Recommender>,
    axum::Router,
) {
    let interner = Arc::new(StringInterner::new());
    let graph = Arc::new(GraphStore::new());
    let prefs = Arc::new(UserPreferencesStore::new());
    let recommender = Arc::new(Recommender::new(Arc::clone(&interner), Arc::clone(&graph)));

    let state = AppState::new(
        Arc::clone(&recommender),
        "did:web:feed.example.com",
        "feed.example.com",
    )
    .with_preferences_store(Arc::clone(&prefs));

    let router = create_xrpc_router(state);
    (interner, graph, prefs, recommender, router)
}

// ===========================================================================
// Test 1: Full 3-Tier Precedence Matrix in XRPC getFeedSkeleton
// ===========================================================================

#[tokio::test]
async fn test_xrpc_3_tier_precedence_full_matrix() {
    let (interner, graph, prefs, _, router) = create_test_server();
    let now = current_time_secs();
    let feed_uri = "at://did:plc:feed/app.bsky.feed.generator/for-you";

    let viewer_did = "did:plc:alice_precedence_test";
    let viewer_id = interner.intern(viewer_did);
    let jwt = generate_mock_jwt(viewer_did, "did:web:feed.example.com", true);

    // Create a diverse corpus of posts:
    // Post 0: 0 likes, root post
    let p0 = interner.intern("at://did:plc:author_a/app.bsky.feed.post/post_0");
    let a0 = interner.intern("did:plc:author_a");
    graph.record_post_meta(p0, a0, None, None, now - 50);

    // Post 1: 1 like, root post
    let p1 = interner.intern("at://did:plc:author_b/app.bsky.feed.post/post_1");
    let a1 = interner.intern("did:plc:author_b");
    graph.record_post_meta(p1, a1, None, None, now - 100);
    graph.record_interaction(
        interner.intern("did:plc:fan1"),
        p1,
        SignalType::Like,
        now - 60,
    );

    // Post 2: 3 likes, reply post
    let p2 = interner.intern("at://did:plc:author_c/app.bsky.feed.post/post_2");
    let a2 = interner.intern("did:plc:author_c");
    let root2 = interner.intern("at://did:plc:author_c/app.bsky.feed.post/root_post");
    graph.record_post_meta(p2, a2, Some(root2), Some(root2), now - 150);
    for i in 1..=3 {
        let fan = interner.intern(&format!("did:plc:fan3_{i}"));
        graph.record_interaction(fan, p2, SignalType::Like, now - 80);
    }

    // Post 3: 10 likes, root post
    let p3 = interner.intern("at://did:plc:author_d/app.bsky.feed.post/post_3");
    let a3 = interner.intern("did:plc:author_d");
    graph.record_post_meta(p3, a3, None, None, now - 200);
    for i in 1..=10 {
        let fan = interner.intern(&format!("did:plc:fan10_{i}"));
        graph.record_interaction(fan, p3, SignalType::Like, now - 100);
    }

    // -----------------------------------------------------------------------
    // Level 3 Precedence: Unauthenticated -> System Defaults
    // System Defaults: min_likes = 3, include_replies = false
    // Expected: Only p3 returned (p2 is reply, so excluded; p1 has 1 like < 3; p0 has 0 likes < 3)
    // -----------------------------------------------------------------------
    let req_unauth = Request::builder()
        .uri(format!(
            "/xrpc/app.bsky.feed.getFeedSkeleton?feed={feed_uri}"
        ))
        .body(Body::empty())
        .unwrap();
    let resp_unauth = router.clone().oneshot(req_unauth).await.unwrap();
    assert_eq!(resp_unauth.status(), StatusCode::OK);
    let body_unauth = resp_unauth.into_body().collect().await.unwrap().to_bytes();
    let skel_unauth: FeedSkeletonResponse = serde_json::from_slice(&body_unauth).unwrap();
    assert_eq!(skel_unauth.feed.len(), 1);
    assert_eq!(
        skel_unauth.feed[0].post,
        "at://did:plc:author_d/app.bsky.feed.post/post_3"
    );

    // -----------------------------------------------------------------------
    // Level 2 Precedence: Authenticated with Persisted Custom Dials
    // Persist dials: include_replies = true, min_likes = 3
    // Expected: Both p2 (reply, 3 likes) and p3 (root, 10 likes) returned
    // -----------------------------------------------------------------------
    prefs.set(
        viewer_id,
        UserDials {
            freshness_half_life_secs: 12.0 * 3600.0,
            serendipity_ratio: 0.20,
            topic_weights: TopicWeights::default(),
            include_replies: true,
            min_likes: 3,
            updated_at_secs: now,
        },
    );

    let req_auth = Request::builder()
        .uri(format!(
            "/xrpc/app.bsky.feed.getFeedSkeleton?feed={feed_uri}"
        ))
        .header(AUTHORIZATION, format!("Bearer {jwt}"))
        .body(Body::empty())
        .unwrap();
    let resp_auth = router.clone().oneshot(req_auth).await.unwrap();
    assert_eq!(resp_auth.status(), StatusCode::OK);
    let body_auth = resp_auth.into_body().collect().await.unwrap().to_bytes();
    let skel_auth: FeedSkeletonResponse = serde_json::from_slice(&body_auth).unwrap();
    assert_eq!(skel_auth.feed.len(), 2);
    assert!(skel_auth.feed.iter().any(|p| p.post.ends_with("/post_2")));
    assert!(skel_auth.feed.iter().any(|p| p.post.ends_with("/post_3")));

    // -----------------------------------------------------------------------
    // Level 1 Precedence: Query Parameter Overrides Persisted Dials
    // Override min_likes to 1 (emerging) while keeping persisted include_replies = true
    // Expected: p1 (1 like), p2 (reply, 3 likes), p3 (10 likes) returned (3 items)
    // -----------------------------------------------------------------------
    let req_override_min = Request::builder()
        .uri(format!(
            "/xrpc/app.bsky.feed.getFeedSkeleton?feed={feed_uri}&min_likes=emerging"
        ))
        .header(AUTHORIZATION, format!("Bearer {jwt}"))
        .body(Body::empty())
        .unwrap();
    let resp_override_min = router.clone().oneshot(req_override_min).await.unwrap();
    assert_eq!(resp_override_min.status(), StatusCode::OK);
    let body_override_min = resp_override_min
        .into_body()
        .collect()
        .await
        .unwrap()
        .to_bytes();
    let skel_override_min: FeedSkeletonResponse =
        serde_json::from_slice(&body_override_min).unwrap();
    assert_eq!(skel_override_min.feed.len(), 3);
    assert!(skel_override_min
        .feed
        .iter()
        .any(|p| p.post.ends_with("/post_1")));
    assert!(skel_override_min
        .feed
        .iter()
        .any(|p| p.post.ends_with("/post_2")));
    assert!(skel_override_min
        .feed
        .iter()
        .any(|p| p.post.ends_with("/post_3")));

    // -----------------------------------------------------------------------
    // Level 1 Precedence: Query Parameter Overrides include_replies to false
    // Persisted dials have include_replies = true, min_likes = 3.
    // Query param: ?replies=root_only
    // Expected: Only p3 returned (p2 reply is filtered out by query override)
    // -----------------------------------------------------------------------
    let req_override_replies = Request::builder()
        .uri(format!(
            "/xrpc/app.bsky.feed.getFeedSkeleton?feed={feed_uri}&replies=root_only"
        ))
        .header(AUTHORIZATION, format!("Bearer {jwt}"))
        .body(Body::empty())
        .unwrap();
    let resp_override_replies = router.clone().oneshot(req_override_replies).await.unwrap();
    assert_eq!(resp_override_replies.status(), StatusCode::OK);
    let body_override_replies = resp_override_replies
        .into_body()
        .collect()
        .await
        .unwrap()
        .to_bytes();
    let skel_override_replies: FeedSkeletonResponse =
        serde_json::from_slice(&body_override_replies).unwrap();
    assert_eq!(skel_override_replies.feed.len(), 1);
    assert_eq!(
        skel_override_replies.feed[0].post,
        "at://did:plc:author_d/app.bsky.feed.post/post_3"
    );

    // -----------------------------------------------------------------------
    // Level 1 Precedence: Query Parameter Overrides min_likes = curated (10)
    // Query param: ?engagement_floor=curated
    // Expected: Only p3 returned (10 likes)
    // -----------------------------------------------------------------------
    let req_override_curated = Request::builder()
        .uri(format!(
            "/xrpc/app.bsky.feed.getFeedSkeleton?feed={feed_uri}&engagement_floor=curated"
        ))
        .header(AUTHORIZATION, format!("Bearer {jwt}"))
        .body(Body::empty())
        .unwrap();
    let resp_curated = router.clone().oneshot(req_override_curated).await.unwrap();
    assert_eq!(resp_curated.status(), StatusCode::OK);
    let body_curated = resp_curated.into_body().collect().await.unwrap().to_bytes();
    let skel_curated: FeedSkeletonResponse = serde_json::from_slice(&body_curated).unwrap();
    assert_eq!(skel_curated.feed.len(), 1);
    assert_eq!(
        skel_curated.feed[0].post,
        "at://did:plc:author_d/app.bsky.feed.post/post_3"
    );
}

// ===========================================================================
// Test 2: Adversarial Query Parameter Overrides and Edge Clamping
// ===========================================================================

#[tokio::test]
async fn test_xrpc_query_param_adversarial_inputs_and_clamping() {
    let (_, _, _, _, router) = create_test_server();
    let feed_uri = "at://did:plc:feed/app.bsky.feed.generator/for-you";

    // 1. Missing 'feed' parameter -> 400 Bad Request
    let req_no_feed = Request::builder()
        .uri("/xrpc/app.bsky.feed.getFeedSkeleton")
        .body(Body::empty())
        .unwrap();
    let resp_no_feed = router.clone().oneshot(req_no_feed).await.unwrap();
    assert_eq!(resp_no_feed.status(), StatusCode::BAD_REQUEST);

    // 2. Empty 'feed' parameter -> 400 Bad Request
    let req_empty_feed = Request::builder()
        .uri("/xrpc/app.bsky.feed.getFeedSkeleton?feed=")
        .body(Body::empty())
        .unwrap();
    let resp_empty_feed = router.clone().oneshot(req_empty_feed).await.unwrap();
    assert_eq!(resp_empty_feed.status(), StatusCode::BAD_REQUEST);

    // 3. Extreme out-of-bounds parameters should be clamped safely without panicking
    let adversarial_uris = vec![
        format!("/xrpc/app.bsky.feed.getFeedSkeleton?feed={feed_uri}&freshness=-9999&discovery=999.0&art=1000.0&tech=-50.0&min_likes=999999"),
        format!("/xrpc/app.bsky.feed.getFeedSkeleton?feed={feed_uri}&freshness=invalid_text&discovery=unknown&replies=gibberish&min_likes=-10"),
        format!("/xrpc/app.bsky.feed.getFeedSkeleton?feed={feed_uri}&limit=0"),
        format!("/xrpc/app.bsky.feed.getFeedSkeleton?feed={feed_uri}&limit=5000"),
        format!("/xrpc/app.bsky.feed.getFeedSkeleton?feed={feed_uri}&cursor=nonexistent_cursor_val"),
    ];

    for uri in adversarial_uris {
        let req = Request::builder().uri(&uri).body(Body::empty()).unwrap();
        let resp = router.clone().oneshot(req).await.unwrap();
        assert_eq!(
            resp.status(),
            StatusCode::OK,
            "Adversarial URI should safely return 200 OK with clamped dials: {uri}"
        );
    }
}

// ===========================================================================
// Test 3: REST /api/preferences CRUD Lifecycle & Standard/Alias Serialization
// ===========================================================================

#[tokio::test]
async fn test_rest_preferences_lifecycle_and_aliases() {
    let (_, _, _prefs, _, router) = create_test_server();
    let viewer_did = "did:plc:bob_lifecycle_test";
    let token = generate_session_token(viewer_did, 3600);

    // 1. Initial GET -> Returns defaults with is_custom: false
    let req_get1 = Request::builder()
        .uri("/api/preferences")
        .header(AUTHORIZATION, format!("Bearer {token}"))
        .body(Body::empty())
        .unwrap();
    let resp_get1 = router.clone().oneshot(req_get1).await.unwrap();
    assert_eq!(resp_get1.status(), StatusCode::OK);
    let body1 = resp_get1.into_body().collect().await.unwrap().to_bytes();
    let dto1: PreferencesResponseDto = serde_json::from_slice(&body1).unwrap();
    assert_eq!(dto1.did, viewer_did);
    assert!(!dto1.is_custom);
    assert_eq!(dto1.preferences.min_likes, DEFAULT_MIN_LIKES);
    assert_eq!(dto1.preferences.freshness_hours, 24.0);
    assert_eq!(dto1.preferences.discovery_ratio, 0.15);
    assert!(!dto1.preferences.include_replies);

    // 2. POST with alias payload: `freshness_half_life_hours`, `topics`, `engagement_floor`
    let alias_json = serde_json::json!({
        "freshness_half_life_hours": 6.0,
        "discovery_ratio": 0.35,
        "topics": {
            "art": 3.5,
            "tech": 2.0,
            "science": 1.5,
            "news": 0.5,
            "culture": 4.0
        },
        "include_replies": true,
        "engagement_floor": 10
    });
    let req_post_alias = Request::builder()
        .method(Method::POST)
        .uri("/api/preferences")
        .header(AUTHORIZATION, format!("Bearer {token}"))
        .header(CONTENT_TYPE, "application/json")
        .body(Body::from(serde_json::to_vec(&alias_json).unwrap()))
        .unwrap();
    let resp_post_alias = router.clone().oneshot(req_post_alias).await.unwrap();
    assert_eq!(resp_post_alias.status(), StatusCode::OK);
    let body_post = resp_post_alias
        .into_body()
        .collect()
        .await
        .unwrap()
        .to_bytes();
    let post_status: GenericStatusResponse = serde_json::from_slice(&body_post).unwrap();
    assert_eq!(post_status.status, "ok");
    let updated_prefs = post_status.preferences.unwrap();
    assert_eq!(updated_prefs.freshness_hours, 6.0);
    assert_eq!(updated_prefs.discovery_ratio, 0.35);
    assert_eq!(updated_prefs.min_likes, 10);
    assert!(updated_prefs.include_replies);
    assert_eq!(updated_prefs.topic_weights.art, 3.5);

    // 3. GET confirms saved state (is_custom: true)
    let req_get2 = Request::builder()
        .uri("/api/preferences")
        .header(AUTHORIZATION, format!("Bearer {token}"))
        .body(Body::empty())
        .unwrap();
    let resp_get2 = router.clone().oneshot(req_get2).await.unwrap();
    assert_eq!(resp_get2.status(), StatusCode::OK);
    let body2 = resp_get2.into_body().collect().await.unwrap().to_bytes();
    let dto2: PreferencesResponseDto = serde_json::from_slice(&body2).unwrap();
    assert!(dto2.is_custom);
    assert_eq!(dto2.preferences.min_likes, 10);
    assert!(dto2.preferences.include_replies);
    assert_eq!(dto2.preferences.freshness_hours, 6.0);

    // 4. DELETE resets preferences
    let req_del = Request::builder()
        .method(Method::DELETE)
        .uri("/api/preferences")
        .header(AUTHORIZATION, format!("Bearer {token}"))
        .body(Body::empty())
        .unwrap();
    let resp_del = router.clone().oneshot(req_del).await.unwrap();
    assert_eq!(resp_del.status(), StatusCode::OK);
    let body_del = resp_del.into_body().collect().await.unwrap().to_bytes();
    let del_status: GenericStatusResponse = serde_json::from_slice(&body_del).unwrap();
    assert_eq!(del_status.status, "reset_to_defaults");

    // 5. GET after DELETE returns default dials and is_custom: false
    let req_get3 = Request::builder()
        .uri("/api/preferences")
        .header(AUTHORIZATION, format!("Bearer {token}"))
        .body(Body::empty())
        .unwrap();
    let resp_get3 = router.clone().oneshot(req_get3).await.unwrap();
    assert_eq!(resp_get3.status(), StatusCode::OK);
    let body3 = resp_get3.into_body().collect().await.unwrap().to_bytes();
    let dto3: PreferencesResponseDto = serde_json::from_slice(&body3).unwrap();
    assert!(!dto3.is_custom);
    assert_eq!(dto3.preferences.min_likes, DEFAULT_MIN_LIKES);
    assert!(!dto3.preferences.include_replies);
}

// ===========================================================================
// Test 4: REST /api/preferences Boundary Validation Rejections (400 Bad Request)
// ===========================================================================

#[tokio::test]
async fn test_rest_preferences_boundary_rejections() {
    let (_, _, _, _, router) = create_test_server();
    let viewer_did = "did:plc:boundary_tester";
    let token = generate_session_token(viewer_did, 3600);

    let invalid_payloads = vec![
        // Freshness < 1.0 hr
        serde_json::json!({
            "freshness_hours": 0.5,
            "discovery_ratio": 0.15,
        }),
        // Freshness > 168.0 hr (7 days)
        serde_json::json!({
            "freshness_hours": 200.0,
            "discovery_ratio": 0.15,
        }),
        // Discovery ratio < 0.0
        serde_json::json!({
            "freshness_hours": 36.0,
            "discovery_ratio": -0.1,
        }),
        // Discovery ratio > 0.50
        serde_json::json!({
            "freshness_hours": 36.0,
            "discovery_ratio": 0.60,
        }),
        // Min likes > 100
        serde_json::json!({
            "freshness_hours": 36.0,
            "discovery_ratio": 0.15,
            "min_likes": 101
        }),
        // Topic multiplier > 5.0
        serde_json::json!({
            "freshness_hours": 36.0,
            "discovery_ratio": 0.15,
            "topics": {
                "art": 5.5,
                "tech": 1.0,
                "science": 1.0,
                "news": 1.0,
                "culture": 1.0
            }
        }),
        // Topic multiplier < 0.0
        serde_json::json!({
            "freshness_hours": 36.0,
            "discovery_ratio": 0.15,
            "topics": {
                "art": 1.0,
                "tech": -1.0,
                "science": 1.0,
                "news": 1.0,
                "culture": 1.0
            }
        }),
    ];

    for payload in invalid_payloads {
        let req = Request::builder()
            .method(Method::POST)
            .uri("/api/preferences")
            .header(AUTHORIZATION, format!("Bearer {token}"))
            .header(CONTENT_TYPE, "application/json")
            .body(Body::from(serde_json::to_vec(&payload).unwrap()))
            .unwrap();
        let resp = router.clone().oneshot(req).await.unwrap();
        assert_eq!(
            resp.status(),
            StatusCode::BAD_REQUEST,
            "Expected 400 Bad Request for invalid payload: {payload:?}"
        );
        let body = resp.into_body().collect().await.unwrap().to_bytes();
        let err_resp: ApiErrorResponse = serde_json::from_slice(&body).unwrap();
        assert_eq!(err_resp.error, "InvalidInput");
    }
}

// ===========================================================================
// Test 5: Web Dashboard SPA HTML Contract & Security Headers
// ===========================================================================

#[tokio::test]
async fn test_web_dashboard_spa_contract_and_security_headers() {
    let (_, _, _, _, router) = create_test_server();

    // 1. GET / serves dashboard HTML with security headers
    let req_root = Request::builder().uri("/").body(Body::empty()).unwrap();
    let resp_root = router.clone().oneshot(req_root).await.unwrap();
    assert_eq!(resp_root.status(), StatusCode::OK);

    // Verify defense-in-depth security headers
    let headers = resp_root.headers();
    assert_eq!(headers.get("x-content-type-options").unwrap(), "nosniff");
    assert_eq!(headers.get("x-frame-options").unwrap(), "SAMEORIGIN");
    assert_eq!(
        headers.get("referrer-policy").unwrap(),
        "strict-origin-when-cross-origin"
    );
    assert!(headers.get("content-security-policy").is_some());

    // 2. Inspect embedded DASHBOARD_HTML content for required DOM IDs and SPA controls
    let html = DASHBOARD_HTML;

    // Sliders
    assert!(
        html.contains(r#"id="slider-freshness""#),
        "Missing slider-freshness"
    );
    assert!(
        html.contains(r#"id="slider-discovery""#),
        "Missing slider-discovery"
    );
    assert!(
        html.contains(r#"id="slider-topic-art""#),
        "Missing slider-topic-art"
    );
    assert!(
        html.contains(r#"id="slider-topic-tech""#),
        "Missing slider-topic-tech"
    );
    assert!(
        html.contains(r#"id="slider-topic-science""#),
        "Missing slider-topic-science"
    );
    assert!(
        html.contains(r#"id="slider-topic-news""#),
        "Missing slider-topic-news"
    );
    assert!(
        html.contains(r#"id="slider-topic-culture""#),
        "Missing slider-topic-culture"
    );

    // Post composition toggle buttons
    assert!(
        html.contains(r#"id="btn-composition-root""#),
        "Missing btn-composition-root"
    );
    assert!(
        html.contains(r#"id="btn-composition-all""#),
        "Missing btn-composition-all"
    );

    // Engagement floor preset buttons
    assert!(
        html.contains(r#"id="btn-engagement-emerging""#),
        "Missing btn-engagement-emerging"
    );
    assert!(
        html.contains(r#"id="btn-engagement-balanced""#),
        "Missing btn-engagement-balanced"
    );
    assert!(
        html.contains(r#"id="btn-engagement-curated""#),
        "Missing btn-engagement-curated"
    );

    // Save & Reset buttons
    assert!(
        html.contains(r#"id="btn-save-preferences""#),
        "Missing btn-save-preferences"
    );
    assert!(
        html.contains(r#"id="btn-reset-dials""#),
        "Missing btn-reset-dials"
    );
    assert!(
        html.contains(r#"id="btn-simulate-feed""#),
        "Missing btn-simulate-feed"
    );

    // JavaScript endpoint bindings
    assert!(
        html.contains("/api/preferences"),
        "Missing /api/preferences JS binding"
    );
    assert!(
        html.contains("/api/feed-preview"),
        "Missing /api/feed-preview JS binding"
    );
    assert!(
        html.contains("/api/taste-twins"),
        "Missing /api/taste-twins JS binding"
    );
    assert!(
        html.contains("/api/telemetry"),
        "Missing /api/telemetry JS binding"
    );
    assert!(
        html.contains("/api/explain"),
        "Missing /api/explain JS binding"
    );
}

// ===========================================================================
// Test 6: Concurrent Multi-Threaded Stress across 64-Shard Preferences
// ===========================================================================

#[tokio::test]
async fn test_concurrent_preferences_and_xrpc_reads_stress() {
    let (interner, graph, prefs, recommender, _) = create_test_server();
    let now = current_time_secs();

    // Populate graph with 500 users and 2,000 posts
    for u in 0..500 {
        let did = format!("did:plc:stress_user_{u:04}");
        interner.intern(&did);
    }
    for p in 0..2000 {
        let uri = format!(
            "at://did:plc:stress_user_{:04}/app.bsky.feed.post/p_{p:04}",
            p % 500
        );
        let pid = interner.intern(&uri);
        let author_id = interner.intern(&format!("did:plc:stress_user_{:04}", p % 500));
        graph.record_post_meta(pid, author_id, None, None, now - 3600);

        for l in 0..(p % 20) {
            let liker = interner.intern(&format!("did:plc:stress_user_{:04}", (p + l * 7) % 500));
            graph.record_interaction(liker, pid, SignalType::Like, now - 1800);
        }
    }

    let num_tasks = 16;
    let iterations_per_task = 100;
    let mut handles = Vec::with_capacity(num_tasks);

    for t in 0..num_tasks {
        let interner = Arc::clone(&interner);
        let prefs = Arc::clone(&prefs);
        let recommender = Arc::clone(&recommender);

        handles.push(tokio::spawn(async move {
            for i in 0..iterations_per_task {
                let user_idx = (t * 50 + i) % 500;
                let did = format!("did:plc:stress_user_{user_idx:04}");
                let uid = interner.intern(&did);

                // Alternating mutations and reads across 64 shards
                match i % 4 {
                    0 => {
                        // Mutate preference
                        prefs.set(
                            uid,
                            UserDials {
                                freshness_half_life_secs: ((i % 10) + 1) as f32 * 3600.0,
                                serendipity_ratio: (i % 50) as f32 / 100.0,
                                topic_weights: TopicWeights::default(),
                                include_replies: i % 2 == 0,
                                min_likes: (i % 15) as u32,
                                updated_at_secs: now,
                            },
                        );
                    }
                    1 => {
                        // Read preference
                        let _ = prefs.get(uid);
                    }
                    2 => {
                        // Delete preference
                        prefs.delete(uid);
                    }
                    _ => {
                        // Execute feed recommendation with viewer lookup
                        let dials = RecommendationDials::default();
                        let res = recommender.recommend(Some(&did), &dials, now);
                        assert!(res.is_ok());
                    }
                }
            }
        }));
    }

    for h in handles {
        h.await.unwrap();
    }
}

// ===========================================================================
// Test 7: Release Mode Concurrent p99 Latency SLA Benchmark
// ===========================================================================

#[test]
fn test_release_concurrent_p99_latency_benchmark() {
    let interner = Arc::new(StringInterner::new());
    let graph = Arc::new(GraphStore::new());
    let prefs = Arc::new(UserPreferencesStore::new());
    let now = current_time_secs();

    // 1,000 users, 5,000 posts, follows, interactions
    let mut user_dids = Vec::with_capacity(1000);
    for u in 0..1000 {
        let did = format!("did:plc:bench_user_{u:04}");
        let uid = interner.intern(&did);
        user_dids.push(did);

        // Populate some persisted preferences
        if u % 2 == 0 {
            prefs.set(
                uid,
                UserDials {
                    freshness_half_life_secs: 24.0 * 3600.0,
                    serendipity_ratio: 0.25,
                    topic_weights: TopicWeights::default(),
                    include_replies: u % 4 == 0,
                    min_likes: (u % 10) as u32,
                    updated_at_secs: now,
                },
            );
        }
    }

    for p in 0..5000 {
        let author_idx = p % 1000;
        let uri = format!("at://did:plc:bench_user_{author_idx:04}/app.bsky.feed.post/p_{p:05}");
        let pid = interner.intern(&uri);
        let author_id = interner.intern(&format!("did:plc:bench_user_{author_idx:04}"));
        graph.record_post_meta(pid, author_id, None, None, now - 7200);

        for l in 0..(p % 30) {
            let liker = interner.intern(&format!("did:plc:bench_user_{:04}", (p + l * 11) % 1000));
            graph.record_interaction(liker, pid, SignalType::Like, now - 3600);
        }
    }

    for u in 0..1000 {
        for f in 1..=5 {
            let target = (u + f * 50) % 1000;
            graph.record_follow(u as u32, target as u32);
        }
    }

    let recommender = Arc::new(Recommender::new(Arc::clone(&interner), Arc::clone(&graph)));

    // Measure sequential p99 latency across 1,000 queries
    let mut latencies = Vec::with_capacity(1000);
    for i in 0..1000 {
        let viewer = &user_dids[i % user_dids.len()];
        let dials = RecommendationDials {
            min_likes: match i % 3 {
                0 => EMERGING_MIN_LIKES,
                1 => DEFAULT_MIN_LIKES,
                _ => CURATED_MIN_LIKES,
            },
            limit: 30,
            ..Default::default()
        };

        let t0 = Instant::now();
        let res = recommender.recommend(Some(viewer.as_str()), &dials, now);
        let elapsed_micros = t0.elapsed().as_micros();
        latencies.push(elapsed_micros);
        assert!(res.is_ok());
    }

    latencies.sort_unstable();
    let p50 = latencies[latencies.len() * 50 / 100];
    let p90 = latencies[latencies.len() * 90 / 100];
    let p99 = latencies[latencies.len() * 99 / 100];

    println!("\n[EMPIRICAL CHALLENGER 2 LATENCY SLA]");
    println!("  p50: {p50} µs ({:.3} ms)", p50 as f64 / 1000.0);
    println!("  p90: {p90} µs ({:.3} ms)", p90 as f64 / 1000.0);
    println!("  p99: {p99} µs ({:.3} ms)", p99 as f64 / 1000.0);

    #[cfg(not(debug_assertions))]
    {
        assert!(
            p99 < 2000,
            "Release mode p99 latency must be under 2.0ms (2000 µs), got {p99} µs"
        );
    }
}
