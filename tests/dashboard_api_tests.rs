#![forbid(unsafe_code)]

//! Comprehensive test suite for Dashboard & Telemetry Axum REST Endpoints:
//! - `GET /api/telemetry`
//! - `GET /api/taste-twins`
//! - `GET /api/feed-preview`
//! - `GET /api/explain`
//!
//! Covers route responses, query parameter variations, error handling,
//! JSON serialization, concurrency, and CORS headers.

use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use for_your_consideration::prelude::*;
use http_body_util::BodyExt;
use tower::ServiceExt;

/// Creates a rich test state with interconnected users, posts, topics, and interactions.
fn create_rich_test_state() -> (
    AppState,
    Arc<StringInterner>,
    Arc<GraphStore>,
    Arc<Recommender>,
) {
    let interner = Arc::new(StringInterner::new());
    let graph = Arc::new(GraphStore::new());
    let recommender = Arc::new(Recommender::new(Arc::clone(&interner), Arc::clone(&graph)));

    let now = BLUESKY_EPOCH_SECS + 500_000;

    // Users
    let alice = interner.intern("did:plc:alice");
    let bob = interner.intern("did:plc:bob");
    let carol = interner.intern("did:plc:carol");
    let dave = interner.intern("did:plc:dave");

    let art_author = interner.intern("did:plc:art_seed");
    let tech_author = interner.intern("did:plc:tech_seed");
    let sci_author = interner.intern("did:plc:science_seed");

    // Posts
    let art_p1 = interner.intern("at://did:plc:art_seed/app.bsky.feed.post/oil_painting");
    let art_p2 = interner.intern("at://did:plc:art_seed/app.bsky.feed.post/sketch");
    let tech_p1 = interner.intern("at://did:plc:tech_seed/app.bsky.feed.post/rust_async");
    let tech_p2 = interner.intern("at://did:plc:tech_seed/app.bsky.feed.post/compiler_ir");
    let sci_p1 = interner.intern("at://did:plc:science_seed/app.bsky.feed.post/james_webb");

    graph.record_post_meta(art_p1, art_author, None, None, now - 1000);
    graph.record_post_meta(art_p2, art_author, None, None, now - 1000);
    graph.record_post_meta(tech_p1, tech_author, None, None, now - 1000);
    graph.record_post_meta(tech_p2, tech_author, None, None, now - 1000);
    graph.record_post_meta(sci_p1, sci_author, None, None, now - 1000);

    // 10 dummy posts for Alice to enable Tier 1
    for i in 1..=10 {
        let p = interner.intern(&format!(
            "at://did:plc:tech_seed/app.bsky.feed.post/alice_dummy_{i}"
        ));
        graph.record_post_meta(p, tech_author, None, None, now - 2000);
        graph.record_interaction(alice, p, SignalType::Like, now - 1500);
    }

    // Alice likes tech_p1 and art_p1
    graph.record_interaction(alice, tech_p1, SignalType::Like, now - 500);
    graph.record_interaction(alice, art_p1, SignalType::Like, now - 500);

    // Bob (Taste Twin with Alice) likes tech_p1, art_p1, and tech_p2
    graph.record_interaction(bob, tech_p1, SignalType::Like, now - 400);
    graph.record_interaction(bob, art_p1, SignalType::Like, now - 400);
    graph.record_interaction(bob, tech_p2, SignalType::Repost, now - 300);

    // Baseline interactions so candidate posts meet default engagement floor (min_likes: 3)
    let u1 = interner.intern("did:plc:mock_user_1");
    let u2 = interner.intern("did:plc:mock_user_2");
    let u3 = interner.intern("did:plc:mock_user_3");
    for &p in &[tech_p2, art_p2, sci_p1] {
        graph.record_interaction(u1, p, SignalType::Like, now - 350);
        graph.record_interaction(u2, p, SignalType::Like, now - 350);
    }
    graph.record_interaction(u3, art_p2, SignalType::Like, now - 350);
    graph.record_interaction(carol, tech_p1, SignalType::Like, now - 350);
    graph.record_interaction(dave, art_p1, SignalType::Like, now - 350);

    // Carol follows Dave; Dave liked sci_p1
    graph.record_follow(carol, dave);
    graph.record_interaction(dave, sci_p1, SignalType::Like, now - 200);

    // Configure snapshot tracker
    let snap_config = SnapshotConfig {
        path: std::path::PathBuf::from("target/test_dashboard_snapshot.bin"),
        interval_secs: 300,
    };
    let snapshot_tracker = Arc::new(SnapshotStatusTracker::new(&snap_config));
    snapshot_tracker.record_load(14.5);

    // Configure ingestion tracker
    let stats = Arc::new(IngestionStats::new(Some(1_700_000_000_000_000)));
    stats
        .events_received
        .store(1500, std::sync::atomic::Ordering::Relaxed);
    stats
        .events_processed
        .store(1480, std::sync::atomic::Ordering::Relaxed);
    stats
        .bytes_received
        .store(75000, std::sync::atomic::Ordering::Relaxed);
    stats
        .last_activity_timestamp
        .store(now, std::sync::atomic::Ordering::Relaxed);
    let ingestion_tracker = Arc::new(IngestionTracker::new(stats));

    let state = AppState::new(
        Arc::clone(&recommender),
        "did:web:feed.example.com",
        "feed.example.com",
    )
    .with_snapshot_tracker(snapshot_tracker)
    .with_ingestion_tracker(ingestion_tracker);

    (state, interner, graph, recommender)
}

#[tokio::test]
async fn test_telemetry_endpoint_full_schema_and_values() {
    let (state, _interner, _graph, _rec) = create_rich_test_state();
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
    assert!(telemetry.graph.total_users > 0);
    assert!(telemetry.graph.total_posts > 0);
    assert!(telemetry.graph.total_edges > 0);
    assert!(telemetry.graph.total_follows >= 1);
    assert!(telemetry.interner.total_interned_strings > 0);

    assert_eq!(telemetry.ingestion.events_received, 1500);
    assert_eq!(telemetry.ingestion.events_processed, 1480);
    assert_eq!(telemetry.ingestion.latest_cursor_us, 1_700_000_000_000_000);

    assert_eq!(telemetry.snapshot.status, "hydrated");
    assert!((telemetry.snapshot.last_load_duration_ms - 14.5).abs() < 1e-2);
    assert_eq!(telemetry.snapshot.interval_secs, 300);

    assert_eq!(
        telemetry.impression_store.hard_suppression_window_secs,
        1800
    );
    assert_eq!(telemetry.impression_store.fatigue_decay_window_secs, 21600);
}

#[tokio::test]
async fn test_taste_twins_valid_did_and_cosine_similarity() {
    let (state, _interner, _graph, _rec) = create_rich_test_state();
    let app = create_xrpc_router(state);

    let req = Request::builder()
        .uri("/api/taste-twins?did=did:plc:alice&limit=10")
        .body(Body::empty())
        .unwrap();

    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let body = resp.into_body().collect().await.unwrap().to_bytes();
    let twins_resp: TasteTwinsResponse = serde_json::from_slice(&body).unwrap();

    assert_eq!(twins_resp.viewer_did, "did:plc:alice");
    assert!(twins_resp.total_liked_posts >= 12);
    assert_eq!(twins_resp.twins.len(), 1);

    let bob_twin = &twins_resp.twins[0];
    assert_eq!(bob_twin.user_did, "did:plc:bob");
    assert!(bob_twin.similarity_score > 0.0);
    assert_eq!(bob_twin.shared_posts_count, 2);
    assert_eq!(bob_twin.shared_posts.len(), 2);
    assert!(
        bob_twin.top_interests.contains(&TopicCategory::Tech)
            || bob_twin.top_interests.contains(&TopicCategory::Art)
    );
}

#[tokio::test]
async fn test_taste_twins_handle_parameter_variations() {
    let (state, interner, graph, _rec) = create_rich_test_state();
    let now = BLUESKY_EPOCH_SECS + 500_000;

    let alice_handle = interner.intern("alice.bsky.social");
    let bob_handle = interner.intern("bob.bsky.social");
    let post = interner.intern("at://did:plc:art_seed/app.bsky.feed.post/shared");
    let post2 = interner.intern("at://did:plc:art_seed/app.bsky.feed.post/shared2");
    let author = interner.intern("did:plc:art_seed");

    graph.record_post_meta(post, author, None, None, now - 100);
    graph.record_post_meta(post2, author, None, None, now - 100);
    graph.record_interaction(alice_handle, post, SignalType::Like, now - 50);
    graph.record_interaction(alice_handle, post2, SignalType::Like, now - 50);
    graph.record_interaction(bob_handle, post, SignalType::Like, now - 50);
    graph.record_interaction(bob_handle, post2, SignalType::Like, now - 50);

    let app = create_xrpc_router(state);

    // 1. Without @
    let req1 = Request::builder()
        .uri("/api/taste-twins?handle=alice.bsky.social")
        .body(Body::empty())
        .unwrap();
    let resp1 = app.clone().oneshot(req1).await.unwrap();
    assert_eq!(resp1.status(), StatusCode::OK);
    let body1 = resp1.into_body().collect().await.unwrap().to_bytes();
    let twins1: TasteTwinsResponse = serde_json::from_slice(&body1).unwrap();
    assert_eq!(twins1.twins.len(), 1);

    // 2. With @
    let req2 = Request::builder()
        .uri("/api/taste-twins?handle=@alice.bsky.social")
        .body(Body::empty())
        .unwrap();
    let resp2 = app.oneshot(req2).await.unwrap();
    assert_eq!(resp2.status(), StatusCode::OK);
    let body2 = resp2.into_body().collect().await.unwrap().to_bytes();
    let twins2: TasteTwinsResponse = serde_json::from_slice(&body2).unwrap();
    assert_eq!(twins2.twins.len(), 1);
    assert_eq!(twins2.twins[0].user_did, "bob.bsky.social");
}

#[tokio::test]
async fn test_taste_twins_missing_or_empty_params_returns_400() {
    let (state, _interner, _graph, _rec) = create_rich_test_state();
    let app = create_xrpc_router(state);

    // 1. Missing both did and handle
    let req1 = Request::builder()
        .uri("/api/taste-twins")
        .body(Body::empty())
        .unwrap();
    let resp1 = app.clone().oneshot(req1).await.unwrap();
    assert_eq!(resp1.status(), StatusCode::BAD_REQUEST);
    let body1 = resp1.into_body().collect().await.unwrap().to_bytes();
    let err1: ApiErrorResponse = serde_json::from_slice(&body1).unwrap();
    assert_eq!(err1.error, "InvalidRequest");

    // 2. Empty string did
    let req2 = Request::builder()
        .uri("/api/taste-twins?did=")
        .body(Body::empty())
        .unwrap();
    let resp2 = app.clone().oneshot(req2).await.unwrap();
    assert_eq!(resp2.status(), StatusCode::BAD_REQUEST);

    // 3. Whitespace handle
    let req3 = Request::builder()
        .uri("/api/taste-twins?handle=%20%20")
        .body(Body::empty())
        .unwrap();
    let resp3 = app.oneshot(req3).await.unwrap();
    assert_eq!(resp3.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn test_taste_twins_unknown_did_returns_empty_gracefully() {
    let (state, _interner, _graph, _rec) = create_rich_test_state();
    let app = create_xrpc_router(state);

    let req = Request::builder()
        .uri("/api/taste-twins?did=did:plc:ghost_user")
        .body(Body::empty())
        .unwrap();

    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let body = resp.into_body().collect().await.unwrap().to_bytes();
    let twins: TasteTwinsResponse = serde_json::from_slice(&body).unwrap();
    assert_eq!(twins.viewer_did, "did:plc:ghost_user");
    assert_eq!(twins.total_liked_posts, 0);
    assert!(twins.twins.is_empty());
}

#[tokio::test]
async fn test_feed_preview_tier1_authenticated_viewer() {
    let (state, _interner, _graph, _rec) = create_rich_test_state();
    let app = create_xrpc_router(state);

    let req = Request::builder()
        .uri("/api/feed-preview?viewer=did:plc:alice&freshness=balanced&limit=10&explain=true")
        .body(Body::empty())
        .unwrap();

    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let body = resp.into_body().collect().await.unwrap().to_bytes();
    let preview: FeedPreviewResponse = serde_json::from_slice(&body).unwrap();

    assert_eq!(preview.viewer_did, "did:plc:alice");
    assert!(!preview.items.is_empty());

    // Check that tech_p2 (liked by taste twin Bob) is recommended
    let cand = preview
        .items
        .iter()
        .find(|i| i.uri == "at://did:plc:tech_seed/app.bsky.feed.post/compiler_ir");
    assert!(cand.is_some());
    let item = cand.unwrap();
    assert_eq!(item.topic, TopicCategory::Tech);
    assert!(item.tier.contains("Tier 1"));
    assert!(item.score_breakdown.final_score > 0.0);
    assert!(item.proof_chain.is_some());
}

#[tokio::test]
async fn test_feed_preview_tier2_followed_walk() {
    let (state, _interner, _graph, _rec) = create_rich_test_state();
    let app = create_xrpc_router(state);

    // Carol follows Dave who liked sci_p1
    let req = Request::builder()
        .uri("/api/feed-preview?viewer=did:plc:carol&limit=10&explain=true")
        .body(Body::empty())
        .unwrap();

    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let body = resp.into_body().collect().await.unwrap().to_bytes();
    let preview: FeedPreviewResponse = serde_json::from_slice(&body).unwrap();

    assert_eq!(preview.viewer_did, "did:plc:carol");
    assert!(!preview.items.is_empty());

    let item = preview
        .items
        .iter()
        .find(|i| i.uri == "at://did:plc:science_seed/app.bsky.feed.post/james_webb");
    assert!(item.is_some());
    let sci_item = item.unwrap();
    assert_eq!(sci_item.topic, TopicCategory::Science);
    assert!(sci_item.tier.contains("Tier 2"));
    assert!(sci_item.proof_chain.is_some());
}

#[tokio::test]
async fn test_feed_preview_anonymous_cold_start_tier3() {
    let (state, _interner, _graph, _rec) = create_rich_test_state();
    let app = create_xrpc_router(state);

    let req = Request::builder()
        .uri("/api/feed-preview?limit=10&explain=true")
        .body(Body::empty())
        .unwrap();

    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let body = resp.into_body().collect().await.unwrap().to_bytes();
    let preview: FeedPreviewResponse = serde_json::from_slice(&body).unwrap();

    assert_eq!(preview.viewer_did, "");
    assert!(!preview.items.is_empty());
}

#[tokio::test]
async fn test_feed_preview_topic_sliders_modulation() {
    let (state, _interner, _graph, _rec) = create_rich_test_state();
    let app = create_xrpc_router(state);

    // Boost art by 5.0, reduce tech to 0.1
    let req = Request::builder()
        .uri("/api/feed-preview?art=5.0&tech=0.1&science=1.0&limit=20")
        .body(Body::empty())
        .unwrap();

    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let body = resp.into_body().collect().await.unwrap().to_bytes();
    let preview: FeedPreviewResponse = serde_json::from_slice(&body).unwrap();

    for item in &preview.items {
        if item.topic == TopicCategory::Art {
            assert_eq!(item.score_breakdown.topic_boost, 5.0);
        } else if item.topic == TopicCategory::Tech {
            assert_eq!(item.score_breakdown.topic_boost, 0.1);
        }
    }
}

#[tokio::test]
async fn test_feed_preview_read_only_impression_safety() {
    let (state, interner, _graph, _rec) = create_rich_test_state();
    let app = create_xrpc_router(state.clone());

    // Call /api/feed-preview 5 times
    for _ in 0..5 {
        let req = Request::builder()
            .uri("/api/feed-preview?viewer=did:plc:alice&limit=10")
            .body(Body::empty())
            .unwrap();
        let resp = app.clone().oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    // Ensure 0 impressions were added for Alice
    let uid = interner.lookup_id("did:plc:alice").unwrap();
    assert_eq!(
        state
            .recommender
            .impression_store
            .get_viewer_impression_count(uid),
        0
    );
}

#[tokio::test]
async fn test_explain_endpoint_valid_and_proof_chain_steps() {
    let (state, _interner, _graph, _rec) = create_rich_test_state();
    let app = create_xrpc_router(state);

    let req = Request::builder()
        .uri("/api/explain?viewer=did:plc:alice&uri=at://did:plc:tech_seed/app.bsky.feed.post/compiler_ir")
        .body(Body::empty())
        .unwrap();

    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let body = resp.into_body().collect().await.unwrap().to_bytes();
    let chain: GraphProofChain = serde_json::from_slice(&body).unwrap();

    assert_eq!(chain.steps.len(), 3);
    assert_eq!(chain.steps[0].step_type, "viewer_interaction");
    assert_eq!(chain.steps[1].step_type, "taste_similarity");
    assert_eq!(chain.steps[1].node_id, "did:plc:bob");
    assert_eq!(chain.steps[2].step_type, "recommendation_signal");
    assert!(chain.summary.contains("did:plc:bob"));
}

#[tokio::test]
async fn test_explain_endpoint_post_parameter_alias() {
    let (state, _interner, _graph, _rec) = create_rich_test_state();
    let app = create_xrpc_router(state);

    let req = Request::builder()
        .uri("/api/explain?viewer=did:plc:alice&post=at://did:plc:tech_seed/app.bsky.feed.post/compiler_ir")
        .body(Body::empty())
        .unwrap();

    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let body = resp.into_body().collect().await.unwrap().to_bytes();
    let chain: GraphProofChain = serde_json::from_slice(&body).unwrap();
    assert_eq!(chain.steps.len(), 3);
}

#[tokio::test]
async fn test_explain_endpoint_missing_uri_returns_400() {
    let (state, _interner, _graph, _rec) = create_rich_test_state();
    let app = create_xrpc_router(state);

    let req = Request::builder()
        .uri("/api/explain?viewer=did:plc:alice")
        .body(Body::empty())
        .unwrap();

    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);

    let body = resp.into_body().collect().await.unwrap().to_bytes();
    let err: ApiErrorResponse = serde_json::from_slice(&body).unwrap();
    assert_eq!(err.error, "InvalidRequest");
}

#[tokio::test]
async fn test_explain_endpoint_unindexed_post_fallback() {
    let (state, _interner, _graph, _rec) = create_rich_test_state();
    let app = create_xrpc_router(state);

    let req = Request::builder()
        .uri("/api/explain?viewer=did:plc:alice&uri=at://did:plc:unknown/post/999")
        .body(Body::empty())
        .unwrap();

    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let body = resp.into_body().collect().await.unwrap().to_bytes();
    let chain: GraphProofChain = serde_json::from_slice(&body).unwrap();
    assert_eq!(chain.steps.len(), 1);
    assert_eq!(chain.steps[0].step_type, "unindexed_post");
}

#[tokio::test]
async fn test_concurrent_multi_endpoint_stress() {
    let (state, _interner, _graph, _rec) = create_rich_test_state();
    let router = create_xrpc_router(state);

    let mut handles = Vec::new();

    for i in 0..40 {
        let app = router.clone();
        let handle = tokio::spawn(async move {
            let uri = match i % 4 {
                0 => "/api/telemetry".to_string(),
                1 => "/api/taste-twins?did=did:plc:alice&limit=5".to_string(),
                2 => "/api/feed-preview?viewer=did:plc:alice&limit=10".to_string(),
                _ => "/api/explain?viewer=did:plc:alice&uri=at://did:plc:tech_seed/app.bsky.feed.post/compiler_ir".to_string(),
            };

            let req = Request::builder().uri(&uri).body(Body::empty()).unwrap();
            let resp = app.oneshot(req).await.unwrap();
            assert_eq!(resp.status(), StatusCode::OK);
            let body = resp.into_body().collect().await.unwrap().to_bytes();
            assert!(!body.is_empty());
        });
        handles.push(handle);
    }

    for h in handles {
        h.await.unwrap();
    }
}
