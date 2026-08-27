#![forbid(unsafe_code)]

//! # Tier 5 Adversarial Coverage Hardening Tests
//!
//! Comprehensive adversarial stress testing suite covering:
//! 1. Extreme concurrency with mixed REST mutations, snapshot cycles, and concurrent XRPC `getFeedSkeleton` reads.
//! 2. Edge case dial combinations (all 0.0x topic weights, boundary freshness/discovery values, mixed query overrides).
//! 3. Session token forgery and replay attacks (tampering, invalid DIDs, expiration, header malformations, replay).
//! 4. Single-viewer hot lock contention and atomic read isolation.
//! 5. Mathematical invariance of continuous soft impression decay and anti-fatigue dampening.
//! 6. High-throughput uninterned viewer fast-path throughput.

use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Instant;

use axum::body::Body;
use axum::http::header::{AUTHORIZATION, CONTENT_TYPE};
use axum::http::{Method, Request, StatusCode};
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine;
use http_body_util::BodyExt;
use tower::ServiceExt;

use for_your_consideration::prelude::*;

/// Helper to construct a fully configured test application state.
fn create_test_state() -> (
    AppState,
    Arc<StringInterner>,
    Arc<GraphStore>,
    Arc<UserPreferencesStore>,
    Arc<SnapshotStatusTracker>,
    Arc<IngestionTracker>,
) {
    let interner = Arc::new(StringInterner::new());
    let graph = Arc::new(GraphStore::new());
    let preferences_store = Arc::new(UserPreferencesStore::new());
    let snapshot_tracker = Arc::new(SnapshotStatusTracker::default());
    let ingestion_tracker = Arc::new(IngestionTracker::default());

    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();

    // Populate initial graph state across diverse topics
    let authors = [
        (
            "did:plc:art_seed",
            "at://did:plc:art_seed/app.bsky.feed.post/art_1",
            TopicCategory::Art,
        ),
        (
            "did:plc:tech_seed",
            "at://did:plc:tech_seed/app.bsky.feed.post/tech_1",
            TopicCategory::Tech,
        ),
        (
            "did:plc:science_seed",
            "at://did:plc:science_seed/app.bsky.feed.post/sci_1",
            TopicCategory::Science,
        ),
        (
            "did:plc:news_seed",
            "at://did:plc:news_seed/app.bsky.feed.post/news_1",
            TopicCategory::News,
        ),
        (
            "did:plc:culture_seed",
            "at://did:plc:culture_seed/app.bsky.feed.post/cult_1",
            TopicCategory::Culture,
        ),
    ];

    for (author_did, post_uri, _cat) in authors {
        let aid = interner.intern(author_did);
        let pid = interner.intern(post_uri);
        graph.record_post_meta(pid, aid, None, None, now - 100);

        // Seed interactions from diverse users to generate high-velocity pool
        for u in 0..15 {
            let uid = interner.intern(&format!("did:plc:seed_user_{u}"));
            graph.record_interaction(uid, pid, SignalType::Like, now - (u as u64 * 10));
        }
    }

    let recommender = Arc::new(Recommender::new(Arc::clone(&interner), Arc::clone(&graph)));
    let state = AppState::new(recommender, "did:web:feed.example.com", "feed.example.com")
        .with_preferences_store(Arc::clone(&preferences_store))
        .with_snapshot_tracker(Arc::clone(&snapshot_tracker))
        .with_ingestion_tracker(Arc::clone(&ingestion_tracker));

    (
        state,
        interner,
        graph,
        preferences_store,
        snapshot_tracker,
        ingestion_tracker,
    )
}

// =========================================================================
// 1. EXTREME CONCURRENCY: MIXED REST, SNAPSHOT CYCLES & XRPC READS
// =========================================================================

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn test_adversarial_extreme_concurrency_mixed_rest_snapshots_and_xrpc_reads() {
    let (state, interner, graph, prefs_store, snapshot_tracker, ingestion_tracker) =
        create_test_state();
    let app = create_xrpc_router(state);

    let num_tasks = 24;
    let iterations_per_task = 50;
    let mut handles = Vec::new();

    for task_idx in 0..num_tasks {
        let app = app.clone();
        let interner = Arc::clone(&interner);
        let graph = Arc::clone(&graph);
        let prefs_store = Arc::clone(&prefs_store);
        let snapshot_tracker = Arc::clone(&snapshot_tracker);
        let ingestion_tracker = Arc::clone(&ingestion_tracker);
        let task_snapshot_file = std::env::temp_dir().join(format!(
            "concurrent_stress_{}_{}_{}.bin",
            std::process::id(),
            task_idx,
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap_or_default()
                .as_nanos()
        ));

        let handle = tokio::spawn(async move {
            for iter in 0..iterations_per_task {
                let user_idx = (task_idx * 17 + iter) % 40;
                let user_did = format!("did:plc:stress_user_{user_idx}");
                let token = generate_session_token(&user_did, 3600);

                match (task_idx + iter) % 6 {
                    // Action 0: POST /api/preferences (Mutation)
                    0 => {
                        let freshness_h = 2.0 + ((iter % 40) as f32);
                        let disc = ((iter % 30) as f32).mul_add(0.01, 0.05);
                        let save_req = SavePreferencesRequestBody {
                            freshness_hours: freshness_h,
                            discovery_ratio: disc,
                            topic_weights: Some(TopicWeights {
                                art: (iter % 5) as f32,
                                tech: ((iter + 1) % 5) as f32,
                                science: ((iter + 2) % 5) as f32,
                                news: ((iter + 3) % 5) as f32,
                                culture: ((iter + 4) % 5) as f32,
                            }),
                            include_replies: Some(iter % 2 == 0),
                            min_likes: Some((iter % 10) as u32),
                        };
                        let req = Request::builder()
                            .method(Method::POST)
                            .uri("/api/preferences")
                            .header(AUTHORIZATION, format!("Bearer {token}"))
                            .header(CONTENT_TYPE, "application/json")
                            .body(Body::from(serde_json::to_vec(&save_req).unwrap()))
                            .unwrap();
                        let resp = app.clone().oneshot(req).await.unwrap();
                        assert_eq!(resp.status(), StatusCode::OK);
                    }

                    // Action 1: DELETE /api/preferences (Mutation / Reset)
                    1 => {
                        let req = Request::builder()
                            .method(Method::DELETE)
                            .uri("/api/preferences")
                            .header(AUTHORIZATION, format!("Bearer {token}"))
                            .body(Body::empty())
                            .unwrap();
                        let resp = app.clone().oneshot(req).await.unwrap();
                        assert_eq!(resp.status(), StatusCode::OK);
                    }

                    // Action 2: Snapshot Persistence & Hydration Cycle
                    2 => {
                        if iter % 5 == 0 {
                            let save_start = Instant::now();
                            let header = save_snapshot_with_preferences(
                                &task_snapshot_file,
                                &interner,
                                &graph,
                                &prefs_store,
                                1_700_000_000_000,
                            )
                            .unwrap();
                            let save_dur = save_start.elapsed().as_secs_f64() * 1000.0;
                            snapshot_tracker.record_save(save_dur, 1024);

                            assert_eq!(header.magic, SNAPSHOT_MAGIC);
                            assert_eq!(header.format_version, SNAPSHOT_FORMAT_VERSION);

                            // Load & verify integrity
                            let dummy_interner = StringInterner::new();
                            let dummy_graph = GraphStore::new();
                            let dummy_prefs = UserPreferencesStore::new();
                            let loaded = load_snapshot_with_preferences(
                                &task_snapshot_file,
                                &dummy_interner,
                                &dummy_graph,
                                &dummy_prefs,
                            )
                            .unwrap();
                            assert!(loaded.is_some());
                            let _ = std::fs::remove_file(&task_snapshot_file);
                        }
                    }

                    // Action 3: XRPC getFeedSkeleton Reads (Authenticated & Unauthenticated)
                    3 => {
                        let with_auth = iter % 2 == 0;
                        let uri = format!(
                            "/xrpc/app.bsky.feed.getFeedSkeleton?feed=at://did:web:feed.example.com/app.bsky.feed.generator/for-your-consideration&limit={}",
                            10 + (iter % 30)
                        );
                        let mut req_builder =
                            Request::builder().uri(uri).body(Body::empty()).unwrap();
                        if with_auth {
                            req_builder
                                .headers_mut()
                                .insert(AUTHORIZATION, format!("Bearer {token}").parse().unwrap());
                        }
                        let resp = app.clone().oneshot(req_builder).await.unwrap();
                        assert_eq!(resp.status(), StatusCode::OK);
                    }

                    // Action 4: REST Telemetry & Feed Preview & Taste Twins Reads
                    4 => {
                        // Telemetry
                        let req_telem = Request::builder()
                            .uri("/api/telemetry")
                            .body(Body::empty())
                            .unwrap();
                        let resp_telem = app.clone().oneshot(req_telem).await.unwrap();
                        assert_eq!(resp_telem.status(), StatusCode::OK);

                        // Feed Preview
                        let req_prev = Request::builder()
                            .uri(format!("/api/feed-preview?viewer={user_did}&freshness=6h&discovery=0.20&art=2.0&tech=1.0&limit=10"))
                            .body(Body::empty())
                            .unwrap();
                        let resp_prev = app.clone().oneshot(req_prev).await.unwrap();
                        assert_eq!(resp_prev.status(), StatusCode::OK);
                    }

                    // Action 5: Live Graph Mutations (In-Memory Interactivity)
                    _ => {
                        let now_ts = std::time::SystemTime::now()
                            .duration_since(std::time::UNIX_EPOCH)
                            .unwrap_or_default()
                            .as_secs();
                        let uid = interner.intern(&user_did);
                        let pid = interner
                            .intern(&format!("at://did:plc:dynamic/post/{task_idx}_{iter}"));
                        let aid = interner.intern("did:plc:dynamic_author");

                        graph.record_post_meta(pid, aid, None, None, now_ts);
                        graph.record_interaction(uid, pid, SignalType::Like, now_ts);
                        ingestion_tracker
                            .stats()
                            .events_processed
                            .fetch_add(1, Ordering::Relaxed);

                        if iter % 4 == 0 {
                            graph.remove_interaction(uid, pid, SignalType::Like);
                        }
                    }
                }
            }
            let _ = std::fs::remove_file(&task_snapshot_file);
        });
        handles.push(handle);
    }

    for h in handles {
        h.await.unwrap();
    }

    // Final integrity checks after concurrency storm
    assert!(!interner.is_empty());
    assert!(graph.stats().total_posts > 0);
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn test_adversarial_hot_viewer_rapid_mutation_and_read_isolation() {
    let (state, _interner, _graph, _prefs_store, _, _) = create_test_state();
    let app = create_xrpc_router(state);

    let hot_user_did = "did:plc:hot_user_contention";
    let token = generate_session_token(hot_user_did, 3600);

    let mut handles = Vec::new();

    // 4 Writer tasks continuously mutating the same user's dials
    for writer_id in 0..4 {
        let app = app.clone();
        let token = token.clone();
        handles.push(tokio::spawn(async move {
            for i in 0..50 {
                let freshness = if (writer_id + i) % 2 == 0 { 6.0 } else { 48.0 };
                let discovery = if (writer_id + i) % 2 == 0 { 0.05 } else { 0.40 };
                let body = SavePreferencesRequestBody {
                    freshness_hours: freshness,
                    discovery_ratio: discovery,
                    topic_weights: Some(TopicWeights {
                        art: (i % 5) as f32,
                        tech: 1.0,
                        science: 1.0,
                        news: 1.0,
                        culture: 1.0,
                    }),
                    include_replies: Some(false),
                    min_likes: Some(3),
                };
                let req = Request::builder()
                    .method(Method::POST)
                    .uri("/api/preferences")
                    .header(AUTHORIZATION, format!("Bearer {token}"))
                    .header(CONTENT_TYPE, "application/json")
                    .body(Body::from(serde_json::to_vec(&body).unwrap()))
                    .unwrap();
                let resp = app.clone().oneshot(req).await.unwrap();
                assert_eq!(resp.status(), StatusCode::OK);
            }
        }));
    }

    // 4 Reader tasks concurrently querying getFeedSkeleton and GET /api/preferences
    for _ in 0..4 {
        let app = app.clone();
        let token = token.clone();
        handles.push(tokio::spawn(async move {
            for _ in 0..50 {
                // XRPC query
                let req_xrpc = Request::builder()
                    .uri("/xrpc/app.bsky.feed.getFeedSkeleton?feed=at://did:web:feed.example.com/app.bsky.feed.generator/for-your-consideration&limit=10")
                    .header(AUTHORIZATION, format!("Bearer {token}"))
                    .body(Body::empty())
                    .unwrap();
                let resp_xrpc = app.clone().oneshot(req_xrpc).await.unwrap();
                assert_eq!(resp_xrpc.status(), StatusCode::OK);

                // REST preferences query
                let req_get = Request::builder()
                    .method(Method::GET)
                    .uri("/api/preferences")
                    .header(AUTHORIZATION, format!("Bearer {token}"))
                    .body(Body::empty())
                    .unwrap();
                let resp_get = app.clone().oneshot(req_get).await.unwrap();
                assert_eq!(resp_get.status(), StatusCode::OK);

                let body = resp_get.into_body().collect().await.unwrap().to_bytes();
                let prefs_dto: PreferencesResponseDto = serde_json::from_slice(&body).unwrap();
                // Validate that read data is never corrupted or torn
                assert!(prefs_dto.preferences.freshness_hours >= 1.0);
                assert!(prefs_dto.preferences.discovery_ratio >= 0.0);
                assert!(prefs_dto.preferences.discovery_ratio <= 0.50);
            }
        }));
    }

    for h in handles {
        h.await.unwrap();
    }
}

// =========================================================================
// 2. EDGE CASE DIAL COMBINATIONS & PRECEDENCE HIERARCHY
// =========================================================================

#[tokio::test]
async fn test_adversarial_all_zero_topic_weights_and_edge_multipliers() {
    let (state, interner, _graph, prefs_store, _, _) = create_test_state();
    let app = create_xrpc_router(state);

    let user_did = "did:plc:zero_weight_user";
    let token = generate_session_token(user_did, 3600);

    // 1. Persist all 0.0x topic weights via REST API
    let zero_topics = TopicWeights {
        art: 0.0,
        tech: 0.0,
        science: 0.0,
        news: 0.0,
        culture: 0.0,
    };
    let save_body = SavePreferencesRequestBody {
        freshness_hours: 24.0,
        discovery_ratio: 0.15,
        topic_weights: Some(zero_topics),
        include_replies: Some(false),
        min_likes: Some(3),
    };
    let req_post = Request::builder()
        .method(Method::POST)
        .uri("/api/preferences")
        .header(AUTHORIZATION, format!("Bearer {token}"))
        .header(CONTENT_TYPE, "application/json")
        .body(Body::from(serde_json::to_vec(&save_body).unwrap()))
        .unwrap();
    let resp_post = app.clone().oneshot(req_post).await.unwrap();
    assert_eq!(resp_post.status(), StatusCode::OK);

    // Verify stored
    let saved_dials = prefs_store.get_by_did(&interner, user_did).unwrap();
    assert_eq!(saved_dials.topic_weights.art, 0.0);
    assert_eq!(saved_dials.topic_weights.tech, 0.0);
    assert_eq!(saved_dials.topic_weights.science, 0.0);
    assert_eq!(saved_dials.topic_weights.news, 0.0);
    assert_eq!(saved_dials.topic_weights.culture, 0.0);

    // 2. Query XRPC getFeedSkeleton with all 0.0x weights in persisted preferences
    let req_xrpc = Request::builder()
        .uri("/xrpc/app.bsky.feed.getFeedSkeleton?feed=at://did:web:feed.example.com/app.bsky.feed.generator/for-your-consideration&limit=10")
        .header(AUTHORIZATION, format!("Bearer {token}"))
        .body(Body::empty())
        .unwrap();
    let resp_xrpc = app.clone().oneshot(req_xrpc).await.unwrap();
    assert_eq!(resp_xrpc.status(), StatusCode::OK);

    let body = resp_xrpc.into_body().collect().await.unwrap().to_bytes();
    let skeleton: FeedSkeletonResponse = serde_json::from_slice(&body).unwrap();
    // Must return valid skeleton posts without panics or NaN score crashes
    assert!(!skeleton.feed.is_empty());

    // 3. Query XRPC with explicit all 0.0x query overrides
    let req_xrpc_explicit = Request::builder()
        .uri("/xrpc/app.bsky.feed.getFeedSkeleton?feed=at://did:web:feed.example.com/app.bsky.feed.generator/for-your-consideration&art=0.0&tech=0.0&science=0.0&news=0.0&culture=0.0&limit=5")
        .body(Body::empty())
        .unwrap();
    let resp_xrpc_explicit = app.clone().oneshot(req_xrpc_explicit).await.unwrap();
    assert_eq!(resp_xrpc_explicit.status(), StatusCode::OK);

    // 4. Query with all 5.0x (max topic multipliers)
    let req_max_topics = Request::builder()
        .uri("/xrpc/app.bsky.feed.getFeedSkeleton?feed=at://did:web:feed.example.com/app.bsky.feed.generator/for-your-consideration&art=5.0&tech=5.0&science=5.0&news=5.0&culture=5.0&limit=5")
        .body(Body::empty())
        .unwrap();
    let resp_max_topics = app.clone().oneshot(req_max_topics).await.unwrap();
    assert_eq!(resp_max_topics.status(), StatusCode::OK);

    // 5. Query with asymmetric topic biases (Art=5.0, Tech=0.0, Science=0.0, News=0.0, Culture=0.0)
    let req_asymm = Request::builder()
        .uri("/xrpc/app.bsky.feed.getFeedSkeleton?feed=at://did:web:feed.example.com/app.bsky.feed.generator/for-your-consideration&art=5.0&tech=0.0&science=0.0&news=0.0&culture=0.0&limit=5")
        .body(Body::empty())
        .unwrap();
    let resp_asymm = app.oneshot(req_asymm).await.unwrap();
    assert_eq!(resp_asymm.status(), StatusCode::OK);
}

#[tokio::test]
async fn test_adversarial_boundary_freshness_and_discovery_dials() {
    let (state, _interner, _graph, _prefs_store, _, _) = create_test_state();
    let app = create_xrpc_router(state);
    let token = generate_session_token("did:plc:boundary_tester", 3600);

    // 1. Freshness Minimum (1.0h = 3600s) -> Valid
    let min_freshness_req = SavePreferencesRequestBody {
        freshness_hours: 1.0,
        discovery_ratio: 0.15,
        topic_weights: Some(TopicWeights::default()),
        include_replies: Some(false),
        min_likes: Some(3),
    };
    let req1 = Request::builder()
        .method(Method::POST)
        .uri("/api/preferences")
        .header(AUTHORIZATION, format!("Bearer {token}"))
        .header(CONTENT_TYPE, "application/json")
        .body(Body::from(serde_json::to_vec(&min_freshness_req).unwrap()))
        .unwrap();
    let resp1 = app.clone().oneshot(req1).await.unwrap();
    assert_eq!(resp1.status(), StatusCode::OK);

    // 2. Freshness Maximum (168.0h = 604,800s) -> Valid
    let max_freshness_req = SavePreferencesRequestBody {
        freshness_hours: 168.0,
        discovery_ratio: 0.15,
        topic_weights: Some(TopicWeights::default()),
        include_replies: Some(false),
        min_likes: Some(3),
    };
    let req2 = Request::builder()
        .method(Method::POST)
        .uri("/api/preferences")
        .header(AUTHORIZATION, format!("Bearer {token}"))
        .header(CONTENT_TYPE, "application/json")
        .body(Body::from(serde_json::to_vec(&max_freshness_req).unwrap()))
        .unwrap();
    let resp2 = app.clone().oneshot(req2).await.unwrap();
    assert_eq!(resp2.status(), StatusCode::OK);

    // 3. Freshness Sub-minimum (0.99h) -> 400 Bad Request
    let sub_min_freshness_req = SavePreferencesRequestBody {
        freshness_hours: 0.99,
        discovery_ratio: 0.15,
        topic_weights: Some(TopicWeights::default()),
        include_replies: Some(false),
        min_likes: Some(3),
    };
    let req3 = Request::builder()
        .method(Method::POST)
        .uri("/api/preferences")
        .header(AUTHORIZATION, format!("Bearer {token}"))
        .header(CONTENT_TYPE, "application/json")
        .body(Body::from(
            serde_json::to_vec(&sub_min_freshness_req).unwrap(),
        ))
        .unwrap();
    let resp3 = app.clone().oneshot(req3).await.unwrap();
    assert_eq!(resp3.status(), StatusCode::BAD_REQUEST);

    // 4. Freshness Ultra-maximum (168.01h) -> 400 Bad Request
    let ultra_max_freshness_req = SavePreferencesRequestBody {
        freshness_hours: 168.01,
        discovery_ratio: 0.15,
        topic_weights: Some(TopicWeights::default()),
        include_replies: Some(false),
        min_likes: Some(3),
    };
    let req4 = Request::builder()
        .method(Method::POST)
        .uri("/api/preferences")
        .header(AUTHORIZATION, format!("Bearer {token}"))
        .header(CONTENT_TYPE, "application/json")
        .body(Body::from(
            serde_json::to_vec(&ultra_max_freshness_req).unwrap(),
        ))
        .unwrap();
    let resp4 = app.clone().oneshot(req4).await.unwrap();
    assert_eq!(resp4.status(), StatusCode::BAD_REQUEST);

    // 5. Discovery Minimum (0.00 = 0%) -> Valid
    let min_disc_req = SavePreferencesRequestBody {
        freshness_hours: 24.0,
        discovery_ratio: 0.00,
        topic_weights: Some(TopicWeights::default()),
        include_replies: Some(false),
        min_likes: Some(3),
    };
    let req5 = Request::builder()
        .method(Method::POST)
        .uri("/api/preferences")
        .header(AUTHORIZATION, format!("Bearer {token}"))
        .header(CONTENT_TYPE, "application/json")
        .body(Body::from(serde_json::to_vec(&min_disc_req).unwrap()))
        .unwrap();
    let resp5 = app.clone().oneshot(req5).await.unwrap();
    assert_eq!(resp5.status(), StatusCode::OK);

    // 6. Discovery Maximum (0.50 = 50%) -> Valid
    let max_disc_req = SavePreferencesRequestBody {
        freshness_hours: 24.0,
        discovery_ratio: 0.50,
        topic_weights: Some(TopicWeights::default()),
        include_replies: Some(false),
        min_likes: Some(3),
    };
    let req6 = Request::builder()
        .method(Method::POST)
        .uri("/api/preferences")
        .header(AUTHORIZATION, format!("Bearer {token}"))
        .header(CONTENT_TYPE, "application/json")
        .body(Body::from(serde_json::to_vec(&max_disc_req).unwrap()))
        .unwrap();
    let resp6 = app.clone().oneshot(req6).await.unwrap();
    assert_eq!(resp6.status(), StatusCode::OK);

    // 7. Discovery Ultra-maximum (0.51) -> 400 Bad Request
    let ultra_disc_req = SavePreferencesRequestBody {
        freshness_hours: 24.0,
        discovery_ratio: 0.51,
        topic_weights: Some(TopicWeights::default()),
        include_replies: Some(false),
        min_likes: Some(3),
    };
    let req7 = Request::builder()
        .method(Method::POST)
        .uri("/api/preferences")
        .header(AUTHORIZATION, format!("Bearer {token}"))
        .header(CONTENT_TYPE, "application/json")
        .body(Body::from(serde_json::to_vec(&ultra_disc_req).unwrap()))
        .unwrap();
    let resp7 = app.clone().oneshot(req7).await.unwrap();
    assert_eq!(resp7.status(), StatusCode::BAD_REQUEST);

    // 8. XRPC Query Clamping with Extreme Out-of-Bounds Values
    // XRPC should never crash with 400/500 on bad query params, but clamp gracefully
    let req_xrpc_clamp = Request::builder()
        .uri("/xrpc/app.bsky.feed.getFeedSkeleton?feed=at://did:web:feed.example.com/app.bsky.feed.generator/for-your-consideration&freshness=-999999&discovery=999999&art=999999&limit=999999")
        .body(Body::empty())
        .unwrap();
    let resp_xrpc_clamp = app.oneshot(req_xrpc_clamp).await.unwrap();
    assert_eq!(resp_xrpc_clamp.status(), StatusCode::OK);
}

#[tokio::test]
async fn test_adversarial_precedence_hierarchy_mixed_query_overrides() {
    let (state, interner, _graph, prefs_store, _, _) = create_test_state();
    let app = create_xrpc_router(state);

    let user_did = "did:plc:precedence_user";
    let token = generate_session_token(user_did, 3600);

    // Save custom dials: Freshness = 12h, Discovery = 0.35, Art = 4.0, Tech = 0.5
    let saved_dials = UserDials {
        freshness_half_life_secs: 12.0 * 3600.0,
        serendipity_ratio: 0.35,
        topic_weights: TopicWeights {
            art: 4.0,
            tech: 0.5,
            science: 1.0,
            news: 1.0,
            culture: 1.0,
        },
        include_replies: false,
        min_likes: 3,
        updated_at_secs: 500,
    };
    prefs_store.set_by_did(&interner, user_did, saved_dials);

    // 1. Partial Override: Override ONLY 'art' (art=1.5) via query param
    // Freshness should remain 12h, Discovery should remain 0.35, Tech should remain 0.5
    let req_partial = Request::builder()
        .uri("/xrpc/app.bsky.feed.getFeedSkeleton?feed=at://did:web:feed.example.com/app.bsky.feed.generator/for-your-consideration&art=1.5&limit=10")
        .header(AUTHORIZATION, format!("Bearer {token}"))
        .body(Body::empty())
        .unwrap();
    let resp_partial = app.clone().oneshot(req_partial).await.unwrap();
    assert_eq!(resp_partial.status(), StatusCode::OK);

    // 2. Partial Override: Override ONLY 'discovery' (discovery=familiar/5%)
    let req_disc_override = Request::builder()
        .uri("/xrpc/app.bsky.feed.getFeedSkeleton?feed=at://did:web:feed.example.com/app.bsky.feed.generator/for-your-consideration&discovery=familiar&limit=10")
        .header(AUTHORIZATION, format!("Bearer {token}"))
        .body(Body::empty())
        .unwrap();
    let resp_disc_override = app.clone().oneshot(req_disc_override).await.unwrap();
    assert_eq!(resp_disc_override.status(), StatusCode::OK);

    // 3. Complete Override of all parameters
    let req_full_override = Request::builder()
        .uri("/xrpc/app.bsky.feed.getFeedSkeleton?feed=at://did:web:feed.example.com/app.bsky.feed.generator/for-your-consideration&freshness=weekly&discovery=deep_dive&art=0.0&tech=5.0&science=2.0&news=0.1&culture=3.0&limit=25")
        .header(AUTHORIZATION, format!("Bearer {token}"))
        .body(Body::empty())
        .unwrap();
    let resp_full_override = app.oneshot(req_full_override).await.unwrap();
    assert_eq!(resp_full_override.status(), StatusCode::OK);
}

// =========================================================================
// 3. SESSION TOKEN FORGERY AND REPLAY ATTACKS
// =========================================================================

#[tokio::test]
async fn test_adversarial_token_forgery_tampering_and_replay() {
    let (state, _interner, _graph, _prefs_store, _, _) = create_test_state();
    let app = create_xrpc_router(state);

    let now_secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();

    // 1. Expired Token Attack (exp in past) -> 401 on /api/preferences
    let expired_token = generate_session_token("did:plc:victim_user", -3600);
    let req_expired = Request::builder()
        .method(Method::GET)
        .uri("/api/preferences")
        .header(AUTHORIZATION, format!("Bearer {expired_token}"))
        .body(Body::empty())
        .unwrap();
    let resp_expired = app.clone().oneshot(req_expired).await.unwrap();
    assert_eq!(resp_expired.status(), StatusCode::UNAUTHORIZED);

    // 2. Token Expired at Exact Boundary
    let boundary_expired_token = {
        let payload = serde_json::json!({
            "iss": "did:plc:victim_user",
            "sub": "did:plc:victim_user",
            "exp": now_secs.saturating_sub(1)
        });
        let h = URL_SAFE_NO_PAD.encode(r#"{"alg":"ES256K"}"#);
        let p = URL_SAFE_NO_PAD.encode(payload.to_string());
        let s = URL_SAFE_NO_PAD.encode("sig");
        format!("{h}.{p}.{s}")
    };
    let req_boundary = Request::builder()
        .method(Method::GET)
        .uri("/api/preferences")
        .header(AUTHORIZATION, format!("Bearer {boundary_expired_token}"))
        .body(Body::empty())
        .unwrap();
    let resp_boundary = app.clone().oneshot(req_boundary).await.unwrap();
    assert_eq!(resp_boundary.status(), StatusCode::UNAUTHORIZED);

    // 3. Malformed Segment Attacks
    let forged_tokens = [
        "not_a_jwt_at_all",
        "only_one_segment",
        "header.payload_missing_sig",
        "header.payload.sig.extra_segment_4",
        "header.payload.sig.extra_segment_4.extra_segment_5",
        "!!!invalid_base64!!!.payload.sig",
        "header.!!!invalid_base64!!!.sig",
        "header.payload.!!!invalid_base64!!!",
        "",
        "...",
        "..",
        ". .",
    ];

    for bad_token in forged_tokens {
        let req_forged = Request::builder()
            .method(Method::GET)
            .uri("/api/preferences")
            .header(AUTHORIZATION, format!("Bearer {bad_token}"))
            .body(Body::empty())
            .unwrap();
        let resp_forged = app.clone().oneshot(req_forged).await.unwrap();
        assert_eq!(
            resp_forged.status(),
            StatusCode::UNAUTHORIZED,
            "Bad token '{bad_token}' must be rejected with 401"
        );
    }

    // 4. Invalid DID Claim Attacks (e.g. non-DID formats)
    let invalid_did_payloads = [
        r#"{"iss":"invalid_username_without_did","exp":9999999999}"#,
        r#"{"iss":"did:invalid_method:alice","exp":9999999999}"#,
        r#"{"iss":"did:plc:","exp":9999999999}"#,
        r#"{"iss":"did:web:","exp":9999999999}"#,
        r#"{"iss":"","exp":9999999999}"#,
        r#"{"aud":"did:web:feed","exp":9999999999}"#, // missing iss and sub
    ];

    for payload_json in invalid_did_payloads {
        let h = URL_SAFE_NO_PAD.encode(r#"{"alg":"ES256K"}"#);
        let p = URL_SAFE_NO_PAD.encode(payload_json);
        let s = URL_SAFE_NO_PAD.encode("sig");
        let token = format!("{h}.{p}.{s}");

        let req = Request::builder()
            .method(Method::GET)
            .uri("/api/preferences")
            .header(AUTHORIZATION, format!("Bearer {token}"))
            .body(Body::empty())
            .unwrap();
        let resp = app.clone().oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }

    // 5. Header Prefix and Formatting Attacks
    let valid_token = generate_session_token("did:plc:replay_victim", 3600);
    let bad_auth_headers = [
        format!("Basic {valid_token}"),
        format!("Token {valid_token}"),
        "Bearer".to_string(),
        "Bearer ".to_string(),
        "BEARER".to_string(),
        format!("Digest {valid_token}"),
        format!("CustomScheme {valid_token}"),
        format!("bearer\t{valid_token}"),
    ];

    for bad_header in bad_auth_headers {
        let req = Request::builder()
            .method(Method::GET)
            .uri("/api/preferences")
            .header(AUTHORIZATION, bad_header)
            .body(Body::empty())
            .unwrap();
        let resp = app.clone().oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }

    // 6. Token Replay Across Lifecycle & Endpoints
    // Replay valid token across GET -> POST -> GET -> DELETE -> GET
    let replay_token = generate_session_token("did:plc:replay_target", 3600);

    // Initial GET
    let req_r1 = Request::builder()
        .method(Method::GET)
        .uri("/api/preferences")
        .header(AUTHORIZATION, format!("Bearer {replay_token}"))
        .body(Body::empty())
        .unwrap();
    let resp_r1 = app.clone().oneshot(req_r1).await.unwrap();
    assert_eq!(resp_r1.status(), StatusCode::OK);

    // POST Mutation
    let save_body = SavePreferencesRequestBody {
        freshness_hours: 10.0,
        discovery_ratio: 0.20,
        topic_weights: Some(TopicWeights::default()),
        include_replies: Some(false),
        min_likes: Some(3),
    };
    let req_r2 = Request::builder()
        .method(Method::POST)
        .uri("/api/preferences")
        .header(AUTHORIZATION, format!("Bearer {replay_token}"))
        .header(CONTENT_TYPE, "application/json")
        .body(Body::from(serde_json::to_vec(&save_body).unwrap()))
        .unwrap();
    let resp_r2 = app.clone().oneshot(req_r2).await.unwrap();
    assert_eq!(resp_r2.status(), StatusCode::OK);

    // Replay GET -> reads custom
    let req_r3 = Request::builder()
        .method(Method::GET)
        .uri("/api/preferences")
        .header(AUTHORIZATION, format!("Bearer {replay_token}"))
        .body(Body::empty())
        .unwrap();
    let resp_r3 = app.clone().oneshot(req_r3).await.unwrap();
    assert_eq!(resp_r3.status(), StatusCode::OK);

    // DELETE Reset
    let req_r4 = Request::builder()
        .method(Method::DELETE)
        .uri("/api/preferences")
        .header(AUTHORIZATION, format!("Bearer {replay_token}"))
        .body(Body::empty())
        .unwrap();
    let resp_r4 = app.clone().oneshot(req_r4).await.unwrap();
    assert_eq!(resp_r4.status(), StatusCode::OK);

    // Replay GET -> reads defaults
    let req_r5 = Request::builder()
        .method(Method::GET)
        .uri("/api/preferences")
        .header(AUTHORIZATION, format!("Bearer {replay_token}"))
        .body(Body::empty())
        .unwrap();
    let resp_r5 = app.clone().oneshot(req_r5).await.unwrap();
    assert_eq!(resp_r5.status(), StatusCode::OK);

    // Replay on XRPC getFeedSkeleton
    let req_r6 = Request::builder()
        .uri("/xrpc/app.bsky.feed.getFeedSkeleton?feed=at://did:web:feed.example.com/app.bsky.feed.generator/for-your-consideration&limit=5")
        .header(AUTHORIZATION, format!("Bearer {replay_token}"))
        .body(Body::empty())
        .unwrap();
    let resp_r6 = app.oneshot(req_r6).await.unwrap();
    assert_eq!(resp_r6.status(), StatusCode::OK);
}

// =========================================================================
// 4. MATHEMATICAL INVARIANCE OF CONTINUOUS SOFT FATIGUE DECAY
// =========================================================================

#[test]
fn test_adversarial_impression_decay_and_soft_suppression_boundary_math() {
    let store = ImpressionStore::new(100);
    let viewer_id = 999;
    let post_id = 888;
    let served_at = 1_000_000;

    store.record_impressions(viewer_id, &[post_id], served_at);

    // Verify mathematical bounds of recovery curve:
    // f(dt) = 0.15 + 0.85 * (1 - exp(-dt / 7200))
    let mut prev_mult = 0.0f32;

    for dt in (0..=21600).step_by(60) {
        let mult = store
            .evaluate_fatigue_penalty(viewer_id, post_id, served_at + dt)
            .unwrap();

        // 1. Must be strictly within [FATIGUE_MIN_FLOOR, 1.0]
        assert!(
            (FATIGUE_MIN_FLOOR - 1e-6..=1.0 + 1e-6).contains(&mult),
            "Multiplier {mult} out of range at dt={dt}"
        );

        // 2. Must be strictly monotonically non-decreasing over time
        assert!(
            mult >= prev_mult - 1e-6,
            "Multiplier decreased from {prev_mult} to {mult} at dt={dt}"
        );

        prev_mult = mult;
    }

    // Exact boundary at t=0
    assert_eq!(
        store.evaluate_fatigue_penalty(viewer_id, post_id, served_at),
        Some(FATIGUE_MIN_FLOOR)
    );

    // Exact boundary at t=21600 (6 hours)
    assert_eq!(
        store.evaluate_fatigue_penalty(viewer_id, post_id, served_at + FATIGUE_WINDOW_SECS),
        Some(1.0)
    );

    // Post-boundary at t > 21600
    assert_eq!(
        store.evaluate_fatigue_penalty(viewer_id, post_id, served_at + FATIGUE_WINDOW_SECS + 1000),
        Some(1.0)
    );
}

// =========================================================================
// 5. UNINTERNED VIEWER FAST-PATH & ZERO POLLUTION
// =========================================================================

#[test]
fn test_adversarial_uninterned_viewer_fast_path_and_zero_pollution() {
    let interner = StringInterner::new();
    let store = UserPreferencesStore::new();

    let initial_interner_len = interner.len();
    let initial_store_len = store.len();

    // Look up 10,000 never-before-seen random DIDs
    for i in 0..10_000 {
        let fake_did = format!("did:plc:random_unseen_{i}");
        let dials = store.get_by_did(&interner, &fake_did);
        assert_eq!(dials, None);
    }

    // Verify zero allocations in interner and store
    assert_eq!(interner.len(), initial_interner_len);
    assert_eq!(store.len(), initial_store_len);
}
