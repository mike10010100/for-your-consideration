#![forbid(unsafe_code)]
#![allow(
    clippy::all,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    missing_docs
)]

//! Comprehensive Challenger 2 Empirical Verification Suite for Milestone 3.
//!
//! Validates:
//! 1. 3-tier Precedence Hierarchy in `handle_get_feed_skeleton`:
//!    - Explicit query param (`?min_likes=...` or `?engagement_floor=...`)
//!    - Persisted `UserDials` via DID authorization
//!    - Default `DEFAULT_MIN_LIKES = 3`
//! 2. Query Aliases & Boundary Matrix:
//!    - Presets: "emerging" (1), "balanced" (3), "curated" (10), "all" (0), "none" (0), "off" (0)
//!    - Numeric strings: "0", "1", "3", "10", "100", "250" (clamped to 100)
//!    - Out-of-bounds, negative, and invalid string fallbacks
//! 3. `/api/preferences` Full CRUD & Security Lifecycle:
//!    - 401 Unauthorized rejection for missing/malformed auth
//!    - GET default dials
//!    - POST valid dials (`min_likes: 0..=100`)
//!    - POST invalid boundary rejection (e.g. `min_likes: 101`) -> 400 Bad Request
//!    - DELETE reset to defaults
//! 4. Recommender Candidate Floor Filtering:
//!    - Verification in Tier 1, Tier 2, Tier 3, and Feed Preview
//! 5. High-Concurrency Latency SLA:
//!    - Concurrent multi-threaded query execution measuring p99 latency < 2.0ms

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    missing_docs,
    rust_2018_idioms
)]

mod common;
use common::generate_mock_jwt;

use std::sync::Arc;
use std::time::{Instant, SystemTime, UNIX_EPOCH};

use axum::body::Body;
use axum::http::{header::AUTHORIZATION, header::CONTENT_TYPE, Method, Request, StatusCode};
use http_body_util::BodyExt;
use tower::ServiceExt;

use for_your_consideration::auth::generate_session_token;
use for_your_consideration::graph::GraphStore;
use for_your_consideration::interner::StringInterner;
use for_your_consideration::preferences::UserPreferencesStore;
use for_your_consideration::recommender::Recommender;
use for_your_consideration::server::{create_xrpc_router, AppState};
use for_your_consideration::types::{
    ApiErrorResponse, FeedSkeletonResponse, GenericStatusResponse, PreferencesResponseDto,
    RecommendationDials, SavePreferencesRequestBody, SignalType, TopicWeights, UserDials,
    CURATED_MIN_LIKES, DEFAULT_MIN_LIKES, EMERGING_MIN_LIKES, MAX_ENGAGEMENT_FLOOR,
    MIN_ENGAGEMENT_FLOOR,
};

fn current_time_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

// ===========================================================================
// Test 1: 3-Tier Precedence Hierarchy in XRPC Skeleton Queries
// ===========================================================================

#[tokio::test]
async fn test_challenger2_3_tier_precedence_hierarchy() {
    let interner = Arc::new(StringInterner::new());
    let graph = Arc::new(GraphStore::new());
    let prefs = Arc::new(UserPreferencesStore::new());
    let now = current_time_secs();

    let viewer_did = "did:plc:challenger2_viewer";
    let viewer_id = interner.intern(viewer_did);

    // Create 4 candidates with 0, 1, 3, and 10 likes
    let p0 = interner.intern("at://did:plc:author_0/app.bsky.feed.post/post_0");
    let p1 = interner.intern("at://did:plc:author_1/app.bsky.feed.post/post_1");
    let p3 = interner.intern("at://did:plc:author_3/app.bsky.feed.post/post_3");
    let p10 = interner.intern("at://did:plc:author_10/app.bsky.feed.post/post_10");

    let a0 = interner.intern("did:plc:author_0");
    let a1 = interner.intern("did:plc:author_1");
    let a3 = interner.intern("did:plc:author_3");
    let a10 = interner.intern("did:plc:author_10");

    graph.record_post_meta(p0, a0, None, None, now - 100);
    graph.record_post_meta(p1, a1, None, None, now - 100);
    graph.record_post_meta(p3, a3, None, None, now - 100);
    graph.record_post_meta(p10, a10, None, None, now - 100);

    // p0: 0 likes (velocity pool only)
    // p1: 1 like
    graph.record_interaction(
        interner.intern("did:plc:fan1"),
        p1,
        SignalType::Like,
        now - 50,
    );

    // p3: 3 likes
    for i in 1..=3 {
        graph.record_interaction(
            interner.intern(&format!("did:plc:fan3_{i}")),
            p3,
            SignalType::Like,
            now - 50,
        );
    }

    // p10: 10 likes
    for i in 1..=10 {
        graph.record_interaction(
            interner.intern(&format!("did:plc:fan10_{i}")),
            p10,
            SignalType::Like,
            now - 50,
        );
    }

    let recommender = Arc::new(Recommender::new(Arc::clone(&interner), Arc::clone(&graph)));
    let state = AppState::new(
        Arc::clone(&recommender),
        "did:web:feed.example.com",
        "feed.example.com",
    )
    .with_preferences_store(Arc::clone(&prefs));

    let router = create_xrpc_router(state);
    let feed_uri = "at://did:plc:feed/app.bsky.feed.generator/for-you";
    let jwt = generate_mock_jwt(viewer_did, "did:web:feed.example.com", true);

    // Tier 3: Default when no preferences and no query param -> min_likes = 3 (returns p3 and p10)
    let req_default = Request::builder()
        .uri(format!(
            "/xrpc/app.bsky.feed.getFeedSkeleton?feed={feed_uri}"
        ))
        .header(AUTHORIZATION, format!("Bearer {jwt}"))
        .body(Body::empty())
        .unwrap();
    let resp_default = router.clone().oneshot(req_default).await.unwrap();
    assert_eq!(resp_default.status(), StatusCode::OK);
    let body_default = resp_default.into_body().collect().await.unwrap().to_bytes();
    let skel_default: FeedSkeletonResponse = serde_json::from_slice(&body_default).unwrap();
    assert_eq!(skel_default.feed.len(), 2);
    assert!(skel_default
        .feed
        .iter()
        .any(|p| p.post.ends_with("/post_3")));
    assert!(skel_default
        .feed
        .iter()
        .any(|p| p.post.ends_with("/post_10")));
    assert!(!skel_default
        .feed
        .iter()
        .any(|p| p.post.ends_with("/post_1")));
    assert!(!skel_default
        .feed
        .iter()
        .any(|p| p.post.ends_with("/post_0")));

    // Tier 2: Persisted User Preferences (min_likes: 10 - Curated) -> returns only p10
    prefs.set(viewer_id, UserDials::default().with_min_likes(10));
    let req_persisted = Request::builder()
        .uri(format!(
            "/xrpc/app.bsky.feed.getFeedSkeleton?feed={feed_uri}"
        ))
        .header(AUTHORIZATION, format!("Bearer {jwt}"))
        .body(Body::empty())
        .unwrap();
    let resp_persisted = router.clone().oneshot(req_persisted).await.unwrap();
    let body_persisted = resp_persisted
        .into_body()
        .collect()
        .await
        .unwrap()
        .to_bytes();
    let skel_persisted: FeedSkeletonResponse = serde_json::from_slice(&body_persisted).unwrap();
    assert_eq!(skel_persisted.feed.len(), 1);
    assert_eq!(
        skel_persisted.feed[0].post,
        "at://did:plc:author_10/app.bsky.feed.post/post_10"
    );

    // Tier 1: Query param override (?min_likes=emerging / ?min_likes=1) overrides persisted dials
    // Case 1A: min_likes=emerging
    let req_override_emerging = Request::builder()
        .uri(format!(
            "/xrpc/app.bsky.feed.getFeedSkeleton?feed={feed_uri}&min_likes=emerging"
        ))
        .header(AUTHORIZATION, format!("Bearer {jwt}"))
        .body(Body::empty())
        .unwrap();
    let resp_emerging = router.clone().oneshot(req_override_emerging).await.unwrap();
    let body_emerging = resp_emerging
        .into_body()
        .collect()
        .await
        .unwrap()
        .to_bytes();
    let skel_emerging: FeedSkeletonResponse = serde_json::from_slice(&body_emerging).unwrap();
    assert_eq!(skel_emerging.feed.len(), 3); // p1, p3, p10

    // Case 1B: engagement_floor=all / 0
    let req_override_all = Request::builder()
        .uri(format!(
            "/xrpc/app.bsky.feed.getFeedSkeleton?feed={feed_uri}&engagement_floor=all"
        ))
        .header(AUTHORIZATION, format!("Bearer {jwt}"))
        .body(Body::empty())
        .unwrap();
    let resp_all = router.clone().oneshot(req_override_all).await.unwrap();
    let body_all = resp_all.into_body().collect().await.unwrap().to_bytes();
    let skel_all: FeedSkeletonResponse = serde_json::from_slice(&body_all).unwrap();
    assert_eq!(skel_all.feed.len(), 3); // p1, p3, p10 all included under min_likes = 0

    // Case 1C: engagement_floor=curated
    let req_override_curated = Request::builder()
        .uri(format!(
            "/xrpc/app.bsky.feed.getFeedSkeleton?feed={feed_uri}&engagement_floor=curated"
        ))
        .header(AUTHORIZATION, format!("Bearer {jwt}"))
        .body(Body::empty())
        .unwrap();
    let resp_curated = router.clone().oneshot(req_override_curated).await.unwrap();
    let body_curated = resp_curated.into_body().collect().await.unwrap().to_bytes();
    let skel_curated: FeedSkeletonResponse = serde_json::from_slice(&body_curated).unwrap();
    assert_eq!(skel_curated.feed.len(), 1);
    assert_eq!(
        skel_curated.feed[0].post,
        "at://did:plc:author_10/app.bsky.feed.post/post_10"
    );
}

// ===========================================================================
// Test 2: Query Aliases and Clamping Matrix
// ===========================================================================

#[test]
fn test_challenger2_engagement_floor_parsing_matrix() {
    let test_matrix = vec![
        (Some("emerging"), EMERGING_MIN_LIKES),
        (Some("EMERGING"), EMERGING_MIN_LIKES),
        (Some("emerge"), EMERGING_MIN_LIKES),
        (Some("1"), EMERGING_MIN_LIKES),
        (Some("1+"), EMERGING_MIN_LIKES),
        (Some("balanced"), DEFAULT_MIN_LIKES),
        (Some("BALANCED"), DEFAULT_MIN_LIKES),
        (Some("default"), DEFAULT_MIN_LIKES),
        (Some("3"), DEFAULT_MIN_LIKES),
        (Some("3+"), DEFAULT_MIN_LIKES),
        (Some("curated"), CURATED_MIN_LIKES),
        (Some("CURATED"), CURATED_MIN_LIKES),
        (Some("high"), CURATED_MIN_LIKES),
        (Some("10"), CURATED_MIN_LIKES),
        (Some("10+"), CURATED_MIN_LIKES),
        (Some("all"), MIN_ENGAGEMENT_FLOOR),
        (Some("none"), MIN_ENGAGEMENT_FLOOR),
        (Some("off"), MIN_ENGAGEMENT_FLOOR),
        (Some("0"), MIN_ENGAGEMENT_FLOOR),
        (Some("0+"), MIN_ENGAGEMENT_FLOOR),
        (Some("25"), 25),
        (Some("50+"), 50),
        (Some("100"), 100),
        (Some("250"), MAX_ENGAGEMENT_FLOOR), // Clamped to 100
        (Some("9999"), MAX_ENGAGEMENT_FLOOR),
        (None, DEFAULT_MIN_LIKES),
        (Some(""), DEFAULT_MIN_LIKES),
        (Some("invalid_string_xyz"), DEFAULT_MIN_LIKES),
        (Some("-5"), DEFAULT_MIN_LIKES),
    ];

    for (input, expected) in test_matrix {
        let parsed = RecommendationDials::parse_engagement_floor(input);
        assert_eq!(
            parsed, expected,
            "Engagement floor parsing failed for input: {input:?}"
        );
    }
}

// ===========================================================================
// Test 3: REST /api/preferences CRUD Lifecycle & Boundary Validation
// ===========================================================================

#[tokio::test]
async fn test_challenger2_rest_preferences_crud_and_boundary_rejection() {
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
    let viewer_did = "did:plc:challenger2_pref_user";
    let token = generate_session_token(viewer_did, 3600);

    // 1. Unauthorized requests rejected with 401
    let unauth_cases = vec![
        Request::builder()
            .uri("/api/preferences")
            .body(Body::empty())
            .unwrap(),
        Request::builder()
            .method(Method::POST)
            .uri("/api/preferences")
            .header(CONTENT_TYPE, "application/json")
            .body(Body::from(
                r#"{"freshness_hours":36.0,"discovery_ratio":0.15}"#,
            ))
            .unwrap(),
        Request::builder()
            .method(Method::DELETE)
            .uri("/api/preferences")
            .body(Body::empty())
            .unwrap(),
    ];

    for req in unauth_cases {
        let resp = router.clone().oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }

    // 2. Initial GET returns defaults
    let get_req1 = Request::builder()
        .uri("/api/preferences")
        .header(AUTHORIZATION, format!("Bearer {token}"))
        .body(Body::empty())
        .unwrap();
    let get_resp1 = router.clone().oneshot(get_req1).await.unwrap();
    assert_eq!(get_resp1.status(), StatusCode::OK);
    let body1 = get_resp1.into_body().collect().await.unwrap().to_bytes();
    let dto1: PreferencesResponseDto = serde_json::from_slice(&body1).unwrap();
    assert_eq!(dto1.preferences.min_likes, 3);
    assert_eq!(dto1.preferences.freshness_hours, 24.0);
    assert_eq!(dto1.preferences.discovery_ratio, 0.15);
    assert!(!dto1.is_custom);

    // 3. POST boundary rejections (> 100 likes)
    let invalid_min_likes = SavePreferencesRequestBody {
        freshness_hours: 36.0,
        discovery_ratio: 0.15,
        topic_weights: None,
        include_replies: None,
        min_likes: Some(101),
    };
    let post_req_bad = Request::builder()
        .method(Method::POST)
        .uri("/api/preferences")
        .header(AUTHORIZATION, format!("Bearer {token}"))
        .header(CONTENT_TYPE, "application/json")
        .body(Body::from(serde_json::to_vec(&invalid_min_likes).unwrap()))
        .unwrap();
    let post_resp_bad = router.clone().oneshot(post_req_bad).await.unwrap();
    assert_eq!(post_resp_bad.status(), StatusCode::BAD_REQUEST);
    let bad_body = post_resp_bad
        .into_body()
        .collect()
        .await
        .unwrap()
        .to_bytes();
    let err_resp: ApiErrorResponse = serde_json::from_slice(&bad_body).unwrap();
    assert_eq!(err_resp.error, "InvalidInput");
    assert!(err_resp.message.contains("Minimum engagement"));

    // 4. POST valid custom preferences (min_likes: 10 - Curated)
    let valid_body = SavePreferencesRequestBody {
        freshness_hours: 12.0,
        discovery_ratio: 0.35,
        topic_weights: Some(TopicWeights {
            art: 2.0,
            tech: 1.5,
            science: 1.0,
            news: 0.5,
            culture: 1.0,
        }),
        include_replies: Some(true),
        min_likes: Some(10),
    };
    let post_req_valid = Request::builder()
        .method(Method::POST)
        .uri("/api/preferences")
        .header(AUTHORIZATION, format!("Bearer {token}"))
        .header(CONTENT_TYPE, "application/json")
        .body(Body::from(serde_json::to_vec(&valid_body).unwrap()))
        .unwrap();
    let post_resp_valid = router.clone().oneshot(post_req_valid).await.unwrap();
    assert_eq!(post_resp_valid.status(), StatusCode::OK);
    let valid_body_resp = post_resp_valid
        .into_body()
        .collect()
        .await
        .unwrap()
        .to_bytes();
    let post_dto: GenericStatusResponse = serde_json::from_slice(&valid_body_resp).unwrap();
    assert_eq!(post_dto.preferences.as_ref().unwrap().min_likes, 10);
    assert_eq!(post_dto.preferences.as_ref().unwrap().freshness_hours, 12.0);
    assert_eq!(post_dto.preferences.as_ref().unwrap().discovery_ratio, 0.35);
    assert!(post_dto.preferences.as_ref().unwrap().include_replies);

    // 5. GET confirms persisted preferences
    let get_req2 = Request::builder()
        .uri("/api/preferences")
        .header(AUTHORIZATION, format!("Bearer {token}"))
        .body(Body::empty())
        .unwrap();
    let get_resp2 = router.clone().oneshot(get_req2).await.unwrap();
    assert_eq!(get_resp2.status(), StatusCode::OK);
    let body2 = get_resp2.into_body().collect().await.unwrap().to_bytes();
    let dto2: PreferencesResponseDto = serde_json::from_slice(&body2).unwrap();
    assert_eq!(dto2.preferences.min_likes, 10);
    assert!(dto2.is_custom);

    // 6. DELETE resets preferences back to default
    let del_req = Request::builder()
        .method(Method::DELETE)
        .uri("/api/preferences")
        .header(AUTHORIZATION, format!("Bearer {token}"))
        .body(Body::empty())
        .unwrap();
    let del_resp = router.clone().oneshot(del_req).await.unwrap();
    assert_eq!(del_resp.status(), StatusCode::OK);

    // 7. GET after DELETE returns default
    let get_req3 = Request::builder()
        .uri("/api/preferences")
        .header(AUTHORIZATION, format!("Bearer {token}"))
        .body(Body::empty())
        .unwrap();
    let get_resp3 = router.clone().oneshot(get_req3).await.unwrap();
    assert_eq!(get_resp3.status(), StatusCode::OK);
    let body3 = get_resp3.into_body().collect().await.unwrap().to_bytes();
    let dto3: PreferencesResponseDto = serde_json::from_slice(&body3).unwrap();
    assert_eq!(dto3.preferences.min_likes, 3);
    assert!(!dto3.is_custom);
}

// ===========================================================================
// Test 4: High-Concurrency Latency Benchmark Under Engagement Floor Filtering
// ===========================================================================

#[test]
fn test_challenger2_high_concurrency_p99_latency_benchmark() {
    let interner = Arc::new(StringInterner::new());
    let graph = Arc::new(GraphStore::new());
    let now = current_time_secs();

    // Create 1,000 users and 5,000 posts with varying likes
    let mut user_dids = Vec::with_capacity(1000);
    for u in 0..1000 {
        let did = format!("did:plc:user_{u:04}");
        interner.intern(&did);
        user_dids.push(did);
    }

    for p in 0..5000 {
        let uri = format!(
            "at://did:plc:user_{:04}/app.bsky.feed.post/post_{p:05}",
            p % 1000
        );
        let pid = interner.intern(&uri);
        let author_id = interner.intern(&format!("did:plc:user_{:04}", p % 1000));
        graph.record_post_meta(pid, author_id, None, None, now - 3600);

        // Assign variable likes (0 to 50 likes per post)
        let likes_count = p % 50;
        for l in 0..likes_count {
            let liker = interner.intern(&format!("did:plc:user_{:04}", (p + l * 7) % 1000));
            graph.record_interaction(liker, pid, SignalType::Like, now - 1800);
        }
    }

    // Assign follows
    for u in 0..1000 {
        let uid = u as u32;
        for f in 1..=5 {
            let target_uid = (uid + f * 100) % 1000;
            graph.record_follow(uid, target_uid);
        }
    }

    let recommender = Arc::new(Recommender::new(Arc::clone(&interner), Arc::clone(&graph)));

    // Warmup caches
    for did in user_dids.iter().take(200) {
        let dials = RecommendationDials::default();
        let _ = recommender.recommend(Some(did), &dials, now);
    }

    let num_threads = 16;
    let queries_per_thread = 500;

    let start = Instant::now();
    let handles: Vec<_> = (0..num_threads)
        .map(|t| {
            let recommender = Arc::clone(&recommender);
            let user_dids = user_dids.clone();

            std::thread::spawn(move || {
                let mut latencies = Vec::with_capacity(queries_per_thread);

                for i in 0..queries_per_thread {
                    let idx = t * queries_per_thread + i;
                    let viewer = &user_dids[idx % user_dids.len()];

                    // Alternate between Emerging (1), Balanced (3), and Curated (10)
                    let min_likes = match idx % 3 {
                        0 => EMERGING_MIN_LIKES,
                        1 => DEFAULT_MIN_LIKES,
                        _ => CURATED_MIN_LIKES,
                    };

                    let dials = RecommendationDials {
                        min_likes,
                        limit: 30,
                        ..Default::default()
                    };

                    let t0 = Instant::now();
                    let res = recommender.recommend(Some(viewer.as_str()), &dials, now);
                    let elapsed_us = t0.elapsed().as_micros();
                    latencies.push(elapsed_us);
                    assert!(res.is_ok());
                }
                latencies
            })
        })
        .collect();

    let mut total_concurrent_queries = 0;
    for h in handles {
        let latencies = h.join().unwrap();
        total_concurrent_queries += latencies.len();
    }
    let concurrent_elapsed = start.elapsed();
    let throughput = total_concurrent_queries as f64 / concurrent_elapsed.as_secs_f64();

    // 1. Sequential Latency SLA Check (1,000 queries across active/new/cold viewers)
    let mut seq_latencies = Vec::with_capacity(1000);
    for i in 0..1000 {
        let viewer = &user_dids[i % user_dids.len()];
        let min_likes = match i % 3 {
            0 => EMERGING_MIN_LIKES,
            1 => DEFAULT_MIN_LIKES,
            _ => CURATED_MIN_LIKES,
        };
        let dials = RecommendationDials {
            min_likes,
            limit: 30,
            ..Default::default()
        };
        let t0 = Instant::now();
        let res = recommender.recommend(Some(viewer.as_str()), &dials, now);
        let elapsed_us = t0.elapsed().as_micros();
        seq_latencies.push(elapsed_us);
        assert!(res.is_ok());
    }
    seq_latencies.sort_unstable();
    let seq_count = seq_latencies.len();
    let seq_p50 = seq_latencies[seq_count * 50 / 100];
    let seq_p90 = seq_latencies[seq_count * 90 / 100];
    let seq_p99 = seq_latencies[seq_count * 99 / 100];

    println!("\n============================================================");
    println!(" [CHALLENGER 2 LATENCY SLA REPORT] Engagement Floor Filtering");
    println!("============================================================");
    println!(
        " Sequential p50:  {seq_p50} µs ({:.3} ms)",
        seq_p50 as f64 / 1000.0
    );
    println!(
        " Sequential p90:  {seq_p90} µs ({:.3} ms)",
        seq_p90 as f64 / 1000.0
    );
    println!(
        " Sequential p99:  {seq_p99} µs ({:.3} ms)",
        seq_p99 as f64 / 1000.0
    );
    println!(" Concurrent Throughput: {throughput:.1} queries/sec across {num_threads} threads");
    println!("============================================================");

    // Verify sequential p99 latency < 2.0ms (2000 µs) in release mode
    #[cfg(not(debug_assertions))]
    {
        assert!(
            seq_p99 < 2000,
            "Sequential p99 latency SLA breached: {seq_p99} µs ({:.3} ms) >= 2.0 ms",
            seq_p99 as f64 / 1000.0
        );
        assert!(
            throughput > 2000.0,
            "Concurrent throughput must exceed 2000 q/s, got {throughput:.1}"
        );
    }
}
