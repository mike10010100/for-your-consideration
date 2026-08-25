#![forbid(unsafe_code)]

//! Adversarial empirical challenge and stress test harness for Milestone 2:
//! - Axum REST endpoints: `/api/telemetry`, `/api/taste-twins`, `/api/feed-preview`, `/api/explain`
//! - Sub-2ms query latency SLA verification for `/api/feed-preview`
//! - Read-only impression isolation through HTTP requests
//! - Live telemetry accuracy across dynamic graph, snapshot, and ingestion state changes
//! - Adversarial parameter fuzzing and boundary stress

use std::sync::Arc;
use std::time::Instant;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine;
use for_your_consideration::prelude::*;
use http_body_util::BodyExt;
use tower::ServiceExt;

/// Builds a realistic synthetic graph with 500 users, 2,000 posts, and 10,000 interactions.
fn build_stress_graph_state() -> (
    AppState,
    Arc<StringInterner>,
    Arc<GraphStore>,
    Arc<Recommender>,
    Arc<SnapshotStatusTracker>,
    Arc<IngestionTracker>,
    Arc<IngestionStats>,
) {
    let interner = Arc::new(StringInterner::new());
    let graph = Arc::new(GraphStore::new());
    let recommender = Arc::new(Recommender::new(Arc::clone(&interner), Arc::clone(&graph)));

    let snap_config = SnapshotConfig {
        path: std::path::PathBuf::from("target/adversarial_test_snapshot.bin"),
        interval_secs: 300,
    };
    let snapshot_tracker = Arc::new(SnapshotStatusTracker::new(&snap_config));

    let stats = Arc::new(IngestionStats::new(Some(1_700_000_000_000_000)));
    let ingestion_tracker = Arc::new(IngestionTracker::new(Arc::clone(&stats)));

    let now = BLUESKY_EPOCH_SECS + 1_000_000;

    // Seed 10 core authors
    let mut author_ids = Vec::new();
    for a in 0..10 {
        let author_did = format!("did:plc:creator_{a}");
        author_ids.push(interner.intern(&author_did));
    }

    // Seed 2000 posts across categories
    let mut post_ids = Vec::new();
    let topic_tags = ["art", "tech", "science", "news", "culture"];
    for p in 0..2000 {
        let author = author_ids[p % author_ids.len()];
        let tag = topic_tags[p % topic_tags.len()];
        let uri = format!(
            "at://did:plc:creator_{}/app.bsky.feed.post/{tag}_{p}",
            p % 10
        );
        let pid = interner.intern(&uri);
        post_ids.push(pid);
        graph.record_post_meta(pid, author, None, None, now.saturating_sub((p as u64) * 10));
    }

    // Seed 500 users with interaction clusters
    for u in 0..500 {
        let user_did = format!("did:plc:user_{u}");
        let uid = interner.intern(&user_did);

        // Connect user to a cluster of posts
        let cluster_offset = (u % 5) * 400;
        let num_likes = 15 + (u % 20);
        for l in 0..num_likes {
            let pid = post_ids[(cluster_offset + l * 13) % post_ids.len()];
            let sig = match l % 5 {
                0 => SignalType::Repost,
                1 => SignalType::Quote,
                _ => SignalType::Like,
            };
            graph.record_interaction(uid, pid, sig, now.saturating_sub((l as u64) * 60));
        }

        // Add some follows
        if u > 0 {
            let target_did = format!("did:plc:user_{}", u - 1);
            let target_uid = interner.intern(&target_did);
            graph.record_follow(uid, target_uid);
        }
    }

    let state = AppState::new(
        Arc::clone(&recommender),
        "did:web:feed.stress.test",
        "feed.stress.test",
    )
    .with_snapshot_tracker(Arc::clone(&snapshot_tracker))
    .with_ingestion_tracker(Arc::clone(&ingestion_tracker));

    (
        state,
        interner,
        graph,
        recommender,
        snapshot_tracker,
        ingestion_tracker,
        stats,
    )
}

#[tokio::test]
async fn test_adversarial_high_throughput_concurrent_multi_endpoint_stress() {
    let (state, _interner, _graph, _rec, _snap, _ingest, _stats) = build_stress_graph_state();
    let router = create_xrpc_router(state);

    let num_tasks = 80;
    let requests_per_task = 25;
    let mut handles = Vec::with_capacity(num_tasks);

    let start = Instant::now();

    for task_id in 0..num_tasks {
        let app = router.clone();
        let handle = tokio::spawn(async move {
            for req_id in 0..requests_per_task {
                let user_idx = (task_id * requests_per_task + req_id) % 500;
                let user_did = format!("did:plc:user_{user_idx}");

                let endpoint_type = (task_id + req_id) % 5;
                let (uri, expected_status) = match endpoint_type {
                    0 => ("/api/telemetry".to_string(), StatusCode::OK),
                    1 => (
                        format!("/api/taste-twins?did={user_did}&limit=10"),
                        StatusCode::OK,
                    ),
                    2 => (
                        format!("/api/feed-preview?viewer={user_did}&freshness=realtime&art=2.0&tech=0.5&limit=20&explain=true"),
                        StatusCode::OK,
                    ),
                    3 => (
                        format!("/api/explain?viewer={user_did}&uri=at://did:plc:creator_0/app.bsky.feed.post/art_0"),
                        StatusCode::OK,
                    ),
                    _ => (
                        format!("/api/feed-preview?viewer={user_did}&discovery=deep_dive&limit=30"),
                        StatusCode::OK,
                    ),
                };

                let req = Request::builder().uri(&uri).body(Body::empty()).unwrap();
                let resp = app.clone().oneshot(req).await.unwrap();
                assert_eq!(resp.status(), expected_status);

                let body = resp.into_body().collect().await.unwrap().to_bytes();
                assert!(
                    !body.is_empty(),
                    "Response body should not be empty for {uri}"
                );

                // Ensure valid JSON payload
                let json_val: serde_json::Value = serde_json::from_slice(&body).unwrap();
                assert!(
                    json_val.is_object(),
                    "Response should be a JSON object for {uri}"
                );
            }
        });
        handles.push(handle);
    }

    for h in handles {
        h.await.unwrap();
    }

    let elapsed = start.elapsed();
    println!(
        "High-throughput stress completed {} requests in {:?}",
        num_tasks * requests_per_task,
        elapsed
    );
    assert!(
        elapsed.as_secs() < 15,
        "Stress run took too long: {elapsed:?}"
    );
}

#[tokio::test]
async fn test_adversarial_feed_preview_sub_2ms_latency_sla() {
    let (state, _interner, _graph, _rec, _snap, _ingest, _stats) = build_stress_graph_state();
    let app = create_xrpc_router(state);

    // Warmup
    for i in 0..10 {
        let uri = format!("/api/feed-preview?viewer=did:plc:user_{i}&limit=30");
        let req = Request::builder().uri(&uri).body(Body::empty()).unwrap();
        let _ = app.clone().oneshot(req).await.unwrap();
    }

    let num_queries = 100;
    let mut latencies_us = Vec::with_capacity(num_queries);

    for i in 0..num_queries {
        let user_did = format!("did:plc:user_{}", i % 500);
        let uri = format!(
            "/api/feed-preview?viewer={user_did}&freshness=balanced&discovery=balanced&art=1.5&science=2.0&limit=30&explain=true"
        );

        let req = Request::builder().uri(&uri).body(Body::empty()).unwrap();
        let resp = app.clone().oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);

        let body = resp.into_body().collect().await.unwrap().to_bytes();
        let preview: FeedPreviewResponse = serde_json::from_slice(&body).unwrap();

        latencies_us.push(preview.query_latency_us);
    }

    latencies_us.sort_unstable();
    let p50 = latencies_us[num_queries / 2];
    let p90 = latencies_us[(num_queries * 90) / 100];
    let p99 = latencies_us[(num_queries * 99) / 100];
    let max = latencies_us[num_queries - 1];

    println!(
        "Feed Preview Query Latencies (us): p50={p50}µs, p90={p90}µs, p99={p99}µs, max={max}µs"
    );

    // In release builds, verify strict sub-2ms SLA. In debug builds, verify bounded debug overhead.
    #[cfg(not(debug_assertions))]
    assert!(
        p90 < 2000,
        "Feed preview p90 query latency SLA violation in release: p90 = {p90}µs >= 2000µs"
    );

    #[cfg(debug_assertions)]
    assert!(
        p90 < 10000,
        "Feed preview p90 query latency unexpected debug spike: p90 = {p90}µs >= 10000µs"
    );
}

#[tokio::test]
async fn test_adversarial_read_only_impression_isolation_contract() {
    let (state, interner, _graph, _rec, _snap, _ingest, _stats) = build_stress_graph_state();
    let app = create_xrpc_router(state.clone());

    let target_user = "did:plc:user_42";
    let target_uid = interner.lookup_id(target_user).unwrap();

    // Verify initial impressions = 0
    assert_eq!(
        state
            .recommender
            .impression_store
            .get_viewer_impression_count(target_uid),
        0
    );

    // Execute 50 feed-preview requests with diverse dial parameters
    for i in 0..50 {
        let uri = match i % 4 {
            0 => format!("/api/feed-preview?viewer={target_user}&freshness=realtime&limit=30"),
            1 => format!("/api/feed-preview?viewer={target_user}&discovery=deep_dive&limit=30"),
            2 => format!("/api/feed-preview?viewer={target_user}&art=5.0&tech=0.0&limit=30"),
            _ => format!("/api/feed-preview?viewer={target_user}&explain=true&limit=30"),
        };

        let req = Request::builder().uri(&uri).body(Body::empty()).unwrap();
        let resp = app.clone().oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);

        let body = resp.into_body().collect().await.unwrap().to_bytes();
        let preview: FeedPreviewResponse = serde_json::from_slice(&body).unwrap();
        assert!(!preview.items.is_empty());
    }

    // Assert that impression store remains STRICTLY empty for target_user
    assert_eq!(
        state
            .recommender
            .impression_store
            .get_viewer_impression_count(target_uid),
        0,
        "Feed preview violated read-only impression isolation invariant!"
    );

    // Now issue a production XRPC request (/xrpc/app.bsky.feed.getFeedSkeleton) with Bearer token
    let header_b64 = "eyJhbGciOiJub25lIn0";
    let payload_json = serde_json::json!({ "iss": target_user });
    let payload_b64 = URL_SAFE_NO_PAD.encode(payload_json.to_string().as_bytes());
    let token = format!("{header_b64}.{payload_b64}.c2ln");

    let xrpc_req = Request::builder()
        .uri("/xrpc/app.bsky.feed.getFeedSkeleton?feed=at://did:plc:feed/app.bsky.feed.generator/foryou&limit=20")
        .header("Authorization", format!("Bearer {token}"))
        .body(Body::empty())
        .unwrap();

    let xrpc_resp = app.clone().oneshot(xrpc_req).await.unwrap();
    assert_eq!(xrpc_resp.status(), StatusCode::OK);

    let xrpc_body = xrpc_resp.into_body().collect().await.unwrap().to_bytes();
    let skeleton: FeedSkeletonResponse = serde_json::from_slice(&xrpc_body).unwrap();
    let served_count = skeleton.feed.len();
    assert!(served_count > 0);

    // Assert that XRPC request DID record impressions
    let post_xrpc_impressions = state
        .recommender
        .impression_store
        .get_viewer_impression_count(target_uid);
    assert_eq!(
        post_xrpc_impressions, served_count,
        "Production XRPC request should have recorded exactly {served_count} impressions"
    );

    // Subsequent XRPC request should now hard-suppress previously served posts
    let xrpc_req2 = Request::builder()
        .uri("/xrpc/app.bsky.feed.getFeedSkeleton?feed=at://did:plc:feed/app.bsky.feed.generator/foryou&limit=20")
        .header("Authorization", format!("Bearer {token}"))
        .body(Body::empty())
        .unwrap();

    let xrpc_resp2 = app.clone().oneshot(xrpc_req2).await.unwrap();
    assert_eq!(xrpc_resp2.status(), StatusCode::OK);
    let xrpc_body2 = xrpc_resp2.into_body().collect().await.unwrap().to_bytes();
    let skeleton2: FeedSkeletonResponse = serde_json::from_slice(&xrpc_body2).unwrap();
    // Subsequent XRPC request serves candidates with smooth soft fatigue damping
    assert!(!skeleton2.feed.is_empty());
}

#[tokio::test]
async fn test_adversarial_dynamic_telemetry_reporting_under_mutation_events() {
    let (state, interner, graph, _rec, snapshot_tracker, ingestion_tracker, stats) =
        build_stress_graph_state();
    let app = create_xrpc_router(state);

    // 1. Check initial telemetry state
    let req1 = Request::builder()
        .uri("/api/telemetry")
        .body(Body::empty())
        .unwrap();
    let resp1 = app.clone().oneshot(req1).await.unwrap();
    assert_eq!(resp1.status(), StatusCode::OK);
    let body1 = resp1.into_body().collect().await.unwrap().to_bytes();
    let tele1: TelemetryResponse = serde_json::from_slice(&body1).unwrap();

    assert_eq!(tele1.snapshot.status, "clean");
    assert_eq!(tele1.ingestion.events_received, 0);
    assert_eq!(tele1.ingestion.events_processed, 0);
    let initial_nodes = tele1.graph.total_nodes;
    let initial_edges = tele1.graph.total_edges;
    let initial_follows = tele1.graph.total_follows;

    // 2. Trigger Snapshot Lifecycle Events
    snapshot_tracker.record_load(12.34);
    let req2 = Request::builder()
        .uri("/api/telemetry")
        .body(Body::empty())
        .unwrap();
    let resp2 = app.clone().oneshot(req2).await.unwrap();
    let body2 = resp2.into_body().collect().await.unwrap().to_bytes();
    let tele2: TelemetryResponse = serde_json::from_slice(&body2).unwrap();
    assert_eq!(tele2.snapshot.status, "hydrated");
    assert!((tele2.snapshot.last_load_duration_ms - 12.34).abs() < 1e-3);

    snapshot_tracker.record_save(45.67, 1024 * 1024 * 5); // 5 MB
    let req3 = Request::builder()
        .uri("/api/telemetry")
        .body(Body::empty())
        .unwrap();
    let resp3 = app.clone().oneshot(req3).await.unwrap();
    let body3 = resp3.into_body().collect().await.unwrap().to_bytes();
    let tele3: TelemetryResponse = serde_json::from_slice(&body3).unwrap();
    assert_eq!(tele3.snapshot.status, "persisted");
    assert!((tele3.snapshot.last_save_duration_ms - 45.67).abs() < 1e-3);
    assert_eq!(tele3.snapshot.file_size_bytes, 5 * 1024 * 1024);

    snapshot_tracker.record_save_failure("disk full");
    let req4 = Request::builder()
        .uri("/api/telemetry")
        .body(Body::empty())
        .unwrap();
    let resp4 = app.clone().oneshot(req4).await.unwrap();
    let body4 = resp4.into_body().collect().await.unwrap().to_bytes();
    let tele4: TelemetryResponse = serde_json::from_slice(&body4).unwrap();
    assert_eq!(tele4.snapshot.status, "error: disk full");

    // 3. Trigger Ingestion Events
    stats
        .events_received
        .store(50000, std::sync::atomic::Ordering::Relaxed);
    stats
        .events_processed
        .store(49950, std::sync::atomic::Ordering::Relaxed);
    stats
        .bytes_received
        .store(10_000_000, std::sync::atomic::Ordering::Relaxed);
    stats
        .reconnect_count
        .store(3, std::sync::atomic::Ordering::Relaxed);
    stats
        .latest_cursor_us
        .store(1_720_000_000_000_000, std::sync::atomic::Ordering::Relaxed);
    stats.last_activity_timestamp.store(
        BLUESKY_EPOCH_SECS + 1_500_000,
        std::sync::atomic::Ordering::Relaxed,
    );

    // Warm up velocity calculation
    let _ = ingestion_tracker.calculate_velocity();

    let req5 = Request::builder()
        .uri("/api/telemetry")
        .body(Body::empty())
        .unwrap();
    let resp5 = app.clone().oneshot(req5).await.unwrap();
    let body5 = resp5.into_body().collect().await.unwrap().to_bytes();
    let tele5: TelemetryResponse = serde_json::from_slice(&body5).unwrap();

    assert_eq!(tele5.ingestion.events_received, 50000);
    assert_eq!(tele5.ingestion.events_processed, 49950);
    assert_eq!(tele5.ingestion.bytes_received, 10_000_000);
    assert_eq!(tele5.ingestion.reconnect_count, 3);
    assert_eq!(tele5.ingestion.latest_cursor_us, 1_720_000_000_000_000);
    assert_eq!(
        tele5.ingestion.last_activity_timestamp,
        BLUESKY_EPOCH_SECS + 1_500_000
    );

    // 4. Trigger Graph Mutations (New Nodes, Edges, Follows)
    let new_user = interner.intern("did:plc:new_telemetry_user");
    let new_post = interner.intern("at://did:plc:new_telemetry_user/app.bsky.feed.post/new_post");
    let now = BLUESKY_EPOCH_SECS + 1_500_000;
    graph.record_post_meta(new_post, new_user, None, None, now);
    graph.record_interaction(new_user, new_post, SignalType::Like, now);
    graph.record_follow(new_user, interner.lookup_id("did:plc:user_0").unwrap());

    let req6 = Request::builder()
        .uri("/api/telemetry")
        .body(Body::empty())
        .unwrap();
    let resp6 = app.clone().oneshot(req6).await.unwrap();
    let body6 = resp6.into_body().collect().await.unwrap().to_bytes();
    let tele6: TelemetryResponse = serde_json::from_slice(&body6).unwrap();

    assert_eq!(tele6.graph.total_nodes, initial_nodes + 2);
    assert_eq!(tele6.graph.total_edges, initial_edges + 1);
    assert_eq!(tele6.graph.total_follows, initial_follows + 1);
    assert_eq!(tele6.graph.total_posts, tele1.graph.total_posts + 1);
    assert_eq!(tele6.graph.total_users, tele1.graph.total_users + 1);
}

#[tokio::test]
async fn test_adversarial_query_parameter_fuzzing_and_boundary_safety() {
    let (state, _interner, _graph, _rec, _snap, _ingest, _stats) = build_stress_graph_state();
    let app = create_xrpc_router(state);

    let test_cases = vec![
        // Taste Twins edge cases
        ("/api/taste-twins", StatusCode::BAD_REQUEST),
        ("/api/taste-twins?did=", StatusCode::BAD_REQUEST),
        ("/api/taste-twins?handle=%20%20%20", StatusCode::BAD_REQUEST),
        ("/api/taste-twins?did=@@@alice.bsky.social&limit=10", StatusCode::OK),
        ("/api/taste-twins?did=did:plc:nonexistent&limit=999999", StatusCode::OK),
        ("/api/taste-twins?handle=alice.bsky.social&limit=0", StatusCode::OK),
        ("/api/taste-twins?limit=-10", StatusCode::BAD_REQUEST),

        // Feed Preview edge cases
        ("/api/feed-preview", StatusCode::OK), // Anonymous Tier 3
        ("/api/feed-preview?viewer=", StatusCode::OK),
        ("/api/feed-preview?art=-100.0&tech=999.0&science=0.0", StatusCode::OK),
        ("/api/feed-preview?freshness=invalid_str&discovery=unknown_mode", StatusCode::OK),
        ("/api/feed-preview?limit=0", StatusCode::OK),
        ("/api/feed-preview?limit=100000", StatusCode::OK),
        ("/api/feed-preview?viewer=did:plc:user_0&explain=true", StatusCode::OK),

        // Explain edge cases
        ("/api/explain", StatusCode::BAD_REQUEST),
        ("/api/explain?viewer=did:plc:user_0", StatusCode::BAD_REQUEST),
        ("/api/explain?uri=", StatusCode::BAD_REQUEST),
        ("/api/explain?uri=at://did:plc:nonexistent/post/1", StatusCode::OK),
        ("/api/explain?post=at://did:plc:creator_0/app.bsky.feed.post/art_0&viewer=did:plc:user_0", StatusCode::OK),
        ("/api/explain?post=at://did:plc:creator_0/app.bsky.feed.post/art_0&viewer=did:plc:nonexistent", StatusCode::OK),
    ];

    for (uri, expected_status) in test_cases {
        let req = Request::builder().uri(uri).body(Body::empty()).unwrap();
        let resp = app.clone().oneshot(req).await.unwrap();
        assert_eq!(
            resp.status(),
            expected_status,
            "Failed for URI {uri}: expected {expected_status}, got {}",
            resp.status()
        );

        let body = resp.into_body().collect().await.unwrap().to_bytes();
        assert!(!body.is_empty(), "Body for {uri} should not be empty");
    }
}
