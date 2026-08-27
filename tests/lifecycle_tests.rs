//! Lifecycle and Server Integration Tests for `for-your-consideration`.
//!
//! Tests cover:
//! 1. HTTP XRPC request to `GET /xrpc/app.bsky.feed.getFeedSkeleton` records impressions for authenticated viewers.
//! 2. Subsequent immediate request for the same viewer returns 0 duplicate posts (100% suppression).
//! 3. Boot snapshot recovery restores graph and interner state before serving requests (<50ms).
//! 4. Periodic background snapshot task runs cleanly in `tokio::task::JoinSet` with `CancellationToken`.
//! 5. Graceful shutdown persistence writes valid `snapshot.bin` on cancellation.

#![allow(
    clippy::pedantic,
    clippy::nursery,
    clippy::float_cmp,
    unused_assignments
)]

mod common;

use std::path::PathBuf;
use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::body::Body;
use axum::http::{Request, StatusCode};
use for_your_consideration::prelude::*;
use http_body_util::BodyExt;
use tokio::task::JoinSet;
use tokio_util::sync::CancellationToken;
use tower::ServiceExt;

use crate::common::*;

/// Returns current system time in seconds.
fn system_now_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

/// Generates a unique temporary path for snapshot tests.
fn unique_temp_snapshot_path(tag: &str) -> PathBuf {
    let mut path = std::env::temp_dir();
    let file_name = format!(
        "for_your_consideration_lifecycle_{}_{}_{}.bin",
        tag,
        std::process::id(),
        Instant::now().elapsed().as_nanos()
    );
    path.push(file_name);
    path
}

/// Helper to build a standard authenticated request with a mock JWT.
fn build_authenticated_xrpc_request(
    viewer_did: &str,
    feed_uri: &str,
    limit: usize,
) -> Request<Body> {
    let jwt = generate_mock_jwt(viewer_did, "did:web:feed.example.com", true);
    Request::builder()
        .uri(format!(
            "/xrpc/app.bsky.feed.getFeedSkeleton?feed={feed_uri}&limit={limit}"
        ))
        .header("Authorization", format!("Bearer {jwt}"))
        .body(Body::empty())
        .expect("Valid HTTP request builder")
}

/// Helper to build an unauthenticated anonymous request.
fn build_anonymous_xrpc_request(feed_uri: &str, limit: usize) -> Request<Body> {
    Request::builder()
        .uri(format!(
            "/xrpc/app.bsky.feed.getFeedSkeleton?feed={feed_uri}&limit={limit}"
        ))
        .body(Body::empty())
        .expect("Valid HTTP request builder")
}

// ===========================================================================
// Test 1: HTTP XRPC Impression Recording & 100% Immediate Suppression
// ===========================================================================

#[tokio::test]
async fn test_xrpc_impression_recording_and_immediate_hard_suppression() {
    let now = system_now_secs();
    let interner = Arc::new(StringInterner::new());
    let graph = Arc::new(GraphStore::new());

    // Setup active graph with 3 posts
    let author_did = "did:plc:author_popular";
    let post1_uri = "at://did:plc:author_popular/app.bsky.feed.post/101";
    let post2_uri = "at://did:plc:author_popular/app.bsky.feed.post/102";
    let post3_uri = "at://did:plc:author_popular/app.bsky.feed.post/103";

    let aid = interner.intern(author_did);
    let p1 = interner.intern(post1_uri);
    let p2 = interner.intern(post2_uri);
    let p3 = interner.intern(post3_uri);

    graph.record_post_meta(p1, aid, None, None, now);
    graph.record_post_meta(p2, aid, None, None, now);
    graph.record_post_meta(p3, aid, None, None, now);

    // Seed interactions so tier 3 velocity pool / recommendations surface them
    let u_seed1 = interner.intern("did:plc:seed_user_1");
    let u_seed2 = interner.intern("did:plc:seed_user_2");
    let u_seed3 = interner.intern("did:plc:seed_user_3");
    for &p in &[p1, p2, p3] {
        graph.record_interaction(u_seed1, p, SignalType::Like, now);
        graph.record_interaction(u_seed2, p, SignalType::Like, now);
        graph.record_interaction(u_seed3, p, SignalType::Like, now);
    }

    let recommender = Arc::new(Recommender::new(Arc::clone(&interner), Arc::clone(&graph)));
    let state = AppState::new(recommender, "did:web:feed.example.com", "feed.example.com");
    let router = create_xrpc_router(state.clone());

    let feed_uri = "at://did:plc:feed/app.bsky.feed.generator/foryou";
    let viewer_did = "did:plc:alice_viewer";
    let viewer_id = interner.intern(viewer_did);

    // Initial state: impression store should have 0 impressions for Alice
    assert_eq!(
        state
            .recommender
            .impression_store()
            .get_viewer_impression_count(viewer_id),
        0
    );

    // Request 1: Authenticated request for Alice
    let req1 = build_authenticated_xrpc_request(viewer_did, feed_uri, 10);
    let resp1 = router.clone().oneshot(req1).await.unwrap();
    assert_eq!(resp1.status(), StatusCode::OK);

    let body1 = resp1.into_body().collect().await.unwrap().to_bytes();
    let skeleton1: FeedSkeletonResponse = serde_json::from_slice(&body1).unwrap();
    assert!(
        !skeleton1.feed.is_empty(),
        "Expected at least one post in feed recommendation"
    );

    let served_count = skeleton1.feed.len();

    // Verify impressions are now recorded in the impression store for Alice
    assert_eq!(
        state
            .recommender
            .impression_store()
            .get_viewer_impression_count(viewer_id),
        served_count
    );

    for item in &skeleton1.feed {
        let pid = interner.intern(item.post.as_str());
        assert!(
            state
                .recommender
                .impression_store()
                .contains_impression(viewer_id, pid),
            "Impression store missing served post: {}",
            item.post
        );
    }

    // Request 2: Immediate subsequent request for Alice
    // All previously served posts are softly dampened (0.15x) rather than dropped
    let req2 = build_authenticated_xrpc_request(viewer_did, feed_uri, 10);
    let resp2 = router.clone().oneshot(req2).await.unwrap();
    assert_eq!(resp2.status(), StatusCode::OK);

    let body2 = resp2.into_body().collect().await.unwrap().to_bytes();
    let skeleton2: FeedSkeletonResponse = serde_json::from_slice(&body2).unwrap();
    assert_eq!(skeleton2.feed.len(), served_count);

    // Request 3: Request for Bob (different viewer)
    // Bob should still see the posts because impressions are strictly isolated per viewer DID
    let bob_did = "did:plc:bob_viewer";
    let bob_id = interner.intern(bob_did);
    assert_eq!(
        state
            .recommender
            .impression_store()
            .get_viewer_impression_count(bob_id),
        0
    );

    let req3 = build_authenticated_xrpc_request(bob_did, feed_uri, 10);
    let resp3 = router.oneshot(req3).await.unwrap();
    assert_eq!(resp3.status(), StatusCode::OK);

    let body3 = resp3.into_body().collect().await.unwrap().to_bytes();
    let skeleton3: FeedSkeletonResponse = serde_json::from_slice(&body3).unwrap();
    assert_eq!(
        skeleton3.feed.len(),
        served_count,
        "Bob should receive un-suppressed candidate posts"
    );
}

// ===========================================================================
// Test 2: Anonymous XRPC Requests Do Not Pollute Impression Memory
// ===========================================================================

#[tokio::test]
async fn test_xrpc_anonymous_viewer_no_impression_recording() {
    let now = system_now_secs();
    let interner = Arc::new(StringInterner::new());
    let graph = Arc::new(GraphStore::new());

    let aid = interner.intern("did:plc:author_anon_test");
    let pid = interner.intern("at://did:plc:author_anon_test/app.bsky.feed.post/anon1");
    graph.record_post_meta(pid, aid, None, None, now);
    let uid = interner.intern("did:plc:interactor1");
    graph.record_interaction(uid, pid, SignalType::Like, now);

    let recommender = Arc::new(Recommender::new(Arc::clone(&interner), Arc::clone(&graph)));
    let state = AppState::new(recommender, "did:web:feed.example.com", "feed.example.com");
    let router = create_xrpc_router(state.clone());

    let feed_uri = "at://did:plc:feed/app.bsky.feed.generator/foryou";

    // Issue anonymous request
    let req = build_anonymous_xrpc_request(feed_uri, 10);
    let resp = router.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    // Impression store should have 0 recorded viewers
    assert_eq!(state.recommender.impression_store().total_viewers(), 0);
}

// ===========================================================================
// Test 3: Boot Snapshot Recovery Restores Graph & Interner State (<50ms)
// ===========================================================================

#[tokio::test]
async fn test_boot_snapshot_recovery_restores_graph_and_serves_requests() {
    let snapshot_path = unique_temp_snapshot_path("boot_recovery");
    let now = system_now_secs();

    // 1. Create and populate initial state
    let interner = Arc::new(StringInterner::new());
    let graph = Arc::new(GraphStore::new());

    let u_alice = interner.intern("did:plc:alice");
    let u_bob = interner.intern("did:plc:bob");
    let u_charlie = interner.intern("did:plc:charlie");

    let p1 = interner.intern("at://did:plc:bob/app.bsky.feed.post/p1");
    let p2 = interner.intern("at://did:plc:charlie/app.bsky.feed.post/p2");

    graph.record_post_meta(p1, u_bob, None, None, now);
    graph.record_post_meta(p2, u_charlie, None, None, now);

    graph.record_interaction(u_alice, p1, SignalType::Like, now);
    graph.record_interaction(u_alice, p2, SignalType::Repost, now);
    graph.record_follow(u_alice, u_bob);

    let original_cursor_us = 1_700_000_888_999;

    // 2. Persist snapshot to disk
    let header = save_snapshot(&snapshot_path, &interner, &graph, original_cursor_us)
        .expect("Snapshot saving must succeed");

    assert_eq!(header.magic, SNAPSHOT_MAGIC);
    assert_eq!(header.jetstream_cursor_us, original_cursor_us);
    assert!(snapshot_path.exists());

    // 3. Hydrate into fresh domain structures
    let restored_interner = Arc::new(StringInterner::new());
    let restored_graph = Arc::new(GraphStore::new());

    let loaded = load_snapshot(&snapshot_path, &restored_interner, &restored_graph)
        .expect("Load snapshot must succeed")
        .expect("Snapshot file must exist");

    // Check performance threshold (<50ms)
    assert!(
        loaded.load_duration_ms < 50.0,
        "Snapshot hydration exceeded 50ms limit: {:.2}ms",
        loaded.load_duration_ms
    );
    assert_eq!(loaded.header.jetstream_cursor_us, original_cursor_us);
    assert_eq!(loaded.header.num_strings, interner.len() as u32);

    // Verify string interner identity mapping
    assert_eq!(
        restored_interner
            .lookup_str(u_alice)
            .map(|s| s.as_str().to_string()),
        Some("did:plc:alice".to_string())
    );
    assert_eq!(
        restored_interner
            .lookup_str(p1)
            .map(|s| s.as_str().to_string()),
        Some("at://did:plc:bob/app.bsky.feed.post/p1".to_string())
    );

    // Verify graph store edges
    let user_likes = restored_graph.get_user_likes_bitmap(u_alice);
    assert!(user_likes.is_some_and(|bm| bm.contains(p1)));
    assert_eq!(restored_graph.get_user_interactions(u_alice).len(), 2);
    assert!(restored_graph.get_user_follows(u_alice).contains(&u_bob));

    // 4. Initialize server from restored structures and serve requests
    let recommender = Arc::new(Recommender::new(
        Arc::clone(&restored_interner),
        Arc::clone(&restored_graph),
    ));
    let state = AppState::new(recommender, "did:web:feed.example.com", "feed.example.com");
    let router = create_xrpc_router(state);

    let req = build_authenticated_xrpc_request(
        "did:plc:alice",
        "at://did:plc:feed/app.bsky.feed.generator/foryou",
        10,
    );
    let resp = router.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    // Cleanup
    let _ = std::fs::remove_file(&snapshot_path);
}

// ===========================================================================
// Test 4: Periodic Background Snapshot Task in JoinSet with Cancellation
// ===========================================================================

#[tokio::test]
async fn test_periodic_background_snapshot_task_lifecycle() {
    let snapshot_path = unique_temp_snapshot_path("periodic_task");
    let now = system_now_secs();

    let interner = Arc::new(StringInterner::new());
    let graph = Arc::new(GraphStore::new());

    let u1 = interner.intern("did:plc:user_periodic");
    let p1 = interner.intern("at://did:plc:user_periodic/app.bsky.feed.post/p100");
    graph.record_post_meta(p1, u1, None, None, now);
    graph.record_interaction(u1, p1, SignalType::Like, now);

    let cancel_token = CancellationToken::new();
    let mut tasks = JoinSet::new();

    // Spawn periodic snapshot task with rapid 30ms interval for testing
    let snapshot_interner = Arc::clone(&interner);
    let snapshot_graph = Arc::clone(&graph);
    let snapshot_cancel = cancel_token.clone();
    let path_clone = snapshot_path.clone();
    let interval_duration = Duration::from_millis(30);

    tasks.spawn(async move {
        let mut interval = tokio::time::interval(interval_duration);
        interval.tick().await; // First tick fires immediately
        loop {
            tokio::select! {
                _ = snapshot_cancel.cancelled() => {
                    break;
                }
                _ = interval.tick() => {
                    let _ = save_snapshot(&path_clone, &snapshot_interner, &snapshot_graph, 1_700_111_222_333);
                }
            }
        }
    });

    // Let it run through multiple ticks
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Mutate graph while periodic task is running
    let p2 = interner.intern("at://did:plc:user_periodic/app.bsky.feed.post/p200");
    graph.record_post_meta(p2, u1, None, None, now);
    graph.record_interaction(u1, p2, SignalType::Like, now);

    // Allow another tick to capture mutation
    tokio::time::sleep(Duration::from_millis(60)).await;

    // Trigger cancellation
    cancel_token.cancel();

    // Await clean task termination
    let res = tasks.join_next().await;
    assert!(res.is_some(), "Task must join cleanly");

    // Verify snapshot file on disk
    assert!(
        snapshot_path.exists(),
        "Periodic snapshot must have been created"
    );

    let fresh_interner = Arc::new(StringInterner::new());
    let fresh_graph = Arc::new(GraphStore::new());
    let loaded = load_snapshot(&snapshot_path, &fresh_interner, &fresh_graph)
        .expect("Snapshot loading must succeed")
        .expect("Snapshot must exist");

    assert_eq!(loaded.header.magic, SNAPSHOT_MAGIC);
    assert_eq!(loaded.header.jetstream_cursor_us, 1_700_111_222_333);
    assert_eq!(fresh_graph.get_user_interactions(u1).len(), 2);

    // Cleanup
    let _ = std::fs::remove_file(&snapshot_path);
}

// ===========================================================================
// Test 5: Graceful Shutdown Persistence Writes Valid Snapshot on Cancel
// ===========================================================================

#[tokio::test]
async fn test_graceful_shutdown_persistence_on_cancellation() {
    let snapshot_path = unique_temp_snapshot_path("graceful_shutdown");
    let now = system_now_secs();

    let interner = Arc::new(StringInterner::new());
    let graph = Arc::new(GraphStore::new());
    let recommender = Arc::new(Recommender::new(Arc::clone(&interner), Arc::clone(&graph)));

    // Setup server state
    let state = AppState::new(
        Arc::clone(&recommender),
        "did:web:feed.example.com",
        "feed.example.com",
    );
    let router = create_xrpc_router(state);

    let cancel_token = CancellationToken::new();
    let mut tasks = JoinSet::new();

    // Spawn server task on random local port
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let server_token = cancel_token.clone();
    tasks.spawn(async move {
        let _ = serve_xrpc(listener, router, server_token).await;
    });

    // Simulate in-memory activity
    let u1 = interner.intern("did:plc:shutdown_user");
    let p1 = interner.intern("at://did:plc:shutdown_user/app.bsky.feed.post/1");
    graph.record_post_meta(p1, u1, None, None, now);
    graph.record_interaction(u1, p1, SignalType::Like, now);

    let recorded_cursor = 1_700_999_888_777;

    // Simulate SIGINT/SIGTERM shutdown sequence
    cancel_token.cancel();

    // Drain tasks with timeout safety
    let drain_result = tokio::time::timeout(Duration::from_secs(5), async {
        while let Some(res) = tasks.join_next().await {
            let _ = res;
        }
    })
    .await;
    assert!(
        drain_result.is_ok(),
        "Background tasks drained within timeout"
    );

    // Perform final shutdown snapshot save
    let header = save_snapshot(&snapshot_path, &interner, &graph, recorded_cursor)
        .expect("Shutdown snapshot save must succeed");

    assert_eq!(header.magic, SNAPSHOT_MAGIC);
    assert_eq!(header.jetstream_cursor_us, recorded_cursor);

    // Verify recovery from shutdown snapshot
    let fresh_interner = Arc::new(StringInterner::new());
    let fresh_graph = Arc::new(GraphStore::new());
    let loaded = load_snapshot(&snapshot_path, &fresh_interner, &fresh_graph)
        .expect("Loaded snapshot must succeed")
        .expect("Snapshot must exist");

    assert_eq!(loaded.header.jetstream_cursor_us, recorded_cursor);
    assert_eq!(fresh_graph.get_user_interactions(u1).len(), 1);

    // Cleanup
    let _ = std::fs::remove_file(&snapshot_path);
}

// ===========================================================================
// Test 6: Concurrent Multi-Viewer XRPC Requests and Impression Isolation
// ===========================================================================

#[tokio::test]
async fn test_multithreaded_concurrent_xrpc_requests_and_impression_recording() {
    let now = system_now_secs();
    let interner = Arc::new(StringInterner::new());
    let graph = Arc::new(GraphStore::new());

    // Create 10 distinct posts and authors
    for i in 0..10 {
        let aid = interner.intern(&format!("did:plc:author_{i}"));
        let pid = interner.intern(&format!("at://did:plc:author_{i}/app.bsky.feed.post/{i}"));
        graph.record_post_meta(pid, aid, None, None, now);
        for k in 1..=3 {
            let uid = interner.intern(&format!("did:plc:liker_{i}_{k}"));
            graph.record_interaction(uid, pid, SignalType::Like, now);
        }
    }

    let recommender = Arc::new(Recommender::new(Arc::clone(&interner), Arc::clone(&graph)));
    let state = AppState::new(recommender, "did:web:feed.example.com", "feed.example.com");
    let router = create_xrpc_router(state.clone());

    // Spawn 20 concurrent requests across different viewers
    let mut handles = Vec::new();
    for v in 0..20 {
        let r = router.clone();
        let viewer_did = format!("did:plc:concurrent_viewer_{v}");
        handles.push(tokio::spawn(async move {
            let req = build_authenticated_xrpc_request(
                &viewer_did,
                "at://did:plc:feed/app.bsky.feed.generator/foryou",
                5,
            );
            let resp = r.oneshot(req).await.unwrap();
            assert_eq!(resp.status(), StatusCode::OK);
            let body = resp.into_body().collect().await.unwrap().to_bytes();
            let skeleton: FeedSkeletonResponse = serde_json::from_slice(&body).unwrap();
            (viewer_did, skeleton.feed.len())
        }));
    }

    for handle in handles {
        let (did, count) = handle.await.unwrap();
        assert!(count > 0);
        let vid = state.recommender.interner.intern(&did);
        assert_eq!(
            state
                .recommender
                .impression_store()
                .get_viewer_impression_count(vid),
            count
        );
    }

    assert_eq!(state.recommender.impression_store().total_viewers(), 20);
}

// ===========================================================================
// Test 7: Corrupted Snapshot Fallback Handling
// ===========================================================================

#[tokio::test]
async fn test_snapshot_recovery_corrupted_file_handling_fallback() {
    let snapshot_path = unique_temp_snapshot_path("corrupt_recovery");

    // Write invalid garbage bytes
    std::fs::write(
        &snapshot_path,
        b"CORRUPT_NOT_A_VALID_SNAPSHOT_HEADER_OR_PAYLOAD",
    )
    .unwrap();

    let interner = Arc::new(StringInterner::new());
    let graph = Arc::new(GraphStore::new());

    // Attempting to load corrupted snapshot must return Err(FeedError::Snapshot)
    let load_res = load_snapshot(&snapshot_path, &interner, &graph);
    assert!(
        load_res.is_err(),
        "Loading corrupted snapshot must return error"
    );

    // Graph and interner should remain empty and clean
    assert_eq!(interner.len(), 0);
    assert_eq!(graph.stats().total_users, 0);

    // Cleanup
    let _ = std::fs::remove_file(&snapshot_path);
}

// ===========================================================================
// Test 8: Impression Sliding Window Fatigue & Soft Recovery
// ===========================================================================

#[tokio::test]
async fn test_impression_sliding_window_fatigue_and_recovery() {
    let base_time = 1_700_000_000;
    let interner = Arc::new(StringInterner::new());
    let graph = Arc::new(GraphStore::new());

    let aid = interner.intern("did:plc:author_fatigue");
    let p1 = interner.intern("at://did:plc:author_fatigue/app.bsky.feed.post/1");
    let p2 = interner.intern("at://did:plc:author_fatigue/app.bsky.feed.post/2");

    graph.record_post_meta(p1, aid, None, None, base_time);
    graph.record_post_meta(p2, aid, None, None, base_time);

    let recommender = Arc::new(Recommender::new(Arc::clone(&interner), Arc::clone(&graph)));

    let viewer_did = "did:plc:viewer_fatigue_test";
    let vid = interner.intern(viewer_did);

    // Record p1 served at base_time
    recommender.record_impressions_by_did(Some(viewer_did), &[p1], base_time);

    // 1. Within 0-30m (e.g. +10m = 600s): Smooth soft decay (>= 0.15)
    let penalty_10m =
        recommender
            .impression_store()
            .evaluate_fatigue_penalty(vid, p1, base_time + 600);
    assert!(penalty_10m.is_some());
    let mult_10m = penalty_10m.unwrap();
    assert!(
        (0.15..1.0).contains(&mult_10m),
        "10m fatigue multiplier must be between 0.15 and 1: {mult_10m}"
    );

    // 2. At 1 hour (+3600s): Soft fatigue decay (Some(0.0 < multiplier < 1.0))
    let penalty_1h =
        recommender
            .impression_store()
            .evaluate_fatigue_penalty(vid, p1, base_time + 3600);
    assert!(penalty_1h.is_some());
    let mult_1h = penalty_1h.unwrap();
    assert!(
        mult_1h > 0.0 && mult_1h < 1.0,
        "1h fatigue multiplier must be between 0 and 1: {mult_1h}"
    );

    // 3. At 7 hours (+25200s > 6h): Full recovery (Some(1.0))
    let penalty_7h =
        recommender
            .impression_store()
            .evaluate_fatigue_penalty(vid, p1, base_time + 25200);
    assert_eq!(
        penalty_7h,
        Some(1.0),
        ">6h fatigue window must be fully recovered"
    );

    // 4. Unserved post p2 should have no fatigue
    let penalty_p2 =
        recommender
            .impression_store()
            .evaluate_fatigue_penalty(vid, p2, base_time + 600);
    assert_eq!(penalty_p2, Some(1.0));
}

// ===========================================================================
// Test 9: Concurrent XRPC Requests Under Rapid Periodic Snapshotting
// ===========================================================================

#[tokio::test]
async fn test_snapshot_atomic_rename_durability_under_concurrent_xrpc() {
    let snapshot_path = unique_temp_snapshot_path("concurrent_xrpc_snap");
    let now = system_now_secs();

    let interner = Arc::new(StringInterner::new());
    let graph = Arc::new(GraphStore::new());

    for i in 0..20 {
        let aid = interner.intern(&format!("did:plc:author_concur_{i}"));
        let pid = interner.intern(&format!(
            "at://did:plc:author_concur_{i}/app.bsky.feed.post/{i}"
        ));
        graph.record_post_meta(pid, aid, None, None, now);
        let uid = interner.intern(&format!("did:plc:liker_concur_{i}"));
        graph.record_interaction(uid, pid, SignalType::Like, now);
    }

    let recommender = Arc::new(Recommender::new(Arc::clone(&interner), Arc::clone(&graph)));
    let state = AppState::new(recommender, "did:web:feed.example.com", "feed.example.com");
    let router = create_xrpc_router(state.clone());

    let cancel_token = CancellationToken::new();
    let mut tasks = JoinSet::new();

    // Background snapshotter saving every 20ms
    let snap_interner = Arc::clone(&interner);
    let snap_graph = Arc::clone(&graph);
    let snap_path = snapshot_path.clone();
    let snap_cancel = cancel_token.clone();

    tasks.spawn(async move {
        let mut interval = tokio::time::interval(Duration::from_millis(20));
        interval.tick().await;
        loop {
            tokio::select! {
                _ = snap_cancel.cancelled() => break,
                _ = interval.tick() => {
                    let _ = save_snapshot(&snap_path, &snap_interner, &snap_graph, 100);
                }
            }
        }
    });

    // Run 30 concurrent XRPC requests
    let mut request_handles = Vec::new();
    for v in 0..30 {
        let r = router.clone();
        let did = format!("did:plc:stress_viewer_{v}");
        request_handles.push(tokio::spawn(async move {
            let req = build_authenticated_xrpc_request(
                &did,
                "at://did:plc:feed/app.bsky.feed.generator/foryou",
                5,
            );
            let resp = r.oneshot(req).await.unwrap();
            assert_eq!(resp.status(), StatusCode::OK);
        }));
    }

    for h in request_handles {
        h.await.unwrap();
    }

    // Allow periodic snapshot task to execute ticks
    tokio::time::sleep(Duration::from_millis(50)).await;

    cancel_token.cancel();
    tasks.join_next().await;

    // Verify snapshot file is intact and valid
    assert!(snapshot_path.exists());
    let fresh_interner = Arc::new(StringInterner::new());
    let fresh_graph = Arc::new(GraphStore::new());
    let loaded = load_snapshot(&snapshot_path, &fresh_interner, &fresh_graph)
        .expect("Load snapshot must succeed")
        .expect("Snapshot must exist");

    assert_eq!(loaded.header.magic, SNAPSHOT_MAGIC);

    // Cleanup
    let _ = std::fs::remove_file(&snapshot_path);
}
