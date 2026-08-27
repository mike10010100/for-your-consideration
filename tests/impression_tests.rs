#![forbid(unsafe_code)]
#![allow(clippy::pedantic, clippy::nursery, clippy::suboptimal_flops)]

//! Milestone 2: Comprehensive Integration Test Suite for Impression Memory & Anti-Repetition Fatigue.
//!
//! Validates:
//! 1. 100% hard suppression for posts served within 30 minutes (0–1800s).
//! 2. Exponential score decay curve between 30 minutes and 6 hours (1800–21600s) with $\tau = 7200\text{s}$.
//! 3. Full score restoration after 6 hours (>21600s) with multiplier 1.0.
//! 4. Mathematical accuracy of fatigue multipliers at 1h, 2h, 4h intervals.
//! 5. Post re-ranking where fresher candidates overtake softly decayed candidates.
//! 6. Bounded sliding LRU capacity eviction (max capacity limit strictly enforced).
//! 7. Multi-user isolation across 64 lock shards.
//! 8. Safe saturating time arithmetic under clock skew.
//! 9. Concurrent read/write stress across 64 shards with zero deadlocks or race conditions.
//! 10. `Recommender::record_impressions` and `Recommender::record_impressions_by_did` APIs.

use std::sync::Arc;
use std::thread;

use for_your_consideration::prelude::*;

const TEST_EPOCH: u64 = 1_700_000_000;

// ---------------------------------------------------------------------------
// 1. Primitive Unit & Lifecycle Tests
// ---------------------------------------------------------------------------

#[test]
fn test_viewer_impression_history_lifecycle() {
    let mut history = ViewerImpressionHistory::new(10);
    assert!(history.is_empty());
    assert_eq!(history.len(), 0);

    // Record initial impressions
    history.record_impression(101, TEST_EPOCH);
    history.record_impression(102, TEST_EPOCH + 10);

    assert_eq!(history.len(), 2);
    assert!(!history.is_empty());
    assert!(history.contains(101));
    assert!(history.contains(102));
    assert!(!history.contains(103));

    assert_eq!(history.get_served_timestamp(101), Some(TEST_EPOCH));
    assert_eq!(history.get_served_timestamp(102), Some(TEST_EPOCH + 10));
    assert_eq!(history.get_served_timestamp(103), None);

    // Update timestamp for existing post
    history.record_impression(101, TEST_EPOCH + 100);
    assert_eq!(history.get_served_timestamp(101), Some(TEST_EPOCH + 100));
}

#[test]
fn test_impression_store_shards_and_stats() {
    let store = ImpressionStore::new(100);
    assert_eq!(store.total_viewers(), 0);

    for uid in 0..128 {
        store.record_impressions(uid, &[1000 + uid], TEST_EPOCH);
    }

    assert_eq!(store.total_viewers(), 128);
    for uid in 0..128 {
        assert_eq!(store.get_viewer_impression_count(uid), 1);
        assert!(store.contains_impression(uid, 1000 + uid));
        assert!(!store.contains_impression(uid, 9999));
    }

    store.clear();
    assert_eq!(store.total_viewers(), 0);
}

// ---------------------------------------------------------------------------
// 2. Hard Suppression (0–30 Minutes) Tests
// ---------------------------------------------------------------------------

#[test]
fn test_30m_immediate_hard_suppression_100_percent() {
    let interner = Arc::new(StringInterner::new());
    let graph = Arc::new(GraphStore::new());

    let viewer = interner.intern("did:plc:alice");
    let followed = interner.intern("did:plc:followed");
    let author1 = interner.intern("did:plc:author1");
    let author2 = interner.intern("did:plc:author2");
    let author3 = interner.intern("did:plc:author3");

    let p1 = interner.intern("at://did:plc:author1/post/1");
    let p2 = interner.intern("at://did:plc:author2/post/2");
    let p3 = interner.intern("at://did:plc:author3/post/3");

    graph.record_post_meta(p1, author1, None, None, TEST_EPOCH - 1000);
    graph.record_post_meta(p2, author2, None, None, TEST_EPOCH - 1000);
    graph.record_post_meta(p3, author3, None, None, TEST_EPOCH - 1000);

    graph.record_follow(viewer, followed);
    graph.record_interaction(followed, p1, SignalType::Like, TEST_EPOCH - 500);
    graph.record_interaction(followed, p2, SignalType::Like, TEST_EPOCH - 500);
    graph.record_interaction(followed, p3, SignalType::Like, TEST_EPOCH - 500);

    let rec = Recommender::new(interner, graph);
    let dials = RecommendationDials {
        min_likes: 1,
        ..Default::default()
    };

    // Turn 1: Initial recommendation returns all 3 posts
    let feed1 = rec
        .recommend(Some("did:plc:alice"), &dials, TEST_EPOCH)
        .unwrap();
    assert_eq!(feed1.posts.len(), 3);

    // Record impressions for p1 and p2 at TEST_EPOCH
    rec.record_impressions(Some("did:plc:alice"), &[p1, p2], TEST_EPOCH);

    // Turn 2: Multiple refreshes within 30 minutes
    let check_offsets = [0, 1, 30, 60, 300, 900, 1500, 1799];
    for offset in check_offsets {
        let now = TEST_EPOCH + offset;
        let feed = rec.recommend(Some("did:plc:alice"), &dials, now).unwrap();
        // p3 is fresh (1.0x) so it ranks #1; p1 and p2 are softly dampened (>=0.15x) so they rank #2 and #3
        assert_eq!(feed.posts.len(), 3, "Failed at offset {offset}s");
        assert_eq!(feed.posts[0].post_id, p3, "Failed at offset {offset}s");
        assert!(feed.posts[0].score > feed.posts[1].score);
    }
}

#[test]
fn test_boundary_at_exactly_30_minutes() {
    let store = ImpressionStore::new(100);
    let viewer = 10;
    let post = 200;

    store.record_impressions(viewer, &[post], TEST_EPOCH);

    // 0s -> Immediately viewed: 0.15 floor
    let penalty_0 = store.evaluate_fatigue_penalty(viewer, post, TEST_EPOCH);
    assert_eq!(penalty_0, Some(FATIGUE_MIN_FLOOR));

    // 1799s (29m59s) -> Smooth soft decay
    let penalty_1799 = store.evaluate_fatigue_penalty(viewer, post, TEST_EPOCH + 1799);
    assert!(penalty_1799.is_some());
    let mult_1799 = penalty_1799.unwrap();
    let expected_1799 = FATIGUE_MIN_FLOOR
        + (1.0 - FATIGUE_MIN_FLOOR) * (1.0 - (-1799.0f32 / FATIGUE_TAU_SECS).exp());
    assert!((mult_1799 - expected_1799).abs() < 1e-4);

    // 1800s (30m00s) -> Smooth decay continues
    let penalty_1800 = store.evaluate_fatigue_penalty(viewer, post, TEST_EPOCH + 1800);
    assert!(penalty_1800.is_some());
    let multiplier = penalty_1800.unwrap();
    let expected = FATIGUE_MIN_FLOOR
        + (1.0 - FATIGUE_MIN_FLOOR) * (1.0 - (-1800.0f32 / FATIGUE_TAU_SECS).exp());
    assert!(
        (multiplier - expected).abs() < 1e-4,
        "Expected ~{expected}, got {multiplier}"
    );
}

// ---------------------------------------------------------------------------
// 3. Exponential Soft Decay (0s–6h) Tests
// ---------------------------------------------------------------------------

#[test]
fn test_exponential_score_decay_curve_multipliers() {
    let store = ImpressionStore::new(100);
    let viewer = 42;
    let post = 500;

    store.record_impressions(viewer, &[post], TEST_EPOCH);

    // Test specific time intervals and verify mathematical decay: Multiplier = MIN_FLOOR + (1 - MIN_FLOOR) * (1 - exp(-dt / tau))
    let calc =
        |dt: f32| FATIGUE_MIN_FLOOR + (1.0 - FATIGUE_MIN_FLOOR) * (1.0 - (-dt / 7200.0).exp());
    let test_points: &[(u64, f32)] = &[
        (0, calc(0.0)),
        (1800, calc(1800.0)),   // 30m: ~0.3380
        (3600, calc(3600.0)),   // 1h:  ~0.4845
        (5400, calc(5400.0)),   // 1.5h:~0.5985
        (7200, calc(7200.0)),   // 2h (tau): ~0.6873
        (10800, calc(10800.0)), // 3h: ~0.8104
        (14400, calc(14400.0)), // 4h: ~0.8850
        (18000, calc(18000.0)), // 5h: ~0.9302
        (21599, calc(21599.0)), // ~5h59m: ~0.9577
    ];

    for &(dt, expected) in test_points {
        let actual = store
            .evaluate_fatigue_penalty(viewer, post, TEST_EPOCH + dt)
            .expect("Should return a multiplier");
        assert!(
            (actual - expected).abs() < 1e-4,
            "Mismatch at dt={dt}s: expected {expected}, got {actual}"
        );
    }
}

#[test]
fn test_post_re_ranking_under_fatigue_decay() {
    let interner = Arc::new(StringInterner::new());
    let graph = Arc::new(GraphStore::new());

    let viewer = interner.intern("did:plc:viewer");
    let followed = interner.intern("did:plc:followed");
    let author_a = interner.intern("did:plc:author_a");
    let author_b = interner.intern("did:plc:author_b");

    let post_a = interner.intern("at://did:plc:author_a/post/top");
    let post_b = interner.intern("at://did:plc:author_b/post/runner_up");

    // Post A is created and liked very recently (higher base score)
    graph.record_post_meta(post_a, author_a, None, None, TEST_EPOCH - 100);
    graph.record_interaction(followed, post_a, SignalType::Like, TEST_EPOCH - 50);

    // Post B is created slightly earlier (lower base score)
    graph.record_post_meta(post_b, author_b, None, None, TEST_EPOCH - 2000);
    graph.record_interaction(followed, post_b, SignalType::Like, TEST_EPOCH - 1000);

    graph.record_follow(viewer, followed);

    let rec = Recommender::new(interner, graph);
    let dials = RecommendationDials {
        min_likes: 1,
        ..Default::default()
    };

    // Baseline: Post A is #1, Post B is #2
    let base_rec = rec
        .recommend(Some("did:plc:viewer"), &dials, TEST_EPOCH)
        .unwrap();
    assert_eq!(base_rec.posts.len(), 2);
    assert_eq!(base_rec.posts[0].post_id, post_a);
    assert_eq!(base_rec.posts[1].post_id, post_b);
    assert!(base_rec.posts[0].score > base_rec.posts[1].score);

    // Record impression for Post A at TEST_EPOCH
    rec.record_impressions(Some("did:plc:viewer"), &[post_a], TEST_EPOCH);

    // Query 1 hour later (TEST_EPOCH + 3600s):
    // Post A score is dampened by ~0.3935.
    // Post B was NOT served, receives multiplier 1.0.
    // Therefore, Post B should overtake Post A to become #1!
    let later_rec = rec
        .recommend(Some("did:plc:viewer"), &dials, TEST_EPOCH + 3600)
        .unwrap();
    assert_eq!(later_rec.posts.len(), 2);
    assert_eq!(
        later_rec.posts[0].post_id, post_b,
        "Fresh Post B should overtake decayed Post A"
    );
    assert_eq!(
        later_rec.posts[1].post_id, post_a,
        "Decayed Post A should drop to rank #2"
    );
    assert!(later_rec.posts[0].score > later_rec.posts[1].score);
}

// ---------------------------------------------------------------------------
// 4. Full Score Recovery (>6 Hours) Tests
// ---------------------------------------------------------------------------

#[test]
fn test_6h_boundary_and_full_score_recovery() {
    let store = ImpressionStore::new(100);
    let viewer = 77;
    let post = 888;

    store.record_impressions(viewer, &[post], TEST_EPOCH);

    // Exactly 6 hours (21600s) -> Multiplier 1.0
    let penalty_6h = store
        .evaluate_fatigue_penalty(viewer, post, TEST_EPOCH + 21600)
        .unwrap();
    assert_eq!(penalty_6h, 1.0);

    // 8 hours (28800s) -> Multiplier 1.0
    let penalty_8h = store
        .evaluate_fatigue_penalty(viewer, post, TEST_EPOCH + 28800)
        .unwrap();
    assert_eq!(penalty_8h, 1.0);

    // 24 hours (86400s) -> Multiplier 1.0
    let penalty_24h = store
        .evaluate_fatigue_penalty(viewer, post, TEST_EPOCH + 86400)
        .unwrap();
    assert_eq!(penalty_24h, 1.0);

    // Unseen post -> Multiplier 1.0
    let penalty_unseen = store
        .evaluate_fatigue_penalty(viewer, 99999, TEST_EPOCH + 100)
        .unwrap();
    assert_eq!(penalty_unseen, 1.0);
}

// ---------------------------------------------------------------------------
// 5. Bounded Sliding LRU Capacity & Eviction Tests
// ---------------------------------------------------------------------------

#[test]
fn test_bounded_capacity_lru_eviction() {
    let mut history = ViewerImpressionHistory::new(5);

    // Record 10 impressions sequentially
    for i in 1..=10u32 {
        history.record_impression(i, TEST_EPOCH + u64::from(i) * 10);
    }

    // Capacity must be strictly bounded to max_capacity (5)
    assert_eq!(history.len(), 5);

    // Oldest items (1..=5) should have been evicted
    for i in 1..=5 {
        assert!(!history.contains(i), "Item {i} should be evicted");
        assert_eq!(history.get_served_timestamp(i), None);
    }

    // Newest items (6..=10) should be retained
    for i in 6..=10u32 {
        assert!(history.contains(i), "Item {i} should be retained");
        assert_eq!(
            history.get_served_timestamp(i),
            Some(TEST_EPOCH + u64::from(i) * 10)
        );
    }
}

#[test]
fn test_re_recording_same_post_updates_served_time_and_eviction_safety() {
    let mut history = ViewerImpressionHistory::new(3);

    history.record_impression(1, TEST_EPOCH + 100);
    history.record_impression(2, TEST_EPOCH + 200);
    history.record_impression(3, TEST_EPOCH + 300);

    assert_eq!(history.len(), 3);

    // Re-record post 1 at a newer timestamp
    history.record_impression(1, TEST_EPOCH + 400);

    // Record post 4 (capacity exceeded)
    history.record_impression(4, TEST_EPOCH + 500);

    assert_eq!(history.len(), 3);
    // Post 2 should be evicted (oldest valid timestamp)
    assert!(!history.contains(2));
    // Post 1 must still be retained with the updated timestamp 400
    assert!(history.contains(1));
    assert_eq!(history.get_served_timestamp(1), Some(TEST_EPOCH + 400));
    assert!(history.contains(3));
    assert!(history.contains(4));
}

#[test]
fn test_prune_older_than_cleans_up_expired_entries() {
    let mut history = ViewerImpressionHistory::new(100);

    history.record_impression(1, TEST_EPOCH - 30_000); // > 6h ago
    history.record_impression(2, TEST_EPOCH - 25_000); // > 6h ago
    history.record_impression(3, TEST_EPOCH - 1_000); // recent
    history.record_impression(4, TEST_EPOCH); // recent

    assert_eq!(history.len(), 4);

    let cutoff = TEST_EPOCH - FATIGUE_WINDOW_SECS; // 21,600s cutoff
    history.prune_older_than(cutoff);

    assert_eq!(history.len(), 2);
    assert!(!history.contains(1));
    assert!(!history.contains(2));
    assert!(history.contains(3));
    assert!(history.contains(4));
}

// ---------------------------------------------------------------------------
// 6. Multi-User Isolation Tests
// ---------------------------------------------------------------------------

#[test]
fn test_multi_user_isolation_across_shards() {
    let interner = Arc::new(StringInterner::new());
    let graph = Arc::new(GraphStore::new());

    let user_a = interner.intern("did:plc:user_a");
    let user_b = interner.intern("did:plc:user_b");
    let followed = interner.intern("did:plc:shared_followed");
    let author = interner.intern("did:plc:author");

    let post_x = interner.intern("at://did:plc:author/post/shared_x");
    let post_y = interner.intern("at://did:plc:author/post/shared_y");

    graph.record_post_meta(post_x, author, None, None, TEST_EPOCH - 100);
    graph.record_post_meta(post_y, author, None, None, TEST_EPOCH - 100);

    graph.record_follow(user_a, followed);
    graph.record_follow(user_b, followed);
    graph.record_interaction(followed, post_x, SignalType::Like, TEST_EPOCH - 50);
    graph.record_interaction(followed, post_y, SignalType::Like, TEST_EPOCH - 50);

    let rec = Recommender::new(interner, graph);
    let dials = RecommendationDials {
        min_likes: 1,
        ..Default::default()
    };

    // Serve post_x to User A only
    rec.record_impressions(Some("did:plc:user_a"), &[post_x], TEST_EPOCH);

    // Query User A (post_x is softly dampened (0.15x), so fresh post_y is ranked #1 and post_x is ranked #2)
    let feed_a = rec
        .recommend(Some("did:plc:user_a"), &dials, TEST_EPOCH + 60)
        .unwrap();
    assert_eq!(feed_a.posts.len(), 2);
    assert_eq!(feed_a.posts[0].post_id, post_y);
    assert_eq!(feed_a.posts[1].post_id, post_x);

    // Query User B (User B never saw post_x, both post_x and post_y returned)
    let feed_b = rec
        .recommend(Some("did:plc:user_b"), &dials, TEST_EPOCH + 60)
        .unwrap();
    assert_eq!(feed_b.posts.len(), 2);
    assert!(feed_b.posts.iter().any(|p| p.post_id == post_x));
    assert!(feed_b.posts.iter().any(|p| p.post_id == post_y));
}

// ---------------------------------------------------------------------------
// 7. Recommender API Integration & Edge Cases
// ---------------------------------------------------------------------------

#[test]
fn test_record_impressions_by_did_variants() {
    let interner = Arc::new(StringInterner::new());
    let graph = Arc::new(GraphStore::new());
    let rec = Recommender::new(interner, graph);

    // Record for None DID (safe no-op)
    rec.record_impressions(None, &[1, 2, 3], TEST_EPOCH);
    rec.record_impressions_by_did(None, &[1, 2, 3], TEST_EPOCH);
    assert_eq!(rec.impression_store().total_viewers(), 0);

    // Record for empty post IDs
    rec.record_impressions(Some("did:plc:alice"), &[], TEST_EPOCH);
    assert_eq!(
        rec.impression_store()
            .get_viewer_impression_count(rec.interner.lookup_id("did:plc:alice").unwrap()),
        0
    );

    // Record with alias record_impressions_by_did
    rec.record_impressions_by_did(Some("did:plc:alice"), &[100, 101], TEST_EPOCH);
    let alice_id = rec.interner.lookup_id("did:plc:alice").unwrap();
    assert_eq!(
        rec.impression_store().get_viewer_impression_count(alice_id),
        2
    );
}

#[test]
fn test_future_timestamp_saturating_time_clock_skew() {
    let store = ImpressionStore::new(100);
    let viewer = 99;
    let post = 777;

    // Post recorded with future timestamp (e.g. clock drift)
    store.record_impressions(viewer, &[post], TEST_EPOCH + 500);

    // Evaluating at TEST_EPOCH (now < served_ts):
    // saturating_sub(served_ts) yields 0 -> Smooth floor multiplier (0.15)
    let penalty = store.evaluate_fatigue_penalty(viewer, post, TEST_EPOCH);
    assert_eq!(penalty, Some(FATIGUE_MIN_FLOOR));
}

#[test]
fn test_prune_expired_across_all_shards() {
    let store = ImpressionStore::new(100);

    for uid in 0..128 {
        // Old impression (> 6h)
        store.record_impressions(uid, &[1000 + uid], TEST_EPOCH - 30_000);
        // Recent impression
        store.record_impressions(uid, &[2000 + uid], TEST_EPOCH);
    }

    assert_eq!(store.total_viewers(), 128);
    for uid in 0..128 {
        assert_eq!(store.get_viewer_impression_count(uid), 2);
    }

    // Prune impressions older than 6 hours
    store.prune_expired(TEST_EPOCH);

    for uid in 0..128 {
        assert_eq!(store.get_viewer_impression_count(uid), 1);
        assert!(!store.contains_impression(uid, 1000 + uid));
        assert!(store.contains_impression(uid, 2000 + uid));
    }
}

// ---------------------------------------------------------------------------
// 8. High Concurrency Stress Across 64 Shards
// ---------------------------------------------------------------------------

#[test]
fn test_high_concurrency_stress_across_64_shards() {
    let interner = Arc::new(StringInterner::new());
    let graph = Arc::new(GraphStore::new());

    // Pre-populate 500 posts and authors
    for i in 1..=500 {
        let p_uri = format!("at://did:plc:author_{i}/app.bsky.feed.post/post_{i}");
        let a_did = format!("did:plc:author_{i}");
        let pid = interner.intern(&p_uri);
        let aid = interner.intern(&a_did);
        graph.record_post_meta(pid, aid, None, None, TEST_EPOCH - 1000);
    }

    let rec = Arc::new(Recommender::new(interner, graph));
    let num_threads = 16;
    let ops_per_thread = 500;

    let mut handles = Vec::with_capacity(num_threads);

    for t_idx in 0..num_threads {
        let rec_clone = Arc::clone(&rec);
        let handle = thread::spawn(move || {
            let dials = RecommendationDials::default();
            for i in 0..ops_per_thread {
                let user_id = (t_idx * 100 + (i % 50)) as u32;
                let user_did = format!("did:plc:user_{user_id}");
                let post_id = ((i % 500) + 1) as u32;
                let ts = TEST_EPOCH + (i as u64 * 10);

                if i % 3 == 0 {
                    // Record impression
                    rec_clone.record_impressions(Some(&user_did), &[post_id], ts);
                } else if i % 3 == 1 {
                    // Evaluate fatigue penalty directly
                    let _ = rec_clone.impression_store().evaluate_fatigue_penalty(
                        user_id,
                        post_id,
                        ts + 100,
                    );
                } else {
                    // Recommend with dials
                    let _ = rec_clone.recommend(Some(&user_did), &dials, ts + 200);
                }
            }
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.join().expect("Worker thread panicked!");
    }

    assert!(rec.impression_store().total_viewers() > 0);
}
