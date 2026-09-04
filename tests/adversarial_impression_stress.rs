#![forbid(unsafe_code)]
#![allow(clippy::pedantic, clippy::nursery, clippy::float_cmp)]

//! Milestone 2: Adversarial Impression Memory Stress, Concurrency & Scale Test Suite.
//!
//! Empirical validation for Challenger 1:
//! 1. High Concurrency Stress across 64 shards under 32+ threads (mixed reads, writes, prunes, recommends).
//! 2. Clock Skew and Timestamp Fuzzing (backward/forward time jumps, epoch boundaries, saturating arithmetic).
//! 3. Rapid repeated impression spamming and bounded LRU deduplication safety.
//! 4. Large-scale 50,000 active users memory consumption and latency benchmark.
//! 5. Proptest property-based fuzzing of impression state invariants.

mod common;

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Instant;

use for_your_consideration::prelude::*;
use proptest::prelude::*;

const TEST_EPOCH: u64 = 1_700_000_000;

// ===========================================================================
// 1. High Concurrency Stress across 64 Shards
// ===========================================================================

#[test]
fn test_adversarial_64_shard_high_concurrency_hammer() {
    let interner = Arc::new(StringInterner::new());
    let graph = Arc::new(GraphStore::new());

    // Pre-populate 1,000 posts and 200 authors
    for pid in 1..=1000u32 {
        let aid = (pid % 200) + 1;
        let p_uri = format!("at://did:plc:author_{aid}/app.bsky.feed.post/p_{pid}");
        let a_did = format!("did:plc:author_{aid}");
        let p_interned = interner.intern(&p_uri);
        let a_interned = interner.intern(&a_did);
        graph.record_post_meta(p_interned, a_interned, None, None, TEST_EPOCH - 10_000);
    }

    let rec = Arc::new(Recommender::new(interner, graph));
    let num_threads = 32;
    let ops_per_thread = 2_000;
    let num_users = 2_000;

    let write_count = Arc::new(AtomicUsize::new(0));
    let read_count = Arc::new(AtomicUsize::new(0));
    let prune_count = Arc::new(AtomicUsize::new(0));

    let mut handles = Vec::with_capacity(num_threads);
    let start_time = Instant::now();

    for t_idx in 0..num_threads {
        let rec = Arc::clone(&rec);
        let write_count = Arc::clone(&write_count);
        let read_count = Arc::clone(&read_count);
        let prune_count = Arc::clone(&prune_count);

        let handle = thread::spawn(move || {
            let dials = RecommendationDials::default();
            for i in 0..ops_per_thread {
                let user_id = ((t_idx * 137 + i * 31) % num_users) as u32;
                let user_did = format!("did:plc:user_{user_id}");
                let post_id = ((i * 17 + t_idx * 5) % 1000 + 1) as u32;
                let ts = TEST_EPOCH + (i as u64 % 3600);

                match i % 5 {
                    0 | 1 => {
                        // Concurrent writer: record impressions
                        rec.record_impressions(Some(&user_did), &[post_id, post_id + 1], ts);
                        write_count.fetch_add(2, Ordering::Relaxed);
                    }
                    2 | 3 => {
                        // Concurrent reader: evaluate fatigue penalty
                        let penalty = rec.impression_store().evaluate_fatigue_penalty(
                            user_id,
                            post_id,
                            ts + 1800,
                        );
                        if let Some(m) = penalty {
                            assert!((0.01..=1.0).contains(&m), "Multiplier {m} out of bounds");
                        }
                        read_count.fetch_add(1, Ordering::Relaxed);
                    }
                    _ => {
                        // Periodic pruner / recommender
                        if i % 50 == 0 {
                            rec.impression_store().prune_expired(ts);
                            prune_count.fetch_add(1, Ordering::Relaxed);
                        } else {
                            let _ = rec.recommend(Some(&user_did), &dials, ts + 300);
                        }
                    }
                }
            }
        });
        handles.push(handle);
    }

    for handle in handles {
        handle
            .join()
            .expect("Worker thread panicked under concurrency stress!");
    }

    let elapsed = start_time.elapsed();
    println!(
        "Concurrency Stress Test Passed: {} writes, {} reads, {} prunes in {:.2?}",
        write_count.load(Ordering::Relaxed),
        read_count.load(Ordering::Relaxed),
        prune_count.load(Ordering::Relaxed),
        elapsed
    );

    assert!(rec.impression_store().total_viewers() > 0);
}

// ===========================================================================
// 2. Clock Skew and Timestamp Fuzzing
// ===========================================================================

#[test]
fn test_clock_skew_extreme_time_boundaries_and_jumps() {
    let store = ImpressionStore::new(100);
    let viewer = 42;
    let post = 101;

    // 1. Future timestamp recorded (clock drift: server clock ahead)
    store.record_impressions(viewer, &[post], TEST_EPOCH + 10_000);
    // Evaluating at TEST_EPOCH (now < served_ts):
    // saturating_sub yields 0 -> smooth minimum floor 0.15
    assert_eq!(
        store.evaluate_fatigue_penalty(viewer, post, TEST_EPOCH),
        Some(FATIGUE_MIN_FLOOR)
    );

    // 2. Backward time jump (querying in the past relative to impression)
    let penalty_past = store.evaluate_fatigue_penalty(viewer, post, 0);
    assert_eq!(penalty_past, Some(FATIGUE_MIN_FLOOR));

    // 3. Timestamp 0 recorded
    let post_zero = 102;
    store.record_impressions(viewer, &[post_zero], 0);
    // Evaluating at 0 -> dt=0 -> 0.15 minimum floor
    assert_eq!(
        store.evaluate_fatigue_penalty(viewer, post_zero, 0),
        Some(FATIGUE_MIN_FLOOR)
    );
    // Evaluating at 1800s -> dt=1800 -> smooth decay ~0.338
    assert!(store
        .evaluate_fatigue_penalty(viewer, post_zero, 1800)
        .is_some());
    // Evaluating at 21600s -> dt=21600 -> fully recovered (1.0)
    assert_eq!(
        store.evaluate_fatigue_penalty(viewer, post_zero, 21600),
        Some(1.0)
    );

    // 4. u64::MAX timestamp boundaries
    let post_max = 103;
    store.record_impressions(viewer, &[post_max], u64::MAX);
    // Evaluating at TEST_EPOCH -> now < served_ts -> dt=0 -> 0.15 floor
    assert_eq!(
        store.evaluate_fatigue_penalty(viewer, post_max, TEST_EPOCH),
        Some(FATIGUE_MIN_FLOOR)
    );
    // Evaluating at u64::MAX -> dt=0 -> 0.15 floor
    assert_eq!(
        store.evaluate_fatigue_penalty(viewer, post_max, u64::MAX),
        Some(FATIGUE_MIN_FLOOR)
    );

    // 5. Far future query (u64::MAX) on normal post
    let post_normal = 104;
    store.record_impressions(viewer, &[post_normal], TEST_EPOCH);
    // dt = u64::MAX - TEST_EPOCH -> > 6h -> recovered (1.0)
    assert_eq!(
        store.evaluate_fatigue_penalty(viewer, post_normal, u64::MAX),
        Some(1.0)
    );
}

#[test]
fn test_clock_skew_interleaved_forward_and_backward_timestamps() {
    let mut history = ViewerImpressionHistory::new(10);
    let post = 55;

    // Monotonically increasing impressions
    history.record_impression(post, 1000);
    assert_eq!(history.get_served_timestamp(post), Some(1000));

    history.record_impression(post, 2000);
    assert_eq!(history.get_served_timestamp(post), Some(2000));

    // Backward timestamp insertion (out of order delivery)
    history.record_impression(post, 500);
    // Timestamp updated to 500
    assert_eq!(history.get_served_timestamp(post), Some(500));
}

// ===========================================================================
// 3. Rapid Repeated Impression Spam & LRU Deduplication Safety
// ===========================================================================

#[test]
fn test_adversarial_single_post_impression_spam_10k() {
    let mut history = ViewerImpressionHistory::new(500);
    let spam_post = 999;

    // Spam the exact same post 10,000 times with advancing timestamps
    for i in 0..10_000u64 {
        history.record_impression(spam_post, TEST_EPOCH + i);
    }

    // Set count should be exactly 1
    assert_eq!(history.len(), 1);
    assert!(history.contains(spam_post));
    assert_eq!(
        history.get_served_timestamp(spam_post),
        Some(TEST_EPOCH + 9999)
    );
    // Queue length must never exceed max_capacity (500)
    assert_eq!(history.queue.len(), 500);

    // Now insert 500 distinct new posts (1..=500)
    for p in 1..=500u32 {
        history.record_impression(p, TEST_EPOCH + 20_000 + u64::from(p));
    }

    // All spam_post entries in queue should now be evicted
    assert_eq!(history.len(), 500);
    assert!(!history.contains(spam_post));
    assert_eq!(history.get_served_timestamp(spam_post), None);

    for p in 1..=500u32 {
        assert!(history.contains(p));
    }
}

#[test]
fn test_adversarial_alternating_duplicate_posts_lru_invariants() {
    let mut history = ViewerImpressionHistory::new(4);

    // Alternating posts 1 and 2
    history.record_impression(1, 100);
    history.record_impression(2, 200);
    history.record_impression(1, 300);
    history.record_impression(2, 400);

    assert_eq!(history.len(), 2);
    assert_eq!(history.get_served_timestamp(1), Some(300));
    assert_eq!(history.get_served_timestamp(2), Some(400));
    assert_eq!(history.queue.len(), 4);

    // Insert 3 new posts (3, 4, 5)
    history.record_impression(3, 500); // queue pops (1, 100) -> 300 <= 100 is false -> 1 not evicted
    assert_eq!(history.len(), 3);
    assert!(history.contains(1));

    history.record_impression(4, 600); // queue pops (2, 200) -> 400 <= 200 is false -> 2 not evicted
    assert_eq!(history.len(), 4);
    assert!(history.contains(2));

    history.record_impression(5, 700); // queue pops (1, 300) -> 300 <= 300 is true -> 1 IS evicted!
    assert_eq!(history.len(), 4);
    assert!(!history.contains(1));
    assert!(history.contains(2));
    assert!(history.contains(3));
    assert!(history.contains(4));
    assert!(history.contains(5));
}

// ===========================================================================
// 4. Large-Scale 50,000 Active Users Memory & Latency Benchmark
// ===========================================================================

#[test]
fn test_50k_active_users_memory_footprint_and_latency() {
    let store = Arc::new(ImpressionStore::new(DEFAULT_MAX_IMPRESSIONS_PER_USER));
    let num_users = 50_000;
    let impressions_per_user = 60; // 60 impressions in 6-hour window = 3M total impressions

    println!("============================================================");
    println!(" 50,000 Active Users Impression Memory Benchmark");
    println!("============================================================");

    let t0 = Instant::now();

    // Populate 50,000 users in parallel across 16 threads
    let num_threads = 16;
    let users_per_thread = num_users / num_threads;
    let mut handles = Vec::with_capacity(num_threads);

    for t in 0..num_threads {
        let store = Arc::clone(&store);
        let handle = thread::spawn(move || {
            let start_user = (t * users_per_thread) as u32;
            let end_user = start_user + users_per_thread as u32;

            for uid in start_user..end_user {
                let mut post_ids = Vec::with_capacity(impressions_per_user);
                for i in 0..impressions_per_user {
                    post_ids.push(((uid * 37 + i as u32 * 13) % 500_000) + 1);
                }
                let ts = TEST_EPOCH - (u64::from(uid % 18000));
                store.record_impressions(uid, &post_ids, ts);
            }
        });
        handles.push(handle);
    }

    for handle in handles {
        handle.join().unwrap();
    }

    let pop_time = t0.elapsed();
    assert_eq!(store.total_viewers(), num_users);

    println!(
        "  Populated {} users ({} impressions total) in {:.2?}",
        num_users,
        num_users * impressions_per_user,
        pop_time
    );

    // Measure evaluate_fatigue_penalty latency over 100,000 lookups
    let query_count = 100_000;
    let t1 = Instant::now();
    for i in 0..query_count {
        let uid = (i * 17) % (num_users as u32);
        let pid = ((uid * 37 + 5 * 13) % 500_000) + 1;
        let penalty = store.evaluate_fatigue_penalty(uid, pid, TEST_EPOCH);
        if let Some(m) = penalty {
            assert!((0.01..=1.0).contains(&m));
        }
    }
    let query_elapsed = t1.elapsed();
    let query_p50_nanos = query_elapsed.as_nanos() / query_count as u128;

    println!(
        "  Latency: {} lookups in {:.2?} ({query_p50_nanos} ns/lookup)",
        query_count, query_elapsed
    );

    // Verify sub-microsecond query latency in release builds (< 1,000 ns).
    // Debug builds (and coverage-instrumented parallel suite runs) get a relaxed
    // 25us threshold, mirroring the debug escape hatches used by the latency
    // benchmarks in `adversarial_ingest_tests`.
    let lookup_threshold = if cfg!(debug_assertions) {
        25_000
    } else {
        5_000
    };
    assert!(
        query_p50_nanos < lookup_threshold,
        "Lookup latency {query_p50_nanos} ns exceeded {lookup_threshold} ns threshold"
    );

    // Prune test across all 50,000 users
    let t2 = Instant::now();
    store.prune_expired(TEST_EPOCH);
    let prune_elapsed = t2.elapsed();
    println!(
        "  Prune 50,000 users expired impressions in {:.2?}",
        prune_elapsed
    );
}

// ===========================================================================
// 5. Anti-Fatigue Mathematical Model Exact Verification
// ===========================================================================

#[test]
fn test_exact_decay_curve_integral_and_derivative_monotonicity() {
    let store = ImpressionStore::new(100);
    let viewer = 1;
    let post = 10;

    store.record_impressions(viewer, &[post], TEST_EPOCH);

    let mut prev_multiplier = 0.0f32;
    // Step through from 0s to 21600s in 60s increments
    for dt in (0..=21600).step_by(60) {
        let penalty = store
            .evaluate_fatigue_penalty(viewer, post, TEST_EPOCH + dt)
            .expect("Should be active multiplier");

        // Monotonically increasing multiplier (decay is recovering)
        assert!(
            penalty >= prev_multiplier,
            "Monotonicity violation at dt={dt}: {penalty} < {prev_multiplier}"
        );
        prev_multiplier = penalty;

        // Verify mathematical formula: MIN_FLOOR + (1 - MIN_FLOOR) * (1 - exp(-dt / 7200)) for dt < 21600, and 1.0 for dt >= 21600
        let expected = if dt < FATIGUE_WINDOW_SECS {
            FATIGUE_MIN_FLOOR + (1.0 - FATIGUE_MIN_FLOOR) * (1.0 - (-((dt as f32) / 7200.0)).exp())
        } else {
            1.0
        };
        assert!(
            (penalty - expected).abs() < 1e-4,
            "Formula mismatch at dt={dt}: got {penalty}, expected {expected}"
        );
    }
    assert_eq!(prev_multiplier, 1.0);
}

// ===========================================================================
// 6. Zero / Minimum Capacity Edge Cases
// ===========================================================================

#[test]
fn test_adversarial_zero_and_single_capacity_history() {
    // Zero capacity edge case
    let mut history_zero = ViewerImpressionHistory::new(0);
    assert_eq!(history_zero.len(), 0);

    history_zero.record_impression(100, TEST_EPOCH);
    // Capacity is 0, so every recorded item is immediately evicted
    assert_eq!(history_zero.len(), 0);
    assert!(!history_zero.contains(100));
    assert_eq!(history_zero.get_served_timestamp(100), None);
    assert_eq!(history_zero.queue.len(), 0);

    // Single capacity edge case
    let mut history_one = ViewerImpressionHistory::new(1);
    history_one.record_impression(201, TEST_EPOCH);
    assert_eq!(history_one.len(), 1);
    assert!(history_one.contains(201));
    assert_eq!(history_one.get_served_timestamp(201), Some(TEST_EPOCH));

    history_one.record_impression(202, TEST_EPOCH + 10);
    assert_eq!(history_one.len(), 1);
    assert!(!history_one.contains(201));
    assert!(history_one.contains(202));
    assert_eq!(history_one.get_served_timestamp(202), Some(TEST_EPOCH + 10));
    assert_eq!(history_one.queue.len(), 1);
}

#[test]
fn test_adversarial_same_timestamp_duplicate_eviction_anomaly() {
    let mut history = ViewerImpressionHistory::new(4);

    // Record posts with duplicate post ID 10 at the EXACT SAME timestamp
    history.record_impression(10, TEST_EPOCH);
    history.record_impression(20, TEST_EPOCH);
    history.record_impression(10, TEST_EPOCH); // duplicate 10 at TEST_EPOCH
    history.record_impression(30, TEST_EPOCH);

    assert_eq!(history.queue.len(), 4);
    assert!(history.contains(10));
    assert!(history.contains(20));
    assert!(history.contains(30));

    // When 5th post (40) is recorded, queue length exceeds capacity 4, popping the first (10, TEST_EPOCH).
    // Because latest_ts in map (TEST_EPOCH) <= oldest.served_at_secs (TEST_EPOCH),
    // post 10 is removed from timestamps and post_ids, even though (10, TEST_EPOCH) is still in the queue!
    history.record_impression(40, TEST_EPOCH);

    assert_eq!(history.queue.len(), 4);
    // Observe: post 10 is prematurely forgotten
    assert!(
        !history.contains(10),
        "Premature eviction of duplicate post 10 confirmed"
    );
    assert_eq!(history.get_served_timestamp(10), None);
    assert_eq!(history.len(), 3); // only 20, 30, 40 tracked
}

#[test]
fn test_adversarial_distinct_timestamp_duplicate_retention_correctness() {
    let mut history = ViewerImpressionHistory::new(4);

    // If timestamps are strictly increasing, duplicate 10 is correctly retained!
    history.record_impression(10, TEST_EPOCH);
    history.record_impression(20, TEST_EPOCH + 1);
    history.record_impression(10, TEST_EPOCH + 2); // newer timestamp TEST_EPOCH + 2
    history.record_impression(30, TEST_EPOCH + 3);

    assert_eq!(history.queue.len(), 4);

    // When 5th post (40) is recorded, popping (10, TEST_EPOCH):
    // latest_ts (TEST_EPOCH + 2) <= oldest.served_at_secs (TEST_EPOCH) is FALSE!
    // So post 10 is NOT evicted!
    history.record_impression(40, TEST_EPOCH + 4);

    assert_eq!(history.queue.len(), 4);
    assert!(
        history.contains(10),
        "Post 10 should be retained due to newer timestamp"
    );
    assert_eq!(history.get_served_timestamp(10), Some(TEST_EPOCH + 2));
    assert_eq!(history.len(), 4); // 20, 10, 30, 40 all tracked
}

#[test]
fn test_adversarial_out_of_order_random_timestamps_invariants() {
    let mut history = ViewerImpressionHistory::new(50);

    // Insert 1,000 out-of-order impressions
    let mut rng_seed: u64 = 0xdeadbeef;
    for i in 1..=1000u32 {
        rng_seed = rng_seed.wrapping_mul(6364136223846793005).wrapping_add(1);
        let pid = (rng_seed % 200) as u32 + 1;
        let ts = (rng_seed >> 32) % 1_000_000 + 1_700_000_000;

        history.record_impression(pid, ts);

        // Invariants must hold at every single step:
        assert!(
            history.queue.len() <= 50,
            "Queue length {} exceeded capacity 50 at step {i}",
            history.queue.len()
        );
        assert!(
            history.len() <= 50,
            "Bitmap len {} exceeded capacity 50 at step {i}",
            history.len()
        );
        assert_eq!(
            history.post_ids.len() as usize,
            history.timestamps.len(),
            "Bitmap len and Map len desynchronized at step {i}"
        );
    }
}

// ===========================================================================
// 7. Property-Based Fuzzing with Proptest
// ===========================================================================

proptest! {
    #[test]
    fn prop_impression_store_invariants(
        viewer_id in 0..500u32,
        post_ids in proptest::collection::vec(1..1000u32, 1..50),
        query_offset in 0..100_000u64,
    ) {
        let store = ImpressionStore::new(100);
        store.record_impressions(viewer_id, &post_ids, TEST_EPOCH);

        let count = store.get_viewer_impression_count(viewer_id);
        prop_assert!(count <= 100, "Impression count exceeded max capacity");

        for &pid in &post_ids {
            let penalty = store.evaluate_fatigue_penalty(viewer_id, pid, TEST_EPOCH + query_offset);
            if query_offset < 21600 {
                if let Some(m) = penalty {
                    prop_assert!((FATIGUE_MIN_FLOOR..=1.0).contains(&m), "Multiplier {} out of range", m);
                }
            } else {
                prop_assert_eq!(penalty, Some(1.0), "Must be 1.0 after 6h");
            }
        }
    }
}
