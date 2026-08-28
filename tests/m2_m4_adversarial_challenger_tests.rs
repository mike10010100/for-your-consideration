#![forbid(unsafe_code)]
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic, missing_docs)]

use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::Instant;

use for_your_consideration::prelude::*;

/// 1. High-throughput mutation bursts (~1,000 to 10,000 ev/s) and sub-1ms candidate retrieval
#[test]
fn test_adversarial_high_throughput_mutation_burst_cache_hits() {
    let graph = Arc::new(GraphStore::new());
    let base_time = BLUESKY_EPOCH_SECS + 500_000;

    // Seed 50 candidate posts
    for post_id in 1..=50 {
        let author_id = 1000 + post_id;
        graph.record_post_meta(post_id, author_id, None, None, base_time);
        for u in 1..=post_id {
            graph.record_interaction(
                u,
                post_id,
                SignalType::Like,
                base_time + (u64::from(u) % 100),
            );
        }
    }

    // Initial query to populate cache
    let t0 = base_time + 1000;
    let initial_candidates = graph.get_velocity_pool_candidates_at(t0, 20);
    assert_eq!(initial_candidates.len(), 20);
    assert_eq!(initial_candidates[0], 50); // Post 50 had the most likes

    // Spawn 4 writer threads simulating a massive ingestion burst of >= 5,000 events
    let running = Arc::new(AtomicBool::new(true));
    let mutation_count = Arc::new(AtomicU64::new(0));
    let mut writer_handles = Vec::new();

    for thread_idx in 0..4 {
        let graph_clone = Arc::clone(&graph);
        let running_clone = Arc::clone(&running);
        let count_clone = Arc::clone(&mutation_count);
        let handle = std::thread::spawn(move || {
            let mut iter = 0u32;
            while running_clone.load(Ordering::Relaxed) {
                let user_id = 5000 + (thread_idx * 10_000) + (iter % 1000);
                let post_id = 1 + (iter % 50);
                graph_clone.record_interaction(
                    user_id,
                    post_id,
                    SignalType::Like,
                    t0 + 2, // Within the 10s TTL window
                );
                count_clone.fetch_add(1, Ordering::Relaxed);
                iter = iter.wrapping_add(1);
            }
        });
        writer_handles.push(handle);
    }

    // While writers are bursting, execute cache-hit read queries and verify sub-1ms latency
    let query_time = t0 + 4; // 4 seconds after t0 (< 10s TTL)
    let mut hit_latencies = Vec::new();

    // Loop until writers have produced at least 5,000 mutations to guarantee high-burst conditions
    while mutation_count.load(Ordering::Relaxed) < 5000 {
        let start = Instant::now();
        let candidates = graph.get_velocity_pool_candidates_at(query_time, 20);
        let elapsed = start.elapsed();
        hit_latencies.push(elapsed);

        // Verify result matches cached output exactly (no re-scan)
        assert_eq!(candidates, initial_candidates);
    }

    // Stop writers
    running.store(false, Ordering::Relaxed);
    for handle in writer_handles {
        let _ = handle.join();
    }

    let total_mutations = mutation_count.load(Ordering::Relaxed);
    assert!(
        total_mutations >= 5000,
        "Expected >= 5000 mutations, got {total_mutations}"
    );

    // Compute latency statistics
    hit_latencies.sort();
    let p50 = hit_latencies[hit_latencies.len() / 2];
    let p95 = hit_latencies[(hit_latencies.len() * 95) / 100];
    let p99 = hit_latencies[(hit_latencies.len() * 99) / 100];
    let max = hit_latencies[hit_latencies.len() - 1];

    println!(
        "Cache Hit Latency across {} queries under {total_mutations} concurrent mutations: p50={p50:?}, p95={p95:?}, p99={p99:?}, max={max:?}",
        hit_latencies.len()
    );

    // Hard requirement: sub-1ms (1,000,000 ns = 1000 µs) retrieval for p99 on cache hits
    assert!(
        p99.as_micros() < 1000,
        "P99 cache hit latency must be < 1ms, got {:?}",
        p99
    );
}

/// 2. TTL Expiry (>10s): Verify fresh candidate re-computation at and beyond 10s boundary
#[test]
fn test_adversarial_ttl_expiry_and_decay_recomputation() {
    let graph = GraphStore::new();
    let base_time = BLUESKY_EPOCH_SECS + 600_000;

    // Seed post 1 with moderate activity at base_time
    graph.record_post_meta(1, 101, None, None, base_time);
    for u in 1..=5 {
        graph.record_interaction(u, 1, SignalType::Like, base_time);
    }

    // Seed post 2 with low activity at base_time
    graph.record_post_meta(2, 102, None, None, base_time);
    graph.record_interaction(1, 2, SignalType::Like, base_time);

    // Query 1 at t = base_time + 100: sets cache (evaluated_at = base_time + 100)
    let t0 = base_time + 100;
    let res_t0 = graph.get_velocity_pool_candidates_at(t0, 10);
    assert_eq!(res_t0, vec![1, 2]);

    // Add a viral post 999 at t = t0 + 2 with 100 likes
    graph.record_post_meta(999, 999, None, None, t0 + 2);
    for u in 100..200 {
        graph.record_interaction(u, 999, SignalType::Like, t0 + 2);
    }

    // Query at t0 + 9 (elapsed = 9s < 10s TTL): Must hit cache and NOT include post 999
    let res_t9 = graph.get_velocity_pool_candidates_at(t0 + 9, 10);
    assert_eq!(res_t9, vec![1, 2]);
    assert!(!res_t9.contains(&999));

    // Query at t0 + 10 (elapsed = 10s >= VELOCITY_CACHE_TTL_SECS = 10s): Must expire and recompute
    let res_t10 = graph.get_velocity_pool_candidates_at(t0 + 10, 10);
    assert!(res_t10.contains(&999));
    assert_eq!(res_t10[0], 999); // Viral post 999 must now be #1

    // Add another super viral post 888 at t0 + 12 with 500 reposts
    graph.record_post_meta(888, 888, None, None, t0 + 12);
    for u in 300..800 {
        graph.record_interaction(u, 888, SignalType::Repost, t0 + 12);
    }

    // Query at t0 + 15 (elapsed = 5s since t0+10 recomputation): Must hit cache and NOT contain 888 yet
    let res_t15 = graph.get_velocity_pool_candidates_at(t0 + 15, 10);
    assert_eq!(res_t15, res_t10);
    assert!(!res_t15.contains(&888));

    // Query at t0 + 21 (elapsed = 11s > 10s TTL): Must expire and place 888 as #1
    let res_t21 = graph.get_velocity_pool_candidates_at(t0 + 21, 10);
    assert_eq!(res_t21[0], 888);
    assert_eq!(res_t21[1], 999);
}

/// 3. Clock-warp safety: simulate backwards clock jumps and verify graceful re-evaluation without panic
#[test]
fn test_adversarial_clock_warp_backward_and_forward_jumps() {
    let graph = GraphStore::new();
    let base_time = BLUESKY_EPOCH_SECS + 700_000;

    graph.record_post_meta(100, 10, None, None, base_time);
    graph.record_interaction(1, 100, SignalType::Like, base_time);
    graph.record_post_meta(200, 20, None, None, base_time);
    graph.record_interaction(2, 200, SignalType::Repost, base_time);

    // Initial query at t = base_time + 10_000
    let evaluated_t = base_time + 10_000;
    let initial = graph.get_velocity_pool_candidates_at(evaluated_t, 10);
    assert_eq!(initial.len(), 2);

    // 1. Backward clock jump by 1 second (evaluated_t - 1)
    let jump_minus_1 = graph.get_velocity_pool_candidates_at(evaluated_t - 1, 10);
    assert_eq!(jump_minus_1.len(), 2);

    // 2. Backward clock jump by 5,000 seconds
    let jump_minus_5k = graph.get_velocity_pool_candidates_at(evaluated_t - 5_000, 10);
    assert_eq!(jump_minus_5k.len(), 2);

    // 3. Extreme backward clock jump to 0 (epoch start)
    let jump_to_zero = graph.get_velocity_pool_candidates_at(0, 10);
    // At t = 0, no posts exist within 6-hour window of t=0, so result is empty
    assert!(jump_to_zero.is_empty());

    // 4. Backward jump followed by forward re-population
    let post_warp = graph.get_velocity_pool_candidates_at(base_time + 50, 10);
    assert_eq!(post_warp.len(), 2);

    // 5. Extreme forward jump (100,000,000 seconds into the future)
    let extreme_future = graph.get_velocity_pool_candidates_at(base_time + 100_000_000, 10);
    // Posts from base_time are far outside the 6-hour window of base_time + 100_000_000
    assert!(extreme_future.is_empty());

    // 6. Limit edge cases: limit = 0, limit = usize::MAX, limit = 1
    assert!(graph
        .get_velocity_pool_candidates_at(base_time + 50, 0)
        .is_empty());
    let top_1 = graph.get_velocity_pool_candidates_at(base_time + 50, 1);
    assert_eq!(top_1.len(), 1);
    let all_candidates = graph.get_velocity_pool_candidates_at(base_time + 50, usize::MAX);
    assert_eq!(all_candidates.len(), 2);
}

/// 4. Cache Invalidation Discipline on `clear()`, `prune_older_than()`, and `restore_from_snapshot()`
#[test]
fn test_adversarial_cache_invalidation_lifecycle_discipline() {
    let graph = GraphStore::new();
    let base_time = BLUESKY_EPOCH_SECS + 800_000;

    // Seed post 55
    graph.record_post_meta(55, 1, None, None, base_time);
    graph.record_interaction(1, 55, SignalType::Like, base_time);

    // Query populates cache
    let res1 = graph.get_velocity_pool_candidates_at(base_time + 2, 10);
    assert_eq!(res1, vec![55]);

    // (A) Test clear() invalidation
    graph.clear();
    // Cache must be None; querying at same timestamp must recompute on empty graph -> empty vec
    let res_after_clear = graph.get_velocity_pool_candidates_at(base_time + 2, 10);
    assert!(
        res_after_clear.is_empty(),
        "clear() must invalidate velocity cache"
    );

    // (B) Test prune_older_than() invalidation
    graph.record_post_meta(66, 2, None, None, base_time + 100);
    graph.record_interaction(2, 66, SignalType::Like, base_time + 100);
    let res2 = graph.get_velocity_pool_candidates_at(base_time + 102, 10);
    assert_eq!(res2, vec![66]);

    // Prune everything up to base_time + 200
    graph.prune_older_than(base_time + 200);
    let res_after_prune = graph.get_velocity_pool_candidates_at(base_time + 102, 10);
    assert!(
        res_after_prune.is_empty(),
        "prune_older_than() must invalidate velocity cache"
    );

    // (C) Test restore_from_snapshot() invalidation
    graph.record_post_meta(77, 3, None, None, base_time + 300);
    graph.record_interaction(3, 77, SignalType::Like, base_time + 300);
    let res3 = graph.get_velocity_pool_candidates_at(base_time + 302, 10);
    assert_eq!(res3, vec![77]);

    // Restore snapshot containing only post 99
    let mut snap = GraphSnapshotData::default();
    snap.post_metadata.push((
        99,
        PostMeta {
            author_id: 9,
            root_id: None,
            parent_id: None,
            created_at: base_time + 400,
        },
    ));
    snap.active_recent_posts.push((99, base_time + 400));
    snap.post_interactions.push((
        99,
        vec![CompactEdge::new(9, SignalType::Like, base_time + 400)],
    ));
    graph.restore_from_snapshot(snap);

    // Query must return post 99 from snapshot, not post 77 from old cache
    let res_after_restore = graph.get_velocity_pool_candidates_at(base_time + 405, 10);
    assert_eq!(
        res_after_restore,
        vec![99],
        "restore_from_snapshot() must invalidate velocity cache"
    );
}

/// 5. High Concurrency Race Condition Stress Test: Concurrent Readers and Writers with Cache Invalidation
#[test]
fn test_adversarial_concurrent_readers_writers_and_invalidation() {
    let graph = Arc::new(GraphStore::new());
    let base_time = BLUESKY_EPOCH_SECS + 900_000;

    // Seed baseline posts
    for i in 1..=20 {
        graph.record_post_meta(i, 100 + i, None, None, base_time);
        graph.record_interaction(10, i, SignalType::Like, base_time + 1);
    }

    let running = Arc::new(AtomicBool::new(true));
    let mut handles = Vec::new();

    // 4 Concurrent Writers
    for writer_id in 0..4 {
        let g = Arc::clone(&graph);
        let r = Arc::clone(&running);
        handles.push(std::thread::spawn(move || {
            let mut i = 0u32;
            while r.load(Ordering::Relaxed) {
                let pid = 1 + (i % 20);
                let uid = 1000 + (writer_id * 10_000) + (i % 500);
                g.record_interaction(uid, pid, SignalType::Like, base_time + 5);
                i = i.wrapping_add(1);
            }
        }));
    }

    // 4 Concurrent Readers querying get_velocity_pool_candidates_at
    let read_count = Arc::new(AtomicU64::new(0));
    for _ in 0..4 {
        let g = Arc::clone(&graph);
        let r = Arc::clone(&running);
        let rc = Arc::clone(&read_count);
        handles.push(std::thread::spawn(move || {
            while r.load(Ordering::Relaxed) {
                let candidates = g.get_velocity_pool_candidates_at(base_time + 5, 10);
                // Must always be non-empty and bounded by limit
                assert!(!candidates.is_empty());
                assert!(candidates.len() <= 10);
                rc.fetch_add(1, Ordering::Relaxed);
            }
        }));
    }

    // Run for 200 milliseconds under high contention
    std::thread::sleep(std::time::Duration::from_millis(200));

    // Invalidate cache while threads are actively running
    graph.prune_older_than(base_time);

    // Let them run for another 100 milliseconds
    std::thread::sleep(std::time::Duration::from_millis(100));

    running.store(false, Ordering::Relaxed);
    for h in handles {
        let _ = h.join();
    }

    let total_reads = read_count.load(Ordering::Relaxed);
    assert!(
        total_reads > 500,
        "Expected >= 500 concurrent reads, got {total_reads}"
    );
}
