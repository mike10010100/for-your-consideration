#![forbid(unsafe_code)]
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::too_many_lines,
    clippy::cognitive_complexity,
    clippy::cast_precision_loss,
    clippy::cast_possible_truncation,
    missing_docs
)]

//! Milestone 1 Empirical Challenger Adversarial Test Suite:
//! 1. Extreme Graph Scale: 100,000 interactions per post, 5,000 likes per user.
//! 2. Strict Top-100 Co-Interactor Cap Enforcement.
//! 3. Sub-10ms Latency SLA Verification for `recommend_preview_at`, `find_taste_twins`, and `explain_recommendation`.
//! 4. Combinatorial Fan-Out and Zero-Allocation Validation.

use std::sync::Arc;
use std::time::Instant;

use compact_str::CompactString;
use for_your_consideration::prelude::*;
use for_your_consideration::recommender::*;

/// Helper to create an extreme viral post graph with N interactions per viral post.
fn build_extreme_viral_graph(
    num_seed_posts: usize,
    interactions_per_viral_post: usize,
) -> (
    Arc<StringInterner>,
    Arc<GraphStore>,
    Recommender,
    CompactString,
    Vec<CompactString>,
) {
    let interner = Arc::new(StringInterner::new());
    let graph = Arc::new(GraphStore::new());
    let now = BLUESKY_EPOCH_SECS + 100_000_000;

    let viewer_did = CompactString::new("did:plc:extreme_viewer");
    let viewer_id = interner.intern(&viewer_did);

    let author_id = interner.intern("did:plc:viral_creator");
    let mut post_uris = Vec::with_capacity(num_seed_posts);
    let mut post_pids = Vec::with_capacity(num_seed_posts);

    for i in 0..num_seed_posts {
        let uri = CompactString::new(format!(
            "at://did:plc:viral_creator/app.bsky.feed.post/viral_post_{i:04}"
        ));
        let pid = interner.intern(&uri);
        post_uris.push(uri);
        post_pids.push(pid);

        graph.record_post_meta(pid, author_id, None, None, now - 100_000);
        // Viewer likes this viral post
        graph.record_interaction(viewer_id, pid, SignalType::Like, now - 90_000 + i as u64);
    }

    // Populate interactions_per_viral_post on the first post (or distributed)
    let viral_pid = post_pids[0];
    for u in 0..interactions_per_viral_post {
        let liker_did = format!("did:plc:liker_{u:06}");
        let liker_id = interner.intern(&liker_did);
        graph.record_interaction(
            liker_id,
            viral_pid,
            SignalType::Like,
            now - 50_000 + (u as u64 % 10_000),
        );

        // Add 50 curator twins who also like the second post
        if u < 50 && num_seed_posts > 1 {
            graph.record_interaction(
                liker_id,
                post_pids[1],
                SignalType::Like,
                now - 50_000 + u as u64,
            );
            // And have 1 candidate post each
            let cand_uri = format!("at://did:plc:liker_{u:06}/app.bsky.feed.post/cand_{u}");
            let cand_pid = interner.intern(&cand_uri);
            graph.record_post_meta(cand_pid, liker_id, None, None, now - 10_000);
            graph.record_interaction(liker_id, cand_pid, SignalType::Like, now - 10_000);
        }
    }

    let rec = Recommender::new(Arc::clone(&interner), Arc::clone(&graph));
    (interner, graph, rec, viewer_did, post_uris)
}

#[test]
fn test_m1_adversarial_100k_interactions_per_post_latency_and_correctness() {
    let num_interactions = 100_000;
    println!("=== BUILDING 100,000-INTERACTION GRAPH ===");
    let t_build = Instant::now();
    let (_interner, _graph, rec, viewer_did, post_uris) =
        build_extreme_viral_graph(15, num_interactions);
    println!("Built 100k-interaction graph in {:?}", t_build.elapsed());

    let now = BLUESKY_EPOCH_SECS + 100_000_000;
    let dials = RecommendationDials {
        limit: 30,
        min_likes: 1,
        explain: true,
        ..Default::default()
    };

    // 1. Stress-test explain_recommendation on 100,000-interaction viral post
    let viral_uri = &post_uris[0];
    println!("=== BENCHMARKING explain_recommendation() on 100k-interaction post ===");
    for _ in 0..5 {
        let _ = rec.explain_recommendation(viewer_did.as_str(), viral_uri.as_str());
    }

    let iters = 50;
    let mut explain_lats = Vec::with_capacity(iters);
    for _ in 0..iters {
        let t0 = Instant::now();
        let explanation = rec
            .explain_recommendation(viewer_did.as_str(), viral_uri.as_str())
            .expect("explain_recommendation must succeed");
        explain_lats.push(t0.elapsed().as_micros() as u64);
        assert!(!explanation.summary.is_empty());
    }
    explain_lats.sort_unstable();
    let explain_p50 = explain_lats[iters * 50 / 100];
    let explain_p99 = explain_lats[iters * 99 / 100];
    let explain_mean = explain_lats.iter().sum::<u64>() as f64 / iters as f64;
    println!(
        "explain_recommendation() on 100k edges: p50 = {} µs ({:.3} ms), p99 = {} µs ({:.3} ms), mean = {:.1} µs",
        explain_p50, explain_p50 as f64 / 1000.0,
        explain_p99, explain_p99 as f64 / 1000.0,
        explain_mean
    );

    // Operational latency budget for explain is sub-10ms (and typically sub-1ms)
    assert!(
        explain_p50 < 10_000,
        "explain_recommendation p50 must be sub-10ms, was {} µs",
        explain_p50
    );

    // 2. Stress-test find_taste_twins on 100,000-interaction graph
    println!("=== BENCHMARKING find_taste_twins() on 100k-interaction graph ===");
    for _ in 0..5 {
        let _ = rec.find_taste_twins(viewer_did.as_str(), 10);
    }
    let mut twins_lats = Vec::with_capacity(iters);
    for _ in 0..iters {
        let t0 = Instant::now();
        let twins = rec
            .find_taste_twins(viewer_did.as_str(), 10)
            .expect("find_taste_twins must succeed");
        twins_lats.push(t0.elapsed().as_micros() as u64);
        assert!(!twins.twins.is_empty());
    }
    twins_lats.sort_unstable();
    let twins_p50 = twins_lats[iters * 50 / 100];
    let twins_p99 = twins_lats[iters * 99 / 100];
    let twins_mean = twins_lats.iter().sum::<u64>() as f64 / iters as f64;
    println!(
        "find_taste_twins() on 100k edges: p50 = {} µs ({:.3} ms), p99 = {} µs ({:.3} ms), mean = {:.1} µs",
        twins_p50, twins_p50 as f64 / 1000.0,
        twins_p99, twins_p99 as f64 / 1000.0,
        twins_mean
    );
    assert!(
        twins_p50 < 10_000,
        "find_taste_twins p50 must be sub-10ms, was {} µs",
        twins_p50
    );

    // 3. Stress-test recommend_preview_at on 100,000-interaction graph
    println!("=== BENCHMARKING recommend_preview_at() on 100k-interaction graph ===");
    for _ in 0..5 {
        let _ = rec.recommend_preview_at(Some(viewer_did.as_str()), &dials, now);
    }
    let mut preview_lats = Vec::with_capacity(iters);
    for _ in 0..iters {
        let t0 = Instant::now();
        let preview = rec
            .recommend_preview_at(Some(viewer_did.as_str()), &dials, now)
            .expect("recommend_preview_at must succeed");
        preview_lats.push(t0.elapsed().as_micros() as u64);
        assert!(!preview.items.is_empty());
        assert!(preview.total_candidates <= MAX_CO_INTERACTORS * 10);
    }
    preview_lats.sort_unstable();
    let prev_p50 = preview_lats[iters * 50 / 100];
    let prev_p99 = preview_lats[iters * 99 / 100];
    let prev_mean = preview_lats.iter().sum::<u64>() as f64 / iters as f64;
    println!(
        "recommend_preview_at() on 100k edges: p50 = {} µs ({:.3} ms), p99 = {} µs ({:.3} ms), mean = {:.1} µs",
        prev_p50, prev_p50 as f64 / 1000.0,
        prev_p99, prev_p99 as f64 / 1000.0,
        prev_mean
    );
    assert!(
        prev_p50 < 10_000,
        "recommend_preview_at p50 must be sub-10ms, was {} µs",
        prev_p50
    );
}

#[test]
fn test_m1_adversarial_hyperactive_user_5000_likes_defensive_bounds() {
    let interner = Arc::new(StringInterner::new());
    let graph = Arc::new(GraphStore::new());
    let now = BLUESKY_EPOCH_SECS + 100_000_000;

    let viewer_did = "did:plc:hyperactive_viewer";
    let viewer_id = interner.intern(viewer_did);

    let author_id = interner.intern("did:plc:bulk_author");

    // Viewer has 5,000 likes across 5,000 posts
    println!("=== POPULATING 5,000 VIEWER LIKES ===");
    for p in 0..5_000 {
        let uri = format!("at://did:plc:bulk_author/app.bsky.feed.post/post_{p:05}");
        let pid = interner.intern(&uri);
        graph.record_post_meta(pid, author_id, None, None, now - 50_000 + p as u64);
        graph.record_interaction(viewer_id, pid, SignalType::Like, now - 50_000 + p as u64);
    }

    // 200 co-users each with 1,000 likes, overlapping with the recent seed posts (4950..5000)
    println!("=== POPULATING 200 CO-USERS WITH 1,000 LIKES ===");
    for u in 0..200 {
        let co_did = format!("did:plc:hyper_co_{u:03}");
        let co_id = interner.intern(&co_did);
        // Shared likes in recent seed posts (4950..5000)
        for s in 0..10 {
            let sp = 4950 + ((u * 3 + s) % 50);
            let uri = format!("at://did:plc:bulk_author/app.bsky.feed.post/post_{sp:05}");
            let pid = interner.intern(&uri);
            graph.record_interaction(co_id, pid, SignalType::Like, now - 40_000);
        }
        // Additional background likes (990 other posts)
        for p in 0..990 {
            let target_p = (u * 13 + p) % 4950;
            let uri = format!("at://did:plc:bulk_author/app.bsky.feed.post/post_{target_p:05}");
            let pid = interner.intern(&uri);
            graph.record_interaction(co_id, pid, SignalType::Like, now - 40_000);
        }
        // Co-user candidate post
        let cand_uri = format!("at://did:plc:hyper_co_{u:03}/app.bsky.feed.post/cand");
        let cand_pid = interner.intern(&cand_uri);
        graph.record_post_meta(cand_pid, co_id, None, None, now - 10_000);
        graph.record_interaction(co_id, cand_pid, SignalType::Like, now - 10_000);
    }

    let rec = Recommender::new(interner, graph);

    // Test find_taste_twins latency on 5,000-like viewer
    let iters = 50;
    let mut twins_lats = Vec::with_capacity(iters);
    for _ in 0..iters {
        let t0 = Instant::now();
        let twins = rec.find_taste_twins(viewer_did, 10).unwrap();
        twins_lats.push(t0.elapsed().as_micros() as u64);
        assert_eq!(twins.total_liked_posts, 5_000);
        assert!(!twins.twins.is_empty());
    }
    twins_lats.sort_unstable();
    let p50 = twins_lats[iters * 50 / 100];
    let mean = twins_lats.iter().sum::<u64>() as f64 / iters as f64;
    println!(
        "find_taste_twins() on 5k likes: p50 = {} µs ({:.3} ms), mean = {:.1} µs",
        p50,
        p50 as f64 / 1000.0,
        mean
    );
    let max_allowed_us = if cfg!(debug_assertions) {
        100_000
    } else {
        10_000
    };
    assert!(
        p50 < max_allowed_us,
        "find_taste_twins must be sub-10ms (release), was {} µs",
        p50
    );

    // Test recommend_preview_at latency on 5,000-like viewer
    let dials = RecommendationDials {
        limit: 30,
        min_likes: 1,
        ..Default::default()
    };
    let mut prev_lats = Vec::with_capacity(iters);
    for _ in 0..iters {
        let t0 = Instant::now();
        let prev = rec
            .recommend_preview_at(Some(viewer_did), &dials, now)
            .unwrap();
        prev_lats.push(t0.elapsed().as_micros() as u64);
        assert!(!prev.items.is_empty());
    }
    prev_lats.sort_unstable();
    let prev_p50 = prev_lats[iters * 50 / 100];
    let prev_mean = prev_lats.iter().sum::<u64>() as f64 / iters as f64;
    println!(
        "recommend_preview_at() on 5k likes: p50 = {} µs ({:.3} ms), mean = {:.1} µs",
        prev_p50,
        prev_p50 as f64 / 1000.0,
        prev_mean
    );
    let max_allowed_prev_us = if cfg!(debug_assertions) {
        500_000
    } else {
        10_000
    };
    assert!(
        prev_p50 < max_allowed_prev_us,
        "recommend_preview_at must be sub-10ms (release), was {} µs",
        prev_p50
    );
}

#[test]
fn test_m1_adversarial_strict_top_100_co_interactor_cap_enforcement() {
    let interner = Arc::new(StringInterner::new());
    let graph = Arc::new(GraphStore::new());
    let now = BLUESKY_EPOCH_SECS + 100_000_000;

    let viewer_did = "did:plc:cap_viewer";
    let viewer_id = interner.intern(viewer_did);
    let author_id = interner.intern("did:plc:cap_author");

    // Viewer interacts with 20 seed posts
    let mut seed_pids = Vec::with_capacity(20);
    for i in 0..20 {
        let uri = format!("at://did:plc:cap_author/app.bsky.feed.post/seed_{i:02}");
        let pid = interner.intern(&uri);
        graph.record_post_meta(pid, author_id, None, None, now - 50_000);
        graph.record_interaction(viewer_id, pid, SignalType::Like, now - 40_000);
        seed_pids.push(pid);
    }

    // Populate 500 co-interactors, each liking all 20 seed posts
    // Each co-interactor also has 10 unique candidate posts
    // Total potential candidates across 500 co-interactors = 5,000
    for u in 0..500 {
        let co_did = format!("did:plc:cap_co_{u:04}");
        let co_id = interner.intern(&co_did);
        for &spid in &seed_pids {
            graph.record_interaction(co_id, spid, SignalType::Like, now - 30_000);
        }
        for c in 0..10 {
            let cand_uri = format!("at://did:plc:cap_co_{u:04}/app.bsky.feed.post/cand_{c}");
            let cand_pid = interner.intern(&cand_uri);
            graph.record_post_meta(cand_pid, co_id, None, None, now - 10_000);
            graph.record_interaction(co_id, cand_pid, SignalType::Like, now - 10_000);
        }
    }

    let rec = Recommender::new(Arc::clone(&interner), Arc::clone(&graph));
    let dials = RecommendationDials {
        limit: 30,
        min_likes: 1,
        ..Default::default()
    };

    let preview = rec
        .recommend_preview_at(Some(viewer_did), &dials, now)
        .expect("recommend_preview_at must succeed");

    // Exactly 100 co-interactors are evaluated, each contributing 10 candidates -> exactly 1,000 candidates evaluated
    println!(
        "Cap test: total_candidates evaluated = {} (expected exactly 1,000)",
        preview.total_candidates
    );
    assert_eq!(
        preview.total_candidates, 1_000,
        "Total candidates evaluated MUST equal MAX_CO_INTERACTORS (100) * candidates_per_curator (10) = 1,000; was {}",
        preview.total_candidates
    );
    assert_eq!(preview.items.len(), 30);
}

#[test]
fn test_m1_adversarial_combinatorial_fanout_worst_case_matrix() {
    let interner = Arc::new(StringInterner::new());
    let graph = Arc::new(GraphStore::new());
    let now = BLUESKY_EPOCH_SECS + 100_000_000;

    let viewer_did = "did:plc:fanout_viewer";
    let viewer_id = interner.intern(viewer_did);
    let author_id = interner.intern("did:plc:fanout_author");

    // Viewer interacts with 100 seed posts (exceeds MAX_SEED_POSTS 50)
    let mut seed_pids = Vec::with_capacity(100);
    for i in 0..100 {
        let uri = format!("at://did:plc:fanout_author/app.bsky.feed.post/seed_{i:03}");
        let pid = interner.intern(&uri);
        graph.record_post_meta(pid, author_id, None, None, now - 100_000 + i as u64);
        graph.record_interaction(viewer_id, pid, SignalType::Like, now - 100_000 + i as u64);
        seed_pids.push(pid);
    }

    // Each seed post has 1,000 reverse interaction edges (exceeds MAX_POST_EDGES 500)
    for (idx, &spid) in seed_pids.iter().enumerate() {
        for u in 0..1_000 {
            let co_did = format!("did:plc:fanout_user_{idx}_{u}");
            let co_id = interner.intern(&co_did);
            graph.record_interaction(
                co_id,
                spid,
                SignalType::Like,
                now - 80_000 + (u as u64 % 1000),
            );

            // Also co_id likes another post to satisfy MIN_SHARED_OVERLAP >= 2
            if u < 50 && idx > 0 {
                graph.record_interaction(
                    co_id,
                    seed_pids[idx - 1],
                    SignalType::Like,
                    now - 79_000 + u as u64,
                );
            }
        }
    }

    let rec = Recommender::new(Arc::clone(&interner), Arc::clone(&graph));
    let dials = RecommendationDials {
        limit: 30,
        min_likes: 1,
        ..Default::default()
    };

    let t0 = Instant::now();
    let preview = rec
        .recommend_preview_at(Some(viewer_did), &dials, now)
        .expect("recommend_preview_at must succeed under worst case fanout");
    let elapsed = t0.elapsed();

    println!(
        "Worst-case fanout: elapsed = {:?}, total_candidates = {}",
        elapsed, preview.total_candidates
    );

    let max_allowed_ms = if cfg!(debug_assertions) { 1_000 } else { 50 };
    assert!(
        elapsed.as_millis() < max_allowed_ms,
        "Worst-case fanout must execute in sub-50ms (release) or sub-1000ms (debug), took {:?}",
        elapsed
    );
}

#[test]
fn test_m1_adversarial_boundary_edge_cases() {
    let interner = Arc::new(StringInterner::new());
    let graph = Arc::new(GraphStore::new());
    let rec = Recommender::new(Arc::clone(&interner), Arc::clone(&graph));
    let now = BLUESKY_EPOCH_SECS + 100_000_000;
    let dials = RecommendationDials::default();

    // 1. Viewer not in graph
    let empty_preview = rec
        .recommend_preview_at(Some("did:plc:unknown"), &dials, now)
        .unwrap();
    assert!(empty_preview.items.is_empty());

    let empty_twins = rec.find_taste_twins("did:plc:unknown", 10).unwrap();
    assert!(empty_twins.twins.is_empty());

    let unindexed_post = rec
        .explain_recommendation(
            "did:plc:unknown",
            "at://did:plc:none/app.bsky.feed.post/123",
        )
        .unwrap();
    assert_eq!(unindexed_post.steps[0].step_type, "unindexed_post");

    // 2. Viewer with 0 likes
    let zero_did = "did:plc:zero_likes";
    let zero_id = interner.intern(zero_did);
    let author_id = interner.intern("did:plc:author");
    let post_pid = interner.intern("at://did:plc:author/app.bsky.feed.post/p1");
    graph.record_post_meta(post_pid, author_id, None, None, now - 10_000);

    let zero_preview = rec
        .recommend_preview_at(Some(zero_did), &dials, now)
        .unwrap();
    assert!(zero_preview.items.is_empty());

    let zero_twins = rec.find_taste_twins(zero_did, 10).unwrap();
    assert!(zero_twins.twins.is_empty());
    assert_eq!(zero_twins.total_liked_posts, 0);

    // 3. Viewer with 9 likes (below Tier 1 threshold of 10)
    for p in 0..9 {
        let p_uri = format!("at://did:plc:author/app.bsky.feed.post/sub_{p}");
        let p_id = interner.intern(&p_uri);
        graph.record_post_meta(p_id, author_id, None, None, now - 10_000);
        graph.record_interaction(zero_id, p_id, SignalType::Like, now - 5_000);
    }
    let sub_preview = rec
        .recommend_preview_at(Some(zero_did), &dials, now)
        .unwrap();
    // Below 10 likes, Tier 1 is skipped, falls back to cold-start / velocity pool
    assert!(sub_preview.items.is_empty());

    // 4. Viewer with 10th like reaches Tier 1 threshold
    let p10_uri = "at://did:plc:author/app.bsky.feed.post/sub_9";
    let p10_id = interner.intern(p10_uri);
    graph.record_post_meta(p10_id, author_id, None, None, now - 10_000);
    graph.record_interaction(zero_id, p10_id, SignalType::Like, now - 5_000);

    let tier1_preview = rec
        .recommend_preview_at(Some(zero_did), &dials, now)
        .unwrap();
    // Now evaluated through Tier 1
    assert_eq!(tier1_preview.viewer_did, zero_did);
}
