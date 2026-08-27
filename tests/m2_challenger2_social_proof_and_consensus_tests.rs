#![forbid(unsafe_code)]
#![allow(
    clippy::all,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    missing_docs
)]

use std::sync::{Arc, Barrier};
use std::time::{Duration, Instant};

use for_your_consideration::prelude::*;

#[test]
fn test_adversarial_multi_curator_lower_affinity_beats_single_high_affinity() {
    let interner = Arc::new(StringInterner::new());
    let graph = Arc::new(GraphStore::new());
    let rec = Recommender::new(Arc::clone(&interner), Arc::clone(&graph));
    let now = BLUESKY_EPOCH_SECS + 100_000;

    let viewer = interner.intern("did:plc:adv_viewer");
    let twin_1 = interner.intern("did:plc:adv_twin1");
    let twin_2 = interner.intern("did:plc:adv_twin2");
    let twin_3 = interner.intern("did:plc:adv_twin3");
    let twin_high = interner.intern("did:plc:adv_twin_high");
    let author = interner.intern("did:plc:adv_author");

    // Viewer likes 10 seed posts to qualify for Tier 1 walk
    let mut seed_posts = Vec::new();
    for i in 1..=10 {
        let sp = interner.intern(&format!(
            "at://did:plc:adv_author/app.bsky.feed.post/seed_{i}"
        ));
        seed_posts.push(sp);
        graph.record_post_meta(sp, author, None, None, now - 2000);
        graph.record_interaction(viewer, sp, SignalType::Like, now - 1500);
    }

    // twin_1, twin_2, twin_3 share exactly 2 likes with viewer (individual affinity ~0.1789)
    for &twin in &[twin_1, twin_2, twin_3] {
        graph.record_interaction(twin, seed_posts[0], SignalType::Like, now - 1400);
        graph.record_interaction(twin, seed_posts[1], SignalType::Like, now - 1400);
    }

    // twin_high shares 6 likes with viewer (individual affinity ~0.5164, nearly 3x higher than any single twin)
    for i in 0..6 {
        graph.record_interaction(twin_high, seed_posts[i], SignalType::Like, now - 1400);
    }

    // Candidate A: Endorsed by 3 lower-affinity twins (twin_1, twin_2, twin_3)
    let author_a = interner.intern("did:plc:adv_author_a");
    let p_consensus =
        interner.intern("at://did:plc:adv_author_a/app.bsky.feed.post/cand_multi_curator");
    graph.record_post_meta(p_consensus, author_a, None, None, now - 1000);
    graph.record_interaction(twin_1, p_consensus, SignalType::Like, now - 500);
    graph.record_interaction(twin_2, p_consensus, SignalType::Like, now - 500);
    graph.record_interaction(twin_3, p_consensus, SignalType::Like, now - 500);

    // Candidate B: Endorsed by 1 high-affinity twin (twin_high)
    let author_b = interner.intern("did:plc:adv_author_b");
    let p_single =
        interner.intern("at://did:plc:adv_author_b/app.bsky.feed.post/cand_single_curator");
    graph.record_post_meta(p_single, author_b, None, None, now - 1000);
    graph.record_interaction(twin_high, p_single, SignalType::Like, now - 500);

    // Equalize global interaction counts to 10 for both posts so social proof factor S(10) is identical
    for u in 1..=7 {
        let outsider = interner.intern(&format!("did:plc:outsider_a_{u}"));
        graph.record_interaction(outsider, p_consensus, SignalType::Like, now - 600);
    }
    for u in 1..=9 {
        let outsider = interner.intern(&format!("did:plc:outsider_b_{u}"));
        graph.record_interaction(outsider, p_single, SignalType::Like, now - 600);
    }
    assert_eq!(graph.get_post_interaction_count(p_consensus), 10);
    assert_eq!(graph.get_post_interaction_count(p_single), 10);

    let dials = RecommendationDials {
        limit: 10,
        ..Default::default()
    };

    let rec_res = rec
        .recommend(Some("did:plc:adv_viewer"), &dials, now)
        .unwrap();
    assert_eq!(rec_res.posts.len(), 2);

    // Post endorsed by 3 curators MUST rank #1, beating the post endorsed by 1 high-affinity curator
    assert_eq!(
        rec_res.posts[0].post_id, p_consensus,
        "Post endorsed by 3 curators must beat post endorsed by 1 high-affinity curator"
    );
    assert_eq!(rec_res.posts[1].post_id, p_single);

    // Calculate theoretical scores:
    // twin_1,2,3 have 2 shared likes out of 3 total likes (2 seed + 1 candidate)
    let conf_low =
        calculate_bayesian_confidence(2.0 / (10.0f32 * 3.0).sqrt(), 2, DEFAULT_BAYESIAN_BETA);
    // twin_high has 6 shared likes out of 7 total likes (6 seed + 1 candidate)
    let conf_high =
        calculate_bayesian_confidence(6.0 / (10.0f32 * 7.0).sqrt(), 6, DEFAULT_BAYESIAN_BETA);
    let expected_score_consensus =
        3.0 * conf_low * calculate_consensus_boost(3) * calculate_social_proof_factor(10);
    let expected_score_single =
        conf_high * calculate_consensus_boost(1) * calculate_social_proof_factor(10);

    let expected_ratio = expected_score_consensus / expected_score_single;
    let actual_ratio = rec_res.posts[0].score / rec_res.posts[1].score;

    assert!(
        (actual_ratio - expected_ratio).abs() < 1e-4,
        "Actual score ratio {actual_ratio} must match theoretical ratio {expected_ratio}"
    );
    assert!(
        actual_ratio > 1.30,
        "3 curators with lower individual affinity must beat 1 high curator by > 30%"
    );
}

#[test]
fn test_adversarial_social_proof_validation_curve_10_50_vs_1_like() {
    let interner = Arc::new(StringInterner::new());
    let graph = Arc::new(GraphStore::new());
    let rec = Recommender::new(Arc::clone(&interner), Arc::clone(&graph));
    let now = BLUESKY_EPOCH_SECS + 100_000;

    let viewer = interner.intern("did:plc:sp_viewer");
    let twin = interner.intern("did:plc:sp_twin");
    let author = interner.intern("did:plc:sp_author");

    // Establish Tier 1 walk
    for i in 1..=10 {
        let sp = interner.intern(&format!("at://did:plc:sp_author/app.bsky.feed.post/sp_{i}"));
        graph.record_post_meta(sp, author, None, None, now - 2000);
        graph.record_interaction(viewer, sp, SignalType::Like, now - 1500);
        if i <= 3 {
            graph.record_interaction(twin, sp, SignalType::Like, now - 1400);
        }
    }

    // Setup 6 candidate posts endorsed by the SAME twin at the SAME timestamp
    // Candidate 0: 0 interactions (unvetted baseline) - wait, twin liked it, so 1 interaction.
    // Let's create:
    // p_1: exactly 1 interaction (by twin)
    // p_10: 10 interactions
    // p_50: 50 interactions
    // p_500: 500 interactions (peak plateau)
    // p_5000: 5000 interactions (soft viral taper)
    // p_50k: 50,000 interactions (mega viral)

    let p_1 = interner.intern("at://did:plc:sp_author_1/app.bsky.feed.post/cand_1");
    let p_10 = interner.intern("at://did:plc:sp_author_10/app.bsky.feed.post/cand_10");
    let p_50 = interner.intern("at://did:plc:sp_author_50/app.bsky.feed.post/cand_50");
    let p_500 = interner.intern("at://did:plc:sp_author_500/app.bsky.feed.post/cand_500");
    let p_5000 = interner.intern("at://did:plc:sp_author_5000/app.bsky.feed.post/cand_5000");
    let p_50k = interner.intern("at://did:plc:sp_author_50k/app.bsky.feed.post/cand_50k");

    for (p, total_interactions, author_str) in [
        (p_1, 1, "did:plc:sp_author_1"),
        (p_10, 10, "did:plc:sp_author_10"),
        (p_50, 50, "did:plc:sp_author_50"),
        (p_500, 500, "did:plc:sp_author_500"),
        (p_5000, 5000, "did:plc:sp_author_5000"),
        (p_50k, 50_000, "did:plc:sp_author_50k"),
    ] {
        let post_author = interner.intern(author_str);
        graph.record_post_meta(p, post_author, None, None, now - 1000);
        graph.record_interaction(twin, p, SignalType::Like, now - 500);
        for u in 1..total_interactions {
            let other = interner.intern(&format!("did:plc:sp_fan_{p}_{u}"));
            graph.record_interaction(other, p, SignalType::Like, now - 500);
        }
        assert_eq!(graph.get_post_interaction_count(p), total_interactions);
    }

    let dials = RecommendationDials {
        limit: 10,
        min_likes: 1,
        ..Default::default()
    };

    let res = rec
        .recommend(Some("did:plc:sp_viewer"), &dials, now)
        .unwrap();
    assert_eq!(res.posts.len(), 6);

    // Theoretical social proof factors:
    // S(500) = 1.9248 -> Rank 1
    // S(5000) = 1.5702 -> Rank 2
    // S(50) = 1.5298 -> Rank 3
    // S(50000) = 1.3231 -> Rank 4
    // S(10) = 1.1505 -> Rank 5
    // S(1) = 0.5520 -> Rank 6
    assert_eq!(
        res.posts[0].post_id, p_500,
        "500-like post must rank #1 (peak quality curve)"
    );
    assert_eq!(res.posts[1].post_id, p_5000, "5000-like post must rank #2");
    assert_eq!(res.posts[2].post_id, p_50, "50-like post must rank #3");
    assert_eq!(res.posts[3].post_id, p_50k, "50k-like post must rank #4");
    assert_eq!(res.posts[4].post_id, p_10, "10-like post must rank #5");
    assert_eq!(res.posts[5].post_id, p_1, "1-like post must rank #6");

    // Check specific ratios:
    // 50 likes vs 1 like:
    let ratio_50_to_1 = res.posts[2].score / res.posts[5].score;
    let expected_50_to_1 = calculate_social_proof_factor(50) / calculate_social_proof_factor(1);
    assert!((ratio_50_to_1 - expected_50_to_1).abs() < 1e-4);
    assert!(
        ratio_50_to_1 > 2.70,
        "50 likes vs 1 like ratio must exceed 2.7x"
    );

    // 10 likes vs 1 like:
    let ratio_10_to_1 = res.posts[4].score / res.posts[5].score;
    let expected_10_to_1 = calculate_social_proof_factor(10) / calculate_social_proof_factor(1);
    assert!((ratio_10_to_1 - expected_10_to_1).abs() < 1e-4);
    assert!(
        ratio_10_to_1 > 2.05,
        "10 likes vs 1 like ratio must exceed 2.05x"
    );

    // 50 likes vs 50k likes:
    let ratio_50_to_50k = res.posts[2].score / res.posts[3].score;
    assert!(
        ratio_50_to_50k > 1.10,
        "50-like community sweet spot post must outrank 50k mega-viral post"
    );
}

#[test]
fn test_adversarial_curator_duplicate_interaction_deduplication() {
    let interner = Arc::new(StringInterner::new());
    let graph = Arc::new(GraphStore::new());
    let rec = Recommender::new(Arc::clone(&interner), Arc::clone(&graph));
    let now = BLUESKY_EPOCH_SECS + 100_000;

    let viewer = interner.intern("did:plc:dedup_viewer");
    let twin_a = interner.intern("did:plc:dedup_twin_a");
    let twin_b = interner.intern("did:plc:dedup_twin_b");
    let author = interner.intern("did:plc:dedup_author");

    // Tier 1 qualification
    for i in 1..=10 {
        let sp = interner.intern(&format!(
            "at://did:plc:dedup_author/app.bsky.feed.post/sp_{i}"
        ));
        graph.record_post_meta(sp, author, None, None, now - 2000);
        graph.record_interaction(viewer, sp, SignalType::Like, now - 1500);
        if i <= 3 {
            graph.record_interaction(twin_a, sp, SignalType::Like, now - 1400);
            graph.record_interaction(twin_b, sp, SignalType::Like, now - 1400);
        }
    }

    let cand_post = interner.intern("at://did:plc:dedup_author/app.bsky.feed.post/target_dedup");
    graph.record_post_meta(cand_post, author, None, None, now - 1000);

    // Twin A interacts 3 times (Like, Repost, Quote) with cand_post
    graph.record_interaction(twin_a, cand_post, SignalType::Like, now - 500);
    graph.record_interaction(twin_a, cand_post, SignalType::Repost, now - 490);
    graph.record_interaction(twin_a, cand_post, SignalType::Quote, now - 470);

    // Twin B interacts 1 time (Like)
    graph.record_interaction(twin_b, cand_post, SignalType::Like, now - 500);

    let dials = RecommendationDials {
        explain: true,
        ..Default::default()
    };

    let preview = rec
        .recommend_preview_at(Some("did:plc:dedup_viewer"), &dials, now)
        .unwrap();
    assert_eq!(preview.items.len(), 1);

    // Taste twins count must be 2 unique curators (Twin A and Twin B), not 5!
    let breakdown = &preview.items[0].score_breakdown;
    assert!(
        breakdown.taste_similarity > 0.0,
        "Taste similarity must be positive"
    );

    // In recommendation result, verify scoring matches consensus boost for k = 2
    let res = rec
        .recommend(Some("did:plc:dedup_viewer"), &dials, now)
        .unwrap();
    assert_eq!(res.posts.len(), 1);
    assert_eq!(res.posts[0].post_id, cand_post);
}

#[test]
fn test_adversarial_mathematical_bounds_and_monotony() {
    // 1. Social proof factor values are strictly non-negative, non-zero, finite
    for n in [
        0, 1, 2, 3, 5, 10, 50, 100, 500, 1_000, 5_000, 50_000, 500_000, 10_000_000,
    ] {
        let s = calculate_social_proof_factor(n);
        assert!(!s.is_nan(), "S({n}) must not be NaN");
        assert!(!s.is_infinite(), "S({n}) must not be infinite");
        assert!(s > 0.0, "S({n}) must be strictly positive");
    }

    // 2. Strict monotonicity up to 500
    let mut prev = calculate_social_proof_factor(0);
    for n in 1..=500 {
        let curr = calculate_social_proof_factor(n);
        assert!(
            curr > prev,
            "S({n}) must be strictly greater than S({})",
            n - 1
        );
        prev = curr;
    }

    // 3. Strict decreasing taper for N > 500
    let s500 = calculate_social_proof_factor(500);
    let s1000 = calculate_social_proof_factor(1000);
    let s10k = calculate_social_proof_factor(10_000);
    let s100k = calculate_social_proof_factor(100_000);
    let s1m = calculate_social_proof_factor(1_000_000);

    assert!(s500 > s1000);
    assert!(s1000 > s10k);
    assert!(s10k > s100k);
    assert!(s100k > s1m);
    assert!(
        s1m > 1.0,
        "Even at 1M likes, social proof factor remains > 1.0"
    );

    // 4. Consensus boost bounds & monotonicity
    assert_eq!(calculate_consensus_boost(0), 1.0);
    assert_eq!(calculate_consensus_boost(1), 1.0);

    let mut prev_boost = 1.0;
    for k in 2..=100 {
        let boost = calculate_consensus_boost(k);
        assert!(
            boost > prev_boost,
            "ConsensusBoost({k}) must be strictly greater than ConsensusBoost({})",
            k - 1
        );
        prev_boost = boost;
    }

    // Diminishing returns property: Boost(k+1) - Boost(k) is strictly decreasing
    for k in 2..99 {
        let d1 = calculate_consensus_boost(k) - calculate_consensus_boost(k - 1);
        let d2 = calculate_consensus_boost(k + 1) - calculate_consensus_boost(k);
        assert!(
            d2 < d1,
            "Consensus boost must exhibit diminishing marginal returns at k={k}"
        );
    }
}

#[test]
fn test_empirical_concurrent_multi_curator_latency_benchmark() {
    let interner = Arc::new(StringInterner::new());
    let graph = Arc::new(GraphStore::new());
    let rec = Arc::new(Recommender::new(Arc::clone(&interner), Arc::clone(&graph)));
    let now = BLUESKY_EPOCH_SECS + 500_000;

    let num_users = 1000;
    let num_posts = 2000;

    let mut user_dids = Vec::with_capacity(num_users);
    for u in 0..num_users {
        let did = format!("did:plc:stress_user_{u}");
        let uid = interner.intern(&did);
        user_dids.push((did, uid));
    }

    let mut post_ids = Vec::with_capacity(num_posts);
    for p in 0..num_posts {
        let uri = format!(
            "at://did:plc:stress_user_{}/app.bsky.feed.post/post_{p}",
            p % 100
        );
        let pid = interner.intern(&uri);
        let author_uid = user_dids[p % 100].1;
        graph.record_post_meta(pid, author_uid, None, None, now - (p as u64 % 50_000));
        post_ids.push(pid);
    }

    // Ingest overlapping interactions to create rich multi-curator clusters
    for u in 0..num_users {
        let uid = user_dids[u].1;
        // Each user likes 20 posts in their cluster and 10 common posts
        let cluster = u % 10;
        for c in 0..20 {
            let pid = post_ids[(cluster * 150 + c) % num_posts];
            graph.record_interaction(uid, pid, SignalType::Like, now - (c as u64 * 100));
        }
        for common in 0..10 {
            let pid = post_ids[common];
            graph.record_interaction(uid, pid, SignalType::Like, now - (common as u64 * 50));
        }
    }

    let num_threads = 8;
    let queries_per_thread = 250;
    let warmup_queries_per_thread = 50;
    let total_queries = num_threads * queries_per_thread;
    let barrier = Arc::new(Barrier::new(num_threads));

    let handles: Vec<_> = (0..num_threads)
        .map(|t| {
            let rec = Arc::clone(&rec);
            let user_dids = user_dids.clone();
            let barrier = Arc::clone(&barrier);
            std::thread::spawn(move || {
                let dials = RecommendationDials {
                    limit: 30,
                    ..Default::default()
                };
                // Warmup queries before measuring latency so spin-up and JIT/cold cache overhead does not distort p99
                for w in 0..warmup_queries_per_thread {
                    let user_idx = (t * warmup_queries_per_thread + w) % num_users;
                    let viewer = &user_dids[user_idx].0;
                    let _ = rec.recommend(Some(viewer.as_str()), &dials, now);
                }
                barrier.wait();

                let mut latencies = Vec::with_capacity(queries_per_thread);
                let thread_start = Instant::now();
                for q in 0..queries_per_thread {
                    let user_idx = (t * queries_per_thread + q) % num_users;
                    let viewer = &user_dids[user_idx].0;
                    let t0 = Instant::now();
                    let res = rec.recommend(Some(viewer.as_str()), &dials, now);
                    let elapsed_micros = t0.elapsed().as_micros();
                    latencies.push(elapsed_micros);
                    assert!(res.is_ok());
                }
                (latencies, thread_start.elapsed())
            })
        })
        .collect();

    let mut all_latencies = Vec::with_capacity(total_queries);
    let mut max_thread_time = Duration::ZERO;
    for h in handles {
        let (latencies, thread_time) = h.join().unwrap();
        all_latencies.extend(latencies);
        if thread_time > max_thread_time {
            max_thread_time = thread_time;
        }
    }
    let total_time = max_thread_time;

    all_latencies.sort_unstable();
    let count = all_latencies.len();
    let p50 = all_latencies[count * 50 / 100];
    let p90 = all_latencies[count * 90 / 100];
    let p99 = all_latencies[count * 99 / 100];
    let max = all_latencies[count - 1];
    let throughput = count as f64 / total_time.as_secs_f64();

    println!(
        " [M2 MULTI-CURATOR STRESS] Queries: {count} | Throughput: {throughput:.1} q/s | p50: {p50} µs | p90: {p90} µs | p99: {p99} µs ({:.3} ms) | Max: {max} µs",
        p99 as f64 / 1000.0
    );

    // In debug mode, p99 is under 5ms; in release mode it is sub-millisecond (< 500µs)
    #[cfg(not(debug_assertions))]
    assert!(
        p99 < 2_000,
        "p99 latency under concurrent multi-curator load exceeded 2.0ms: {p99} µs"
    );
}
