//! Adversarial challenge, stress tests, property tests, synthetic topologies,
//! and empirical latency benchmark for Milestone 2 (`for-your-consideration::recommender::Recommender`).

#![forbid(unsafe_code)]
#![allow(clippy::pedantic, clippy::nursery, clippy::float_cmp)]

mod common;

use std::collections::HashSet;
use std::sync::Arc;
use std::time::Instant;

use ahash::{AHashMap, AHashSet};
use for_your_consideration::graph::GraphStore;
use for_your_consideration::interner::StringInterner;
use for_your_consideration::recommender::Recommender;
use for_your_consideration::types::{
    RecommendationDials, RecommendationSource, SignalType, BLUESKY_EPOCH_SECS, DEFAULT_PAGE_LIMIT,
};
use proptest::prelude::*;

use crate::common::SyntheticGraphBuilder;

fn test_now() -> u64 {
    BLUESKY_EPOCH_SECS + 1_000_000
}

// ===========================================================================
// Challenge 1: Author Diversity Flood (10,000 Top-Scored Posts)
// ===========================================================================

#[test]
fn test_adversarial_author_diversity_10k_posts() {
    let interner = Arc::new(StringInterner::new());
    let graph = Arc::new(GraphStore::new());
    let now = test_now();

    let viewer = interner.intern("did:plc:target_viewer");
    let co_user = interner.intern("did:plc:seed_collaborator");
    let dominant_author = interner.intern("did:plc:spammer_author");

    // Seed post that connects viewer and co_user
    let seed_post = interner.intern("at://did:plc:seed_collaborator/post/seed");
    graph.record_post_meta(seed_post, co_user, None, None, now - 1000);
    graph.record_interaction(viewer, seed_post, SignalType::Like, now - 900);
    // Give viewer at least 10 likes so they qualify for Tier 1
    for i in 1..=12 {
        let p = interner.intern(&format!("at://did:plc:filler/post/{i}"));
        graph.record_post_meta(p, co_user, None, None, now - 1000);
        graph.record_interaction(viewer, p, SignalType::Like, now - 900);
        if i <= 2 {
            graph.record_interaction(co_user, p, SignalType::Like, now - 850);
        }
    }
    graph.record_interaction(co_user, seed_post, SignalType::Like, now - 850);

    // Dominant author has 10,000 posts, all liked with high priority by co_user
    for i in 1..=10_000 {
        let p = interner.intern(&format!("at://did:plc:spammer_author/post/{i}"));
        graph.record_post_meta(p, dominant_author, None, None, now - 500);
        graph.record_interaction(co_user, p, SignalType::Repost, now - 400);
    }

    // 50 minority authors have 2 posts each
    for a in 1..=50 {
        let minor_author = interner.intern(&format!("did:plc:minor_author_{a}"));
        for p_idx in 1..=2 {
            let p = interner.intern(&format!("at://did:plc:minor_author_{a}/post/{p_idx}"));
            graph.record_post_meta(p, minor_author, None, None, now - 500);
            graph.record_interaction(co_user, p, SignalType::Like, now - 400);
        }
    }

    let rec = Recommender::new(interner, graph);
    let dials = RecommendationDials {
        limit: 50,
        explore_ratio: 0.0, // test raw diversity constraint
        min_likes: 1,
        ..Default::default()
    };

    let res = rec
        .recommend(Some("did:plc:target_viewer"), &dials, now)
        .unwrap();

    assert_eq!(res.posts.len(), 50, "Expected a full page of 50 candidates");

    let mut author_counts: AHashMap<u32, usize> = AHashMap::new();
    for p in &res.posts {
        *author_counts.entry(p.author_id).or_insert(0) += 1;
    }

    // Dominant author must have AT MOST 2 posts
    let dominant_count = author_counts.get(&dominant_author).copied().unwrap_or(0);
    assert_eq!(
        dominant_count, 2,
        "Dominant author with 10k posts must be strictly capped at 2 posts"
    );

    // Every other author must have AT MOST 2 posts
    for (&author, &count) in &author_counts {
        assert!(
            count <= 2,
            "Author {author} exceeded diversity cap with {count} posts"
        );
    }

    // Must have at least 25 distinct authors in the page
    assert!(
        author_counts.len() >= 25,
        "Page must be highly diverse across authors (found {} distinct authors)",
        author_counts.len()
    );
}

// ===========================================================================
// Challenge 2: Conversation Root Dampening Stress Test (Deep Tree & Wide Spam)
// ===========================================================================

#[test]
fn test_adversarial_root_dampening_5k_reply_tree() {
    let interner = Arc::new(StringInterner::new());
    let graph = Arc::new(GraphStore::new());
    let now = test_now();

    let viewer = interner.intern("did:plc:tree_viewer");
    let co_user = interner.intern("did:plc:tree_co_user");

    // Viewer qualifies for Tier 1
    for i in 1..=10 {
        let p = interner.intern(&format!("at://did:plc:author/seed/{i}"));
        let a = interner.intern(&format!("did:plc:author_{i}"));
        graph.record_post_meta(p, a, None, None, now - 1000);
        graph.record_interaction(viewer, p, SignalType::Like, now - 900);
        graph.record_interaction(co_user, p, SignalType::Like, now - 850);
    }

    // 1 viral root post
    let root_post = interner.intern("at://did:plc:op/post/mega_root");
    let op_author = interner.intern("did:plc:op");
    graph.record_post_meta(root_post, op_author, None, None, now - 2000);
    graph.record_interaction(co_user, root_post, SignalType::Like, now - 1500);

    // 5,000 replies nested across distinct authors under the same root_id
    for i in 1..=5000 {
        let reply_post = interner.intern(&format!("at://did:plc:replier_{i}/post/reply_{i}"));
        let replier = interner.intern(&format!("did:plc:replier_{i}"));
        // Give reply i a slightly different timestamp so scores vary
        graph.record_post_meta(
            reply_post,
            replier,
            Some(root_post),
            Some(root_post),
            now - (2000 - (i % 1000) as u64),
        );
        graph.record_interaction(
            co_user,
            reply_post,
            SignalType::Like,
            now - (1500 - (i % 1000) as u64),
        );
    }

    // Add 10 independent other root posts
    let mut other_roots = Vec::new();
    for i in 1..=10 {
        let other_root = interner.intern(&format!("at://did:plc:other_{i}/post/indep_{i}"));
        let other_a = interner.intern(&format!("did:plc:other_{i}"));
        graph.record_post_meta(other_root, other_a, None, None, now - 500);
        graph.record_interaction(co_user, other_root, SignalType::Like, now - 300);
        other_roots.push(other_root);
    }

    let rec = Recommender::new(interner, graph);
    let dials = RecommendationDials {
        limit: 100,
        explore_ratio: 0.0,
        min_likes: 1,
        ..Default::default()
    };

    let res = rec
        .recommend(Some("did:plc:tree_viewer"), &dials, now)
        .unwrap();

    // Check that across the entire result, exactly 1 post from the mega_root tree survived
    let mega_tree_posts: Vec<_> = res
        .posts
        .iter()
        .filter(|p| {
            let meta = rec.graph().get_post_meta(p.post_id).unwrap();
            p.post_id == root_post || meta.root_id == Some(root_post)
        })
        .collect();

    assert_eq!(
        mega_tree_posts.len(),
        1,
        "Exactly 1 post from a 5,000-reply tree must survive thread dampening"
    );

    // Total posts returned should be 1 (from mega tree) + up to 10 (from other roots)
    assert!(
        res.posts.len() <= 11,
        "Expected at most 11 posts, got {}",
        res.posts.len()
    );
    assert!(!res.posts.is_empty());
}

// ===========================================================================
// Challenge 3: 85/15 Exploration Ratio & Serendipity Tagging
// ===========================================================================

#[test]
fn test_adversarial_serendipity_exploration_tagging() {
    let interner = Arc::new(StringInterner::new());
    let graph = Arc::new(GraphStore::new());
    let now = test_now();

    // Cold start with 20 velocity pool posts
    for i in 1..=20 {
        let post = interner.intern(&format!("at://did:plc:author_{i}/post/1"));
        let author = interner.intern(&format!("did:plc:author_{i}"));
        graph.record_post_meta(post, author, None, None, now - 100);
        // Add likes in sliding window
        for u in 1..=10 {
            let user = interner.intern(&format!("did:plc:voter_{u}"));
            graph.record_interaction(user, post, SignalType::Like, now - (100 - u as u64));
        }
    }

    let rec = Recommender::new(interner, graph);

    // Test with standard 0.15 exploration ratio
    let dials = RecommendationDials {
        limit: 20,
        explore_ratio: 0.15,
        ..Default::default()
    };

    let res = rec.recommend(None, &dials, now).unwrap();
    assert_eq!(res.posts.len(), 20);

    // Total = 20. explore_count = round(20 * 0.15) = 3.
    // Exploit count = 17.
    let exploit_posts = &res.posts[0..17];
    let explore_posts = &res.posts[17..20];

    for p in exploit_posts {
        assert_eq!(
            p.source,
            RecommendationSource::Tier3VelocityPool,
            "Top 17 posts should have primary source tag"
        );
    }

    for p in explore_posts {
        assert_eq!(
            p.source,
            RecommendationSource::ExplorationSerendipity,
            "Bottom 3 posts should have ExplorationSerendipity tag"
        );
    }

    // Test boundary: explore_ratio = 0.0 -> no exploration tags
    let dials_zero = RecommendationDials {
        limit: 20,
        explore_ratio: 0.0,
        ..Default::default()
    };
    let res_zero = rec.recommend(None, &dials_zero, now).unwrap();
    for p in &res_zero.posts {
        assert_eq!(p.source, RecommendationSource::Tier3VelocityPool);
    }

    // Test boundary: explore_ratio = 1.0 (pure explore) -> check defensive handling
    let dials_full = RecommendationDials {
        limit: 20,
        explore_ratio: 1.0,
        ..Default::default()
    };
    let res_full = rec.recommend(None, &dials_full, now).unwrap();
    assert_eq!(res_full.posts.len(), 20);

    // Test NaN / Inf explore_ratio defensive handling
    let dials_nan = RecommendationDials {
        limit: 20,
        explore_ratio: f32::NAN,
        ..Default::default()
    };
    let res_nan = rec.recommend(None, &dials_nan, now);
    assert!(res_nan.is_ok(), "NaN explore ratio must not panic");
}

// ===========================================================================
// Challenge 4: Cursor Pagination Monotonicity, Non-Repeating & Exhaustive Walk
// ===========================================================================

#[test]
fn test_adversarial_pagination_exhaustive_walk() {
    let interner = Arc::new(StringInterner::new());
    let graph = Arc::new(GraphStore::new());
    let now = test_now();

    // Create 157 distinct candidates from 157 authors in velocity pool
    let total_candidates = 157;
    for i in 1..=total_candidates {
        let post = interner.intern(&format!("at://did:plc:author_{i}/post/1"));
        let author = interner.intern(&format!("did:plc:author_{i}"));
        graph.record_post_meta(post, author, None, None, now - 100);
        // Interaction to establish velocity
        let voter = interner.intern("did:plc:voter");
        graph.record_interaction(voter, post, SignalType::Like, now - 50);
    }

    let rec = Recommender::new(interner, graph);
    let page_size = 13;

    let mut cursor: Option<String> = None;
    let mut seen_posts = Vec::new();
    let mut page_count = 0;

    loop {
        page_count += 1;
        assert!(page_count < 50, "Infinite pagination loop detected!");

        let dials = RecommendationDials {
            limit: page_size,
            cursor: cursor.clone(),
            explore_ratio: 0.0,
            min_likes: 1,
            ..Default::default()
        };

        let res = rec.recommend(None, &dials, now).unwrap();
        if res.posts.is_empty() {
            assert!(res.cursor.is_none());
            break;
        }

        for p in &res.posts {
            seen_posts.push(p.post_id);
        }

        cursor = res.cursor;
        if cursor.is_none() {
            break;
        }
    }

    // Verify: in velocity pool top 100 candidates are kept
    // Graph velocity pool caps candidates at 100
    let expected_count = total_candidates.min(100);
    assert_eq!(
        seen_posts.len(),
        expected_count,
        "Total paginated posts must match candidate pool size"
    );

    // Verify uniqueness: zero duplicates across all pages
    let unique_posts: HashSet<u32> = seen_posts.iter().copied().collect();
    assert_eq!(
        unique_posts.len(),
        seen_posts.len(),
        "All paginated posts must be strictly unique (no duplicates across pages)"
    );

    // Test corrupted cursor handling:
    let corrupt_cursors = vec![
        "",
        "   ",
        "!!!invalid_base64!!!",
        "999999999",
        "-5",
        "NaN",
        "AAAAAA==",
    ];

    for bad_cursor in corrupt_cursors {
        let dials = RecommendationDials {
            limit: 10,
            cursor: Some(bad_cursor.to_string()),
            ..Default::default()
        };
        let res = rec.recommend(None, &dials, now);
        assert!(res.is_ok(), "Bad cursor '{bad_cursor}' must not panic");
    }
}

// ===========================================================================
// Challenge 5: Synthetic Topologies & Graph Edge Cases
// ===========================================================================

#[test]
fn test_adversarial_dense_clique_topology() {
    let interner = Arc::new(StringInterner::new());
    let graph = Arc::new(GraphStore::new());
    let now = test_now();

    // 50 users, all follow all, all like each other's posts
    let n_users = 50;
    let users: Vec<u32> = (1..=n_users)
        .map(|i| interner.intern(&format!("did:plc:clique_user_{i}")))
        .collect();

    let mut posts = Vec::new();
    for &u in &users {
        for p_idx in 1..=2 {
            let p_uri = format!("at://did:plc:clique_user_{u}/post/{p_idx}");
            let pid = interner.intern(&p_uri);
            graph.record_post_meta(pid, u, None, None, now - 1000);
            posts.push((u, pid));
        }
    }

    // Full clique follows & interactions
    for &u1 in &users {
        for &u2 in &users {
            if u1 != u2 {
                graph.record_follow(u1, u2);
            }
        }
    }

    for &u in &users {
        for &(_author, pid) in &posts {
            graph.record_interaction(u, pid, SignalType::Like, now - 500);
        }
    }

    let rec = Recommender::new(interner, graph);
    let dials = RecommendationDials::default();

    // Query for any user
    let res = rec
        .recommend(Some("did:plc:clique_user_1"), &dials, now)
        .unwrap();
    // Since user_1 has liked ALL posts in the clique, seen deduplication filters them all!
    assert!(
        res.posts.is_empty(),
        "User who liked all clique posts should receive empty deduped feed"
    );

    // Query for an outside observer who follows clique members
    let outsider = "did:plc:outsider";
    let outsider_id = rec.interner().intern(outsider);
    rec.graph().record_follow(outsider_id, users[0]);
    rec.graph().record_follow(outsider_id, users[1]);

    let res_outsider = rec.recommend(Some(outsider), &dials, now).unwrap();
    assert!(
        !res_outsider.posts.is_empty(),
        "Outsider following clique members should receive recommendations"
    );
    // Verify diversity holds
    let mut author_counts: AHashMap<u32, usize> = AHashMap::new();
    for p in &res_outsider.posts {
        *author_counts.entry(p.author_id).or_insert(0) += 1;
        assert!(author_counts[&p.author_id] <= 2);
    }
}

#[test]
fn test_adversarial_bipartite_star_with_viral_hub() {
    let interner = Arc::new(StringInterner::new());
    let graph = Arc::new(GraphStore::new());
    let now = test_now();

    let hub_author = interner.intern("did:plc:hub_author");
    let viral_post = interner.intern("at://did:plc:hub_author/post/viral");
    graph.record_post_meta(viral_post, hub_author, None, None, now - 1000);

    let viewer = interner.intern("did:plc:star_viewer");
    let seed = interner.intern("at://did:plc:star_viewer/post/seed");
    graph.record_post_meta(seed, viewer, None, None, now - 2000);

    // Viewer likes 10 posts to qualify for Tier 1
    for i in 1..=10 {
        let sp = interner.intern(&format!("at://did:plc:hub_author/post/seed_{i}"));
        graph.record_post_meta(sp, hub_author, None, None, now - 1500);
        graph.record_interaction(viewer, sp, SignalType::Like, now - 1400);
    }

    // 500 leaf users all like seed posts and the viral post, plus some niche posts
    for u in 1..=500 {
        let leaf = interner.intern(&format!("did:plc:leaf_{u}"));
        let sp = interner.intern(&format!(
            "at://did:plc:hub_author/post/seed_{}",
            (u % 10) + 1
        ));
        graph.record_interaction(leaf, sp, SignalType::Like, now - 1300);
        // All like viral post (500 likes) -> high BM25 popularity dampener
        graph.record_interaction(leaf, viral_post, SignalType::Like, now - 1200);

        // Leaf also likes a niche post
        let niche_author = interner.intern(&format!("did:plc:niche_author_{u}"));
        let niche_post = interner.intern(&format!("at://did:plc:niche_{u}/post/1"));
        graph.record_post_meta(niche_post, niche_author, None, None, now - 1000);
        graph.record_interaction(leaf, niche_post, SignalType::Like, now - 100);
    }

    let rec = Recommender::new(interner, graph);
    let dials = RecommendationDials {
        limit: 30,
        ..Default::default()
    };

    let res = rec
        .recommend(Some("did:plc:star_viewer"), &dials, now)
        .unwrap();
    assert!(!res.posts.is_empty());
    // Verify BM25 dampener and diversity: Hub author cannot dominate the entire feed
    let hub_posts = res
        .posts
        .iter()
        .filter(|p| p.author_id == hub_author)
        .count();
    assert!(hub_posts <= 2);
}

// ===========================================================================
// Challenge 6: Empirical Latency Profiling (p99 < 2.0ms Target)
// ===========================================================================

#[test]
fn test_adversarial_p99_latency_under_two_milliseconds() {
    let interner = Arc::new(StringInterner::new());
    let graph = Arc::new(GraphStore::new());
    let now = test_now();

    // Populate a realistic graph: 1,000 users, 3,000 posts, 20,000 interactions, 5,000 follows
    let num_users = 1000;
    let num_posts = 3000;

    let users: Vec<u32> = (1..=num_users)
        .map(|i| interner.intern(&format!("did:plc:bench_user_{i}")))
        .collect();

    let posts: Vec<u32> = (1..=num_posts)
        .map(|i| {
            let author = users[i % num_users];
            let pid = interner.intern(&format!("at://did:plc:bench_user_{author}/post/{i}"));
            graph.record_post_meta(pid, author, None, None, now - (i as u64 * 10));
            pid
        })
        .collect();

    // Follows
    for i in 0..num_users {
        for f in 1..=5 {
            let target = (i + f * 37) % num_users;
            graph.record_follow(users[i], users[target]);
        }
    }

    // Interactions
    for i in 0..20_000 {
        let u = users[i % num_users];
        let p = posts[(i * 13) % num_posts];
        let sig = match i % 3 {
            0 => SignalType::Like,
            1 => SignalType::Repost,
            _ => SignalType::Quote,
        };
        graph.record_interaction(u, p, sig, now - (i as u64 % 5000));
    }

    let rec = Recommender::new(interner, graph);
    let dials = RecommendationDials::default();

    // Warm-up queries
    for i in 0..50 {
        let did = format!("did:plc:bench_user_{}", (i % num_users) + 1);
        let _ = rec.recommend(Some(&did), &dials, now).unwrap();
    }

    // Measure 1,000 queries
    let mut latencies_us = Vec::with_capacity(1000);
    for i in 0..1000 {
        let did = format!("did:plc:bench_user_{}", (i % num_users) + 1);
        let start = Instant::now();
        let _ = rec.recommend(Some(&did), &dials, now).unwrap();
        let elapsed = start.elapsed();
        latencies_us.push(elapsed.as_micros() as u64);
    }

    latencies_us.sort_unstable();

    let min_us = latencies_us[0];
    let p50_us = latencies_us[latencies_us.len() * 50 / 100];
    let p90_us = latencies_us[latencies_us.len() * 90 / 100];
    let p95_us = latencies_us[latencies_us.len() * 95 / 100];
    let p99_us = latencies_us[latencies_us.len() * 99 / 100];
    let max_us = latencies_us[latencies_us.len() - 1];
    let sum_us: u64 = latencies_us.iter().sum();
    let mean_us = sum_us as f64 / latencies_us.len() as f64;

    println!("\n=== EMPIRICAL LATENCY BENCHMARK RESULTS (1000 queries) ===");
    println!(
        "Min:     {:>6} µs ({:.3} ms)",
        min_us,
        min_us as f64 / 1000.0
    );
    println!("Mean:    {:>6.1} µs ({:.3} ms)", mean_us, mean_us / 1000.0);
    println!(
        "p50:     {:>6} µs ({:.3} ms)",
        p50_us,
        p50_us as f64 / 1000.0
    );
    println!(
        "p90:     {:>6} µs ({:.3} ms)",
        p90_us,
        p90_us as f64 / 1000.0
    );
    println!(
        "p95:     {:>6} µs ({:.3} ms)",
        p95_us,
        p95_us as f64 / 1000.0
    );
    println!(
        "p99:     {:>6} µs ({:.3} ms)",
        p99_us,
        p99_us as f64 / 1000.0
    );
    println!(
        "Max:     {:>6} µs ({:.3} ms)",
        max_us,
        max_us as f64 / 1000.0
    );
    println!("===========================================================\n");

    // p99 must be well under 2.0ms (2,000 µs) in release mode; allow up to 150ms in unoptimized debug mode
    let p99_threshold = if cfg!(debug_assertions) {
        150_000
    } else {
        2_000
    };
    assert!(
        p99_us < p99_threshold,
        "p99 latency ({p99_us} µs) exceeded {p99_threshold} µs SLA threshold!"
    );
}

// ===========================================================================
// Challenge 7: Proptest Invariant Fuzzing
// ===========================================================================

proptest! {
    #![proptest_config(ProptestConfig::with_cases(50))]

    #[test]
    fn test_proptest_recommendation_invariants(
        limit in 0usize..100usize,
        explore_ratio in -0.5f32..1.5f32,
        half_life_secs in 1u32..100_000u32,
        explain in proptest::bool::ANY,
    ) {
        let (interner, graph) = SyntheticGraphBuilder::standard_cold_start_fixture(test_now());
        let rec = Recommender::new(interner, graph);
        let now = test_now();

        let dials = RecommendationDials {
            limit,
            explore_ratio,
            half_life_secs: half_life_secs as f32,
            explain,
            ..Default::default()
        };

        // Test active, new, cold, and anonymous users
        let viewers = [
            Some("did:plc:active_user"),
            Some("did:plc:new_user"),
            Some("did:plc:cold_user"),
            None,
        ];

        for viewer in viewers {
            let res = rec.recommend(viewer, &dials, now);
            prop_assert!(res.is_ok(), "Recommendation query failed unexpectedly");
            let rec_res = res.unwrap();

            // Invariant 1: Post count <= effective limit
            let effective_limit = if limit == 0 { DEFAULT_PAGE_LIMIT } else { limit };
            prop_assert!(
                rec_res.posts.len() <= effective_limit,
                "Returned {} posts, which exceeds effective limit {}",
                rec_res.posts.len(),
                effective_limit
            );

            // Invariant 2: Author diversity constraint (<= 2 per author)
            let mut author_counts: AHashMap<u32, usize> = AHashMap::new();
            for p in &rec_res.posts {
                let count = author_counts.entry(p.author_id).or_insert(0);
                *count += 1;
                prop_assert!(
                    *count <= 2,
                    "Author {} appears {} times in feed (max allowed: 2)",
                    p.author_id,
                    *count
                );
            }

            // Invariant 3: Unique posts in page
            let mut seen_pids = AHashSet::new();
            for p in &rec_res.posts {
                prop_assert!(
                    seen_pids.insert(p.post_id),
                    "Duplicate post_id {} found in same page",
                    p.post_id
                );
            }

            // Invariant 4: Explainability trace consistency
            if explain {
                for p in &rec_res.posts {
                    prop_assert!(p.explain.is_some(), "Explain field must be populated when explain=true");
                }
            }
        }
    }
}
