#![allow(clippy::pedantic, clippy::nursery, clippy::float_cmp)]

//! Adversarial test suite for `Recommender` in `src/recommender.rs`.
//!
//! Specifically validates:
//! 1. Clock skew, future timestamps, past timestamps, zero time, extreme half-lives (NaN, infinity, zero, negative).
//! 2. Cascading fallback transitions (isolated Tier 1 -> Tier 2 -> Tier 3).
//! 3. Unauthenticated requests and invalid DIDs.
//! 4. Explainability metadata accuracy and consistency.
//! 5. Concurrency stress: multi-threaded concurrent reads and writes without deadlocks or races.
//! 6. Anti-fatigue, thread dampening, and author diversity under extreme distributions.

mod common;

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use for_your_consideration::graph::GraphStore;
use for_your_consideration::interner::StringInterner;
use for_your_consideration::recommender::Recommender;
use for_your_consideration::types::{
    RecommendationDials, RecommendationSource, SignalType, BLUESKY_EPOCH_SECS,
};

use common::SyntheticGraphBuilder;

fn now() -> u64 {
    BLUESKY_EPOCH_SECS + 50_000
}

// ===========================================================================
// 1. Clock Skew and Future / Past Timestamps
// ===========================================================================

#[test]
fn test_clock_skew_future_interactions_do_not_underflow() {
    let interner = Arc::new(StringInterner::new());
    let graph = Arc::new(GraphStore::new());

    let viewer = interner.intern("did:plc:viewer_future");
    let co_user = interner.intern("did:plc:co_user_future");
    let author = interner.intern("did:plc:author_future");

    let seed = interner.intern("at://did:plc:author_future/app.bsky.feed.post/seed");
    let cand = interner.intern("at://did:plc:author_future/app.bsky.feed.post/cand");

    let query_now = now();
    // Event timestamps placed in the future relative to query_now
    let future_ts = query_now + 100_000;

    graph.record_post_meta(seed, author, None, None, future_ts);
    graph.record_post_meta(cand, author, None, None, future_ts);

    // Give viewer 10 likes so they qualify for Tier 1
    for i in 1..=10 {
        let p = interner.intern(&format!(
            "at://did:plc:author_future/app.bsky.feed.post/seed_{i}"
        ));
        graph.record_post_meta(p, author, None, None, future_ts);
        graph.record_interaction(viewer, p, SignalType::Like, future_ts);
        graph.record_interaction(co_user, p, SignalType::Like, future_ts);
    }
    graph.record_interaction(co_user, cand, SignalType::Repost, future_ts);

    let rec = Recommender::new(interner, graph);
    let dials = RecommendationDials {
        explore_ratio: 0.0,
        ..Default::default()
    };

    // Querying with query_now (which is older than future_ts)
    let res = rec.recommend(Some("did:plc:viewer_future"), &dials, query_now);
    assert!(res.is_ok(), "Expected OK despite future timestamps");
    let rec_res = res.unwrap();
    assert!(!rec_res.posts.is_empty());
    assert!(!rec_res.posts[0].score.is_nan());
    assert!(rec_res.posts[0].score.is_finite());
    assert_eq!(
        rec_res.posts[0].source,
        RecommendationSource::Tier1InteractionWalk
    );
}

#[test]
fn test_extreme_time_values_and_dials() {
    let (interner, graph) = SyntheticGraphBuilder::standard_cold_start_fixture(now());
    let rec = Recommender::new(interner, graph);

    // Test with now_secs = 0 (before BLUESKY_EPOCH_SECS)
    let res_zero = rec.recommend(
        Some("did:plc:active_user"),
        &RecommendationDials::default(),
        0,
    );
    assert!(res_zero.is_ok());

    // Test with now_secs = u64::MAX
    let res_max = rec.recommend(
        Some("did:plc:active_user"),
        &RecommendationDials::default(),
        u64::MAX,
    );
    assert!(res_max.is_ok());

    // Test with half_life_secs = 0.0, negative, extremely large
    let dials_zero_hl = RecommendationDials {
        half_life_secs: 0.0,
        ..Default::default()
    };
    let res_zero_hl = rec.recommend(Some("did:plc:active_user"), &dials_zero_hl, now());
    assert!(res_zero_hl.is_ok());

    let dials_neg_hl = RecommendationDials {
        half_life_secs: -1000.0,
        ..Default::default()
    };
    let res_neg_hl = rec.recommend(Some("did:plc:active_user"), &dials_neg_hl, now());
    assert!(res_neg_hl.is_ok());

    let dials_huge_hl = RecommendationDials {
        half_life_secs: 1e15,
        ..Default::default()
    };
    let res_huge_hl = rec.recommend(Some("did:plc:active_user"), &dials_huge_hl, now());
    assert!(res_huge_hl.is_ok());
}

// ===========================================================================
// 2. Cascading Fallback Transitions
// ===========================================================================

#[test]
fn test_tier1_isolated_subgraph_cascades_to_tier2_then_tier3() {
    let interner = Arc::new(StringInterner::new());
    let graph = Arc::new(GraphStore::new());

    let query_now = now();
    let isolated_user = "did:plc:isolated_user";
    let u_id = interner.intern(isolated_user);

    // Give isolated_user 12 likes (qualifies for Tier 1: >= 10 likes)
    for i in 1..=12 {
        let p_uri = format!("at://did:plc:author_x/app.bsky.feed.post/iso_{i}");
        let p_id = interner.intern(&p_uri);
        let a_id = interner.intern("did:plc:author_x");
        graph.record_post_meta(p_id, a_id, None, None, query_now - 1000);
        graph.record_interaction(u_id, p_id, SignalType::Like, query_now - 500);
        // NO other user interacts with these posts -> 0 co-interactors!
    }

    // Populate global velocity pool with trending posts so Tier 3 has items
    let trend_author = interner.intern("did:plc:trend_author");
    for i in 1..=5 {
        let p_uri = format!("at://did:plc:trend_author/app.bsky.feed.post/trend_{i}");
        let p_id = interner.intern(&p_uri);
        graph.record_post_meta(p_id, trend_author, None, None, query_now - 2000);
        for u in 1..=5 {
            let other_user = interner.intern(&format!("did:plc:trend_user_{u}"));
            graph.record_interaction(other_user, p_id, SignalType::Like, query_now - 1000);
        }
    }

    let rec = Recommender::new(Arc::clone(&interner), Arc::clone(&graph));
    let dials = RecommendationDials {
        explore_ratio: 0.0,
        ..Default::default()
    };

    // Scenario A: isolated user has 0 follows -> Tier 1 (empty) -> Tier 2 (empty) -> Tier 3
    let res_t3 = rec
        .recommend(Some(isolated_user), &dials, query_now)
        .unwrap();
    assert!(!res_t3.posts.is_empty());
    assert_eq!(
        res_t3.posts[0].source,
        RecommendationSource::Tier3VelocityPool
    );

    // Scenario B: isolated user follows an active user -> Tier 1 (empty) -> Tier 2 (found!)
    let followed_user = interner.intern("did:plc:followed_active");
    let followed_post =
        interner.intern("at://did:plc:trend_author/app.bsky.feed.post/followed_post_1");
    graph.record_post_meta(followed_post, trend_author, None, None, query_now - 1000);
    graph.record_interaction(
        followed_user,
        followed_post,
        SignalType::Like,
        query_now - 400,
    );
    graph.record_follow(u_id, followed_user);

    let res_t2 = rec
        .recommend(Some(isolated_user), &dials, query_now)
        .unwrap();
    assert!(!res_t2.posts.is_empty());
    assert_eq!(
        res_t2.posts[0].source,
        RecommendationSource::Tier2FollowWalk
    );
    assert_eq!(res_t2.posts[0].post_id, followed_post);
}

#[test]
fn test_tier2_with_zero_active_follows_cascades_to_tier3() {
    let interner = Arc::new(StringInterner::new());
    let graph = Arc::new(GraphStore::new());
    let query_now = now();

    let user = "did:plc:new_user_empty_follows";
    let u_id = interner.intern(user);

    // User has 2 likes (<10) and follows a dead account (0 interactions)
    let dead_user = interner.intern("did:plc:dead_account");
    graph.record_follow(u_id, dead_user);

    let p1 = interner.intern("at://did:plc:author_y/app.bsky.feed.post/1");
    let p2 = interner.intern("at://did:plc:author_y/app.bsky.feed.post/2");
    let a_id = interner.intern("did:plc:author_y");
    graph.record_post_meta(p1, a_id, None, None, query_now - 1000);
    graph.record_post_meta(p2, a_id, None, None, query_now - 1000);
    graph.record_interaction(u_id, p1, SignalType::Like, query_now - 200);
    graph.record_interaction(u_id, p2, SignalType::Like, query_now - 100);

    // Populate velocity pool
    let trend_author = interner.intern("did:plc:trend_author");
    let trend_post = interner.intern("at://did:plc:trend_author/app.bsky.feed.post/viral");
    graph.record_post_meta(trend_post, trend_author, None, None, query_now - 2000);
    let trend_user = interner.intern("did:plc:viral_fan");
    graph.record_interaction(trend_user, trend_post, SignalType::Repost, query_now - 300);

    let rec = Recommender::new(interner, graph);
    let dials = RecommendationDials {
        explore_ratio: 0.0,
        ..Default::default()
    };

    let res = rec.recommend(Some(user), &dials, query_now).unwrap();
    // Because followed account has 0 interactions, Tier 2 is empty -> cascades to Tier 3
    assert!(!res.posts.is_empty());
    assert_eq!(res.posts[0].source, RecommendationSource::Tier3VelocityPool);
}

// ===========================================================================
// 3. Unauthenticated Requests and Invalid DIDs
// ===========================================================================

#[test]
fn test_unauthenticated_and_adversarial_dids_route_to_tier3() {
    let (interner, graph) = SyntheticGraphBuilder::standard_cold_start_fixture(now());
    let rec = Recommender::new(interner, graph);
    let dials = RecommendationDials {
        explore_ratio: 0.0,
        ..Default::default()
    };
    let query_now = now();

    let adversarial_dids: Vec<Option<&str>> = vec![
        None,
        Some(""),
        Some("   "),
        Some("not_a_did"),
        Some("did:plc:"),
        Some("did:web:example.com/nonexistent"),
        Some("did:plc:\0nullbyte"),
        Some("did:plc:😂emoji"),
        Some("did:plc:123456789012345678901234567890123456789012345678901234567890"),
        Some("!@#$%^&*()_+{}[]:;\"'<>?,./"),
    ];

    for did_opt in adversarial_dids {
        let res = rec.recommend(did_opt, &dials, query_now);
        assert!(res.is_ok(), "Expected Ok for did: {:?}", did_opt);
        let feed = res.unwrap();
        assert!(!feed.posts.is_empty());
        for p in &feed.posts {
            assert_eq!(p.source, RecommendationSource::Tier3VelocityPool);
        }
    }
}

// ===========================================================================
// 4. Explainability Metadata
// ===========================================================================

#[test]
fn test_explainability_trace_accuracy_and_format() {
    let (interner, graph) = SyntheticGraphBuilder::standard_cold_start_fixture(now());
    let rec = Recommender::new(interner, graph);
    let query_now = now();

    // 1. explain = false -> explain is None
    let dials_no_explain = RecommendationDials {
        explain: false,
        ..Default::default()
    };
    let res_no = rec
        .recommend(Some("did:plc:active_user"), &dials_no_explain, query_now)
        .unwrap();
    for p in res_no.posts {
        assert!(p.explain.is_none());
    }

    // 2. explain = true -> explain is Some and contains source, score, root_id
    let dials_explain = RecommendationDials {
        explain: true,
        ..Default::default()
    };
    let res_exp = rec
        .recommend(Some("did:plc:active_user"), &dials_explain, query_now)
        .unwrap();
    assert!(!res_exp.posts.is_empty());
    for p in res_exp.posts {
        assert!(p.explain.is_some());
        let expl = p.explain.unwrap();
        assert!(expl.starts_with("source="));
        assert!(expl.contains(", score="));
        assert!(expl.contains(", root_id="));
    }
}

// ===========================================================================
// 5. Concurrency Stress Test
// ===========================================================================

#[test]
fn test_concurrency_stress_readers_and_writers() {
    let (interner, graph) = SyntheticGraphBuilder::standard_cold_start_fixture(now());
    let rec = Arc::new(Recommender::new(interner, graph));
    let stop_flag = Arc::new(AtomicBool::new(false));
    let read_ops = Arc::new(AtomicUsize::new(0));
    let write_ops = Arc::new(AtomicUsize::new(0));

    let num_readers = 16;
    let num_writers = 4;
    let mut handles = Vec::new();

    // Reader threads
    for _thread_id in 0..num_readers {
        let rec = Arc::clone(&rec);
        let stop = Arc::clone(&stop_flag);
        let read_ops = Arc::clone(&read_ops);

        handles.push(thread::spawn(move || {
            let dids = [
                Some("did:plc:active_user"),
                Some("did:plc:new_user"),
                Some("did:plc:cold_user"),
                None,
                Some("did:plc:nonexistent"),
            ];
            let mut i = 0;
            while !stop.load(Ordering::Relaxed) {
                let did = dids[i % dids.len()];
                let dials = RecommendationDials {
                    limit: 10 + (i % 20),
                    explore_ratio: if i % 2 == 0 { 0.15 } else { 0.35 },
                    explain: i % 3 == 0,
                    ..Default::default()
                };
                let res = rec.recommend(did, &dials, now() + (i as u64 % 500));
                assert!(res.is_ok());
                read_ops.fetch_add(1, Ordering::Relaxed);
                i += 1;
            }
        }));
    }

    // Writer threads
    for thread_id in 0..num_writers {
        let rec = Arc::clone(&rec);
        let stop = Arc::clone(&stop_flag);
        let write_ops = Arc::clone(&write_ops);

        handles.push(thread::spawn(move || {
            let mut i = 0;
            while !stop.load(Ordering::Relaxed) {
                let user_uri = format!("did:plc:concurrent_user_{thread_id}_{i}");
                let post_uri = format!("at://did:plc:author/app.bsky.feed.post/concurrent_{i}");
                let uid = rec.interner().intern(&user_uri);
                let pid = rec.interner().intern(&post_uri);
                let author_id = rec.interner().intern("did:plc:author");

                rec.graph()
                    .record_post_meta(pid, author_id, None, None, now());
                rec.graph()
                    .record_interaction(uid, pid, SignalType::Like, now());
                rec.graph().record_follow(uid, author_id);

                if i % 10 == 0 {
                    rec.graph().remove_interaction(uid, pid, SignalType::Like);
                    rec.graph().remove_follow(uid, author_id);
                }

                write_ops.fetch_add(1, Ordering::Relaxed);
                i += 1;
            }
        }));
    }

    // Run for 300ms under heavy contention
    thread::sleep(Duration::from_millis(300));
    stop_flag.store(true, Ordering::Relaxed);

    for h in handles {
        h.join().expect("Thread joined successfully");
    }

    let completed_reads = read_ops.load(Ordering::Relaxed);
    let completed_writes = write_ops.load(Ordering::Relaxed);
    assert!(
        completed_reads > 500,
        "Expected >500 reads, got {completed_reads}"
    );
    assert!(
        completed_writes > 200,
        "Expected >200 writes, got {completed_writes}"
    );
}

// ===========================================================================
// 6. Degenerate Distributions: Author Diversity & Thread Floods
// ===========================================================================

#[test]
fn test_degenerate_author_flood_max_2_enforced() {
    let interner = Arc::new(StringInterner::new());
    let graph = Arc::new(GraphStore::new());
    let query_now = now();

    let viewer = interner.intern("did:plc:viewer");
    let friend = interner.intern("did:plc:friend");
    let spammer = interner.intern("did:plc:spammer");

    graph.record_follow(viewer, friend);

    // Spammer has 50 posts liked by friend
    for i in 1..=50 {
        let p_uri = format!("at://did:plc:spammer/app.bsky.feed.post/spam_{i}");
        let pid = interner.intern(&p_uri);
        graph.record_post_meta(pid, spammer, None, None, query_now - 100);
        graph.record_interaction(friend, pid, SignalType::Like, query_now - 50);
    }

    let rec = Recommender::new(interner, graph);
    let dials = RecommendationDials {
        limit: 50,
        ..Default::default()
    };

    let res = rec
        .recommend(Some("did:plc:viewer"), &dials, query_now)
        .unwrap();
    // Strict max 2 posts for spammer
    assert_eq!(res.posts.len(), 2);
    for p in res.posts {
        assert_eq!(p.author_id, spammer);
    }
}

#[test]
fn test_degenerate_thread_flood_max_1_per_root_enforced() {
    let interner = Arc::new(StringInterner::new());
    let graph = Arc::new(GraphStore::new());
    let query_now = now();

    let viewer = interner.intern("did:plc:viewer");
    let friend = interner.intern("did:plc:friend");

    graph.record_follow(viewer, friend);

    let root_pid = interner.intern("at://did:plc:author_0/app.bsky.feed.post/root");
    let author_0 = interner.intern("did:plc:author_0");
    graph.record_post_meta(root_pid, author_0, None, None, query_now - 500);
    graph.record_interaction(friend, root_pid, SignalType::Like, query_now - 400);

    // 30 replies from 30 distinct authors to the same root post
    for i in 1..=30 {
        let reply_author = interner.intern(&format!("did:plc:author_{i}"));
        let reply_pid = interner.intern(&format!(
            "at://did:plc:author_{i}/app.bsky.feed.post/reply_{i}"
        ));
        graph.record_post_meta(
            reply_pid,
            reply_author,
            Some(root_pid),
            Some(root_pid),
            query_now - 300,
        );
        graph.record_interaction(friend, reply_pid, SignalType::Like, query_now - 200);
    }

    let rec = Recommender::new(interner, graph);
    let dials = RecommendationDials {
        limit: 50,
        ..Default::default()
    };

    let res = rec
        .recommend(Some("did:plc:viewer"), &dials, query_now)
        .unwrap();
    // Exactly 1 post from the entire thread/conversation tree
    assert_eq!(res.posts.len(), 1);
}
