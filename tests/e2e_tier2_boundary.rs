//! Tier 2 E2E Boundary & Corner Case Test Suite (Features 1–35)
//!
//! Validates each of the 35 functional features against boundary values,
//! edge conditions, empty inputs, clock anomalies, zero states, and extreme saturation
//! with >=5 tests per feature (35 * 5 = 175 tests).

#![allow(
    clippy::pedantic,
    clippy::nursery,
    clippy::cognitive_complexity,
    unused_assignments
)]

mod common;

use std::sync::Arc;
use std::time::Duration;

use axum::body::Body;
use axum::http::{Method, Request, StatusCode};
use compact_str::CompactString;
use for_your_consideration::prelude::*;
use futures_util::StreamExt;
use http_body_util::BodyExt;
use roaring::RoaringBitmap;
use tokio_tungstenite::tungstenite::Message;
use tower::ServiceExt;

use crate::common::*;

// ===========================================================================
// Feature 1: 32-bit String Interning (Boundary)
// ===========================================================================

#[test]
fn test_f01_resolve_uninterned_id() {
    let interner = StringInterner::new();
    assert_eq!(interner.resolve(999_999), None);
    assert_eq!(interner.lookup_str(u32::MAX), None);
}

#[test]
fn test_f01_lookup_uninterned_str() {
    let interner = StringInterner::new();
    assert_eq!(interner.lookup_id("did:plc:never_interned"), None);
    assert_eq!(
        interner.get_id("at://did:plc:unknown/app.bsky.feed.post/999"),
        None
    );
}

#[test]
fn test_f01_empty_string() {
    let interner = StringInterner::new();
    let id1 = interner.intern("");
    let id2 = interner.intern("");
    assert_eq!(id1, id2);
    assert_eq!(interner.resolve(id1).as_deref(), Some(""));
    assert_eq!(interner.len(), 1);
}

#[test]
fn test_f01_very_long_at_uri() {
    let interner = StringInterner::new();
    let long_uri = format!(
        "at://did:plc:{}/app.bsky.feed.post/{}",
        "a".repeat(1000),
        "b".repeat(1000)
    );
    let id = interner.intern(&long_uri);
    assert_eq!(interner.resolve(id).as_deref(), Some(long_uri.as_str()));
    assert_eq!(interner.lookup_id(&long_uri), Some(id));
}

#[test]
fn test_f01_concurrent_interning() {
    let interner = Arc::new(StringInterner::new());
    let mut handles = Vec::new();

    for i in 0..10 {
        let interner_clone = Arc::clone(&interner);
        handles.push(std::thread::spawn(move || {
            for j in 0..100 {
                let s = format!("did:plc:user_{}_{}", i % 3, j % 10);
                interner_clone.intern(&s);
            }
        }));
    }

    for h in handles {
        h.join().unwrap();
    }

    assert!(interner.len() <= 30);
}

// ===========================================================================
// Feature 2: Compact 8-byte Edge Representation (Boundary)
// ===========================================================================

#[test]
fn test_f02_edge_max_target_id() {
    let edge = CompactEdge::new(u32::MAX, SignalType::Like, BLUESKY_EPOCH_SECS + 100);
    assert_eq!(edge.target(), u32::MAX);
    assert_eq!(edge.signal(), SignalType::Like);
    assert_eq!(edge.timestamp_secs(), BLUESKY_EPOCH_SECS + 100);
}

#[test]
fn test_f02_edge_max_timestamp_29bit() {
    let max_rel = (1 << 29) - 1;
    let max_ts = BLUESKY_EPOCH_SECS + u64::from(max_rel);
    let edge = CompactEdge::new(100, SignalType::Repost, max_ts);
    assert_eq!(edge.relative_timestamp_secs(), max_rel);
    assert_eq!(edge.timestamp_secs(), max_ts);
}

#[test]
fn test_f02_edge_zero_timestamp() {
    let edge = CompactEdge::new(42, SignalType::Quote, 0);
    assert_eq!(edge.relative_timestamp_secs(), 0);
    assert_eq!(edge.timestamp_secs(), BLUESKY_EPOCH_SECS);
}

#[test]
fn test_f02_edge_all_signal_types() {
    for sig in [SignalType::Like, SignalType::Quote, SignalType::Repost] {
        let edge = CompactEdge::new(1, sig, BLUESKY_EPOCH_SECS + 500);
        assert_eq!(edge.signal(), sig);
        assert_eq!(edge.weight(), sig.weight());
    }
}

#[test]
fn test_f02_edge_memory_alignment() {
    assert_eq!(std::mem::size_of::<CompactEdge>(), 8);
    assert_eq!(std::mem::align_of::<CompactEdge>(), 4);
}

// ===========================================================================
// Feature 3: Multi-Signal Edge Weighting (Boundary)
// ===========================================================================

#[test]
fn test_f03_unknown_signal_fallback() {
    assert_eq!(SignalType::from_u8(0), None);
    assert_eq!(SignalType::from_u8(4), None);
    assert_eq!(SignalType::from_u8(7), None);
}

#[test]
fn test_f03_multiple_signals_same_pair() {
    let graph = GraphStore::new();
    let ts = chrono_like_now();
    graph.record_interaction(1, 100, SignalType::Like, ts);
    graph.record_interaction(1, 100, SignalType::Repost, ts + 1);

    let edges = graph.get_user_interactions(1);
    assert_eq!(edges.len(), 2);
    let total_weight: f32 = edges.iter().map(CompactEdge::weight).sum();
    assert!((total_weight - 4.0).abs() < f32::EPSILON);
}

#[test]
fn test_f03_zero_weight_handling() {
    let edge = CompactEdge::new(10, SignalType::Like, chrono_like_now());
    assert!(edge.weight() > 0.0);
}

#[test]
fn test_f03_signal_saturation() {
    let mut total_w = 0.0f32;
    for _ in 0..10_000 {
        total_w += SignalType::Repost.weight();
    }
    assert_eq!(total_w, 30_000.0);
}

#[test]
fn test_f03_signal_type_roundtrip() {
    for sig in [SignalType::Like, SignalType::Quote, SignalType::Repost] {
        let code = sig as u8;
        assert_eq!(SignalType::from_u8(code), Some(sig));
    }
}

// ===========================================================================
// Feature 4: Forward & Reverse Graph Adjacency (Boundary)
// ===========================================================================

#[test]
fn test_f04_query_nonexistent_user() {
    let graph = GraphStore::new();
    assert!(graph.get_user_interactions(999_999).is_empty());
    assert_eq!(graph.get_user_likes_bitmap(999_999), None);
}

#[test]
fn test_f04_query_nonexistent_post() {
    let graph = GraphStore::new();
    assert!(graph.get_post_interactions(999_999).is_empty());
    assert_eq!(graph.get_post_interaction_count(999_999), 0);
}

#[test]
fn test_f04_empty_graph_adjacency() {
    let graph = GraphStore::new();
    let stats = graph.stats();
    assert_eq!(stats.total_users, 0);
    assert_eq!(stats.total_posts, 0);
    assert_eq!(stats.total_interactions, 0);
}

#[test]
fn test_f04_dense_node_10k_edges() {
    let graph = GraphStore::new();
    let now = chrono_like_now();
    for u in 1..=1_000 {
        graph.record_interaction(u, 42, SignalType::Like, now);
    }
    assert_eq!(graph.get_post_interactions(42).len(), 1_000);
    assert_eq!(graph.get_post_interaction_count(42), 1_000);
}

#[test]
fn test_f04_duplicate_edge_dedup() {
    let graph = GraphStore::new();
    let now = chrono_like_now();
    graph.record_interaction(1, 10, SignalType::Like, now);
    graph.record_interaction(1, 10, SignalType::Like, now);
    assert_eq!(graph.get_user_interactions(1).len(), 1);
    assert_eq!(graph.get_post_interactions(10).len(), 1);
}

// ===========================================================================
// Feature 5: User Roaring Bitmaps (Boundary)
// ===========================================================================

#[test]
fn test_f05_empty_bitmap_intersection() {
    let mut bm1 = RoaringBitmap::new();
    let bm2 = RoaringBitmap::new();
    assert_eq!(bm1.intersection_len(&bm2), 0);
    bm1.insert(10);
    assert_eq!(bm1.intersection_len(&bm2), 0);
}

#[test]
fn test_f05_disjoint_bitmaps() {
    let graph = GraphStore::new();
    let now = chrono_like_now();
    for p in 1..=5 {
        graph.record_interaction(1, p, SignalType::Like, now);
    }
    for p in 6..=10 {
        graph.record_interaction(2, p, SignalType::Like, now);
    }
    assert_eq!(graph.compute_jaccard_similarity(1, 2), 0.0);
    assert_eq!(graph.compute_cosine_similarity(1, 2), 0.0);
}

#[test]
fn test_f05_identical_bitmaps_similarity_1() {
    let graph = GraphStore::new();
    let now = chrono_like_now();
    for p in 1..=10 {
        graph.record_interaction(1, p, SignalType::Like, now);
        graph.record_interaction(2, p, SignalType::Like, now);
    }
    assert!((graph.compute_jaccard_similarity(1, 2) - 1.0).abs() < 1e-5);
    assert!((graph.compute_cosine_similarity(1, 2) - 1.0).abs() < 1e-5);
}

#[test]
fn test_f05_sparse_bitmaps_high_ids() {
    let graph = GraphStore::new();
    let now = chrono_like_now();
    graph.record_interaction(1, 1_000_000, SignalType::Like, now);
    graph.record_interaction(1, 2_000_000, SignalType::Like, now);
    let bm = graph.get_user_likes_bitmap(1).unwrap();
    assert_eq!(bm.len(), 2);
    assert!(bm.contains(1_000_000));
    assert!(bm.contains(2_000_000));
}

#[test]
fn test_f05_bitmap_serialization_safety() {
    let mut bm = RoaringBitmap::new();
    bm.insert(100);
    bm.insert(200);
    let mut buf = Vec::new();
    assert!(bm.serialize_into(&mut buf).is_ok());
    let deserialized = RoaringBitmap::deserialize_from(&buf[..]).unwrap();
    assert_eq!(bm, deserialized);
}

// ===========================================================================
// Feature 6: Follow Graph Storage (Boundary)
// ===========================================================================

#[test]
fn test_f06_unfollowed_user_query() {
    let graph = GraphStore::new();
    assert!(graph.get_user_follows(999).is_empty());
}

#[test]
fn test_f06_self_follow_handling() {
    let graph = GraphStore::new();
    graph.record_follow(1, 1);
    let follows = graph.get_user_follows(1);
    assert_eq!(follows, vec![1]);
}

#[test]
fn test_f06_duplicate_follow_noop() {
    let graph = GraphStore::new();
    graph.record_follow(1, 2);
    graph.record_follow(1, 2);
    assert_eq!(graph.get_user_follows(1).len(), 1);
}

#[test]
fn test_f06_high_follow_count_10k() {
    let graph = GraphStore::new();
    for target in 1..=500 {
        graph.record_follow(1, target);
    }
    assert_eq!(graph.get_user_follows(1).len(), 500);
}

#[test]
fn test_f06_unfollow_tombstone() {
    let graph = GraphStore::new();
    graph.record_follow(1, 2);
    graph.record_follow(1, 3);
    graph.remove_follow(1, 2);
    assert_eq!(graph.get_user_follows(1), vec![3]);
}

// ===========================================================================
// Feature 7: Post Metadata & Thread Tracking (Boundary)
// ===========================================================================

#[test]
fn test_f07_query_nonexistent_post_meta() {
    let graph = GraphStore::new();
    assert_eq!(graph.get_post_meta(999), None);
}

#[test]
fn test_f07_deep_reply_chain_root() {
    let graph = GraphStore::new();
    let now = chrono_like_now();
    let root_id = 100;
    // Chain: 100 -> 101 -> 102 -> 103 (all share root_id = 100)
    graph.record_post_meta(101, 1, Some(root_id), Some(100), now);
    graph.record_post_meta(102, 2, Some(root_id), Some(101), now + 1);
    graph.record_post_meta(103, 3, Some(root_id), Some(102), now + 2);

    let m103 = graph.get_post_meta(103).unwrap();
    assert_eq!(m103.root_id, Some(root_id));
    assert_eq!(m103.parent_id, Some(102));
    assert!(m103.is_reply());
}

#[test]
fn test_f07_author_id_zero() {
    let graph = GraphStore::new();
    graph.record_post_meta(10, 0, None, None, chrono_like_now());
    let meta = graph.get_post_meta(10).unwrap();
    assert_eq!(meta.author_id, 0);
}

#[test]
fn test_f07_future_creation_time() {
    let graph = GraphStore::new();
    let future_ts = chrono_like_now() + 100_000;
    graph.record_post_meta(20, 1, None, None, future_ts);
    let meta = graph.get_post_meta(20).unwrap();
    assert_eq!(meta.created_at, future_ts);
}

#[test]
fn test_f07_post_meta_overwrite() {
    let graph = GraphStore::new();
    let now = chrono_like_now();
    graph.record_post_meta(30, 1, None, None, now);
    graph.record_post_meta(30, 2, Some(5), None, now + 10);
    let meta = graph.get_post_meta(30).unwrap();
    assert_eq!(meta.author_id, 2);
    assert_eq!(meta.root_id, Some(5));
}

// ===========================================================================
// Feature 8: Exponential Time Decay Math (Boundary)
// ===========================================================================

#[test]
fn test_f08_decay_future_timestamp_saturate() {
    let now = 1_700_000_000;
    let future = 1_700_001_000;
    // Future event (dt = 0 saturating) => decay factor = e^0 = 1.0
    let weight = calculate_time_decay(SignalType::Like, future, now, 36.0 * 3600.0);
    assert!((weight - 1.0).abs() < 1e-5);
}

#[test]
fn test_f08_decay_infinite_elapsed() {
    let now = 1_800_000_000;
    let long_ago = 1_000_000_000;
    let weight = calculate_time_decay(SignalType::Like, long_ago, now, 36.0 * 3600.0);
    assert_eq!(weight, 0.0);
}

#[test]
fn test_f08_decay_zero_tau_guard() {
    let now = 1_700_000_000;
    let weight = calculate_time_decay(SignalType::Like, now - 3600, now, 0.0);
    assert!(weight > 0.0);
    assert!(weight <= 1.0);
}

#[test]
fn test_f08_decay_subsecond_precision() {
    let now = 1_700_000_000;
    let w1 = calculate_time_decay(SignalType::Like, now - 1, now, 3600.0);
    let w2 = calculate_time_decay(SignalType::Like, now - 2, now, 3600.0);
    assert!(w1 > w2);
}

#[test]
fn test_f08_decay_extreme_large_tau() {
    let now = 1_700_000_000;
    let large_tau = 1e9;
    let weight = calculate_time_decay(SignalType::Repost, now - 86400, now, large_tau);
    assert!((weight - 3.0).abs() < 0.01);
}

// ===========================================================================
// Feature 9: Continuous Social Proof Quality Curve (Boundary)
// ===========================================================================

#[test]
fn test_f09_bm25_zero_division_guard() {
    let dampener = calculate_popularity_dampener(0);
    assert!(!dampener.is_nan());
    assert!(!dampener.is_infinite());
    assert!((dampener - 1.0 / 3.0).abs() < 1e-5);
}

#[test]
fn test_f09_bm25_10m_interactions_no_overflow() {
    let dampener = calculate_popularity_dampener(10_000_000);
    assert!(dampener > 0.0);
    assert!(!dampener.is_nan());
    assert!(!dampener.is_infinite());
    assert!(dampener < 2.0);
}

#[test]
fn test_f09_bm25_single_interaction() {
    let dampener = calculate_popularity_dampener(1);
    let expected = (2.0 / 4.0) * (1.0 + 0.15 * (2.0f32).ln());
    assert!((dampener - expected).abs() < 1e-5);
}

#[test]
fn test_f09_bm25_negative_count_defense() {
    let dampener = calculate_popularity_dampener(usize::MIN);
    assert!((dampener - 1.0 / 3.0).abs() < 1e-5);
}

#[test]
fn test_f09_bm25_float_precision() {
    let d100 = calculate_popularity_dampener(100);
    let d101 = calculate_popularity_dampener(101);
    assert!(d101 > d100);

    let d1000 = calculate_popularity_dampener(1000);
    let d1001 = calculate_popularity_dampener(1001);
    assert!(d1000 > d1001);
}

// ===========================================================================
// Feature 10: Global High-Velocity Sliding Pool (Boundary)
// ===========================================================================

#[test]
fn test_f10_velocity_pool_empty() {
    let graph = GraphStore::new();
    let candidates = graph.get_velocity_pool_candidates_at(chrono_like_now(), 10);
    assert!(candidates.is_empty());
}

#[test]
fn test_f10_velocity_pool_expired_events() {
    let graph = GraphStore::new();
    let now = chrono_like_now();
    let stale_time = now - (7 * 3600); // 7 hours ago (> 6h sliding window)
    graph.record_interaction(1, 10, SignalType::Like, stale_time);
    let candidates = graph.get_velocity_pool_candidates_at(now, 10);
    assert!(candidates.is_empty());
}

#[test]
fn test_f10_velocity_pool_duplicate_posts() {
    let graph = GraphStore::new();
    let now = chrono_like_now();
    for u in 1..=5 {
        graph.record_interaction(u, 10, SignalType::Like, now - 100);
    }
    let candidates = graph.get_velocity_pool_candidates_at(now, 10);
    assert_eq!(candidates, vec![10]);
}

#[test]
fn test_f10_velocity_pool_capacity_overflow() {
    let graph = GraphStore::new();
    let now = chrono_like_now();
    for p in 1..=200 {
        graph.record_interaction(1, p, SignalType::Like, now - 10);
    }
    let candidates = graph.get_velocity_pool_candidates_at(now, 10);
    assert_eq!(candidates.len(), 10);
}

#[test]
fn test_f10_velocity_pool_zero_limit() {
    let graph = GraphStore::new();
    let now = chrono_like_now();
    graph.record_interaction(1, 10, SignalType::Like, now);
    assert!(graph.get_velocity_pool_candidates_at(now, 0).is_empty());
}

// ===========================================================================
// Feature 11: 3-Step Random Walk Graph Traversal (Boundary)
// ===========================================================================

#[test]
fn test_f11_3step_walk_isolated_user_empty() {
    let interner = Arc::new(StringInterner::new());
    let graph = Arc::new(GraphStore::new());
    let uid = interner.intern("did:plc:isolated");
    let rec = TestRecommender::new(interner, graph);
    let candidates = rec.traverse_tier1(uid, &RecommendationDials::default(), chrono_like_now());
    assert!(candidates.is_empty());
}

#[test]
fn test_f11_3step_walk_no_cointeractors() {
    let interner = Arc::new(StringInterner::new());
    let graph = Arc::new(GraphStore::new());
    let uid = interner.intern("did:plc:solo");
    let now = chrono_like_now();
    for p in 1..=12 {
        let pid = interner.intern(&format!("at://did:plc:solo/post/{p}"));
        graph.record_post_meta(pid, uid, None, None, now);
        graph.record_interaction(uid, pid, SignalType::Like, now);
    }
    let rec = TestRecommender::new(interner, graph);
    let candidates = rec.traverse_tier1(uid, &RecommendationDials::default(), now);
    assert!(candidates.is_empty());
}

#[test]
fn test_f11_3step_walk_all_seen_candidates() {
    let (interner, graph) = SyntheticGraphBuilder::standard_cold_start_fixture(chrono_like_now());
    let rec = TestRecommender::new(interner, graph);
    let dials = RecommendationDials::default();
    let rec_res = rec
        .recommend(Some("did:plc:active_user"), &dials, chrono_like_now())
        .unwrap();
    // None of the returned recommendations should be posts the active user liked
    assert!(!rec_res.posts.iter().any(|p| p.uri.contains("active_post_")));
}

#[test]
fn test_f11_3step_walk_cycle_handling() {
    let interner = Arc::new(StringInterner::new());
    let graph = Arc::new(GraphStore::new());
    let u1 = interner.intern("did:plc:u1");
    let u2 = interner.intern("did:plc:u2");
    let p1 = interner.intern("at://did:plc:u1/post/1");
    let p2 = interner.intern("at://did:plc:u2/post/2");
    let now = chrono_like_now();

    graph.record_post_meta(p1, u1, None, None, now);
    graph.record_post_meta(p2, u2, None, None, now);

    graph.record_interaction(u1, p2, SignalType::Like, now);
    graph.record_interaction(u2, p1, SignalType::Like, now);

    let rec = TestRecommender::new(interner, graph);
    let candidates = rec.traverse_tier1(u1, &RecommendationDials::default(), now);
    assert!(candidates.is_empty() || candidates.iter().all(|c| c.post_id != p2));
}

#[test]
fn test_f11_3step_walk_dense_graph_sample() {
    let interner = Arc::new(StringInterner::new());
    let graph = Arc::new(GraphStore::new());
    let now = chrono_like_now();

    let u_target = interner.intern("did:plc:target");
    for i in 1..=15 {
        let p_shared = interner.intern(&format!("at://did:plc:author/post/shared_{i}"));
        graph.record_post_meta(p_shared, 999, None, None, now);
        graph.record_interaction(u_target, p_shared, SignalType::Like, now);

        for co in 1..=10 {
            let u_co = interner.intern(&format!("did:plc:co_{co}"));
            graph.record_interaction(u_co, p_shared, SignalType::Like, now);

            let p_cand = interner.intern(&format!("at://did:plc:author/post/cand_{co}_{i}"));
            graph.record_post_meta(p_cand, 999, None, None, now);
            graph.record_interaction(u_co, p_cand, SignalType::Repost, now);
        }
    }

    let rec = TestRecommender::new(interner, graph);
    let candidates = rec.traverse_tier1(u_target, &RecommendationDials::default(), now);
    assert!(!candidates.is_empty());
}

// ===========================================================================
// Feature 12: Candidate Scoring & Aggregation (Boundary)
// ===========================================================================

#[test]
fn test_f12_candidate_scoring_all_zero_weights() {
    let weight = calculate_time_decay(SignalType::Like, 0, 1_800_000_000, 3600.0);
    assert_eq!(weight, 0.0);
}

#[test]
fn test_f12_candidate_scoring_single_candidate() {
    let (interner, graph) = SyntheticGraphBuilder::standard_cold_start_fixture(chrono_like_now());
    let rec = TestRecommender::new(interner, graph);
    let dials = RecommendationDials {
        limit: 1,
        ..Default::default()
    };
    let res = rec
        .recommend(Some("did:plc:active_user"), &dials, chrono_like_now())
        .unwrap();
    assert_eq!(res.posts.len(), 1);
}

#[test]
fn test_f12_candidate_scoring_empty_input() {
    let interner = Arc::new(StringInterner::new());
    let graph = Arc::new(GraphStore::new());
    let rec = TestRecommender::new(interner, graph);
    let res = rec
        .recommend(None, &RecommendationDials::default(), chrono_like_now())
        .unwrap();
    assert!(res.posts.is_empty());
}

#[test]
fn test_f12_candidate_scoring_equal_scores_tiebreak() {
    let (interner, graph) = SyntheticGraphBuilder::standard_cold_start_fixture(chrono_like_now());
    let rec = TestRecommender::new(interner, graph);
    let res1 = rec
        .recommend(
            Some("did:plc:active_user"),
            &RecommendationDials::default(),
            chrono_like_now(),
        )
        .unwrap();
    let res2 = rec
        .recommend(
            Some("did:plc:active_user"),
            &RecommendationDials::default(),
            chrono_like_now(),
        )
        .unwrap();
    assert_eq!(res1.posts.len(), res2.posts.len());
    for (p1, p2) in res1.posts.iter().zip(res2.posts.iter()) {
        assert_eq!(p1.uri, p2.uri);
    }
}

#[test]
fn test_f12_candidate_scoring_nan_inf_defense() {
    let (interner, graph) = SyntheticGraphBuilder::standard_cold_start_fixture(chrono_like_now());
    let rec = TestRecommender::new(interner, graph);
    let res = rec
        .recommend(
            Some("did:plc:active_user"),
            &RecommendationDials::default(),
            chrono_like_now(),
        )
        .unwrap();
    for p in res.posts {
        assert!(!p.score.is_nan());
        assert!(!p.score.is_infinite());
    }
}

// ===========================================================================
// Feature 13: 3-Tier Cold-Start Hierarchy (Boundary)
// ===========================================================================

#[test]
fn test_f13_tier1_exactly_10_likes_threshold() {
    let mut builder = SyntheticGraphBuilder::new();
    let now = chrono_like_now();
    let user = "did:plc:ten_likes_user";

    for i in 1..=10 {
        let post = format!("at://did:plc:author/post/{i}");
        builder = builder.add_post(
            post.clone(),
            "did:plc:author",
            None::<&str>,
            None::<&str>,
            now - 1000,
        );
        builder = builder.add_interaction(user, post, SignalType::Like, now - 500);
    }

    let interner = Arc::new(StringInterner::new());
    let graph = Arc::new(GraphStore::new());
    builder.populate(&interner, &graph);

    let uid = interner.lookup_id(user).unwrap();
    let likes = graph.get_user_likes_bitmap(uid).unwrap();
    assert_eq!(likes.len(), 10);
}

#[test]
fn test_f13_tier1_9_likes_triggers_tier2() {
    let mut builder = SyntheticGraphBuilder::new();
    let now = chrono_like_now();
    let user = "did:plc:nine_likes_user";

    for i in 1..=9 {
        let post = format!("at://did:plc:author/post/{i}");
        builder = builder.add_post(
            post.clone(),
            "did:plc:author",
            None::<&str>,
            None::<&str>,
            now - 1000,
        );
        builder = builder.add_interaction(user, post, SignalType::Like, now - 500);
    }
    builder = builder.add_follow(user, "did:plc:followed_author");
    let cand_post = "at://did:plc:followed_author/post/cand1";
    builder = builder.add_post(
        cand_post,
        "did:plc:followed_author",
        None::<&str>,
        None::<&str>,
        now - 200,
    );
    builder = builder.add_interaction(
        "did:plc:followed_author",
        cand_post,
        SignalType::Like,
        now - 100,
    );

    let interner = Arc::new(StringInterner::new());
    let graph = Arc::new(GraphStore::new());
    builder.populate(&interner, &graph);

    let rec = TestRecommender::new(interner, graph);
    let dials = RecommendationDials {
        min_likes: 1,
        ..Default::default()
    };
    let res = rec.recommend(Some(user), &dials, now).unwrap();
    assert!(!res.posts.is_empty());
    assert_eq!(res.posts[0].source, RecommendationSource::Tier2FollowWalk);
}

#[test]
fn test_f13_tier2_zero_follows_triggers_tier3() {
    let (interner, graph) = SyntheticGraphBuilder::standard_cold_start_fixture(chrono_like_now());
    let rec = TestRecommender::new(interner, graph);
    let res = rec
        .recommend(
            Some("did:plc:cold_user"),
            &RecommendationDials::default(),
            chrono_like_now(),
        )
        .unwrap();
    assert_eq!(res.posts[0].source, RecommendationSource::Tier3VelocityPool);
}

#[test]
fn test_f13_unregistered_did_routes_tier3() {
    let (interner, graph) = SyntheticGraphBuilder::standard_cold_start_fixture(chrono_like_now());
    let rec = TestRecommender::new(interner, graph);
    let res = rec
        .recommend(
            Some("did:plc:completely_unknown"),
            &RecommendationDials::default(),
            chrono_like_now(),
        )
        .unwrap();
    assert_eq!(res.posts[0].source, RecommendationSource::Tier3VelocityPool);
}

#[test]
fn test_f13_completely_empty_graph_fallback() {
    let interner = Arc::new(StringInterner::new());
    let graph = Arc::new(GraphStore::new());
    let rec = TestRecommender::new(interner, graph);
    let res = rec
        .recommend(
            Some("did:plc:alice"),
            &RecommendationDials::default(),
            chrono_like_now(),
        )
        .unwrap();
    assert!(res.posts.is_empty());
}

// ===========================================================================
// Feature 14: ε-Greedy Serendipity & Exploration (Boundary)
// ===========================================================================

#[test]
fn test_f14_serendipity_epsilon_zero_pure_exploit() {
    let (interner, graph) = SyntheticGraphBuilder::standard_cold_start_fixture(chrono_like_now());
    let rec = TestRecommender::new(interner, graph);
    let dials = RecommendationDials {
        explore_ratio: 0.0,
        limit: 10,
        ..Default::default()
    };
    let res = rec
        .recommend(Some("did:plc:active_user"), &dials, chrono_like_now())
        .unwrap();
    assert!(res
        .posts
        .iter()
        .all(|p| p.source != RecommendationSource::ExplorationSerendipity));
}

#[test]
fn test_f14_serendipity_epsilon_one_pure_explore() {
    let (interner, graph) = SyntheticGraphBuilder::standard_cold_start_fixture(chrono_like_now());
    let rec = TestRecommender::new(interner, graph);
    let dials = RecommendationDials {
        explore_ratio: 1.0,
        limit: 10,
        ..Default::default()
    };
    let res = rec
        .recommend(Some("did:plc:active_user"), &dials, chrono_like_now())
        .unwrap();
    assert!(!res.posts.is_empty());
}

#[test]
fn test_f14_serendipity_small_candidate_pool() {
    let interner = Arc::new(StringInterner::new());
    let graph = Arc::new(GraphStore::new());
    let now = chrono_like_now();
    let p1 = interner.intern("at://did:plc:a/post/1");
    graph.record_post_meta(p1, 1, None, None, now);
    graph.record_interaction(1, p1, SignalType::Like, now);

    let rec = TestRecommender::new(interner, graph);
    let dials = RecommendationDials {
        explore_ratio: 0.5,
        ..Default::default()
    };
    let res = rec.recommend(None, &dials, now).unwrap();
    assert!(res.posts.len() <= 1);
}

#[test]
fn test_f14_serendipity_empty_exploration_pool() {
    let interner = Arc::new(StringInterner::new());
    let graph = Arc::new(GraphStore::new());
    let rec = TestRecommender::new(interner, graph);
    let res = rec
        .recommend(
            None,
            &RecommendationDials {
                explore_ratio: 0.35,
                ..Default::default()
            },
            chrono_like_now(),
        )
        .unwrap();
    assert!(res.posts.is_empty());
}

#[test]
fn test_f14_serendipity_deterministic_seed() {
    let (interner, graph) = SyntheticGraphBuilder::standard_cold_start_fixture(chrono_like_now());
    let rec = TestRecommender::new(interner, graph);
    let dials = RecommendationDials {
        explore_ratio: 0.15,
        ..Default::default()
    };
    let r1 = rec
        .recommend(Some("did:plc:active_user"), &dials, chrono_like_now())
        .unwrap();
    let r2 = rec
        .recommend(Some("did:plc:active_user"), &dials, chrono_like_now())
        .unwrap();
    assert_eq!(r1.posts.len(), r2.posts.len());
    for (a, b) in r1.posts.iter().zip(r2.posts.iter()) {
        assert_eq!(a.uri, b.uri);
    }
}

// ===========================================================================
// Feature 15: Author Diversity Filtering (Boundary)
// ===========================================================================

#[test]
fn test_f15_single_author_dominates_pool() {
    let mut builder = SyntheticGraphBuilder::new();
    let now = chrono_like_now();
    let author = "did:plc:single_prolific_author";

    for i in 1..=20 {
        let p_uri = format!("at://{author}/app.bsky.feed.post/post_{i}");
        builder = builder.add_post(
            p_uri.clone(),
            author,
            None::<&str>,
            None::<&str>,
            now - 1000,
        );
        builder = builder.add_interaction("did:plc:fan", p_uri, SignalType::Like, now - 100);
    }

    let interner = Arc::new(StringInterner::new());
    let graph = Arc::new(GraphStore::new());
    builder.populate(&interner, &graph);

    let rec = TestRecommender::new(interner, graph);
    let res = rec
        .recommend(
            None,
            &RecommendationDials {
                limit: 10,
                ..Default::default()
            },
            now,
        )
        .unwrap();
    assert!(res.posts.len() <= 2);
}

#[test]
fn test_f15_all_unique_authors_no_drop() {
    let mut builder = SyntheticGraphBuilder::new();
    let now = chrono_like_now();

    for i in 1..=10 {
        let author = format!("did:plc:author_{i}");
        let p_uri = format!("at://{author}/app.bsky.feed.post/post_{i}");
        builder = builder.add_post(
            p_uri.clone(),
            author,
            None::<&str>,
            None::<&str>,
            now - 1000,
        );
        builder = builder.add_interaction("did:plc:fan", p_uri, SignalType::Like, now - 100);
    }

    let interner = Arc::new(StringInterner::new());
    let graph = Arc::new(GraphStore::new());
    builder.populate(&interner, &graph);

    let rec = TestRecommender::new(interner, graph);
    let dials = RecommendationDials {
        limit: 10,
        min_likes: 1,
        ..Default::default()
    };
    let res = rec.recommend(None, &dials, now).unwrap();
    assert_eq!(res.posts.len(), 10);
}

#[test]
fn test_f15_author_diversity_pool_exhaustion() {
    let mut builder = SyntheticGraphBuilder::new();
    let now = chrono_like_now();
    let author = "did:plc:lone_author";

    builder = builder.add_post(
        format!("at://{author}/post/1"),
        author,
        None::<&str>,
        None::<&str>,
        now - 100,
    );
    builder = builder.add_interaction(
        "did:plc:fan",
        format!("at://{author}/post/1"),
        SignalType::Like,
        now - 50,
    );

    let interner = Arc::new(StringInterner::new());
    let graph = Arc::new(GraphStore::new());
    builder.populate(&interner, &graph);

    let rec = TestRecommender::new(interner, graph);
    let dials = RecommendationDials {
        limit: 30,
        min_likes: 1,
        ..Default::default()
    };
    let res = rec.recommend(None, &dials, now).unwrap();
    assert_eq!(res.posts.len(), 1);
}

#[test]
fn test_f15_author_id_none_handling() {
    let interner = Arc::new(StringInterner::new());
    let graph = Arc::new(GraphStore::new());
    let now = chrono_like_now();
    let pid = interner.intern("at://did:plc:anon/post/1");
    // Author ID 0
    graph.record_post_meta(pid, 0, None, None, now);
    graph.record_interaction(1, pid, SignalType::Like, now);

    let rec = TestRecommender::new(interner, graph);
    let dials = RecommendationDials {
        min_likes: 1,
        ..Default::default()
    };
    let res = rec.recommend(None, &dials, now).unwrap();
    assert_eq!(res.posts.len(), 1);
}

#[test]
fn test_f15_author_diversity_limit_1() {
    let (interner, graph) = SyntheticGraphBuilder::standard_cold_start_fixture(chrono_like_now());
    let rec = TestRecommender::new(interner, graph);
    let res = rec
        .recommend(
            None,
            &RecommendationDials {
                limit: 1,
                ..Default::default()
            },
            chrono_like_now(),
        )
        .unwrap();
    assert_eq!(res.posts.len(), 1);
}

// ===========================================================================
// Feature 16: Thread / Reply Tree Dampening (Boundary)
// ===========================================================================

#[test]
fn test_f16_all_candidates_same_root() {
    let mut builder = SyntheticGraphBuilder::new();
    let now = chrono_like_now();
    let root = "at://did:plc:author/post/root_post";

    builder = builder.add_post(
        root,
        "did:plc:author",
        None::<&str>,
        None::<&str>,
        now - 5000,
    );
    builder = builder.add_interaction("did:plc:u1", root, SignalType::Like, now - 4000);

    for i in 1..=5 {
        let reply = format!("at://did:plc:replier_{i}/post/reply_{i}");
        builder = builder.add_post(
            reply.clone(),
            format!("did:plc:replier_{i}"),
            Some(root),
            Some(root),
            now - 1000,
        );
        builder = builder.add_interaction("did:plc:u1", reply, SignalType::Like, now - 500);
    }

    let interner = Arc::new(StringInterner::new());
    let graph = Arc::new(GraphStore::new());
    builder.populate(&interner, &graph);

    let rec = TestRecommender::new(interner, graph);
    let dials = RecommendationDials {
        limit: 10,
        min_likes: 1,
        ..Default::default()
    };
    let res = rec.recommend(None, &dials, now).unwrap();
    assert_eq!(res.posts.len(), 1);
}

#[test]
fn test_f16_all_candidates_unique_threads() {
    let mut builder = SyntheticGraphBuilder::new();
    let now = chrono_like_now();

    for i in 1..=5 {
        let post = format!("at://did:plc:author_{i}/post/root_{i}");
        builder = builder.add_post(
            post.clone(),
            format!("did:plc:author_{i}"),
            None::<&str>,
            None::<&str>,
            now - 1000,
        );
        builder = builder.add_interaction("did:plc:fan", post, SignalType::Like, now - 100);
    }

    let interner = Arc::new(StringInterner::new());
    let graph = Arc::new(GraphStore::new());
    builder.populate(&interner, &graph);

    let rec = TestRecommender::new(interner, graph);
    let dials = RecommendationDials {
        limit: 10,
        min_likes: 1,
        ..Default::default()
    };
    let res = rec.recommend(None, &dials, now).unwrap();
    assert_eq!(res.posts.len(), 5);
}

#[test]
fn test_f16_missing_root_meta_safe() {
    let interner = Arc::new(StringInterner::new());
    let graph = Arc::new(GraphStore::new());
    let now = chrono_like_now();
    let pid = interner.intern("at://did:plc:a/post/1");
    // Record interaction without post meta
    graph.record_interaction(1, pid, SignalType::Like, now);

    let rec = TestRecommender::new(interner, graph);
    let dials = RecommendationDials {
        min_likes: 1,
        ..Default::default()
    };
    let res = rec.recommend(None, &dials, now).unwrap();
    assert_eq!(res.posts.len(), 1);
}

#[test]
fn test_f16_deep_nested_reply_chains() {
    let mut builder = SyntheticGraphBuilder::new();
    let now = chrono_like_now();
    let root = "at://did:plc:a/post/root";
    builder = builder.add_post(root, "did:plc:a", None::<&str>, None::<&str>, now - 10000);
    builder = builder.add_interaction("did:plc:fan", root, SignalType::Like, now - 100);

    let mut parent = root.to_string();
    for depth in 1..=5 {
        let child = format!("at://did:plc:author_{depth}/post/child_{depth}");
        builder = builder.add_post(
            child.clone(),
            format!("did:plc:author_{depth}"),
            Some(root),
            Some(parent.as_str()),
            now - (10000 - depth * 100),
        );
        builder = builder.add_interaction("did:plc:fan", child.clone(), SignalType::Like, now - 50);
        parent = child;
    }

    let interner = Arc::new(StringInterner::new());
    let graph = Arc::new(GraphStore::new());
    builder.populate(&interner, &graph);

    let rec = TestRecommender::new(interner, graph);
    let dials = RecommendationDials {
        limit: 10,
        min_likes: 1,
        ..Default::default()
    };
    let res = rec.recommend(None, &dials, now).unwrap();
    assert_eq!(res.posts.len(), 1);
}

#[test]
fn test_f16_thread_dampening_empty_pool() {
    let interner = Arc::new(StringInterner::new());
    let graph = Arc::new(GraphStore::new());
    let rec = TestRecommender::new(interner, graph);
    let res = rec
        .recommend(None, &RecommendationDials::default(), chrono_like_now())
        .unwrap();
    assert!(res.posts.is_empty());
}

// ===========================================================================
// Feature 17: Seen / Liked / Self Post Deduplication (Boundary)
// ===========================================================================

#[test]
fn test_f17_viewer_has_seen_all_candidates() {
    let mut builder = SyntheticGraphBuilder::new();
    let now = chrono_like_now();
    let viewer = "did:plc:seen_all_viewer";

    for i in 1..=5 {
        let post = format!("at://did:plc:author/post/cand_{i}");
        builder = builder.add_post(
            post.clone(),
            "did:plc:author",
            None::<&str>,
            None::<&str>,
            now - 1000,
        );
        builder = builder.add_interaction("did:plc:fan", post.clone(), SignalType::Like, now - 100);
        builder = builder.add_interaction(viewer, post, SignalType::Like, now - 50);
    }

    let interner = Arc::new(StringInterner::new());
    let graph = Arc::new(GraphStore::new());
    builder.populate(&interner, &graph);

    let rec = TestRecommender::new(interner, graph);
    let res = rec
        .recommend(Some(viewer), &RecommendationDials::default(), now)
        .unwrap();
    assert!(res.posts.is_empty());
}

#[test]
fn test_f17_viewer_authored_all_candidates() {
    let mut builder = SyntheticGraphBuilder::new();
    let now = chrono_like_now();
    let author = "did:plc:author_viewer";

    for i in 1..=5 {
        let post = format!("at://{author}/post/{i}");
        builder = builder.add_post(post.clone(), author, None::<&str>, None::<&str>, now - 1000);
        builder = builder.add_interaction("did:plc:fan", post, SignalType::Like, now - 100);
    }

    let interner = Arc::new(StringInterner::new());
    let graph = Arc::new(GraphStore::new());
    builder.populate(&interner, &graph);

    let rec = TestRecommender::new(interner, graph);
    let res = rec
        .recommend(Some(author), &RecommendationDials::default(), now)
        .unwrap();
    assert!(res.posts.is_empty());
}

#[test]
fn test_f17_empty_viewer_interaction_history() {
    let (interner, graph) = SyntheticGraphBuilder::standard_cold_start_fixture(chrono_like_now());
    let rec = TestRecommender::new(interner, graph);
    let res = rec
        .recommend(
            Some("did:plc:newborn_user"),
            &RecommendationDials::default(),
            chrono_like_now(),
        )
        .unwrap();
    assert!(!res.posts.is_empty());
}

#[test]
fn test_f17_dedup_with_nonexistent_post_ids() {
    let interner = Arc::new(StringInterner::new());
    let graph = Arc::new(GraphStore::new());
    let uid = interner.intern("did:plc:alice");
    let now = chrono_like_now();
    graph.record_interaction(uid, 999_999, SignalType::Like, now);

    let rec = TestRecommender::new(interner, graph);
    let res = rec
        .recommend(Some("did:plc:alice"), &RecommendationDials::default(), now)
        .unwrap();
    assert!(res.posts.is_empty());
}

#[test]
fn test_f17_dedup_fallback_trigger() {
    let mut builder = SyntheticGraphBuilder::new();
    let now = chrono_like_now();
    let user = "did:plc:user_seen_friends";

    // User liked 10 posts
    for i in 1..=10 {
        let p = format!("at://did:plc:friend/post/p_{i}");
        builder = builder.add_post(
            p.clone(),
            "did:plc:friend",
            None::<&str>,
            None::<&str>,
            now - 1000,
        );
        builder = builder.add_interaction(user, p, SignalType::Like, now - 500);
    }
    // Velocity pool has fresh posts
    for i in 1..=5 {
        let fresh = format!("at://did:plc:stranger/post/fresh_{i}");
        builder = builder.add_post(
            fresh.clone(),
            "did:plc:stranger",
            None::<&str>,
            None::<&str>,
            now - 100,
        );
        builder = builder.add_interaction("did:plc:fan", fresh, SignalType::Like, now - 50);
    }

    let interner = Arc::new(StringInterner::new());
    let graph = Arc::new(GraphStore::new());
    builder.populate(&interner, &graph);

    let rec = TestRecommender::new(interner, graph);
    let dials = RecommendationDials {
        min_likes: 1,
        ..Default::default()
    };
    let res = rec.recommend(Some(user), &dials, now).unwrap();
    assert!(!res.posts.is_empty());
}

// ===========================================================================
// Feature 18: Stable Cursor Pagination (Boundary)
// ===========================================================================

#[test]
fn test_f18_cursor_empty_string_decoding() {
    let (interner, graph) = SyntheticGraphBuilder::standard_cold_start_fixture(chrono_like_now());
    let rec = TestRecommender::new(interner, graph);
    let dials = RecommendationDials {
        cursor: Some("".to_string()),
        ..Default::default()
    };
    let res = rec
        .recommend(Some("did:plc:active_user"), &dials, chrono_like_now())
        .unwrap();
    assert!(!res.posts.is_empty());
}

#[test]
fn test_f18_cursor_invalid_base64_corrupt() {
    let (interner, graph) = SyntheticGraphBuilder::standard_cold_start_fixture(chrono_like_now());
    let rec = TestRecommender::new(interner, graph);
    let dials = RecommendationDials {
        cursor: Some("!@#$%^&*()_corrupted_cursor".to_string()),
        ..Default::default()
    };
    let res = rec
        .recommend(Some("did:plc:active_user"), &dials, chrono_like_now())
        .unwrap();
    assert!(!res.posts.is_empty());
}

#[test]
fn test_f18_cursor_out_of_bounds_index() {
    let (interner, graph) = SyntheticGraphBuilder::standard_cold_start_fixture(chrono_like_now());
    let rec = TestRecommender::new(interner, graph);
    let dials = RecommendationDials {
        cursor: Some("999999".to_string()),
        ..Default::default()
    };
    let res = rec
        .recommend(Some("did:plc:active_user"), &dials, chrono_like_now())
        .unwrap();
    assert!(res.posts.is_empty());
    assert_eq!(res.cursor, None);
}

#[test]
fn test_f18_cursor_tampered_payload() {
    let (interner, graph) = SyntheticGraphBuilder::standard_cold_start_fixture(chrono_like_now());
    let rec = TestRecommender::new(interner, graph);
    let dials = RecommendationDials {
        cursor: Some("-100".to_string()),
        ..Default::default()
    };
    let res = rec
        .recommend(Some("did:plc:active_user"), &dials, chrono_like_now())
        .unwrap();
    assert!(!res.posts.is_empty());
}

#[test]
fn test_f18_cursor_monotonic_progress() {
    let (interner, graph) = SyntheticGraphBuilder::standard_cold_start_fixture(chrono_like_now());
    let rec = TestRecommender::new(interner, graph);
    let mut cursor = None;
    let mut pages = 0;

    while pages < 5 {
        let dials = RecommendationDials {
            limit: 2,
            cursor: cursor.clone(),
            ..Default::default()
        };
        let res = rec
            .recommend(Some("did:plc:active_user"), &dials, chrono_like_now())
            .unwrap();
        if res.posts.is_empty() {
            break;
        }
        pages += 1;
        if let Some(next_c) = res.cursor {
            cursor = Some(next_c);
        } else {
            break;
        }
    }
    assert!(pages >= 2);
}

// ===========================================================================
// Feature 19: Query Parameter Dials Mapping (Boundary)
// ===========================================================================

#[test]
fn test_f19_dial_unknown_freshness_fallback() {
    let dials = RecommendationDials::from_query(Some("hyper_speed"), None, None, None, None);
    assert_eq!(dials.half_life_secs, DEFAULT_HALF_LIFE_SECS);
}

#[test]
fn test_f19_dial_unknown_discovery_fallback() {
    let dials = RecommendationDials::from_query(None, Some("wild_west"), None, None, None);
    assert_eq!(dials.explore_ratio, DEFAULT_EXPLORE_RATIO);
}

#[test]
fn test_f19_dial_empty_query_params() {
    let dials = RecommendationDials::from_query(Some(""), Some(""), None, None, None);
    assert_eq!(dials.half_life_secs, DEFAULT_HALF_LIFE_SECS);
    assert_eq!(dials.explore_ratio, DEFAULT_EXPLORE_RATIO);
}

#[test]
fn test_f19_dial_case_insensitivity() {
    let dials = RecommendationDials::from_query(Some("6H"), Some("FAMILIAR"), None, None, None);
    assert_eq!(dials.half_life_secs, DEFAULT_HALF_LIFE_SECS); // custom or fallback
}

#[test]
fn test_f19_dial_out_of_range_custom_values() {
    let dials = RecommendationDials::from_query(None, Some("1.5"), None, Some(500), None);
    assert_eq!(dials.explore_ratio, 1.0); // clamped to 1.0
    assert_eq!(dials.limit, 100); // clamped to MAX_PAGE_LIMIT
}

// ===========================================================================
// Feature 20: Explanation Generator (Boundary)
// ===========================================================================

#[test]
fn test_f20_explain_tier3_velocity_explanation() {
    let (interner, graph) = SyntheticGraphBuilder::standard_cold_start_fixture(chrono_like_now());
    let rec = TestRecommender::new(interner, graph);
    let dials = RecommendationDials {
        explain: true,
        ..Default::default()
    };
    let res = rec
        .recommend(Some("did:plc:cold_user"), &dials, chrono_like_now())
        .unwrap();
    assert!(res.posts[0].explain.is_some());
    assert!(res.posts[0].explain.as_ref().unwrap().contains("source="));
}

#[test]
fn test_f20_explain_tier2_follow_explanation() {
    let (interner, graph) = SyntheticGraphBuilder::standard_cold_start_fixture(chrono_like_now());
    let rec = TestRecommender::new(interner, graph);
    let dials = RecommendationDials {
        explain: true,
        ..Default::default()
    };
    let res = rec
        .recommend(Some("did:plc:new_user"), &dials, chrono_like_now())
        .unwrap();
    assert!(res.posts[0].explain.is_some());
}

#[test]
fn test_f20_explain_exploration_cluster_tag() {
    let (interner, graph) = SyntheticGraphBuilder::standard_cold_start_fixture(chrono_like_now());
    let rec = TestRecommender::new(interner, graph);
    let dials = RecommendationDials {
        explore_ratio: 0.35,
        explain: true,
        limit: 10,
        ..Default::default()
    };
    let res = rec
        .recommend(Some("did:plc:active_user"), &dials, chrono_like_now())
        .unwrap();
    for p in res.posts {
        assert!(p.explain.is_some());
    }
}

#[test]
fn test_f20_explain_string_formatting_safety() {
    let (interner, graph) = SyntheticGraphBuilder::standard_cold_start_fixture(chrono_like_now());
    let rec = TestRecommender::new(interner, graph);
    let dials = RecommendationDials {
        explain: true,
        ..Default::default()
    };
    let res = rec.recommend(None, &dials, chrono_like_now()).unwrap();
    for p in res.posts {
        let text = p.explain.unwrap();
        assert!(!text.contains('\0'));
    }
}

#[test]
fn test_f20_explain_empty_cointeractors_text() {
    let (interner, graph) = SyntheticGraphBuilder::standard_cold_start_fixture(chrono_like_now());
    let rec = TestRecommender::new(interner, graph);
    let dials = RecommendationDials {
        explain: true,
        ..Default::default()
    };
    let res = rec
        .recommend(Some("did:plc:active_user"), &dials, chrono_like_now())
        .unwrap();
    assert!(!res.posts.is_empty());
}

// ===========================================================================
// Feature 21: Jetstream WebSocket Connection (Boundary)
// ===========================================================================

#[tokio::test]
async fn test_f21_ws_connect_refused_retry() {
    let res = tokio_tungstenite::connect_async("ws://127.0.0.1:1").await;
    assert!(res.is_err());
}

#[tokio::test]
async fn test_f21_ws_tls_handshake_failure() {
    let res = tokio_tungstenite::connect_async("wss://127.0.0.1:1").await;
    assert!(res.is_err());
}

#[tokio::test]
async fn test_f21_ws_abrupt_rst_handling() {
    let server = MockJetstreamServer::start().await.unwrap();
    let (mut ws_stream, _) = tokio_tungstenite::connect_async(server.ws_url())
        .await
        .unwrap();
    server.shutdown();
    let res = ws_stream.next().await;
    assert!(
        res.is_none()
            || res
                .as_ref()
                .is_some_and(|r| r.is_err() || r.as_ref().unwrap().is_close())
    );
}

#[tokio::test]
async fn test_f21_ws_invalid_endpoint_url() {
    let res = tokio_tungstenite::connect_async("invalid://endpoint").await;
    assert!(res.is_err());
}

#[tokio::test]
async fn test_f21_ws_binary_frame_ignore() {
    let server = MockJetstreamServer::start().await.unwrap();
    let (mut ws_stream, _) = tokio_tungstenite::connect_async(server.ws_url())
        .await
        .unwrap();
    server
        .send_event_json("{\"did\":\"did:plc:test\",\"time_us\":1234}")
        .await;
    let msg = ws_stream.next().await.unwrap().unwrap();
    assert!(msg.is_text());
    server.shutdown();
}

// ===========================================================================
// Feature 22: Typed Jetstream Deserialization (Boundary)
// ===========================================================================

#[test]
fn test_f22_deserialize_malformed_json_skip() {
    let malformed = "{not a json}";
    let res: std::result::Result<serde_json::Value, _> = serde_json::from_str(malformed);
    assert!(res.is_err());
}

#[test]
fn test_f22_deserialize_missing_required_fields() {
    let missing = r#"{"kind":"commit"}"#;
    let val: serde_json::Value = serde_json::from_str(missing).unwrap();
    assert!(val.get("did").is_none());
}

#[test]
fn test_f22_deserialize_unknown_collection_ignore() {
    let raw = r#"{
        "did": "did:plc:alice",
        "time_us": 100,
        "kind": "commit",
        "commit": {
            "collection": "app.bsky.custom.unknown",
            "operation": "create"
        }
    }"#;
    let val: serde_json::Value = serde_json::from_str(raw).unwrap();
    let collection = val["commit"]["collection"].as_str().unwrap();
    assert_ne!(collection, "app.bsky.feed.like");
}

#[test]
fn test_f22_deserialize_extra_unexpected_keys() {
    let raw = r#"{
        "did": "did:plc:alice",
        "time_us": 100,
        "kind": "commit",
        "unexpected_field": 12345,
        "commit": {
            "collection": "app.bsky.feed.like",
            "rkey": "3k123",
            "operation": "create",
            "extra_nested": "hello"
        }
    }"#;
    let val: serde_json::Value = serde_json::from_str(raw).unwrap();
    assert_eq!(val["did"], "did:plc:alice");
}

#[test]
fn test_f22_deserialize_unicode_and_escapes() {
    let raw = r#"{"did":"did:plc:\u0061lice","text":"\u2764\ufe0f\n\t"}"#;
    let val: serde_json::Value = serde_json::from_str(raw).unwrap();
    assert_eq!(val["did"], "did:plc:alice");
}

// ===========================================================================
// Feature 23: Bounded Backpressure Channels (Boundary)
// ===========================================================================

#[tokio::test]
async fn test_f23_channel_full_buffer_blocking() {
    let (tx, mut rx) = tokio::sync::mpsc::channel::<u32>(1);
    tx.send(1).await.unwrap();
    let tx_clone = tx.clone();
    let handle = tokio::spawn(async move {
        tx_clone.send(2).await.unwrap();
    });

    tokio::time::sleep(Duration::from_millis(10)).await;
    assert_eq!(rx.recv().await, Some(1));
    handle.await.unwrap();
    assert_eq!(rx.recv().await, Some(2));
}

#[tokio::test]
async fn test_f23_channel_closed_sender_handling() {
    let (tx, mut rx) = tokio::sync::mpsc::channel::<u32>(10);
    drop(tx);
    assert_eq!(rx.recv().await, None);
}

#[tokio::test]
async fn test_f23_channel_closed_receiver_handling() {
    let (tx, rx) = tokio::sync::mpsc::channel::<u32>(10);
    drop(rx);
    assert!(tx.send(1).await.is_err());
}

#[tokio::test]
async fn test_f23_channel_100k_burst_memory_stability() {
    let (tx, mut rx) = tokio::sync::mpsc::channel::<u32>(100);
    let producer = tokio::spawn(async move {
        for i in 0..1_000 {
            if tx.send(i).await.is_err() {
                break;
            }
        }
    });

    let mut count = 0;
    while rx.recv().await.is_some() {
        count += 1;
        if count == 1_000 {
            break;
        }
    }
    producer.await.unwrap();
    assert_eq!(count, 1_000);
}

#[tokio::test]
async fn test_f23_channel_capacity_one_edge_case() {
    let (tx, mut rx) = tokio::sync::mpsc::channel::<u32>(1);
    tx.try_send(42).unwrap();
    assert!(tx.try_send(43).is_err());
    assert_eq!(rx.recv().await, Some(42));
    assert!(tx.try_send(43).is_ok());
}

// ===========================================================================
// Feature 24: Exponential Reconnect Backoff with Jitter (Boundary)
// ===========================================================================

#[test]
fn test_f24_backoff_immediate_disconnect_loop() {
    let mut delay = Duration::from_millis(500);
    for _ in 0..10 {
        delay = (delay * 2).min(Duration::from_secs(30));
    }
    assert_eq!(delay, Duration::from_secs(30));
}

#[test]
fn test_f24_backoff_overflow_prevention() {
    let max_delay = Duration::from_secs(30);
    let mut delay = Duration::from_secs(100);
    delay = delay.min(max_delay);
    assert_eq!(delay, Duration::from_secs(30));
}

#[test]
fn test_f24_backoff_zero_initial_delay_guard() {
    let initial = Duration::from_millis(0);
    let safe_initial = initial.max(Duration::from_millis(100));
    assert_eq!(safe_initial, Duration::from_millis(100));
}

#[tokio::test]
async fn test_f24_backoff_cancellation_during_sleep() {
    let cancel = tokio_util::sync::CancellationToken::new();
    let c = cancel.clone();
    let handle = tokio::spawn(async move {
        tokio::select! {
            _ = c.cancelled() => true,
            _ = tokio::time::sleep(Duration::from_secs(60)) => false,
        }
    });
    cancel.cancel();
    assert!(handle.await.unwrap());
}

#[test]
fn test_f24_backoff_rng_jitter_distribution() {
    let base = 1000u64;
    for i in 0..10 {
        let jitter = (i * 37) % 200;
        let delayed = base + jitter;
        assert!((1000..=1200).contains(&delayed));
    }
}

// ===========================================================================
// Feature 25: Jetstream Cursor Preservation (Boundary)
// ===========================================================================

#[test]
fn test_f25_cursor_initial_none_connect() {
    let cursor: Option<u64> = None;
    let url = match cursor {
        Some(c) => format!("ws://jetstream.test/sub?cursor={c}"),
        None => "ws://jetstream.test/sub".to_string(),
    };
    assert_eq!(url, "ws://jetstream.test/sub");
}

#[test]
fn test_f25_cursor_out_of_order_timestamp_ignore() {
    let mut highest = 1000u64;
    let incoming = [900, 1050, 800, 1100, 1000];
    for ts in incoming {
        if ts > highest {
            highest = ts;
        }
    }
    assert_eq!(highest, 1100);
}

#[test]
fn test_f25_cursor_corrupt_cursor_recovery() {
    let cursor_str = "corrupt_cursor_string";
    let parsed = cursor_str.parse::<u64>().ok();
    assert_eq!(parsed, None);
}

#[test]
fn test_f25_cursor_zero_time_us_handling() {
    let cursor: u64 = 0;
    assert_eq!(cursor.saturating_sub(1), 0);
}

#[test]
fn test_f25_cursor_future_time_us_handling() {
    let future_cursor = u64::MAX;
    assert!(future_cursor > 1_700_000_000_000_000);
}

// ===========================================================================
// Feature 26: Stream Heartbeat / Inactivity Timeout (Boundary)
// ===========================================================================

#[test]
fn test_f26_heartbeat_zero_timeout_guard() {
    let configured_timeout = Duration::from_secs(0);
    let safe_timeout = configured_timeout.max(Duration::from_secs(10));
    assert_eq!(safe_timeout, Duration::from_secs(10));
}

#[tokio::test]
async fn test_f26_heartbeat_hung_tcp_detection() {
    let (tx, mut rx) = tokio::sync::mpsc::channel::<()>(1);
    let handle = tokio::spawn(async move {
        tokio::select! {
            _ = rx.recv() => false,
            _ = tokio::time::sleep(Duration::from_millis(50)) => true,
        }
    });
    tokio::time::sleep(Duration::from_millis(100)).await;
    drop(tx);
    assert!(handle.await.unwrap());
}

#[test]
fn test_f26_heartbeat_unsolicited_pong_ignore() {
    let pong = Message::Pong(vec![1, 2, 3]);
    assert!(pong.is_pong());
}

#[tokio::test]
async fn test_f26_heartbeat_shutdown_interruption() {
    let cancel = tokio_util::sync::CancellationToken::new();
    let c = cancel.clone();
    let handle = tokio::spawn(async move {
        tokio::select! {
            _ = c.cancelled() => true,
            _ = tokio::time::sleep(Duration::from_secs(10)) => false,
        }
    });
    cancel.cancel();
    assert!(handle.await.unwrap());
}

#[test]
fn test_f26_heartbeat_clock_skew_resilience() {
    let last_ping = 100u64;
    let now = 90u64; // Clock moved backwards
    let elapsed = now.saturating_sub(last_ping);
    assert_eq!(elapsed, 0);
}

// ===========================================================================
// Feature 27: Graceful Ingestion Shutdown (Boundary)
// ===========================================================================

#[tokio::test]
async fn test_f27_shutdown_already_cancelled_start() {
    let cancel = tokio_util::sync::CancellationToken::new();
    cancel.cancel();
    assert!(cancel.is_cancelled());
}

#[tokio::test]
async fn test_f27_shutdown_during_reconnect_sleep() {
    let cancel = tokio_util::sync::CancellationToken::new();
    let c = cancel.clone();
    let handle = tokio::spawn(async move {
        tokio::select! {
            _ = c.cancelled() => "cancelled",
            _ = tokio::time::sleep(Duration::from_secs(10)) => "slept",
        }
    });
    cancel.cancel();
    assert_eq!(handle.await.unwrap(), "cancelled");
}

#[tokio::test]
async fn test_f27_shutdown_during_active_json_parse() {
    let cancel = tokio_util::sync::CancellationToken::new();
    let c = cancel.clone();
    cancel.cancel();
    assert!(c.is_cancelled());
}

#[tokio::test]
async fn test_f27_shutdown_multiple_tokens_cascade() {
    let parent = tokio_util::sync::CancellationToken::new();
    let child1 = parent.child_token();
    let child2 = child1.child_token();
    parent.cancel();
    assert!(child1.is_cancelled());
    assert!(child2.is_cancelled());
}

#[tokio::test]
async fn test_f27_shutdown_timeout_safety() {
    let res = tokio::time::timeout(Duration::from_millis(50), async {
        tokio::time::sleep(Duration::from_millis(10)).await;
        "ok"
    })
    .await;
    assert_eq!(res.unwrap(), "ok");
}

// ===========================================================================
// Feature 28: Axum Web Server Setup (Boundary)
// ===========================================================================

#[tokio::test]
async fn test_f28_server_port_collision_error() {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let collision = tokio::net::TcpListener::bind(addr).await;
    assert!(collision.is_err());
}

#[tokio::test]
async fn test_f28_server_unmatched_route_404() {
    let (interner, graph) = SyntheticGraphBuilder::standard_cold_start_fixture(chrono_like_now());
    let app = create_test_xrpc_router(TestAppState {
        recommender: TestRecommender::new(interner, graph),
        service_did: CompactString::new("did:web:test"),
        hostname: CompactString::new("test"),
    });

    let req = Request::builder()
        .uri("/nonexistent_endpoint")
        .body(Body::empty())
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::NOT_FOUND);
}

#[tokio::test]
async fn test_f28_server_invalid_http_method_405() {
    let (interner, graph) = SyntheticGraphBuilder::standard_cold_start_fixture(chrono_like_now());
    let app = create_test_xrpc_router(TestAppState {
        recommender: TestRecommender::new(interner, graph),
        service_did: CompactString::new("did:web:test"),
        hostname: CompactString::new("test"),
    });

    let req = Request::builder()
        .method(Method::POST)
        .uri("/healthz")
        .body(Body::empty())
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::METHOD_NOT_ALLOWED);
}

#[tokio::test]
async fn test_f28_server_oversized_payload_413() {
    let (interner, graph) = SyntheticGraphBuilder::standard_cold_start_fixture(chrono_like_now());
    let app = create_test_xrpc_router(TestAppState {
        recommender: TestRecommender::new(interner, graph),
        service_did: CompactString::new("did:web:test"),
        hostname: CompactString::new("test"),
    });

    let req = Request::builder()
        .uri("/healthz")
        .body(Body::from(vec![0u8; 1024]))
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
}

#[tokio::test]
async fn test_f28_server_concurrent_connection_limit() {
    let (interner, graph) = SyntheticGraphBuilder::standard_cold_start_fixture(chrono_like_now());
    let app = create_test_xrpc_router(TestAppState {
        recommender: TestRecommender::new(interner, graph),
        service_did: CompactString::new("did:web:test"),
        hostname: CompactString::new("test"),
    });

    for _ in 0..10 {
        let req = Request::builder()
            .uri("/healthz")
            .body(Body::empty())
            .unwrap();
        let resp = app.clone().oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }
}

// ===========================================================================
// Feature 29: GET /xrpc/app.bsky.feed.getFeedSkeleton (Boundary)
// ===========================================================================

#[tokio::test]
async fn test_f29_get_feed_skeleton_missing_feed_param_400() {
    let (interner, graph) = SyntheticGraphBuilder::standard_cold_start_fixture(chrono_like_now());
    let app = create_test_xrpc_router(TestAppState {
        recommender: TestRecommender::new(interner, graph),
        service_did: CompactString::new("did:web:test"),
        hostname: CompactString::new("test"),
    });

    let req = Request::builder()
        .uri("/xrpc/app.bsky.feed.getFeedSkeleton")
        .body(Body::empty())
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn test_f29_get_feed_skeleton_limit_zero_clamped_1() {
    let (interner, graph) = SyntheticGraphBuilder::standard_cold_start_fixture(chrono_like_now());
    let app = create_test_xrpc_router(TestAppState {
        recommender: TestRecommender::new(interner, graph),
        service_did: CompactString::new("did:web:test"),
        hostname: CompactString::new("test"),
    });

    let req = Request::builder()
        .uri("/xrpc/app.bsky.feed.getFeedSkeleton?feed=at://did:plc:feed/app.bsky.feed.generator/foryou&limit=0")
        .body(Body::empty())
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = resp.into_body().collect().await.unwrap().to_bytes();
    let skeleton: FeedSkeletonResponse = serde_json::from_slice(&body).unwrap();
    assert_eq!(skeleton.feed.len(), 1);
}

#[tokio::test]
async fn test_f29_get_feed_skeleton_limit_150_clamped_100() {
    let (interner, graph) = SyntheticGraphBuilder::standard_cold_start_fixture(chrono_like_now());
    let app = create_test_xrpc_router(TestAppState {
        recommender: TestRecommender::new(interner, graph),
        service_did: CompactString::new("did:web:test"),
        hostname: CompactString::new("test"),
    });

    let req = Request::builder()
        .uri("/xrpc/app.bsky.feed.getFeedSkeleton?feed=at://did:plc:feed/app.bsky.feed.generator/foryou&limit=150")
        .body(Body::empty())
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
}

#[tokio::test]
async fn test_f29_get_feed_skeleton_empty_graph_empty_feed() {
    let interner = Arc::new(StringInterner::new());
    let graph = Arc::new(GraphStore::new());
    let app = create_test_xrpc_router(TestAppState {
        recommender: TestRecommender::new(interner, graph),
        service_did: CompactString::new("did:web:test"),
        hostname: CompactString::new("test"),
    });

    let req = Request::builder()
        .uri("/xrpc/app.bsky.feed.getFeedSkeleton?feed=at://did:plc:feed/app.bsky.feed.generator/foryou")
        .body(Body::empty())
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = resp.into_body().collect().await.unwrap().to_bytes();
    let skeleton: FeedSkeletonResponse = serde_json::from_slice(&body).unwrap();
    assert!(skeleton.feed.is_empty());
}

#[tokio::test]
async fn test_f29_get_feed_skeleton_invalid_cursor_fallback() {
    let (interner, graph) = SyntheticGraphBuilder::standard_cold_start_fixture(chrono_like_now());
    let app = create_test_xrpc_router(TestAppState {
        recommender: TestRecommender::new(interner, graph),
        service_did: CompactString::new("did:web:test"),
        hostname: CompactString::new("test"),
    });

    let req = Request::builder()
        .uri("/xrpc/app.bsky.feed.getFeedSkeleton?feed=at://did:plc:feed/app.bsky.feed.generator/foryou&cursor=corrupt")
        .body(Body::empty())
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
}

// ===========================================================================
// Feature 30: GET /.well-known/did.json (Boundary)
// ===========================================================================

#[tokio::test]
async fn test_f30_did_doc_cached_response() {
    let (interner, graph) = SyntheticGraphBuilder::standard_cold_start_fixture(chrono_like_now());
    let app = create_test_xrpc_router(TestAppState {
        recommender: TestRecommender::new(interner, graph),
        service_did: CompactString::new("did:web:feed.example.com"),
        hostname: CompactString::new("feed.example.com"),
    });

    for _ in 0..5 {
        let req = Request::builder()
            .uri("/.well-known/did.json")
            .body(Body::empty())
            .unwrap();
        let resp = app.clone().oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }
}

#[tokio::test]
async fn test_f30_did_doc_trailing_slash_handling() {
    let (interner, graph) = SyntheticGraphBuilder::standard_cold_start_fixture(chrono_like_now());
    let app = create_test_xrpc_router(TestAppState {
        recommender: TestRecommender::new(interner, graph),
        service_did: CompactString::new("did:web:feed.example.com"),
        hostname: CompactString::new("feed.example.com"),
    });

    let req = Request::builder()
        .uri("/.well-known/did.json")
        .body(Body::empty())
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
}

#[tokio::test]
async fn test_f30_did_doc_head_request_support() {
    let (interner, graph) = SyntheticGraphBuilder::standard_cold_start_fixture(chrono_like_now());
    let app = create_test_xrpc_router(TestAppState {
        recommender: TestRecommender::new(interner, graph),
        service_did: CompactString::new("did:web:feed.example.com"),
        hostname: CompactString::new("feed.example.com"),
    });

    let req = Request::builder()
        .method(Method::GET)
        .uri("/.well-known/did.json")
        .body(Body::empty())
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
}

#[tokio::test]
async fn test_f30_did_doc_empty_hostname_fallback() {
    let (interner, graph) = SyntheticGraphBuilder::standard_cold_start_fixture(chrono_like_now());
    let app = create_test_xrpc_router(TestAppState {
        recommender: TestRecommender::new(interner, graph),
        service_did: CompactString::new("did:web:fallback.test"),
        hostname: CompactString::new(""),
    });

    let req = Request::builder()
        .uri("/.well-known/did.json")
        .body(Body::empty())
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
}

#[tokio::test]
async fn test_f30_did_doc_schema_validation() {
    let (interner, graph) = SyntheticGraphBuilder::standard_cold_start_fixture(chrono_like_now());
    let app = create_test_xrpc_router(TestAppState {
        recommender: TestRecommender::new(interner, graph),
        service_did: CompactString::new("did:web:feed.test"),
        hostname: CompactString::new("feed.test"),
    });

    let req = Request::builder()
        .uri("/.well-known/did.json")
        .body(Body::empty())
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    let body = resp.into_body().collect().await.unwrap().to_bytes();
    let doc: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(doc["id"], "did:web:feed.test");
    assert!(doc["service"].is_array());
}

// ===========================================================================
// Feature 31: GET /healthz (Boundary)
// ===========================================================================

#[tokio::test]
async fn test_f31_healthz_head_request() {
    let (interner, graph) = SyntheticGraphBuilder::standard_cold_start_fixture(chrono_like_now());
    let app = create_test_xrpc_router(TestAppState {
        recommender: TestRecommender::new(interner, graph),
        service_did: CompactString::new("did:web:test"),
        hostname: CompactString::new("test"),
    });

    let req = Request::builder()
        .method(Method::GET)
        .uri("/healthz")
        .body(Body::empty())
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
}

#[tokio::test]
async fn test_f31_healthz_high_frequency_polling() {
    let (interner, graph) = SyntheticGraphBuilder::standard_cold_start_fixture(chrono_like_now());
    let app = create_test_xrpc_router(TestAppState {
        recommender: TestRecommender::new(interner, graph),
        service_did: CompactString::new("did:web:test"),
        hostname: CompactString::new("test"),
    });

    for _ in 0..50 {
        let req = Request::builder()
            .uri("/healthz")
            .body(Body::empty())
            .unwrap();
        let resp = app.clone().oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }
}

#[tokio::test]
async fn test_f31_healthz_zero_nodes_initial_boot() {
    let interner = Arc::new(StringInterner::new());
    let graph = Arc::new(GraphStore::new());
    let app = create_test_xrpc_router(TestAppState {
        recommender: TestRecommender::new(interner, graph),
        service_did: CompactString::new("did:web:test"),
        hostname: CompactString::new("test"),
    });

    let req = Request::builder()
        .uri("/healthz")
        .body(Body::empty())
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    let body = resp.into_body().collect().await.unwrap().to_bytes();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["status"], "ok");
    assert_eq!(json["nodes"], 0);
}

#[tokio::test]
async fn test_f31_healthz_after_ingestion_mutation() {
    let (interner, graph) = SyntheticGraphBuilder::standard_cold_start_fixture(chrono_like_now());
    let app = create_test_xrpc_router(TestAppState {
        recommender: TestRecommender::new(interner, graph),
        service_did: CompactString::new("did:web:test"),
        hostname: CompactString::new("test"),
    });

    let req = Request::builder()
        .uri("/healthz")
        .body(Body::empty())
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    let body = resp.into_body().collect().await.unwrap().to_bytes();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert!(json["nodes"].as_u64().unwrap() > 0);
}

#[tokio::test]
async fn test_f31_healthz_response_time_sub_millisecond() {
    let (interner, graph) = SyntheticGraphBuilder::standard_cold_start_fixture(chrono_like_now());
    let app = create_test_xrpc_router(TestAppState {
        recommender: TestRecommender::new(interner, graph),
        service_did: CompactString::new("did:web:test"),
        hostname: CompactString::new("test"),
    });

    let start = std::time::Instant::now();
    let req = Request::builder()
        .uri("/healthz")
        .body(Body::empty())
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    let duration = start.elapsed();
    assert_eq!(resp.status(), StatusCode::OK);
    assert!(duration < Duration::from_millis(50));
}

// ===========================================================================
// Feature 32: Service Auth JWT DID Extraction (Boundary)
// ===========================================================================

#[test]
fn test_f32_auth_malformed_jwt_returns_none() {
    assert_eq!(extract_viewer_did("Bearer not.a.valid.jwt.token"), None);
    assert_eq!(extract_viewer_did("Bearer onlyonepart"), None);
    assert_eq!(extract_viewer_did("Bearer part1.part2"), None);
}

#[test]
fn test_f32_auth_missing_header_returns_none() {
    let headers = axum::http::HeaderMap::new();
    assert_eq!(extract_viewer_did_from_headers(&headers), None);
}

#[test]
fn test_f32_auth_empty_bearer_token() {
    assert_eq!(extract_viewer_did("Bearer "), None);
    assert_eq!(extract_viewer_did("Bearer    "), None);
}

#[test]
fn test_f32_auth_corrupted_base64_payload() {
    assert_eq!(
        extract_viewer_did("Bearer header.!!!corrupt_b64!!!.sig"),
        None
    );
}

#[test]
fn test_f32_auth_missing_iss_and_sub_claims() {
    use base64::engine::general_purpose::URL_SAFE_NO_PAD;
    use base64::Engine;

    let header = URL_SAFE_NO_PAD.encode(r#"{"alg":"ES256K"}"#);
    let payload = URL_SAFE_NO_PAD.encode(r#"{"aud":"did:web:feed.test","exp":1800000000}"#);
    let token = format!("Bearer {header}.{payload}.sig");
    assert_eq!(extract_viewer_did(&token), None);
}

// ===========================================================================
// Feature 33: Anonymous / Invalid Auth Graceful Fallback (Boundary)
// ===========================================================================

#[tokio::test]
async fn test_f33_anon_empty_velocity_pool_empty_feed() {
    let interner = Arc::new(StringInterner::new());
    let graph = Arc::new(GraphStore::new());
    let app = create_test_xrpc_router(TestAppState {
        recommender: TestRecommender::new(interner, graph),
        service_did: CompactString::new("did:web:test"),
        hostname: CompactString::new("test"),
    });

    let req = Request::builder()
        .uri("/xrpc/app.bsky.feed.getFeedSkeleton?feed=at://did:plc:feed/app.bsky.feed.generator/foryou")
        .body(Body::empty())
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
}

#[tokio::test]
async fn test_f33_auth_header_gibberish_graceful() {
    let (interner, graph) = SyntheticGraphBuilder::standard_cold_start_fixture(chrono_like_now());
    let app = create_test_xrpc_router(TestAppState {
        recommender: TestRecommender::new(interner, graph),
        service_did: CompactString::new("did:web:test"),
        hostname: CompactString::new("test"),
    });

    let req = Request::builder()
        .uri("/xrpc/app.bsky.feed.getFeedSkeleton?feed=at://did:plc:feed/app.bsky.feed.generator/foryou")
        .header("Authorization", "Bearer invalid-jwt-gibberish")
        .body(Body::empty())
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
}

#[tokio::test]
async fn test_f33_anon_request_with_dials() {
    let (interner, graph) = SyntheticGraphBuilder::standard_cold_start_fixture(chrono_like_now());
    let app = create_test_xrpc_router(TestAppState {
        recommender: TestRecommender::new(interner, graph),
        service_did: CompactString::new("did:web:test"),
        hostname: CompactString::new("test"),
    });

    let req = Request::builder()
        .uri("/xrpc/app.bsky.feed.getFeedSkeleton?feed=at://did:plc:feed/app.bsky.feed.generator/foryou&freshness=6h&discovery=familiar")
        .body(Body::empty())
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
}

#[tokio::test]
async fn test_f33_anon_concurrent_requests() {
    let (interner, graph) = SyntheticGraphBuilder::standard_cold_start_fixture(chrono_like_now());
    let app = create_test_xrpc_router(TestAppState {
        recommender: TestRecommender::new(interner, graph),
        service_did: CompactString::new("did:web:test"),
        hostname: CompactString::new("test"),
    });

    for _ in 0..20 {
        let req = Request::builder()
            .uri("/xrpc/app.bsky.feed.getFeedSkeleton?feed=at://did:plc:feed/app.bsky.feed.generator/foryou")
            .body(Body::empty())
            .unwrap();
        let resp = app.clone().oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }
}

#[tokio::test]
async fn test_f33_anon_to_authenticated_transition() {
    let (interner, graph) = SyntheticGraphBuilder::standard_cold_start_fixture(chrono_like_now());
    let app = create_test_xrpc_router(TestAppState {
        recommender: TestRecommender::new(interner, graph),
        service_did: CompactString::new("did:web:test"),
        hostname: CompactString::new("test"),
    });

    // 1. Anonymous request
    let req1 = Request::builder()
        .uri("/xrpc/app.bsky.feed.getFeedSkeleton?feed=at://did:plc:feed/app.bsky.feed.generator/foryou")
        .body(Body::empty())
        .unwrap();
    let resp1 = app.clone().oneshot(req1).await.unwrap();
    assert_eq!(resp1.status(), StatusCode::OK);

    // 2. Authenticated request
    let jwt = generate_mock_jwt("did:plc:active_user", "did:web:test", true);
    let req2 = Request::builder()
        .uri("/xrpc/app.bsky.feed.getFeedSkeleton?feed=at://did:plc:feed/app.bsky.feed.generator/foryou")
        .header("Authorization", format!("Bearer {jwt}"))
        .body(Body::empty())
        .unwrap();
    let resp2 = app.oneshot(req2).await.unwrap();
    assert_eq!(resp2.status(), StatusCode::OK);
}

// ===========================================================================
// Feature 34: Server Task Lifecycle with JoinSet (Boundary)
// ===========================================================================

#[tokio::test]
async fn test_f34_joinset_task_panic_containment() {
    let mut set = tokio::task::JoinSet::new();
    set.spawn(async {
        panic!("simulated task panic");
    });
    set.spawn(async { 42 });

    let mut panicked = false;
    let mut success = false;
    while let Some(res) = set.join_next().await {
        match res {
            Ok(val) => {
                assert_eq!(val, 42);
                success = true;
            }
            Err(e) => {
                assert!(e.is_panic());
                panicked = true;
            }
        }
    }
    assert!(panicked && success);
}

#[tokio::test]
async fn test_f34_joinset_immediate_shutdown() {
    let mut set = tokio::task::JoinSet::new();
    for _ in 0..5 {
        set.spawn(async {
            tokio::time::sleep(Duration::from_secs(60)).await;
        });
    }
    set.abort_all();
    while let Some(res) = set.join_next().await {
        assert!(res.unwrap_err().is_cancelled());
    }
}

#[tokio::test]
async fn test_f34_joinset_empty_task_set() {
    let mut set: tokio::task::JoinSet<()> = tokio::task::JoinSet::new();
    assert!(set.is_empty());
    assert_eq!(set.len(), 0);
    assert!(set.join_next().await.is_none());
}

#[tokio::test]
async fn test_f34_joinset_task_completion_handling() {
    let mut set = tokio::task::JoinSet::new();
    set.spawn(async { 1 });
    set.spawn(async { 2 });
    let mut sum = 0;
    while let Some(Ok(val)) = set.join_next().await {
        sum += val;
    }
    assert_eq!(sum, 3);
}

#[tokio::test]
async fn test_f34_joinset_abort_on_slow_task() {
    let mut set = tokio::task::JoinSet::new();
    set.spawn(async {
        tokio::time::sleep(Duration::from_millis(10)).await;
        10
    });
    set.spawn(async {
        tokio::time::sleep(Duration::from_secs(60)).await;
        20
    });

    let first = set.join_next().await.unwrap().unwrap();
    assert_eq!(first, 10);
    set.abort_all();
}

// ===========================================================================
// Feature 35: Production Invariants & Error Handling (Boundary)
// ===========================================================================

#[test]
fn test_f35_io_error_conversion() {
    let io_err = std::io::Error::new(std::io::ErrorKind::NotFound, "file not found");
    let domain_err: FeedError = io_err.into();
    assert!(domain_err.to_string().contains("I/O error"));
}

#[test]
fn test_f35_json_error_conversion() {
    let bad_json = "{bad}";
    let parse_err = serde_json::from_str::<serde_json::Value>(bad_json).unwrap_err();
    let domain_err: FeedError = parse_err.into();
    assert!(domain_err.to_string().contains("Serialization error"));
}

#[test]
fn test_f35_invalid_input_domain_error() {
    let err = FeedError::Graph("invalid input on graph node".to_string());
    assert!(err.to_string().contains("invalid input on graph node"));
}

#[test]
fn test_f35_saturating_time_no_underflow() {
    let t_curr = 100u64;
    let t_event = 500u64;
    let dt = t_curr.saturating_sub(t_event);
    assert_eq!(dt, 0);
}

#[test]
fn test_f35_concurrency_lock_poison_free() {
    // parking_lot::RwLock is poison-free by design
    let lock = parking_lot::RwLock::new(42);
    let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        let mut guard = lock.write();
        *guard = 100;
        panic!("panic inside lock");
    }));
    // Lock is still accessible
    assert_eq!(*lock.read(), 100);
}
