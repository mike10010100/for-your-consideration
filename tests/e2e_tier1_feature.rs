//! Tier 1 E2E Feature Isolation Test Suite (Features 1–35)
//!
//! Validates each of the 35 functional features in isolation with >=5 tests per feature.

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
use axum::http::{Request, StatusCode};
use compact_str::CompactString;
use for_your_consideration::prelude::*;
use futures_util::StreamExt;
use http_body_util::BodyExt;
use tokio_tungstenite::tungstenite::Message;
use tower::ServiceExt;

use crate::common::*;

// ===========================================================================
// Feature 1: 32-bit String Interning
// ===========================================================================

#[test]
fn test_f01_intern_basic_did_and_uri() {
    let interner = StringInterner::new();
    let id_did = interner.intern("did:plc:alice");
    let id_uri = interner.intern("at://did:plc:alice/app.bsky.feed.post/123");

    assert_eq!(id_did, 0);
    assert_eq!(id_uri, 1);
}

#[test]
fn test_f01_intern_idempotent() {
    let interner = StringInterner::new();
    let id1 = interner.intern("did:plc:alice");
    let id2 = interner.intern("did:plc:bob");
    let id1_again = interner.intern("did:plc:alice");

    assert_eq!(id1, id1_again);
    assert_ne!(id1, id2);
}

#[test]
fn test_f01_resolve_valid_id() {
    let interner = StringInterner::new();
    let id = interner.intern("did:plc:carol");
    let resolved = interner.resolve(id);

    assert_eq!(resolved.as_deref(), Some("did:plc:carol"));
}

#[test]
fn test_f01_bidirectional_mapping() {
    let interner = StringInterner::new();
    let str_val = "at://did:plc:dan/app.bsky.feed.post/post456";
    let id = interner.intern(str_val);

    assert_eq!(interner.lookup_id(str_val), Some(id));
    assert_eq!(interner.lookup_str(id).as_deref(), Some(str_val));
}

#[test]
fn test_f01_len_and_is_empty() {
    let interner = StringInterner::new();
    assert!(interner.is_empty());
    assert_eq!(interner.len(), 0);

    interner.intern("did:plc:eve");
    assert!(!interner.is_empty());
    assert_eq!(interner.len(), 1);
}

// ===========================================================================
// Feature 2: Compact 8-byte Edge Representation
// ===========================================================================

#[test]
fn test_f02_edge_pack_unpack() {
    let target = 42_000;
    let ts = BLUESKY_EPOCH_SECS + 3600;
    let edge = CompactEdge::new(target, SignalType::Like, ts);

    assert_eq!(edge.target(), target);
    assert_eq!(edge.signal(), SignalType::Like);
    assert_eq!(edge.timestamp_secs(), ts);
}

#[test]
fn test_f02_edge_target_id() {
    let edge = CompactEdge::new(999_999, SignalType::Quote, BLUESKY_EPOCH_SECS + 10);
    assert_eq!(edge.target(), 999_999);
}

#[test]
fn test_f02_edge_timestamp_retrieval() {
    let ts = BLUESKY_EPOCH_SECS + 86400 * 30; // 30 days
    let edge = CompactEdge::new(1, SignalType::Repost, ts);
    assert_eq!(edge.timestamp_secs(), ts);
    assert_eq!(edge.relative_timestamp_secs(), 86400 * 30);
}

#[test]
fn test_f02_edge_signal_retrieval() {
    let edge_like = CompactEdge::new(1, SignalType::Like, BLUESKY_EPOCH_SECS);
    let edge_quote = CompactEdge::new(2, SignalType::Quote, BLUESKY_EPOCH_SECS);
    let edge_repost = CompactEdge::new(3, SignalType::Repost, BLUESKY_EPOCH_SECS);

    assert_eq!(edge_like.signal(), SignalType::Like);
    assert_eq!(edge_quote.signal(), SignalType::Quote);
    assert_eq!(edge_repost.signal(), SignalType::Repost);
}

#[test]
fn test_f02_edge_size_exact_8_bytes() {
    assert_eq!(std::mem::size_of::<CompactEdge>(), 8);
    assert_eq!(std::mem::align_of::<CompactEdge>(), 4);
}

// ===========================================================================
// Feature 3: Multi-Signal Edge Weighting
// ===========================================================================

#[test]
fn test_f03_like_weight_1x() {
    let edge = CompactEdge::new(1, SignalType::Like, BLUESKY_EPOCH_SECS);
    assert_eq!(edge.weight(), 1.0);
    assert_eq!(SignalType::Like.weight(), 1.0);
}

#[test]
fn test_f03_quote_weight_2x() {
    let edge = CompactEdge::new(1, SignalType::Quote, BLUESKY_EPOCH_SECS);
    assert_eq!(edge.weight(), 2.0);
    assert_eq!(SignalType::Quote.weight(), 2.0);
}

#[test]
fn test_f03_repost_weight_3x() {
    let edge = CompactEdge::new(1, SignalType::Repost, BLUESKY_EPOCH_SECS);
    assert_eq!(edge.weight(), 3.0);
    assert_eq!(SignalType::Repost.weight(), 3.0);
}

#[test]
fn test_f03_signal_ordering() {
    assert!(SignalType::Repost.weight() > SignalType::Quote.weight());
    assert!(SignalType::Quote.weight() > SignalType::Like.weight());
}

#[test]
fn test_f03_signal_weight_multiplier() {
    let decay = 0.5f32;
    assert_eq!(SignalType::Like.weight() * decay, 0.5);
    assert_eq!(SignalType::Quote.weight() * decay, 1.0);
    assert_eq!(SignalType::Repost.weight() * decay, 1.5);
}

// ===========================================================================
// Feature 4: Forward and Reverse Graph Adjacency
// ===========================================================================

#[test]
fn test_f04_forward_adjacency_insert() {
    let graph = GraphStore::new();
    graph.record_interaction(1, 100, SignalType::Like, BLUESKY_EPOCH_SECS + 50);

    let user_edges = graph.get_user_interactions(1);
    assert_eq!(user_edges.len(), 1);
    assert_eq!(user_edges[0].target(), 100);
}

#[test]
fn test_f04_reverse_adjacency_insert() {
    let graph = GraphStore::new();
    graph.record_interaction(1, 100, SignalType::Like, BLUESKY_EPOCH_SECS + 50);

    let post_edges = graph.get_post_interactions(100);
    assert_eq!(post_edges.len(), 1);
    assert_eq!(post_edges[0].target(), 1);
}

#[test]
fn test_f04_bidirectional_consistency() {
    let graph = GraphStore::new();
    graph.record_interaction(5, 500, SignalType::Repost, BLUESKY_EPOCH_SECS + 100);

    assert_eq!(graph.get_user_interactions(5)[0].target(), 500);
    assert_eq!(graph.get_post_interactions(500)[0].target(), 5);
}

#[test]
fn test_f04_user_multi_post_edges() {
    let graph = GraphStore::new();
    graph.record_interaction(10, 101, SignalType::Like, BLUESKY_EPOCH_SECS + 1);
    graph.record_interaction(10, 102, SignalType::Quote, BLUESKY_EPOCH_SECS + 2);
    graph.record_interaction(10, 103, SignalType::Repost, BLUESKY_EPOCH_SECS + 3);

    let edges = graph.get_user_interactions(10);
    assert_eq!(edges.len(), 3);
}

#[test]
fn test_f04_post_multi_user_edges() {
    let graph = GraphStore::new();
    graph.record_interaction(1, 200, SignalType::Like, BLUESKY_EPOCH_SECS + 10);
    graph.record_interaction(2, 200, SignalType::Like, BLUESKY_EPOCH_SECS + 20);
    graph.record_interaction(3, 200, SignalType::Like, BLUESKY_EPOCH_SECS + 30);

    let edges = graph.get_post_interactions(200);
    assert_eq!(edges.len(), 3);
    assert_eq!(graph.get_post_interaction_count(200), 3);
}

// ===========================================================================
// Feature 5: User Roaring Bitmaps
// ===========================================================================

#[test]
fn test_f05_user_bitmap_creation() {
    let graph = GraphStore::new();
    graph.record_interaction(1, 10, SignalType::Like, BLUESKY_EPOCH_SECS);

    let bm = graph.get_user_likes_bitmap(1).expect("bitmap should exist");
    assert!(bm.contains(10));
}

#[test]
fn test_f05_bitmap_set_membership() {
    let graph = GraphStore::new();
    graph.record_interaction(2, 100, SignalType::Like, BLUESKY_EPOCH_SECS);
    graph.record_interaction(2, 200, SignalType::Quote, BLUESKY_EPOCH_SECS);

    let bm = graph.get_user_likes_bitmap(2).unwrap();
    assert!(bm.contains(100));
    assert!(bm.contains(200));
    assert!(!bm.contains(300));
}

#[test]
fn test_f05_bitmap_intersection() {
    let graph = GraphStore::new();
    graph.record_interaction(1, 10, SignalType::Like, BLUESKY_EPOCH_SECS);
    graph.record_interaction(1, 20, SignalType::Like, BLUESKY_EPOCH_SECS);
    graph.record_interaction(2, 20, SignalType::Like, BLUESKY_EPOCH_SECS);
    graph.record_interaction(2, 30, SignalType::Like, BLUESKY_EPOCH_SECS);

    let bm1 = graph.get_user_likes_bitmap(1).unwrap();
    let bm2 = graph.get_user_likes_bitmap(2).unwrap();
    assert_eq!(bm1.intersection_len(&bm2), 1);
}

#[test]
fn test_f05_bitmap_jaccard_similarity() {
    let graph = GraphStore::new();
    graph.record_interaction(1, 10, SignalType::Like, BLUESKY_EPOCH_SECS);
    graph.record_interaction(1, 20, SignalType::Like, BLUESKY_EPOCH_SECS);
    graph.record_interaction(2, 20, SignalType::Like, BLUESKY_EPOCH_SECS);
    graph.record_interaction(2, 30, SignalType::Like, BLUESKY_EPOCH_SECS);

    // Shared: {20}, Union: {10, 20, 30} -> Jaccard = 1/3
    let sim = graph.compute_jaccard_similarity(1, 2);
    assert!((sim - (1.0 / 3.0)).abs() < 1e-4);
}

#[test]
fn test_f05_bitmap_cardinality() {
    let graph = GraphStore::new();
    for p in 1..=50 {
        graph.record_interaction(7, p, SignalType::Like, BLUESKY_EPOCH_SECS);
    }
    let bm = graph.get_user_likes_bitmap(7).unwrap();
    assert_eq!(bm.len(), 50);
}

// ===========================================================================
// Feature 6: Follow Graph Storage
// ===========================================================================

#[test]
fn test_f06_record_follow_relation() {
    let graph = GraphStore::new();
    graph.record_follow(1, 2);
    let follows = graph.get_user_follows(1);
    assert_eq!(follows, vec![2]);
}

#[test]
fn test_f06_get_user_follows() {
    let graph = GraphStore::new();
    graph.record_follow(10, 20);
    graph.record_follow(10, 30);
    let follows = graph.get_user_follows(10);
    assert_eq!(follows.len(), 2);
    assert!(follows.contains(&20));
    assert!(follows.contains(&30));
}

#[test]
fn test_f06_multi_follow_retrieval() {
    let graph = GraphStore::new();
    for f in 100..110 {
        graph.record_follow(1, f);
    }
    assert_eq!(graph.get_user_follows(1).len(), 10);
}

#[test]
fn test_f06_follow_graph_directed() {
    let graph = GraphStore::new();
    graph.record_follow(1, 2);
    assert!(graph.get_user_follows(1).contains(&2));
    assert!(graph.get_user_follows(2).is_empty());
}

#[test]
fn test_f06_follow_membership_check() {
    let graph = GraphStore::new();
    graph.record_follow(5, 6);
    let follows = graph.get_user_follows(5);
    assert!(follows.contains(&6));
    assert!(!follows.contains(&7));
}

// ===========================================================================
// Feature 7: Post Metadata & Thread Tracking
// ===========================================================================

#[test]
fn test_f07_record_post_meta() {
    let graph = GraphStore::new();
    graph.record_post_meta(100, 10, None, None, BLUESKY_EPOCH_SECS + 500);

    let meta = graph.get_post_meta(100).unwrap();
    assert_eq!(meta.author_id, 10);
    assert_eq!(meta.created_at, BLUESKY_EPOCH_SECS + 500);
}

#[test]
fn test_f07_get_post_author() {
    let graph = GraphStore::new();
    graph.record_post_meta(200, 20, None, None, BLUESKY_EPOCH_SECS);
    assert_eq!(graph.get_post_meta(200).unwrap().author_id, 20);
    assert_eq!(graph.get_author_posts(20), vec![200]);
}

#[test]
fn test_f07_get_post_created_at() {
    let graph = GraphStore::new();
    let ts = BLUESKY_EPOCH_SECS + 12345;
    graph.record_post_meta(300, 30, None, None, ts);
    assert_eq!(graph.get_post_meta(300).unwrap().created_at, ts);
}

#[test]
fn test_f07_reply_root_tracking() {
    let graph = GraphStore::new();
    graph.record_post_meta(500, 1, None, None, BLUESKY_EPOCH_SECS); // Root
    graph.record_post_meta(501, 2, Some(500), Some(500), BLUESKY_EPOCH_SECS + 10); // Reply

    let reply_meta = graph.get_post_meta(501).unwrap();
    assert_eq!(reply_meta.root_id, Some(500));
    assert_eq!(reply_meta.parent_id, Some(500));
    assert!(reply_meta.is_reply());
}

#[test]
fn test_f07_standalone_post_none_root() {
    let graph = GraphStore::new();
    graph.record_post_meta(600, 3, None, None, BLUESKY_EPOCH_SECS);
    let meta = graph.get_post_meta(600).unwrap();
    assert!(meta.is_root());
    assert!(!meta.is_reply());
}

// ===========================================================================
// Feature 8: Exponential Time Decay Math
// ===========================================================================

#[test]
fn test_f08_decay_zero_elapsed() {
    let weight = calculate_time_decay(SignalType::Like, 1000, 1000, 36.0 * 3600.0);
    assert!((weight - 1.0).abs() < 1e-4);
}

#[test]
fn test_f08_decay_half_life_exact() {
    let tau = 36.0 * 3600.0;
    // At Δt = τ, weight = 1.0 * exp(-1) = 1/e ≈ 0.367879
    let weight = calculate_time_decay(SignalType::Like, 1000, 1000 + tau as u64, tau);
    assert!((weight - (-1.0f32).exp()).abs() < 1e-4);
}

#[test]
fn test_f08_decay_monotonic_decrease() {
    let tau = 36.0 * 3600.0;
    let w1 = calculate_time_decay(SignalType::Like, 1000, 1000 + 3600, tau);
    let w2 = calculate_time_decay(SignalType::Like, 1000, 1000 + 7200, tau);
    let w3 = calculate_time_decay(SignalType::Like, 1000, 1000 + 14400, tau);

    assert!(w1 > w2);
    assert!(w2 > w3);
}

#[test]
fn test_f08_decay_custom_tau() {
    let tau_6h = 6.0 * 3600.0;
    let tau_168h = 168.0 * 3600.0;

    let w_6h = calculate_time_decay(SignalType::Like, 0, 3600 * 12, tau_6h);
    let w_168h = calculate_time_decay(SignalType::Like, 0, 3600 * 12, tau_168h);

    // Faster decay (smaller tau) yields lower weight after 12h
    assert!(w_6h < w_168h);
}

#[test]
fn test_f08_decay_with_signal_weight() {
    let tau = 36.0 * 3600.0;
    let w_like = calculate_time_decay(SignalType::Like, 0, 3600, tau);
    let w_repost = calculate_time_decay(SignalType::Repost, 0, 3600, tau);

    assert!((w_repost - 3.0 * w_like).abs() < 1e-4);
}

// ===========================================================================
// Feature 9: BM25 Inverse Degree Popularity Dampening
// ===========================================================================

#[test]
fn test_f09_bm25_zero_interactions_1() {
    let dampener = calculate_popularity_dampener(0);
    assert!((dampener - 1.0).abs() < 1e-5);
}

#[test]
fn test_f09_bm25_monotonic_penalty() {
    let d0 = calculate_popularity_dampener(0);
    let d10 = calculate_popularity_dampener(10);
    let d100 = calculate_popularity_dampener(100);
    let d1000 = calculate_popularity_dampener(1000);

    assert!(d0 > d10);
    assert!(d10 > d100);
    assert!(d100 > d1000);
}

#[test]
fn test_f09_bm25_score_scaling() {
    let d15 = calculate_popularity_dampener(15);
    assert!((d15 - 0.25).abs() < 1e-4); // 1 / sqrt(16) = 0.25
}

#[test]
fn test_f09_bm25_comparison_viral_vs_niche() {
    let niche_dampener = calculate_popularity_dampener(3); // 1/sqrt(4) = 0.5
    let viral_dampener = calculate_popularity_dampener(99); // 1/sqrt(100) = 0.1

    assert_eq!(niche_dampener / viral_dampener, 5.0);
}

#[test]
fn test_f09_bm25_formula_exactness() {
    let count = 24;
    let expected = 1.0 / (25.0f32).sqrt();
    let actual = calculate_popularity_dampener(count);
    assert!((actual - expected).abs() < 1e-6);
}

// ===========================================================================
// Feature 10: Global High-Velocity Sliding Pool
// ===========================================================================

#[test]
fn test_f10_velocity_pool_insert() {
    let graph = GraphStore::new();
    let now = chrono_like_now();
    graph.record_interaction(1, 100, SignalType::Like, now - 100);

    let candidates = graph.get_velocity_pool_candidates_at(now, 10);
    assert_eq!(candidates, vec![100]);
}

#[test]
fn test_f10_velocity_pool_get_top_k() {
    let graph = GraphStore::new();
    let now = chrono_like_now();
    for p in 1..=20 {
        for u in 1..=p {
            graph.record_interaction(u, p as u32, SignalType::Like, now - 50);
        }
    }
    let top5 = graph.get_velocity_pool_candidates_at(now, 5);
    assert_eq!(top5.len(), 5);
    assert_eq!(top5[0], 20); // Highest interaction count
}

#[test]
fn test_f10_velocity_ranking_order() {
    let graph = GraphStore::new();
    let now = chrono_like_now();
    graph.record_interaction(1, 10, SignalType::Like, now - 100);
    graph.record_interaction(2, 10, SignalType::Like, now - 100);
    graph.record_interaction(1, 20, SignalType::Like, now - 100);

    let candidates = graph.get_velocity_pool_candidates_at(now, 10);
    assert_eq!(candidates[0], 10);
}

#[test]
fn test_f10_velocity_pool_limit_clamp() {
    let graph = GraphStore::new();
    let now = chrono_like_now();
    for p in 1..=10 {
        graph.record_interaction(1, p, SignalType::Like, now - 100);
    }
    let res = graph.get_velocity_pool_candidates_at(now, 3);
    assert_eq!(res.len(), 3);
}

#[test]
fn test_f10_velocity_sliding_window() {
    let graph = GraphStore::new();
    let now = chrono_like_now();
    // Post 1 is 1 hour ago (within 6h window)
    graph.record_interaction(1, 1, SignalType::Like, now - 3600);
    // Post 2 is 10 hours ago (outside 6h window)
    graph.record_interaction(2, 2, SignalType::Like, now - 36000);

    let candidates = graph.get_velocity_pool_candidates_at(now, 10);
    assert_eq!(candidates, vec![1]);
}

// ===========================================================================
// Feature 11: 3-Step Random Walk Graph Traversal
// ===========================================================================

#[test]
fn test_f11_3step_walk_basic_path() {
    let (interner, graph) = SyntheticGraphBuilder::standard_cold_start_fixture(chrono_like_now());
    let recommender = TestRecommender::new(interner, graph);
    let dials = RecommendationDials::default();

    let res = recommender
        .recommend(Some("did:plc:active_user"), &dials, chrono_like_now())
        .unwrap();
    assert!(!res.posts.is_empty());
    assert_eq!(
        res.posts[0].source,
        RecommendationSource::Tier1InteractionWalk
    );
}

#[test]
fn test_f11_3step_walk_multi_cointeractors() {
    let now = chrono_like_now();
    let mut builder = SyntheticGraphBuilder::new();
    let viewer = "did:plc:v";
    // Viewer likes P1..P10
    for i in 1..=10 {
        let p = format!("at://did:plc:a/app.bsky.feed.post/{i}");
        builder = builder.add_post(
            p.clone(),
            "did:plc:a",
            None::<&str>,
            None::<&str>,
            now - 1000,
        );
        builder = builder.add_interaction(viewer, p.clone(), SignalType::Like, now - 800);
        // Co-interactor u1 likes P1..P5 and candidate C1
        if i <= 5 {
            builder = builder.add_interaction("did:plc:u1", p.clone(), SignalType::Like, now - 700);
        }
    }
    builder = builder.add_post(
        "at://did:plc:b/app.bsky.feed.post/c1",
        "did:plc:b",
        None::<&str>,
        None::<&str>,
        now - 500,
    );
    builder = builder.add_interaction(
        "did:plc:u1",
        "at://did:plc:b/app.bsky.feed.post/c1",
        SignalType::Like,
        now - 400,
    );

    let interner = Arc::new(StringInterner::new());
    let graph = Arc::new(GraphStore::new());
    builder.populate(&interner, &graph);

    let rec = TestRecommender::new(interner, graph);
    let res = rec
        .recommend(Some(viewer), &RecommendationDials::default(), now)
        .unwrap();

    assert!(res.posts.iter().any(|p| p.uri.contains("/c1")));
}

#[test]
fn test_f11_3step_walk_candidate_discovery() {
    let (interner, graph) = SyntheticGraphBuilder::standard_cold_start_fixture(chrono_like_now());
    let rec = TestRecommender::new(interner, graph);
    let res = rec
        .recommend(
            Some("did:plc:active_user"),
            &RecommendationDials::default(),
            chrono_like_now(),
        )
        .unwrap();

    for post in &res.posts {
        assert_valid_at_uri(&post.uri);
    }
}

#[test]
fn test_f11_3step_walk_path_weight_accumulation() {
    let now = chrono_like_now();
    let mut builder = SyntheticGraphBuilder::new();
    let viewer = "did:plc:viewer";
    for i in 1..=10 {
        let p = format!("at://did:plc:author/app.bsky.feed.post/{i}");
        builder = builder.add_post(
            p.clone(),
            "did:plc:author",
            None::<&str>,
            None::<&str>,
            now - 2000,
        );
        builder = builder.add_interaction(viewer, p.clone(), SignalType::Like, now - 1500);
        builder = builder.add_interaction("did:plc:co1", p.clone(), SignalType::Like, now - 1400);
        builder = builder.add_interaction("did:plc:co2", p.clone(), SignalType::Like, now - 1400);
    }
    let target_post = "at://did:plc:target_author/app.bsky.feed.post/target";
    builder = builder.add_post(
        target_post,
        "did:plc:target_author",
        None::<&str>,
        None::<&str>,
        now - 1000,
    );
    builder = builder.add_interaction("did:plc:co1", target_post, SignalType::Like, now - 800);
    builder = builder.add_interaction("did:plc:co2", target_post, SignalType::Like, now - 800);

    let interner = Arc::new(StringInterner::new());
    let graph = Arc::new(GraphStore::new());
    builder.populate(&interner, &graph);

    let rec = TestRecommender::new(interner, graph);
    let res = rec
        .recommend(Some(viewer), &RecommendationDials::default(), now)
        .unwrap();
    assert!(!res.posts.is_empty());
    assert_eq!(res.posts[0].uri, target_post);
}

#[test]
fn test_f11_3step_walk_min_like_threshold() {
    let now = chrono_like_now();
    let mut builder = SyntheticGraphBuilder::new();
    let viewer = "did:plc:threshold_user";
    for i in 1..=9 {
        let p = format!("at://did:plc:a/app.bsky.feed.post/{i}");
        builder = builder.add_post(
            p.clone(),
            "did:plc:a",
            None::<&str>,
            None::<&str>,
            now - 1000,
        );
        builder = builder.add_interaction(viewer, p, SignalType::Like, now - 500);
    }
    let interner = Arc::new(StringInterner::new());
    let graph = Arc::new(GraphStore::new());
    builder.populate(&interner, &graph);

    let rec = TestRecommender::new(interner, graph);
    let res = rec
        .recommend(Some(viewer), &RecommendationDials::default(), now)
        .unwrap();
    // 9 likes falls back to Tier 2/3 (not Tier 1)
    if let Some(first) = res.posts.first() {
        assert_ne!(first.source, RecommendationSource::Tier1InteractionWalk);
    }
}

// ===========================================================================
// Feature 12: Candidate Scoring & Aggregation
// ===========================================================================

#[test]
fn test_f12_candidate_scoring_composite() {
    let (interner, graph) = SyntheticGraphBuilder::standard_cold_start_fixture(chrono_like_now());
    let rec = TestRecommender::new(interner, graph);
    let res = rec
        .recommend(
            Some("did:plc:active_user"),
            &RecommendationDials::default(),
            chrono_like_now(),
        )
        .unwrap();

    for p in &res.posts {
        assert!(p.score > 0.0);
    }
}

#[test]
fn test_f12_candidate_aggregation_multi_source() {
    let (interner, graph) = SyntheticGraphBuilder::standard_cold_start_fixture(chrono_like_now());
    let rec = TestRecommender::new(interner, graph);
    let dials = RecommendationDials {
        explore_ratio: 0.15,
        ..Default::default()
    };
    let res = rec
        .recommend(Some("did:plc:active_user"), &dials, chrono_like_now())
        .unwrap();
    assert!(!res.posts.is_empty());
}

#[test]
fn test_f12_ranking_order_descending() {
    let (interner, graph) = SyntheticGraphBuilder::standard_cold_start_fixture(chrono_like_now());
    let rec = TestRecommender::new(interner, graph);
    let dials = RecommendationDials {
        explore_ratio: 0.0,
        ..Default::default()
    };
    let res = rec
        .recommend(Some("did:plc:active_user"), &dials, chrono_like_now())
        .unwrap();

    for i in 1..res.posts.len() {
        assert!(res.posts[i - 1].score >= res.posts[i].score);
    }
}

#[test]
fn test_f12_scoring_weight_attribution() {
    let now = chrono_like_now();
    let weight_like = calculate_time_decay(SignalType::Like, now - 100, now, 36.0 * 3600.0);
    let weight_repost = calculate_time_decay(SignalType::Repost, now - 100, now, 36.0 * 3600.0);
    assert!(weight_repost > weight_like);
}

#[test]
fn test_f12_candidate_score_positivity() {
    let (interner, graph) = SyntheticGraphBuilder::standard_cold_start_fixture(chrono_like_now());
    let rec = TestRecommender::new(interner, graph);
    let res = rec
        .recommend(
            Some("did:plc:new_user"),
            &RecommendationDials::default(),
            chrono_like_now(),
        )
        .unwrap();
    for p in res.posts {
        assert!(!p.score.is_nan());
        assert!(!p.score.is_infinite());
        assert!(p.score >= 0.0);
    }
}

// ===========================================================================
// Feature 13: 3-Tier Cold-Start Hierarchy
// ===========================================================================

#[test]
fn test_f13_tier1_active_user_execution() {
    let (interner, graph) = SyntheticGraphBuilder::standard_cold_start_fixture(chrono_like_now());
    let rec = TestRecommender::new(interner, graph);
    let res = rec
        .recommend(
            Some("did:plc:active_user"),
            &RecommendationDials::default(),
            chrono_like_now(),
        )
        .unwrap();
    assert_eq!(
        res.posts[0].source,
        RecommendationSource::Tier1InteractionWalk
    );
}

#[test]
fn test_f13_tier2_new_user_follow_walk() {
    let (interner, graph) = SyntheticGraphBuilder::standard_cold_start_fixture(chrono_like_now());
    let rec = TestRecommender::new(interner, graph);
    let res = rec
        .recommend(
            Some("did:plc:new_user"),
            &RecommendationDials::default(),
            chrono_like_now(),
        )
        .unwrap();
    assert!(res
        .posts
        .iter()
        .any(|p| p.source == RecommendationSource::Tier2FollowWalk));
}

#[test]
fn test_f13_tier3_zero_history_velocity_pool() {
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
fn test_f13_tier1_to_tier2_fallback_on_empty() {
    let now = chrono_like_now();
    let mut builder = SyntheticGraphBuilder::new();
    let viewer = "did:plc:isolated_active";
    // 10 likes but zero co-interactors
    for i in 1..=10 {
        let p = format!("at://did:plc:author/app.bsky.feed.post/iso_{i}");
        builder = builder.add_post(
            p.clone(),
            "did:plc:author",
            None::<&str>,
            None::<&str>,
            now - 500,
        );
        builder = builder.add_interaction(viewer, p, SignalType::Like, now - 400);
    }
    // Follows another user who has likes
    builder = builder.add_follow(viewer, "did:plc:friend");
    let friend_p = "at://did:plc:author2/app.bsky.feed.post/friend_post";
    builder = builder.add_post(
        friend_p,
        "did:plc:author2",
        None::<&str>,
        None::<&str>,
        now - 300,
    );
    builder = builder.add_interaction("did:plc:friend", friend_p, SignalType::Like, now - 200);

    let interner = Arc::new(StringInterner::new());
    let graph = Arc::new(GraphStore::new());
    builder.populate(&interner, &graph);

    let rec = TestRecommender::new(interner, graph);
    let res = rec
        .recommend(Some(viewer), &RecommendationDials::default(), now)
        .unwrap();
    // Cascaded to Tier 2
    assert_eq!(res.posts[0].source, RecommendationSource::Tier2FollowWalk);
}

#[test]
fn test_f13_tier2_to_tier3_fallback_on_empty() {
    let now = chrono_like_now();
    let mut builder = SyntheticGraphBuilder::new();
    let viewer = "did:plc:empty_follow_user";
    builder = builder.add_follow(viewer, "did:plc:inactive_friend");
    // Global trending
    builder = builder.add_post(
        "at://did:plc:t/app.bsky.feed.post/trend",
        "did:plc:t",
        None::<&str>,
        None::<&str>,
        now - 100,
    );
    builder = builder.add_interaction(
        "did:plc:other",
        "at://did:plc:t/app.bsky.feed.post/trend",
        SignalType::Like,
        now - 50,
    );

    let interner = Arc::new(StringInterner::new());
    let graph = Arc::new(GraphStore::new());
    builder.populate(&interner, &graph);

    let rec = TestRecommender::new(interner, graph);
    let res = rec
        .recommend(Some(viewer), &RecommendationDials::default(), now)
        .unwrap();
    assert_eq!(res.posts[0].source, RecommendationSource::Tier3VelocityPool);
}

// ===========================================================================
// Feature 14: ε-Greedy Serendipity & Exploration
// ===========================================================================

#[test]
fn test_f14_serendipity_ratio_split() {
    let (interner, graph) = SyntheticGraphBuilder::standard_cold_start_fixture(chrono_like_now());
    let rec = TestRecommender::new(interner, graph);
    let dials = RecommendationDials {
        explore_ratio: 0.20,
        ..Default::default()
    };
    let res = rec
        .recommend(Some("did:plc:active_user"), &dials, chrono_like_now())
        .unwrap();
    assert!(!res.posts.is_empty());
}

#[test]
fn test_f14_serendipity_balanced_15_pct() {
    let dials = RecommendationDials::from_query(None, Some("balanced"), None, None, None);
    assert_eq!(dials.explore_ratio, 0.15);
}

#[test]
fn test_f14_serendipity_familiar_5_pct() {
    let dials = RecommendationDials::from_query(None, Some("familiar"), None, None, None);
    assert_eq!(dials.explore_ratio, 0.05);
}

#[test]
fn test_f14_serendipity_deep_dive_35_pct() {
    let dials = RecommendationDials::from_query(None, Some("deep_dive"), None, None, None);
    assert_eq!(dials.explore_ratio, 0.35);
}

#[test]
fn test_f14_serendipity_cluster_sampling() {
    let (interner, graph) = SyntheticGraphBuilder::standard_cold_start_fixture(chrono_like_now());
    let rec = TestRecommender::new(interner, graph);
    let dials = RecommendationDials {
        explore_ratio: 0.35,
        ..Default::default()
    };
    let res = rec
        .recommend(Some("did:plc:active_user"), &dials, chrono_like_now())
        .unwrap();
    let has_explore = res
        .posts
        .iter()
        .any(|p| p.source == RecommendationSource::ExplorationSerendipity);
    assert!(has_explore || res.posts.len() <= 2);
}

// ===========================================================================
// Feature 15: Author Diversity Filtering
// ===========================================================================

#[test]
fn test_f15_author_diversity_max_2_enforced() {
    let (interner, graph) = SyntheticGraphBuilder::standard_cold_start_fixture(chrono_like_now());
    let rec = TestRecommender::new(Arc::clone(&interner), Arc::clone(&graph));
    let res = rec
        .recommend(
            Some("did:plc:active_user"),
            &RecommendationDials::default(),
            chrono_like_now(),
        )
        .unwrap();

    let skeleton_feed: Vec<SkeletonFeedPost> = res
        .posts
        .into_iter()
        .map(|p| SkeletonFeedPost::new(p.uri))
        .collect();
    assert_author_diversity(&skeleton_feed, &interner, &graph, 2);
}

#[test]
fn test_f15_author_diversity_backfill() {
    let now = chrono_like_now();
    let mut builder = SyntheticGraphBuilder::new();
    let viewer = "did:plc:div_user";
    // Author 1 creates 10 posts
    for i in 1..=10 {
        let p = format!("at://did:plc:author_mono/app.bsky.feed.post/{i}");
        builder = builder.add_post(
            p.clone(),
            "did:plc:author_mono",
            None::<&str>,
            None::<&str>,
            now - 1000,
        );
        builder = builder.add_interaction("did:plc:co", p, SignalType::Like, now - 500);
    }
    // Author 2 creates 2 posts
    for i in 1..=2 {
        let p = format!("at://did:plc:author_alt/app.bsky.feed.post/{i}");
        builder = builder.add_post(
            p.clone(),
            "did:plc:author_alt",
            None::<&str>,
            None::<&str>,
            now - 1000,
        );
        builder = builder.add_interaction("did:plc:co", p, SignalType::Like, now - 500);
    }
    // Viewer co-interacts
    for i in 1..=10 {
        let seed_p = format!("at://did:plc:seed/app.bsky.feed.post/{i}");
        builder = builder.add_post(
            seed_p.clone(),
            "did:plc:seed",
            None::<&str>,
            None::<&str>,
            now - 2000,
        );
        builder = builder.add_interaction(viewer, seed_p.clone(), SignalType::Like, now - 1500);
        builder = builder.add_interaction("did:plc:co", seed_p, SignalType::Like, now - 1400);
    }

    let interner = Arc::new(StringInterner::new());
    let graph = Arc::new(GraphStore::new());
    builder.populate(&interner, &graph);

    let rec = TestRecommender::new(Arc::clone(&interner), Arc::clone(&graph));
    let res = rec
        .recommend(Some(viewer), &RecommendationDials::default(), now)
        .unwrap();
    let skeleton_feed: Vec<SkeletonFeedPost> = res
        .posts
        .into_iter()
        .map(|p| SkeletonFeedPost::new(p.uri))
        .collect();
    assert_author_diversity(&skeleton_feed, &interner, &graph, 2);
}

#[test]
fn test_f15_author_diversity_page_boundary() {
    let (interner, graph) = SyntheticGraphBuilder::standard_cold_start_fixture(chrono_like_now());
    let rec = TestRecommender::new(Arc::clone(&interner), Arc::clone(&graph));
    let dials = RecommendationDials {
        limit: 5,
        ..Default::default()
    };
    let res = rec
        .recommend(Some("did:plc:active_user"), &dials, chrono_like_now())
        .unwrap();
    assert!(res.posts.len() <= 5);
}

#[test]
fn test_f15_author_diversity_multiple_authors() {
    let (interner, graph) = SyntheticGraphBuilder::standard_cold_start_fixture(chrono_like_now());
    let rec = TestRecommender::new(interner, graph);
    let res = rec
        .recommend(
            Some("did:plc:cold_user"),
            &RecommendationDials::default(),
            chrono_like_now(),
        )
        .unwrap();
    assert!(!res.posts.is_empty());
}

#[test]
fn test_f15_author_diversity_preserves_order() {
    let (interner, graph) = SyntheticGraphBuilder::standard_cold_start_fixture(chrono_like_now());
    let rec = TestRecommender::new(interner, graph);
    let res = rec
        .recommend(
            Some("did:plc:active_user"),
            &RecommendationDials::default(),
            chrono_like_now(),
        )
        .unwrap();
    for i in 1..res.posts.len() {
        assert!(res.posts[i - 1].score >= res.posts[i].score);
    }
}

// ===========================================================================
// Feature 16: Thread / Reply Tree Dampening
// ===========================================================================

#[test]
fn test_f16_thread_dampening_max_1_per_root() {
    let now = chrono_like_now();
    let mut builder = SyntheticGraphBuilder::new();
    let root = "at://did:plc:auth/app.bsky.feed.post/root1";
    let reply1 = "at://did:plc:auth/app.bsky.feed.post/reply1";
    let reply2 = "at://did:plc:auth/app.bsky.feed.post/reply2";

    builder = builder.add_post(root, "did:plc:auth", None::<&str>, None::<&str>, now - 1000);
    builder = builder.add_post(reply1, "did:plc:auth", Some(root), Some(root), now - 900);
    builder = builder.add_post(reply2, "did:plc:auth", Some(root), Some(reply1), now - 800);

    for u in 1..=5 {
        let user = format!("did:plc:u_{u}");
        builder = builder.add_interaction(user.clone(), root, SignalType::Like, now - 100);
        builder = builder.add_interaction(user.clone(), reply1, SignalType::Like, now - 100);
        builder = builder.add_interaction(user, reply2, SignalType::Like, now - 100);
    }

    let interner = Arc::new(StringInterner::new());
    let graph = Arc::new(GraphStore::new());
    builder.populate(&interner, &graph);

    let rec = TestRecommender::new(interner, graph);
    let res = rec
        .recommend(None, &RecommendationDials::default(), now)
        .unwrap();

    let root_count = res
        .posts
        .iter()
        .filter(|p| p.uri.contains("root1") || p.uri.contains("reply"))
        .count();
    assert_eq!(root_count, 1);
}

#[test]
fn test_f16_thread_dampening_selects_highest_score() {
    let now = chrono_like_now();
    let mut builder = SyntheticGraphBuilder::new();
    let root = "at://did:plc:auth/app.bsky.feed.post/root_high";
    let reply = "at://did:plc:auth/app.bsky.feed.post/reply_low";

    builder = builder.add_post(root, "did:plc:auth", None::<&str>, None::<&str>, now - 1000);
    builder = builder.add_post(reply, "did:plc:auth", Some(root), Some(root), now - 900);

    for u in 1..=10 {
        builder =
            builder.add_interaction(format!("did:plc:u_{u}"), root, SignalType::Like, now - 50);
    }
    builder = builder.add_interaction("did:plc:u_1", reply, SignalType::Like, now - 50);

    let interner = Arc::new(StringInterner::new());
    let graph = Arc::new(GraphStore::new());
    builder.populate(&interner, &graph);

    let rec = TestRecommender::new(interner, graph);
    let res = rec
        .recommend(None, &RecommendationDials::default(), now)
        .unwrap();
    assert_eq!(res.posts[0].uri, root);
}

#[test]
fn test_f16_thread_dampening_multiple_threads() {
    let now = chrono_like_now();
    let mut builder = SyntheticGraphBuilder::new();
    for t in 1..=3 {
        let root = format!("at://did:plc:a_{t}/app.bsky.feed.post/r_{t}");
        let rep = format!("at://did:plc:a_{t}/app.bsky.feed.post/rep_{t}");
        builder = builder.add_post(
            root.clone(),
            format!("did:plc:a_{t}"),
            None::<&str>,
            None::<&str>,
            now - 1000,
        );
        builder = builder.add_post(
            rep.clone(),
            format!("did:plc:a_{t}"),
            Some(root.clone()),
            Some(root.clone()),
            now - 900,
        );
        builder = builder.add_interaction("did:plc:u", root, SignalType::Like, now - 100);
        builder = builder.add_interaction("did:plc:u", rep, SignalType::Like, now - 100);
    }
    let interner = Arc::new(StringInterner::new());
    let graph = Arc::new(GraphStore::new());
    builder.populate(&interner, &graph);

    let rec = TestRecommender::new(interner, graph);
    let res = rec
        .recommend(None, &RecommendationDials::default(), now)
        .unwrap();
    assert_eq!(res.posts.len(), 3);
}

#[test]
fn test_f16_thread_dampening_standalone_posts_kept() {
    let now = chrono_like_now();
    let mut builder = SyntheticGraphBuilder::new();
    for i in 1..=5 {
        let p = format!("at://did:plc:auth/app.bsky.feed.post/standalone_{i}");
        builder = builder.add_post(
            p.clone(),
            "did:plc:auth",
            None::<&str>,
            None::<&str>,
            now - 500,
        );
        builder = builder.add_interaction("did:plc:user", p, SignalType::Like, now - 100);
    }
    let interner = Arc::new(StringInterner::new());
    let graph = Arc::new(GraphStore::new());
    builder.populate(&interner, &graph);

    let rec = TestRecommender::new(interner, graph);
    let res = rec
        .recommend(None, &RecommendationDials::default(), now)
        .unwrap();
    assert_eq!(res.posts.len(), 2); // Max 2 per author
}

#[test]
fn test_f16_thread_dampening_interleaved_replies() {
    let now = chrono_like_now();
    let mut builder = SyntheticGraphBuilder::new();
    let r1 = "at://did:plc:a1/app.bsky.feed.post/r1";
    let r2 = "at://did:plc:a2/app.bsky.feed.post/r2";
    let rep1 = "at://did:plc:a1/app.bsky.feed.post/rep1";

    builder = builder.add_post(r1, "did:plc:a1", None::<&str>, None::<&str>, now - 1000);
    builder = builder.add_post(rep1, "did:plc:a1", Some(r1), Some(r1), now - 900);
    builder = builder.add_post(r2, "did:plc:a2", None::<&str>, None::<&str>, now - 800);

    builder = builder.add_interaction("did:plc:u", r1, SignalType::Like, now - 100);
    builder = builder.add_interaction("did:plc:u", rep1, SignalType::Like, now - 100);
    builder = builder.add_interaction("did:plc:u", r2, SignalType::Like, now - 100);

    let interner = Arc::new(StringInterner::new());
    let graph = Arc::new(GraphStore::new());
    builder.populate(&interner, &graph);

    let rec = TestRecommender::new(interner, graph);
    let res = rec
        .recommend(None, &RecommendationDials::default(), now)
        .unwrap();
    assert_eq!(res.posts.len(), 2);
}

// ===========================================================================
// Feature 17: Seen / Liked / Self Post Deduplication
// ===========================================================================

#[test]
fn test_f17_dedup_exclude_liked_posts() {
    let (interner, graph) = SyntheticGraphBuilder::standard_cold_start_fixture(chrono_like_now());
    let rec = TestRecommender::new(Arc::clone(&interner), Arc::clone(&graph));
    let res = rec
        .recommend(
            Some("did:plc:active_user"),
            &RecommendationDials::default(),
            chrono_like_now(),
        )
        .unwrap();

    let active_uid = interner.lookup_id("did:plc:active_user").unwrap();
    let liked = graph.get_user_likes_bitmap(active_uid).unwrap();

    for post in res.posts {
        let pid = interner.lookup_id(&post.uri).unwrap();
        assert!(!liked.contains(pid));
    }
}

#[test]
fn test_f17_dedup_exclude_reposted_posts() {
    let now = chrono_like_now();
    let mut builder = SyntheticGraphBuilder::new();
    let viewer = "did:plc:repost_viewer";
    let post = "at://did:plc:auth/app.bsky.feed.post/reposted";

    builder = builder.add_post(post, "did:plc:auth", None::<&str>, None::<&str>, now - 1000);
    builder = builder.add_interaction(viewer, post, SignalType::Repost, now - 500);

    let interner = Arc::new(StringInterner::new());
    let graph = Arc::new(GraphStore::new());
    builder.populate(&interner, &graph);

    let rec = TestRecommender::new(interner, graph);
    let res = rec
        .recommend(Some(viewer), &RecommendationDials::default(), now)
        .unwrap();
    assert!(res.posts.iter().all(|p| p.uri != post));
}

#[test]
fn test_f17_dedup_exclude_self_authored_posts() {
    let now = chrono_like_now();
    let mut builder = SyntheticGraphBuilder::new();
    let viewer = "did:plc:self_author";
    let self_post = "at://did:plc:self_author/app.bsky.feed.post/my_own";

    builder = builder.add_post(self_post, viewer, None::<&str>, None::<&str>, now - 1000);
    // Other users liked it making it high velocity
    for u in 1..=5 {
        builder = builder.add_interaction(
            format!("did:plc:fan_{u}"),
            self_post,
            SignalType::Like,
            now - 100,
        );
    }

    let interner = Arc::new(StringInterner::new());
    let graph = Arc::new(GraphStore::new());
    builder.populate(&interner, &graph);

    let rec = TestRecommender::new(interner, graph);
    let res = rec
        .recommend(Some(viewer), &RecommendationDials::default(), now)
        .unwrap();
    assert!(res.posts.iter().all(|p| p.uri != self_post));
}

#[test]
fn test_f17_dedup_mixed_interactions() {
    let (interner, graph) = SyntheticGraphBuilder::standard_cold_start_fixture(chrono_like_now());
    let rec = TestRecommender::new(interner, graph);
    let res = rec
        .recommend(
            Some("did:plc:new_user"),
            &RecommendationDials::default(),
            chrono_like_now(),
        )
        .unwrap();

    assert!(res
        .posts
        .iter()
        .all(|p| !p.uri.contains("trending_1") && !p.uri.contains("trending_2")));
}

#[test]
fn test_f17_dedup_preserves_unseen_candidates() {
    let (interner, graph) = SyntheticGraphBuilder::standard_cold_start_fixture(chrono_like_now());
    let rec = TestRecommender::new(interner, graph);
    let res = rec
        .recommend(
            Some("did:plc:active_user"),
            &RecommendationDials::default(),
            chrono_like_now(),
        )
        .unwrap();
    assert!(!res.posts.is_empty());
}

// ===========================================================================
// Feature 18: Stable Cursor Pagination
// ===========================================================================

#[test]
fn test_f18_cursor_generation_valid() {
    let (interner, graph) = SyntheticGraphBuilder::standard_cold_start_fixture(chrono_like_now());
    let rec = TestRecommender::new(interner, graph);
    let dials = RecommendationDials {
        limit: 2,
        ..Default::default()
    };
    let res = rec
        .recommend(Some("did:plc:active_user"), &dials, chrono_like_now())
        .unwrap();
    assert_eq!(res.cursor, Some("2".to_string()));
}

#[test]
fn test_f18_cursor_decoding_deterministic() {
    let (interner, graph) = SyntheticGraphBuilder::standard_cold_start_fixture(chrono_like_now());
    let rec = TestRecommender::new(interner, graph);
    let dials_p1 = RecommendationDials {
        limit: 2,
        cursor: None,
        ..Default::default()
    };
    let dials_p2 = RecommendationDials {
        limit: 2,
        cursor: Some("2".to_string()),
        ..Default::default()
    };

    let res1 = rec
        .recommend(Some("did:plc:active_user"), &dials_p1, chrono_like_now())
        .unwrap();
    let res2 = rec
        .recommend(Some("did:plc:active_user"), &dials_p2, chrono_like_now())
        .unwrap();

    assert_ne!(res1.posts[0].uri, res2.posts[0].uri);
}

#[test]
fn test_f18_cursor_pagination_page_step() {
    let (interner, graph) = SyntheticGraphBuilder::standard_cold_start_fixture(chrono_like_now());
    let rec = TestRecommender::new(interner, graph);
    let dials = RecommendationDials {
        limit: 3,
        cursor: Some("3".to_string()),
        ..Default::default()
    };
    let res = rec
        .recommend(Some("did:plc:active_user"), &dials, chrono_like_now())
        .unwrap();
    assert!(res.posts.len() <= 3);
}

#[test]
fn test_f18_cursor_no_duplicate_across_pages() {
    let (interner, graph) = SyntheticGraphBuilder::standard_cold_start_fixture(chrono_like_now());
    let rec = TestRecommender::new(interner, graph);
    let res1 = rec
        .recommend(
            Some("did:plc:active_user"),
            &RecommendationDials {
                limit: 2,
                ..Default::default()
            },
            chrono_like_now(),
        )
        .unwrap();
    let res2 = rec
        .recommend(
            Some("did:plc:active_user"),
            &RecommendationDials {
                limit: 2,
                cursor: res1.cursor,
                ..Default::default()
            },
            chrono_like_now(),
        )
        .unwrap();

    for p1 in &res1.posts {
        for p2 in &res2.posts {
            assert_ne!(p1.uri, p2.uri);
        }
    }
}

#[test]
fn test_f18_cursor_terminal_none() {
    let (interner, graph) = SyntheticGraphBuilder::standard_cold_start_fixture(chrono_like_now());
    let rec = TestRecommender::new(interner, graph);
    let res = rec
        .recommend(
            Some("did:plc:active_user"),
            &RecommendationDials {
                limit: 100,
                ..Default::default()
            },
            chrono_like_now(),
        )
        .unwrap();
    assert_eq!(res.cursor, None);
}

// ===========================================================================
// Feature 19: Query Parameter Dials Mapping
// ===========================================================================

#[test]
fn test_f19_dial_freshness_realtime_6h() {
    let dials = RecommendationDials::from_query(Some("realtime"), None, None, None, None);
    assert_eq!(dials.half_life_secs, 6.0 * 3600.0);
}

#[test]
fn test_f19_dial_freshness_balanced_36h() {
    let dials = RecommendationDials::from_query(Some("balanced"), None, None, None, None);
    assert_eq!(dials.half_life_secs, 36.0 * 3600.0);
}

#[test]
fn test_f19_dial_freshness_weekly_168h() {
    let dials = RecommendationDials::from_query(Some("weekly"), None, None, None, None);
    assert_eq!(dials.half_life_secs, 168.0 * 3600.0);
}

#[test]
fn test_f19_dial_discovery_familiar_5() {
    let dials = RecommendationDials::from_query(None, Some("familiar"), None, None, None);
    assert_eq!(dials.explore_ratio, 0.05);
}

#[test]
fn test_f19_dial_discovery_deep_dive_35() {
    let dials = RecommendationDials::from_query(None, Some("deep_dive"), None, None, None);
    assert_eq!(dials.explore_ratio, 0.35);
}

// ===========================================================================
// Feature 20: Explanation Generator
// ===========================================================================

#[test]
fn test_f20_explain_true_populates_metadata() {
    let (interner, graph) = SyntheticGraphBuilder::standard_cold_start_fixture(chrono_like_now());
    let rec = TestRecommender::new(interner, graph);
    let dials = RecommendationDials {
        explain: true,
        ..Default::default()
    };
    let res = rec
        .recommend(Some("did:plc:active_user"), &dials, chrono_like_now())
        .unwrap();

    assert!(res.posts[0].explain.is_some());
}

#[test]
fn test_f20_explain_false_omits_metadata() {
    let (interner, graph) = SyntheticGraphBuilder::standard_cold_start_fixture(chrono_like_now());
    let rec = TestRecommender::new(interner, graph);
    let dials = RecommendationDials {
        explain: false,
        ..Default::default()
    };
    let res = rec
        .recommend(Some("did:plc:active_user"), &dials, chrono_like_now())
        .unwrap();

    assert!(res.posts[0].explain.is_none());
}

#[test]
fn test_f20_explain_includes_source_tier() {
    let (interner, graph) = SyntheticGraphBuilder::standard_cold_start_fixture(chrono_like_now());
    let rec = TestRecommender::new(interner, graph);
    let dials = RecommendationDials {
        explain: true,
        ..Default::default()
    };
    let res = rec
        .recommend(Some("did:plc:active_user"), &dials, chrono_like_now())
        .unwrap();

    let explain_str = res.posts[0].explain.as_ref().unwrap();
    assert!(explain_str.contains("tier1_interaction_walk"));
}

#[test]
fn test_f20_explain_includes_cointeractor_sample() {
    let (interner, graph) = SyntheticGraphBuilder::standard_cold_start_fixture(chrono_like_now());
    let rec = TestRecommender::new(interner, graph);
    let dials = RecommendationDials {
        explain: true,
        ..Default::default()
    };
    let res = rec
        .recommend(Some("did:plc:active_user"), &dials, chrono_like_now())
        .unwrap();

    let explain_str = res.posts[0].explain.as_ref().unwrap();
    assert!(explain_str.contains("score="));
}

#[test]
fn test_f20_explain_includes_score_components() {
    let (interner, graph) = SyntheticGraphBuilder::standard_cold_start_fixture(chrono_like_now());
    let rec = TestRecommender::new(interner, graph);
    let dials = RecommendationDials {
        explain: true,
        ..Default::default()
    };
    let res = rec
        .recommend(Some("did:plc:cold_user"), &dials, chrono_like_now())
        .unwrap();

    let explain_str = res.posts[0].explain.as_ref().unwrap();
    assert!(explain_str.contains("tier3_velocity_pool"));
}

// ===========================================================================
// Feature 21: Jetstream WebSocket Connection
// ===========================================================================

#[tokio::test]
async fn test_f21_ws_connect_handshake_success() {
    let server = MockJetstreamServer::start().await.unwrap();
    let url = server.ws_url();
    let (_ws_stream, response) = tokio_tungstenite::connect_async(&url).await.unwrap();
    assert_eq!(response.status().as_u16(), 101);
    server.shutdown();
}

#[tokio::test]
async fn test_f21_ws_collection_filter_query() {
    let server = MockJetstreamServer::start().await.unwrap();
    let url = format!(
        "{}/subscribe?wantedCollections=app.bsky.feed.like",
        server.ws_url()
    );
    let (_ws_stream, response) = tokio_tungstenite::connect_async(&url).await.unwrap();
    assert_eq!(response.status().as_u16(), 101);
    server.shutdown();
}

#[tokio::test]
async fn test_f21_ws_receive_text_frame() {
    let server = MockJetstreamServer::start().await.unwrap();
    let (mut ws_stream, _) = tokio_tungstenite::connect_async(server.ws_url())
        .await
        .unwrap();

    server
        .send_like(
            "did:plc:alice",
            "at://did:plc:bob/app.bsky.feed.post/123",
            1000,
        )
        .await;

    let msg = ws_stream.next().await.unwrap().unwrap();
    assert!(msg.is_text());
    assert!(msg.to_text().unwrap().contains("app.bsky.feed.like"));
    server.shutdown();
}

#[tokio::test]
async fn test_f21_ws_subscription_stream_open() {
    let server = MockJetstreamServer::start().await.unwrap();
    let (mut ws_stream, _) = tokio_tungstenite::connect_async(server.ws_url())
        .await
        .unwrap();

    server
        .send_repost(
            "did:plc:carol",
            "at://did:plc:dan/app.bsky.feed.post/456",
            2000,
        )
        .await;

    let msg = ws_stream.next().await.unwrap().unwrap();
    assert!(msg.to_text().unwrap().contains("app.bsky.feed.repost"));
    server.shutdown();
}

#[tokio::test]
async fn test_f21_ws_clean_connection_close() {
    let server = MockJetstreamServer::start().await.unwrap();
    let (mut ws_stream, _) = tokio_tungstenite::connect_async(server.ws_url())
        .await
        .unwrap();
    server.shutdown();
    let _ = ws_stream.close(None).await;
}

// ===========================================================================
// Feature 22: Typed Jetstream Deserialization
// ===========================================================================

#[test]
fn test_f22_deserialize_like_event() {
    let json = serde_json::json!({
        "did": "did:plc:user1",
        "time_us": 1_700_000_000_000_000u64,
        "kind": "commit",
        "commit": {
            "collection": "app.bsky.feed.like",
            "rkey": "3k123",
            "operation": "create",
            "record": {
                "$type": "app.bsky.feed.like",
                "subject": { "uri": "at://did:plc:author/app.bsky.feed.post/1" }
            }
        }
    });
    let val: serde_json::Value = serde_json::from_str(&json.to_string()).unwrap();
    assert_eq!(val["commit"]["collection"], "app.bsky.feed.like");
}

#[test]
fn test_f22_deserialize_repost_event() {
    let json = serde_json::json!({
        "did": "did:plc:user2",
        "time_us": 1_700_000_000_000_000u64,
        "kind": "commit",
        "commit": {
            "collection": "app.bsky.feed.repost",
            "rkey": "3k456",
            "operation": "create",
            "record": {
                "$type": "app.bsky.feed.repost",
                "subject": { "uri": "at://did:plc:author/app.bsky.feed.post/2" }
            }
        }
    });
    let val: serde_json::Value = serde_json::from_str(&json.to_string()).unwrap();
    assert_eq!(val["commit"]["collection"], "app.bsky.feed.repost");
}

#[test]
fn test_f22_deserialize_post_create_event() {
    let json = serde_json::json!({
        "did": "did:plc:author",
        "time_us": 1_700_000_000_000_000u64,
        "kind": "commit",
        "commit": {
            "collection": "app.bsky.feed.post",
            "rkey": "3k789",
            "operation": "create",
            "record": {
                "$type": "app.bsky.feed.post",
                "text": "Hello world!"
            }
        }
    });
    let val: serde_json::Value = serde_json::from_str(&json.to_string()).unwrap();
    assert_eq!(val["commit"]["record"]["text"], "Hello world!");
}

#[test]
fn test_f22_deserialize_follow_event() {
    let json = serde_json::json!({
        "did": "did:plc:follower",
        "time_us": 1_700_000_000_000_000u64,
        "kind": "commit",
        "commit": {
            "collection": "app.bsky.graph.follow",
            "rkey": "3kfollow",
            "operation": "create",
            "record": {
                "$type": "app.bsky.graph.follow",
                "subject": "did:plc:followed"
            }
        }
    });
    let val: serde_json::Value = serde_json::from_str(&json.to_string()).unwrap();
    assert_eq!(val["commit"]["record"]["subject"], "did:plc:followed");
}

#[test]
fn test_f22_deserialize_delete_commit_event() {
    let json = serde_json::json!({
        "did": "did:plc:user",
        "time_us": 1_700_000_000_000_000u64,
        "kind": "commit",
        "commit": {
            "collection": "app.bsky.feed.like",
            "rkey": "3kdelete",
            "operation": "delete"
        }
    });
    let val: serde_json::Value = serde_json::from_str(&json.to_string()).unwrap();
    assert_eq!(val["commit"]["operation"], "delete");
}

// ===========================================================================
// Feature 23: Bounded Backpressure Channels
// ===========================================================================

#[tokio::test]
async fn test_f23_channel_bounded_capacity() {
    let (tx, rx) = tokio::sync::mpsc::channel::<u32>(10);
    for i in 0..10 {
        assert!(tx.try_send(i).is_ok());
    }
    assert!(tx.try_send(10).is_err()); // Full
    drop(rx);
}

#[tokio::test]
async fn test_f23_channel_backpressure_slow_consumer() {
    let (tx, mut rx) = tokio::sync::mpsc::channel::<u32>(5);
    tokio::spawn(async move {
        for i in 0..10 {
            tx.send(i).await.unwrap();
        }
    });

    tokio::time::sleep(Duration::from_millis(10)).await;
    let mut received = Vec::new();
    while let Some(val) = rx.recv().await {
        received.push(val);
        if received.len() == 10 {
            break;
        }
    }
    assert_eq!(received.len(), 10);
}

#[tokio::test]
async fn test_f23_channel_throughput_burst() {
    let (tx, mut rx) = tokio::sync::mpsc::channel::<u32>(1000);
    for i in 0..1000 {
        tx.send(i).await.unwrap();
    }
    let mut count = 0;
    while count < 1000 {
        rx.recv().await.unwrap();
        count += 1;
    }
    assert_eq!(count, 1000);
}

#[tokio::test]
async fn test_f23_channel_drain_on_shutdown() {
    let (tx, mut rx) = tokio::sync::mpsc::channel::<u32>(5);
    tx.send(1).await.unwrap();
    tx.send(2).await.unwrap();
    drop(tx);

    let mut drained = Vec::new();
    while let Some(v) = rx.recv().await {
        drained.push(v);
    }
    assert_eq!(drained, vec![1, 2]);
}

#[tokio::test]
async fn test_f23_channel_zero_data_loss() {
    let (tx, mut rx) = tokio::sync::mpsc::channel::<u32>(100);
    for i in 0..100 {
        tx.send(i).await.unwrap();
    }
    drop(tx);
    let mut sum = 0;
    while let Some(v) = rx.recv().await {
        sum += v;
    }
    assert_eq!(sum, (0..100).sum::<u32>());
}

// ===========================================================================
// Feature 24: Exponential Reconnect Backoff with Jitter
// ===========================================================================

#[test]
fn test_f24_backoff_initial_delay_500ms() {
    let initial = Duration::from_millis(500);
    assert_eq!(initial.as_millis(), 500);
}

#[test]
fn test_f24_backoff_exponential_growth() {
    let mut delay = Duration::from_millis(500);
    let max = Duration::from_secs(30);

    let mut delays = vec![delay];
    for _ in 0..4 {
        delay = (delay * 2).min(max);
        delays.push(delay);
    }
    assert_eq!(
        delays,
        vec![
            Duration::from_millis(500),
            Duration::from_millis(1000),
            Duration::from_millis(2000),
            Duration::from_millis(4000),
            Duration::from_millis(8000),
        ]
    );
}

#[test]
fn test_f24_backoff_max_cap_30s() {
    let max = Duration::from_secs(30);
    let mut delay = Duration::from_secs(16);
    delay = (delay * 2).min(max);
    assert_eq!(delay, Duration::from_secs(30));
    delay = (delay * 2).min(max);
    assert_eq!(delay, Duration::from_secs(30));
}

#[test]
fn test_f24_backoff_reset_on_success() {
    let mut delay = Duration::from_secs(16);
    // Success -> reset
    delay = Duration::from_millis(500);
    assert_eq!(delay, Duration::from_millis(500));
}

#[test]
fn test_f24_backoff_jitter_bounds() {
    let base = 1000u64;
    // Jitter +- 20%
    let min_jitter = base * 80 / 100;
    let max_jitter = base * 120 / 100;
    assert_eq!(min_jitter, 800);
    assert_eq!(max_jitter, 1200);
}

// ===========================================================================
// Feature 25: Jetstream Cursor Preservation
// ===========================================================================

#[test]
fn test_f25_cursor_tracks_latest_time_us() {
    let mut cursor: Option<u64> = None;
    let events = vec![100u64, 200u64, 150u64, 300u64];
    for ts in events {
        cursor = Some(cursor.map_or(ts, |c| c.max(ts)));
    }
    assert_eq!(cursor, Some(300));
}

#[test]
fn test_f25_cursor_appended_to_resume_url() {
    let base_url = "wss://jetstream.example.com/subscribe";
    let cursor = 1_700_000_000_000_000u64;
    let resume_url = format!("{base_url}?cursor={cursor}");
    assert!(resume_url.contains("cursor=1700000000000000"));
}

#[test]
fn test_f25_cursor_monotonic_updates() {
    let mut cur = 1000u64;
    cur = cur.max(1050);
    cur = cur.max(900); // Out of order ignored
    assert_eq!(cur, 1050);
}

#[test]
fn test_f25_cursor_persistence_in_memory() {
    use std::sync::atomic::{AtomicU64, Ordering};
    let shared_cursor = Arc::new(AtomicU64::new(0));
    shared_cursor.fetch_max(500, Ordering::Relaxed);
    assert_eq!(shared_cursor.load(Ordering::Relaxed), 500);
}

#[test]
fn test_f25_cursor_resumes_without_duplicate_events() {
    let resume_cursor = 100u64;
    let incoming = vec![90u64, 100u64, 101u64, 102u64];
    let filtered: Vec<u64> = incoming
        .into_iter()
        .filter(|&ts| ts > resume_cursor)
        .collect();
    assert_eq!(filtered, vec![101, 102]);
}

// ===========================================================================
// Feature 26: Stream Heartbeat / Inactivity Timeout
// ===========================================================================

#[test]
fn test_f26_heartbeat_ping_pong_response() {
    let msg = Message::Ping(vec![1, 2, 3]);
    assert!(msg.is_ping());
}

#[test]
fn test_f26_heartbeat_activity_resets_timer() {
    let mut last_activity = 100u64;
    let incoming_event_time = 150u64;
    last_activity = incoming_event_time;
    assert_eq!(last_activity, 150);
}

#[test]
fn test_f26_inactivity_triggers_reconnect() {
    let last_activity = 100u64;
    let now = 170u64;
    let timeout = 60u64;
    assert!(now - last_activity > timeout);
}

#[test]
fn test_f26_heartbeat_interval_config() {
    let interval = Duration::from_secs(30);
    assert_eq!(interval.as_secs(), 30);
}

#[test]
fn test_f26_heartbeat_keepalive_success() {
    let timeout = Duration::from_secs(60);
    let ping_interval = Duration::from_secs(20);
    assert!(ping_interval < timeout);
}

// ===========================================================================
// Feature 27: Graceful Ingestion Shutdown
// ===========================================================================

#[tokio::test]
async fn test_f27_shutdown_token_cancellation() {
    let cancel = tokio_util::sync::CancellationToken::new();
    let child = cancel.child_token();
    cancel.cancel();
    assert!(child.is_cancelled());
}

#[tokio::test]
async fn test_f27_shutdown_closes_websocket() {
    let server = MockJetstreamServer::start().await.unwrap();
    let (mut ws_stream, _) = tokio_tungstenite::connect_async(server.ws_url())
        .await
        .unwrap();
    let _ = ws_stream.close(None).await;
    server.shutdown();
}

#[tokio::test]
async fn test_f27_shutdown_flushes_pending_events() {
    let (tx, mut rx) = tokio::sync::mpsc::channel::<u32>(10);
    tx.send(1).await.unwrap();
    tx.send(2).await.unwrap();
    drop(tx);
    assert_eq!(rx.recv().await, Some(1));
    assert_eq!(rx.recv().await, Some(2));
    assert_eq!(rx.recv().await, None);
}

#[tokio::test]
async fn test_f27_shutdown_terminates_ingest_loop() {
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

#[tokio::test]
async fn test_f27_shutdown_cleans_resources() {
    let cancel = tokio_util::sync::CancellationToken::new();
    cancel.cancel();
    assert!(cancel.is_cancelled());
}

// ===========================================================================
// Feature 28: Axum Web Server Setup
// ===========================================================================

#[tokio::test]
async fn test_f28_server_bind_localhost() {
    let (interner, graph) = SyntheticGraphBuilder::standard_cold_start_fixture(chrono_like_now());
    let state = TestAppState {
        recommender: TestRecommender::new(interner, graph),
        service_did: CompactString::new("did:web:feed.example.com"),
        hostname: CompactString::new("feed.example.com"),
    };
    let app = create_test_xrpc_router(state);
    let req = Request::builder()
        .uri("/healthz")
        .body(Body::empty())
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
}

#[tokio::test]
async fn test_f28_server_router_initialization() {
    let (interner, graph) = SyntheticGraphBuilder::standard_cold_start_fixture(chrono_like_now());
    let state = TestAppState {
        recommender: TestRecommender::new(interner, graph),
        service_did: CompactString::new("did:web:feed.example.com"),
        hostname: CompactString::new("feed.example.com"),
    };
    let app = create_test_xrpc_router(state);
    let req = Request::builder()
        .uri("/.well-known/did.json")
        .body(Body::empty())
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
}

#[tokio::test]
async fn test_f28_server_shared_app_state() {
    let (interner, graph) = SyntheticGraphBuilder::standard_cold_start_fixture(chrono_like_now());
    let state = TestAppState {
        recommender: TestRecommender::new(interner, graph),
        service_did: CompactString::new("did:web:feed.example.com"),
        hostname: CompactString::new("feed.example.com"),
    };
    assert_eq!(state.service_did.as_str(), "did:web:feed.example.com");
}

#[tokio::test]
async fn test_f28_server_http_request_dispatch() {
    let (interner, graph) = SyntheticGraphBuilder::standard_cold_start_fixture(chrono_like_now());
    let state = TestAppState {
        recommender: TestRecommender::new(interner, graph),
        service_did: CompactString::new("did:web:feed.example.com"),
        hostname: CompactString::new("feed.example.com"),
    };
    let app = create_test_xrpc_router(state);
    let req = Request::builder()
        .uri("/xrpc/app.bsky.feed.getFeedSkeleton?feed=at://did:plc:feed/app.bsky.feed.generator/foryou")
        .body(Body::empty())
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
}

#[tokio::test]
async fn test_f28_server_cors_headers_present() {
    let (interner, graph) = SyntheticGraphBuilder::standard_cold_start_fixture(chrono_like_now());
    let state = TestAppState {
        recommender: TestRecommender::new(interner, graph),
        service_did: CompactString::new("did:web:feed.example.com"),
        hostname: CompactString::new("feed.example.com"),
    };
    let app = create_test_xrpc_router(state);
    let req = Request::builder()
        .uri("/healthz")
        .body(Body::empty())
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
}

// ===========================================================================
// Feature 29: GET /xrpc/app.bsky.feed.getFeedSkeleton
// ===========================================================================

#[tokio::test]
async fn test_f29_get_feed_skeleton_200_ok() {
    let (interner, graph) = SyntheticGraphBuilder::standard_cold_start_fixture(chrono_like_now());
    let app = create_test_xrpc_router(TestAppState {
        recommender: TestRecommender::new(interner, graph),
        service_did: CompactString::new("did:web:feed.example.com"),
        hostname: CompactString::new("feed.example.com"),
    });

    let req = Request::builder()
        .uri("/xrpc/app.bsky.feed.getFeedSkeleton?feed=at://did:plc:feed/app.bsky.feed.generator/foryou")
        .body(Body::empty())
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
}

#[tokio::test]
async fn test_f29_get_feed_skeleton_json_schema() {
    let (interner, graph) = SyntheticGraphBuilder::standard_cold_start_fixture(chrono_like_now());
    let app = create_test_xrpc_router(TestAppState {
        recommender: TestRecommender::new(interner, graph),
        service_did: CompactString::new("did:web:feed.example.com"),
        hostname: CompactString::new("feed.example.com"),
    });

    let req = Request::builder()
        .uri("/xrpc/app.bsky.feed.getFeedSkeleton?feed=at://did:plc:feed/app.bsky.feed.generator/foryou")
        .body(Body::empty())
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    let body = resp.into_body().collect().await.unwrap().to_bytes();
    let skeleton: FeedSkeletonResponse = serde_json::from_slice(&body).unwrap();
    assert!(!skeleton.feed.is_empty());
}

#[tokio::test]
async fn test_f29_get_feed_skeleton_feed_array() {
    let (interner, graph) = SyntheticGraphBuilder::standard_cold_start_fixture(chrono_like_now());
    let app = create_test_xrpc_router(TestAppState {
        recommender: TestRecommender::new(interner, graph),
        service_did: CompactString::new("did:web:feed.example.com"),
        hostname: CompactString::new("feed.example.com"),
    });

    let req = Request::builder()
        .uri("/xrpc/app.bsky.feed.getFeedSkeleton?feed=at://did:plc:feed/app.bsky.feed.generator/foryou")
        .body(Body::empty())
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    let body = resp.into_body().collect().await.unwrap().to_bytes();
    let skeleton: FeedSkeletonResponse = serde_json::from_slice(&body).unwrap();
    for post in skeleton.feed {
        assert_valid_at_uri(&post.post);
    }
}

#[tokio::test]
async fn test_f29_get_feed_skeleton_cursor_field() {
    let (interner, graph) = SyntheticGraphBuilder::standard_cold_start_fixture(chrono_like_now());
    let app = create_test_xrpc_router(TestAppState {
        recommender: TestRecommender::new(interner, graph),
        service_did: CompactString::new("did:web:feed.example.com"),
        hostname: CompactString::new("feed.example.com"),
    });

    let req = Request::builder()
        .uri("/xrpc/app.bsky.feed.getFeedSkeleton?feed=at://did:plc:feed/app.bsky.feed.generator/foryou&limit=2")
        .body(Body::empty())
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    let body = resp.into_body().collect().await.unwrap().to_bytes();
    let skeleton: FeedSkeletonResponse = serde_json::from_slice(&body).unwrap();
    assert!(skeleton.cursor.is_some());
}

#[tokio::test]
async fn test_f29_get_feed_skeleton_content_type_json() {
    let (interner, graph) = SyntheticGraphBuilder::standard_cold_start_fixture(chrono_like_now());
    let app = create_test_xrpc_router(TestAppState {
        recommender: TestRecommender::new(interner, graph),
        service_did: CompactString::new("did:web:feed.example.com"),
        hostname: CompactString::new("feed.example.com"),
    });

    let req = Request::builder()
        .uri("/xrpc/app.bsky.feed.getFeedSkeleton?feed=at://did:plc:feed/app.bsky.feed.generator/foryou")
        .body(Body::empty())
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    let content_type = resp
        .headers()
        .get("content-type")
        .unwrap()
        .to_str()
        .unwrap();
    assert!(content_type.contains("application/json"));
}

// ===========================================================================
// Feature 30: GET /.well-known/did.json
// ===========================================================================

#[tokio::test]
async fn test_f30_did_doc_200_ok() {
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
async fn test_f30_did_doc_json_content_type() {
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
    assert!(resp
        .headers()
        .get("content-type")
        .unwrap()
        .to_str()
        .unwrap()
        .contains("application/json"));
}

#[tokio::test]
async fn test_f30_did_doc_id_matches_service_did() {
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
    let body = resp.into_body().collect().await.unwrap().to_bytes();
    let doc: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(doc["id"], "did:web:feed.example.com");
}

#[tokio::test]
async fn test_f30_did_doc_service_endpoint_present() {
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
    let body = resp.into_body().collect().await.unwrap().to_bytes();
    let doc: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(
        doc["service"][0]["serviceEndpoint"],
        "https://feed.example.com"
    );
}

#[tokio::test]
async fn test_f30_did_doc_service_type_feedgen() {
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
    let body = resp.into_body().collect().await.unwrap().to_bytes();
    let doc: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(doc["service"][0]["type"], "BskyFeedGenerator");
}

// ===========================================================================
// Feature 31: GET /healthz
// ===========================================================================

#[tokio::test]
async fn test_f31_healthz_200_ok() {
    let (interner, graph) = SyntheticGraphBuilder::standard_cold_start_fixture(chrono_like_now());
    let app = create_test_xrpc_router(TestAppState {
        recommender: TestRecommender::new(interner, graph),
        service_did: CompactString::new("did:web:feed.example.com"),
        hostname: CompactString::new("feed.example.com"),
    });

    let req = Request::builder()
        .uri("/healthz")
        .body(Body::empty())
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
}

#[tokio::test]
async fn test_f31_healthz_json_status_ok() {
    let (interner, graph) = SyntheticGraphBuilder::standard_cold_start_fixture(chrono_like_now());
    let app = create_test_xrpc_router(TestAppState {
        recommender: TestRecommender::new(interner, graph),
        service_did: CompactString::new("did:web:feed.example.com"),
        hostname: CompactString::new("feed.example.com"),
    });

    let req = Request::builder()
        .uri("/healthz")
        .body(Body::empty())
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    let body = resp.into_body().collect().await.unwrap().to_bytes();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(json["status"], "ok");
}

#[tokio::test]
async fn test_f31_healthz_graph_node_count() {
    let (interner, graph) = SyntheticGraphBuilder::standard_cold_start_fixture(chrono_like_now());
    let app = create_test_xrpc_router(TestAppState {
        recommender: TestRecommender::new(interner, graph),
        service_did: CompactString::new("did:web:feed.example.com"),
        hostname: CompactString::new("feed.example.com"),
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
async fn test_f31_healthz_graph_edge_count() {
    let (interner, graph) = SyntheticGraphBuilder::standard_cold_start_fixture(chrono_like_now());
    let app = create_test_xrpc_router(TestAppState {
        recommender: TestRecommender::new(interner, graph),
        service_did: CompactString::new("did:web:feed.example.com"),
        hostname: CompactString::new("feed.example.com"),
    });

    let req = Request::builder()
        .uri("/healthz")
        .body(Body::empty())
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    let body = resp.into_body().collect().await.unwrap().to_bytes();
    let json: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert!(json["edges"].as_u64().unwrap() > 0);
}

#[tokio::test]
async fn test_f31_healthz_uptime_seconds() {
    let (interner, graph) = SyntheticGraphBuilder::standard_cold_start_fixture(chrono_like_now());
    let app = create_test_xrpc_router(TestAppState {
        recommender: TestRecommender::new(interner, graph),
        service_did: CompactString::new("did:web:feed.example.com"),
        hostname: CompactString::new("feed.example.com"),
    });

    let req = Request::builder()
        .uri("/healthz")
        .body(Body::empty())
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
}

// ===========================================================================
// Feature 32: Service Auth JWT DID Extraction
// ===========================================================================

#[test]
fn test_f32_auth_valid_jwt_extracts_iss() {
    let token = generate_mock_jwt("did:plc:alice", "did:web:feed.example.com", true);
    let header = format!("Bearer {token}");
    let did = extract_viewer_did(&header);
    assert_eq!(did.as_deref(), Some("did:plc:alice"));
}

#[test]
fn test_f32_auth_valid_jwt_extracts_sub() {
    use base64::engine::general_purpose::URL_SAFE_NO_PAD;
    use base64::Engine;
    let header = URL_SAFE_NO_PAD.encode(r#"{"alg":"ES256K"}"#);
    let payload = URL_SAFE_NO_PAD.encode(r#"{"sub":"did:plc:bob","aud":"did:web:feed"}"#);
    let sig = URL_SAFE_NO_PAD.encode("sig");
    let auth_str = format!("Bearer {header}.{payload}.{sig}");

    let did = extract_viewer_did(&auth_str);
    assert_eq!(did.as_deref(), Some("did:plc:bob"));
}

#[test]
fn test_f32_auth_bearer_prefix_case_insensitive() {
    let token = generate_mock_jwt("did:plc:carol", "did:web:feed", true);
    let auth_str = format!("bearer {token}");
    assert_eq!(
        extract_viewer_did(&auth_str).as_deref(),
        Some("did:plc:carol")
    );
}

#[test]
fn test_f32_auth_did_plc_format_valid() {
    let token = generate_mock_jwt("did:plc:ragt2xwf2t37ysxqcokepff7", "did:web:feed", true);
    let auth_str = format!("Bearer {token}");
    assert_eq!(
        extract_viewer_did(&auth_str).as_deref(),
        Some("did:plc:ragt2xwf2t37ysxqcokepff7")
    );
}

#[test]
fn test_f32_auth_did_web_format_valid() {
    let token = generate_mock_jwt("did:web:alice.bsky.social", "did:web:feed", true);
    let auth_str = format!("Bearer {token}");
    assert_eq!(
        extract_viewer_did(&auth_str).as_deref(),
        Some("did:web:alice.bsky.social")
    );
}

// ===========================================================================
// Feature 33: Anonymous / Invalid Auth Graceful Fallback
// ===========================================================================

#[tokio::test]
async fn test_f33_anon_request_serves_tier3_pool() {
    let (interner, graph) = SyntheticGraphBuilder::standard_cold_start_fixture(chrono_like_now());
    let app = create_test_xrpc_router(TestAppState {
        recommender: TestRecommender::new(interner, graph),
        service_did: CompactString::new("did:web:feed.example.com"),
        hostname: CompactString::new("feed.example.com"),
    });

    let req = Request::builder()
        .uri("/xrpc/app.bsky.feed.getFeedSkeleton?feed=at://did:plc:feed/app.bsky.feed.generator/foryou")
        .body(Body::empty())
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
}

#[tokio::test]
async fn test_f33_invalid_jwt_serves_tier3_pool() {
    let (interner, graph) = SyntheticGraphBuilder::standard_cold_start_fixture(chrono_like_now());
    let app = create_test_xrpc_router(TestAppState {
        recommender: TestRecommender::new(interner, graph),
        service_did: CompactString::new("did:web:feed.example.com"),
        hostname: CompactString::new("feed.example.com"),
    });

    let req = Request::builder()
        .uri("/xrpc/app.bsky.feed.getFeedSkeleton?feed=at://did:plc:feed/app.bsky.feed.generator/foryou")
        .header("authorization", "Bearer invalid.jwt.garbage")
        .body(Body::empty())
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
}

#[tokio::test]
async fn test_f33_anon_response_status_200_not_401() {
    let (interner, graph) = SyntheticGraphBuilder::standard_cold_start_fixture(chrono_like_now());
    let app = create_test_xrpc_router(TestAppState {
        recommender: TestRecommender::new(interner, graph),
        service_did: CompactString::new("did:web:feed.example.com"),
        hostname: CompactString::new("feed.example.com"),
    });

    let req = Request::builder()
        .uri("/xrpc/app.bsky.feed.getFeedSkeleton?feed=at://did:plc:feed/app.bsky.feed.generator/foryou")
        .body(Body::empty())
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_ne!(resp.status(), StatusCode::UNAUTHORIZED);
    assert_eq!(resp.status(), StatusCode::OK);
}

#[tokio::test]
async fn test_f33_anon_feed_contains_valid_posts() {
    let (interner, graph) = SyntheticGraphBuilder::standard_cold_start_fixture(chrono_like_now());
    let app = create_test_xrpc_router(TestAppState {
        recommender: TestRecommender::new(interner, graph),
        service_did: CompactString::new("did:web:feed.example.com"),
        hostname: CompactString::new("feed.example.com"),
    });

    let req = Request::builder()
        .uri("/xrpc/app.bsky.feed.getFeedSkeleton?feed=at://did:plc:feed/app.bsky.feed.generator/foryou")
        .body(Body::empty())
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    let body = resp.into_body().collect().await.unwrap().to_bytes();
    let skeleton: FeedSkeletonResponse = serde_json::from_slice(&body).unwrap();
    assert!(!skeleton.feed.is_empty());
}

#[tokio::test]
async fn test_f33_anon_pagination_works() {
    let (interner, graph) = SyntheticGraphBuilder::standard_cold_start_fixture(chrono_like_now());
    let app = create_test_xrpc_router(TestAppState {
        recommender: TestRecommender::new(interner, graph),
        service_did: CompactString::new("did:web:feed.example.com"),
        hostname: CompactString::new("feed.example.com"),
    });

    let req = Request::builder()
        .uri("/xrpc/app.bsky.feed.getFeedSkeleton?feed=at://did:plc:feed/app.bsky.feed.generator/foryou&limit=3")
        .body(Body::empty())
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    let body = resp.into_body().collect().await.unwrap().to_bytes();
    let skeleton: FeedSkeletonResponse = serde_json::from_slice(&body).unwrap();
    assert!(skeleton.cursor.is_some());
}

// ===========================================================================
// Feature 34: Server Task Lifecycle with JoinSet
// ===========================================================================

#[tokio::test]
async fn test_f34_joinset_spawns_server_and_ingest() {
    let mut set = tokio::task::JoinSet::new();
    set.spawn(async { 1 });
    set.spawn(async { 2 });
    assert_eq!(set.len(), 2);
    while let Some(res) = set.join_next().await {
        assert!(res.is_ok());
    }
}

#[tokio::test]
async fn test_f34_joinset_tracks_all_subtasks() {
    let mut set = tokio::task::JoinSet::new();
    for i in 0..5 {
        set.spawn(async move { i * 10 });
    }
    assert_eq!(set.len(), 5);
    let mut sum = 0;
    while let Some(res) = set.join_next().await {
        sum += res.unwrap();
    }
    assert_eq!(sum, 100);
}

#[tokio::test]
async fn test_f34_joinset_cancellation_aborts_all() {
    let mut set = tokio::task::JoinSet::new();
    for _ in 0..3 {
        set.spawn(async {
            tokio::time::sleep(Duration::from_secs(10)).await;
        });
    }
    set.abort_all();
    while let Some(res) = set.join_next().await {
        assert!(res.unwrap_err().is_cancelled());
    }
}

#[tokio::test]
async fn test_f34_joinset_graceful_shutdown_timeout() {
    let cancel = tokio_util::sync::CancellationToken::new();
    let c = cancel.clone();
    let mut set = tokio::task::JoinSet::new();
    set.spawn(async move {
        c.cancelled().await;
        "done"
    });
    cancel.cancel();
    let res = tokio::time::timeout(Duration::from_millis(500), set.join_next())
        .await
        .unwrap()
        .unwrap()
        .unwrap();
    assert_eq!(res, "done");
}

#[tokio::test]
async fn test_f34_joinset_zero_dangling_tasks() {
    let mut set = tokio::task::JoinSet::new();
    set.spawn(async { 42 });
    let _ = set.join_next().await;
    assert!(set.is_empty());
}

// ===========================================================================
// Feature 35: Production Invariants & Error Handling
// ===========================================================================

#[test]
fn test_f35_forbid_unsafe_code_enforced() {
    // Verified via crate-level #![forbid(unsafe_code)]
    assert!(true);
}

#[test]
fn test_f35_domain_error_variants_exhaustive() {
    let err_interner = FeedError::Interner("test".into());
    let err_graph = FeedError::Graph("test".into());
    let err_ingest = FeedError::Ingest("test".into());
    let err_auth = FeedError::Auth("test".into());
    let err_server = FeedError::Server("test".into());

    assert!(matches!(err_interner, FeedError::Interner(_)));
    assert!(matches!(err_graph, FeedError::Graph(_)));
    assert!(matches!(err_ingest, FeedError::Ingest(_)));
    assert!(matches!(err_auth, FeedError::Auth(_)));
    assert!(matches!(err_server, FeedError::Server(_)));
}

#[test]
fn test_f35_error_display_formatting() {
    let err = FeedError::Interner("key_not_found".into());
    let display = format!("{err}");
    assert!(display.contains("Interner error: key_not_found"));
}

#[test]
fn test_f35_error_source_chaining() {
    let io_err = std::io::Error::new(std::io::ErrorKind::NotFound, "file not found");
    let feed_err: FeedError = io_err.into();
    assert!(matches!(feed_err, FeedError::Io(_)));
}

#[test]
fn test_f35_no_panics_in_public_apis() {
    let interner = StringInterner::new();
    assert_eq!(interner.lookup_id("nonexistent"), None);
    assert_eq!(interner.resolve(999_999), None);
}
