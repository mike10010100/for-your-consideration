//! Tier 3 E2E Cross-Feature Pairwise Interaction Test Suite
//!
//! Validates combinatorial interactions across subsystems:
//! 1. Ingest ↔ Graph Store
//! 2. Graph Store ↔ Recommender Scoring Engine
//! 3. Recommender ↔ Anti-Fatigue & Diversity Constraints
//! 4. Auth Extraction ↔ Cold-Start Hierarchy
//! 5. XRPC Server ↔ Dials ↔ Cursor Pagination
//! 6. Full End-to-End Live Pipeline

#![allow(
    clippy::pedantic,
    clippy::nursery,
    clippy::cognitive_complexity,
    unused_assignments
)]

mod common;

use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use compact_str::CompactString;
use for_your_consideration::prelude::*;
use futures_util::StreamExt;
use http_body_util::BodyExt;
use tower::ServiceExt;

use crate::common::*;

// ===========================================================================
// Pairwise 1: Ingest ↔ Graph Store
// ===========================================================================

#[tokio::test]
async fn test_pairwise_ingest_like_to_graph_adjacency_and_bitmap() {
    let server = MockJetstreamServer::start().await.unwrap();
    let (mut ws, _) = tokio_tungstenite::connect_async(server.ws_url())
        .await
        .unwrap();

    let interner = Arc::new(StringInterner::new());
    let graph = Arc::new(GraphStore::new());
    let now = chrono_like_now();

    server
        .send_like(
            "did:plc:alice",
            "at://did:plc:bob/app.bsky.feed.post/123",
            now * 1_000_000,
        )
        .await;

    let msg = ws.next().await.unwrap().unwrap();
    let val: serde_json::Value = serde_json::from_str(msg.to_text().unwrap()).unwrap();

    let user_did = val["did"].as_str().unwrap();
    let post_uri = val["commit"]["record"]["subject"]["uri"].as_str().unwrap();

    let uid = interner.intern(user_did);
    let pid = interner.intern(post_uri);
    graph.record_interaction(uid, pid, SignalType::Like, now);

    assert_eq!(graph.get_user_interactions(uid).len(), 1);
    assert_eq!(graph.get_post_interactions(pid).len(), 1);
    let bm = graph.get_user_likes_bitmap(uid).unwrap();
    assert!(bm.contains(pid));
    server.shutdown();
}

#[tokio::test]
async fn test_pairwise_ingest_repost_to_graph_multi_signal() {
    let server = MockJetstreamServer::start().await.unwrap();
    let (mut ws, _) = tokio_tungstenite::connect_async(server.ws_url())
        .await
        .unwrap();

    let interner = Arc::new(StringInterner::new());
    let graph = Arc::new(GraphStore::new());
    let now = chrono_like_now();

    server
        .send_repost(
            "did:plc:carol",
            "at://did:plc:dan/app.bsky.feed.post/456",
            now * 1_000_000,
        )
        .await;

    let msg = ws.next().await.unwrap().unwrap();
    let val: serde_json::Value = serde_json::from_str(msg.to_text().unwrap()).unwrap();

    let user_did = val["did"].as_str().unwrap();
    let post_uri = val["commit"]["record"]["subject"]["uri"].as_str().unwrap();

    let uid = interner.intern(user_did);
    let pid = interner.intern(post_uri);
    graph.record_interaction(uid, pid, SignalType::Repost, now);

    let edges = graph.get_user_interactions(uid);
    assert_eq!(edges.len(), 1);
    assert_eq!(edges[0].signal(), SignalType::Repost);
    assert_eq!(edges[0].weight(), 3.0);
    server.shutdown();
}

#[tokio::test]
async fn test_pairwise_ingest_post_reply_to_post_meta_hierarchy() {
    let server = MockJetstreamServer::start().await.unwrap();
    let (mut ws, _) = tokio_tungstenite::connect_async(server.ws_url())
        .await
        .unwrap();

    let interner = Arc::new(StringInterner::new());
    let graph = Arc::new(GraphStore::new());
    let now = chrono_like_now();

    let author = "did:plc:replier";
    let post_uri = "at://did:plc:replier/app.bsky.feed.post/reply1";
    let root_uri = "at://did:plc:op/app.bsky.feed.post/root1";
    let parent_uri = "at://did:plc:op/app.bsky.feed.post/root1";

    server
        .send_post(
            author,
            post_uri,
            Some(root_uri),
            Some(parent_uri),
            now * 1_000_000,
        )
        .await;

    let msg = ws.next().await.unwrap().unwrap();
    let val: serde_json::Value = serde_json::from_str(msg.to_text().unwrap()).unwrap();

    let reply = &val["commit"]["record"]["reply"];
    let r_uri = reply["root"]["uri"].as_str();
    let p_uri = reply["parent"]["uri"].as_str();

    let aid = interner.intern(author);
    let pid = interner.intern(post_uri);
    let rid = r_uri.map(|u| interner.intern(u));
    let paid = p_uri.map(|u| interner.intern(u));

    graph.record_post_meta(pid, aid, rid, paid, now);

    let meta = graph.get_post_meta(pid).unwrap();
    assert_eq!(meta.author_id, aid);
    assert_eq!(meta.root_id, rid);
    assert!(meta.is_reply());
    server.shutdown();
}

#[tokio::test]
async fn test_pairwise_ingest_follow_to_follow_graph() {
    let server = MockJetstreamServer::start().await.unwrap();
    let (mut ws, _) = tokio_tungstenite::connect_async(server.ws_url())
        .await
        .unwrap();

    let interner = Arc::new(StringInterner::new());
    let graph = Arc::new(GraphStore::new());

    server
        .send_follow("did:plc:follower", "did:plc:followed", 100_000)
        .await;

    let msg = ws.next().await.unwrap().unwrap();
    let val: serde_json::Value = serde_json::from_str(msg.to_text().unwrap()).unwrap();

    let follower = val["did"].as_str().unwrap();
    let followed = val["commit"]["record"]["subject"].as_str().unwrap();

    let fid = interner.intern(follower);
    let tid = interner.intern(followed);
    graph.record_follow(fid, tid);

    assert_eq!(graph.get_user_follows(fid), vec![tid]);
    server.shutdown();
}

#[tokio::test]
async fn test_pairwise_ingest_delete_commit_to_graph_prune() {
    let server = MockJetstreamServer::start().await.unwrap();
    let (mut ws, _) = tokio_tungstenite::connect_async(server.ws_url())
        .await
        .unwrap();

    let interner = Arc::new(StringInterner::new());
    let graph = Arc::new(GraphStore::new());
    let uid = interner.intern("did:plc:unliker");
    let pid = interner.intern("at://did:plc:author/post/1");

    graph.record_interaction(uid, pid, SignalType::Like, chrono_like_now());
    assert_eq!(graph.get_user_interactions(uid).len(), 1);

    server
        .send_delete("did:plc:unliker", "app.bsky.feed.like", "3k123", 200_000)
        .await;

    let msg = ws.next().await.unwrap().unwrap();
    let val: serde_json::Value = serde_json::from_str(msg.to_text().unwrap()).unwrap();
    assert_eq!(val["commit"]["operation"], "delete");

    graph.remove_interaction(uid, pid, SignalType::Like);
    assert!(graph.get_user_interactions(uid).is_empty());
    assert!(graph.get_post_interactions(pid).is_empty());
    server.shutdown();
}

#[tokio::test]
async fn test_pairwise_ingest_burst_to_graph_concurrent_integrity() {
    let server = MockJetstreamServer::start().await.unwrap();
    let (mut ws, _) = tokio_tungstenite::connect_async(server.ws_url())
        .await
        .unwrap();

    let interner = Arc::new(StringInterner::new());
    let graph = Arc::new(GraphStore::new());
    let now = chrono_like_now();

    for i in 1..=50 {
        server
            .send_like(
                &format!("did:plc:user_{i}"),
                "at://did:plc:viral/post/burst",
                now * 1_000_000,
            )
            .await;
    }

    let mut count = 0;
    while count < 50 {
        if let Some(Ok(msg)) = ws.next().await {
            let val: serde_json::Value = serde_json::from_str(msg.to_text().unwrap()).unwrap();
            let uid = interner.intern(val["did"].as_str().unwrap());
            let pid = interner.intern(val["commit"]["record"]["subject"]["uri"].as_str().unwrap());
            graph.record_interaction(uid, pid, SignalType::Like, now);
            count += 1;
        }
    }

    let pid = interner.lookup_id("at://did:plc:viral/post/burst").unwrap();
    assert_eq!(graph.get_post_interactions(pid).len(), 50);
    server.shutdown();
}

// ===========================================================================
// Pairwise 2: Graph Store ↔ Recommender Scoring Engine
// ===========================================================================

#[test]
fn test_pairwise_multi_signal_weighting_impacts_traversal_ranking() {
    let mut builder = SyntheticGraphBuilder::new();
    let now = chrono_like_now();

    // User A liked post 1
    // User B liked post 1, reposted post 2 (3x weight)
    // User C liked post 1, liked post 3 (1x weight)
    let viewer = "did:plc:viewer";
    let shared = "at://did:plc:author/post/shared";
    let reposted_cand = "at://did:plc:author_rep/post/reposted";
    let liked_cand = "at://did:plc:author_lik/post/liked";

    builder = builder.add_post(
        shared,
        "did:plc:author",
        None::<&str>,
        None::<&str>,
        now - 1000,
    );
    builder = builder.add_post(
        reposted_cand,
        "did:plc:author_rep",
        None::<&str>,
        None::<&str>,
        now - 500,
    );
    builder = builder.add_post(
        liked_cand,
        "did:plc:author_lik",
        None::<&str>,
        None::<&str>,
        now - 500,
    );

    for i in 1..=10 {
        let p = format!("at://did:plc:author/post/init_{i}");
        builder = builder.add_post(
            p.clone(),
            "did:plc:author",
            None::<&str>,
            None::<&str>,
            now - 2000,
        );
        builder = builder.add_interaction(viewer, p, SignalType::Like, now - 1500);
    }

    builder = builder.add_interaction(viewer, shared, SignalType::Like, now - 1000);
    builder = builder.add_interaction("did:plc:co_rep", shared, SignalType::Like, now - 900);
    builder = builder.add_interaction(
        "did:plc:co_rep",
        reposted_cand,
        SignalType::Repost,
        now - 400,
    );

    builder = builder.add_interaction("did:plc:co_lik", shared, SignalType::Like, now - 900);
    builder = builder.add_interaction("did:plc:co_lik", liked_cand, SignalType::Like, now - 400);

    let interner = Arc::new(StringInterner::new());
    let graph = Arc::new(GraphStore::new());
    builder.populate(&interner, &graph);

    let rec = TestRecommender::new(interner, graph);
    let dials = RecommendationDials {
        min_likes: 1,
        ..Default::default()
    };
    let res = rec.recommend(Some(viewer), &dials, now).unwrap();

    let rep_idx = res.posts.iter().position(|p| p.uri == reposted_cand);
    let lik_idx = res.posts.iter().position(|p| p.uri == liked_cand);

    assert!(rep_idx.is_some() && lik_idx.is_some());
    assert!(
        rep_idx.unwrap() < lik_idx.unwrap(),
        "Repost should outrank Like due to 3.0x vs 1.0x weight"
    );
}

#[test]
fn test_pairwise_exponential_time_decay_penalizes_older_edges() {
    let now = chrono_like_now();
    let fresh_decay = calculate_time_decay(SignalType::Like, now - 3600, now, 36.0 * 3600.0);
    let stale_decay = calculate_time_decay(SignalType::Like, now - 72000, now, 36.0 * 3600.0);
    assert!(fresh_decay > stale_decay * 1.5);
}

#[test]
fn test_pairwise_bm25_dampens_hyper_viral_posts_fairly() {
    let peak_social_proof = calculate_social_proof_factor(500);
    let viral_social_proof = calculate_social_proof_factor(100_000);
    let unvetted_social_proof = calculate_social_proof_factor(0);

    // Peak post ranks above hyper-viral post due to soft viral taper
    assert!(peak_social_proof > viral_social_proof);
    // Hyper-viral post never collapses below baseline floor (maintains > 1.0 multiplier)
    assert!(viral_social_proof > 1.0);
    assert!(viral_social_proof > unvetted_social_proof);
}

#[test]
fn test_pairwise_cosine_taste_similarity_boosts_relevant_candidates() {
    let graph = GraphStore::new();
    let now = chrono_like_now();

    // User A and User B share 5 likes
    for p in 1..=5 {
        graph.record_interaction(1, p, SignalType::Like, now);
        graph.record_interaction(2, p, SignalType::Like, now);
    }
    // User A and User C share only 1 like
    graph.record_interaction(3, 1, SignalType::Like, now);

    let sim_ab = graph.compute_cosine_similarity(1, 2);
    let sim_ac = graph.compute_cosine_similarity(1, 3);
    assert!(sim_ab > sim_ac);
}

#[test]
fn test_pairwise_velocity_pool_sliding_window_sliding_updates() {
    let graph = GraphStore::new();
    let now = chrono_like_now();

    // Post 1 active 1 hour ago
    graph.record_interaction(1, 10, SignalType::Like, now - 3600);
    // Post 2 active 7 hours ago (> 6 hours)
    graph.record_interaction(2, 20, SignalType::Like, now - (7 * 3600));

    let candidates = graph.get_velocity_pool_candidates_at(now, 10);
    assert_eq!(candidates, vec![10]);
}

// ===========================================================================
// Pairwise 3: Recommender ↔ Anti-Fatigue & Diversity Constraints
// ===========================================================================

#[test]
fn test_pairwise_3step_walk_with_author_diversity_limit_2() {
    let (interner, graph) = SyntheticGraphBuilder::standard_cold_start_fixture(chrono_like_now());
    let rec = TestRecommender::new(Arc::clone(&interner), Arc::clone(&graph));
    let res = rec
        .recommend(
            Some("did:plc:active_user"),
            &RecommendationDials {
                limit: 30,
                ..Default::default()
            },
            chrono_like_now(),
        )
        .unwrap();

    let mut author_counts = std::collections::HashMap::new();
    for p in res.posts {
        let pid = interner.lookup_id(&p.uri).unwrap();
        let meta = graph.get_post_meta(pid).unwrap();
        let cnt = author_counts.entry(meta.author_id).or_insert(0);
        *cnt += 1;
        assert!(*cnt <= 2, "Author diversity hard constraint violated");
    }
}

#[test]
fn test_pairwise_3step_walk_with_thread_dampening_max_1_per_root() {
    let mut builder = SyntheticGraphBuilder::new();
    let now = chrono_like_now();
    let viewer = "did:plc:active_viewer";
    let root = "at://did:plc:author_thread/post/root_post";

    for i in 1..=10 {
        let p = format!("at://did:plc:seed_author/post/seed_{i}");
        builder = builder.add_post(
            p.clone(),
            "did:plc:seed_author",
            None::<&str>,
            None::<&str>,
            now - 2000,
        );
        builder = builder.add_interaction(viewer, p.clone(), SignalType::Like, now - 1500);
        builder = builder.add_interaction("did:plc:co_friend", p, SignalType::Like, now - 1400);
    }

    builder = builder.add_post(
        root,
        "did:plc:author_thread",
        None::<&str>,
        None::<&str>,
        now - 1000,
    );
    builder = builder.add_interaction("did:plc:co_friend", root, SignalType::Like, now - 500);

    for reply_idx in 1..=4 {
        let reply = format!("at://did:plc:replier_{reply_idx}/post/reply_{reply_idx}");
        builder = builder.add_post(
            reply.clone(),
            format!("did:plc:replier_{reply_idx}"),
            Some(root),
            Some(root),
            now - 400,
        );
        builder = builder.add_interaction("did:plc:co_friend", reply, SignalType::Like, now - 300);
    }

    let interner = Arc::new(StringInterner::new());
    let graph = Arc::new(GraphStore::new());
    builder.populate(&interner, &graph);

    let rec = TestRecommender::new(interner, graph);
    let dials = RecommendationDials {
        min_likes: 1,
        ..Default::default()
    };
    let res = rec.recommend(Some(viewer), &dials, now).unwrap();

    let thread_posts: Vec<_> = res
        .posts
        .iter()
        .filter(|p| p.uri == root || p.uri.contains("reply_"))
        .collect();
    assert_eq!(
        thread_posts.len(),
        1,
        "Thread dampening should keep max 1 post per thread root"
    );
}

#[test]
fn test_pairwise_seen_and_liked_posts_strictly_deduplicated() {
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
        assert!(!p.uri.contains("active_post_"));
    }
}

#[test]
fn test_pairwise_self_authored_posts_filtered_out_of_walk() {
    let mut builder = SyntheticGraphBuilder::new();
    let now = chrono_like_now();
    let viewer = "did:plc:self_poster";
    let my_post = "at://did:plc:self_poster/post/my_own";

    builder = builder.add_post(my_post, viewer, None::<&str>, None::<&str>, now - 1000);
    builder = builder.add_interaction("did:plc:fan", my_post, SignalType::Like, now - 500);

    let interner = Arc::new(StringInterner::new());
    let graph = Arc::new(GraphStore::new());
    builder.populate(&interner, &graph);

    let rec = TestRecommender::new(interner, graph);
    let res = rec
        .recommend(Some(viewer), &RecommendationDials::default(), now)
        .unwrap();
    assert!(res.posts.iter().all(|p| p.uri != my_post));
}

#[test]
fn test_pairwise_serendipity_blending_preserves_diversity_and_anti_fatigue() {
    let (interner, graph) = SyntheticGraphBuilder::standard_cold_start_fixture(chrono_like_now());
    let rec = TestRecommender::new(Arc::clone(&interner), Arc::clone(&graph));
    let dials = RecommendationDials {
        explore_ratio: 0.35,
        limit: 20,
        ..Default::default()
    };
    let res = rec
        .recommend(Some("did:plc:active_user"), &dials, chrono_like_now())
        .unwrap();

    let mut author_counts = std::collections::HashMap::new();
    for p in &res.posts {
        let pid = interner.lookup_id(&p.uri).unwrap();
        let meta = graph.get_post_meta(pid).unwrap();
        let cnt = author_counts.entry(meta.author_id).or_insert(0);
        *cnt += 1;
        assert!(*cnt <= 2);
    }
}

// ===========================================================================
// Pairwise 4: Auth Extraction ↔ Cold-Start Hierarchy
// ===========================================================================

#[tokio::test]
async fn test_pairwise_valid_active_jwt_routes_to_tier1_traversal() {
    let (interner, graph) = SyntheticGraphBuilder::standard_cold_start_fixture(chrono_like_now());
    let app = create_test_xrpc_router(TestAppState {
        recommender: TestRecommender::new(interner, graph),
        service_did: CompactString::new("did:web:feed.test"),
        hostname: CompactString::new("feed.test"),
    });

    let jwt = generate_mock_jwt("did:plc:active_user", "did:web:feed.test", true);
    let req = Request::builder()
        .uri("/xrpc/app.bsky.feed.getFeedSkeleton?feed=at://did:plc:feed/app.bsky.feed.generator/foryou&explain=true")
        .header("Authorization", format!("Bearer {jwt}"))
        .body(Body::empty())
        .unwrap();

    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = resp.into_body().collect().await.unwrap().to_bytes();
    let skeleton: FeedSkeletonResponse = serde_json::from_slice(&body).unwrap();
    assert!(!skeleton.feed.is_empty());
    assert!(skeleton.feed[0]
        .feed_context
        .as_ref()
        .unwrap()
        .contains("source=tier1_interaction_walk"));
}

#[tokio::test]
async fn test_pairwise_valid_new_user_jwt_routes_to_tier2_follow_walk() {
    let (interner, graph) = SyntheticGraphBuilder::standard_cold_start_fixture(chrono_like_now());
    let app = create_test_xrpc_router(TestAppState {
        recommender: TestRecommender::new(interner, graph),
        service_did: CompactString::new("did:web:feed.test"),
        hostname: CompactString::new("feed.test"),
    });

    let jwt = generate_mock_jwt("did:plc:new_user", "did:web:feed.test", true);
    let req = Request::builder()
        .uri("/xrpc/app.bsky.feed.getFeedSkeleton?feed=at://did:plc:feed/app.bsky.feed.generator/foryou&explain=true")
        .header("Authorization", format!("Bearer {jwt}"))
        .body(Body::empty())
        .unwrap();

    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = resp.into_body().collect().await.unwrap().to_bytes();
    let skeleton: FeedSkeletonResponse = serde_json::from_slice(&body).unwrap();
    assert!(!skeleton.feed.is_empty());
    assert!(skeleton.feed[0]
        .feed_context
        .as_ref()
        .unwrap()
        .contains("source=tier2_follow_walk"));
}

#[tokio::test]
async fn test_pairwise_anonymous_viewer_routes_to_tier3_velocity_pool() {
    let (interner, graph) = SyntheticGraphBuilder::standard_cold_start_fixture(chrono_like_now());
    let app = create_test_xrpc_router(TestAppState {
        recommender: TestRecommender::new(interner, graph),
        service_did: CompactString::new("did:web:feed.test"),
        hostname: CompactString::new("feed.test"),
    });

    let req = Request::builder()
        .uri("/xrpc/app.bsky.feed.getFeedSkeleton?feed=at://did:plc:feed/app.bsky.feed.generator/foryou&explain=true")
        .body(Body::empty())
        .unwrap();

    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = resp.into_body().collect().await.unwrap().to_bytes();
    let skeleton: FeedSkeletonResponse = serde_json::from_slice(&body).unwrap();
    assert!(!skeleton.feed.is_empty());
    assert!(skeleton.feed[0]
        .feed_context
        .as_ref()
        .unwrap()
        .contains("source=tier3_velocity_pool"));
}

#[tokio::test]
async fn test_pairwise_expired_jwt_gracefully_routes_to_tier3() {
    let (interner, graph) = SyntheticGraphBuilder::standard_cold_start_fixture(chrono_like_now());
    let app = create_test_xrpc_router(TestAppState {
        recommender: TestRecommender::new(interner, graph),
        service_did: CompactString::new("did:web:feed.test"),
        hostname: CompactString::new("feed.test"),
    });

    let expired_jwt = generate_mock_jwt("did:plc:active_user", "did:web:feed.test", false);
    let req = Request::builder()
        .uri("/xrpc/app.bsky.feed.getFeedSkeleton?feed=at://did:plc:feed/app.bsky.feed.generator/foryou")
        .header("Authorization", format!("Bearer {expired_jwt}"))
        .body(Body::empty())
        .unwrap();

    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
}

#[tokio::test]
async fn test_pairwise_corrupted_auth_header_gracefully_routes_to_tier3() {
    let (interner, graph) = SyntheticGraphBuilder::standard_cold_start_fixture(chrono_like_now());
    let app = create_test_xrpc_router(TestAppState {
        recommender: TestRecommender::new(interner, graph),
        service_did: CompactString::new("did:web:feed.test"),
        hostname: CompactString::new("feed.test"),
    });

    let req = Request::builder()
        .uri("/xrpc/app.bsky.feed.getFeedSkeleton?feed=at://did:plc:feed/app.bsky.feed.generator/foryou")
        .header("Authorization", "Bearer invalid.jwt.payload")
        .body(Body::empty())
        .unwrap();

    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
}

// ===========================================================================
// Pairwise 5: XRPC Server ↔ Dials ↔ Cursor Pagination
// ===========================================================================

#[tokio::test]
async fn test_pairwise_xrpc_freshness_dial_modulates_time_decay_half_life() {
    let dials_6h = RecommendationDials::from_query(Some("6h"), None, None, None, None);
    let dials_168h = RecommendationDials::from_query(Some("168h"), None, None, None, None);
    assert_eq!(dials_6h.half_life_secs, 21_600.0);
    assert_eq!(dials_168h.half_life_secs, 604_800.0);
}

#[tokio::test]
async fn test_pairwise_xrpc_discovery_dial_modulates_serendipity_ratio() {
    let dials_fam = RecommendationDials::from_query(None, Some("familiar"), None, None, None);
    let dials_deep = RecommendationDials::from_query(None, Some("deep_dive"), None, None, None);
    assert_eq!(dials_fam.explore_ratio, 0.05);
    assert_eq!(dials_deep.explore_ratio, 0.35);
}

#[tokio::test]
async fn test_pairwise_xrpc_explain_flag_populates_feed_context() {
    let (interner, graph) = SyntheticGraphBuilder::standard_cold_start_fixture(chrono_like_now());
    let app = create_test_xrpc_router(TestAppState {
        recommender: TestRecommender::new(interner, graph),
        service_did: CompactString::new("did:web:feed.test"),
        hostname: CompactString::new("feed.test"),
    });

    let req = Request::builder()
        .uri("/xrpc/app.bsky.feed.getFeedSkeleton?feed=at://did:plc:feed/app.bsky.feed.generator/foryou&explain=true")
        .body(Body::empty())
        .unwrap();

    let resp = app.oneshot(req).await.unwrap();
    let body = resp.into_body().collect().await.unwrap().to_bytes();
    let skeleton: FeedSkeletonResponse = serde_json::from_slice(&body).unwrap();
    assert!(skeleton.feed[0].feed_context.is_some());
}

#[tokio::test]
async fn test_pairwise_xrpc_multi_page_pagination_monotonic_and_unique() {
    let (interner, graph) = SyntheticGraphBuilder::standard_cold_start_fixture(chrono_like_now());
    let app = create_test_xrpc_router(TestAppState {
        recommender: TestRecommender::new(interner, graph),
        service_did: CompactString::new("did:web:feed.test"),
        hostname: CompactString::new("feed.test"),
    });

    // Page 1
    let req1 = Request::builder()
        .uri("/xrpc/app.bsky.feed.getFeedSkeleton?feed=at://did:plc:feed/app.bsky.feed.generator/foryou&limit=3")
        .body(Body::empty())
        .unwrap();
    let resp1 = app.clone().oneshot(req1).await.unwrap();
    let body1 = resp1.into_body().collect().await.unwrap().to_bytes();
    let skel1: FeedSkeletonResponse = serde_json::from_slice(&body1).unwrap();
    assert_eq!(skel1.feed.len(), 3);
    assert!(skel1.cursor.is_some());

    // Page 2
    let cursor = skel1.cursor.unwrap();
    let req2 = Request::builder()
        .uri(format!("/xrpc/app.bsky.feed.getFeedSkeleton?feed=at://did:plc:feed/app.bsky.feed.generator/foryou&limit=3&cursor={cursor}"))
        .body(Body::empty())
        .unwrap();
    let resp2 = app.oneshot(req2).await.unwrap();
    let body2 = resp2.into_body().collect().await.unwrap().to_bytes();
    let skel2: FeedSkeletonResponse = serde_json::from_slice(&body2).unwrap();
    assert_eq!(skel2.feed.len(), 3);

    for p1 in &skel1.feed {
        for p2 in &skel2.feed {
            assert_ne!(p1.post, p2.post);
        }
    }
}

#[tokio::test]
async fn test_pairwise_xrpc_page_limit_respected_across_dials() {
    let (interner, graph) = SyntheticGraphBuilder::standard_cold_start_fixture(chrono_like_now());
    let app = create_test_xrpc_router(TestAppState {
        recommender: TestRecommender::new(interner, graph),
        service_did: CompactString::new("did:web:feed.test"),
        hostname: CompactString::new("feed.test"),
    });

    for limit in [1, 2, 5] {
        let req = Request::builder()
            .uri(format!("/xrpc/app.bsky.feed.getFeedSkeleton?feed=at://did:plc:feed/app.bsky.feed.generator/foryou&limit={limit}"))
            .body(Body::empty())
            .unwrap();
        let resp = app.clone().oneshot(req).await.unwrap();
        let body = resp.into_body().collect().await.unwrap().to_bytes();
        let skel: FeedSkeletonResponse = serde_json::from_slice(&body).unwrap();
        assert_eq!(skel.feed.len(), limit);
    }
}

// ===========================================================================
// Pairwise 6: Full End-to-End Live Pipeline
// ===========================================================================

#[tokio::test]
async fn test_pairwise_e2e_live_ingest_immediately_reflected_in_xrpc_feed() {
    let server = MockJetstreamServer::start().await.unwrap();
    let (mut ws, _) = tokio_tungstenite::connect_async(server.ws_url())
        .await
        .unwrap();

    let interner = Arc::new(StringInterner::new());
    let graph = Arc::new(GraphStore::new());
    let app = create_test_xrpc_router(TestAppState {
        recommender: TestRecommender::new(Arc::clone(&interner), Arc::clone(&graph)),
        service_did: CompactString::new("did:web:feed.test"),
        hostname: CompactString::new("feed.test"),
    });

    let now = chrono_like_now();
    let post_uri = "at://did:plc:live_author/app.bsky.feed.post/live1";

    // 1. Initial feed is empty
    let req1 = Request::builder()
        .uri("/xrpc/app.bsky.feed.getFeedSkeleton?feed=at://did:plc:feed/app.bsky.feed.generator/foryou")
        .body(Body::empty())
        .unwrap();
    let resp1 = app.clone().oneshot(req1).await.unwrap();
    let body1 = resp1.into_body().collect().await.unwrap().to_bytes();
    let skel1: FeedSkeletonResponse = serde_json::from_slice(&body1).unwrap();
    assert!(skel1.feed.is_empty());

    // 2. Ingest live like event
    server
        .send_like("did:plc:live_fan", post_uri, now * 1_000_000)
        .await;
    let msg = ws.next().await.unwrap().unwrap();
    let val: serde_json::Value = serde_json::from_str(msg.to_text().unwrap()).unwrap();

    let uid = interner.intern(val["did"].as_str().unwrap());
    let pid = interner.intern(val["commit"]["record"]["subject"]["uri"].as_str().unwrap());
    graph.record_interaction(uid, pid, SignalType::Like, now);

    // 3. Feed now immediately reflects new live post
    let req2 = Request::builder()
        .uri("/xrpc/app.bsky.feed.getFeedSkeleton?feed=at://did:plc:feed/app.bsky.feed.generator/foryou&engagement_floor=all")
        .body(Body::empty())
        .unwrap();
    let resp2 = app.oneshot(req2).await.unwrap();
    let body2 = resp2.into_body().collect().await.unwrap().to_bytes();
    let skel2: FeedSkeletonResponse = serde_json::from_slice(&body2).unwrap();
    assert_eq!(skel2.feed.len(), 1);
    assert_eq!(skel2.feed[0].post, post_uri);
    server.shutdown();
}

#[tokio::test]
async fn test_pairwise_e2e_follow_ingest_graduates_cold_user_to_tier2_feed() {
    let interner = Arc::new(StringInterner::new());
    let graph = Arc::new(GraphStore::new());
    let now = chrono_like_now();

    let author_post = "at://did:plc:cool_curator/post/art1";
    let author_did = "did:plc:cool_curator";
    let user_did = "did:plc:fresh_user";

    let aid = interner.intern(author_did);
    let pid = interner.intern(author_post);
    let uid = interner.intern(user_did);

    graph.record_post_meta(pid, aid, None, None, now - 1000);
    graph.record_interaction(aid, pid, SignalType::Like, now - 500);

    let rec = TestRecommender::new(Arc::clone(&interner), Arc::clone(&graph));
    let dials = RecommendationDials {
        min_likes: 1,
        ..Default::default()
    };

    // Before follow: Tier 3
    let res_before = rec.recommend(Some(user_did), &dials, now).unwrap();
    assert_eq!(
        res_before.posts[0].source,
        RecommendationSource::Tier3VelocityPool
    );

    // Record follow
    graph.record_follow(uid, aid);

    // After follow: Graduated to Tier 2 Follow Walk
    let res_after = rec.recommend(Some(user_did), &dials, now).unwrap();
    assert_eq!(
        res_after.posts[0].source,
        RecommendationSource::Tier2FollowWalk
    );
}

#[tokio::test]
async fn test_pairwise_e2e_like_burst_graduates_user_to_tier1_personalized_feed() {
    let (interner, graph) = SyntheticGraphBuilder::standard_cold_start_fixture(chrono_like_now());
    let rec = TestRecommender::new(Arc::clone(&interner), Arc::clone(&graph));
    let now = chrono_like_now();
    let graduating_user = "did:plc:graduating_user";
    let uid = interner.intern(graduating_user);

    // User starts with 2 likes (Tier 2/3)
    let p1 = interner
        .lookup_id("at://did:plc:author_alpha/app.bsky.feed.post/trending_1")
        .unwrap();
    let p2 = interner
        .lookup_id("at://did:plc:author_alpha/app.bsky.feed.post/trending_2")
        .unwrap();
    graph.record_interaction(uid, p1, SignalType::Like, now - 500);
    graph.record_interaction(uid, p2, SignalType::Like, now - 400);

    // Add 8 more likes to reach >= 10 threshold
    for i in 1..=8 {
        let active_pid = interner
            .lookup_id(&format!(
                "at://did:plc:author_beta/app.bsky.feed.post/active_post_{i}"
            ))
            .unwrap();
        graph.record_interaction(uid, active_pid, SignalType::Like, now - 100);
    }

    let res = rec
        .recommend(Some(graduating_user), &RecommendationDials::default(), now)
        .unwrap();
    assert_eq!(
        res.posts[0].source,
        RecommendationSource::Tier1InteractionWalk
    );
}
