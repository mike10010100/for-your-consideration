#![allow(clippy::float_cmp)]

use std::sync::Arc;
use std::thread;

use for_your_consideration::prelude::*;

#[test]
fn test_compact_edge_representation_and_weights() {
    let base_ts = BLUESKY_EPOCH_SECS + 3600;

    let edge_like = CompactEdge::new(100, SignalType::Like, base_ts);
    assert_eq!(edge_like.target(), 100);
    assert_eq!(edge_like.signal(), SignalType::Like);
    assert_eq!(edge_like.weight(), 1.0);
    assert_eq!(edge_like.timestamp_secs(), base_ts);

    let edge_quote = CompactEdge::new(200, SignalType::Quote, base_ts + 120);
    assert_eq!(edge_quote.target(), 200);
    assert_eq!(edge_quote.signal(), SignalType::Quote);
    assert_eq!(edge_quote.weight(), 2.0);
    assert_eq!(edge_quote.timestamp_secs(), base_ts + 120);

    let edge_repost = CompactEdge::new(300, SignalType::Repost, base_ts + 300);
    assert_eq!(edge_repost.target(), 300);
    assert_eq!(edge_repost.signal(), SignalType::Repost);
    assert_eq!(edge_repost.weight(), 3.0);
    assert_eq!(edge_repost.timestamp_secs(), base_ts + 300);

    // Memory footprint verification
    assert_eq!(std::mem::size_of::<CompactEdge>(), 8);
}

#[test]
fn test_interner_concurrent_stress() {
    let interner = Arc::new(StringInterner::new());
    let mut handles = Vec::new();

    // 16 worker threads concurrently interning overlapping strings
    for thread_idx in 0..16 {
        let interner_clone = Arc::clone(&interner);
        handles.push(thread::spawn(move || {
            for i in 0..500 {
                let shared_did = format!("did:plc:shared_user_{}", i % 50);
                let id = interner_clone.intern(&shared_did);
                assert_eq!(
                    interner_clone.lookup_str(id).as_deref(),
                    Some(shared_did.as_str())
                );
                assert_eq!(interner_clone.lookup_id(&shared_did), Some(id));
            }
            let unique_uri = format!("at://did:plc:thread_{thread_idx}/app.bsky.feed.post/999");
            let id_unique = interner_clone.intern(&unique_uri);
            assert_eq!(
                interner_clone.lookup_str(id_unique).as_deref(),
                Some(unique_uri.as_str())
            );
        }));
    }

    for handle in handles {
        handle.join().expect("Worker thread panicked");
    }

    assert_eq!(interner.len(), 50 + 16);
}

#[test]
fn test_graph_store_forward_and_reverse_adjacency() {
    let graph = GraphStore::new();
    let user_a = 1;
    let user_b = 2;
    let post_x = 100;
    let post_y = 200;
    let ts = BLUESKY_EPOCH_SECS + 10_000;

    graph.record_interaction(user_a, post_x, SignalType::Like, ts);
    graph.record_interaction(user_a, post_y, SignalType::Repost, ts + 10);
    graph.record_interaction(user_b, post_x, SignalType::Quote, ts + 20);

    // User A forward edges
    let u_a_edges = graph.get_user_interactions(user_a);
    assert_eq!(u_a_edges.len(), 2);
    assert_eq!(u_a_edges[0].target(), post_x);
    assert_eq!(u_a_edges[0].signal(), SignalType::Like);
    assert_eq!(u_a_edges[1].target(), post_y);
    assert_eq!(u_a_edges[1].signal(), SignalType::Repost);

    // Post X reverse edges
    let p_x_edges = graph.get_post_interactions(post_x);
    assert_eq!(p_x_edges.len(), 2);
    assert_eq!(p_x_edges[0].target(), user_a);
    assert_eq!(p_x_edges[0].signal(), SignalType::Like);
    assert_eq!(p_x_edges[1].target(), user_b);
    assert_eq!(p_x_edges[1].signal(), SignalType::Quote);

    // Roaring Bitmaps check
    let bm_a = graph
        .get_user_likes_bitmap(user_a)
        .expect("User A bitmap missing");
    assert!(bm_a.contains(post_x));
    assert!(bm_a.contains(post_y));
    assert_eq!(bm_a.len(), 2);

    let bm_b = graph
        .get_user_likes_bitmap(user_b)
        .expect("User B bitmap missing");
    assert!(bm_b.contains(post_x));
    assert!(!bm_b.contains(post_y));
    assert_eq!(bm_b.len(), 1);
}

#[test]
fn test_graph_store_similarity_metrics() {
    let graph = GraphStore::new();
    let u1 = 1;
    let u2 = 2;
    let u3 = 3;
    let ts = BLUESKY_EPOCH_SECS + 500;

    // u1 liked posts: 10, 20, 30, 40 (size = 4)
    for p in [10, 20, 30, 40] {
        graph.record_interaction(u1, p, SignalType::Like, ts);
    }

    // u2 liked posts: 10, 20, 30, 50 (size = 4, intersection = 3, union = 5)
    for p in [10, 20, 30, 50] {
        graph.record_interaction(u2, p, SignalType::Like, ts);
    }

    // u3 liked posts: 99, 100 (size = 2, disjoint)
    for p in [99, 100] {
        graph.record_interaction(u3, p, SignalType::Like, ts);
    }

    let jaccard_1_2 = graph.compute_jaccard_similarity(u1, u2);
    assert!((jaccard_1_2 - 0.6).abs() < 1e-4);

    let cosine_1_2 = graph.compute_cosine_similarity(u1, u2);
    assert!((cosine_1_2 - 0.75).abs() < 1e-4);

    let jaccard_1_3 = graph.compute_jaccard_similarity(u1, u3);
    assert_eq!(jaccard_1_3, 0.0);

    let cosine_1_3 = graph.compute_cosine_similarity(u1, u3);
    assert_eq!(cosine_1_3, 0.0);
}

#[test]
fn test_follow_graph_operations() {
    let graph = GraphStore::new();
    let follower = 10;
    let followed_1 = 20;
    let followed_2 = 30;

    graph.record_follow(follower, followed_1);
    graph.record_follow(follower, followed_2);
    graph.record_follow(follower, followed_1); // duplicate insertion

    let follows = graph.get_user_follows(follower);
    assert_eq!(follows.len(), 2);
    assert!(follows.contains(&followed_1));
    assert!(follows.contains(&followed_2));

    graph.remove_follow(follower, followed_1);
    let follows_after = graph.get_user_follows(follower);
    assert_eq!(follows_after, vec![followed_2]);
}

#[test]
fn test_post_metadata_and_hierarchy() {
    let graph = GraphStore::new();
    let author_a = 100;
    let author_b = 200;
    let root_post = 10;
    let reply_post = 11;
    let nested_reply = 12;
    let ts = BLUESKY_EPOCH_SECS + 1_000;

    graph.record_post_meta(root_post, author_a, None, None, ts);
    graph.record_post_meta(
        reply_post,
        author_b,
        Some(root_post),
        Some(root_post),
        ts + 30,
    );
    graph.record_post_meta(
        nested_reply,
        author_a,
        Some(root_post),
        Some(reply_post),
        ts + 60,
    );

    let meta_root = graph.get_post_meta(root_post).unwrap();
    assert_eq!(meta_root.author_id, author_a);
    assert!(meta_root.is_root());
    assert!(!meta_root.is_reply());

    let meta_reply = graph.get_post_meta(reply_post).unwrap();
    assert_eq!(meta_reply.author_id, author_b);
    assert_eq!(meta_reply.root_id, Some(root_post));
    assert_eq!(meta_reply.parent_id, Some(root_post));
    assert!(meta_reply.is_reply());

    let meta_nested = graph.get_post_meta(nested_reply).unwrap();
    assert_eq!(meta_nested.author_id, author_a);
    assert_eq!(meta_nested.root_id, Some(root_post));
    assert_eq!(meta_nested.parent_id, Some(reply_post));

    // Author posts
    let author_a_posts = graph.get_author_posts(author_a);
    assert_eq!(author_a_posts.len(), 2);
    assert!(author_a_posts.contains(&root_post));
    assert!(author_a_posts.contains(&nested_reply));
}

#[test]
fn test_exponential_time_decay_and_popularity_dampener() {
    let now = 1_700_000_000;
    let tau_36h = 36.0 * 3600.0;

    let score_now = calculate_time_decay(SignalType::Like, now, now, tau_36h);
    assert_eq!(score_now, 1.0);

    let score_repost_now = calculate_time_decay(SignalType::Repost, now, now, tau_36h);
    assert_eq!(score_repost_now, 3.0);

    let score_18h_later = calculate_time_decay(SignalType::Like, now, now + 18 * 3600, tau_36h);
    let expected_18h = (-0.5f32).exp();
    assert!((score_18h_later - expected_18h).abs() < 1e-5);

    // BM25 Dampener
    assert_eq!(calculate_popularity_dampener(0), 1.0);
    assert_eq!(calculate_popularity_dampener(3), 0.5);
    assert_eq!(calculate_popularity_dampener(8), 1.0 / 3.0);
}

#[test]
fn test_high_velocity_sliding_pool() {
    let graph = GraphStore::new();
    let current_ts = BLUESKY_EPOCH_SECS + 100_000;

    // Post 1: 10 likes 10 minutes ago
    for u in 1..=10 {
        graph.record_interaction(u, 1001, SignalType::Like, current_ts - 600);
    }

    // Post 2: 3 reposts 5 minutes ago
    for u in 1..=3 {
        graph.record_interaction(u, 1002, SignalType::Repost, current_ts - 300);
    }

    // Post 3: 20 likes 12 hours ago (outside 6-hour window)
    for u in 1..=20 {
        graph.record_interaction(u, 1003, SignalType::Like, current_ts - 12 * 3600);
    }

    let top_candidates = graph.get_velocity_pool_candidates_at(current_ts, 5);
    assert_eq!(top_candidates.len(), 2);
    // Post 1002 (reposts 3.0x, dampener 0.5 -> score ~4.44) ranks ahead of Post 1001 (likes 1.0x, dampener ~0.30 -> score ~2.93)
    assert_eq!(top_candidates[0], 1002);
    assert_eq!(top_candidates[1], 1001);
    assert!(!top_candidates.contains(&1003));
}

#[test]
fn test_concurrent_graph_mutations() {
    let graph = Arc::new(GraphStore::new());
    let mut handles = Vec::new();
    let base_ts = BLUESKY_EPOCH_SECS + 50_000;

    for thread_idx in 0..16 {
        let graph_clone = Arc::clone(&graph);
        handles.push(thread::spawn(move || {
            for i in 0..100 {
                let user_id = (thread_idx * 100 + i) as u32;
                let post_id = (i % 20) as u32;
                graph_clone.record_interaction(
                    user_id,
                    post_id,
                    SignalType::Like,
                    base_ts + i as u64,
                );
                graph_clone.record_follow(user_id, (user_id + 1) % 1600);
                graph_clone.record_post_meta(post_id, user_id, None, None, base_ts);
            }
        }));
    }

    for handle in handles {
        handle.join().expect("Graph mutation thread panicked");
    }

    let stats = graph.stats();
    assert_eq!(stats.total_users, 1600);
    assert_eq!(stats.total_posts, 20);
    assert_eq!(stats.total_interactions, 1600);
    assert_eq!(stats.total_follows, 1600);
}
