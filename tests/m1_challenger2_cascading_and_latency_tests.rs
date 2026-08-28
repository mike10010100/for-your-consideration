#![forbid(unsafe_code)]
#![allow(
    clippy::all,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    missing_docs
)]

//! Challenger 2 Empirical Test Suite for Milestone 1 (M1: Bayesian Taste Shrinkage & Overlap Filtering).
//!
//! Focus:
//! 1. Cascading fallback verification: users with only single-overlap (S=1) interactions cleanly fall back to
//!    Tier 2 (Follow Walk) and Tier 3 (Velocity Pool) without panics, errors, or empty recommendations.
//! 2. Taste Twins API verification: returns empty lists when all neighbors have S < 2, and properly ranks
//!    when S >= 2 neighbors exist.
//! 3. High-concurrency recommendation latency and throughput performance under Milestone 1 changes.
//! 4. Adversarial fan-out stress test with massive single-overlap noisy neighbors.

use std::sync::Arc;
use std::time::Instant;

use compact_str::CompactString;
use for_your_consideration::prelude::*;

fn setup_engine() -> (Arc<StringInterner>, Arc<GraphStore>, Arc<Recommender>) {
    let interner = Arc::new(StringInterner::new());
    let graph = Arc::new(GraphStore::new());
    let rec = Arc::new(Recommender::new(Arc::clone(&interner), Arc::clone(&graph)));
    (interner, graph, rec)
}

// ===========================================================================
// Section 1: Cascading Fallback from Tier 1 -> Tier 2 (Follow Walk)
// ===========================================================================

#[test]
fn test_cascading_fallback_tier1_to_tier2_under_single_overlap() {
    let (interner, graph, rec) = setup_engine();
    let now = BLUESKY_EPOCH_SECS + 500_000;

    let viewer_did = "did:plc:viewer_single_overlaps_with_follows";
    let v_id = interner.intern(viewer_did);

    // Viewer has 12 liked posts (>= 10 threshold for Tier 1 entry)
    let mut viewer_post_ids = Vec::new();
    for i in 1..=12 {
        let uri = format!("at://did:plc:author_seed/app.bsky.feed.post/{i}");
        let pid = interner.intern(&uri);
        graph.record_post_meta(pid, 999, None, None, now - 10_000);
        graph.record_interaction(v_id, pid, SignalType::Like, now - 5_000);
        viewer_post_ids.push(pid);
    }

    // 12 distinct co-interactors, each sharing EXACTLY ONE distinct post with viewer
    // (None of them have S >= 2)
    for (idx, &v_pid) in viewer_post_ids.iter().enumerate() {
        let co_did = format!("did:plc:co_user_{idx}");
        let co_uid = interner.intern(&co_did);
        // Co-interactor shares v_pid with viewer
        graph.record_interaction(co_uid, v_pid, SignalType::Like, now - 4_000);

        // Co-interactor also liked a candidate post (which should NOT be recommended via Tier 1)
        let cand_uri = format!("at://did:plc:cand_author/app.bsky.feed.post/single_cand_{idx}");
        let cand_pid = interner.intern(&cand_uri);
        graph.record_post_meta(cand_pid, 888, None, None, now - 1_000);
        graph.record_interaction(co_uid, cand_pid, SignalType::Like, now - 1_000);
    }

    // Viewer follows a curated author / followee
    let followee_did = "did:plc:curated_followee";
    let f_id = interner.intern(followee_did);
    graph.record_follow(v_id, f_id);

    // Followee interacted with 3 high-quality fresh posts (distinct authors to respect diversity)
    let mut follow_post_uris = Vec::new();
    for i in 1..=3 {
        let f_uri = format!("at://did:plc:followee_author_{i}/app.bsky.feed.post/follow_cand_{i}");
        let f_pid = interner.intern(&f_uri);
        graph.record_post_meta(f_pid, 700 + i as u32, None, None, now - 500);
        graph.record_interaction(f_id, f_pid, SignalType::Like, now - 200);
        follow_post_uris.push(f_uri);
    }

    // Also populate some Tier 3 velocity posts
    for i in 1..=5 {
        let v_uri = format!("at://did:plc:velocity_author_{i}/app.bsky.feed.post/vel_{i}");
        let v_pid = interner.intern(&v_uri);
        graph.record_post_meta(v_pid, 600 + i as u32, None, None, now - 100);
        graph.record_interaction(12345, v_pid, SignalType::Like, now - 50);
    }

    // Execute recommend() for the viewer
    let dials = RecommendationDials {
        explore_ratio: 0.0,
        explain: true,
        limit: 10,
        min_likes: 1,
        ..Default::default()
    };

    let result = rec.recommend(Some(viewer_did), &dials, now);
    assert!(
        result.is_ok(),
        "Recommendation query must succeed without errors"
    );

    let feed = result.unwrap();
    assert!(
        !feed.posts.is_empty(),
        "Feed must not be empty! Cascaded to Tier 2"
    );

    // All returned posts must be from the Tier 2 followee interactions!
    for post in &feed.posts {
        assert!(
            follow_post_uris.contains(&post.uri.to_string()),
            "Expected post from Tier 2 follow walk, found: {}",
            post.uri
        );
        // Verify explanation reflects Tier 2 Follow Walk
        if let Some(ref explain) = post.explain {
            assert!(
                explain.contains("tier2_follow_walk"),
                "Explanation should show Tier 2 source: {explain}"
            );
        }
    }

    // Ensure none of the single-overlap candidate posts leaked into the feed
    for idx in 0..12 {
        let single_cand_uri =
            format!("at://did:plc:cand_author/app.bsky.feed.post/single_cand_{idx}");
        assert!(
            !feed.posts.iter().any(|p| p.uri.as_str() == single_cand_uri),
            "Single overlap candidate {single_cand_uri} must NOT appear in feed"
        );
    }
}

// ===========================================================================
// Section 2: Cascading Fallback from Tier 1 -> Tier 2 (empty) -> Tier 3 (Velocity Pool)
// ===========================================================================

#[test]
fn test_cascading_fallback_tier1_to_tier3_velocity_pool() {
    let (interner, graph, rec) = setup_engine();
    let now = BLUESKY_EPOCH_SECS + 500_000;

    let viewer_did = "did:plc:viewer_no_follows_only_single_overlaps";
    let v_id = interner.intern(viewer_did);

    // Viewer has 15 liked posts (>= 10 threshold for Tier 1)
    for i in 1..=15 {
        let uri = format!("at://did:plc:author_seed/app.bsky.feed.post/seed_{i}");
        let pid = interner.intern(&uri);
        // Seed posts created outside 6-hour window so they don't enter Tier 3 velocity pool
        graph.record_post_meta(pid, 999, None, None, now - 30_000);
        graph.record_interaction(v_id, pid, SignalType::Like, now - 30_000);

        // Single-overlap co-user for each post
        let co_uid = interner.intern(&format!("did:plc:co_single_{i}"));
        graph.record_interaction(co_uid, pid, SignalType::Like, now - 29_000);

        // Single-overlap candidate post (created outside 6-hour window)
        let single_cand_pid =
            interner.intern(&format!("at://did:plc:cand/app.bsky.feed.post/single_{i}"));
        graph.record_post_meta(single_cand_pid, 888, None, None, now - 28_000);
        graph.record_interaction(co_uid, single_cand_pid, SignalType::Like, now - 28_000);
    }

    // Viewer follows NO ONE (Tier 2 is guaranteed empty)
    assert!(graph.get_user_follows(v_id).is_empty());

    // Populate Tier 3 Velocity Pool with 10 high-velocity posts with distinct authors
    let mut velocity_uris = Vec::new();
    for i in 1..=10 {
        let vel_uri = format!("at://did:plc:viral_creator_{i}/app.bsky.feed.post/trending_{i}");
        let vel_pid = interner.intern(&vel_uri);
        graph.record_post_meta(vel_pid, 500 + i as u32, None, None, now - 200);
        // Add multiple interactions within recent window to populate velocity pool
        for u in 1000..1020 {
            graph.record_interaction(u, vel_pid, SignalType::Like, now - (i as u64 * 10));
        }
        velocity_uris.push(vel_uri);
    }

    let dials = RecommendationDials {
        explore_ratio: 0.0,
        explain: true,
        limit: 10,
        ..Default::default()
    };

    // Execute recommend()
    let result = rec.recommend(Some(viewer_did), &dials, now);
    assert!(
        result.is_ok(),
        "Recommendation must succeed without panicking"
    );

    let feed = result.unwrap();
    assert!(
        !feed.posts.is_empty(),
        "Feed must cleanly cascade to Tier 3 velocity pool"
    );
    assert_eq!(
        feed.posts.len(),
        10,
        "Should return full page from Tier 3 pool"
    );

    // All returned posts must be from the Tier 3 velocity pool
    for post in &feed.posts {
        assert!(
            velocity_uris.contains(&post.uri.to_string()),
            "Post {} should be from Tier 3 velocity pool",
            post.uri
        );
        if let Some(ref explain) = post.explain {
            assert!(
                explain.contains("tier3_velocity_pool"),
                "Explanation should indicate Tier 3 source: {explain}"
            );
        }
    }
}

// ===========================================================================
// Section 3: Feed Preview Cascading Behavior
// ===========================================================================

#[test]
fn test_recommend_preview_cascades_cleanly_when_tier1_empty() {
    let (interner, graph, rec) = setup_engine();
    let now = BLUESKY_EPOCH_SECS + 500_000;

    let viewer_did = "did:plc:viewer_preview_single_overlap";
    let v_id = interner.intern(viewer_did);

    // Viewer has 10 likes (qualifies for Tier 1 entry)
    for i in 1..=10 {
        let uri = format!("at://did:plc:author/app.bsky.feed.post/{i}");
        let pid = interner.intern(&uri);
        graph.record_post_meta(pid, 100, None, None, now - 1000);
        graph.record_interaction(v_id, pid, SignalType::Like, now - 500);

        // Every co-user shares only 1 post (S=1)
        let co_id = interner.intern(&format!("did:plc:single_co_{i}"));
        graph.record_interaction(co_id, pid, SignalType::Like, now - 400);
    }

    // Populate velocity pool for Tier 3 fallback
    for i in 1..=5 {
        let vel_uri = format!("at://did:plc:trend/app.bsky.feed.post/v_{i}");
        let vel_pid = interner.intern(&vel_uri);
        graph.record_post_meta(vel_pid, 200, None, None, now - 100);
        graph.record_interaction(9999, vel_pid, SignalType::Like, now - 50);
    }

    let dials = RecommendationDials {
        min_likes: 1,
        ..Default::default()
    };
    let preview_res = rec.recommend_preview(Some(viewer_did), &dials);
    assert!(preview_res.is_ok(), "Preview must succeed");

    let preview = preview_res.unwrap();
    assert!(
        !preview.items.is_empty(),
        "Preview items must not be empty after cascade"
    );
    assert!(
        preview.total_candidates > 0,
        "Preview total_candidates must be > 0"
    );

    // Candidate breakdown must have positive final score
    for cand in &preview.items {
        assert!(cand.score_breakdown.final_score > 0.0);
    }
}

// ===========================================================================
// Section 4: Taste Twins API Returns Empty on S < 2
// ===========================================================================

#[test]
fn test_taste_twins_api_returns_empty_when_all_neighbors_have_single_overlap() {
    let (interner, graph, rec) = setup_engine();
    let now = BLUESKY_EPOCH_SECS + 500_000;

    let viewer_did = "did:plc:isolated_curator";
    let v_id = interner.intern(viewer_did);

    // Viewer liked 25 distinct posts
    let mut post_ids = Vec::new();
    for i in 1..=25 {
        let uri = format!("at://did:plc:art/app.bsky.feed.post/{i}");
        let pid = interner.intern(&uri);
        graph.record_post_meta(pid, 100, None, None, now - 10_000);
        graph.record_interaction(v_id, pid, SignalType::Like, now - 5_000);
        post_ids.push(pid);
    }

    // 50 other users, each sharing EXACTLY 1 post with viewer (no pair shares >= 2)
    for u in 0..50 {
        let other_did = format!("did:plc:other_user_{u}");
        let other_id = interner.intern(&other_did);
        let shared_post = post_ids[u % post_ids.len()];
        graph.record_interaction(other_id, shared_post, SignalType::Like, now - 2_000);

        // Add 5 other non-shared likes for each other user
        for j in 1..=5 {
            let non_shared_pid =
                interner.intern(&format!("at://did:plc:other/app.bsky.feed.post/u{u}_{j}"));
            graph.record_post_meta(non_shared_pid, 200, None, None, now - 10_000);
            graph.record_interaction(other_id, non_shared_pid, SignalType::Like, now - 1_000);
        }
    }

    // Query Taste Twins API
    let resp = rec.find_taste_twins(viewer_did, 20).unwrap();
    assert_eq!(resp.viewer_did, viewer_did);
    assert_eq!(resp.total_liked_posts, 25);
    assert!(
        resp.twins.is_empty(),
        "All 50 neighbors have S=1, so Taste Twins API MUST return an empty list!"
    );
    assert!(resp.query_latency_us > 0 || resp.twins.is_empty());

    // Now introduce ONE genuine twin with S=2 shared posts
    let real_twin_did = "did:plc:genuine_twin";
    let real_twin_id = interner.intern(real_twin_did);
    graph.record_interaction(real_twin_id, post_ids[0], SignalType::Like, now - 1_000);
    graph.record_interaction(real_twin_id, post_ids[1], SignalType::Like, now - 1_000);

    let resp2 = rec.find_taste_twins(viewer_did, 20).unwrap();
    assert_eq!(
        resp2.twins.len(),
        1,
        "Exactly 1 twin with S=2 must be returned"
    );
    assert_eq!(resp2.twins[0].user_did, real_twin_did);
    assert_eq!(resp2.twins[0].shared_posts_count, 2);

    // Verify Bayesian confidence score:
    // raw cosine = 2 / sqrt(25 * 2) = 2 / sqrt(50) = 2 / 7.0710678 ≈ 0.2828427
    // shrinkage = 2 / (2 + 3) = 0.40
    // confidence = 0.2828427 * 0.40 ≈ 0.113137
    let expected_conf = (2.0f32 / (50.0f32).sqrt()) * 0.40f32;
    assert!(
        (resp2.twins[0].similarity_score - expected_conf).abs() < 1e-4,
        "Similarity score mismatch: expected {expected_conf}, got {}",
        resp2.twins[0].similarity_score
    );
}

// ===========================================================================
// Section 5: Adversarial Dense Single-Overlap Fan-Out Stress
// ===========================================================================

#[test]
fn test_adversarial_dense_single_overlap_fanout_latency() {
    let (interner, graph, rec) = setup_engine();
    let now = BLUESKY_EPOCH_SECS + 500_000;

    let viewer_did = "did:plc:viewer_dense_fanout";
    let v_id = interner.intern(viewer_did);

    // Super-viral post liked by 2,000 users
    let viral_post_uri = "at://did:plc:viral/app.bsky.feed.post/super_viral_mega";
    let viral_pid = interner.intern(viral_post_uri);
    graph.record_post_meta(viral_pid, 999, None, None, now - 10_000);

    // Viewer likes the viral post plus 9 other personal posts
    graph.record_interaction(v_id, viral_pid, SignalType::Like, now - 5_000);
    let mut personal_posts = Vec::new();
    for i in 1..=9 {
        let p_uri = format!("at://did:plc:personal/app.bsky.feed.post/{i}");
        let pid = interner.intern(&p_uri);
        graph.record_post_meta(pid, 888, None, None, now - 10_000);
        graph.record_interaction(v_id, pid, SignalType::Like, now - 5_000);
        personal_posts.push(pid);
    }

    // 1,990 noisy co-interactors like ONLY the viral post (S=1 with viewer)
    for u in 1..=1990 {
        let noise_did = format!("did:plc:noise_user_{u}");
        let noise_id = interner.intern(&noise_did);
        graph.record_interaction(noise_id, viral_pid, SignalType::Like, now - 3_000);
    }

    // 10 genuine curators like the viral post AND at least 2 personal posts (S >= 3)
    for u in 1..=10 {
        let curator_did = format!("did:plc:curator_user_{u}");
        let curator_id = interner.intern(&curator_did);
        graph.record_interaction(curator_id, viral_pid, SignalType::Like, now - 3_000);
        graph.record_interaction(curator_id, personal_posts[0], SignalType::Like, now - 3_000);
        graph.record_interaction(curator_id, personal_posts[1], SignalType::Like, now - 3_000);

        // Curator endorses a candidate post
        let cand_uri = format!("at://did:plc:curator_recs/app.bsky.feed.post/cand_{u}");
        let cand_pid = interner.intern(&cand_uri);
        graph.record_post_meta(cand_pid, 777, None, None, now - 1_000);
        graph.record_interaction(curator_id, cand_pid, SignalType::Like, now - 1_000);
    }

    // Measure find_taste_twins latency under 2000-user fan-out
    let start_twins = Instant::now();
    let twins_resp = rec.find_taste_twins(viewer_did, 20).unwrap();
    let elapsed_twins = start_twins.elapsed();

    assert_eq!(
        twins_resp.twins.len(),
        10,
        "Only the 10 genuine curators (S=3) should qualify; 1990 single-overlaps dropped"
    );
    // In debug mode, traversal of 2000 roaring bitmaps should take < 200ms; < 10ms in release
    let max_twins_ms = if cfg!(debug_assertions) { 200 } else { 10 };
    assert!(
        elapsed_twins.as_millis() < max_twins_ms,
        "Taste twins query took too long: {:?}",
        elapsed_twins
    );

    // Measure recommendation query latency under 2000-user fan-out
    let dials = RecommendationDials {
        min_likes: 1,
        ..Default::default()
    };
    let start_rec = Instant::now();
    let rec_res = rec.recommend(Some(viewer_did), &dials, now);
    let elapsed_rec = start_rec.elapsed();

    assert!(rec_res.is_ok());
    let feed = rec_res.unwrap();
    assert!(!feed.posts.is_empty());
    let max_rec_ms = if cfg!(debug_assertions) { 200 } else { 10 };
    assert!(
        elapsed_rec.as_millis() < max_rec_ms,
        "Recommendation query took too long: {:?}",
        elapsed_rec
    );
}

// ===========================================================================
// Section 6: High-Concurrency Multi-Threaded Read/Write Stress Test
// ===========================================================================

#[test]
fn test_concurrent_recommendation_and_taste_twins_stress() {
    let (interner, graph, rec) = setup_engine();
    let now = BLUESKY_EPOCH_SECS + 500_000;

    // Seed 100 users, 200 posts
    let mut user_dids = Vec::new();
    for u in 0..100 {
        let did = CompactString::new(format!("did:plc:stress_user_{u:04}"));
        interner.intern(&did);
        user_dids.push(did);
    }

    for p in 0..200 {
        let uri = CompactString::new(format!("at://did:plc:author/app.bsky.feed.post/{p:04}"));
        let pid = interner.intern(&uri);
        graph.record_post_meta(pid, (p % 20) as u32, None, None, now - 1000);
    }

    // Seed initial interactions: some single overlaps, some multi overlaps
    for i in 0..1000 {
        let uid = (i % 100) as u32;
        let pid = ((i * 7) % 200) as u32;
        graph.record_interaction(uid, pid, SignalType::Like, now - (i as u64 % 500));
    }

    // Follows
    for u in 0..100 {
        let uid = u as u32;
        graph.record_follow(uid, (uid + 1) % 100);
        graph.record_follow(uid, (uid + 2) % 100);
    }

    let user_dids = Arc::new(user_dids);
    let dials = Arc::new(RecommendationDials::default());

    let mut handles = Vec::new();

    // Spawn 8 concurrent reader threads
    for t in 0..8 {
        let rec_clone = Arc::clone(&rec);
        let users_clone = Arc::clone(&user_dids);
        let dials_clone = Arc::clone(&dials);

        let handle = std::thread::spawn(move || {
            let iterations = 200;
            for i in 0..iterations {
                let user_idx = (t * 25 + i) % users_clone.len();
                let viewer = users_clone[user_idx].as_str();

                if i % 3 == 0 {
                    let r = rec_clone.recommend(Some(viewer), &dials_clone, now);
                    assert!(r.is_ok());
                } else if i % 3 == 1 {
                    let twins = rec_clone.find_taste_twins(viewer, 10);
                    assert!(twins.is_ok());
                } else {
                    let preview = rec_clone.recommend_preview(Some(viewer), &dials_clone);
                    assert!(preview.is_ok());
                }
            }
        });
        handles.push(handle);
    }

    // Spawn 2 concurrent writer threads
    for w in 0..2 {
        let graph_clone = Arc::clone(&graph);
        let interner_clone = Arc::clone(&interner);
        let handle = std::thread::spawn(move || {
            for i in 0..100 {
                let uid = ((w * 50 + i) % 100) as u32;
                let post_uri = format!("at://did:plc:writer/app.bsky.feed.post/w_{w}_{i}");
                let pid = interner_clone.intern(&post_uri);
                graph_clone.record_post_meta(pid, uid, None, None, now);
                graph_clone.record_interaction(uid, pid, SignalType::Like, now);
            }
        });
        handles.push(handle);
    }

    for h in handles {
        h.join().expect("Thread should not panic");
    }
}
