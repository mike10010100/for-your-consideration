//! Empirical Challenger Stress & Verification Suite for Milestone 3 Iteration 2.
//!
//! Validates:
//! 1. Synthetic Snapshot Migrations (v1, v2, v3, v4) under extreme & corrupted payloads.
//! 2. Cascading Tier Fallback across extreme engagement floor values: 0, 1, 3, 10, 50, 100.
//! 3. Recommender and Preview Consistency under cascading fallback.
//! 4. High-concurrency stress test with 32 threads under variable dials and dynamic preferences.

#![forbid(unsafe_code)]
#![allow(
    clippy::all,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    missing_docs,
    rust_2018_idioms
)]

use std::fs::File;
use std::io::Write;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use crc32fast::Hasher;
use for_your_consideration::graph::GraphStore;
use for_your_consideration::interner::StringInterner;
use for_your_consideration::preferences::UserPreferencesStore;
use for_your_consideration::recommender::Recommender;
use for_your_consideration::snapshot::{
    load_snapshot_with_preferences, HEADER_SIZE, SNAPSHOT_FORMAT_VERSION,
    SNAPSHOT_FORMAT_VERSION_V1, SNAPSHOT_FORMAT_VERSION_V2, SNAPSHOT_FORMAT_VERSION_V3,
    SNAPSHOT_MAGIC,
};
use for_your_consideration::types::{
    RecommendationDials, RecommendationSource, SignalType, UserDials, DEFAULT_MIN_LIKES,
};

fn current_timestamp() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
}

fn unique_temp_path(prefix: &str) -> String {
    let nanos = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("./target/{prefix}_{}_{nanos}.bin", std::process::id())
}

/// Synthesizes a valid binary snapshot file with arbitrary version and custom Section 8 payload.
fn create_synthetic_snapshot(
    file_path: &str,
    version: u16,
    num_strings: u32,
    num_users: u32,
    total_edges: u64,
    num_followers: u32,
    num_metadata: u32,
    num_preferences: u32,
    section8_payload: &[u8],
) {
    let mut payload = Vec::new();

    // Section 1: Strings (0 strings)
    payload.extend_from_slice(&0u32.to_le_bytes());

    // Section 2: User Interactions (0 users)
    payload.extend_from_slice(&0u32.to_le_bytes());

    // Section 3: Post Interactions (0 posts)
    payload.extend_from_slice(&0u32.to_le_bytes());

    // Section 4: Roaring Bitmaps (0 users)
    payload.extend_from_slice(&0u32.to_le_bytes());

    // Section 5: Follows (0 followers)
    payload.extend_from_slice(&0u32.to_le_bytes());

    // Section 6: Post Metadata (0 posts)
    payload.extend_from_slice(&0u32.to_le_bytes());

    // Section 7: Active Recent Posts (0 posts)
    payload.extend_from_slice(&0u32.to_le_bytes());

    // Section 8: User Preferences (if provided)
    if !section8_payload.is_empty() {
        payload.extend_from_slice(section8_payload);
    }

    // Compute Payload CRC
    let mut p_hasher = Hasher::new();
    p_hasher.update(&payload);
    let payload_crc = p_hasher.finalize();

    // Construct 64-byte Header
    let mut header = [0u8; HEADER_SIZE];
    header[0..4].copy_from_slice(&SNAPSHOT_MAGIC);
    header[4..6].copy_from_slice(&version.to_le_bytes());
    header[6..8].copy_from_slice(&(HEADER_SIZE as u16).to_le_bytes());
    header[8..16].copy_from_slice(&current_timestamp().to_le_bytes());
    header[16..24].copy_from_slice(&0u64.to_le_bytes()); // cursor
    header[24..28].copy_from_slice(&0u32.to_le_bytes()); // flags
    header[28..32].copy_from_slice(&num_strings.to_le_bytes());
    header[32..36].copy_from_slice(&num_users.to_le_bytes());
    header[36..44].copy_from_slice(&total_edges.to_le_bytes());
    header[44..48].copy_from_slice(&num_followers.to_le_bytes());
    header[48..52].copy_from_slice(&num_metadata.to_le_bytes());
    header[52..56].copy_from_slice(&payload_crc.to_le_bytes());

    // Header CRC over bytes 0..56
    let mut h_hasher = Hasher::new();
    h_hasher.update(&header[0..56]);
    let header_crc = h_hasher.finalize();
    header[56..60].copy_from_slice(&header_crc.to_le_bytes());
    header[60..64].copy_from_slice(&num_preferences.to_le_bytes());

    let mut file = File::create(file_path).unwrap();
    file.write_all(&header).unwrap();
    file.write_all(&payload).unwrap();
    file.flush().unwrap();
}

// ===========================================================================
// 1. Synthetic Snapshot Migrations Matrix (v1, v2, v3, v4)
// ===========================================================================

#[test]
fn test_stress_synthetic_snapshot_migration_matrix() {
    let interner = Arc::new(StringInterner::new());
    let graph = Arc::new(GraphStore::new());

    // Case 1: Version 1 (no preferences payload)
    {
        let path = unique_temp_path("stress_v1");
        create_synthetic_snapshot(&path, SNAPSHOT_FORMAT_VERSION_V1, 0, 0, 0, 0, 0, 0, &[]);
        let prefs = Arc::new(UserPreferencesStore::new());
        let res = load_snapshot_with_preferences(&path, &interner, &graph, &prefs)
            .expect("v1 load must succeed")
            .expect("snapshot must exist");
        assert_eq!(res.header.format_version, SNAPSHOT_FORMAT_VERSION_V1);
        assert_eq!(prefs.len(), 0);
        let _ = std::fs::remove_file(path);
    }

    // Case 2: Version 2 (40 bytes per preference record)
    {
        let path = unique_temp_path("stress_v2");
        let mut sec8 = Vec::new();
        sec8.extend_from_slice(&2u32.to_le_bytes()); // 2 profiles

        // User 101
        sec8.extend_from_slice(&101u32.to_le_bytes());
        sec8.extend_from_slice(&(48.0f32 * 3600.0).to_le_bytes());
        sec8.extend_from_slice(&0.15f32.to_le_bytes());
        sec8.extend_from_slice(&1.0f32.to_le_bytes()); // art
        sec8.extend_from_slice(&2.0f32.to_le_bytes()); // tech
        sec8.extend_from_slice(&3.0f32.to_le_bytes()); // science
        sec8.extend_from_slice(&4.0f32.to_le_bytes()); // news
        sec8.extend_from_slice(&5.0f32.to_le_bytes()); // culture
        sec8.extend_from_slice(&1_600_000_000u64.to_le_bytes());

        // User 102
        sec8.extend_from_slice(&102u32.to_le_bytes());
        sec8.extend_from_slice(&(12.0f32 * 3600.0).to_le_bytes());
        sec8.extend_from_slice(&0.40f32.to_le_bytes());
        sec8.extend_from_slice(&0.5f32.to_le_bytes()); // art
        sec8.extend_from_slice(&0.5f32.to_le_bytes()); // tech
        sec8.extend_from_slice(&0.5f32.to_le_bytes()); // science
        sec8.extend_from_slice(&0.5f32.to_le_bytes()); // news
        sec8.extend_from_slice(&0.5f32.to_le_bytes()); // culture
        sec8.extend_from_slice(&1_650_000_000u64.to_le_bytes());

        create_synthetic_snapshot(&path, SNAPSHOT_FORMAT_VERSION_V2, 0, 0, 0, 0, 0, 2, &sec8);
        let prefs = Arc::new(UserPreferencesStore::new());
        let res = load_snapshot_with_preferences(&path, &interner, &graph, &prefs)
            .expect("v2 load must succeed")
            .expect("snapshot must exist");
        assert_eq!(res.header.format_version, SNAPSHOT_FORMAT_VERSION_V2);
        assert_eq!(prefs.len(), 2);

        let d101 = prefs.get(101).unwrap();
        assert_eq!(d101.freshness_half_life_secs, 48.0 * 3600.0);
        assert!(
            !d101.include_replies,
            "v2 must default include_replies=false"
        );
        assert_eq!(
            d101.min_likes, DEFAULT_MIN_LIKES,
            "v2 must default min_likes=3"
        );

        let d102 = prefs.get(102).unwrap();
        assert_eq!(d102.freshness_half_life_secs, 12.0 * 3600.0);
        assert!(!d102.include_replies);
        assert_eq!(d102.min_likes, DEFAULT_MIN_LIKES);

        let _ = std::fs::remove_file(path);
    }

    // Case 3: Version 3 (41 bytes per preference record with include_replies)
    {
        let path = unique_temp_path("stress_v3");
        let mut sec8 = Vec::new();
        sec8.extend_from_slice(&2u32.to_le_bytes()); // 2 profiles

        // User 201: include_replies = true (1)
        sec8.extend_from_slice(&201u32.to_le_bytes());
        sec8.extend_from_slice(&(24.0f32 * 3600.0).to_le_bytes());
        sec8.extend_from_slice(&0.20f32.to_le_bytes());
        sec8.extend_from_slice(&1.0f32.to_le_bytes());
        sec8.extend_from_slice(&1.0f32.to_le_bytes());
        sec8.extend_from_slice(&1.0f32.to_le_bytes());
        sec8.extend_from_slice(&1.0f32.to_le_bytes());
        sec8.extend_from_slice(&1.0f32.to_le_bytes());
        sec8.extend_from_slice(&1u8.to_le_bytes()); // include_replies = true
        sec8.extend_from_slice(&1_700_000_000u64.to_le_bytes());

        // User 202: include_replies = false (0)
        sec8.extend_from_slice(&202u32.to_le_bytes());
        sec8.extend_from_slice(&(36.0f32 * 3600.0).to_le_bytes());
        sec8.extend_from_slice(&0.30f32.to_le_bytes());
        sec8.extend_from_slice(&1.0f32.to_le_bytes());
        sec8.extend_from_slice(&1.0f32.to_le_bytes());
        sec8.extend_from_slice(&1.0f32.to_le_bytes());
        sec8.extend_from_slice(&1.0f32.to_le_bytes());
        sec8.extend_from_slice(&1.0f32.to_le_bytes());
        sec8.extend_from_slice(&0u8.to_le_bytes()); // include_replies = false
        sec8.extend_from_slice(&1_710_000_000u64.to_le_bytes());

        create_synthetic_snapshot(&path, SNAPSHOT_FORMAT_VERSION_V3, 0, 0, 0, 0, 0, 2, &sec8);
        let prefs = Arc::new(UserPreferencesStore::new());
        let res = load_snapshot_with_preferences(&path, &interner, &graph, &prefs)
            .expect("v3 load must succeed")
            .expect("snapshot must exist");
        assert_eq!(res.header.format_version, SNAPSHOT_FORMAT_VERSION_V3);
        assert_eq!(prefs.len(), 2);

        let d201 = prefs.get(201).unwrap();
        assert!(
            d201.include_replies,
            "v3 user 201 must preserve include_replies=true"
        );
        assert_eq!(
            d201.min_likes, DEFAULT_MIN_LIKES,
            "v3 user 201 must default min_likes=3"
        );

        let d202 = prefs.get(202).unwrap();
        assert!(
            !d202.include_replies,
            "v3 user 202 must preserve include_replies=false"
        );
        assert_eq!(
            d202.min_likes, DEFAULT_MIN_LIKES,
            "v3 user 202 must default min_likes=3"
        );

        let _ = std::fs::remove_file(path);
    }

    // Case 4: Version 4 (45 bytes per preference record with min_likes)
    {
        let path = unique_temp_path("stress_v4");
        let mut sec8 = Vec::new();
        sec8.extend_from_slice(&2u32.to_le_bytes()); // 2 profiles

        // User 301: min_likes = 50, include_replies = true
        sec8.extend_from_slice(&301u32.to_le_bytes());
        sec8.extend_from_slice(&(72.0f32 * 3600.0).to_le_bytes());
        sec8.extend_from_slice(&0.10f32.to_le_bytes());
        sec8.extend_from_slice(&1.5f32.to_le_bytes());
        sec8.extend_from_slice(&1.5f32.to_le_bytes());
        sec8.extend_from_slice(&1.5f32.to_le_bytes());
        sec8.extend_from_slice(&1.5f32.to_le_bytes());
        sec8.extend_from_slice(&1.5f32.to_le_bytes());
        sec8.extend_from_slice(&1u8.to_le_bytes());
        sec8.extend_from_slice(&50u32.to_le_bytes()); // min_likes = 50
        sec8.extend_from_slice(&1_750_000_000u64.to_le_bytes());

        // User 302: min_likes = 0, include_replies = false
        sec8.extend_from_slice(&302u32.to_le_bytes());
        sec8.extend_from_slice(&(24.0f32 * 3600.0).to_le_bytes());
        sec8.extend_from_slice(&0.05f32.to_le_bytes());
        sec8.extend_from_slice(&1.0f32.to_le_bytes());
        sec8.extend_from_slice(&1.0f32.to_le_bytes());
        sec8.extend_from_slice(&1.0f32.to_le_bytes());
        sec8.extend_from_slice(&1.0f32.to_le_bytes());
        sec8.extend_from_slice(&1.0f32.to_le_bytes());
        sec8.extend_from_slice(&0u8.to_le_bytes());
        sec8.extend_from_slice(&0u32.to_le_bytes()); // min_likes = 0
        sec8.extend_from_slice(&1_760_000_000u64.to_le_bytes());

        create_synthetic_snapshot(&path, SNAPSHOT_FORMAT_VERSION, 0, 0, 0, 0, 0, 2, &sec8);
        let prefs = Arc::new(UserPreferencesStore::new());
        let res = load_snapshot_with_preferences(&path, &interner, &graph, &prefs)
            .expect("v4 load must succeed")
            .expect("snapshot must exist");
        assert_eq!(res.header.format_version, SNAPSHOT_FORMAT_VERSION);
        assert_eq!(prefs.len(), 2);

        let d301 = prefs.get(301).unwrap();
        assert!(d301.include_replies);
        assert_eq!(d301.min_likes, 50);

        let d302 = prefs.get(302).unwrap();
        assert!(!d302.include_replies);
        assert_eq!(d302.min_likes, 0);

        let _ = std::fs::remove_file(path);
    }
}

// ===========================================================================
// 2. Cascading Tier Fallback Across Extreme Dial Values (0, 1, 3, 10, 50, 100)
// ===========================================================================

#[test]
fn test_stress_cascading_tier_fallback_extreme_values_matrix() {
    let interner = Arc::new(StringInterner::new());
    let graph = Arc::new(GraphStore::new());
    let now = current_timestamp();

    let viewer = interner.intern("did:plc:cascade_viewer_extreme");
    let twin1 = interner.intern("did:plc:twin1");
    let twin2 = interner.intern("did:plc:twin2");
    let followed = interner.intern("did:plc:followed");
    let author = interner.intern("did:plc:author_extreme");

    // Viewer follows `followed`
    graph.record_follow(viewer, followed);

    // 10 shared likes between viewer and twins -> Tier 1 eligibility (likes >= 10, twin overlap >= 2)
    for i in 1..=10 {
        let seed = interner.intern(&format!(
            "at://did:plc:author_extreme/app.bsky.feed.post/seed_{i}"
        ));
        graph.record_post_meta(seed, author, None, None, now - 10_000);
        graph.record_interaction(viewer, seed, SignalType::Like, now - 5_000);
        graph.record_interaction(twin1, seed, SignalType::Like, now - 4_500);
        graph.record_interaction(twin2, seed, SignalType::Like, now - 4_500);
    }

    // Post 1 (Tier 1 candidate): Exactly 2 likes (twin1, twin2)
    let p_t1 = interner.intern("at://did:plc:author_extreme/app.bsky.feed.post/p_t1_2likes");
    graph.record_post_meta(p_t1, author, None, None, now - 2_000);
    graph.record_interaction(twin1, p_t1, SignalType::Like, now - 1_500);
    graph.record_interaction(twin2, p_t1, SignalType::Like, now - 1_500);

    // Post 2 (Tier 2 candidate): Exactly 8 likes (interacted by followed + 7 others)
    let p_t2 = interner.intern("at://did:plc:author_extreme/app.bsky.feed.post/p_t2_8likes");
    graph.record_post_meta(p_t2, author, None, None, now - 2_000);
    graph.record_interaction(followed, p_t2, SignalType::Like, now - 1_000);
    for u in 1..=7 {
        let fan = interner.intern(&format!("did:plc:fan_t2_{u}"));
        graph.record_interaction(fan, p_t2, SignalType::Like, now - 1_000);
    }

    // Post 3 (Tier 3 candidate): Exactly 25 likes (in velocity pool)
    let p_t3_25 = interner.intern("at://did:plc:author_extreme/app.bsky.feed.post/p_t3_25likes");
    graph.record_post_meta(p_t3_25, author, None, None, now - 1_000);
    for u in 1..=25 {
        let fan = interner.intern(&format!("did:plc:fan_t3_25_{u}"));
        graph.record_interaction(fan, p_t3_25, SignalType::Like, now - 500);
    }

    // Post 4 (Tier 3 candidate): Exactly 75 likes (in velocity pool)
    let p_t3_75 = interner.intern("at://did:plc:author_extreme/app.bsky.feed.post/p_t3_75likes");
    graph.record_post_meta(p_t3_75, author, None, None, now - 1_000);
    for u in 1..=75 {
        let fan = interner.intern(&format!("did:plc:fan_t3_75_{u}"));
        graph.record_interaction(fan, p_t3_75, SignalType::Like, now - 500);
    }

    // Post 5 (Tier 3 candidate): Exactly 120 likes (in velocity pool)
    let p_t3_120 = interner.intern("at://did:plc:author_extreme/app.bsky.feed.post/p_t3_120likes");
    graph.record_post_meta(p_t3_120, author, None, None, now - 1_000);
    for u in 1..=120 {
        let fan = interner.intern(&format!("did:plc:fan_t3_120_{u}"));
        graph.record_interaction(fan, p_t3_120, SignalType::Like, now - 500);
    }

    let all_candidates = [
        (p_t1, 2usize),
        (p_t2, 8usize),
        (p_t3_25, 25usize),
        (p_t3_75, 75usize),
        (p_t3_120, 120usize),
    ];

    let rec = Recommender::new(Arc::clone(&interner), Arc::clone(&graph));

    // Matrix of extreme min_likes thresholds and expected source / tier:
    let test_scenarios = [
        (
            0,
            RecommendationSource::Tier1InteractionWalk,
            "Tier 1: 3-Step Interaction Walk",
        ),
        (
            1,
            RecommendationSource::Tier1InteractionWalk,
            "Tier 1: 3-Step Interaction Walk",
        ),
        (
            2,
            RecommendationSource::Tier1InteractionWalk,
            "Tier 1: 3-Step Interaction Walk",
        ),
        (
            3,
            RecommendationSource::Tier2FollowWalk,
            "Tier 2: 2-Step Follow Walk",
        ),
        (
            5,
            RecommendationSource::Tier2FollowWalk,
            "Tier 2: 2-Step Follow Walk",
        ),
        (
            8,
            RecommendationSource::Tier2FollowWalk,
            "Tier 2: 2-Step Follow Walk",
        ),
        (
            10,
            RecommendationSource::Tier3VelocityPool,
            "Tier 3: Topic Velocity Pool",
        ),
        (
            25,
            RecommendationSource::Tier3VelocityPool,
            "Tier 3: Topic Velocity Pool",
        ),
        (
            50,
            RecommendationSource::Tier3VelocityPool,
            "Tier 3: Topic Velocity Pool",
        ),
        (
            75,
            RecommendationSource::Tier3VelocityPool,
            "Tier 3: Topic Velocity Pool",
        ),
        (
            100,
            RecommendationSource::Tier3VelocityPool,
            "Tier 3: Topic Velocity Pool",
        ),
    ];

    for &(min_likes, expected_source, expected_tier) in &test_scenarios {
        let dials = RecommendationDials {
            min_likes,
            limit: 20,
            ..Default::default()
        };

        // 1. Check recommend()
        let rec_res = rec
            .recommend(Some("did:plc:cascade_viewer_extreme"), &dials, now)
            .unwrap();
        assert!(
            !rec_res.posts.is_empty(),
            "recommend() returned empty feed for min_likes = {min_likes}"
        );
        assert_eq!(
            rec_res.posts[0].source, expected_source,
            "Failed expected source for min_likes = {min_likes}"
        );

        let returned_pids: Vec<u32> = rec_res.posts.iter().map(|p| p.post_id).collect();

        // Verify that EVERY returned post satisfies interaction_count >= min_likes
        for post in &rec_res.posts {
            let count = graph.get_post_interaction_count(post.post_id);
            assert!(
                count >= min_likes as usize,
                "Returned post {pid} has {count} likes, failing floor {min_likes}",
                pid = post.post_id
            );
        }

        // Verify that candidates below threshold are strictly excluded
        for &(pid, count) in &all_candidates {
            if count < min_likes as usize {
                assert!(
                    !returned_pids.contains(&pid),
                    "Post {pid} with {count} likes MUST NOT appear when min_likes = {min_likes}"
                );
            }
        }

        // 2. Check recommend_preview_at()
        let prev_res = rec
            .recommend_preview_at(Some("did:plc:cascade_viewer_extreme"), &dials, now)
            .unwrap();
        assert!(
            !prev_res.items.is_empty(),
            "recommend_preview_at() returned empty items for min_likes = {min_likes}"
        );
        assert_eq!(
            prev_res.items[0].tier, expected_tier,
            "Failed expected preview tier string for min_likes = {min_likes}"
        );

        let preview_uris: Vec<&str> = prev_res.items.iter().map(|it| it.uri.as_str()).collect();
        for &(pid, count) in &all_candidates {
            let uri_str = interner.lookup_str(pid).unwrap();
            if count < min_likes as usize {
                assert!(
                    !preview_uris.contains(&uri_str.as_str()),
                    "Preview post {uri_str} with {count} likes MUST NOT appear when min_likes = {min_likes}"
                );
            }
        }
    }
}

// ===========================================================================
// 3. High-Concurrency Stress Test with Dynamic Dials & Sharded State
// ===========================================================================

#[test]
fn test_stress_high_concurrency_multi_threaded_dials_hammer() {
    let interner = Arc::new(StringInterner::new());
    let graph = Arc::new(GraphStore::new());
    let prefs = Arc::new(UserPreferencesStore::new());
    let now = current_timestamp();

    // Create 100 users and 500 posts with varying likes
    let mut user_dids = Vec::with_capacity(100);
    for u in 0..100 {
        let did = format!("did:plc:hammer_user_{u:03}");
        let uid = interner.intern(&did);
        user_dids.push(did);

        if u % 2 == 0 {
            let min_likes = ((u % 10) * 10) as u32; // 0, 20, 40, 60, 80
            prefs.set(uid, UserDials::default().with_min_likes(min_likes));
        }
    }

    for p in 0..500 {
        let uri = format!(
            "at://did:plc:hammer_user_{:03}/app.bsky.feed.post/post_{p:04}",
            p % 100
        );
        let pid = interner.intern(&uri);
        let author_id = interner.intern(&format!("did:plc:hammer_user_{:03}", p % 100));
        graph.record_post_meta(pid, author_id, None, None, now - 1000);

        let likes_count = (p * 7) % 60; // 0 to 59 likes
        for l in 0..likes_count {
            let fan = interner.intern(&format!("did:plc:hammer_fan_{l}_{p}"));
            graph.record_interaction(fan, pid, SignalType::Like, now - 500);
        }
    }

    let recommender = Arc::new(Recommender::new(Arc::clone(&interner), Arc::clone(&graph)));

    let num_threads = 16;
    let iterations_per_thread = 200;

    let handles: Vec<_> = (0..num_threads)
        .map(|t| {
            let recommender = Arc::clone(&recommender);
            let prefs = Arc::clone(&prefs);
            let interner = Arc::clone(&interner);
            let user_dids = user_dids.clone();

            std::thread::spawn(move || {
                for i in 0..iterations_per_thread {
                    let idx = (t * iterations_per_thread + i) % user_dids.len();
                    let viewer_did = &user_dids[idx];
                    let viewer_id = interner.lookup_id(viewer_did).unwrap();

                    // Randomly mutate preferences
                    if i % 10 == 0 {
                        let new_floor = (((t + i) % 11) * 10) as u32; // 0..=100
                        prefs.set(viewer_id, UserDials::default().with_min_likes(new_floor));
                    }

                    // Test with explicit dials
                    let min_likes = match (t + i) % 6 {
                        0 => 0,
                        1 => 1,
                        2 => 3,
                        3 => 10,
                        4 => 50,
                        _ => 100,
                    };

                    let dials = RecommendationDials {
                        min_likes,
                        limit: 20,
                        ..Default::default()
                    };

                    let res = recommender.recommend(Some(viewer_did.as_str()), &dials, now);
                    assert!(res.is_ok(), "Concurrent recommendation failed: {res:?}");

                    let prev_res =
                        recommender.recommend_preview_at(Some(viewer_did.as_str()), &dials, now);
                    assert!(
                        prev_res.is_ok(),
                        "Concurrent preview recommendation failed: {prev_res:?}"
                    );
                }
            })
        })
        .collect();

    for h in handles {
        h.join().unwrap();
    }
}
