//! Adversarial stress and durability tests for Milestone 3 (M3).
//!
//! Stress tests:
//! 1. Snapshot cross-version migrations (v1 -> v4, v2 -> v4, v3 -> v4, v4 -> v4 roundtrips).
//! 2. Section 8 corruption and CRC32 payload defense in v4.
//! 3. Validation boundary matrix and query parameter parsing for `min_likes` and `engagement_floor`.
//! 4. Strict candidate exclusion under multi-tier recommender and preview endpoints.

#![forbid(unsafe_code)]
#![allow(
    clippy::all,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    missing_docs
)]

use std::fs::File;
use std::io::{Read, Seek, SeekFrom, Write};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use crc32fast::Hasher;
use for_your_consideration::graph::GraphStore;
use for_your_consideration::interner::StringInterner;
use for_your_consideration::preferences::UserPreferencesStore;
use for_your_consideration::recommender::Recommender;
use for_your_consideration::snapshot::{
    load_snapshot_with_preferences, save_snapshot_with_preferences, HEADER_SIZE,
    SNAPSHOT_FORMAT_VERSION, SNAPSHOT_FORMAT_VERSION_V1, SNAPSHOT_FORMAT_VERSION_V2,
    SNAPSHOT_FORMAT_VERSION_V3, SNAPSHOT_MAGIC,
};
use for_your_consideration::types::{
    FeedPreviewQuery, RecommendationDials, SignalType, TopicWeights, UserDials, CURATED_MIN_LIKES,
    DEFAULT_MIN_LIKES, EMERGING_MIN_LIKES, MAX_ENGAGEMENT_FLOOR, MIN_ENGAGEMENT_FLOOR,
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

// ===========================================================================
// 1. Snapshot Cross-Version Migrations
// ===========================================================================

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

    // Section 1: Strings (empty string table: 0 strings)
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

#[test]
fn test_snapshot_v1_migration_loads_with_clean_preferences() {
    let path = unique_temp_path("v1_snap");
    create_synthetic_snapshot(&path, SNAPSHOT_FORMAT_VERSION_V1, 0, 0, 0, 0, 0, 0, &[]);

    let interner = Arc::new(StringInterner::new());
    let graph = Arc::new(GraphStore::new());
    let prefs = Arc::new(UserPreferencesStore::new());

    let res = load_snapshot_with_preferences(&path, &interner, &graph, &prefs)
        .expect("v1 load must succeed")
        .expect("snapshot must exist");

    assert_eq!(res.header.format_version, SNAPSHOT_FORMAT_VERSION_V1);
    assert_eq!(prefs.len(), 0);

    let _ = std::fs::remove_file(path);
}

#[test]
fn test_snapshot_v2_migration_populates_preferences_with_defaults() {
    let path = unique_temp_path("v2_snap");

    // Build Section 8 for v2: 1 user preference record (40 bytes per record)
    // Layout: uid (u32), freshness (f32), serendipity (f32), art (f32), tech (f32), science (f32), news (f32), culture (f32), updated_at (u64)
    let mut sec8 = Vec::new();
    sec8.extend_from_slice(&1u32.to_le_bytes()); // 1 profile
    sec8.extend_from_slice(&42u32.to_le_bytes()); // uid = 42
    sec8.extend_from_slice(&(72.0f32 * 3600.0).to_le_bytes()); // freshness = 72h
    sec8.extend_from_slice(&0.35f32.to_le_bytes()); // serendipity = 0.35
    sec8.extend_from_slice(&2.0f32.to_le_bytes()); // art
    sec8.extend_from_slice(&1.5f32.to_le_bytes()); // tech
    sec8.extend_from_slice(&0.5f32.to_le_bytes()); // science
    sec8.extend_from_slice(&1.0f32.to_le_bytes()); // news
    sec8.extend_from_slice(&3.0f32.to_le_bytes()); // culture
    sec8.extend_from_slice(&1_700_000_000u64.to_le_bytes()); // updated_at

    create_synthetic_snapshot(&path, SNAPSHOT_FORMAT_VERSION_V2, 0, 0, 0, 0, 0, 1, &sec8);

    let interner = Arc::new(StringInterner::new());
    let graph = Arc::new(GraphStore::new());
    let prefs = Arc::new(UserPreferencesStore::new());

    let res = load_snapshot_with_preferences(&path, &interner, &graph, &prefs)
        .expect("v2 load must succeed")
        .expect("snapshot must exist");

    assert_eq!(res.header.format_version, SNAPSHOT_FORMAT_VERSION_V2);
    assert_eq!(prefs.len(), 1);

    let user_dials = prefs.get(42).expect("User 42 must be restored");
    assert_eq!(user_dials.freshness_half_life_secs, 72.0 * 3600.0);
    assert_eq!(user_dials.serendipity_ratio, 0.35);
    assert_eq!(user_dials.topic_weights.art, 2.0);
    assert_eq!(user_dials.topic_weights.tech, 1.5);
    assert_eq!(user_dials.topic_weights.science, 0.5);
    assert_eq!(user_dials.topic_weights.news, 1.0);
    assert_eq!(user_dials.topic_weights.culture, 3.0);
    // Backward compatibility defaults for v2 migration:
    assert!(!user_dials.include_replies);
    assert_eq!(user_dials.min_likes, DEFAULT_MIN_LIKES);
    assert_eq!(user_dials.updated_at_secs, 1_700_000_000);

    let _ = std::fs::remove_file(path);
}

#[test]
fn test_snapshot_v3_migration_populates_include_replies_and_default_min_likes() {
    let path = unique_temp_path("v3_snap");

    // Build Section 8 for v3: 1 user preference record (41 bytes per record)
    // Layout: uid (u32), 7x f32 (28 bytes), include_replies (u8), updated_at (u64)
    let mut sec8 = Vec::new();
    sec8.extend_from_slice(&1u32.to_le_bytes()); // 1 profile
    sec8.extend_from_slice(&99u32.to_le_bytes()); // uid = 99
    sec8.extend_from_slice(&(24.0f32 * 3600.0).to_le_bytes()); // freshness = 24h
    sec8.extend_from_slice(&0.10f32.to_le_bytes()); // serendipity = 0.10
    sec8.extend_from_slice(&1.0f32.to_le_bytes()); // art
    sec8.extend_from_slice(&1.0f32.to_le_bytes()); // tech
    sec8.extend_from_slice(&1.0f32.to_le_bytes()); // science
    sec8.extend_from_slice(&1.0f32.to_le_bytes()); // news
    sec8.extend_from_slice(&1.0f32.to_le_bytes()); // culture
    sec8.extend_from_slice(&1u8.to_le_bytes()); // include_replies = true (1)
    sec8.extend_from_slice(&1_750_000_000u64.to_le_bytes()); // updated_at

    create_synthetic_snapshot(&path, SNAPSHOT_FORMAT_VERSION_V3, 0, 0, 0, 0, 0, 1, &sec8);

    let interner = Arc::new(StringInterner::new());
    let graph = Arc::new(GraphStore::new());
    let prefs = Arc::new(UserPreferencesStore::new());

    let res = load_snapshot_with_preferences(&path, &interner, &graph, &prefs);

    // Check whether v3 snapshot loading is supported:
    match res {
        Ok(Some(loaded)) => {
            assert_eq!(loaded.header.format_version, SNAPSHOT_FORMAT_VERSION_V3);
            let user_dials = prefs.get(99).expect("User 99 must be restored");
            assert!(
                user_dials.include_replies,
                "include_replies should be preserved from v3"
            );
            assert_eq!(
                user_dials.min_likes, DEFAULT_MIN_LIKES,
                "min_likes should default to DEFAULT_MIN_LIKES in v3"
            );
            assert_eq!(user_dials.updated_at_secs, 1_750_000_000);
        }
        Ok(None) => panic!("Snapshot file exists but returned None"),
        Err(e) => {
            panic!("Snapshot v3 migration failed: {e}");
        }
    }

    let _ = std::fs::remove_file(path);
}

#[test]
fn test_snapshot_v4_roundtrip_all_boundary_values() {
    let interner = Arc::new(StringInterner::new());
    let graph = Arc::new(GraphStore::new());
    let prefs = Arc::new(UserPreferencesStore::new());
    let now = current_timestamp();

    let test_cases = vec![
        ("did:plc:min_floor", 0, true),
        ("did:plc:emerging", 1, false),
        ("did:plc:default", 3, true),
        ("did:plc:curated", 10, false),
        ("did:plc:max_floor", 100, true),
        ("did:plc:arbitrary_57", 57, false),
    ];

    for &(did, min_likes, include_replies) in &test_cases {
        let uid = interner.intern(did);
        let dials = UserDials {
            freshness_half_life_secs: 18.0 * 3600.0,
            serendipity_ratio: 0.22,
            topic_weights: TopicWeights {
                art: 1.1,
                tech: 2.2,
                science: 3.3,
                news: 4.4,
                culture: 0.5,
            },
            include_replies,
            min_likes,
            updated_at_secs: now + u64::from(min_likes),
        };
        prefs.set(uid, dials);
    }

    let path = unique_temp_path("v4_roundtrip");
    save_snapshot_with_preferences(&path, &interner, &graph, &prefs, now).unwrap();

    let loaded_interner = Arc::new(StringInterner::new());
    let loaded_graph = Arc::new(GraphStore::new());
    let loaded_prefs = Arc::new(UserPreferencesStore::new());

    let loaded =
        load_snapshot_with_preferences(&path, &loaded_interner, &loaded_graph, &loaded_prefs)
            .unwrap()
            .unwrap();

    assert_eq!(loaded.header.format_version, SNAPSHOT_FORMAT_VERSION);
    assert_eq!(loaded.header.num_preferences as usize, test_cases.len());

    for &(did, min_likes, include_replies) in &test_cases {
        let uid = loaded_interner.intern(did);
        let restored = loaded_prefs.get(uid).expect("Profile must be restored");
        assert_eq!(restored.min_likes, min_likes);
        assert_eq!(restored.include_replies, include_replies);
        assert_eq!(restored.freshness_half_life_secs, 18.0 * 3600.0);
        assert_eq!(restored.serendipity_ratio, 0.22);
        assert_eq!(restored.topic_weights.art, 1.1);
        assert_eq!(restored.topic_weights.tech, 2.2);
        assert_eq!(restored.topic_weights.science, 3.3);
        assert_eq!(restored.topic_weights.news, 4.4);
        assert_eq!(restored.topic_weights.culture, 0.5);
    }

    let _ = std::fs::remove_file(path);
}

#[test]
fn test_snapshot_v4_corruption_defense() {
    let interner = Arc::new(StringInterner::new());
    let graph = Arc::new(GraphStore::new());
    let prefs = Arc::new(UserPreferencesStore::new());
    let now = current_timestamp();

    let uid = interner.intern("did:plc:corrupt_test_user");
    prefs.set(uid, UserDials::default().with_min_likes(7));

    let path = unique_temp_path("v4_corrupt");
    save_snapshot_with_preferences(&path, &interner, &graph, &prefs, now).unwrap();

    // Corrupt one byte in the payload
    let mut file = std::fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(&path)
        .unwrap();
    let mut bytes = Vec::new();
    file.read_to_end(&mut bytes).unwrap();

    // Flip a byte near the end (in Section 8)
    let len = bytes.len();
    bytes[len - 5] ^= 0xFF;

    file.seek(SeekFrom::Start(0)).unwrap();
    file.write_all(&bytes).unwrap();
    file.flush().unwrap();
    drop(file);

    let loaded_interner = Arc::new(StringInterner::new());
    let loaded_graph = Arc::new(GraphStore::new());
    let loaded_prefs = Arc::new(UserPreferencesStore::new());

    let res = load_snapshot_with_preferences(&path, &loaded_interner, &loaded_graph, &loaded_prefs);
    assert!(
        res.is_err(),
        "Corrupted snapshot payload MUST fail CRC32 verification"
    );
    let err_msg = res.unwrap_err().to_string();
    assert!(err_msg.contains("Payload CRC32 mismatch") || err_msg.contains("CRC32"));

    let _ = std::fs::remove_file(path);
}

// ===========================================================================
// 2. Validation Bounds and Query Parsing Stress Matrix
// ===========================================================================

#[test]
fn test_engagement_floor_parsing_exhaustive_matrix() {
    // 1. Standard Presets (case-insensitive, whitespace trimmed)
    let presets = [
        ("emerging", EMERGING_MIN_LIKES),
        ("emerge", EMERGING_MIN_LIKES),
        ("EMERGING", EMERGING_MIN_LIKES),
        ("  emerging  ", EMERGING_MIN_LIKES),
        ("1", EMERGING_MIN_LIKES),
        ("1+", EMERGING_MIN_LIKES),
        ("balanced", DEFAULT_MIN_LIKES),
        ("default", DEFAULT_MIN_LIKES),
        ("BALANCED", DEFAULT_MIN_LIKES),
        ("  balanced  ", DEFAULT_MIN_LIKES),
        ("3", DEFAULT_MIN_LIKES),
        ("3+", DEFAULT_MIN_LIKES),
        ("curated", CURATED_MIN_LIKES),
        ("high", CURATED_MIN_LIKES),
        ("CURATED", CURATED_MIN_LIKES),
        ("10", CURATED_MIN_LIKES),
        ("10+", CURATED_MIN_LIKES),
        ("all", MIN_ENGAGEMENT_FLOOR),
        ("none", MIN_ENGAGEMENT_FLOOR),
        ("off", MIN_ENGAGEMENT_FLOOR),
        ("0", MIN_ENGAGEMENT_FLOOR),
        ("0+", MIN_ENGAGEMENT_FLOOR),
    ];

    for (input, expected) in presets {
        assert_eq!(
            RecommendationDials::parse_engagement_floor(Some(input)),
            expected,
            "Failed parsing preset '{input}'"
        );
    }

    // 2. Numeric & Clamping
    assert_eq!(RecommendationDials::parse_engagement_floor(Some("0")), 0);
    assert_eq!(RecommendationDials::parse_engagement_floor(Some("1")), 1);
    assert_eq!(RecommendationDials::parse_engagement_floor(Some("5")), 5);
    assert_eq!(RecommendationDials::parse_engagement_floor(Some("25+")), 25);
    assert_eq!(
        RecommendationDials::parse_engagement_floor(Some("100")),
        100
    );
    assert_eq!(
        RecommendationDials::parse_engagement_floor(Some("100+")),
        100
    );
    assert_eq!(
        RecommendationDials::parse_engagement_floor(Some("101")),
        100
    );
    assert_eq!(
        RecommendationDials::parse_engagement_floor(Some("99999")),
        100
    );

    // 3. Fallbacks on invalid strings / None
    assert_eq!(
        RecommendationDials::parse_engagement_floor(None),
        DEFAULT_MIN_LIKES
    );
    assert_eq!(
        RecommendationDials::parse_engagement_floor(Some("")),
        DEFAULT_MIN_LIKES
    );
    assert_eq!(
        RecommendationDials::parse_engagement_floor(Some("   ")),
        DEFAULT_MIN_LIKES
    );
    assert_eq!(
        RecommendationDials::parse_engagement_floor(Some("-5")),
        DEFAULT_MIN_LIKES
    );
    assert_eq!(
        RecommendationDials::parse_engagement_floor(Some("invalid_str")),
        DEFAULT_MIN_LIKES
    );
    assert_eq!(
        RecommendationDials::parse_engagement_floor(Some("NaN")),
        DEFAULT_MIN_LIKES
    );
    assert_eq!(
        RecommendationDials::parse_engagement_floor(Some("null")),
        DEFAULT_MIN_LIKES
    );
}

#[test]
fn test_user_dials_boundary_validation_matrix() {
    let mut dials = UserDials::default();

    // Test min_likes across boundaries
    for valid_val in [0, 1, 3, 10, 50, 99, 100] {
        dials.min_likes = valid_val;
        assert!(
            dials.validate().is_ok(),
            "min_likes = {valid_val} should be valid"
        );
    }

    for invalid_val in [101, 102, 500, 10_000, u32::MAX] {
        dials.min_likes = invalid_val;
        let res = dials.validate();
        assert!(
            res.is_err(),
            "min_likes = {invalid_val} should fail validation"
        );
        assert!(res.unwrap_err().contains("Minimum engagement floor"));
    }
}

#[test]
fn test_feed_preview_query_to_dials_conversion() {
    // 1. Default when min_likes and engagement_floor are None
    let q1 = FeedPreviewQuery::default();
    let d1 = q1.to_dials();
    assert_eq!(d1.min_likes, DEFAULT_MIN_LIKES);

    // 2. Explicit numeric min_likes
    let q2 = FeedPreviewQuery {
        min_likes: Some(15),
        ..Default::default()
    };
    let d2 = q2.to_dials();
    assert_eq!(d2.min_likes, 15);

    // 3. Explicit numeric min_likes clamped
    let q3 = FeedPreviewQuery {
        min_likes: Some(250),
        ..Default::default()
    };
    let d3 = q3.to_dials();
    assert_eq!(d3.min_likes, MAX_ENGAGEMENT_FLOOR);

    // 4. String preset engagement_floor
    let q4 = FeedPreviewQuery {
        engagement_floor: Some("curated".to_string()),
        ..Default::default()
    };
    let d4 = q4.to_dials();
    assert_eq!(d4.min_likes, CURATED_MIN_LIKES);

    // 5. min_likes takes precedence if both provided
    let q5 = FeedPreviewQuery {
        min_likes: Some(7),
        engagement_floor: Some("emerging".to_string()),
        ..Default::default()
    };
    let d5 = q5.to_dials();
    assert_eq!(d5.min_likes, 7);
}

// ===========================================================================
// 3. Strict Candidate Exclusion Recommender Tests
// ===========================================================================

#[test]
fn test_strict_candidate_exclusion_exhaustive_thresholds() {
    let interner = Arc::new(StringInterner::new());
    let graph = Arc::new(GraphStore::new());
    let rec = Recommender::new(Arc::clone(&interner), Arc::clone(&graph));
    let now = current_timestamp();

    let viewer = interner.intern("did:plc:strict_viewer");
    let twin = interner.intern("did:plc:strict_twin");
    let seed_author = interner.intern("did:plc:strict_seed_author");

    // Tier 1 Seed overlap (10 posts)
    for i in 1..=10 {
        let sp = interner.intern(&format!(
            "at://did:plc:strict_seed_author/app.bsky.feed.post/seed_{i}"
        ));
        graph.record_post_meta(sp, seed_author, None, None, now - 5000);
        graph.record_interaction(viewer, sp, SignalType::Like, now - 4000);
        if i <= 3 {
            graph.record_interaction(twin, sp, SignalType::Like, now - 3000);
        }
    }

    // Create 6 candidate posts with exactly 0, 1, 2, 3, 9, 10 likes:
    // (Note: Candidate with 0 likes won't be liked by twin, so we introduce another author/candidate mechanism)
    let post_likes_counts = [(0, 0), (1, 1), (2, 2), (3, 3), (4, 9), (5, 10)];
    let mut post_ids = Vec::new();

    for &(idx, count) in &post_likes_counts {
        let author = interner.intern(&format!("did:plc:author_strict_{idx}"));
        let pid = interner.intern(&format!(
            "at://did:plc:author_strict_{idx}/app.bsky.feed.post/cand_{count}likes"
        ));
        graph.record_post_meta(pid, author, None, None, now - 2000);

        // Twin likes posts 1..5 to make them eligible in Tier 1
        if count > 0 {
            graph.record_interaction(twin, pid, SignalType::Like, now - 1000);
            for u in 1..count {
                let fan = interner.intern(&format!("did:plc:fan_{idx}_{u}"));
                graph.record_interaction(fan, pid, SignalType::Like, now - 1000);
            }
        }
        post_ids.push((pid, count));
    }

    // Test different min_likes thresholds:
    let test_thresholds = [
        (0, vec![1, 2, 3, 9, 10]), // All Tier 1 candidates qualify
        (1, vec![1, 2, 3, 9, 10]), // 1+ likes qualify
        (2, vec![2, 3, 9, 10]),    // 2+ likes qualify (1 like excluded)
        (3, vec![3, 9, 10]),       // 3+ likes qualify (1, 2 excluded)
        (4, vec![9, 10]),          // 4+ likes qualify (1, 2, 3 excluded)
        (10, vec![10]),            // 10+ likes qualify (1..9 excluded)
        (11, vec![]),              // None qualify
    ];

    for (threshold, expected_counts) in test_thresholds {
        let dials = RecommendationDials {
            min_likes: threshold,
            limit: 50,
            ..Default::default()
        };

        // 1. Test recommend()
        let rec_res = rec
            .recommend(Some("did:plc:strict_viewer"), &dials, now)
            .unwrap();
        let returned_pids: Vec<u32> = rec_res.posts.iter().map(|p| p.post_id).collect();

        // Verify that NO post with interaction count < threshold is present
        for &(pid, count) in &post_ids {
            let contains = returned_pids.contains(&pid);
            if expected_counts.contains(&count) {
                assert!(
                    contains,
                    "Expected post with {count} likes to be returned for threshold {threshold}"
                );
            } else {
                assert!(
                    !contains,
                    "Post with {count} likes MUST NOT be returned when min_likes = {threshold}"
                );
            }
        }

        // 2. Test recommend_preview_at()
        let prev_res = rec
            .recommend_preview_at(Some("did:plc:strict_viewer"), &dials, now)
            .unwrap();
        assert_eq!(
            prev_res.items.len(),
            expected_counts.len(),
            "Preview count mismatch for threshold {threshold}"
        );
    }
}
