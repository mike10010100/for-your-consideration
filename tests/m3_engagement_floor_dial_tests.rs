//! Integration test suite for Milestone 3: Configurable Engagement Floor Dial & Full Integration.
//!
//! Validates:
//! 1. Domain Models, Dials & DTOs (parsing, validation, builder methods, conversions).
//! 2. Recommender filtering across Tier 1, Tier 2, and Tier 3 under various `min_likes` settings.
//! 3. Snapshot format version 4 serialization, roundtrip fidelity, and v1/v2/v3 backward compatibility.
//! 4. XRPC 3-tier precedence hierarchy (`min_likes`/`engagement_floor` query override -> persisted dials -> default).
//! 5. REST `/api/preferences` GET, POST, DELETE lifecycle with `min_likes`.

#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic, missing_docs)]

mod common;
use common::generate_mock_jwt;

use std::fs;
use std::io::Write;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use axum::body::Body;
use axum::http::{Request, StatusCode};
use crc32fast::Hasher;
use http_body_util::BodyExt;
use tower::ServiceExt;

use for_your_consideration::auth::generate_session_token;
use for_your_consideration::graph::GraphStore;
use for_your_consideration::interner::StringInterner;
use for_your_consideration::preferences::UserPreferencesStore;
use for_your_consideration::recommender::Recommender;
use for_your_consideration::server::{create_xrpc_router, AppState};
use for_your_consideration::snapshot::{
    load_snapshot_with_preferences, save_snapshot_with_preferences, HEADER_SIZE,
    SNAPSHOT_FORMAT_VERSION, SNAPSHOT_FORMAT_VERSION_V1, SNAPSHOT_FORMAT_VERSION_V2,
    SNAPSHOT_FORMAT_VERSION_V3, SNAPSHOT_MAGIC,
};
use for_your_consideration::types::{
    FeedSkeletonResponse, GenericStatusResponse, PreferencesResponseDto, RecommendationDials,
    RecommendationSource, SavePreferencesRequestBody, SignalType, TopicWeights, UserDials,
    CURATED_MIN_LIKES, DEFAULT_MIN_LIKES, EMERGING_MIN_LIKES, MAX_ENGAGEMENT_FLOOR,
    MIN_ENGAGEMENT_FLOOR,
};

fn current_time_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
}

fn create_synthetic_snapshot_file(
    file_path: &str,
    version: u16,
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
    header[8..16].copy_from_slice(&current_time_secs().to_le_bytes());
    header[16..24].copy_from_slice(&0u64.to_le_bytes()); // cursor
    header[24..28].copy_from_slice(&0u32.to_le_bytes()); // flags
    header[28..32].copy_from_slice(&0u32.to_le_bytes()); // num_strings = 0
    header[32..36].copy_from_slice(&0u32.to_le_bytes()); // num_users = 0
    header[36..44].copy_from_slice(&0u64.to_le_bytes()); // total_edges = 0
    header[44..48].copy_from_slice(&0u32.to_le_bytes()); // num_followers = 0
    header[48..52].copy_from_slice(&0u32.to_le_bytes()); // num_metadata = 0
    header[52..56].copy_from_slice(&payload_crc.to_le_bytes());

    // Header CRC over bytes 0..56
    let mut h_hasher = Hasher::new();
    h_hasher.update(&header[0..56]);
    let header_crc = h_hasher.finalize();
    header[56..60].copy_from_slice(&header_crc.to_le_bytes());
    header[60..64].copy_from_slice(&num_preferences.to_le_bytes());

    let mut file = fs::File::create(file_path).unwrap();
    file.write_all(&header).unwrap();
    file.write_all(&payload).unwrap();
    file.flush().unwrap();
}

fn unique_temp_path(prefix: &str) -> String {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_nanos();
    format!("./target/{prefix}_{now}.bin")
}

// ===========================================================================
// 1. Domain Models, Dials & DTOs Tests
// ===========================================================================

#[test]
fn test_m3_dials_parsing_and_aliases() {
    assert_eq!(
        RecommendationDials::parse_engagement_floor(Some("emerging")),
        EMERGING_MIN_LIKES
    );
    assert_eq!(
        RecommendationDials::parse_engagement_floor(Some("EMERGING")),
        EMERGING_MIN_LIKES
    );
    assert_eq!(
        RecommendationDials::parse_engagement_floor(Some("balanced")),
        DEFAULT_MIN_LIKES
    );
    assert_eq!(
        RecommendationDials::parse_engagement_floor(Some("curated")),
        CURATED_MIN_LIKES
    );
    assert_eq!(
        RecommendationDials::parse_engagement_floor(Some("all")),
        MIN_ENGAGEMENT_FLOOR
    );
    assert_eq!(
        RecommendationDials::parse_engagement_floor(Some("none")),
        MIN_ENGAGEMENT_FLOOR
    );
    assert_eq!(
        RecommendationDials::parse_engagement_floor(Some("off")),
        MIN_ENGAGEMENT_FLOOR
    );

    // Numeric parsing and clamping
    assert_eq!(RecommendationDials::parse_engagement_floor(Some("5")), 5);
    assert_eq!(RecommendationDials::parse_engagement_floor(Some("0")), 0);
    assert_eq!(
        RecommendationDials::parse_engagement_floor(Some("100")),
        100
    );
    assert_eq!(
        RecommendationDials::parse_engagement_floor(Some("250")),
        MAX_ENGAGEMENT_FLOOR
    );

    // None or invalid fallback to default
    assert_eq!(
        RecommendationDials::parse_engagement_floor(None),
        DEFAULT_MIN_LIKES
    );
    assert_eq!(
        RecommendationDials::parse_engagement_floor(Some("unknown_preset")),
        DEFAULT_MIN_LIKES
    );
    assert_eq!(
        RecommendationDials::parse_engagement_floor(Some("")),
        DEFAULT_MIN_LIKES
    );
}

#[test]
fn test_m3_user_dials_validation_boundaries() {
    let mut dials = UserDials::default();
    assert_eq!(dials.min_likes, DEFAULT_MIN_LIKES);
    assert!(dials.validate().is_ok());

    dials.min_likes = 0;
    assert!(dials.validate().is_ok());

    dials.min_likes = 100;
    assert!(dials.validate().is_ok());

    dials.min_likes = 101;
    let err = dials.validate();
    assert!(err.is_err());
    assert!(err.unwrap_err().contains("Minimum engagement"));
}

#[test]
fn test_m3_dials_builders_and_conversions() {
    let rec_dials = RecommendationDials::default().with_min_likes(7);
    assert_eq!(rec_dials.min_likes, 7);

    let user_dials = UserDials::default().with_min_likes(12);
    assert_eq!(user_dials.min_likes, 12);

    let converted_rec = user_dials.to_recommendation_dials();
    assert_eq!(converted_rec.min_likes, 12);

    let back_to_user = UserDials::from_recommendation_dials(&converted_rec, 9999);
    assert_eq!(back_to_user.min_likes, 12);
    assert_eq!(back_to_user.updated_at_secs, 9999);

    let mut base_rec = RecommendationDials::default();
    user_dials.apply_to_recommendation_dials(&mut base_rec);
    assert_eq!(base_rec.min_likes, 12);
}

// ===========================================================================
// 2. Recommender Filtering Integration Tests
// ===========================================================================

#[test]
fn test_m3_recommender_filtering_tier1_tier2_tier3() {
    let interner = Arc::new(StringInterner::new());
    let graph = Arc::new(GraphStore::new());
    let rec = Recommender::new(Arc::clone(&interner), Arc::clone(&graph));
    let now = current_time_secs();

    let viewer = interner.intern("did:plc:m3_viewer");
    let twin = interner.intern("did:plc:m3_twin");
    let author_seed = interner.intern("did:plc:m3_author_seed");
    let author_1 = interner.intern("did:plc:m3_author_1");
    let author_2 = interner.intern("did:plc:m3_author_2");
    let author_3 = interner.intern("did:plc:m3_author_3");

    // Establish Tier 1 eligibility (10 seed posts liked by viewer, 2 shared with twin)
    for i in 1..=10 {
        let sp = interner.intern(&format!(
            "at://did:plc:m3_author_seed/app.bsky.feed.post/seed_{i}"
        ));
        graph.record_post_meta(sp, author_seed, None, None, now - 2000);
        graph.record_interaction(viewer, sp, SignalType::Like, now - 1500);
        if i <= 2 {
            graph.record_interaction(twin, sp, SignalType::Like, now - 1400);
        }
    }

    // Candidate 1 (p_1like): 1 like (by twin) from author_1
    let p_1like = interner.intern("at://did:plc:m3_author_1/app.bsky.feed.post/cand_1like");
    graph.record_post_meta(p_1like, author_1, None, None, now - 1000);
    graph.record_interaction(twin, p_1like, SignalType::Like, now - 500);

    // Candidate 2 (p_3likes): 3 likes (by twin + 2 others) from author_2
    let p_3likes = interner.intern("at://did:plc:m3_author_2/app.bsky.feed.post/cand_3likes");
    graph.record_post_meta(p_3likes, author_2, None, None, now - 1000);
    graph.record_interaction(twin, p_3likes, SignalType::Like, now - 500);
    for u in 1..=2 {
        let other = interner.intern(&format!("did:plc:fan_3_{u}"));
        graph.record_interaction(other, p_3likes, SignalType::Like, now - 500);
    }

    // Candidate 3 (p_10likes): 10 likes (by twin + 9 others) from author_3
    let p_10likes = interner.intern("at://did:plc:m3_author_3/app.bsky.feed.post/cand_10likes");
    graph.record_post_meta(p_10likes, author_3, None, None, now - 1000);
    graph.record_interaction(twin, p_10likes, SignalType::Like, now - 500);
    for u in 1..=9 {
        let other = interner.intern(&format!("did:plc:fan_10_{u}"));
        graph.record_interaction(other, p_10likes, SignalType::Like, now - 500);
    }

    // Case A: min_likes = 3 (Default / Balanced) -> p_3likes and p_10likes qualify, p_1like excluded
    let dials_def = RecommendationDials {
        min_likes: DEFAULT_MIN_LIKES,
        limit: 10,
        ..Default::default()
    };
    let res_def = rec
        .recommend(Some("did:plc:m3_viewer"), &dials_def, now)
        .unwrap();
    assert_eq!(res_def.posts.len(), 2);
    assert!(res_def.posts.iter().any(|p| p.post_id == p_3likes));
    assert!(res_def.posts.iter().any(|p| p.post_id == p_10likes));
    assert!(!res_def.posts.iter().any(|p| p.post_id == p_1like));

    // Case B: min_likes = 1 (Emerging) -> all 3 qualify
    let dials_emerging = RecommendationDials {
        min_likes: EMERGING_MIN_LIKES,
        limit: 10,
        ..Default::default()
    };
    let res_emerging = rec
        .recommend(Some("did:plc:m3_viewer"), &dials_emerging, now)
        .unwrap();
    assert_eq!(res_emerging.posts.len(), 3);
    assert!(res_emerging.posts.iter().any(|p| p.post_id == p_1like));

    // Case C: min_likes = 10 (Curated) -> only p_10likes qualifies
    let dials_curated = RecommendationDials {
        min_likes: CURATED_MIN_LIKES,
        limit: 10,
        ..Default::default()
    };
    let res_curated = rec
        .recommend(Some("did:plc:m3_viewer"), &dials_curated, now)
        .unwrap();
    assert_eq!(res_curated.posts.len(), 1);
    assert_eq!(res_curated.posts[0].post_id, p_10likes);

    // Case D: recommend_preview reflects min_likes
    let prev_curated = rec
        .recommend_preview_at(Some("did:plc:m3_viewer"), &dials_curated, now)
        .unwrap();
    assert_eq!(prev_curated.items.len(), 1);
    assert_eq!(
        prev_curated.items[0].uri,
        "at://did:plc:m3_author_3/app.bsky.feed.post/cand_10likes"
    );
}

// ===========================================================================
// 3. Snapshot Format Version 4 Persistence Tests
// ===========================================================================

#[test]
fn test_m3_snapshot_v4_roundtrip_and_backward_compatibility() {
    let interner = Arc::new(StringInterner::new());
    let graph = Arc::new(GraphStore::new());
    let prefs = Arc::new(UserPreferencesStore::new());
    let now = current_time_secs();

    let user_did = "did:plc:v4_test_user";
    let uid = interner.intern(user_did);

    let custom_dials = UserDials {
        min_likes: 7,
        freshness_half_life_secs: 72.0 * 3600.0,
        serendipity_ratio: 0.25,
        topic_weights: TopicWeights::default(),
        include_replies: false,
        updated_at_secs: 1_234_567,
    };
    prefs.set(uid, custom_dials);

    let snapshot_path = unique_temp_path("snap_v4_test");
    save_snapshot_with_preferences(&snapshot_path, &interner, &graph, &prefs, now).unwrap();

    let loaded_interner = Arc::new(StringInterner::new());
    let loaded_graph = Arc::new(GraphStore::new());
    let loaded_prefs = Arc::new(UserPreferencesStore::new());

    let res = load_snapshot_with_preferences(
        &snapshot_path,
        &loaded_interner,
        &loaded_graph,
        &loaded_prefs,
    )
    .unwrap()
    .unwrap();

    assert_eq!(res.header.format_version, SNAPSHOT_FORMAT_VERSION);

    let loaded_uid = loaded_interner.intern(user_did);
    let restored_dials = loaded_prefs.get(loaded_uid).unwrap();
    assert_eq!(restored_dials.min_likes, 7);
    assert_eq!(restored_dials.freshness_half_life_secs, 72.0 * 3600.0);
    assert_eq!(restored_dials.serendipity_ratio, 0.25);
    assert_eq!(restored_dials.updated_at_secs, 1_234_567);

    let _ = fs::remove_file(&snapshot_path);
}

#[test]
fn test_m3_synthetic_snapshot_v1_migration() {
    let snapshot_path = unique_temp_path("snap_v1_synthetic");
    create_synthetic_snapshot_file(&snapshot_path, SNAPSHOT_FORMAT_VERSION_V1, 0, &[]);

    let loaded_interner = Arc::new(StringInterner::new());
    let loaded_graph = Arc::new(GraphStore::new());
    let loaded_prefs = Arc::new(UserPreferencesStore::new());

    let res = load_snapshot_with_preferences(
        &snapshot_path,
        &loaded_interner,
        &loaded_graph,
        &loaded_prefs,
    )
    .expect("v1 load must succeed")
    .expect("snapshot must exist");

    assert_eq!(res.header.format_version, SNAPSHOT_FORMAT_VERSION_V1);
    assert_eq!(loaded_prefs.len(), 0);

    let _ = fs::remove_file(&snapshot_path);
}

#[test]
fn test_m3_synthetic_snapshot_v2_migration() {
    let snapshot_path = unique_temp_path("snap_v2_synthetic");

    // Build Section 8 for v2: 1 user preference record (40 bytes per record)
    // Layout: uid (u32), 7x f32 (28 bytes), updated_at (u64)
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

    create_synthetic_snapshot_file(&snapshot_path, SNAPSHOT_FORMAT_VERSION_V2, 1, &sec8);

    let loaded_interner = Arc::new(StringInterner::new());
    let loaded_graph = Arc::new(GraphStore::new());
    let loaded_prefs = Arc::new(UserPreferencesStore::new());

    let res = load_snapshot_with_preferences(
        &snapshot_path,
        &loaded_interner,
        &loaded_graph,
        &loaded_prefs,
    )
    .expect("v2 load must succeed")
    .expect("snapshot must exist");

    assert_eq!(res.header.format_version, SNAPSHOT_FORMAT_VERSION_V2);
    assert_eq!(loaded_prefs.len(), 1);

    let user_dials = loaded_prefs.get(42).expect("User 42 must be restored");
    assert_eq!(user_dials.freshness_half_life_secs, 72.0 * 3600.0);
    assert_eq!(user_dials.serendipity_ratio, 0.35);
    assert_eq!(user_dials.topic_weights.art, 2.0);
    assert_eq!(user_dials.topic_weights.tech, 1.5);
    assert_eq!(user_dials.topic_weights.science, 0.5);
    assert_eq!(user_dials.topic_weights.news, 1.0);
    assert_eq!(user_dials.topic_weights.culture, 3.0);
    // Backward compatibility defaults for v2 migration:
    assert!(
        !user_dials.include_replies,
        "include_replies must default to false for v2"
    );
    assert_eq!(
        user_dials.min_likes, DEFAULT_MIN_LIKES,
        "min_likes must default to DEFAULT_MIN_LIKES for v2"
    );
    assert_eq!(user_dials.updated_at_secs, 1_700_000_000);

    let _ = fs::remove_file(&snapshot_path);
}

#[test]
fn test_m3_synthetic_snapshot_v3_migration() {
    let snapshot_path = unique_temp_path("snap_v3_synthetic");

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

    create_synthetic_snapshot_file(&snapshot_path, SNAPSHOT_FORMAT_VERSION_V3, 1, &sec8);

    let loaded_interner = Arc::new(StringInterner::new());
    let loaded_graph = Arc::new(GraphStore::new());
    let loaded_prefs = Arc::new(UserPreferencesStore::new());

    let res = load_snapshot_with_preferences(
        &snapshot_path,
        &loaded_interner,
        &loaded_graph,
        &loaded_prefs,
    )
    .expect("v3 load must succeed")
    .expect("snapshot must exist");

    assert_eq!(res.header.format_version, SNAPSHOT_FORMAT_VERSION_V3);
    assert_eq!(loaded_prefs.len(), 1);

    let user_dials = loaded_prefs.get(99).expect("User 99 must be restored");
    assert!(
        user_dials.include_replies,
        "include_replies should be preserved from v3"
    );
    assert_eq!(
        user_dials.min_likes, DEFAULT_MIN_LIKES,
        "min_likes should default to DEFAULT_MIN_LIKES in v3"
    );
    assert_eq!(user_dials.updated_at_secs, 1_750_000_000);

    let _ = fs::remove_file(&snapshot_path);
}

#[test]
fn test_m3_cascading_tier_fallback_under_engagement_floor() {
    let interner = Arc::new(StringInterner::new());
    let graph = Arc::new(GraphStore::new());
    let now = current_time_secs();

    let viewer = interner.intern("did:plc:cascade_viewer");
    let co_user1 = interner.intern("did:plc:co_user1");
    let co_user2 = interner.intern("did:plc:co_user2");
    let followed = interner.intern("did:plc:followed_user");
    let author = interner.intern("did:plc:author");

    // Viewer follows `followed`
    graph.record_follow(viewer, followed);

    // Setup 10 shared likes between viewer and co_user1 & co_user2 for Tier 1 qualification
    for i in 1..=10 {
        let pad_post = interner.intern(&format!("at://did:plc:author/app.bsky.feed.post/pad_{i}"));
        graph.record_post_meta(pad_post, author, None, None, now - 10_000);
        graph.record_interaction(viewer, pad_post, SignalType::Like, now - 5_000);
        graph.record_interaction(co_user1, pad_post, SignalType::Like, now - 4_000);
        graph.record_interaction(co_user2, pad_post, SignalType::Like, now - 4_000);
    }

    // Tier 1 candidate post: only 2 likes total (co_user1 and co_user2)
    let p_t1 = interner.intern("at://did:plc:author/app.bsky.feed.post/t1_post_2likes");
    graph.record_post_meta(p_t1, author, None, None, now - 2_000);
    graph.record_interaction(co_user1, p_t1, SignalType::Like, now - 1_500);
    graph.record_interaction(co_user2, p_t1, SignalType::Like, now - 1_400);

    // Tier 2 candidate post: 5 likes total (followed by viewer, interacted by followed + 4 others)
    let p_t2 = interner.intern("at://did:plc:author/app.bsky.feed.post/t2_post_5likes");
    graph.record_post_meta(p_t2, author, None, None, now - 2_000);
    graph.record_interaction(followed, p_t2, SignalType::Like, now - 1_000);
    for u in 100..104 {
        let other = interner.intern(&format!("did:plc:other_{u}"));
        graph.record_interaction(other, p_t2, SignalType::Like, now - 1_000);
    }

    // Tier 3 candidate post: 15 likes total (in velocity pool)
    let p_t3 = interner.intern("at://did:plc:author/app.bsky.feed.post/t3_post_15likes");
    graph.record_post_meta(p_t3, author, None, None, now - 1_000);
    for u in 200..215 {
        let vel_user = interner.intern(&format!("did:plc:vel_{u}"));
        graph.record_interaction(vel_user, p_t3, SignalType::Like, now - 500);
    }

    let rec = Recommender::new(interner, graph);

    // Case 1: min_likes = 1 (Emerging preset) -> Tier 1 candidate meets floor
    let dials_1 = RecommendationDials {
        min_likes: 1,
        ..Default::default()
    };
    let res_1 = rec
        .recommend(Some("did:plc:cascade_viewer"), &dials_1, now)
        .unwrap();
    assert_eq!(
        res_1.posts[0].source,
        RecommendationSource::Tier1InteractionWalk
    );
    assert_eq!(res_1.posts[0].post_id, p_t1);

    // Case 2: min_likes = 4 (Balanced/custom) -> Tier 1 candidate (2 likes) fails floor; cascades to Tier 2 (5 likes)
    let dials_4 = RecommendationDials {
        min_likes: 4,
        ..Default::default()
    };
    let res_4 = rec
        .recommend(Some("did:plc:cascade_viewer"), &dials_4, now)
        .unwrap();
    assert_eq!(res_4.posts[0].source, RecommendationSource::Tier2FollowWalk);
    assert_eq!(res_4.posts[0].post_id, p_t2);

    // Case 3: min_likes = 10 (Curated preset) -> Tier 1 (2 likes) & Tier 2 (5 likes) fail floor; cascades to Tier 3 (15 likes)
    let dials_10 = RecommendationDials {
        min_likes: 10,
        ..Default::default()
    };
    let res_10 = rec
        .recommend(Some("did:plc:cascade_viewer"), &dials_10, now)
        .unwrap();
    assert_eq!(
        res_10.posts[0].source,
        RecommendationSource::Tier3VelocityPool
    );
    assert_eq!(res_10.posts[0].post_id, p_t3);

    // Case 4: Preview endpoint reflects cascading fallback
    let prev_4 = rec
        .recommend_preview_at(Some("did:plc:cascade_viewer"), &dials_4, now)
        .unwrap();
    assert_eq!(prev_4.items.len(), 1);
    assert_eq!(
        prev_4.items[0].uri,
        "at://did:plc:author/app.bsky.feed.post/t2_post_5likes"
    );
    assert_eq!(prev_4.items[0].tier, "Tier 2: 2-Step Follow Walk");

    let prev_10 = rec
        .recommend_preview_at(Some("did:plc:cascade_viewer"), &dials_10, now)
        .unwrap();
    assert_eq!(prev_10.items.len(), 1);
    assert_eq!(
        prev_10.items[0].uri,
        "at://did:plc:author/app.bsky.feed.post/t3_post_15likes"
    );
    assert_eq!(prev_10.items[0].tier, "Tier 3: Topic Velocity Pool");
}

// ===========================================================================
// 4. XRPC 3-Tier Precedence Hierarchy Tests
// ===========================================================================

#[tokio::test]
async fn test_m3_xrpc_3_tier_precedence_hierarchy() {
    let interner = Arc::new(StringInterner::new());
    let graph = Arc::new(GraphStore::new());
    let prefs = Arc::new(UserPreferencesStore::new());
    let now = current_time_secs();

    let viewer_did = "did:plc:xrpc_hier_viewer";
    let viewer_id = interner.intern(viewer_did);
    let author_1 = interner.intern("did:plc:author_1");
    let author_2 = interner.intern("did:plc:author_2");
    let author_3 = interner.intern("did:plc:author_3");

    // Create 3 posts:
    // p1 has 1 like
    // p2 has 3 likes
    // p3 has 10 likes
    let p1 = interner.intern("at://did:plc:author_1/app.bsky.feed.post/post_1");
    let p2 = interner.intern("at://did:plc:author_2/app.bsky.feed.post/post_2");
    let p3 = interner.intern("at://did:plc:author_3/app.bsky.feed.post/post_3");

    graph.record_post_meta(p1, author_1, None, None, now - 100);
    graph.record_post_meta(p2, author_2, None, None, now - 100);
    graph.record_post_meta(p3, author_3, None, None, now - 100);

    // p1: 1 like
    graph.record_interaction(
        interner.intern("did:plc:fan_1"),
        p1,
        SignalType::Like,
        now - 50,
    );

    // p2: 3 likes
    for i in 1..=3 {
        graph.record_interaction(
            interner.intern(&format!("did:plc:fan_2_{i}")),
            p2,
            SignalType::Like,
            now - 50,
        );
    }

    // p3: 10 likes
    for i in 1..=10 {
        graph.record_interaction(
            interner.intern(&format!("did:plc:fan_3_{i}")),
            p3,
            SignalType::Like,
            now - 50,
        );
    }

    let recommender = Arc::new(Recommender::new(Arc::clone(&interner), Arc::clone(&graph)));
    let state = AppState::new(
        Arc::clone(&recommender),
        "did:web:feed.example.com",
        "feed.example.com",
    )
    .with_preferences_store(Arc::clone(&prefs));

    let router = create_xrpc_router(state);
    let feed_uri = "at://did:plc:feed/app.bsky.feed.generator/foryou";
    let jwt = generate_mock_jwt(viewer_did, "did:web:feed.example.com", true);

    // Tier 3: Default when no preferences and no query param -> min_likes = 3 (returns p2 and p3)
    let req_def = Request::builder()
        .uri(format!(
            "/xrpc/app.bsky.feed.getFeedSkeleton?feed={feed_uri}"
        ))
        .header("Authorization", format!("Bearer {jwt}"))
        .body(Body::empty())
        .unwrap();
    let resp_def = router.clone().oneshot(req_def).await.unwrap();
    let body_def = resp_def.into_body().collect().await.unwrap().to_bytes();
    let skel_def: FeedSkeletonResponse = serde_json::from_slice(&body_def).unwrap();
    assert_eq!(skel_def.feed.len(), 2);

    // Tier 2: Persisted user preferences min_likes = 10 (Curated) -> returns only p3
    prefs.set(viewer_id, UserDials::default().with_min_likes(10));
    let req_persisted = Request::builder()
        .uri(format!(
            "/xrpc/app.bsky.feed.getFeedSkeleton?feed={feed_uri}"
        ))
        .header("Authorization", format!("Bearer {jwt}"))
        .body(Body::empty())
        .unwrap();
    let resp_persisted = router.clone().oneshot(req_persisted).await.unwrap();
    let body_persisted = resp_persisted
        .into_body()
        .collect()
        .await
        .unwrap()
        .to_bytes();
    let skel_persisted: FeedSkeletonResponse = serde_json::from_slice(&body_persisted).unwrap();
    assert_eq!(skel_persisted.feed.len(), 1);
    assert_eq!(
        skel_persisted.feed[0].post,
        "at://did:plc:author_3/app.bsky.feed.post/post_3"
    );

    // Tier 1: Query param engagement_floor=emerging (min_likes = 1) overrides persisted dials -> returns all 3
    let req_override = Request::builder()
        .uri(format!(
            "/xrpc/app.bsky.feed.getFeedSkeleton?feed={feed_uri}&engagement_floor=emerging"
        ))
        .header("Authorization", format!("Bearer {jwt}"))
        .body(Body::empty())
        .unwrap();
    let resp_override = router.clone().oneshot(req_override).await.unwrap();
    let body_override = resp_override
        .into_body()
        .collect()
        .await
        .unwrap()
        .to_bytes();
    let skel_override: FeedSkeletonResponse = serde_json::from_slice(&body_override).unwrap();
    assert_eq!(skel_override.feed.len(), 3);
}

// ===========================================================================
// 5. REST /api/preferences Lifecycle Tests
// ===========================================================================

#[tokio::test]
async fn test_m3_rest_preferences_lifecycle() {
    let interner = Arc::new(StringInterner::new());
    let graph = Arc::new(GraphStore::new());
    let prefs = Arc::new(UserPreferencesStore::new());
    let recommender = Arc::new(Recommender::new(Arc::clone(&interner), Arc::clone(&graph)));

    let state = AppState::new(
        Arc::clone(&recommender),
        "did:web:feed.example.com",
        "feed.example.com",
    )
    .with_preferences_store(Arc::clone(&prefs));

    let router = create_xrpc_router(state);
    let viewer_did = "did:plc:rest_pref_user";
    let token = generate_session_token(viewer_did, 3600);

    // 1. Initial GET returns defaults (min_likes: 3)
    let get_req1 = Request::builder()
        .uri("/api/preferences")
        .header("Authorization", format!("Bearer {token}"))
        .body(Body::empty())
        .unwrap();
    let get_resp1 = router.clone().oneshot(get_req1).await.unwrap();
    assert_eq!(get_resp1.status(), StatusCode::OK);
    let body1 = get_resp1.into_body().collect().await.unwrap().to_bytes();
    let dto1: PreferencesResponseDto = serde_json::from_slice(&body1).unwrap();
    assert_eq!(dto1.preferences.min_likes, 3);
    assert!(!dto1.is_custom);

    // 2. POST custom min_likes: 8
    let save_body = SavePreferencesRequestBody {
        freshness_hours: 48.0,
        discovery_ratio: 0.20,
        topic_weights: None,
        include_replies: Some(false),
        min_likes: Some(8),
    };
    let post_req = Request::builder()
        .method("POST")
        .uri("/api/preferences")
        .header("Authorization", format!("Bearer {token}"))
        .header("Content-Type", "application/json")
        .body(Body::from(serde_json::to_vec(&save_body).unwrap()))
        .unwrap();
    let post_resp = router.clone().oneshot(post_req).await.unwrap();
    assert_eq!(post_resp.status(), StatusCode::OK);
    let post_body = post_resp.into_body().collect().await.unwrap().to_bytes();
    let post_dto: GenericStatusResponse = serde_json::from_slice(&post_body).unwrap();
    assert_eq!(post_dto.preferences.as_ref().unwrap().min_likes, 8);
    assert_eq!(post_dto.dials.as_ref().unwrap().min_likes, 8);

    // 3. Subsequent GET returns persisted min_likes: 8
    let get_req2 = Request::builder()
        .uri("/api/preferences")
        .header("Authorization", format!("Bearer {token}"))
        .body(Body::empty())
        .unwrap();
    let get_resp2 = router.clone().oneshot(get_req2).await.unwrap();
    assert_eq!(get_resp2.status(), StatusCode::OK);
    let body2 = get_resp2.into_body().collect().await.unwrap().to_bytes();
    let dto2: PreferencesResponseDto = serde_json::from_slice(&body2).unwrap();
    assert_eq!(dto2.preferences.min_likes, 8);
    assert!(dto2.is_custom);

    // 4. DELETE resets preferences back to default
    let del_req = Request::builder()
        .method("DELETE")
        .uri("/api/preferences")
        .header("Authorization", format!("Bearer {token}"))
        .body(Body::empty())
        .unwrap();
    let del_resp = router.clone().oneshot(del_req).await.unwrap();
    assert_eq!(del_resp.status(), StatusCode::OK);

    // 5. GET after DELETE returns default min_likes: 3
    let get_req3 = Request::builder()
        .uri("/api/preferences")
        .header("Authorization", format!("Bearer {token}"))
        .body(Body::empty())
        .unwrap();
    let get_resp3 = router.clone().oneshot(get_req3).await.unwrap();
    assert_eq!(get_resp3.status(), StatusCode::OK);
    let body3 = get_resp3.into_body().collect().await.unwrap().to_bytes();
    let dto3: PreferencesResponseDto = serde_json::from_slice(&body3).unwrap();
    assert_eq!(dto3.preferences.min_likes, 3);
    assert!(!dto3.is_custom);
}
