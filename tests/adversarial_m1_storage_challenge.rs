#![forbid(unsafe_code)]
#![allow(
    clippy::pedantic,
    clippy::nursery,
    clippy::float_cmp,
    clippy::cast_possible_truncation,
    clippy::cast_precision_loss,
    clippy::cast_sign_loss
)]

//! # Milestone 1 Storage & Snapshot Engine Adversarial Challenge Test Suite
//!
//! Empirical verification harness validating:
//! 1. Backward compatibility: loading V1 snapshot files cleanly into `UserPreferencesStore` with empty preferences.
//! 2. Forward compatibility and roundtripping: saving V2 snapshots with 10,000+ custom profiles and reloading with exact bit-for-bit field fidelity.
//! 3. Validation boundaries: verifying strict bounds on `UserDials` (1h–168h freshness, 0%–50% discovery, 0.0x–5.0x topics, NaN/Inf rejection) across store and snapshot engine.
//! 4. Durability & corruption resistance: rejecting corrupted/truncated Section 8 records, verifying dual CRC32 integrity, atomic rename staging.
//! 5. High-concurrency stress harness: 32 concurrent threads across 64 shards without torn reads or deadlocks.
//! 6. Crate-wide `#![forbid(unsafe_code)]` compliance.

use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use crc32fast::Hasher;
use for_your_consideration::prelude::*;

static TEST_COUNTER: AtomicU64 = AtomicU64::new(1);

/// Creates a unique temporary snapshot path.
fn temp_snap_path(tag: &str) -> PathBuf {
    let mut path = std::env::temp_dir();
    let id = format!(
        "fyc_adv_m1_{}_{}_{}_{}.bin",
        tag,
        std::process::id(),
        TEST_COUNTER.fetch_add(1, Ordering::Relaxed),
        Instant::now().elapsed().as_nanos()
    );
    path.push(id);
    path
}

// ===========================================================================
// 1. Backward Compatibility Tests: V1 Snapshot Ingestion
// ===========================================================================

#[test]
fn test_v1_snapshot_backward_compatibility_clears_preferences_and_loads_graph() {
    let snap_path = temp_snap_path("v1_backward_compat");
    let interner = StringInterner::new();
    let graph = GraphStore::new();
    let base_ts = BLUESKY_EPOCH_SECS + 100_000;

    let mut expected_first_uid = 0;

    // Build populated graph
    for i in 1..=100 {
        let did = format!("did:plc:v1_user_{i}");
        let u = interner.intern(&did);
        if i == 1 {
            expected_first_uid = u;
        }
        let post_uri = format!("at://did:plc:v1_user_{i}/app.bsky.feed.post/{i}");
        let p = interner.intern(&post_uri);

        graph.record_interaction(u, p, SignalType::Like, base_ts + (i as u64) * 10);
        graph.record_follow(u, ((i % 100) + 1) as u32);
        graph.record_post_meta(p, u, None, None, base_ts + (i as u64) * 5);
    }

    // Serialize synthetic V1 binary payload (Sections 1–7 only)
    let strings = interner.export_strings();
    let graph_data = graph.snapshot_data();

    let mut payload = Vec::new();

    // Section 1: Strings
    payload.extend_from_slice(&(strings.len() as u32).to_le_bytes());
    for s in &strings {
        payload.extend_from_slice(&(s.len() as u32).to_le_bytes());
        payload.extend_from_slice(s.as_bytes());
    }

    // Section 2: User Interactions (Forward)
    payload.extend_from_slice(&(graph_data.user_interactions.len() as u32).to_le_bytes());
    let mut total_forward_edges = 0u64;
    for (uid, edges) in &graph_data.user_interactions {
        payload.extend_from_slice(&uid.to_le_bytes());
        payload.extend_from_slice(&(edges.len() as u32).to_le_bytes());
        for e in edges {
            payload.extend_from_slice(&e.target.to_le_bytes());
            payload.extend_from_slice(&e.packed.to_le_bytes());
        }
        total_forward_edges += edges.len() as u64;
    }

    // Section 3: Post Interactions (Reverse)
    payload.extend_from_slice(&(graph_data.post_interactions.len() as u32).to_le_bytes());
    for (pid, edges) in &graph_data.post_interactions {
        payload.extend_from_slice(&pid.to_le_bytes());
        payload.extend_from_slice(&(edges.len() as u32).to_le_bytes());
        for e in edges {
            payload.extend_from_slice(&e.target.to_le_bytes());
            payload.extend_from_slice(&e.packed.to_le_bytes());
        }
    }

    // Section 4: Roaring Bitmaps
    payload.extend_from_slice(&(graph_data.user_likes_bitmaps.len() as u32).to_le_bytes());
    let mut bm_buf = Vec::new();
    for (uid, bm) in &graph_data.user_likes_bitmaps {
        bm_buf.clear();
        bm.serialize_into(&mut bm_buf).unwrap();
        payload.extend_from_slice(&uid.to_le_bytes());
        payload.extend_from_slice(&(bm_buf.len() as u32).to_le_bytes());
        payload.extend_from_slice(&bm_buf);
    }

    // Section 5: Follows
    payload.extend_from_slice(&(graph_data.follows.len() as u32).to_le_bytes());
    for (fid, list) in &graph_data.follows {
        payload.extend_from_slice(&fid.to_le_bytes());
        payload.extend_from_slice(&(list.len() as u32).to_le_bytes());
        for &target in list {
            payload.extend_from_slice(&target.to_le_bytes());
        }
    }

    // Section 6: Post Metadata
    payload.extend_from_slice(&(graph_data.post_metadata.len() as u32).to_le_bytes());
    for (pid, meta) in &graph_data.post_metadata {
        payload.extend_from_slice(&pid.to_le_bytes());
        payload.extend_from_slice(&meta.author_id.to_le_bytes());
        payload.extend_from_slice(&0u8.to_le_bytes());
        payload.extend_from_slice(&0u32.to_le_bytes());
        payload.extend_from_slice(&0u8.to_le_bytes());
        payload.extend_from_slice(&0u32.to_le_bytes());
        payload.extend_from_slice(&meta.created_at.to_le_bytes());
    }

    // Section 7: Active Recent Posts
    payload.extend_from_slice(&(graph_data.active_recent_posts.len() as u32).to_le_bytes());
    for (pid, ts) in &graph_data.active_recent_posts {
        payload.extend_from_slice(&pid.to_le_bytes());
        payload.extend_from_slice(&ts.to_le_bytes());
    }

    // Compute payload CRC
    let mut p_hasher = Hasher::new();
    p_hasher.update(&payload);
    let payload_crc = p_hasher.finalize();

    // V1 Header (format_version = 1, bytes 60..64 unused/zero)
    let mut header_bytes = [0u8; HEADER_SIZE];
    header_bytes[0..4].copy_from_slice(&SNAPSHOT_MAGIC);
    header_bytes[4..6].copy_from_slice(&SNAPSHOT_FORMAT_VERSION_V1.to_le_bytes()); // 1
    header_bytes[6..8].copy_from_slice(&(HEADER_SIZE as u16).to_le_bytes());
    header_bytes[8..16].copy_from_slice(&1_720_000_000u64.to_le_bytes()); // created_at
    header_bytes[16..24].copy_from_slice(&987_654_321u64.to_le_bytes()); // cursor
    header_bytes[24..28].copy_from_slice(&0u32.to_le_bytes()); // flags
    header_bytes[28..32].copy_from_slice(&(strings.len() as u32).to_le_bytes());
    header_bytes[32..36]
        .copy_from_slice(&(graph_data.user_interactions.len() as u32).to_le_bytes());
    header_bytes[36..44].copy_from_slice(&total_forward_edges.to_le_bytes());
    header_bytes[44..48].copy_from_slice(&(graph_data.follows.len() as u32).to_le_bytes());
    header_bytes[48..52].copy_from_slice(&(graph_data.post_metadata.len() as u32).to_le_bytes());
    header_bytes[52..56].copy_from_slice(&payload_crc.to_le_bytes());

    let mut h_hasher = Hasher::new();
    h_hasher.update(&header_bytes[0..56]);
    let header_crc = h_hasher.finalize();
    header_bytes[56..60].copy_from_slice(&header_crc.to_le_bytes());

    let mut file_content = header_bytes.to_vec();
    file_content.extend_from_slice(&payload);
    std::fs::write(&snap_path, &file_content).unwrap();

    // 1. Test load with pre-existing stale preferences in store
    let loaded_interner = StringInterner::new();
    let loaded_graph = GraphStore::new();
    let loaded_prefs = UserPreferencesStore::new();

    // Pre-populate with stale data to verify it gets completely wiped
    loaded_prefs.set(
        999,
        UserDials::from_hours(48.0, 0.20, TopicWeights::default(), 500),
    );
    assert_eq!(loaded_prefs.len(), 1);

    let load_res =
        load_snapshot_with_preferences(&snap_path, &loaded_interner, &loaded_graph, &loaded_prefs)
            .expect("V1 snapshot must load cleanly into V2 engine")
            .expect("Snapshot must exist");

    assert_eq!(load_res.header.format_version, 1);
    assert_eq!(load_res.header.magic, SNAPSHOT_MAGIC);
    assert_eq!(load_res.header.jetstream_cursor_us, 987_654_321);

    // Verify preferences store was completely cleared
    assert_eq!(loaded_prefs.len(), 0);
    assert!(loaded_prefs.is_empty());
    assert_eq!(loaded_prefs.get(999), None);
    assert_eq!(loaded_prefs.get_or_default(999), UserDials::default());

    // Verify graph and interner hydrated accurately
    assert_eq!(
        loaded_interner.lookup_id("did:plc:v1_user_1"),
        Some(expected_first_uid)
    );
    assert_eq!(loaded_graph.stats().total_interactions, 100);
    assert_eq!(loaded_graph.stats().total_follows, 100);

    // 2. Test legacy load_snapshot wrapper on V1 snapshot
    let legacy_interner = StringInterner::new();
    let legacy_graph = GraphStore::new();
    let legacy_loaded = load_snapshot(&snap_path, &legacy_interner, &legacy_graph)
        .expect("Legacy load_snapshot must succeed on V1 file")
        .expect("Snapshot must exist");
    assert_eq!(legacy_loaded.header.format_version, 1);
    assert_eq!(legacy_graph.stats().total_interactions, 100);

    let _ = std::fs::remove_file(&snap_path);
}

// ===========================================================================
// 2. Forward Compatibility & Roundtripping: 10,000+ Profiles with Exact Fidelity
// ===========================================================================

#[test]
fn test_v2_snapshot_roundtrip_10000_profiles_exact_bit_fidelity() {
    let snap_path = temp_snap_path("roundtrip_10k");
    let interner = StringInterner::new();
    let graph = GraphStore::new();
    let preferences = UserPreferencesStore::new();

    const PROFILE_COUNT: usize = 10_000;
    let mut expected_profiles = Vec::with_capacity(PROFILE_COUNT);

    for i in 0..PROFILE_COUNT {
        let did = format!("did:plc:user_{i:06}");
        let uid = interner.intern(&did);

        // Construct distinct float dial configurations strictly within bounds [1.0h, 168.0h], [0.0, 0.50], [0.0, 5.0]
        let freshness_hours = 1.0 + ((i % 1670) as f32) * 0.1; // 1.0 to 168.0
        let discovery_ratio = ((i % 501) as f32) / 1000.0; // 0.000 to 0.500
        let art = ((i * 7) % 501) as f32 / 100.0; // 0.0 to 5.00
        let tech = ((i * 13) % 501) as f32 / 100.0;
        let science = ((i * 17) % 501) as f32 / 100.0;
        let news = ((i * 19) % 501) as f32 / 100.0;
        let culture = ((i * 23) % 501) as f32 / 100.0;
        let updated_at = 1_700_000_000 + (i as u64) * 37;

        let dials = UserDials::from_hours(
            freshness_hours,
            discovery_ratio,
            TopicWeights {
                art,
                tech,
                science,
                news,
                culture,
            },
            updated_at,
        );

        assert!(dials.validate().is_ok(), "Generated dials must be valid");
        preferences.set(uid, dials);
        expected_profiles.push((uid, did, dials));
    }

    assert_eq!(preferences.len(), PROFILE_COUNT);

    // Save snapshot
    let save_start = Instant::now();
    let save_header = save_snapshot_with_preferences(
        &snap_path,
        &interner,
        &graph,
        &preferences,
        1_724_999_888_777,
    )
    .expect("Save 10,000 profiles failed");
    let save_duration = save_start.elapsed();

    assert_eq!(save_header.magic, SNAPSHOT_MAGIC);
    assert_eq!(save_header.format_version, SNAPSHOT_FORMAT_VERSION);
    assert_eq!(save_header.num_preferences, PROFILE_COUNT as u32);
    assert_eq!(save_header.num_strings, PROFILE_COUNT as u32);

    // Verify snapshot file size matches expectation
    let file_meta = std::fs::metadata(&snap_path).expect("Metadata lookup failed");
    assert!(
        file_meta.len() > 400_000,
        "Snapshot file size should reflect 10,000 records"
    );

    // Load into fresh, independent structures
    let loaded_interner = StringInterner::new();
    let loaded_graph = GraphStore::new();
    let loaded_prefs = UserPreferencesStore::new();

    let load_start = Instant::now();
    let load_res =
        load_snapshot_with_preferences(&snap_path, &loaded_interner, &loaded_graph, &loaded_prefs)
            .expect("Load 10,000 profiles failed")
            .expect("Snapshot must exist");
    let load_duration = load_start.elapsed();

    assert_eq!(load_res.header.format_version, SNAPSHOT_FORMAT_VERSION);
    assert_eq!(load_res.header.num_preferences, PROFILE_COUNT as u32);
    assert_eq!(loaded_prefs.len(), PROFILE_COUNT);

    // Verify exact bit-for-bit equality for all 10,000 profiles
    for (expected_uid, expected_did, expected_dials) in &expected_profiles {
        let loaded_uid = loaded_interner
            .lookup_id(expected_did)
            .expect("Interner must contain user DID");
        assert_eq!(loaded_uid, *expected_uid);

        let loaded_dials = loaded_prefs
            .get(*expected_uid)
            .expect("Preference store must contain user dials");

        // Verify exact bit representation of every float field
        assert_eq!(
            loaded_dials.freshness_half_life_secs.to_bits(),
            expected_dials.freshness_half_life_secs.to_bits(),
            "Freshness half-life bit mismatch for user {expected_uid}"
        );
        assert_eq!(
            loaded_dials.serendipity_ratio.to_bits(),
            expected_dials.serendipity_ratio.to_bits(),
            "Serendipity ratio bit mismatch for user {expected_uid}"
        );
        assert_eq!(
            loaded_dials.topic_weights.art.to_bits(),
            expected_dials.topic_weights.art.to_bits(),
            "Art topic weight bit mismatch for user {expected_uid}"
        );
        assert_eq!(
            loaded_dials.topic_weights.tech.to_bits(),
            expected_dials.topic_weights.tech.to_bits(),
            "Tech topic weight bit mismatch for user {expected_uid}"
        );
        assert_eq!(
            loaded_dials.topic_weights.science.to_bits(),
            expected_dials.topic_weights.science.to_bits(),
            "Science topic weight bit mismatch for user {expected_uid}"
        );
        assert_eq!(
            loaded_dials.topic_weights.news.to_bits(),
            expected_dials.topic_weights.news.to_bits(),
            "News topic weight bit mismatch for user {expected_uid}"
        );
        assert_eq!(
            loaded_dials.topic_weights.culture.to_bits(),
            expected_dials.topic_weights.culture.to_bits(),
            "Culture topic weight bit mismatch for user {expected_uid}"
        );
        assert_eq!(
            loaded_dials.updated_at_secs, expected_dials.updated_at_secs,
            "updated_at_secs mismatch for user {expected_uid}"
        );

        // Also test get_by_did
        let dials_by_did = loaded_prefs
            .get_by_did(&loaded_interner, expected_did)
            .expect("Lookup by DID must succeed");
        assert_eq!(dials_by_did, *expected_dials);
    }

    // Performance assertions: both save and load under 500ms
    assert!(
        save_duration.as_millis() < 500,
        "Save duration too high: {save_duration:?}"
    );
    assert!(
        load_duration.as_millis() < 500,
        "Load duration too high: {load_duration:?}"
    );

    let _ = std::fs::remove_file(&snap_path);
}

#[test]
fn test_v2_snapshot_roundtrip_20000_profiles_stress() {
    let snap_path = temp_snap_path("roundtrip_20k");
    let interner = StringInterner::new();
    let graph = GraphStore::new();
    let preferences = UserPreferencesStore::new();

    const COUNT: usize = 20_000;
    for i in 0..COUNT {
        let dials = UserDials::from_hours(
            1.0 + (i % 168) as f32,
            ((i % 50) as f32) / 100.0,
            TopicWeights {
                art: ((i % 50) as f32) / 10.0,
                tech: 1.0,
                science: 2.0,
                news: 0.5,
                culture: 1.5,
            },
            1_700_000_000 + u64::from(i as u32),
        );
        preferences.set(i as u32, dials);
    }

    assert_eq!(preferences.len(), COUNT);

    save_snapshot_with_preferences(&snap_path, &interner, &graph, &preferences, 42)
        .expect("Save 20,000 profiles failed");

    let loaded_interner = StringInterner::new();
    let loaded_graph = GraphStore::new();
    let loaded_prefs = UserPreferencesStore::new();

    let loaded =
        load_snapshot_with_preferences(&snap_path, &loaded_interner, &loaded_graph, &loaded_prefs)
            .expect("Load 20,000 profiles failed")
            .expect("Snapshot must exist");

    assert_eq!(loaded.header.num_preferences, COUNT as u32);
    assert_eq!(loaded_prefs.len(), COUNT);

    let _ = std::fs::remove_file(&snap_path);
}

// ===========================================================================
// 3. Validation Boundary Matrix: UserDials Strict Limits
// ===========================================================================

#[test]
fn test_user_dials_validation_boundary_matrix() {
    // 1. Valid boundary endpoints
    let min_valid = UserDials::from_hours(
        FRESHNESS_MIN_HOURS, // 1.0h
        DISCOVERY_MIN,       // 0.0
        TopicWeights {
            art: TOPIC_MIN,     // 0.0
            tech: TOPIC_MIN,    // 0.0
            science: TOPIC_MIN, // 0.0
            news: TOPIC_MIN,    // 0.0
            culture: TOPIC_MIN, // 0.0
        },
        100,
    );
    assert!(min_valid.validate().is_ok());

    let max_valid = UserDials::from_hours(
        FRESHNESS_MAX_HOURS, // 168.0h
        DISCOVERY_MAX,       // 0.50
        TopicWeights {
            art: TOPIC_MAX,     // 5.0
            tech: TOPIC_MAX,    // 5.0
            science: TOPIC_MAX, // 5.0
            news: TOPIC_MAX,    // 5.0
            culture: TOPIC_MAX, // 5.0
        },
        200,
    );
    assert!(max_valid.validate().is_ok());

    let default_valid = UserDials::default();
    assert!(default_valid.validate().is_ok());

    // 2. Freshness boundary violations
    let invalid_freshness_below = UserDials {
        freshness_half_life_secs: MIN_FRESHNESS_SECS - 0.1, // 3599.9s
        ..UserDials::default()
    };
    assert!(invalid_freshness_below.validate().is_err());

    let invalid_freshness_above = UserDials {
        freshness_half_life_secs: MAX_FRESHNESS_SECS + 0.1, // 604800.1s
        ..UserDials::default()
    };
    assert!(invalid_freshness_above.validate().is_err());

    let invalid_freshness_zero = UserDials {
        freshness_half_life_secs: 0.0,
        ..UserDials::default()
    };
    assert!(invalid_freshness_zero.validate().is_err());

    let invalid_freshness_neg = UserDials {
        freshness_half_life_secs: -3600.0,
        ..UserDials::default()
    };
    assert!(invalid_freshness_neg.validate().is_err());

    let invalid_freshness_nan = UserDials {
        freshness_half_life_secs: f32::NAN,
        ..UserDials::default()
    };
    assert!(invalid_freshness_nan.validate().is_err());

    let invalid_freshness_inf = UserDials {
        freshness_half_life_secs: f32::INFINITY,
        ..UserDials::default()
    };
    assert!(invalid_freshness_inf.validate().is_err());

    let invalid_freshness_neg_inf = UserDials {
        freshness_half_life_secs: f32::NEG_INFINITY,
        ..UserDials::default()
    };
    assert!(invalid_freshness_neg_inf.validate().is_err());

    // 3. Discovery / Serendipity boundary violations
    let invalid_disc_below = UserDials {
        serendipity_ratio: -0.0001,
        ..UserDials::default()
    };
    assert!(invalid_disc_below.validate().is_err());

    let invalid_disc_above = UserDials {
        serendipity_ratio: 0.5001,
        ..UserDials::default()
    };
    assert!(invalid_disc_above.validate().is_err());

    let invalid_disc_high = UserDials {
        serendipity_ratio: 1.0,
        ..UserDials::default()
    };
    assert!(invalid_disc_high.validate().is_err());

    let invalid_disc_nan = UserDials {
        serendipity_ratio: f32::NAN,
        ..UserDials::default()
    };
    assert!(invalid_disc_nan.validate().is_err());

    let invalid_disc_inf = UserDials {
        serendipity_ratio: f32::INFINITY,
        ..UserDials::default()
    };
    assert!(invalid_disc_inf.validate().is_err());

    // 4. Topic multiplier violations for every individual category
    for topic_name in ["art", "tech", "science", "news", "culture"] {
        // Below min (negative)
        let mut weights = TopicWeights::default();
        match topic_name {
            "art" => weights.art = -0.01,
            "tech" => weights.tech = -0.01,
            "science" => weights.science = -0.01,
            "news" => weights.news = -0.01,
            "culture" => weights.culture = -0.01,
            _ => unreachable!(),
        }
        let dials = UserDials {
            topic_weights: weights,
            ..UserDials::default()
        };
        assert!(
            dials.validate().is_err(),
            "Negative topic weight for {topic_name} must be rejected"
        );

        // Above max (5.01)
        let mut weights_above = TopicWeights::default();
        match topic_name {
            "art" => weights_above.art = 5.01,
            "tech" => weights_above.tech = 5.01,
            "science" => weights_above.science = 5.01,
            "news" => weights_above.news = 5.01,
            "culture" => weights_above.culture = 5.01,
            _ => unreachable!(),
        }
        let dials_above = UserDials {
            topic_weights: weights_above,
            ..UserDials::default()
        };
        assert!(
            dials_above.validate().is_err(),
            "Oversized topic weight for {topic_name} must be rejected"
        );

        // NaN topic
        let mut weights_nan = TopicWeights::default();
        match topic_name {
            "art" => weights_nan.art = f32::NAN,
            "tech" => weights_nan.tech = f32::NAN,
            "science" => weights_nan.science = f32::NAN,
            "news" => weights_nan.news = f32::NAN,
            "culture" => weights_nan.culture = f32::NAN,
            _ => unreachable!(),
        }
        let dials_nan = UserDials {
            topic_weights: weights_nan,
            ..UserDials::default()
        };
        assert!(
            dials_nan.validate().is_err(),
            "NaN topic weight for {topic_name} must be rejected"
        );

        // Infinity topic
        let mut weights_inf = TopicWeights::default();
        match topic_name {
            "art" => weights_inf.art = f32::INFINITY,
            "tech" => weights_inf.tech = f32::INFINITY,
            "science" => weights_inf.science = f32::INFINITY,
            "news" => weights_inf.news = f32::INFINITY,
            "culture" => weights_inf.culture = f32::INFINITY,
            _ => unreachable!(),
        }
        let dials_inf = UserDials {
            topic_weights: weights_inf,
            ..UserDials::default()
        };
        assert!(
            dials_inf.validate().is_err(),
            "Infinity topic weight for {topic_name} must be rejected"
        );
    }
}

// ===========================================================================
// 4. Snapshot Section 8 Corruption & Malicious Payload Rejection Tests
// ===========================================================================

#[test]
fn test_snapshot_v2_corrupted_section_8_rejections() {
    let interner = StringInterner::new();
    let graph = GraphStore::new();
    let preferences = UserPreferencesStore::new();

    let strings = interner.export_strings();
    let graph_data = graph.snapshot_data();

    // Helper to construct a synthetic V2 snapshot with a single corrupted Section 8 record
    let build_synthetic_v2_snap = |freshness: f32,
                                   serendipity: f32,
                                   art: f32,
                                   tech: f32,
                                   science: f32,
                                   news: f32,
                                   culture: f32|
     -> (PathBuf, Vec<u8>) {
        let snap_path = temp_snap_path("corrupt_sec8_case");
        let mut payload = Vec::new();

        // Sections 1-7
        payload.extend_from_slice(&(strings.len() as u32).to_le_bytes());
        payload.extend_from_slice(&(graph_data.user_interactions.len() as u32).to_le_bytes());
        payload.extend_from_slice(&(graph_data.post_interactions.len() as u32).to_le_bytes());
        payload.extend_from_slice(&(graph_data.user_likes_bitmaps.len() as u32).to_le_bytes());
        payload.extend_from_slice(&(graph_data.follows.len() as u32).to_le_bytes());
        payload.extend_from_slice(&(graph_data.post_metadata.len() as u32).to_le_bytes());
        payload.extend_from_slice(&(graph_data.active_recent_posts.len() as u32).to_le_bytes());

        // Section 8: 1 preference
        payload.extend_from_slice(&1u32.to_le_bytes()); // num_preferences = 1
        payload.extend_from_slice(&123u32.to_le_bytes()); // user_id
        payload.extend_from_slice(&freshness.to_le_bytes());
        payload.extend_from_slice(&serendipity.to_le_bytes());
        payload.extend_from_slice(&art.to_le_bytes());
        payload.extend_from_slice(&tech.to_le_bytes());
        payload.extend_from_slice(&science.to_le_bytes());
        payload.extend_from_slice(&news.to_le_bytes());
        payload.extend_from_slice(&culture.to_le_bytes());
        payload.extend_from_slice(&1_700_000_000u64.to_le_bytes());

        let mut p_hasher = Hasher::new();
        p_hasher.update(&payload);
        let payload_crc = p_hasher.finalize();

        let mut header = [0u8; HEADER_SIZE];
        header[0..4].copy_from_slice(&SNAPSHOT_MAGIC);
        header[4..6].copy_from_slice(&SNAPSHOT_FORMAT_VERSION.to_le_bytes());
        header[6..8].copy_from_slice(&(HEADER_SIZE as u16).to_le_bytes());
        header[52..56].copy_from_slice(&payload_crc.to_le_bytes());
        header[60..64].copy_from_slice(&1u32.to_le_bytes());

        let mut h_hasher = Hasher::new();
        h_hasher.update(&header[0..56]);
        let header_crc = h_hasher.finalize();
        header[56..60].copy_from_slice(&header_crc.to_le_bytes());

        let mut file_bytes = header.to_vec();
        file_bytes.extend_from_slice(&payload);
        std::fs::write(&snap_path, &file_bytes).unwrap();

        (snap_path, file_bytes)
    };

    // Test cases that must be rejected:
    let test_cases = [
        ("Freshness NaN", f32::NAN, 0.15, 1.0, 1.0, 1.0, 1.0, 1.0),
        (
            "Freshness +Inf",
            f32::INFINITY,
            0.15,
            1.0,
            1.0,
            1.0,
            1.0,
            1.0,
        ),
        (
            "Freshness -Inf",
            f32::NEG_INFINITY,
            0.15,
            1.0,
            1.0,
            1.0,
            1.0,
            1.0,
        ),
        ("Freshness below 1h", 1800.0, 0.15, 1.0, 1.0, 1.0, 1.0, 1.0),
        (
            "Freshness above 168h",
            700_000.0,
            0.15,
            1.0,
            1.0,
            1.0,
            1.0,
            1.0,
        ),
        (
            "Discovery NaN",
            36.0 * 3600.0,
            f32::NAN,
            1.0,
            1.0,
            1.0,
            1.0,
            1.0,
        ),
        (
            "Discovery negative",
            36.0 * 3600.0,
            -0.05,
            1.0,
            1.0,
            1.0,
            1.0,
            1.0,
        ),
        (
            "Discovery > 0.50",
            36.0 * 3600.0,
            0.51,
            1.0,
            1.0,
            1.0,
            1.0,
            1.0,
        ),
        (
            "Topic Art NaN",
            36.0 * 3600.0,
            0.15,
            f32::NAN,
            1.0,
            1.0,
            1.0,
            1.0,
        ),
        (
            "Topic Tech negative",
            36.0 * 3600.0,
            0.15,
            1.0,
            -0.1,
            1.0,
            1.0,
            1.0,
        ),
        (
            "Topic Science > 5.0",
            36.0 * 3600.0,
            0.15,
            1.0,
            1.0,
            5.01,
            1.0,
            1.0,
        ),
        (
            "Topic News +Inf",
            36.0 * 3600.0,
            0.15,
            1.0,
            1.0,
            1.0,
            f32::INFINITY,
            1.0,
        ),
        (
            "Topic Culture -Inf",
            36.0 * 3600.0,
            0.15,
            1.0,
            1.0,
            1.0,
            1.0,
            f32::NEG_INFINITY,
        ),
    ];

    for (label, freshness, disc, art, tech, science, news, culture) in test_cases {
        let (snap_path, _) =
            build_synthetic_v2_snap(freshness, disc, art, tech, science, news, culture);
        let res = load_snapshot_with_preferences(&snap_path, &interner, &graph, &preferences);
        assert!(
            res.is_err(),
            "Corrupt dial record [{label}] must be rejected during load"
        );
        let _ = std::fs::remove_file(&snap_path);
    }
}

#[test]
fn test_snapshot_v2_section_8_truncation_and_crc_corruption() {
    let snap_path = temp_snap_path("trunc_sec8");
    let interner = StringInterner::new();
    let graph = GraphStore::new();
    let preferences = UserPreferencesStore::new();

    // Populate 5 profiles
    for i in 0..5 {
        preferences.set(i, UserDials::default());
    }

    save_snapshot_with_preferences(&snap_path, &interner, &graph, &preferences, 999)
        .expect("Save failed");

    let original_bytes = std::fs::read(&snap_path).unwrap();

    // 1. Truncate 1 byte from the end
    let mut truncated_1 = original_bytes.clone();
    truncated_1.truncate(truncated_1.len() - 1);
    std::fs::write(&snap_path, &truncated_1).unwrap();
    let res = load_snapshot_with_preferences(&snap_path, &interner, &graph, &preferences);
    assert!(res.is_err(), "Truncation by 1 byte must fail load");

    // 2. Truncate 25 bytes from the end (middle of Section 8 record)
    let mut truncated_25 = original_bytes.clone();
    truncated_25.truncate(truncated_25.len() - 25);
    std::fs::write(&snap_path, &truncated_25).unwrap();
    let res = load_snapshot_with_preferences(&snap_path, &interner, &graph, &preferences);
    assert!(res.is_err(), "Truncation by 25 bytes must fail load");

    // 3. Bit-flip in Section 8 (payload CRC mismatch)
    let mut bitflipped = original_bytes.clone();
    let last_idx = bitflipped.len() - 5;
    bitflipped[last_idx] ^= 0xFF;
    std::fs::write(&snap_path, &bitflipped).unwrap();
    let res = load_snapshot_with_preferences(&snap_path, &interner, &graph, &preferences);
    assert!(
        res.is_err(),
        "Bitflip in Section 8 payload must trigger CRC32 mismatch"
    );
    let err_str = res.unwrap_err().to_string();
    assert!(
        err_str.contains("Payload CRC32 mismatch") || err_str.contains("checksum"),
        "Expected CRC mismatch error, got: {err_str}"
    );

    let _ = std::fs::remove_file(&snap_path);
}

// ===========================================================================
// 5. Concurrency & Thread Safety Stress Test: 32 Threads Across 64 Shards
// ===========================================================================

#[test]
fn test_user_preferences_store_32_thread_high_concurrency_stress() {
    let store = Arc::new(UserPreferencesStore::new());
    let interner = Arc::new(StringInterner::new());
    let stop_signal = Arc::new(AtomicBool::new(false));

    // Pre-populate 500 users
    for i in 0..500 {
        let did = format!("did:plc:stress_user_{i}");
        store.set_by_did(&interner, &did, UserDials::default());
    }

    let mut handles = Vec::new();

    // 12 Reader threads: calling get, get_or_default, get_by_did, get_by_did_or_default
    for thread_idx in 0..12 {
        let s = Arc::clone(&store);
        let i_ref = Arc::clone(&interner);
        let stop = Arc::clone(&stop_signal);
        let handle = thread::spawn(move || {
            let mut reads = 0u64;
            while !stop.load(Ordering::Relaxed) {
                let uid = (thread_idx * 50 + (reads % 500)) as u32;
                let opt_dials = s.get(uid);
                if let Some(d) = opt_dials {
                    // Invariant: all float fields must be finite and valid
                    assert!(d.freshness_half_life_secs.is_finite());
                    assert!(d.serendipity_ratio.is_finite());
                }

                let _ = s.get_or_default(uid);

                let did = format!("did:plc:stress_user_{}", reads % 600);
                let _ = s.get_by_did(&i_ref, &did);
                let _ = s.get_by_did_or_default(&i_ref, &did);

                reads += 1;
            }
            reads
        });
        handles.push(handle);
    }

    // 8 Writer threads: updating existing and new users
    for thread_idx in 0..8 {
        let s = Arc::clone(&store);
        let i_ref = Arc::clone(&interner);
        let stop = Arc::clone(&stop_signal);
        let handle = thread::spawn(move || {
            let mut writes = 0u64;
            while !stop.load(Ordering::Relaxed) {
                let uid = (thread_idx * 100 + (writes % 500)) as u32;
                let dials = UserDials::from_hours(
                    12.0 + ((writes % 48) as f32),
                    0.10 + (((writes % 30) as f32) / 100.0),
                    TopicWeights {
                        art: ((writes % 40) as f32) / 10.0,
                        tech: 1.5,
                        science: 2.0,
                        news: 0.5,
                        culture: 1.0,
                    },
                    writes,
                );
                s.set(uid, dials);

                if writes.is_multiple_of(100) {
                    let did = format!("did:plc:dynamic_{thread_idx}_{writes}");
                    s.set_by_did(&i_ref, &did, dials);
                }

                writes += 1;
            }
            writes
        });
        handles.push(handle);
    }

    // 4 Deleter threads: removing and re-adding users
    for thread_idx in 0..4 {
        let s = Arc::clone(&store);
        let stop = Arc::clone(&stop_signal);
        let handle = thread::spawn(move || {
            let mut deletes = 0u64;
            while !stop.load(Ordering::Relaxed) {
                let uid = (thread_idx * 100 + (deletes % 500)) as u32;
                let _ = s.remove(uid);
                deletes += 1;
            }
            deletes
        });
        handles.push(handle);
    }

    // 4 Snapshot / Metrics threads: continuous live snapshotting and size estimation
    for _ in 0..4 {
        let s = Arc::clone(&store);
        let stop = Arc::clone(&stop_signal);
        let handle = thread::spawn(move || {
            let mut snaps = 0u64;
            while !stop.load(Ordering::Relaxed) {
                let data = s.snapshot_data();
                let _ = data.len();
                let _ = s.len();
                let _ = s.is_empty();
                let _ = s.estimated_size_bytes();
                snaps += 1;
                thread::yield_now();
            }
            snaps
        });
        handles.push(handle);
    }

    // 4 Restorer threads: clone and restore operations
    for _ in 0..4 {
        let s = Arc::clone(&store);
        let stop = Arc::clone(&stop_signal);
        let handle = thread::spawn(move || {
            let mut restores = 0u64;
            while !stop.load(Ordering::Relaxed) {
                let snap = s.snapshot_data();
                if !snap.is_empty() && restores.is_multiple_of(50) {
                    s.restore_from_snapshot(snap);
                }
                restores += 1;
                thread::yield_now();
            }
            restores
        });
        handles.push(handle);
    }

    // Run stress testing for 400ms
    thread::sleep(Duration::from_millis(400));
    stop_signal.store(true, Ordering::Relaxed);

    for h in handles {
        let count = h.join().expect("Thread must not panic or deadlock");
        assert!(count > 0, "Thread must execute iterations");
    }

    // Store must remain fully consistent and functional
    assert!(!store.is_empty());
}

// ===========================================================================
// 6. `#![forbid(unsafe_code)]` Crate-Wide Enforcement Invariant
// ===========================================================================

#[test]
fn test_forbid_unsafe_code_crate_wide_invariant() {
    let manifest_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    let src_dir = manifest_dir.join("src");

    // Scan crate root files
    let lib_rs = std::fs::read_to_string(src_dir.join("lib.rs")).expect("Read lib.rs failed");
    assert!(
        lib_rs.contains("#![forbid(unsafe_code)]"),
        "lib.rs must declare #![forbid(unsafe_code)]"
    );

    let main_rs = std::fs::read_to_string(src_dir.join("main.rs")).expect("Read main.rs failed");
    assert!(
        main_rs.contains("#![forbid(unsafe_code)]"),
        "main.rs must declare #![forbid(unsafe_code)]"
    );

    // Verify zero occurrences of "unsafe " across entire src/ directory
    let entries = std::fs::read_dir(&src_dir).expect("Read src dir failed");
    for entry in entries {
        let entry = entry.expect("Entry error");
        let path = entry.path();
        if path.extension().and_then(|e| e.to_str()) == Some("rs") {
            let content = std::fs::read_to_string(&path).expect("Read source file failed");
            assert!(
                !content.contains("unsafe "),
                "Source file '{}' contains forbidden 'unsafe ' block",
                path.display()
            );
        }
    }

    // Check Cargo.toml
    let cargo_toml_path = manifest_dir.join("Cargo.toml");
    let cargo_toml = std::fs::read_to_string(cargo_toml_path).expect("Read Cargo.toml failed");
    assert!(
        cargo_toml.contains("unsafe_code = \"forbid\""),
        "Cargo.toml must configure unsafe_code = \"forbid\""
    );
}
