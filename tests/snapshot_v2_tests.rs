#![forbid(unsafe_code)]
#![allow(clippy::float_cmp)]

use std::fs::File;
use std::io::Write;
use std::path::PathBuf;
use std::time::Instant;

use crc32fast::Hasher;
use for_your_consideration::prelude::*;

/// Helper to create a unique temporary snapshot path for each test.
fn temp_snapshot_path(test_name: &str) -> PathBuf {
    let mut path = std::env::temp_dir();
    let unique_id = format!(
        "fyc_snap_v2_{}_{}_{}.bin",
        test_name,
        std::process::id(),
        Instant::now().elapsed().as_nanos()
    );
    path.push(unique_id);
    path
}

#[test]
fn test_snapshot_v2_roundtrip_populated_preferences() {
    let snapshot_path = temp_snapshot_path("roundtrip_v2");
    let interner = StringInterner::new();
    let graph = GraphStore::new();
    let preferences = UserPreferencesStore::new();

    let u1 = interner.intern("did:plc:alice");
    let u2 = interner.intern("did:plc:bob");
    let p1 = interner.intern("at://did:plc:alice/app.bsky.feed.post/1");

    let now_secs = BLUESKY_EPOCH_SECS + 50_000;
    graph.record_interaction(u1, p1, SignalType::Like, now_secs);
    graph.record_follow(u1, u2);
    graph.record_post_meta(p1, u1, None, None, now_secs);

    let dials1 = UserDials::from_hours(
        12.0,
        0.25,
        TopicWeights {
            art: 1.5,
            tech: 2.0,
            science: 0.5,
            news: 1.0,
            culture: 1.2,
        },
        now_secs,
    );

    let dials2 = UserDials::from_hours(
        72.0,
        0.05,
        TopicWeights {
            art: 0.0,
            tech: 3.5,
            science: 1.0,
            news: 0.1,
            culture: 0.8,
        },
        now_secs + 10,
    );

    preferences.set(u1, dials1);
    preferences.set(u2, dials2);

    let cursor_us = 1_724_500_000_123_456;
    let save_header =
        save_snapshot_with_preferences(&snapshot_path, &interner, &graph, &preferences, cursor_us)
            .expect("Save V2 snapshot failed");

    assert_eq!(save_header.magic, SNAPSHOT_MAGIC);
    assert_eq!(save_header.format_version, SNAPSHOT_FORMAT_VERSION);
    assert_eq!(save_header.num_preferences, 2);

    // Load into fresh structures
    let loaded_interner = StringInterner::new();
    let loaded_graph = GraphStore::new();
    let loaded_preferences = UserPreferencesStore::new();

    let loaded = load_snapshot_with_preferences(
        &snapshot_path,
        &loaded_interner,
        &loaded_graph,
        &loaded_preferences,
    )
    .expect("Load snapshot failed")
    .expect("Snapshot must exist");

    assert_eq!(loaded.header.format_version, SNAPSHOT_FORMAT_VERSION);
    assert_eq!(loaded.header.num_preferences, 2);
    assert_eq!(loaded_preferences.len(), 2);
    assert_eq!(loaded_preferences.get(u1), Some(dials1));
    assert_eq!(loaded_preferences.get(u2), Some(dials2));
    assert_eq!(loaded_interner.lookup_id("did:plc:alice"), Some(u1));
    assert_eq!(loaded_graph.stats().total_interactions, 1);

    let _ = std::fs::remove_file(&snapshot_path);
}

#[test]
fn test_snapshot_v1_to_v2_backward_compatibility() {
    let snapshot_path = temp_snapshot_path("compat_v1_to_v2");
    let interner = StringInterner::new();
    let graph = GraphStore::new();

    let u1 = interner.intern("did:plc:v1user");
    let p1 = interner.intern("at://did:plc:v1user/post/1");
    graph.record_interaction(u1, p1, SignalType::Like, BLUESKY_EPOCH_SECS + 100);
    graph.record_post_meta(p1, u1, None, None, BLUESKY_EPOCH_SECS + 100);

    // Construct a synthetic Version 1 snapshot file (7 sections, format_version = 1)
    let strings = interner.export_strings();
    let graph_data = graph.snapshot_data();

    let mut payload = Vec::new();
    // Section 1: Strings
    payload.extend_from_slice(&(strings.len() as u32).to_le_bytes());
    for s in &strings {
        payload.extend_from_slice(&(s.len() as u32).to_le_bytes());
        payload.extend_from_slice(s.as_bytes());
    }
    // Section 2: User interactions
    payload.extend_from_slice(&(graph_data.user_interactions.len() as u32).to_le_bytes());
    for (uid, edges) in &graph_data.user_interactions {
        payload.extend_from_slice(&uid.to_le_bytes());
        payload.extend_from_slice(&(edges.len() as u32).to_le_bytes());
        for e in edges {
            payload.extend_from_slice(&e.target.to_le_bytes());
            payload.extend_from_slice(&e.packed.to_le_bytes());
        }
    }
    // Section 3: Post interactions
    payload.extend_from_slice(&(graph_data.post_interactions.len() as u32).to_le_bytes());
    for (pid, edges) in &graph_data.post_interactions {
        payload.extend_from_slice(&pid.to_le_bytes());
        payload.extend_from_slice(&(edges.len() as u32).to_le_bytes());
        for e in edges {
            payload.extend_from_slice(&e.target.to_le_bytes());
            payload.extend_from_slice(&e.packed.to_le_bytes());
        }
    }
    // Section 4: Roaring bitmaps
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
    // Section 6: Post metadata
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
    // Section 7: Active recent posts
    payload.extend_from_slice(&(graph_data.active_recent_posts.len() as u32).to_le_bytes());
    for (pid, ts) in &graph_data.active_recent_posts {
        payload.extend_from_slice(&pid.to_le_bytes());
        payload.extend_from_slice(&ts.to_le_bytes());
    }

    // Compute payload CRC
    let mut p_hasher = Hasher::new();
    p_hasher.update(&payload);
    let payload_crc = p_hasher.finalize();

    // V1 Header with format_version = 1
    let mut header = [0u8; HEADER_SIZE];
    header[0..4].copy_from_slice(&SNAPSHOT_MAGIC);
    header[4..6].copy_from_slice(&SNAPSHOT_FORMAT_VERSION_V1.to_le_bytes()); // Version 1
    header[6..8].copy_from_slice(&(HEADER_SIZE as u16).to_le_bytes());
    header[8..16].copy_from_slice(&1_700_000_000u64.to_le_bytes());
    header[16..24].copy_from_slice(&555_000u64.to_le_bytes());
    header[28..32].copy_from_slice(&(strings.len() as u32).to_le_bytes());
    header[32..36].copy_from_slice(&1u32.to_le_bytes());
    header[36..44].copy_from_slice(&1u64.to_le_bytes());
    header[48..52].copy_from_slice(&1u32.to_le_bytes());
    header[52..56].copy_from_slice(&payload_crc.to_le_bytes());

    let mut h_hasher = Hasher::new();
    h_hasher.update(&header[0..56]);
    let header_crc = h_hasher.finalize();
    header[56..60].copy_from_slice(&header_crc.to_le_bytes());

    let mut file_bytes = header.to_vec();
    file_bytes.extend_from_slice(&payload);
    std::fs::write(&snapshot_path, &file_bytes).unwrap();

    // Load V1 file with load_snapshot_with_preferences
    let loaded_interner = StringInterner::new();
    let loaded_graph = GraphStore::new();
    let loaded_preferences = UserPreferencesStore::new();

    let loaded = load_snapshot_with_preferences(
        &snapshot_path,
        &loaded_interner,
        &loaded_graph,
        &loaded_preferences,
    )
    .expect("V1 snapshot must load seamlessly into V2 engine")
    .expect("Snapshot must exist");

    assert_eq!(loaded.header.format_version, 1);
    assert_eq!(loaded_preferences.len(), 0);
    assert!(loaded_preferences.is_empty());
    assert_eq!(loaded_interner.lookup_id("did:plc:v1user"), Some(u1));
    assert_eq!(loaded_graph.stats().total_interactions, 1);

    let _ = std::fs::remove_file(&snapshot_path);
}

#[test]
fn test_snapshot_v2_with_empty_preferences() {
    let snapshot_path = temp_snapshot_path("empty_prefs");
    let interner = StringInterner::new();
    let graph = GraphStore::new();
    let preferences = UserPreferencesStore::new();

    save_snapshot_with_preferences(&snapshot_path, &interner, &graph, &preferences, 123)
        .expect("Save empty preferences V2 failed");

    let loaded_interner = StringInterner::new();
    let loaded_graph = GraphStore::new();
    let loaded_preferences = UserPreferencesStore::new();

    let loaded = load_snapshot_with_preferences(
        &snapshot_path,
        &loaded_interner,
        &loaded_graph,
        &loaded_preferences,
    )
    .expect("Load empty preferences V2 failed")
    .expect("Snapshot must exist");

    assert_eq!(loaded.header.format_version, SNAPSHOT_FORMAT_VERSION);
    assert_eq!(loaded.header.num_preferences, 0);
    assert!(loaded_preferences.is_empty());

    let _ = std::fs::remove_file(&snapshot_path);
}

#[test]
fn test_snapshot_v2_loaded_by_legacy_load_snapshot() {
    let snapshot_path = temp_snapshot_path("legacy_load");
    let interner = StringInterner::new();
    let graph = GraphStore::new();
    let preferences = UserPreferencesStore::new();

    let u = interner.intern("did:plc:test");
    preferences.set(u, UserDials::default());

    save_snapshot_with_preferences(&snapshot_path, &interner, &graph, &preferences, 456)
        .expect("Save failed");

    let loaded_interner = StringInterner::new();
    let loaded_graph = GraphStore::new();
    let loaded = load_snapshot(&snapshot_path, &loaded_interner, &loaded_graph)
        .expect("Legacy load must succeed on V2 snapshot")
        .expect("Snapshot must exist");

    assert_eq!(loaded.header.format_version, SNAPSHOT_FORMAT_VERSION);
    assert_eq!(loaded_interner.lookup_id("did:plc:test"), Some(u));

    let _ = std::fs::remove_file(&snapshot_path);
}

#[test]
fn test_snapshot_v2_boundary_dial_values() {
    let snapshot_path = temp_snapshot_path("boundary_dials");
    let interner = StringInterner::new();
    let graph = GraphStore::new();
    let preferences = UserPreferencesStore::new();

    let min_dials = UserDials::from_hours(
        FRESHNESS_MIN_HOURS,
        DISCOVERY_MIN,
        TopicWeights {
            art: TOPIC_MIN,
            tech: TOPIC_MIN,
            science: TOPIC_MIN,
            news: TOPIC_MIN,
            culture: TOPIC_MIN,
        },
        100,
    );

    let max_dials = UserDials::from_hours(
        FRESHNESS_MAX_HOURS,
        DISCOVERY_MAX,
        TopicWeights {
            art: TOPIC_MAX,
            tech: TOPIC_MAX,
            science: TOPIC_MAX,
            news: TOPIC_MAX,
            culture: TOPIC_MAX,
        },
        200,
    );

    preferences.set(1, min_dials);
    preferences.set(2, max_dials);

    save_snapshot_with_preferences(&snapshot_path, &interner, &graph, &preferences, 999)
        .expect("Save boundary dials failed");

    let loaded_interner = StringInterner::new();
    let loaded_graph = GraphStore::new();
    let loaded_preferences = UserPreferencesStore::new();

    load_snapshot_with_preferences(
        &snapshot_path,
        &loaded_interner,
        &loaded_graph,
        &loaded_preferences,
    )
    .expect("Load boundary dials failed")
    .expect("Snapshot must exist");

    assert_eq!(loaded_preferences.get(1), Some(min_dials));
    assert_eq!(loaded_preferences.get(2), Some(max_dials));

    let _ = std::fs::remove_file(&snapshot_path);
}

#[test]
fn test_snapshot_v2_corrupted_dial_record_rejected() {
    let snapshot_path = temp_snapshot_path("corrupt_dials");
    let interner = StringInterner::new();
    let graph = GraphStore::new();
    let preferences = UserPreferencesStore::new();

    // Construct valid base payload
    let strings = interner.export_strings();
    let graph_data = graph.snapshot_data();

    let mut payload = Vec::new();
    payload.extend_from_slice(&(strings.len() as u32).to_le_bytes());
    payload.extend_from_slice(&(graph_data.user_interactions.len() as u32).to_le_bytes());
    payload.extend_from_slice(&(graph_data.post_interactions.len() as u32).to_le_bytes());
    payload.extend_from_slice(&(graph_data.user_likes_bitmaps.len() as u32).to_le_bytes());
    payload.extend_from_slice(&(graph_data.follows.len() as u32).to_le_bytes());
    payload.extend_from_slice(&(graph_data.post_metadata.len() as u32).to_le_bytes());
    payload.extend_from_slice(&(graph_data.active_recent_posts.len() as u32).to_le_bytes());

    // Section 8: 1 corrupt preference (NaN freshness)
    payload.extend_from_slice(&1u32.to_le_bytes()); // num_preferences = 1
    payload.extend_from_slice(&42u32.to_le_bytes()); // user_id = 42
    payload.extend_from_slice(&f32::NAN.to_le_bytes()); // freshness = NaN
    payload.extend_from_slice(&0.15f32.to_le_bytes()); // discovery = 0.15
    payload.extend_from_slice(&1.0f32.to_le_bytes()); // art
    payload.extend_from_slice(&1.0f32.to_le_bytes()); // tech
    payload.extend_from_slice(&1.0f32.to_le_bytes()); // science
    payload.extend_from_slice(&1.0f32.to_le_bytes()); // news
    payload.extend_from_slice(&1.0f32.to_le_bytes()); // culture
    payload.extend_from_slice(&[0u8]); // include_replies = false
    payload.extend_from_slice(&3u32.to_le_bytes()); // min_likes = 3
    payload.extend_from_slice(&1000u64.to_le_bytes()); // updated_at

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
    std::fs::write(&snapshot_path, &file_bytes).unwrap();

    let res = load_snapshot_with_preferences(&snapshot_path, &interner, &graph, &preferences);
    assert!(
        res.is_err(),
        "Corrupted preference record with NaN freshness must be rejected"
    );
    let err = res.unwrap_err().to_string();
    assert!(
        err.contains("Corrupted user preference") || err.contains("Freshness"),
        "Unexpected error message: {err}"
    );

    let _ = std::fs::remove_file(&snapshot_path);
}

#[test]
fn test_snapshot_v2_truncated_section_8_rejected() {
    let snapshot_path = temp_snapshot_path("truncated_sec8");
    let interner = StringInterner::new();
    let graph = GraphStore::new();
    let preferences = UserPreferencesStore::new();

    preferences.set(10, UserDials::default());
    save_snapshot_with_preferences(&snapshot_path, &interner, &graph, &preferences, 0)
        .expect("Save failed");

    let mut file_bytes = std::fs::read(&snapshot_path).unwrap();
    // Truncate last 10 bytes (middle of Section 8)
    file_bytes.truncate(file_bytes.len().saturating_sub(10));
    std::fs::write(&snapshot_path, &file_bytes).unwrap();

    let res = load_snapshot_with_preferences(&snapshot_path, &interner, &graph, &preferences);
    assert!(res.is_err(), "Truncated Section 8 must be rejected");

    let _ = std::fs::remove_file(&snapshot_path);
}

#[test]
fn test_snapshot_v2_atomic_rename_cleanup() {
    let snapshot_path = temp_snapshot_path("atomic_rename");
    let tmp_path = {
        let mut tmp_name = snapshot_path.file_name().unwrap().to_os_string();
        tmp_name.push(".tmp");
        snapshot_path.with_file_name(tmp_name)
    };

    // Pre-create a dirty .tmp file
    {
        let mut f = File::create(&tmp_path).unwrap();
        f.write_all(b"DIRTY_LEFTOVER_TEMP_DATA").unwrap();
    }
    assert!(tmp_path.exists());

    let interner = StringInterner::new();
    let graph = GraphStore::new();
    let preferences = UserPreferencesStore::new();

    save_snapshot_with_preferences(&snapshot_path, &interner, &graph, &preferences, 777)
        .expect("Save must overwrite dirty tmp file cleanly");

    assert!(snapshot_path.exists());
    assert!(!tmp_path.exists(), ".tmp file must be renamed away");

    let loaded = load_snapshot_with_preferences(&snapshot_path, &interner, &graph, &preferences)
        .expect("Must load successfully")
        .expect("Must exist");
    assert_eq!(loaded.header.jetstream_cursor_us, 777);

    let _ = std::fs::remove_file(&snapshot_path);
}

#[test]
fn test_snapshot_v2_scale_5000_profiles() {
    let snapshot_path = temp_snapshot_path("scale_5000");
    let interner = StringInterner::new();
    let graph = GraphStore::new();
    let preferences = UserPreferencesStore::new();

    for i in 0..5000 {
        let dials = UserDials::from_hours(
            1.0 + (i % 168) as f32,
            ((i % 50) as f32) / 100.0,
            TopicWeights::default(),
            u64::from(i),
        );
        preferences.set(i, dials);
    }

    let save_start = Instant::now();
    save_snapshot_with_preferences(&snapshot_path, &interner, &graph, &preferences, 12345)
        .expect("Save 5000 profiles failed");
    let save_duration = save_start.elapsed();
    assert!(
        save_duration.as_millis() < 500,
        "Save duration too high: {:?}",
        save_duration
    );

    let loaded_interner = StringInterner::new();
    let loaded_graph = GraphStore::new();
    let loaded_preferences = UserPreferencesStore::new();

    let load_start = Instant::now();
    let loaded = load_snapshot_with_preferences(
        &snapshot_path,
        &loaded_interner,
        &loaded_graph,
        &loaded_preferences,
    )
    .expect("Load 5000 profiles failed")
    .expect("Must exist");
    let load_duration = load_start.elapsed();

    assert_eq!(loaded.header.num_preferences, 5000);
    assert_eq!(loaded_preferences.len(), 5000);
    assert!(
        load_duration.as_millis() < 500,
        "Load duration too high: {:?}",
        load_duration
    );

    let _ = std::fs::remove_file(&snapshot_path);
}
