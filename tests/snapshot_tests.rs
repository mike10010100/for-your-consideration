#![allow(clippy::float_cmp)]

use std::fs::File;
use std::io::{Read, Write};
use std::path::PathBuf;
use std::time::Instant;

use for_your_consideration::prelude::*;

/// Helper to create a unique temporary snapshot path for each test.
fn temp_snapshot_path(test_name: &str) -> PathBuf {
    let mut path = std::env::temp_dir();
    let unique_id = format!(
        "for_your_consideration_snap_{}_{}_{}.bin",
        test_name,
        std::process::id(),
        Instant::now().elapsed().as_nanos()
    );
    path.push(unique_id);
    path
}

#[test]
fn test_snapshot_roundtrip_empty_graph() {
    let snapshot_path = temp_snapshot_path("empty");
    let interner = StringInterner::new();
    let graph = GraphStore::new();

    // 1. Save empty snapshot
    let cursor_us = 1_700_000_000_000_000;
    let header = save_snapshot(&snapshot_path, &interner, &graph, cursor_us)
        .expect("Failed to save empty snapshot");

    assert_eq!(header.magic, SNAPSHOT_MAGIC);
    assert_eq!(header.format_version, SNAPSHOT_FORMAT_VERSION);
    assert_eq!(header.jetstream_cursor_us, cursor_us);
    assert_eq!(header.num_strings, 0);
    assert_eq!(header.num_users, 0);
    assert_eq!(header.total_forward_edges, 0);
    assert_eq!(header.num_followers, 0);
    assert_eq!(header.num_post_metadata, 0);

    // 2. Load into fresh structures
    let loaded_interner = StringInterner::new();
    let loaded_graph = GraphStore::new();
    let loaded = load_snapshot(&snapshot_path, &loaded_interner, &loaded_graph)
        .expect("Failed to load empty snapshot")
        .expect("Snapshot should exist");

    assert_eq!(loaded.header.magic, SNAPSHOT_MAGIC);
    assert_eq!(loaded.header.jetstream_cursor_us, cursor_us);
    assert_eq!(loaded_interner.len(), 0);
    assert_eq!(loaded_graph.stats().total_users, 0);

    // Clean up
    let _ = std::fs::remove_file(&snapshot_path);
}

#[test]
fn test_snapshot_roundtrip_populated_graph() {
    let snapshot_path = temp_snapshot_path("populated");
    let interner = StringInterner::new();
    let graph = GraphStore::new();

    // 1. Populate interner
    let u1_str = "did:plc:alice";
    let u2_str = "did:plc:bob";
    let u3_str = "did:plc:charlie";
    let p1_str = "at://did:plc:alice/app.bsky.feed.post/post1";
    let p2_str = "at://did:plc:bob/app.bsky.feed.post/post2";
    let p3_str = "at://did:plc:charlie/app.bsky.feed.post/post3";

    let u1 = interner.intern(u1_str);
    let u2 = interner.intern(u2_str);
    let u3 = interner.intern(u3_str);
    let p1 = interner.intern(p1_str);
    let p2 = interner.intern(p2_str);
    let p3 = interner.intern(p3_str);

    // 2. Populate graph
    let base_ts = BLUESKY_EPOCH_SECS + 10_000;
    graph.record_interaction(u1, p1, SignalType::Like, base_ts);
    graph.record_interaction(u1, p2, SignalType::Repost, base_ts + 100);
    graph.record_interaction(u2, p1, SignalType::Quote, base_ts + 200);
    graph.record_interaction(u3, p3, SignalType::Like, base_ts + 300);

    graph.record_follow(u1, u2);
    graph.record_follow(u1, u3);
    graph.record_follow(u2, u3);

    graph.record_post_meta(p1, u1, None, None, base_ts);
    graph.record_post_meta(p2, u2, Some(p1), Some(p1), base_ts + 50);
    graph.record_post_meta(p3, u3, Some(p1), Some(p2), base_ts + 80);

    let cursor_us = 1_724_500_000_123_456;

    // 3. Save snapshot
    let save_header =
        save_snapshot(&snapshot_path, &interner, &graph, cursor_us).expect("Save snapshot failed");

    assert_eq!(save_header.num_strings, 6);
    assert_eq!(save_header.num_users, 3);
    assert_eq!(save_header.total_forward_edges, 4);
    assert_eq!(save_header.num_followers, 2);
    assert_eq!(save_header.num_post_metadata, 3);
    assert_eq!(save_header.jetstream_cursor_us, cursor_us);

    // 4. Load into fresh stores
    let restored_interner = StringInterner::new();
    let restored_graph = GraphStore::new();

    let loaded = load_snapshot(&snapshot_path, &restored_interner, &restored_graph)
        .expect("Load snapshot failed")
        .expect("Snapshot should exist");

    assert_eq!(loaded.header.num_strings, 6);
    assert_eq!(loaded.header.jetstream_cursor_us, cursor_us);

    // Verify StringInterner
    assert_eq!(restored_interner.len(), 6);
    assert_eq!(restored_interner.lookup_id(u1_str), Some(u1));
    assert_eq!(restored_interner.lookup_id(u2_str), Some(u2));
    assert_eq!(restored_interner.lookup_id(u3_str), Some(u3));
    assert_eq!(restored_interner.lookup_id(p1_str), Some(p1));
    assert_eq!(restored_interner.lookup_id(p2_str), Some(p2));
    assert_eq!(restored_interner.lookup_id(p3_str), Some(p3));
    assert_eq!(restored_interner.lookup_str(u1).as_deref(), Some(u1_str));

    // Verify GraphStore
    let stats = restored_graph.stats();
    assert_eq!(stats.total_users, 3);
    assert_eq!(stats.total_posts, 3);
    assert_eq!(stats.total_interactions, 4);
    assert_eq!(stats.total_follows, 3);
    assert_eq!(stats.total_metadata_entries, 3);

    // Verify forward interactions
    let u1_edges = restored_graph.get_user_interactions(u1);
    assert_eq!(u1_edges.len(), 2);
    assert_eq!(u1_edges[0].target(), p1);
    assert_eq!(u1_edges[0].signal(), SignalType::Like);
    assert_eq!(u1_edges[1].target(), p2);
    assert_eq!(u1_edges[1].signal(), SignalType::Repost);

    // Verify reverse interactions
    let p1_edges = restored_graph.get_post_interactions(p1);
    assert_eq!(p1_edges.len(), 2);
    assert_eq!(p1_edges[0].target(), u1);
    assert_eq!(p1_edges[1].target(), u2);

    // Verify Roaring Bitmaps
    let bm_u1 = restored_graph.get_user_likes_bitmap(u1).unwrap();
    assert!(bm_u1.contains(p1));
    assert!(bm_u1.contains(p2));
    assert_eq!(bm_u1.len(), 2);

    // Verify Follows
    let u1_follows = restored_graph.get_user_follows(u1);
    assert_eq!(u1_follows.len(), 2);
    assert!(u1_follows.contains(&u2));
    assert!(u1_follows.contains(&u3));

    // Verify Post Metadata and Author Reverse Index
    let meta_p2 = restored_graph.get_post_meta(p2).unwrap();
    assert_eq!(meta_p2.author_id, u2);
    assert_eq!(meta_p2.root_id, Some(p1));
    assert_eq!(meta_p2.parent_id, Some(p1));
    assert!(meta_p2.is_reply());

    let author_u2_posts = restored_graph.get_author_posts(u2);
    assert_eq!(author_u2_posts, vec![p2]);

    // Clean up
    let _ = std::fs::remove_file(&snapshot_path);
}

#[test]
fn test_snapshot_non_existent_file_returns_none() {
    let non_existent_path = PathBuf::from("does_not_exist_123456789.bin");
    let interner = StringInterner::new();
    let graph = GraphStore::new();

    let result = load_snapshot(&non_existent_path, &interner, &graph);
    assert!(result.is_ok());
    assert!(result.unwrap().is_none());
}

#[test]
fn test_snapshot_header_crc32_corruption_detection() {
    let snapshot_path = temp_snapshot_path("corrupt_header_crc");
    let interner = StringInterner::new();
    let graph = GraphStore::new();

    interner.intern("test_string");
    graph.record_interaction(1, 10, SignalType::Like, BLUESKY_EPOCH_SECS + 100);

    save_snapshot(&snapshot_path, &interner, &graph, 12345).expect("Save snapshot failed");

    // Read raw bytes and corrupt byte 10 in the header (created_at field)
    let mut file_bytes = Vec::new();
    {
        let mut file = File::open(&snapshot_path).unwrap();
        file.read_to_end(&mut file_bytes).unwrap();
    }

    file_bytes[10] ^= 0xFF; // flip bits in header

    {
        let mut file = File::create(&snapshot_path).unwrap();
        file.write_all(&file_bytes).unwrap();
    }

    // Load should fail with Header CRC32 checksum mismatch
    let restored_interner = StringInterner::new();
    let restored_graph = GraphStore::new();
    let result = load_snapshot(&snapshot_path, &restored_interner, &restored_graph);

    assert!(result.is_err());
    let err_str = result.unwrap_err().to_string();
    assert!(
        err_str.contains("Header CRC32 checksum mismatch"),
        "Unexpected error: {err_str}"
    );

    // Clean up
    let _ = std::fs::remove_file(&snapshot_path);
}

#[test]
fn test_snapshot_payload_crc32_corruption_detection() {
    let snapshot_path = temp_snapshot_path("corrupt_payload_crc");
    let interner = StringInterner::new();
    let graph = GraphStore::new();

    interner.intern("did:plc:alice");
    graph.record_interaction(1, 10, SignalType::Like, BLUESKY_EPOCH_SECS + 100);

    save_snapshot(&snapshot_path, &interner, &graph, 12345).expect("Save snapshot failed");

    // Corrupt byte 70 (in payload region after 64-byte header)
    let mut file_bytes = Vec::new();
    {
        let mut file = File::open(&snapshot_path).unwrap();
        file.read_to_end(&mut file_bytes).unwrap();
    }

    assert!(file_bytes.len() > HEADER_SIZE);
    file_bytes[HEADER_SIZE + 2] ^= 0xFF; // flip bits in payload

    {
        let mut file = File::create(&snapshot_path).unwrap();
        file.write_all(&file_bytes).unwrap();
    }

    // Load should fail with Payload CRC32 mismatch
    let restored_interner = StringInterner::new();
    let restored_graph = GraphStore::new();
    let result = load_snapshot(&snapshot_path, &restored_interner, &restored_graph);

    assert!(result.is_err());
    let err_str = result.unwrap_err().to_string();
    assert!(
        err_str.contains("Payload CRC32 mismatch"),
        "Unexpected error: {err_str}"
    );

    // Clean up
    let _ = std::fs::remove_file(&snapshot_path);
}

#[test]
fn test_snapshot_invalid_magic_detection() {
    let snapshot_path = temp_snapshot_path("invalid_magic");
    let interner = StringInterner::new();
    let graph = GraphStore::new();

    save_snapshot(&snapshot_path, &interner, &graph, 0).expect("Save failed");

    // Corrupt magic bytes
    let mut file_bytes = Vec::new();
    {
        let mut file = File::open(&snapshot_path).unwrap();
        file.read_to_end(&mut file_bytes).unwrap();
    }

    file_bytes[0] = b'B';
    file_bytes[1] = b'A';
    file_bytes[2] = b'D';
    file_bytes[3] = b'!';

    {
        let mut file = File::create(&snapshot_path).unwrap();
        file.write_all(&file_bytes).unwrap();
    }

    let result = load_snapshot(&snapshot_path, &interner, &graph);
    assert!(result.is_err());
    let err_str = result.unwrap_err().to_string();
    assert!(
        err_str.contains("Invalid snapshot magic bytes"),
        "Unexpected error: {err_str}"
    );

    // Clean up
    let _ = std::fs::remove_file(&snapshot_path);
}

#[test]
fn test_snapshot_unsupported_version_detection() {
    let snapshot_path = temp_snapshot_path("unsupported_version");
    let interner = StringInterner::new();
    let graph = GraphStore::new();

    save_snapshot(&snapshot_path, &interner, &graph, 0).expect("Save failed");

    // Corrupt format version to 99
    let mut file_bytes = Vec::new();
    {
        let mut file = File::open(&snapshot_path).unwrap();
        file.read_to_end(&mut file_bytes).unwrap();
    }

    file_bytes[4] = 99;
    file_bytes[5] = 0;

    {
        let mut file = File::create(&snapshot_path).unwrap();
        file.write_all(&file_bytes).unwrap();
    }

    let result = load_snapshot(&snapshot_path, &interner, &graph);
    assert!(result.is_err());
    let err_str = result.unwrap_err().to_string();
    assert!(
        err_str.contains("Unsupported snapshot version"),
        "Unexpected error: {err_str}"
    );

    // Clean up
    let _ = std::fs::remove_file(&snapshot_path);
}

#[test]
fn test_snapshot_truncated_file_detection() {
    let snapshot_path = temp_snapshot_path("truncated");
    {
        let mut file = File::create(&snapshot_path).unwrap();
        file.write_all(b"FYFD_too_short").unwrap();
    }

    let interner = StringInterner::new();
    let graph = GraphStore::new();
    let result = load_snapshot(&snapshot_path, &interner, &graph);

    assert!(result.is_err());
    let err_str = result.unwrap_err().to_string();
    assert!(err_str.contains("too small"), "Unexpected error: {err_str}");

    // Clean up
    let _ = std::fs::remove_file(&snapshot_path);
}

#[test]
fn test_snapshot_atomic_temp_cleanup() {
    let snapshot_path = temp_snapshot_path("atomic_cleanup");
    let interner = StringInterner::new();
    let graph = GraphStore::new();

    interner.intern("test_atomic");
    save_snapshot(&snapshot_path, &interner, &graph, 100).expect("Save failed");

    // Destination must exist
    assert!(snapshot_path.exists());

    // Temp file (.bin.tmp) must NOT exist
    let tmp_path = snapshot_path.with_extension("bin.tmp");
    let alt_tmp = {
        let mut n = snapshot_path.file_name().unwrap().to_os_string();
        n.push(".tmp");
        snapshot_path.with_file_name(n)
    };
    assert!(!tmp_path.exists());
    assert!(!alt_tmp.exists());

    // Clean up
    let _ = std::fs::remove_file(&snapshot_path);
}

#[test]
fn test_snapshot_performance_sub_50ms() {
    let snapshot_path = temp_snapshot_path("perf_sub_50ms");
    let interner = StringInterner::new();
    let graph = GraphStore::new();

    let base_ts = BLUESKY_EPOCH_SECS + 100_000;

    // Populate large dataset: 5,000 users, 10,000 posts, 50,000 interactions
    for u in 1..=5_000 {
        let did = format!("did:plc:user_{u}");
        interner.intern(&did);
    }

    for p in 1..=10_000 {
        let uri = format!("at://did:plc:author_{}/app.bsky.feed.post/{}", p % 500, p);
        interner.intern(&uri);
        graph.record_post_meta(p as u32, (p % 500) as u32, None, None, base_ts);
    }

    for i in 0..50_000 {
        let uid = ((i % 5_000) + 1) as u32;
        let pid = ((i % 10_000) + 1) as u32;
        let sig = match i % 3 {
            0 => SignalType::Like,
            1 => SignalType::Repost,
            _ => SignalType::Quote,
        };
        graph.record_interaction(uid, pid, sig, base_ts + (i % 3600) as u64);
    }

    for f in 1..=2_500 {
        graph.record_follow(f as u32, ((f + 1) % 5000 + 1) as u32);
    }

    // Save snapshot
    let save_header = save_snapshot(&snapshot_path, &interner, &graph, 9_999_999)
        .expect("Large snapshot save failed");
    assert_eq!(save_header.num_strings, 15_000);
    assert_eq!(save_header.num_users, 5_000);

    // Measure hydration latency
    let fresh_interner = StringInterner::new();
    let fresh_graph = GraphStore::new();

    let start = Instant::now();
    let loaded = load_snapshot(&snapshot_path, &fresh_interner, &fresh_graph)
        .expect("Load snapshot failed")
        .expect("Snapshot should exist");
    let elapsed_ms = start.elapsed().as_secs_f64() * 1000.0;

    println!(
        "Snapshot hydration benchmark: {:.2}ms for {} strings, {} users, {} edges (internal reported: {:.2}ms)",
        elapsed_ms, loaded.header.num_strings, loaded.header.num_users, loaded.header.total_forward_edges, loaded.load_duration_ms
    );

    // PRD requirement: Recovery time < 50 ms
    assert!(
        elapsed_ms < 50.0,
        "Snapshot hydration took {elapsed_ms:.2} ms, exceeding 50 ms budget"
    );

    // Verify stats match exactly
    let restored_stats = fresh_graph.stats();
    assert_eq!(restored_stats.total_users, 5_000);
    assert_eq!(restored_stats.total_posts, 10_000);
    assert_eq!(restored_stats.total_metadata_entries, 10_000);
    assert_eq!(restored_stats.total_follows, 2_500);

    // Clean up
    let _ = std::fs::remove_file(&snapshot_path);
}
