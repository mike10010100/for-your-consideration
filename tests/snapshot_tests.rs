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

    // PRD requirement: recovery time < 50 ms is an optimized-build SLA. Not asserted in
    // unoptimized/coverage-instrumented debug runs (parallel CI runners make wall-clock
    // ceilings nondeterministic); the printed timing above remains for observability.
    #[cfg(not(debug_assertions))]
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

#[test]
fn test_compact_memory_stores_garbage_collection() {
    let interner = StringInterner::new();
    let graph = GraphStore::new();
    let preferences = UserPreferencesStore::new();

    // 1. Populate active entities
    let uid_alice = interner.intern("did:plc:alice");
    let uid_bob = interner.intern("did:plc:bob");
    let pid_active = interner.intern("at://did:plc:bob/app.bsky.feed.post/active");

    // 2. Populate dead entities (will be pruned)
    let uid_dead = interner.intern("did:plc:dead_author");
    let pid_stale1 = interner.intern("at://did:plc:dead_author/app.bsky.feed.post/stale1");
    let _pid_stale2 = interner.intern("at://did:plc:dead_author/app.bsky.feed.post/stale2");

    assert_eq!(interner.len(), 6);

    // 3. Record graph interactions
    let now = BLUESKY_EPOCH_SECS + 100_000;
    let old_ts = BLUESKY_EPOCH_SECS + 1_000; // very old

    graph.record_interaction(uid_alice, pid_active, SignalType::Like, now);
    graph.record_interaction(uid_alice, pid_stale1, SignalType::Like, old_ts);
    graph.record_post_meta(pid_active, uid_bob, None, None, now);
    graph.record_post_meta(pid_stale1, uid_dead, None, None, old_ts);
    graph.record_follow(uid_alice, uid_bob);

    // 4. Save preference for Alice
    let dials = UserDials::default();
    preferences.set(uid_alice, dials);

    // 5. Prune older than cutoff (drops stale1 and dead_author)
    let cutoff = BLUESKY_EPOCH_SECS + 50_000;
    graph.prune_older_than(cutoff);

    assert_eq!(graph.get_user_interactions(uid_alice).len(), 1);
    assert_eq!(graph.get_user_interactions(uid_alice)[0].target, pid_active);
    assert!(graph.get_post_meta(pid_stale1).is_none());

    // 6. Compact memory stores
    let stats = compact_memory_stores(&interner, &graph, &preferences);
    assert_eq!(stats.strings_before, 6);
    assert_eq!(stats.strings_after, 3); // alice, bob, active

    // Dead strings must be purged from interner
    assert_eq!(interner.lookup_id("did:plc:dead_author"), None);
    assert_eq!(
        interner.lookup_id("at://did:plc:dead_author/app.bsky.feed.post/stale1"),
        None
    );
    assert_eq!(
        interner.lookup_id("at://did:plc:dead_author/app.bsky.feed.post/stale2"),
        None
    );

    // Active strings must still exist
    let new_uid_alice = interner
        .lookup_id("did:plc:alice")
        .expect("Alice must exist");
    let new_uid_bob = interner.lookup_id("did:plc:bob").expect("Bob must exist");
    let new_pid_active = interner
        .lookup_id("at://did:plc:bob/app.bsky.feed.post/active")
        .expect("Active post must exist");

    // Graph must have remapped IDs correctly
    let alice_edges = graph.get_user_interactions(new_uid_alice);
    assert_eq!(alice_edges.len(), 1);
    assert_eq!(alice_edges[0].target, new_pid_active);

    let active_meta = graph
        .get_post_meta(new_pid_active)
        .expect("Active meta must exist");
    assert_eq!(active_meta.author_id, new_uid_bob);

    let follows = graph.get_user_follows(new_uid_alice);
    assert_eq!(follows, vec![new_uid_bob]);

    let alice_prefs = preferences
        .get(new_uid_alice)
        .expect("Alice prefs must exist");
    assert_eq!(alice_prefs, dials);

    // 7. Verify snapshot roundtrip with compacted state
    let snapshot_path = temp_snapshot_path("compacted");
    let save_header =
        save_snapshot_with_preferences(&snapshot_path, &interner, &graph, &preferences, 12345)
            .expect("Snapshot save must succeed");
    assert_eq!(save_header.num_strings, 3);

    let fresh_interner = StringInterner::new();
    let fresh_graph = GraphStore::new();
    let fresh_prefs = UserPreferencesStore::new();
    let loaded =
        load_snapshot_with_preferences(&snapshot_path, &fresh_interner, &fresh_graph, &fresh_prefs)
            .expect("Load must succeed")
            .expect("Snapshot must exist");

    assert_eq!(loaded.header.num_strings, 3);
    assert_eq!(fresh_interner.len(), 3);
    assert_eq!(
        fresh_interner.lookup_id("did:plc:alice"),
        Some(new_uid_alice)
    );
    assert_eq!(fresh_interner.lookup_id("did:plc:dead_author"), None);

    let _ = std::fs::remove_file(&snapshot_path);
}

#[test]
fn test_compact_memory_stores_preserves_nsfw_and_author_posts() {
    let interner = StringInterner::new();
    let graph = GraphStore::new();
    let preferences = UserPreferencesStore::new();

    let uid_viewer = interner.intern("did:plc:viewer");
    let uid_author = interner.intern("did:plc:author");
    let _dead = interner.intern("did:plc:unreferenced_dead_string");
    let pid_sfw = interner.intern("at://did:plc:author/app.bsky.feed.post/sfw");
    let pid_nsfw = interner.intern("at://did:plc:author/app.bsky.feed.post/nsfw");
    let pid_stale = interner.intern("at://did:plc:author/app.bsky.feed.post/stale");

    let now = BLUESKY_EPOCH_SECS + 100_000;
    let old = BLUESKY_EPOCH_SECS + 1_000;

    graph.record_interaction(uid_viewer, pid_sfw, SignalType::Like, now);
    graph.record_interaction(uid_viewer, pid_nsfw, SignalType::Repost, now + 10);
    graph.record_interaction(uid_viewer, pid_stale, SignalType::Like, old);

    graph.record_post_meta_with_nsfw(pid_sfw, uid_author, None, None, now, false);
    graph.record_post_meta_with_nsfw(
        pid_nsfw,
        uid_author,
        Some(pid_sfw),
        Some(pid_sfw),
        now + 10,
        true,
    );
    graph.record_post_meta_with_nsfw(pid_stale, uid_author, None, None, old, true);

    graph.prune_older_than(BLUESKY_EPOCH_SECS + 50_000);
    assert_eq!(graph.total_nodes(), graph.stats().total_nodes());

    let compact_stats = compact_memory_stores(&interner, &graph, &preferences);
    assert_eq!(compact_stats.strings_before, 6);
    assert_eq!(compact_stats.strings_after, 4);

    let new_author = interner
        .lookup_id("did:plc:author")
        .expect("author must exist");
    let new_sfw = interner
        .lookup_id("at://did:plc:author/app.bsky.feed.post/sfw")
        .expect("sfw post must exist");
    let new_nsfw = interner
        .lookup_id("at://did:plc:author/app.bsky.feed.post/nsfw")
        .expect("nsfw post must exist");

    let meta_sfw = graph.get_post_meta(new_sfw).expect("sfw meta must exist");
    assert_eq!(meta_sfw.author_id, new_author);
    assert!(
        !meta_sfw.is_nsfw,
        "SFW post must remain is_nsfw=false after compaction"
    );

    let meta_nsfw = graph.get_post_meta(new_nsfw).expect("nsfw meta must exist");
    assert_eq!(meta_nsfw.author_id, new_author);
    assert_eq!(meta_nsfw.root_id, Some(new_sfw));
    assert_eq!(meta_nsfw.parent_id, Some(new_sfw));
    assert!(
        meta_nsfw.is_nsfw,
        "NSFW post must preserve is_nsfw=true after compaction"
    );

    let mut author_posts = graph.get_author_posts(new_author);
    author_posts.sort_unstable();
    let mut expected_posts = vec![new_sfw, new_nsfw];
    expected_posts.sort_unstable();
    assert_eq!(author_posts, expected_posts);
}

#[test]
fn test_prune_older_than_map_hysteresis_and_sparse_shrink() {
    let graph = GraphStore::new();
    let base_ts = BLUESKY_EPOCH_SECS + 10_000;

    // Populate shard 0 with 512 distinct users and posts (all multiples of 64 route to shard 0)
    for i in 1..=512u32 {
        let uid = i * 64;
        let pid = (i + 10_000) * 64;
        let ts = if i <= 32 {
            base_ts + 10
        } else {
            base_ts + 1_000
        };
        graph.record_interaction(uid, pid, SignalType::Like, ts);
        graph.record_post_meta_with_nsfw(pid, uid, None, None, ts, i % 2 == 0);
    }

    let size_before = graph.estimated_size_bytes();
    assert_eq!(graph.total_nodes(), 1024);

    // 1. Steady-state prune removing only ~6.25% (32 of 512) entries:
    // map capacity is within 2x of length, so hysteresis keeps existing bucket allocation.
    graph.prune_older_than(base_ts + 500);
    assert_eq!(graph.total_nodes(), 960);
    assert_eq!(graph.total_nodes(), graph.stats().total_nodes());
    let size_steady = graph.estimated_size_bytes();

    // 2. Sparse prune removing all remaining entries: capacity exceeds 2x len + floor(64),
    // so shrink_map_if_sparse reclaims shard map allocations.
    graph.prune_older_than(base_ts + 2_000);
    assert_eq!(graph.total_nodes(), 0);
    let size_empty = graph.estimated_size_bytes();
    assert!(
        size_empty < size_steady / 4 && size_empty < size_before / 4,
        "Expected sparse prune to shrink shard maps: before={size_before}, steady={size_steady}, empty={size_empty}"
    );
}

#[test]
fn test_systematic_prune_compact_and_snapshot_oracle_exploration() {
    use std::collections::{BTreeMap, BTreeSet};

    // Deterministic xorshift64 generator (seed = 0x5EED_2026_1029)
    let mut rng_state: u64 = 0x5EED_2026_1029;
    let mut next_u64 = || -> u64 {
        rng_state ^= rng_state << 13;
        rng_state ^= rng_state >> 7;
        rng_state ^= rng_state << 17;
        rng_state
    };

    for case_idx in 0..25u32 {
        let interner = StringInterner::new();
        let graph = GraphStore::new();
        let preferences = UserPreferencesStore::new();

        // Independent reference model keyed by string URIs/DIDs
        // edges_oracle: (user_did, post_uri, sig_code) -> timestamp
        let mut edges_oracle: BTreeMap<(String, String, u8), u64> = BTreeMap::new();
        // meta_oracle: post_uri -> (author_did, created_at, is_nsfw)
        let mut meta_oracle: BTreeMap<String, (String, u64, bool)> = BTreeMap::new();

        let num_users = ((next_u64() % 20) + 1) as usize;
        let num_posts = ((next_u64() % 40) + 1) as usize;
        let num_ops = ((next_u64() % 80) + 10) as usize;
        let base_ts = BLUESKY_EPOCH_SECS + 10_000;

        // Pre-intern some dead strings
        for d in 0..(case_idx % 7) {
            interner.intern(&format!("did:plc:dead_{case_idx}_{d}"));
        }

        for _ in 0..num_ops {
            let u_idx = (next_u64() as usize) % num_users;
            let p_idx = (next_u64() as usize) % num_posts;
            let a_idx = (next_u64() as usize) % num_users;
            let user_did = format!("did:plc:u_{case_idx}_{u_idx}");
            let author_did = format!("did:plc:u_{case_idx}_{a_idx}");
            let post_uri = format!("at://{author_did}/app.bsky.feed.post/p_{case_idx}_{p_idx}");

            let ts = base_ts + (next_u64() % 2_000);
            let is_nsfw = (next_u64() & 1) == 1;
            let sig_code = (next_u64() % 3) as u8;
            let sig = match sig_code {
                0 => SignalType::Like,
                1 => SignalType::Repost,
                _ => SignalType::Quote,
            };

            let uid = interner.intern(&user_did);
            let aid = interner.intern(&author_did);
            let pid = interner.intern(&post_uri);

            edges_oracle
                .entry((user_did.clone(), post_uri.clone(), sig_code))
                .or_insert(ts);
            graph.record_interaction(uid, pid, sig, ts);

            meta_oracle.insert(post_uri.clone(), (author_did.clone(), ts, is_nsfw));
            graph.record_post_meta_with_nsfw(pid, aid, None, None, ts, is_nsfw);
        }

        assert_eq!(graph.total_nodes(), graph.stats().total_nodes());

        // Pick a cutoff that prunes a subset (or boundary 0% / 100%) of records
        let cutoff = match case_idx % 5 {
            0 => base_ts,         // retain 100%
            1 => base_ts + 3_000, // prune 100%
            _ => base_ts + (next_u64() % 2_000),
        };

        graph.prune_older_than(cutoff);
        edges_oracle.retain(|_, &mut ts| ts >= cutoff);
        meta_oracle.retain(|_, (_, ts, _)| *ts >= cutoff);

        assert_eq!(graph.total_nodes(), graph.stats().total_nodes());

        // Compact memory stores
        let _ = compact_memory_stores(&interner, &graph, &preferences);
        assert_eq!(graph.total_nodes(), graph.stats().total_nodes());

        // Save and reload into fresh instances
        let snap_path = temp_snapshot_path(&format!("oracle_{case_idx}"));
        save_snapshot_with_preferences(&snap_path, &interner, &graph, &preferences, 999)
            .expect("save must succeed");

        let rest_interner = StringInterner::new();
        let rest_graph = GraphStore::new();
        let rest_prefs = UserPreferencesStore::new();
        load_snapshot_with_preferences(&snap_path, &rest_interner, &rest_graph, &rest_prefs)
            .expect("load must succeed")
            .expect("snapshot must exist");
        let _ = std::fs::remove_file(&snap_path);

        assert_eq!(rest_graph.total_nodes(), rest_graph.stats().total_nodes());
        assert_eq!(rest_graph.stats().total_interactions, edges_oracle.len());
        assert_eq!(rest_graph.stats().total_metadata_entries, meta_oracle.len());

        // Verify every surviving post metadata and NSFW flag matches the oracle
        let mut expected_author_posts: BTreeMap<String, BTreeSet<String>> = BTreeMap::new();
        for (post_uri, (author_did, exp_ts, exp_nsfw)) in &meta_oracle {
            let pid = rest_interner
                .lookup_id(post_uri)
                .expect("surviving post_uri must be interned");
            let aid = rest_interner
                .lookup_id(author_did)
                .expect("surviving author_did must be interned");
            let meta = rest_graph
                .get_post_meta(pid)
                .expect("surviving post meta must exist");
            assert_eq!(meta.author_id, aid);
            assert_eq!(meta.created_at, *exp_ts);
            assert_eq!(
                meta.is_nsfw, *exp_nsfw,
                "case {case_idx}: NSFW mismatch for {post_uri}"
            );
            expected_author_posts
                .entry(author_did.clone())
                .or_default()
                .insert(post_uri.clone());
        }

        // Verify author_posts index matches oracle
        for (author_did, exp_uris) in &expected_author_posts {
            let aid = rest_interner.lookup_id(author_did).unwrap();
            let actual_pids = rest_graph.get_author_posts(aid);
            let actual_uris: BTreeSet<String> = actual_pids
                .into_iter()
                .map(|pid| rest_interner.lookup_str(pid).unwrap().to_string())
                .collect();
            assert_eq!(
                &actual_uris, exp_uris,
                "case {case_idx}: author_posts mismatch"
            );
        }
    }
}
