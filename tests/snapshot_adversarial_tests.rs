#![allow(clippy::pedantic, clippy::nursery, clippy::float_cmp)]

//! Empirical Challenger Test Suite: Milestone 1 Snapshot Adversarial Stress Testing
//!
//! Thoroughly stress-tests:
//! 1. Systematic bit rot & single-bit-flip sweep across header and payload bytes.
//! 2. Truncation sweep across all byte offsets (0 to full length).
//! 3. Trailing garbage injection and corrupted payload lengths.
//! 4. ByteSliceReader fuzzing: invalid UTF-8, malformed RoaringBitmaps, OOB offsets, huge allocation headers.
//! 5. Pathological graph states: 0 nodes, 100k nodes, circular follows, recursive parent loops, unicode extremities.
//! 6. High-concurrency stress harness: simultaneous snapshot saving, graph mutation, and reads.
//! 7. Filesystem edge cases: nested directory creation, repeated overwrites, atomic cleanup.
//! 8. Empirical hydration latency benchmark under load.

mod common;

use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use crc32fast::Hasher;
use for_your_consideration::graph::GraphStore;
use for_your_consideration::interner::StringInterner;
use for_your_consideration::snapshot::{
    load_snapshot, save_snapshot, HEADER_SIZE, SNAPSHOT_FORMAT_VERSION, SNAPSHOT_MAGIC,
};
use for_your_consideration::types::{SignalType, BLUESKY_EPOCH_SECS};

/// Helper to generate unique temp paths.
fn temp_snap_path(tag: &str) -> PathBuf {
    let mut p = std::env::temp_dir();
    p.push(format!(
        "fyf_adv_snap_{}_{}_{}.bin",
        tag,
        std::process::id(),
        Instant::now().elapsed().as_nanos()
    ));
    p
}

/// Helper to create a populated baseline graph with diverse structures.
fn create_test_populated_graph() -> (StringInterner, GraphStore, u64) {
    let interner = StringInterner::new();
    let graph = GraphStore::new();
    let base_ts = BLUESKY_EPOCH_SECS + 50_000;

    for i in 1..=50 {
        let u = interner.intern(&format!("did:plc:user_{i}"));
        let p = interner.intern(&format!("at://did:plc:user_{i}/post/{i}"));
        let p_other = interner.intern(&format!("at://did:plc:user_{}/post/{}", (i % 50) + 1, i));

        graph.record_interaction(u, p, SignalType::Like, base_ts + (i as u64) * 10);
        graph.record_interaction(u, p_other, SignalType::Repost, base_ts + (i as u64) * 20);

        graph.record_follow(u, ((i % 50) + 1) as u32);
        graph.record_post_meta(p, u, None, None, base_ts + (i as u64) * 5);
    }

    (interner, graph, 1_724_000_000_000_000)
}

// ===========================================================================
// 1. Bit Rot & Systematic Single-Bit-Flip Sweep
// ===========================================================================

#[test]
fn test_systematic_single_bit_flip_sweep() {
    let path = temp_snap_path("bitflip_sweep");
    let (interner, graph, cursor_us) = create_test_populated_graph();

    save_snapshot(&path, &interner, &graph, cursor_us).expect("Save snapshot failed");

    let original_bytes = std::fs::read(&path).expect("Failed to read snapshot file");
    assert!(original_bytes.len() > HEADER_SIZE);

    let total_bytes = original_bytes.len();
    let mut corrupt_path = path.clone();
    corrupt_path.set_extension("corrupt.bin");

    // Sweep every single active byte:
    // - Header metadata & header CRC: bytes 0..60 (bytes 60..64 are reserved padding)
    // - Payload: bytes 64..total_bytes (sampled every 3rd byte for performance)
    let mut tests_run = 0;
    for byte_idx in 0..total_bytes {
        // Skip reserved 4-byte padding at offset 60..64 in the fixed 64-byte header
        if (60..64).contains(&byte_idx) {
            continue;
        }

        if byte_idx >= HEADER_SIZE && byte_idx % 3 != 0 {
            continue;
        }

        for bit in 0..8 {
            let mut corrupted = original_bytes.clone();
            corrupted[byte_idx] ^= 1 << bit;

            std::fs::write(&corrupt_path, &corrupted).expect("Failed to write corrupt snapshot");

            let fresh_interner = StringInterner::new();
            let fresh_graph = GraphStore::new();

            let load_res = load_snapshot(&corrupt_path, &fresh_interner, &fresh_graph);

            // In all cases, corrupted files MUST return Err, and MUST NEVER panic
            assert!(
                load_res.is_err(),
                "Corrupted bit {bit} at byte {byte_idx}/{total_bytes} was not detected!"
            );

            tests_run += 1;
        }
    }

    assert!(
        tests_run > 500,
        "Expected at least 500 bit-flip tests, ran {tests_run}"
    );

    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_file(&corrupt_path);
}

#[test]
fn test_random_multi_byte_bit_rot_fuzzing() {
    let path = temp_snap_path("multi_bit_rot");
    let (interner, graph, cursor_us) = create_test_populated_graph();

    save_snapshot(&path, &interner, &graph, cursor_us).expect("Save failed");
    let original_bytes = std::fs::read(&path).expect("Failed to read snapshot");

    let corrupt_path = temp_snap_path("fuzz_target");

    // Run 500 iterations of random bit rots across random non-padding positions
    for iter in 0..500 {
        let mut corrupted = original_bytes.clone();
        let num_corruptions = (iter % 10) + 1;

        for c in 0..num_corruptions {
            let raw_pos = (iter * 37 + c * 13 + 11) % corrupted.len();
            // If in reserved padding 60..64, shift to payload
            let pos = if (60..64).contains(&raw_pos) {
                64 + (raw_pos % 20)
            } else {
                raw_pos
            };
            let bit = (c + iter) % 8;
            corrupted[pos] ^= 1 << bit;
        }

        std::fs::write(&corrupt_path, &corrupted).expect("Write corrupted file failed");

        let fresh_interner = StringInterner::new();
        let fresh_graph = GraphStore::new();

        let result = load_snapshot(&corrupt_path, &fresh_interner, &fresh_graph);
        assert!(
            result.is_err(),
            "Random multi-byte corruption at iter {iter} was silently accepted!"
        );
    }

    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_file(&corrupt_path);
}

// ===========================================================================
// 2. Truncation Sweep Across All Byte Offsets
// ===========================================================================

#[test]
fn test_truncation_sweep_all_byte_offsets() {
    let path = temp_snap_path("trunc_sweep");
    let (interner, graph, cursor_us) = create_test_populated_graph();

    save_snapshot(&path, &interner, &graph, cursor_us).expect("Save snapshot failed");
    let original_bytes = std::fs::read(&path).expect("Read snapshot failed");
    let total_len = original_bytes.len();

    let trunc_path = temp_snap_path("trunc_target");

    // Test every single prefix length from 0 to total_len - 1
    for cut_len in 0..total_len {
        let truncated = &original_bytes[0..cut_len];
        std::fs::write(&trunc_path, truncated).expect("Write truncated file failed");

        let fresh_interner = StringInterner::new();
        let fresh_graph = GraphStore::new();

        let res = load_snapshot(&trunc_path, &fresh_interner, &fresh_graph);
        assert!(
            res.is_err(),
            "Truncation at length {cut_len}/{total_len} did not return error!"
        );
    }

    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_file(&trunc_path);
}

// ===========================================================================
// 3. Trailing Garbage & Boundary Anomalies
// ===========================================================================

#[test]
fn test_trailing_garbage_injection() {
    let path = temp_snap_path("trailing_garbage");
    let (interner, graph, cursor_us) = create_test_populated_graph();

    save_snapshot(&path, &interner, &graph, cursor_us).expect("Save failed");
    let mut bytes = std::fs::read(&path).expect("Read failed");

    // Append 1 byte
    bytes.push(0x42);
    let test_path = temp_snap_path("trailing_1");
    std::fs::write(&test_path, &bytes).unwrap();

    let i = StringInterner::new();
    let g = GraphStore::new();
    let res1 = load_snapshot(&test_path, &i, &g);
    assert!(res1.is_err(), "Trailing 1-byte garbage was not rejected!");

    // Append 1024 bytes of random noise
    bytes.extend(vec![0xAA; 1024]);
    let test_path2 = temp_snap_path("trailing_1024");
    std::fs::write(&test_path2, &bytes).unwrap();

    let res2 = load_snapshot(&test_path2, &i, &g);
    assert!(
        res2.is_err(),
        "Trailing 1024-byte garbage was not rejected!"
    );

    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_file(&test_path);
    let _ = std::fs::remove_file(&test_path2);
}

// ===========================================================================
// 4. Adversarial Deserializer & ByteSliceReader Attacks
// ===========================================================================

#[test]
fn test_adversarial_invalid_utf8_in_payload() {
    // Construct a snapshot where string bytes contain invalid UTF-8 (e.g. 0xFF 0xFE)
    // with recalculation of CRC32 to test the UTF-8 decoder specifically
    let mut payload = Vec::new();

    // Section 1: Strings (1 string of length 4 with invalid UTF-8)
    payload.extend_from_slice(&1u32.to_le_bytes()); // num_strings = 1
    payload.extend_from_slice(&4u32.to_le_bytes()); // str_len = 4
    payload.extend_from_slice(&[0xFF, 0xFE, 0xAA, 0xBB]); // Invalid UTF-8 bytes

    // Sections 2-7: 0 counts
    payload.extend_from_slice(&0u32.to_le_bytes()); // users
    payload.extend_from_slice(&0u32.to_le_bytes()); // posts
    payload.extend_from_slice(&0u32.to_le_bytes()); // bitmaps
    payload.extend_from_slice(&0u32.to_le_bytes()); // follows
    payload.extend_from_slice(&0u32.to_le_bytes()); // metadata
    payload.extend_from_slice(&0u32.to_le_bytes()); // active recent

    // Compute payload CRC
    let mut p_hasher = Hasher::new();
    p_hasher.update(&payload);
    let payload_crc = p_hasher.finalize();

    // Build valid header matching this payload
    let mut header = [0u8; HEADER_SIZE];
    header[0..4].copy_from_slice(&SNAPSHOT_MAGIC);
    header[4..6].copy_from_slice(&SNAPSHOT_FORMAT_VERSION.to_le_bytes());
    header[6..8].copy_from_slice(&(HEADER_SIZE as u16).to_le_bytes());
    header[28..32].copy_from_slice(&1u32.to_le_bytes()); // num_strings
    header[52..56].copy_from_slice(&payload_crc.to_le_bytes());

    let mut h_hasher = Hasher::new();
    h_hasher.update(&header[0..56]);
    let header_crc = h_hasher.finalize();
    header[56..60].copy_from_slice(&header_crc.to_le_bytes());

    let mut file_bytes = header.to_vec();
    file_bytes.extend_from_slice(&payload);

    let test_path = temp_snap_path("invalid_utf8");
    std::fs::write(&test_path, &file_bytes).unwrap();

    let i = StringInterner::new();
    let g = GraphStore::new();
    let res = load_snapshot(&test_path, &i, &g);

    assert!(res.is_err());
    let err = res.unwrap_err().to_string();
    assert!(
        err.contains("Invalid UTF-8"),
        "Expected Invalid UTF-8 error, got: {err}"
    );

    let _ = std::fs::remove_file(&test_path);
}

#[test]
fn test_adversarial_corrupted_roaring_bitmap_payload() {
    let mut payload = Vec::new();

    // Section 1: Strings (0)
    payload.extend_from_slice(&0u32.to_le_bytes());
    // Section 2: User interactions (0)
    payload.extend_from_slice(&0u32.to_le_bytes());
    // Section 3: Post interactions (0)
    payload.extend_from_slice(&0u32.to_le_bytes());

    // Section 4: Roaring Bitmaps (1 user with 8 bytes of garbage bitmap data)
    payload.extend_from_slice(&1u32.to_le_bytes()); // 1 user bitmap
    payload.extend_from_slice(&42u32.to_le_bytes()); // uid = 42
    payload.extend_from_slice(&8u32.to_le_bytes()); // bm length = 8
    payload.extend_from_slice(&[0xDE, 0xAD, 0xBE, 0xEF, 0x00, 0x11, 0x22, 0x33]); // Garbage

    // Section 5-7: 0
    payload.extend_from_slice(&0u32.to_le_bytes());
    payload.extend_from_slice(&0u32.to_le_bytes());
    payload.extend_from_slice(&0u32.to_le_bytes());

    let mut p_hasher = Hasher::new();
    p_hasher.update(&payload);
    let payload_crc = p_hasher.finalize();

    let mut header = [0u8; HEADER_SIZE];
    header[0..4].copy_from_slice(&SNAPSHOT_MAGIC);
    header[4..6].copy_from_slice(&SNAPSHOT_FORMAT_VERSION.to_le_bytes());
    header[6..8].copy_from_slice(&(HEADER_SIZE as u16).to_le_bytes());
    header[52..56].copy_from_slice(&payload_crc.to_le_bytes());

    let mut h_hasher = Hasher::new();
    h_hasher.update(&header[0..56]);
    let header_crc = h_hasher.finalize();
    header[56..60].copy_from_slice(&header_crc.to_le_bytes());

    let mut file_bytes = header.to_vec();
    file_bytes.extend_from_slice(&payload);

    let test_path = temp_snap_path("corrupt_bm");
    std::fs::write(&test_path, &file_bytes).unwrap();

    let i = StringInterner::new();
    let g = GraphStore::new();
    let res = load_snapshot(&test_path, &i, &g);

    assert!(res.is_err());
    let err = res.unwrap_err().to_string();
    assert!(
        err.contains("RoaringBitmap deserialization failure") || err.contains("deserialization"),
        "Expected RoaringBitmap error, got: {err}"
    );

    let _ = std::fs::remove_file(&test_path);
}

#[test]
fn test_adversarial_declared_count_larger_than_actual_payload() {
    let mut payload = Vec::new();
    // Declare 100 strings, but provide 0 strings in payload
    payload.extend_from_slice(&100u32.to_le_bytes());

    let mut p_hasher = Hasher::new();
    p_hasher.update(&payload);
    let payload_crc = p_hasher.finalize();

    let mut header = [0u8; HEADER_SIZE];
    header[0..4].copy_from_slice(&SNAPSHOT_MAGIC);
    header[4..6].copy_from_slice(&SNAPSHOT_FORMAT_VERSION.to_le_bytes());
    header[6..8].copy_from_slice(&(HEADER_SIZE as u16).to_le_bytes());
    header[28..32].copy_from_slice(&100u32.to_le_bytes());
    header[52..56].copy_from_slice(&payload_crc.to_le_bytes());

    let mut h_hasher = Hasher::new();
    h_hasher.update(&header[0..56]);
    let header_crc = h_hasher.finalize();
    header[56..60].copy_from_slice(&header_crc.to_le_bytes());

    let mut file_bytes = header.to_vec();
    file_bytes.extend_from_slice(&payload);

    let test_path = temp_snap_path("eof_count");
    std::fs::write(&test_path, &file_bytes).unwrap();

    let i = StringInterner::new();
    let g = GraphStore::new();
    let res = load_snapshot(&test_path, &i, &g);

    assert!(res.is_err());
    let err = res.unwrap_err().to_string();
    assert!(
        err.contains("Unexpected EOF"),
        "Expected EOF error, got: {err}"
    );

    let _ = std::fs::remove_file(&test_path);
}

// ===========================================================================
// 5. Pathological Graph States & Extreme Topologies
// ===========================================================================

#[test]
fn test_pathological_empty_strings_and_extreme_unicode() {
    let path = temp_snap_path("extreme_unicode");
    let interner = StringInterner::new();
    let graph = GraphStore::new();
    let now = BLUESKY_EPOCH_SECS + 100_000;

    // Test extreme unicode strings: empty, emoji clusters, RTL, Chinese/Japanese/Korean, control chars
    let s_empty = interner.intern("");
    let s_emoji = interner.intern("did:plc:🦀🔥🚀❤️\u{1F600}\u{1F92F}");
    let s_rtl = interner.intern("did:plc:عربى_עברית_مرحبا");
    let s_cjk = interner.intern("did:plc:日本語_中文_한국어_테스트");
    let s_long = interner.intern(&"a".repeat(10_000));

    let p_root = interner.intern("at://did:plc:root/post/1");
    let p_child = interner.intern("at://did:plc:child/post/2");

    graph.record_interaction(s_emoji, p_root, SignalType::Quote, now);
    graph.record_interaction(s_rtl, p_child, SignalType::Like, now + 10);
    graph.record_follow(s_emoji, s_rtl);
    graph.record_follow(s_rtl, s_cjk);
    graph.record_follow(s_cjk, s_emoji); // circular follow

    graph.record_post_meta(p_root, s_cjk, None, None, now);
    graph.record_post_meta(p_child, s_long, Some(p_root), Some(p_root), now + 5);

    let header = save_snapshot(&path, &interner, &graph, 555_444_333).expect("Save failed");
    assert_eq!(header.num_strings, 7);

    let restored_i = StringInterner::new();
    let restored_g = GraphStore::new();
    let loaded = load_snapshot(&path, &restored_i, &restored_g)
        .expect("Load failed")
        .expect("Snapshot should exist");

    assert_eq!(loaded.header.num_strings, 7);
    assert_eq!(restored_i.lookup_str(s_empty).as_deref(), Some(""));
    assert_eq!(
        restored_i.lookup_str(s_emoji).as_deref(),
        Some("did:plc:🦀🔥🚀❤️\u{1F600}\u{1F92F}")
    );
    assert_eq!(
        restored_i.lookup_str(s_rtl).as_deref(),
        Some("did:plc:عربى_עברית_مرحبا")
    );

    // Verify circular follows
    let emoji_follows = restored_g.get_user_follows(s_emoji);
    assert_eq!(emoji_follows, vec![s_rtl]);
    let rtl_follows = restored_g.get_user_follows(s_rtl);
    assert_eq!(rtl_follows, vec![s_cjk]);
    let cjk_follows = restored_g.get_user_follows(s_cjk);
    assert_eq!(cjk_follows, vec![s_emoji]);

    let _ = std::fs::remove_file(&path);
}

#[test]
fn test_pathological_circular_parent_reply_loops_and_detached_roots() {
    let path = temp_snap_path("circular_posts");
    let interner = StringInterner::new();
    let graph = GraphStore::new();
    let now = BLUESKY_EPOCH_SECS + 500;

    let a = interner.intern("did:plc:alice");
    let b = interner.intern("did:plc:bob");

    let p1 = interner.intern("at://did:plc:alice/post/1");
    let p2 = interner.intern("at://did:plc:bob/post/2");
    let p_nonexistent = 999_999u32; // Not in interner

    // Post 1 claims Post 2 is root; Post 2 claims Post 1 is root (cycle)
    graph.record_post_meta(p1, a, Some(p2), Some(p2), now);
    graph.record_post_meta(p2, b, Some(p1), Some(p1), now + 1);

    // Post with detached/nonexistent root
    let p3 = interner.intern("at://did:plc:alice/post/3");
    graph.record_post_meta(p3, a, Some(p_nonexistent), Some(p_nonexistent), now + 2);

    save_snapshot(&path, &interner, &graph, 123).expect("Save snapshot failed");

    let r_interner = StringInterner::new();
    let r_graph = GraphStore::new();
    let loaded = load_snapshot(&path, &r_interner, &r_graph)
        .expect("Load failed")
        .expect("Should exist");

    assert_eq!(loaded.header.num_post_metadata, 3);

    let m1 = r_graph.get_post_meta(p1).unwrap();
    assert_eq!(m1.root_id, Some(p2));

    let m3 = r_graph.get_post_meta(p3).unwrap();
    assert_eq!(m3.root_id, Some(p_nonexistent));

    let _ = std::fs::remove_file(&path);
}

#[test]
fn test_pathological_massive_graph_100k_elements() {
    let path = temp_snap_path("massive_100k");
    let interner = StringInterner::new();
    let graph = GraphStore::new();
    let now = BLUESKY_EPOCH_SECS + 200_000;

    let node_count = 20_000usize;
    let edge_count = 80_000usize;

    for i in 1..=node_count {
        let u = interner.intern(&format!("did:plc:mass_{i}"));
        let p = interner.intern(&format!("at://did:plc:mass_{i}/post/1"));
        graph.record_post_meta(p, u, None, None, now);
    }

    // Generate distinct (u, p) pairs across edges
    for e in 0..edge_count {
        let u = (e % node_count) + 1;
        let p = (((e * 7) + 13) % node_count) + 1;
        let sig = match e % 3 {
            0 => SignalType::Like,
            1 => SignalType::Repost,
            _ => SignalType::Quote,
        };
        graph.record_interaction(u as u32, p as u32, sig, now + (e as u64 % 3600));
    }

    let save_start = Instant::now();
    let header = save_snapshot(&path, &interner, &graph, 888_777_666).expect("Massive save failed");
    let save_ms = save_start.elapsed().as_secs_f64() * 1000.0;

    assert_eq!(header.num_strings, (node_count * 2) as u32);
    let total_edges = header.total_forward_edges as usize;
    assert!(total_edges > 0);

    let r_interner = StringInterner::new();
    let r_graph = GraphStore::new();

    let load_start = Instant::now();
    let loaded = load_snapshot(&path, &r_interner, &r_graph)
        .expect("Massive load failed")
        .expect("Snapshot exists");
    let load_ms = load_start.elapsed().as_secs_f64() * 1000.0;

    println!(
        "Massive Graph ({} strings, {} edges): Save = {:.2}ms, Load = {:.2}ms (internal: {:.2}ms)",
        header.num_strings, total_edges, save_ms, load_ms, loaded.load_duration_ms
    );

    // Verify recovery time within budget (<50ms for release, <250ms for debug)
    let max_budget_ms = if cfg!(debug_assertions) { 250.0 } else { 50.0 };
    assert!(
        load_ms < max_budget_ms,
        "Hydration for 100k elements took {load_ms:.2}ms, exceeding {max_budget_ms}ms requirement"
    );

    assert_eq!(r_graph.stats().total_interactions, total_edges);

    let _ = std::fs::remove_file(&path);
}

// ===========================================================================
// 6. High-Concurrency Stress Test
// ===========================================================================

#[test]
fn test_high_concurrency_continuous_snapshot_and_mutation() {
    let interner = Arc::new(StringInterner::new());
    let graph = Arc::new(GraphStore::new());

    // Pre-populate with initial data
    for i in 1..=500 {
        let u = interner.intern(&format!("did:plc:init_user_{i}"));
        let p = interner.intern(&format!("at://did:plc:init_user_{i}/post/1"));
        graph.record_interaction(u, p, SignalType::Like, BLUESKY_EPOCH_SECS + 100);
        graph.record_follow(u, ((i % 500) + 1) as u32);
        graph.record_post_meta(p, u, None, None, BLUESKY_EPOCH_SECS + 100);
    }

    let running = Arc::new(AtomicBool::new(true));
    let save_count = Arc::new(AtomicUsize::new(0));
    let write_count = Arc::new(AtomicUsize::new(0));
    let read_count = Arc::new(AtomicUsize::new(0));
    let load_verify_count = Arc::new(AtomicUsize::new(0));

    let mut handles = Vec::new();

    // 4 Writer threads
    for thread_id in 0..4 {
        let i_clone = Arc::clone(&interner);
        let g_clone = Arc::clone(&graph);
        let r_clone = Arc::clone(&running);
        let w_count = Arc::clone(&write_count);

        handles.push(thread::spawn(move || {
            let mut counter = 0u32;
            while r_clone.load(Ordering::Relaxed) {
                counter = counter.wrapping_add(1);
                let u_str = format!("did:plc:t{thread_id}_{counter}");
                let p_str = format!("at://did:plc:t{thread_id}/post/{counter}");
                let u = i_clone.intern(&u_str);
                let p = i_clone.intern(&p_str);

                g_clone.record_interaction(
                    u,
                    p,
                    SignalType::Like,
                    BLUESKY_EPOCH_SECS + (counter as u64),
                );
                g_clone.record_follow(u, ((counter % 500) + 1) as u32);
                g_clone.record_post_meta(p, u, None, None, BLUESKY_EPOCH_SECS + (counter as u64));
                w_count.fetch_add(1, Ordering::Relaxed);
            }
        }));
    }

    // 4 Reader threads
    for _ in 0..4 {
        let g_clone = Arc::clone(&graph);
        let r_clone = Arc::clone(&running);
        let rd_count = Arc::clone(&read_count);

        handles.push(thread::spawn(move || {
            let mut q = 1u32;
            while r_clone.load(Ordering::Relaxed) {
                q = (q % 500) + 1;
                let _ = g_clone.get_user_interactions(q);
                let _ = g_clone.get_user_follows(q);
                let _ = g_clone.get_post_meta(q);
                let _ = g_clone.get_user_likes_bitmap(q);
                rd_count.fetch_add(1, Ordering::Relaxed);
            }
        }));
    }

    // 2 Snapshot Saver threads
    let snap_dir = std::env::temp_dir().join(format!("fyf_concurrent_{}", std::process::id()));
    std::fs::create_dir_all(&snap_dir).unwrap();

    for saver_id in 0..2 {
        let i_clone = Arc::clone(&interner);
        let g_clone = Arc::clone(&graph);
        let r_clone = Arc::clone(&running);
        let s_count = Arc::clone(&save_count);
        let lv_count = Arc::clone(&load_verify_count);
        let snap_file = snap_dir.join(format!("snap_saver_{saver_id}.bin"));

        handles.push(thread::spawn(move || {
            let mut seq = 0u64;
            while r_clone.load(Ordering::Relaxed) {
                seq += 1;
                let save_res = save_snapshot(&snap_file, &i_clone, &g_clone, seq * 1000);
                assert!(save_res.is_ok(), "Concurrent snapshot save failed!");
                s_count.fetch_add(1, Ordering::Relaxed);

                // Immediately load and verify the newly saved snapshot in a separate instance
                let test_i = StringInterner::new();
                let test_g = GraphStore::new();
                let load_res = load_snapshot(&snap_file, &test_i, &test_g);
                assert!(load_res.is_ok(), "Concurrent snapshot load failed!");
                let loaded = load_res.unwrap().expect("Snapshot must exist");
                assert_eq!(loaded.header.jetstream_cursor_us, seq * 1000);
                assert!(loaded.header.num_strings > 0);
                lv_count.fetch_add(1, Ordering::Relaxed);

                thread::sleep(Duration::from_millis(1));
            }
        }));
    }

    // Run high-concurrency stress for 1.0 second
    thread::sleep(Duration::from_millis(1000));
    running.store(false, Ordering::SeqCst);

    for h in handles {
        h.join()
            .expect("Worker thread panicked during concurrency stress");
    }

    let final_saves = save_count.load(Ordering::Relaxed);
    let final_writes = write_count.load(Ordering::Relaxed);
    let final_reads = read_count.load(Ordering::Relaxed);
    let final_loads = load_verify_count.load(Ordering::Relaxed);

    println!(
        "High Concurrency Stress: {} saves, {} writes, {} reads, {} verified loads across 10 threads",
        final_saves, final_writes, final_reads, final_loads
    );

    assert!(final_saves >= 5, "Expected at least 5 snapshot saves");
    assert!(final_writes >= 1000, "Expected at least 1000 writes");
    assert!(final_reads >= 1000, "Expected at least 1000 reads");
    assert!(final_loads >= 5, "Expected at least 5 verified loads");

    let _ = std::fs::remove_dir_all(&snap_dir);
}

// ===========================================================================
// 7. Filesystem Edge Cases & Directory Creation
// ===========================================================================

#[test]
fn test_nested_nonexistent_directory_creation() {
    let mut path = std::env::temp_dir();
    path.push(format!("fyf_deep_{}", std::process::id()));
    path.push("level1");
    path.push("level2");
    path.push("level3");
    path.push("snapshot.bin");

    let interner = StringInterner::new();
    let graph = GraphStore::new();
    interner.intern("did:plc:test");

    let save_res = save_snapshot(&path, &interner, &graph, 1);
    assert!(save_res.is_ok(), "Failed to save into nested directory");
    assert!(path.exists());

    let r_interner = StringInterner::new();
    let r_graph = GraphStore::new();
    let load_res = load_snapshot(&path, &r_interner, &r_graph);
    assert!(load_res.is_ok());

    let _ = std::fs::remove_dir_all(
        std::env::temp_dir().join(format!("fyf_deep_{}", std::process::id())),
    );
}

#[test]
fn test_repeated_snapshot_overwrites() {
    let path = temp_snap_path("overwrites");
    let interner = StringInterner::new();
    let graph = GraphStore::new();

    for i in 1..=20 {
        let did = format!("did:plc:user_{i}");
        interner.intern(&did);
        graph.record_interaction(i, 100, SignalType::Like, BLUESKY_EPOCH_SECS + (i as u64));

        let header = save_snapshot(&path, &interner, &graph, i as u64 * 10).expect("Save failed");
        assert_eq!(header.num_strings, i);
        assert_eq!(header.jetstream_cursor_us, i as u64 * 10);

        let r_i = StringInterner::new();
        let r_g = GraphStore::new();
        let loaded = load_snapshot(&path, &r_i, &r_g)
            .expect("Load failed")
            .unwrap();
        assert_eq!(loaded.header.num_strings, i);
        assert_eq!(loaded.header.jetstream_cursor_us, i as u64 * 10);
    }

    let _ = std::fs::remove_file(&path);
}
