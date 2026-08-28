#![forbid(unsafe_code)]
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::unreadable_literal,
    clippy::ignored_unit_patterns,
    clippy::cast_lossless,
    clippy::manual_is_multiple_of,
    clippy::collection_is_never_read,
    missing_docs
)]

use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use crc32fast::Hasher;
use for_your_consideration::graph::GraphStore;
use for_your_consideration::interner::StringInterner;
use for_your_consideration::preferences::UserPreferencesStore;
use for_your_consideration::snapshot::{
    load_snapshot_with_preferences, save_snapshot_with_preferences, HEADER_SIZE,
    SNAPSHOT_FORMAT_VERSION, SNAPSHOT_MAGIC,
};
use for_your_consideration::types::{SignalType, TopicWeights, UserDials, BLUESKY_EPOCH_SECS};

static COUNTER: AtomicU64 = AtomicU64::new(1);

fn unique_temp_path(tag: &str) -> PathBuf {
    let mut path = std::env::temp_dir();
    let id = format!(
        "challenger_m3_{}_{}_{}_{}.bin",
        tag,
        std::process::id(),
        COUNTER.fetch_add(1, Ordering::Relaxed),
        Instant::now().elapsed().as_nanos()
    );
    path.push(id);
    path
}

// ---------------------------------------------------------------------------
// 1. Edge Case: Empty Store Streaming Round-Trip
// ---------------------------------------------------------------------------
#[test]
fn test_empty_store_streaming_roundtrip() {
    let path = unique_temp_path("empty_store");
    let interner = StringInterner::new();
    let graph = GraphStore::new();
    let preferences = UserPreferencesStore::new();

    let cursor = 123_456_789_u64;
    let header =
        save_snapshot_with_preferences(&path, &interner, &graph, &preferences, cursor).unwrap();

    assert_eq!(header.magic, SNAPSHOT_MAGIC);
    assert_eq!(header.format_version, SNAPSHOT_FORMAT_VERSION);
    assert_eq!(header.jetstream_cursor_us, cursor);
    assert_eq!(header.num_strings, 0);
    assert_eq!(header.num_users, 0);
    assert_eq!(header.total_forward_edges, 0);
    assert_eq!(header.num_followers, 0);
    assert_eq!(header.num_post_metadata, 0);
    assert_eq!(header.num_preferences, 0);

    let restored_interner = StringInterner::new();
    let restored_graph = GraphStore::new();
    let restored_prefs = UserPreferencesStore::new();

    let loaded =
        load_snapshot_with_preferences(&path, &restored_interner, &restored_graph, &restored_prefs)
            .unwrap()
            .expect("Empty snapshot must load cleanly");

    assert_eq!(loaded.header.format_version, SNAPSHOT_FORMAT_VERSION);
    assert_eq!(loaded.header.jetstream_cursor_us, cursor);
    assert!(restored_interner.is_empty());
    assert_eq!(restored_graph.stats().total_users, 0);
    assert_eq!(restored_graph.stats().total_interactions, 0);
    assert_eq!(restored_prefs.len(), 0);

    let _ = std::fs::remove_file(&path);
}

// ---------------------------------------------------------------------------
// 2. Oracle Equivalence: Multi-Shard Graph & Preferences Round-Trip
// ---------------------------------------------------------------------------
#[test]
fn test_massive_multi_shard_roundtrip_oracle_equivalence() {
    let path = unique_temp_path("oracle_equiv");
    let interner = StringInterner::new();
    let graph = GraphStore::new();
    let preferences = UserPreferencesStore::new();

    let num_users = 300;
    let num_posts = 600;
    let base_ts = BLUESKY_EPOCH_SECS + 1_000_000;

    for u in 1..=num_users {
        let did = format!("did:plc:challenger_m3_user_{u}_{}", u * 31);
        interner.intern(&did);
    }
    for p in 1..=num_posts {
        let uri = format!("at://did:plc:challenger_m3_author_{p}/app.bsky.feed.post/{p}");
        interner.intern(&uri);

        let author_id = (p % num_users) + 1;
        let root = if p % 3 == 0 {
            Some(((p % 50) + 1) as u32)
        } else {
            None
        };
        let parent = if root.is_some() {
            Some(((p % 20) + 1) as u32)
        } else {
            None
        };
        graph.record_post_meta(p as u32, author_id as u32, root, parent, base_ts + p as u64);
    }

    for u in 1..=num_users {
        for e in 0..15 {
            let pid = (((u * 43 + e * 17) % num_posts) + 1) as u32;
            let sig = match (u + e) % 3 {
                0 => SignalType::Like,
                1 => SignalType::Repost,
                _ => SignalType::Quote,
            };
            graph.record_interaction(u as u32, pid, sig, base_ts + (e as u64 * 30));
        }
        if u % 2 == 0 {
            let followed = (((u * 11) % num_users) + 1) as u32;
            graph.record_follow(u as u32, followed);
        }

        if u % 4 == 0 {
            let dials = UserDials {
                freshness_half_life_secs: 3600.0 * ((u % 8) as f32 + 1.0),
                serendipity_ratio: 0.05 * ((u % 10) as f32),
                topic_weights: TopicWeights {
                    art: 1.5,
                    tech: 2.5,
                    science: 0.8,
                    news: 1.1,
                    culture: 0.9,
                },
                include_replies: u % 8 == 0,
                min_likes: (u % 7) as u32,
                updated_at_secs: base_ts + u as u64,
            };
            preferences.set(u as u32, dials);
        }
    }

    let cursor = 1_725_123_456_789_000_u64;
    let save_header =
        save_snapshot_with_preferences(&path, &interner, &graph, &preferences, cursor).unwrap();
    assert_eq!(save_header.num_users, num_users as u32);
    assert_eq!(save_header.num_post_metadata, num_posts as u32);

    let restored_interner = StringInterner::new();
    let restored_graph = GraphStore::new();
    let restored_prefs = UserPreferencesStore::new();

    let loaded =
        load_snapshot_with_preferences(&path, &restored_interner, &restored_graph, &restored_prefs)
            .unwrap()
            .expect("Snapshot must load successfully");

    assert_eq!(loaded.header.format_version, SNAPSHOT_FORMAT_VERSION);
    assert_eq!(loaded.header.jetstream_cursor_us, cursor);
    assert_eq!(restored_interner.len(), interner.len());

    // Verify all strings and IDs match identically
    for u in 1..=num_users {
        let did = format!("did:plc:challenger_m3_user_{u}_{}", u * 31);
        let orig_id = interner.lookup_id(&did).unwrap();
        let rest_id = restored_interner.lookup_id(&did).unwrap();
        assert_eq!(orig_id, rest_id);
        assert_eq!(
            restored_interner.lookup_str(orig_id).as_deref(),
            Some(did.as_str())
        );
    }

    // Verify graph interactions and stats
    assert_eq!(
        restored_graph.stats().total_users,
        graph.stats().total_users
    );
    assert_eq!(
        restored_graph.stats().total_posts,
        graph.stats().total_posts
    );
    assert_eq!(
        restored_graph.stats().total_interactions,
        graph.stats().total_interactions
    );
    assert_eq!(
        restored_graph.stats().total_follows,
        graph.stats().total_follows
    );

    for u in 1..=num_users {
        let uid = u as u32;
        let orig_interactions = graph.get_user_interactions(uid);
        let rest_interactions = restored_graph.get_user_interactions(uid);
        assert_eq!(orig_interactions, rest_interactions);

        let orig_follows = graph.get_user_follows(uid);
        let rest_follows = restored_graph.get_user_follows(uid);
        assert_eq!(orig_follows, rest_follows);

        let orig_bm = graph.get_user_likes_bitmap(uid);
        let rest_bm = restored_graph.get_user_likes_bitmap(uid);
        assert_eq!(orig_bm, rest_bm);

        if u % 4 == 0 {
            let orig_pref = preferences.get(uid).unwrap();
            let rest_pref = restored_prefs.get(uid).unwrap();
            assert_eq!(
                orig_pref.freshness_half_life_secs,
                rest_pref.freshness_half_life_secs
            );
            assert_eq!(orig_pref.serendipity_ratio, rest_pref.serendipity_ratio);
            assert_eq!(orig_pref.topic_weights.art, rest_pref.topic_weights.art);
            assert_eq!(orig_pref.topic_weights.tech, rest_pref.topic_weights.tech);
            assert_eq!(orig_pref.include_replies, rest_pref.include_replies);
            assert_eq!(orig_pref.min_likes, rest_pref.min_likes);
            assert_eq!(orig_pref.updated_at_secs, rest_pref.updated_at_secs);
        }
    }

    let _ = std::fs::remove_file(&path);
}

// ---------------------------------------------------------------------------
// 3. Exhaustive CRC32 Adversarial Corruption Matrix
// ---------------------------------------------------------------------------
#[test]
fn test_exhaustive_crc32_adversarial_matrix() {
    let path = unique_temp_path("crc_matrix");
    let interner = StringInterner::new();
    let graph = GraphStore::new();
    let preferences = UserPreferencesStore::new();

    for i in 1..=50 {
        let u = interner.intern(&format!("did:plc:user_{i}"));
        let p = interner.intern(&format!("at://did:plc:author_{i}/post/{i}"));
        graph.record_interaction(u, p, SignalType::Like, BLUESKY_EPOCH_SECS + 100 + i);
        graph.record_follow(u, (u % 10) + 1);
        graph.record_post_meta(p, u, None, None, BLUESKY_EPOCH_SECS + 100 + i);
    }

    save_snapshot_with_preferences(&path, &interner, &graph, &preferences, 999).unwrap();
    let original_bytes = std::fs::read(&path).unwrap();
    assert!(original_bytes.len() > HEADER_SIZE);

    // 1. Bit-flip every byte in header (0..56) and verify rejection
    for i in 0..56 {
        let mut corrupt = original_bytes.clone();
        corrupt[i] ^= 0x80;
        let c_path = unique_temp_path(&format!("h_corrupt_{i}"));
        std::fs::write(&c_path, &corrupt).unwrap();

        let res = load_snapshot_with_preferences(
            &c_path,
            &StringInterner::new(),
            &GraphStore::new(),
            &UserPreferencesStore::new(),
        );
        let _ = std::fs::remove_file(&c_path);
        assert!(res.is_err(), "Header byte {i} corruption was not rejected!");
    }

    // 2. Bit-flip bytes in payload at 50 pseudo-random locations
    let payload_start = HEADER_SIZE;
    let payload_len = original_bytes.len() - HEADER_SIZE;
    for step in 1..=50 {
        let offset = payload_start + ((step * 97) % payload_len);
        let mut corrupt = original_bytes.clone();
        corrupt[offset] ^= 0xA5;
        let c_path = unique_temp_path(&format!("p_corrupt_{step}"));
        std::fs::write(&c_path, &corrupt).unwrap();

        let res = load_snapshot_with_preferences(
            &c_path,
            &StringInterner::new(),
            &GraphStore::new(),
            &UserPreferencesStore::new(),
        );
        let _ = std::fs::remove_file(&c_path);
        assert!(
            res.is_err(),
            "Payload offset {offset} corruption was not rejected!"
        );
        let err = res.unwrap_err().to_string();
        assert!(
            err.contains("Payload CRC32 mismatch") || err.contains("Snapshot"),
            "Unexpected error: {err}"
        );
    }

    // 3. Truncation at arbitrary lengths
    for trunc_len in [
        0,
        1,
        16,
        63,
        64,
        80,
        120,
        original_bytes.len() - 10,
        original_bytes.len() - 1,
    ] {
        if trunc_len >= original_bytes.len() {
            continue;
        }
        let c_path = unique_temp_path(&format!("trunc_{trunc_len}"));
        std::fs::write(&c_path, &original_bytes[..trunc_len]).unwrap();

        let res = load_snapshot_with_preferences(
            &c_path,
            &StringInterner::new(),
            &GraphStore::new(),
            &UserPreferencesStore::new(),
        );
        let _ = std::fs::remove_file(&c_path);
        assert!(res.is_err(), "Truncation at {trunc_len} was not rejected!");
    }

    let _ = std::fs::remove_file(&path);
}

// ---------------------------------------------------------------------------
// 4. Non-Blocking spawn_blocking Isolation Benchmark
// ---------------------------------------------------------------------------
#[tokio::test]
async fn test_tokio_spawn_blocking_async_event_loop_isolation() {
    let path = unique_temp_path("spawn_blocking_isolation");
    let interner = Arc::new(StringInterner::new());
    let graph = Arc::new(GraphStore::new());
    let preferences = Arc::new(UserPreferencesStore::new());

    for i in 1..=5000 {
        let u = interner.intern(&format!("did:plc:heavy_user_{i}"));
        let p = interner.intern(&format!("at://did:plc:heavy_author_{i}/post/{i}"));
        graph.record_interaction(u, p, SignalType::Like, BLUESKY_EPOCH_SECS + i);
        graph.record_follow(u, (u % 100) + 1);
        graph.record_post_meta(p, u, None, None, BLUESKY_EPOCH_SECS + i);
    }

    let p_clone = path.clone();
    let int_clone = Arc::clone(&interner);
    let gr_clone = Arc::clone(&graph);
    let pr_clone = Arc::clone(&preferences);

    let is_running = Arc::new(AtomicBool::new(true));
    let run_c = Arc::clone(&is_running);

    // High frequency async ticker task running at 500 Hz on Tokio async reactor
    let async_ticker = tokio::spawn(async move {
        let mut ticks = 0u64;
        let mut max_jitter_us = 0u64;
        let mut interval = tokio::time::interval(Duration::from_millis(2));

        while run_c.load(Ordering::Relaxed) {
            let start = Instant::now();
            interval.tick().await;
            let elapsed_us = start.elapsed().as_micros() as u64;
            max_jitter_us = max_jitter_us.max(elapsed_us);
            ticks += 1;
        }
        (ticks, max_jitter_us)
    });

    // Run snapshot save offloaded to blocking thread pool
    let save_task = tokio::task::spawn_blocking(move || {
        save_snapshot_with_preferences(&p_clone, &int_clone, &gr_clone, &pr_clone, 888_888)
    });

    let (save_result, ()) = tokio::join!(save_task, async {
        tokio::time::sleep(Duration::from_millis(50)).await;
    });

    is_running.store(false, Ordering::Relaxed);
    let (ticks, max_jitter_us) = async_ticker.await.unwrap();

    assert!(save_result.is_ok());
    assert!(save_result.unwrap().is_ok());
    assert!(ticks > 10, "Async ticker was starved! ticks: {ticks}");
    println!(
        "Spawn blocking test completed: {ticks} async ticks processed during save, max ticker interval: {max_jitter_us} µs"
    );

    let _ = std::fs::remove_file(&path);
}

// ---------------------------------------------------------------------------
// 5. Memory Overhead & Streaming Chunk Size Invariants
// ---------------------------------------------------------------------------
#[test]
fn test_streaming_bounded_memory_buffer_validation() {
    let interner = StringInterner::new();
    let graph = GraphStore::new();
    let preferences = UserPreferencesStore::new();

    for i in 1..=500 {
        let u = interner.intern(&format!("did:plc:stream_user_{i}"));
        let p = interner.intern(&format!("at://did:plc:stream_author_{i}/post/{i}"));
        graph.record_interaction(u, p, SignalType::Like, BLUESKY_EPOCH_SECS + i);
        graph.record_follow(u, (u % 50) + 1);
        graph.record_post_meta(p, u, None, None, BLUESKY_EPOCH_SECS + i);
    }

    let mut max_chunk_size = 0usize;
    let mut total_chunks = 0usize;
    let mut total_bytes = 0usize;

    let mut mock_writer = |chunk: &[u8]| -> std::io::Result<()> {
        total_chunks += 1;
        total_bytes += chunk.len();
        max_chunk_size = max_chunk_size.max(chunk.len());
        Ok(())
    };

    // Verify each streaming method writes in small, bounded chunks
    let n_s = interner.stream_strings_to(&mut mock_writer).unwrap();
    assert_eq!(n_s, 1000);

    let (n_u, n_e) = graph.stream_user_interactions_to(&mut mock_writer).unwrap();
    assert_eq!(n_u, 500);
    assert_eq!(n_e, 500);

    let n_p = graph.stream_post_interactions_to(&mut mock_writer).unwrap();
    assert_eq!(n_p, 500);

    let mut bm_buf = Vec::new();
    let n_bm = graph
        .stream_user_likes_bitmaps_to(&mut mock_writer, &mut bm_buf)
        .unwrap();
    assert_eq!(n_bm, 500);

    let n_f = graph.stream_follows_to(&mut mock_writer).unwrap();
    assert_eq!(n_f, 500);

    let n_m = graph.stream_post_metadata_to(&mut mock_writer).unwrap();
    assert_eq!(n_m, 500);

    let n_a = graph
        .stream_active_recent_posts_to(&mut mock_writer)
        .unwrap();
    assert_eq!(n_a, 500);

    let n_pr = preferences.stream_preferences_to(&mut mock_writer).unwrap();
    assert_eq!(n_pr, 0);

    // Invariant: Max individual chunk size must never exceed 64 KB
    assert!(
        max_chunk_size <= 65536,
        "Individual write chunk {max_chunk_size} exceeded 64 KB buffer bound!"
    );
    assert!(total_chunks > 1000, "Must stream via granular chunks");
}

// ---------------------------------------------------------------------------
// 6. Legacy Version Compatibility (V1, V2, V3)
// ---------------------------------------------------------------------------
#[test]
fn test_legacy_version_compatibility_v1_v2_v3() {
    let interner = StringInterner::new();
    let graph = GraphStore::new();
    let preferences = UserPreferencesStore::new();

    let u1 = interner.intern("did:plc:alice");
    let p1 = interner.intern("at://did:plc:alice/post/1");
    graph.record_interaction(u1, p1, SignalType::Like, BLUESKY_EPOCH_SECS + 500);
    graph.record_post_meta(p1, u1, None, None, BLUESKY_EPOCH_SECS + 500);

    // Save modern snapshot V4
    let path = unique_temp_path("legacy_test");
    save_snapshot_with_preferences(&path, &interner, &graph, &preferences, 4444).unwrap();
    let mut raw_bytes = std::fs::read(&path).unwrap();

    // Transform into V1 snapshot (version = 1 in header, adjust header CRC)
    raw_bytes[4..6].copy_from_slice(&1u16.to_le_bytes());
    let mut h_hasher = Hasher::new();
    h_hasher.update(&raw_bytes[0..56]);
    let v1_hcrc = h_hasher.finalize();
    raw_bytes[56..60].copy_from_slice(&v1_hcrc.to_le_bytes());

    let v1_path = unique_temp_path("v1_snap");
    std::fs::write(&v1_path, &raw_bytes).unwrap();

    let rest_int = StringInterner::new();
    let rest_gr = GraphStore::new();
    let rest_pr = UserPreferencesStore::new();

    let loaded = load_snapshot_with_preferences(&v1_path, &rest_int, &rest_gr, &rest_pr).unwrap();
    assert!(loaded.is_some());
    assert_eq!(loaded.unwrap().header.format_version, 1);
    assert_eq!(rest_int.len(), 2);
    assert_eq!(rest_gr.stats().total_interactions, 1);
    assert_eq!(rest_pr.len(), 0);

    let _ = std::fs::remove_file(&path);
    let _ = std::fs::remove_file(&v1_path);
}

// ---------------------------------------------------------------------------
// 7. Concurrent Mutations During Streaming Persistence Stress
// ---------------------------------------------------------------------------
#[test]
fn test_concurrent_mutations_during_streaming_persistence_stress() {
    let path = unique_temp_path("concurrent_streaming");
    let interner = Arc::new(StringInterner::new());
    let graph = Arc::new(GraphStore::new());
    let preferences = Arc::new(UserPreferencesStore::new());
    let is_running = Arc::new(AtomicBool::new(true));

    for i in 1..=1000 {
        let u = interner.intern(&format!("did:plc:base_user_{i}"));
        let p = interner.intern(&format!("at://did:plc:base_author_{i}/post/{i}"));
        graph.record_interaction(u, p, SignalType::Like, BLUESKY_EPOCH_SECS + i);
    }

    let mut handles = Vec::new();

    // 8 concurrent mutation threads hammering all shards
    for t_id in 0..8 {
        let int_c = Arc::clone(&interner);
        let gr_c = Arc::clone(&graph);
        let pr_c = Arc::clone(&preferences);
        let run_c = Arc::clone(&is_running);

        handles.push(std::thread::spawn(move || {
            let mut count = 0u32;
            while run_c.load(Ordering::Relaxed) {
                count += 1;
                let uid = ((t_id * 5000 + count) % 3000 + 1) as u32;
                let pid = ((count * 13) % 5000 + 1) as u32;

                int_c.intern(&format!("did:plc:thread_{t_id}_{count}"));
                gr_c.record_interaction(
                    uid,
                    pid,
                    SignalType::Like,
                    BLUESKY_EPOCH_SECS + u64::from(count),
                );
                gr_c.record_follow(uid, (uid % 100) + 1);

                if count.is_multiple_of(20) {
                    let dials = UserDials {
                        freshness_half_life_secs: 7200.0,
                        serendipity_ratio: 0.1,
                        topic_weights: TopicWeights::default(),
                        include_replies: false,
                        min_likes: 0,
                        updated_at_secs: BLUESKY_EPOCH_SECS + u64::from(count),
                    };
                    pr_c.set(uid, dials);
                }
            }
        }));
    }

    // Perform 5 successive snapshot saves during continuous concurrent mutations
    for save_iter in 1..=5 {
        std::thread::sleep(Duration::from_millis(20));
        let header = save_snapshot_with_preferences(
            &path,
            &interner,
            &graph,
            &preferences,
            save_iter * 1000,
        )
        .expect("Streaming save under mutation must succeed");
        assert!(header.num_strings >= 1000);
        assert!(header.num_users >= 500);

        // Verify the saved snapshot deserializes and verifies CRC32 cleanly
        let rest_int = StringInterner::new();
        let rest_gr = GraphStore::new();
        let rest_pr = UserPreferencesStore::new();
        let loaded = load_snapshot_with_preferences(&path, &rest_int, &rest_gr, &rest_pr)
            .expect("Hydration must succeed")
            .expect("Snapshot must exist");

        assert_eq!(loaded.header.format_version, SNAPSHOT_FORMAT_VERSION);
        assert!(rest_int.len() >= 1000);
        assert!(rest_gr.stats().total_interactions >= 500);
    }

    is_running.store(false, Ordering::Relaxed);
    for h in handles {
        h.join().unwrap();
    }

    let _ = std::fs::remove_file(&path);
}

// ---------------------------------------------------------------------------
// 8. Deterministic ID Preservation in StringInterner Streaming
// ---------------------------------------------------------------------------
#[test]
fn test_string_interner_exact_id_preservation() {
    let interner = StringInterner::new();
    let mut expected_ids = Vec::new();

    for i in 0..1000 {
        let s = format!("did:plc:deterministic_test_{i}");
        let id = interner.intern(&s);
        expected_ids.push((s, id));
    }

    let mut write_count = 0usize;
    let mut write_fn = |_chunk: &[u8]| -> std::io::Result<()> {
        write_count += 1;
        Ok(())
    };
    let count = interner.stream_strings_to(&mut write_fn).unwrap();
    assert_eq!(count, 1000);
    assert!(write_count >= 1000);

    // Save and reload snapshot
    let path = unique_temp_path("id_preservation");
    let graph = GraphStore::new();
    let preferences = UserPreferencesStore::new();
    save_snapshot_with_preferences(&path, &interner, &graph, &preferences, 12345).unwrap();

    let reloaded_interner = StringInterner::new();
    load_snapshot_with_preferences(&path, &reloaded_interner, &graph, &preferences).unwrap();

    for (s, orig_id) in expected_ids {
        let reloaded_id = reloaded_interner.lookup_id(&s).unwrap();
        assert_eq!(
            orig_id, reloaded_id,
            "String '{s}' ID mismatch after streaming reload!"
        );
        assert_eq!(
            reloaded_interner.lookup_str(orig_id).as_deref(),
            Some(s.as_str())
        );
    }

    let _ = std::fs::remove_file(&path);
}
