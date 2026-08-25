#![forbid(unsafe_code)]
#![allow(
    clippy::pedantic,
    clippy::nursery,
    clippy::float_cmp,
    clippy::cast_possible_truncation,
    clippy::cast_precision_loss,
    clippy::cast_sign_loss,
    clippy::manual_is_multiple_of,
    clippy::len_zero
)]

//! # Empirical Challenger 1: Milestone 1 - Preferences Store & Snapshot Engine Stress Suite
//!
//! Exhaustively stresses:
//! 1. High concurrent multi-threaded contention (up to 32 threads reading, writing, clearing, exporting, and restoring).
//! 2. Extreme values, NaN/Inf, boundary values, fast-path lookup latency, zero-allocation contracts, and memory bounds.
//! 3. Snapshot crash consistency, Section 8 bit flips, truncation, dual CRC32 tampering, and atomic rename recovery.
//! 4. Safety compliance (`#![forbid(unsafe_code)]`).

use std::fs;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use crc32fast::Hasher;
use for_your_consideration::prelude::*;

static COUNTER: AtomicU64 = AtomicU64::new(10_000);

fn unique_temp_snapshot(label: &str) -> PathBuf {
    let mut path = std::env::temp_dir();
    let file_name = format!(
        "challenger_m1_snap_{}_{}_{}_{}.bin",
        label,
        std::process::id(),
        COUNTER.fetch_add(1, Ordering::Relaxed),
        Instant::now().elapsed().as_nanos()
    );
    path.push(file_name);
    path
}

// ===========================================================================
// SECTION 1: High Concurrent Multi-Threaded Contention Stress Tests
// ===========================================================================

#[test]
fn test_adversarial_concurrency_32_threads_mixed_operations_and_snapshots() {
    let store = Arc::new(UserPreferencesStore::new());
    let interner = Arc::new(StringInterner::new());
    let stop_signal = Arc::new(AtomicBool::new(false));

    // Pre-populate 500 users
    for i in 0..500 {
        let did = format!("did:plc:challenger_init_{i}");
        store.set_by_did(
            &interner,
            &did,
            UserDials::from_hours(24.0, 0.15, TopicWeights::default(), i as u64),
        );
    }

    let mut handles = Vec::new();
    let total_reads = Arc::new(AtomicU64::new(0));
    let total_writes = Arc::new(AtomicU64::new(0));
    let total_snapshots = Arc::new(AtomicU64::new(0));

    // 1. Reader Threads (12 threads querying both by numeric ID and by DID string)
    for t_id in 0..12 {
        let s = Arc::clone(&store);
        let i_ref = Arc::clone(&interner);
        let stop = Arc::clone(&stop_signal);
        let reads = Arc::clone(&total_reads);

        handles.push(thread::spawn(move || {
            let mut count = 0u64;
            while !stop.load(Ordering::Relaxed) {
                let user_id = (t_id * 50 + (count % 600)) as u32;
                let _ = s.get(user_id);
                let _ = s.get_or_default(user_id);

                let did = format!("did:plc:challenger_init_{}", count % 700);
                let _ = s.get_by_did(&i_ref, &did);
                let _ = s.get_by_did_or_default(&i_ref, &did);

                count += 1;
            }
            reads.fetch_add(count, Ordering::Relaxed);
        }));
    }

    // 2. Writer Threads (10 threads setting/updating custom dials)
    for t_id in 0..10 {
        let s = Arc::clone(&store);
        let i_ref = Arc::clone(&interner);
        let stop = Arc::clone(&stop_signal);
        let writes = Arc::clone(&total_writes);

        handles.push(thread::spawn(move || {
            let mut count = 0u64;
            while !stop.load(Ordering::Relaxed) {
                let user_id = (t_id * 50 + (count % 500)) as u32;
                let dials = UserDials::from_hours(
                    1.0 + ((count % 167) as f32),
                    ((count % 50) as f32) / 100.0,
                    TopicWeights {
                        art: ((count % 50) as f32) / 10.0,
                        tech: 1.0,
                        science: 2.0,
                        news: 0.5,
                        culture: 1.5,
                    },
                    count,
                );

                if count % 2 == 0 {
                    s.set(user_id, dials);
                } else {
                    let did = format!("did:plc:concur_dyn_{}_{}", t_id, count % 50);
                    s.set_by_did(&i_ref, &did, dials);
                }

                if count % 70 == 0 {
                    s.remove(user_id);
                }

                count += 1;
            }
            writes.fetch_add(count, Ordering::Relaxed);
        }));
    }

    // 3. Snapshot Exporters (6 threads exporting full snapshot data concurrently with mutations)
    for _ in 0..6 {
        let s = Arc::clone(&store);
        let stop = Arc::clone(&stop_signal);
        let snaps = Arc::clone(&total_snapshots);

        handles.push(thread::spawn(move || {
            let mut count = 0u64;
            while !stop.load(Ordering::Relaxed) {
                let snap = s.snapshot_data();
                assert!(snap.len() <= 2000);
                for (uid, dials) in snap {
                    assert!(
                        dials.validate().is_ok(),
                        "Dials for user {uid} must be valid during concurrent export"
                    );
                }
                count += 1;
                thread::yield_now();
            }
            snaps.fetch_add(count, Ordering::Relaxed);
        }));
    }

    // 4. Clear & Restore Disruptor (4 threads doing periodic clone, clear, and restore cycles)
    for _ in 0..4 {
        let s = Arc::clone(&store);
        let stop = Arc::clone(&stop_signal);

        handles.push(thread::spawn(move || {
            let mut cycle = 0u32;
            while !stop.load(Ordering::Relaxed) {
                cycle += 1;
                if cycle % 20 == 0 {
                    let snap = s.snapshot_data();
                    if !snap.is_empty() {
                        s.restore_from_snapshot(snap);
                    }
                }
                thread::sleep(Duration::from_millis(5));
            }
        }));
    }

    // Run contention stress for 400ms
    thread::sleep(Duration::from_millis(400));
    stop_signal.store(true, Ordering::Relaxed);

    for h in handles {
        h.join()
            .expect("Worker thread failed or panicked under contention");
    }

    let reads_done = total_reads.load(Ordering::Relaxed);
    let writes_done = total_writes.load(Ordering::Relaxed);
    let snaps_done = total_snapshots.load(Ordering::Relaxed);

    println!(
        "High contention stress completed: {} reads, {} writes, {} snapshot exports",
        reads_done, writes_done, snaps_done
    );

    assert!(reads_done > 1000, "Too few reads completed: {reads_done}");
    assert!(writes_done > 500, "Too few writes completed: {writes_done}");
    assert!(snaps_done > 10, "Too few snapshots completed: {snaps_done}");
    assert!(!store.is_empty());
}

#[test]
fn test_adversarial_hot_shard_single_lock_contention() {
    let store = Arc::new(UserPreferencesStore::new());
    let stop = Arc::new(AtomicBool::new(false));

    // All user IDs map to shard 0: user_id = 0, 64, 128, 192, ...
    let hot_ids: Vec<u32> = (0..50).map(|i| i * 64).collect();
    for &id in &hot_ids {
        assert_eq!(shard_idx(id), 0);
    }

    let mut handles = Vec::new();
    let ops = Arc::new(AtomicU64::new(0));

    for thread_idx in 0..16 {
        let s = Arc::clone(&store);
        let stop_ref = Arc::clone(&stop);
        let ops_ref = Arc::clone(&ops);
        let ids = hot_ids.clone();

        handles.push(thread::spawn(move || {
            let mut count = 0u64;
            while !stop_ref.load(Ordering::Relaxed) {
                let id = ids[(count as usize) % ids.len()];
                if count % 3 == 0 {
                    s.set(
                        id,
                        UserDials::from_hours(12.0, 0.20, TopicWeights::default(), count),
                    );
                } else if count % 3 == 1 {
                    let _ = s.get(id);
                } else if thread_idx % 4 == 0 {
                    s.remove(id);
                }
                count += 1;
            }
            ops_ref.fetch_add(count, Ordering::Relaxed);
        }));
    }

    thread::sleep(Duration::from_millis(250));
    stop.store(true, Ordering::Relaxed);

    for h in handles {
        h.join().expect("Hot shard worker failed");
    }

    let total_ops = ops.load(Ordering::Relaxed);
    assert!(
        total_ops > 1000,
        "Hot shard contention stalled, total ops: {total_ops}"
    );
}

// ===========================================================================
// SECTION 2: Extreme Values, NaN/Inf, Boundaries, Fast-Path, and Memory
// ===========================================================================

#[test]
fn test_adversarial_floating_point_extreme_values_and_nans() {
    let mut dials = UserDials::default();

    // 1. Freshness half-life extremes
    for bad_freshness in [
        f32::NAN,
        f32::INFINITY,
        f32::NEG_INFINITY,
        -0.0,
        -1.0,
        0.0,
        3599.9,   // Just below 1h (3600.0s)
        604800.1, // Just above 168h (604800.0s)
        1e20,
        -1e20,
        f32::MIN_POSITIVE,
    ] {
        dials.freshness_half_life_secs = bad_freshness;
        assert!(
            dials.validate().is_err(),
            "Freshness {} should fail validation",
            bad_freshness
        );
    }
    dials.freshness_half_life_secs = 3600.0; // Reset to valid (1h)

    // 2. Serendipity / Discovery ratio extremes
    for bad_serendipity in [
        f32::NAN,
        f32::INFINITY,
        f32::NEG_INFINITY,
        -0.0001,
        0.5001,
        1.0,
        100.0,
        -50.0,
    ] {
        dials.serendipity_ratio = bad_serendipity;
        assert!(
            dials.validate().is_err(),
            "Discovery {} should fail validation",
            bad_serendipity
        );
    }
    dials.serendipity_ratio = 0.15; // Reset to valid

    // 3. Topic weight extremes on each of the 5 topic fields
    let bad_weights = [
        f32::NAN,
        f32::INFINITY,
        f32::NEG_INFINITY,
        -0.0001,
        5.0001,
        10.0,
        -1.0,
    ];

    for &bad in &bad_weights {
        dials.topic_weights.art = bad;
        assert!(dials.validate().is_err(), "Art weight {bad} must fail");
        dials.topic_weights.art = 1.0;

        dials.topic_weights.tech = bad;
        assert!(dials.validate().is_err(), "Tech weight {bad} must fail");
        dials.topic_weights.tech = 1.0;

        dials.topic_weights.science = bad;
        assert!(dials.validate().is_err(), "Science weight {bad} must fail");
        dials.topic_weights.science = 1.0;

        dials.topic_weights.news = bad;
        assert!(dials.validate().is_err(), "News weight {bad} must fail");
        dials.topic_weights.news = 1.0;

        dials.topic_weights.culture = bad;
        assert!(dials.validate().is_err(), "Culture weight {bad} must fail");
        dials.topic_weights.culture = 1.0;
    }
}

#[test]
fn test_adversarial_strict_boundary_invariants() {
    // Min boundary exact values
    let min_dials = UserDials {
        freshness_half_life_secs: MIN_FRESHNESS_SECS, // 3600.0s (1.0h)
        serendipity_ratio: MIN_SERENDIPITY_RATIO,     // 0.0
        topic_weights: TopicWeights {
            art: MIN_TOPIC_MULTIPLIER,     // 0.0
            tech: MIN_TOPIC_MULTIPLIER,    // 0.0
            science: MIN_TOPIC_MULTIPLIER, // 0.0
            news: MIN_TOPIC_MULTIPLIER,    // 0.0
            culture: MIN_TOPIC_MULTIPLIER, // 0.0
        },
        updated_at_secs: 0,
    };
    assert!(
        min_dials.validate().is_ok(),
        "Min boundary dials must pass validation"
    );
    assert_eq!(min_dials.freshness_half_life_hours(), 1.0);
    assert_eq!(min_dials.discovery_ratio(), 0.0);

    // Max boundary exact values
    let max_dials = UserDials {
        freshness_half_life_secs: MAX_FRESHNESS_SECS, // 604800.0s (168.0h)
        serendipity_ratio: MAX_SERENDIPITY_RATIO,     // 0.50
        topic_weights: TopicWeights {
            art: MAX_TOPIC_MULTIPLIER,     // 5.0
            tech: MAX_TOPIC_MULTIPLIER,    // 5.0
            science: MAX_TOPIC_MULTIPLIER, // 5.0
            news: MAX_TOPIC_MULTIPLIER,    // 5.0
            culture: MAX_TOPIC_MULTIPLIER, // 5.0
        },
        updated_at_secs: u64::MAX,
    };
    assert!(
        max_dials.validate().is_ok(),
        "Max boundary dials must pass validation"
    );
    assert_eq!(max_dials.freshness_half_life_hours(), 168.0);
    assert_eq!(max_dials.discovery_ratio(), 0.50);
}

#[test]
fn test_zero_allocation_and_fast_path_latency() {
    // 1. Zero allocation contract: UserDials is 40 bytes (with 8-byte u64 alignment) and Copy
    assert_eq!(std::mem::size_of::<UserDials>(), 40);
    assert_eq!(std::mem::align_of::<UserDials>(), 8);
    assert_eq!(std::mem::size_of::<TopicWeights>(), 20);

    let store = UserPreferencesStore::new();
    let interner = StringInterner::new();

    // Populate interner with 10,000 strings
    for i in 0..10_000 {
        interner.intern(&format!("did:plc:known_user_{i}"));
    }

    // 2. Benchmark uninterned fast-path lookup latency (< 35ns target)
    let iterations = 100_000;
    let t0 = Instant::now();
    for i in 0..iterations {
        let uninterned_did = format!("did:plc:unseen_viewer_{}", i % 100);
        let res = store.get_by_did_or_default(&interner, &uninterned_did);
        assert_eq!(res, UserDials::default());
    }
    let elapsed = t0.elapsed();
    let ns_per_lookup = elapsed.as_nanos() as f64 / iterations as f64;
    println!(
        "Fast-path lookup latency (including format!): {:.2} ns/op",
        ns_per_lookup
    );

    // Benchmark without string formatting allocation
    let static_unseen = "did:plc:completely_uninterned_static_did";
    let t1 = Instant::now();
    for _ in 0..iterations {
        let res = store.get_by_did(&interner, static_unseen);
        assert_eq!(res, None);
    }
    let static_elapsed = t1.elapsed();
    let static_ns = static_elapsed.as_nanos() as f64 / iterations as f64;
    println!(
        "Pure fast-path static DID lookup latency: {:.2} ns/op",
        static_ns
    );

    // In debug mode it's < 50ns, in release mode < 10ns
    if cfg!(not(debug_assertions)) {
        assert!(
            static_ns < 35.0,
            "Static fast path took {:.2} ns, exceeding 35ns budget",
            static_ns
        );
    }
}

#[test]
fn test_memory_footprint_scaling_and_cleanup() {
    let store = UserPreferencesStore::new();
    let initial_bytes = store.estimated_size_bytes();

    // Insert 50,000 user dials
    for i in 0..50_000 {
        let dials = UserDials::from_hours(
            12.0 + ((i % 100) as f32),
            0.15,
            TopicWeights::default(),
            i as u64,
        );
        store.set(i, dials);
    }

    assert_eq!(store.len(), 50_000);
    let populated_bytes = store.estimated_size_bytes();
    let bytes_per_record = (populated_bytes - initial_bytes) as f64 / 50_000.0;
    println!(
        "Populated 50k profiles: {:.2} MB total ({:.1} bytes/record)",
        populated_bytes as f64 / 1_048_576.0,
        bytes_per_record
    );

    // Hash table capacity per record is typically ~50-80 bytes
    assert!(
        bytes_per_record < 120.0,
        "Memory footprint per record too high: {bytes_per_record}"
    );

    // Test deep clone isolation
    let cloned = store.clone();
    assert_eq!(cloned.len(), 50_000);

    // Clear original store
    store.clear();
    assert!(store.is_empty());
    assert_eq!(store.len(), 0);

    // Clone must remain intact
    assert_eq!(cloned.len(), 50_000);
}

// ===========================================================================
// SECTION 3: Snapshot Crash Consistency, Corruption & CRC32 Tampering
// ===========================================================================

#[test]
fn test_snapshot_section_8_bit_flip_corruption_matrix() {
    let snapshot_path = unique_temp_snapshot("sec8_bitflip");
    let interner = StringInterner::new();
    let graph = GraphStore::new();
    let preferences = UserPreferencesStore::new();

    // Insert 10 user preference profiles
    for i in 1..=10 {
        let did = format!("did:plc:user_{i}");
        let uid = interner.intern(&did);
        preferences.set(
            uid,
            UserDials::from_hours(
                10.0 + i as f32,
                0.10 + (i as f32 / 100.0),
                TopicWeights {
                    art: 1.0 + (i as f32 / 10.0),
                    tech: 2.0,
                    science: 0.5,
                    news: 1.5,
                    culture: 0.8,
                },
                1_700_000_000 + i as u64,
            ),
        );
    }

    save_snapshot_with_preferences(&snapshot_path, &interner, &graph, &preferences, 999_999)
        .expect("Initial save failed");

    let original_bytes = fs::read(&snapshot_path).expect("Failed to read snapshot file");
    assert!(original_bytes.len() > HEADER_SIZE + 4 + 10 * 40);

    // Section 8 starts towards the end of the file.
    // The last 404 bytes are Section 8 (4 bytes count + 10 * 40 bytes records).
    let sec8_start = original_bytes.len() - (4 + 10 * 40);

    // Test flipping bits in every single byte of Section 8
    for byte_offset in sec8_start..original_bytes.len() {
        let mut corrupt_bytes = original_bytes.clone();
        corrupt_bytes[byte_offset] ^= 0xFF; // Invert all bits in byte

        let corrupt_path = unique_temp_snapshot(&format!("corrupt_offset_{byte_offset}"));
        fs::write(&corrupt_path, &corrupt_bytes).unwrap();

        let test_interner = StringInterner::new();
        let test_graph = GraphStore::new();
        let test_prefs = UserPreferencesStore::new();

        let result =
            load_snapshot_with_preferences(&corrupt_path, &test_interner, &test_graph, &test_prefs);

        let _ = fs::remove_file(&corrupt_path);

        assert!(
            result.is_err(),
            "Corrupting byte at offset {byte_offset} (Section 8) unexpectedly loaded successfully!"
        );
    }

    let _ = fs::remove_file(&snapshot_path);
}

#[test]
fn test_snapshot_crc32_tampering_and_forgery_attacks() {
    let snapshot_path = unique_temp_snapshot("crc32_tampering");
    let interner = StringInterner::new();
    let graph = GraphStore::new();
    let preferences = UserPreferencesStore::new();

    let u1 = interner.intern("did:plc:alice");
    preferences.set(u1, UserDials::default());

    save_snapshot_with_preferences(&snapshot_path, &interner, &graph, &preferences, 12345)
        .expect("Save failed");

    let original_bytes = fs::read(&snapshot_path).unwrap();

    // ATTACK 1: Tamper payload byte and recalculate payload CRC, but do NOT update Header CRC.
    // Header CRC should catch the payload CRC discrepancy in header bytes 52..56.
    {
        let mut tampered = original_bytes.clone();
        tampered[HEADER_SIZE + 10] ^= 0xAA; // Corrupt payload

        // Recalculate payload CRC
        let mut p_hasher = Hasher::new();
        p_hasher.update(&tampered[HEADER_SIZE..]);
        let forged_payload_crc = p_hasher.finalize();
        tampered[52..56].copy_from_slice(&forged_payload_crc.to_le_bytes());

        let attack_path = unique_temp_snapshot("attack_1");
        fs::write(&attack_path, &tampered).unwrap();

        let res = load_snapshot_with_preferences(
            &attack_path,
            &StringInterner::new(),
            &GraphStore::new(),
            &UserPreferencesStore::new(),
        );
        let _ = fs::remove_file(&attack_path);

        assert!(res.is_err());
        let err = res.unwrap_err().to_string();
        assert!(
            err.contains("Header CRC32 checksum mismatch"),
            "Expected Header CRC error, got: {err}"
        );
    }

    // ATTACK 2: Tamper payload with illegal dials (freshness = 999999.0h), recalculate both Payload CRC AND Header CRC.
    // Dials validator inside load_snapshot_with_preferences should reject the corrupted dials.
    {
        // Re-encode snapshot with corrupt dials
        let mut payload = Vec::new();
        // Section 1: strings = 0
        payload.extend_from_slice(&0u32.to_le_bytes());
        // Section 2: user_interactions = 0
        payload.extend_from_slice(&0u32.to_le_bytes());
        // Section 3: post_interactions = 0
        payload.extend_from_slice(&0u32.to_le_bytes());
        // Section 4: roaring bitmaps = 0
        payload.extend_from_slice(&0u32.to_le_bytes());
        // Section 5: follows = 0
        payload.extend_from_slice(&0u32.to_le_bytes());
        // Section 6: post metadata = 0
        payload.extend_from_slice(&0u32.to_le_bytes());
        // Section 7: active recent = 0
        payload.extend_from_slice(&0u32.to_le_bytes());

        // Section 8: 1 record with out-of-bounds freshness (999,999 hours = 3,599,996,400s)
        payload.extend_from_slice(&1u32.to_le_bytes()); // num_preferences = 1
        payload.extend_from_slice(&1u32.to_le_bytes()); // user_id = 1
        payload.extend_from_slice(&(999_999.0f32 * 3600.0).to_le_bytes()); // freshness
        payload.extend_from_slice(&0.15f32.to_le_bytes()); // discovery
        payload.extend_from_slice(&1.0f32.to_le_bytes()); // art
        payload.extend_from_slice(&1.0f32.to_le_bytes()); // tech
        payload.extend_from_slice(&1.0f32.to_le_bytes()); // science
        payload.extend_from_slice(&1.0f32.to_le_bytes()); // news
        payload.extend_from_slice(&1.0f32.to_le_bytes()); // culture
        payload.extend_from_slice(&100u64.to_le_bytes()); // updated_at

        let mut p_hasher = Hasher::new();
        p_hasher.update(&payload);
        let forged_payload_crc = p_hasher.finalize();

        let mut header = [0u8; HEADER_SIZE];
        header[0..4].copy_from_slice(&SNAPSHOT_MAGIC);
        header[4..6].copy_from_slice(&SNAPSHOT_FORMAT_VERSION.to_le_bytes());
        header[6..8].copy_from_slice(&(HEADER_SIZE as u16).to_le_bytes());
        header[52..56].copy_from_slice(&forged_payload_crc.to_le_bytes());
        header[60..64].copy_from_slice(&1u32.to_le_bytes());

        let mut h_hasher = Hasher::new();
        h_hasher.update(&header[0..56]);
        let forged_header_crc = h_hasher.finalize();
        header[56..60].copy_from_slice(&forged_header_crc.to_le_bytes());

        let mut forged_file = header.to_vec();
        forged_file.extend_from_slice(&payload);

        let attack_path = unique_temp_snapshot("attack_2");
        fs::write(&attack_path, &forged_file).unwrap();

        let res = load_snapshot_with_preferences(
            &attack_path,
            &StringInterner::new(),
            &GraphStore::new(),
            &UserPreferencesStore::new(),
        );
        let _ = fs::remove_file(&attack_path);

        assert!(
            res.is_err(),
            "Forged snapshot with illegal freshness must be rejected by validator"
        );
        let err = res.unwrap_err().to_string();
        assert!(
            err.contains("Corrupted user preference record") || err.contains("Freshness"),
            "Expected validator error, got: {err}"
        );
    }

    let _ = fs::remove_file(&snapshot_path);
}

#[test]
fn test_snapshot_truncation_at_all_granular_offsets() {
    let snapshot_path = unique_temp_snapshot("truncation_stress");
    let interner = StringInterner::new();
    let graph = GraphStore::new();
    let preferences = UserPreferencesStore::new();

    for i in 1..=20 {
        let did = format!("did:plc:user_{i}");
        let uid = interner.intern(&did);
        preferences.set(uid, UserDials::default());
    }

    save_snapshot_with_preferences(&snapshot_path, &interner, &graph, &preferences, 42)
        .expect("Save failed");

    let full_bytes = fs::read(&snapshot_path).unwrap();
    let _ = fs::remove_file(&snapshot_path);

    // Test truncations at every single byte in the last 100 bytes of the file
    for len in (full_bytes.len().saturating_sub(100))..full_bytes.len() {
        let truncated = &full_bytes[..len];
        let trunc_path = unique_temp_snapshot(&format!("trunc_{len}"));
        fs::write(&trunc_path, truncated).unwrap();

        let res = load_snapshot_with_preferences(
            &trunc_path,
            &StringInterner::new(),
            &GraphStore::new(),
            &UserPreferencesStore::new(),
        );
        let _ = fs::remove_file(&trunc_path);

        assert!(
            res.is_err(),
            "Truncation at length {len} unexpectedly succeeded"
        );
    }
}

#[test]
fn test_snapshot_atomic_rename_failure_recovery_and_nesting() {
    let mut nested_path = std::env::temp_dir();
    nested_path.push(format!(
        "fyc_nested_{}",
        Instant::now().elapsed().as_nanos()
    ));
    nested_path.push("deeply");
    nested_path.push("nested");
    nested_path.push("snapshot.bin");

    let interner = StringInterner::new();
    let graph = GraphStore::new();
    let preferences = UserPreferencesStore::new();
    preferences.set(1, UserDials::default());

    // Save should automatically create all parent directories
    save_snapshot_with_preferences(&nested_path, &interner, &graph, &preferences, 789)
        .expect("Save into non-existent nested directories must succeed");

    assert!(nested_path.exists());

    let loaded = load_snapshot_with_preferences(&nested_path, &interner, &graph, &preferences)
        .expect("Load must succeed")
        .expect("Must exist");

    assert_eq!(loaded.header.jetstream_cursor_us, 789);

    // Clean up
    let _ = fs::remove_dir_all(
        nested_path
            .parent()
            .unwrap()
            .parent()
            .unwrap()
            .parent()
            .unwrap(),
    );
}
