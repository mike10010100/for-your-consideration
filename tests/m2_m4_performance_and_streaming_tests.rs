#![forbid(unsafe_code)]
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic, missing_docs)]

use std::sync::Arc;

use for_your_consideration::prelude::*;

#[test]
fn test_velocity_pool_ttl_cache_hit_under_continuous_mutations() {
    let graph = GraphStore::new();
    let base_time = BLUESKY_EPOCH_SECS + 100_000;

    // Create 10 posts with interaction signals
    for i in 1..=10 {
        let post_id = i;
        let author_id = 100 + i;
        graph.record_post_meta(post_id, author_id, None, None, base_time);
        for u in 1..=i {
            graph.record_interaction(
                u,
                post_id,
                SignalType::Like,
                base_time + (u64::from(u) * 10),
            );
        }
    }

    // Query 1 at base_time: should compute candidates and cache them
    let candidates_t0 = graph.get_velocity_pool_candidates_at(base_time + 100, 10);
    assert!(!candidates_t0.is_empty());
    assert_eq!(candidates_t0[0], 10); // Highest interaction count

    // Simulate 500 firehose mutations at base_time + 2 seconds
    for m in 100..600 {
        graph.record_interaction(m, 1, SignalType::Like, base_time + 102);
    }

    // Query 2 at base_time + 5 seconds (elapsed = 5s < VELOCITY_CACHE_TTL_SECS = 10s):
    // Should hit cache despite 500 mutations!
    let candidates_t5 = graph.get_velocity_pool_candidates_at(base_time + 105, 10);
    assert_eq!(candidates_t0, candidates_t5);

    // Limit slicing check on cache hit
    let top_3 = graph.get_velocity_pool_candidates_at(base_time + 105, 3);
    assert_eq!(top_3, &candidates_t0[..3]);
}

#[test]
fn test_velocity_pool_ttl_cache_expiry_after_10s() {
    let graph = GraphStore::new();
    let base_time = BLUESKY_EPOCH_SECS + 200_000;

    for i in 1..=5 {
        graph.record_post_meta(i, 50, None, None, base_time);
        graph.record_interaction(1, i, SignalType::Like, base_time);
    }

    // Initial evaluation at t = 100
    let c1 = graph.get_velocity_pool_candidates_at(base_time + 100, 5);
    assert_eq!(c1.len(), 5);

    // Add high-velocity post 99 with massive interactions at t = 105
    graph.record_post_meta(99, 50, None, None, base_time + 105);
    for u in 10..30 {
        graph.record_interaction(u, 99, SignalType::Repost, base_time + 105);
    }

    // Query at t = 108 (elapsed 8s < 10s TTL): cache hit, post 99 not in c1
    let c_cached = graph.get_velocity_pool_candidates_at(base_time + 108, 5);
    assert_eq!(c_cached, c1);
    assert!(!c_cached.contains(&99));

    // Query at t = 112 (elapsed 12s > 10s TTL): cache expired, recomputes, post 99 is top candidate!
    let c_fresh = graph.get_velocity_pool_candidates_at(base_time + 112, 5);
    assert!(c_fresh.contains(&99));
    assert_eq!(c_fresh[0], 99);
}

#[test]
fn test_velocity_pool_ttl_cache_clock_warp_safety() {
    let graph = GraphStore::new();
    let base_time = BLUESKY_EPOCH_SECS + 300_000;

    graph.record_post_meta(10, 1, None, None, base_time);
    graph.record_interaction(1, 10, SignalType::Like, base_time);

    // Initial query at t = 5000
    let res1 = graph.get_velocity_pool_candidates_at(base_time + 5000, 10);
    assert_eq!(res1, vec![10]);

    // Clock-warp: system clock jumps backward to t = 2000 (current_time < evaluated_at)
    // Fast path must detect current_time < evaluated_at and safely bypass cache without underflow panic
    let res_warp = graph.get_velocity_pool_candidates_at(base_time + 2000, 10);
    assert_eq!(res_warp, vec![10]);

    // Limit 0 boundary case
    let res_zero = graph.get_velocity_pool_candidates_at(base_time + 2000, 0);
    assert!(res_zero.is_empty());
}

#[test]
fn test_velocity_pool_cache_invalidation_discipline() {
    let graph = GraphStore::new();
    let base_time = BLUESKY_EPOCH_SECS + 400_000;

    graph.record_post_meta(1, 1, None, None, base_time);
    graph.record_interaction(1, 1, SignalType::Like, base_time);

    // Fill cache
    let _ = graph.get_velocity_pool_candidates_at(base_time + 10, 10);

    // Invalidate via clear()
    graph.clear();
    assert_eq!(
        graph.get_velocity_pool_candidates_at(base_time + 10, 10),
        Vec::<u32>::new()
    );

    // Populate again
    graph.record_post_meta(2, 1, None, None, base_time);
    graph.record_interaction(1, 2, SignalType::Like, base_time);
    let _ = graph.get_velocity_pool_candidates_at(base_time + 10, 10);

    // Invalidate via prune_older_than()
    graph.prune_older_than(base_time + 50);
    assert_eq!(
        graph.get_velocity_pool_candidates_at(base_time + 10, 10),
        Vec::<u32>::new()
    );

    // Invalidate via restore_from_snapshot()
    let mut snap = GraphSnapshotData::default();
    snap.post_metadata.push((
        3,
        PostMeta {
            author_id: 1,
            root_id: None,
            parent_id: None,
            created_at: base_time + 60,
        },
    ));
    snap.active_recent_posts.push((3, base_time + 60));
    snap.post_interactions.push((
        3,
        vec![CompactEdge::new(1, SignalType::Like, base_time + 60)],
    ));
    graph.restore_from_snapshot(snap);

    let res_after_restore = graph.get_velocity_pool_candidates_at(base_time + 65, 10);
    assert_eq!(res_after_restore, vec![3]);
}

#[test]
fn test_streaming_snapshot_shard_by_shard_methods() {
    let interner = StringInterner::new();
    let graph = GraphStore::new();
    let preferences = UserPreferencesStore::new();

    let u_alice = interner.intern("did:plc:alice");
    let u_bob = interner.intern("did:plc:bob");
    let p_post1 = interner.intern("at://did:plc:alice/app.bsky.feed.post/1");
    let p_post2 = interner.intern("at://did:plc:bob/app.bsky.feed.post/2");

    let now = BLUESKY_EPOCH_SECS + 500_000;

    graph.record_interaction(u_alice, p_post1, SignalType::Like, now);
    graph.record_interaction(u_alice, p_post2, SignalType::Repost, now + 10);
    graph.record_interaction(u_bob, p_post1, SignalType::Quote, now + 20);
    graph.record_follow(u_alice, u_bob);
    graph.record_post_meta(p_post1, u_alice, None, None, now);
    graph.record_post_meta(p_post2, u_bob, Some(p_post1), Some(p_post1), now + 10);

    let dials = UserDials {
        freshness_half_life_secs: 14400.0,
        serendipity_ratio: 0.2,
        topic_weights: TopicWeights::default(),
        include_replies: false,
        min_likes: 2,
        updated_at_secs: now,
    };
    preferences.set(u_alice, dials);

    // Test counting methods
    assert_eq!(graph.count_non_empty_users(), 2);
    assert_eq!(graph.count_non_empty_posts(), 2);
    assert_eq!(graph.count_non_empty_bitmaps(), 2);
    assert_eq!(graph.count_non_empty_follows(), 1);
    assert_eq!(graph.count_post_metadata(), 2);
    assert_eq!(graph.count_active_recent_posts(), 2);
    assert_eq!(preferences.len(), 1);

    // Test streaming methods into buffer
    let mut string_buf = Vec::new();
    let mut write_str = |data: &[u8]| -> std::io::Result<()> {
        string_buf.extend_from_slice(data);
        Ok(())
    };
    let num_strs = interner.stream_strings_to(&mut write_str).unwrap();
    assert_eq!(num_strs, 4);
    assert!(!string_buf.is_empty());

    let mut user_edge_buf = Vec::new();
    let mut write_user = |data: &[u8]| -> std::io::Result<()> {
        user_edge_buf.extend_from_slice(data);
        Ok(())
    };
    let (num_users, total_forward) = graph.stream_user_interactions_to(&mut write_user).unwrap();
    assert_eq!(num_users, 2);
    assert_eq!(total_forward, 3);
    assert!(!user_edge_buf.is_empty());

    let mut post_edge_buf = Vec::new();
    let mut write_post = |data: &[u8]| -> std::io::Result<()> {
        post_edge_buf.extend_from_slice(data);
        Ok(())
    };
    let num_posts = graph.stream_post_interactions_to(&mut write_post).unwrap();
    assert_eq!(num_posts, 2);
    assert!(!post_edge_buf.is_empty());

    let mut bm_out_buf = Vec::new();
    let mut write_bm = |data: &[u8]| -> std::io::Result<()> {
        bm_out_buf.extend_from_slice(data);
        Ok(())
    };
    let mut reusable_bm = Vec::new();
    let num_bms = graph
        .stream_user_likes_bitmaps_to(&mut write_bm, &mut reusable_bm)
        .unwrap();
    assert_eq!(num_bms, 2);
    assert!(!bm_out_buf.is_empty());

    let mut follow_buf = Vec::new();
    let mut write_follow = |data: &[u8]| -> std::io::Result<()> {
        follow_buf.extend_from_slice(data);
        Ok(())
    };
    let num_follows = graph.stream_follows_to(&mut write_follow).unwrap();
    assert_eq!(num_follows, 1);
    assert!(!follow_buf.is_empty());

    let mut meta_buf = Vec::new();
    let mut write_meta = |data: &[u8]| -> std::io::Result<()> {
        meta_buf.extend_from_slice(data);
        Ok(())
    };
    let num_meta = graph.stream_post_metadata_to(&mut write_meta).unwrap();
    assert_eq!(num_meta, 2);
    assert!(!meta_buf.is_empty());

    let mut active_buf = Vec::new();
    let mut write_active = |data: &[u8]| -> std::io::Result<()> {
        active_buf.extend_from_slice(data);
        Ok(())
    };
    let num_active = graph
        .stream_active_recent_posts_to(&mut write_active)
        .unwrap();
    assert_eq!(num_active, 2);
    assert!(!active_buf.is_empty());

    let mut pref_buf = Vec::new();
    let mut write_pref = |data: &[u8]| -> std::io::Result<()> {
        pref_buf.extend_from_slice(data);
        Ok(())
    };
    let num_prefs = preferences.stream_preferences_to(&mut write_pref).unwrap();
    assert_eq!(num_prefs, 1);
    assert!(!pref_buf.is_empty());
}

#[test]
fn test_streaming_methods_propagate_writer_errors() {
    let interner = StringInterner::new();
    let graph = GraphStore::new();
    let preferences = UserPreferencesStore::new();

    for i in 0..10 {
        interner.intern(&format!("did:plc:err_user_{i}"));
        graph.record_interaction(i + 1, i + 100, SignalType::Like, 1_700_000_000);
        preferences.set(i + 1, UserDials::default());
    }

    // 1. Interner failing writer
    let mut calls = 0usize;
    let mut failing_writer = |_data: &[u8]| -> std::io::Result<()> {
        calls += 1;
        if calls > 3 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::StorageFull,
                "disk full",
            ));
        }
        Ok(())
    };
    let res = interner.stream_strings_to(&mut failing_writer);
    assert!(res.is_err(), "interner writer error must propagate");

    // 2. Preferences failing writer
    let mut pref_calls = 0usize;
    let mut failing_pref_writer = |_data: &[u8]| -> std::io::Result<()> {
        pref_calls += 1;
        if pref_calls > 1 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::PermissionDenied,
                "permission denied",
            ));
        }
        Ok(())
    };
    let pref_res = preferences.stream_preferences_to(&mut failing_pref_writer);
    assert!(pref_res.is_err(), "preferences writer error must propagate");

    // 3. Graph failing writer
    let mut graph_calls = 0usize;
    let mut failing_graph_writer = |_data: &[u8]| -> std::io::Result<()> {
        graph_calls += 1;
        if graph_calls > 1 {
            return Err(std::io::Error::new(
                std::io::ErrorKind::BrokenPipe,
                "broken pipe",
            ));
        }
        Ok(())
    };
    let graph_res = graph.stream_user_interactions_to(&mut failing_graph_writer);
    assert!(graph_res.is_err(), "graph writer error must propagate");
}

#[test]
fn test_streaming_snapshot_roundtrip_integrity() {
    let temp_dir = std::env::temp_dir().join(format!("fyfd_test_{}", rand::random::<u64>()));
    std::fs::create_dir_all(&temp_dir).unwrap();
    let snap_path = temp_dir.join("streaming_snapshot.bin");

    let interner = StringInterner::new();
    let graph = GraphStore::new();
    let preferences = UserPreferencesStore::new();

    // Populate diverse state
    for i in 0..100 {
        let user = interner.intern(&format!("did:plc:user_{i}"));
        let post = interner.intern(&format!("at://did:plc:author_{i}/app.bsky.feed.post/{i}"));
        let author = interner.intern(&format!("did:plc:author_{i}"));

        let ts = BLUESKY_EPOCH_SECS + 1_000 + i;
        graph.record_post_meta(post, author, None, None, ts);
        graph.record_interaction(user, post, SignalType::Like, ts);
        if i % 2 == 0 {
            graph.record_interaction(user, post, SignalType::Repost, ts + 1);
            graph.record_follow(user, author);
        }

        if i % 3 == 0 {
            let dials = UserDials {
                freshness_half_life_secs: 3600.0 * ((i % 10) as f32 + 1.0),
                serendipity_ratio: 0.1 * ((i % 5) as f32),
                topic_weights: TopicWeights {
                    art: ((i % 5) as f32).mul_add(0.5, 1.0),
                    tech: 2.0,
                    science: 0.5,
                    news: 1.2,
                    culture: 0.8,
                },
                include_replies: i % 6 == 0,
                min_likes: (i % 5) as u32,
                updated_at_secs: ts,
            };
            preferences.set(user, dials);
        }
    }

    let cursor = 1_725_000_000_123_456u64;

    // Save streaming snapshot
    let header =
        save_snapshot_with_preferences(&snap_path, &interner, &graph, &preferences, cursor)
            .unwrap();
    assert_eq!(header.magic, SNAPSHOT_MAGIC);
    assert_eq!(header.format_version, SNAPSHOT_FORMAT_VERSION);
    assert_eq!(header.jetstream_cursor_us, cursor);
    assert_eq!(header.num_users, 100);
    assert!(header.total_forward_edges >= 100);

    // Load into fresh stores
    let restored_interner = StringInterner::new();
    let restored_graph = GraphStore::new();
    let restored_prefs = UserPreferencesStore::new();

    let loaded = load_snapshot_with_preferences(
        &snap_path,
        &restored_interner,
        &restored_graph,
        &restored_prefs,
    )
    .unwrap()
    .expect("Snapshot must load successfully");

    assert_eq!(loaded.header.format_version, SNAPSHOT_FORMAT_VERSION);
    assert_eq!(loaded.header.jetstream_cursor_us, cursor);
    assert_eq!(restored_interner.len(), interner.len());
    assert_eq!(restored_graph.count_non_empty_users(), 100);
    assert_eq!(restored_prefs.len(), preferences.len());

    // Verify individual items
    for i in 0..100 {
        let user = restored_interner
            .lookup_id(&format!("did:plc:user_{i}"))
            .unwrap();
        let post = restored_interner
            .lookup_id(&format!("at://did:plc:author_{i}/app.bsky.feed.post/{i}"))
            .unwrap();

        let u_edges = restored_graph.get_user_interactions(user);
        assert!(!u_edges.is_empty());
        assert_eq!(u_edges[0].target(), post);

        if i % 3 == 0 {
            let loaded_dials = restored_prefs
                .get(user)
                .expect("Preferences must be restored");
            let orig_dials = preferences.get(user).expect("Original preferences exist");
            assert_eq!(
                loaded_dials.freshness_half_life_secs,
                orig_dials.freshness_half_life_secs
            );
            assert_eq!(loaded_dials.serendipity_ratio, orig_dials.serendipity_ratio);
            assert_eq!(loaded_dials.include_replies, orig_dials.include_replies);
            assert_eq!(loaded_dials.min_likes, orig_dials.min_likes);
        }
    }

    let _ = std::fs::remove_dir_all(&temp_dir);
}

#[test]
fn test_streaming_snapshot_corruption_rejection() {
    let temp_dir =
        std::env::temp_dir().join(format!("fyfd_corrupt_test_{}", rand::random::<u64>()));
    std::fs::create_dir_all(&temp_dir).unwrap();
    let snap_path = temp_dir.join("corrupt_snapshot.bin");

    let interner = StringInterner::new();
    let graph = GraphStore::new();
    let preferences = UserPreferencesStore::new();

    let u = interner.intern("did:plc:user1");
    let p = interner.intern("at://did:plc:author1/app.bsky.feed.post/1");
    graph.record_interaction(u, p, SignalType::Like, 1_700_000_000);

    save_snapshot_with_preferences(&snap_path, &interner, &graph, &preferences, 12345).unwrap();

    let raw_bytes = std::fs::read(&snap_path).unwrap();

    // 1. Corrupt header CRC
    let mut corrupt_header = raw_bytes.clone();
    corrupt_header[10] ^= 0xFF;
    let header_corrupt_path = temp_dir.join("header_corrupt.bin");
    std::fs::write(&header_corrupt_path, &corrupt_header).unwrap();
    let res = load_snapshot_with_preferences(&header_corrupt_path, &interner, &graph, &preferences);
    assert!(res.is_err());

    // 2. Corrupt payload byte
    let mut corrupt_payload = raw_bytes.clone();
    if corrupt_payload.len() > HEADER_SIZE + 5 {
        corrupt_payload[HEADER_SIZE + 5] ^= 0xFF;
        let payload_corrupt_path = temp_dir.join("payload_corrupt.bin");
        std::fs::write(&payload_corrupt_path, &corrupt_payload).unwrap();
        let res =
            load_snapshot_with_preferences(&payload_corrupt_path, &interner, &graph, &preferences);
        assert!(res.is_err());
    }

    // 3. Truncated snapshot
    let truncated_path = temp_dir.join("truncated.bin");
    std::fs::write(&truncated_path, &raw_bytes[..20]).unwrap();
    let res = load_snapshot_with_preferences(&truncated_path, &interner, &graph, &preferences);
    assert!(res.is_err());

    let _ = std::fs::remove_dir_all(&temp_dir);
}

#[tokio::test]
async fn test_spawn_blocking_async_non_blocking_responsiveness() {
    let temp_dir = std::env::temp_dir().join(format!("fyfd_async_test_{}", rand::random::<u64>()));
    std::fs::create_dir_all(&temp_dir).unwrap();
    let snap_path = temp_dir.join("async_snapshot.bin");

    let interner = Arc::new(StringInterner::new());
    let graph = Arc::new(GraphStore::new());
    let preferences = Arc::new(UserPreferencesStore::new());

    for i in 0..1000 {
        let u = interner.intern(&format!("did:plc:async_user_{i}"));
        let p = interner.intern(&format!(
            "at://did:plc:async_author_{i}/app.bsky.feed.post/{i}"
        ));
        graph.record_interaction(u, p, SignalType::Like, BLUESKY_EPOCH_SECS + i);
    }

    let snap_path_clone = snap_path.clone();
    let interner_clone = Arc::clone(&interner);
    let graph_clone = Arc::clone(&graph);
    let prefs_clone = Arc::clone(&preferences);

    // Concurrently run async tasks on Tokio runtime while snapshot saves in blocking pool
    let async_counter = Arc::new(std::sync::atomic::AtomicU64::new(0));
    let counter_clone = Arc::clone(&async_counter);

    let (save_res, _) = tokio::join!(
        tokio::task::spawn_blocking(move || {
            save_snapshot_with_preferences(
                &snap_path_clone,
                &interner_clone,
                &graph_clone,
                &prefs_clone,
                99999,
            )
        }),
        tokio::spawn(async move {
            for _ in 0..100 {
                counter_clone.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
                tokio::task::yield_now().await;
            }
        })
    );

    assert!(save_res.is_ok());
    assert!(save_res.unwrap().is_ok());
    assert_eq!(
        async_counter.load(std::sync::atomic::Ordering::Relaxed),
        100
    );

    let _ = std::fs::remove_dir_all(&temp_dir);
}
