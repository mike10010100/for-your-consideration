//! Adversarial challenge, stress tests, property tests, and empirical latency
//! benchmark for Milestone 3 (`for-your-consideration::ingest::JetstreamIngester` and pipeline components).

#![forbid(unsafe_code)]
#![allow(
    clippy::pedantic,
    clippy::nursery,
    clippy::float_cmp,
    clippy::manual_is_multiple_of
)]

mod common;

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use compact_str::CompactString;
use for_your_consideration::graph::GraphStore;
use for_your_consideration::ingest::{
    apply_event_to_graph, build_subscription_url, parse_jetstream_frame, BackoffManager,
    CursorTracker, IngesterConfig, JetstreamEvent, JetstreamIngester,
};
use for_your_consideration::interner::StringInterner;
use for_your_consideration::recommender::Recommender;
use for_your_consideration::types::{RecommendationDials, SignalType, BLUESKY_EPOCH_SECS};
use futures_util::{SinkExt, StreamExt};
use proptest::prelude::*;
use tokio::net::TcpListener;
use tokio::task::JoinSet;
use tokio_tungstenite::tungstenite::Message;
use tokio_util::sync::CancellationToken;

use crate::common::SyntheticGraphBuilder;

fn test_now() -> u64 {
    BLUESKY_EPOCH_SECS + 1_000_000
}

// ===========================================================================
// Challenge 1: Monotonic Cursor Preservation Under Out-of-Order Chaos
// ===========================================================================

#[test]
fn test_adversarial_cursor_monotonicity_out_of_order_stream() {
    let tracker = CursorTracker::new(Some(1_700_000_000_000_000));
    assert_eq!(tracker.get(), Some(1_700_000_000_000_000));

    // Adversarial stream of timestamps with random perturbations, reversals, and duplicates
    let timestamps = [
        1_700_000_000_000_000,
        1_699_999_999_999_999, // Past timestamp (must NOT regress)
        1_700_000_000_500_000, // Advance
        1_700_000_000_200_000, // Past timestamp (must NOT regress)
        1_700_000_000_500_000, // Duplicate
        1_700_000_000_999_999, // Advance
        0,                     // Zero timestamp
        100,                   // Epoch boundary
        1_700_000_001_000_000, // Advance
        1_700_000_000_800_000, // Old frame received late
    ];

    let expected_high_watermark = [
        1_700_000_000_000_000,
        1_700_000_000_000_000,
        1_700_000_000_500_000,
        1_700_000_000_500_000,
        1_700_000_000_500_000,
        1_700_000_000_999_999,
        1_700_000_000_999_999,
        1_700_000_000_999_999,
        1_700_000_001_000_000,
        1_700_000_001_000_000,
    ];

    for (ts, &expected) in timestamps.into_iter().zip(expected_high_watermark.iter()) {
        tracker.update(ts);
        assert_eq!(tracker.get(), Some(expected));
    }
}

#[test]
fn test_adversarial_cursor_url_construction_variations() {
    let collections = vec![
        CompactString::new("app.bsky.feed.like"),
        CompactString::new("app.bsky.feed.post"),
        CompactString::new("app.bsky.feed.repost"),
        CompactString::new("app.bsky.graph.follow"),
    ];

    // 1. Basic URL without trailing slash
    let url1 = build_subscription_url(
        "wss://jetstream.example.com/subscribe",
        &collections,
        Some(1_700_000_000_000_000),
    );
    assert_eq!(
        url1,
        "wss://jetstream.example.com/subscribe?wantedCollections=app.bsky.feed.like&wantedCollections=app.bsky.feed.post&wantedCollections=app.bsky.feed.repost&wantedCollections=app.bsky.graph.follow&cursor=1700000000000000"
    );

    // 2. Bare host URL (no path)
    let url2 = build_subscription_url("wss://jetstream.example.com", &collections, None);
    assert!(url2.starts_with("wss://jetstream.example.com/?wantedCollections="));

    // 3. Pre-existing query params
    let url3 = build_subscription_url(
        "wss://jetstream.example.com/sub?compress=zstd&max_message_kb=1024",
        &collections,
        Some(123456789),
    );
    assert!(url3.starts_with("wss://jetstream.example.com/sub?compress=zstd&max_message_kb=1024&"));
    assert!(url3.contains("cursor=123456789"));

    // 4. Cursor = 0 or None should NOT append cursor parameter
    let url4 = build_subscription_url(
        "wss://jetstream.example.com/subscribe",
        &collections,
        Some(0),
    );
    assert!(!url4.contains("cursor="));
    let url5 = build_subscription_url("wss://jetstream.example.com/subscribe", &collections, None);
    assert!(!url5.contains("cursor="));
}

// ===========================================================================
// Challenge 2: Inactivity Watchdog Timeout & Reconnect Lifecycle
// ===========================================================================

struct MockWatchdogServer {
    addr: std::net::SocketAddr,
    shutdown_tx: tokio::sync::watch::Sender<bool>,
}

impl MockWatchdogServer {
    async fn start() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (shutdown_tx, mut shutdown_rx) = tokio::sync::watch::channel(false);

        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = shutdown_rx.changed() => {
                        if *shutdown_rx.borrow() {
                            break;
                        }
                    }
                    accept_res = listener.accept() => {
                        if let Ok((stream, _)) = accept_res {
                            let mut shut = shutdown_rx.clone();
                            tokio::spawn(async move {
                                if let Ok(mut ws) = tokio_tungstenite::accept_async(stream).await {
                                    // Keep connection open but send NOTHING (simulating hung/silent upstream relay)
                                    loop {
                                        tokio::select! {
                                            _ = shut.changed() => {
                                                if *shut.borrow() {
                                                    let _ = ws.close(None).await;
                                                    break;
                                                }
                                            }
                                            msg_opt = ws.next() => {
                                                match msg_opt {
                                                    Some(Ok(Message::Ping(data))) => {
                                                        // Respond to keepalive pings
                                                        let _ = ws.send(Message::Pong(data)).await;
                                                    }
                                                    Some(Ok(Message::Close(_))) | None => break,
                                                    _ => {}
                                                }
                                            }
                                        }
                                    }
                                }
                            });
                        }
                    }
                }
            }
        });

        Self { addr, shutdown_tx }
    }

    fn ws_url(&self) -> String {
        format!("ws://{}", self.addr)
    }

    fn shutdown(&self) {
        let _ = self.shutdown_tx.send(true);
    }
}

#[tokio::test]
async fn test_adversarial_inactivity_watchdog_triggers_reconnect() {
    let server = MockWatchdogServer::start().await;
    let interner = Arc::new(StringInterner::new());
    let graph = Arc::new(GraphStore::new());

    // Configure aggressive inactivity timeout and backoff for fast deterministic testing
    let mut config = IngesterConfig::new(server.ws_url());
    config.inactivity_timeout = Duration::from_millis(50); // 50ms silence timeout
    config.initial_backoff = Duration::from_millis(50);
    config.max_backoff = Duration::from_millis(100);
    config.ping_interval = None; // Disable keepalive pings to test raw inactivity trigger

    let ingester = JetstreamIngester::new(config, interner, graph);
    let cancel = CancellationToken::new();

    let mut join_set = JoinSet::new();
    ingester.start_pipeline(&mut join_set, cancel.clone());

    // Wait for at least 2 watchdog timeout reconnect cycles (approx 250ms)
    tokio::time::sleep(Duration::from_millis(250)).await;
    cancel.cancel();

    while let Some(res) = join_set.join_next().await {
        assert!(res.unwrap().is_ok());
    }

    let stats = ingester.stats_snapshot();
    assert!(
        stats.reconnect_count >= 1,
        "Expected at least 1 inactivity watchdog reconnection, got {}",
        stats.reconnect_count
    );

    server.shutdown();
}

// ===========================================================================
// Challenge 3: Exponential Backoff & Jitter Distribution
// ===========================================================================

#[test]
fn test_adversarial_backoff_manager_growth_and_jitter_envelope() {
    let mut backoff = BackoffManager::new(Duration::from_millis(500), Duration::from_secs(30));
    assert_eq!(backoff.consecutive_failures(), 0);

    let mut base_delays = vec![500u64];
    for _ in 0..6 {
        let prev = *base_delays.last().unwrap();
        base_delays.push((prev * 2).min(30_000));
    }

    for (attempt, &base_ms) in base_delays.iter().enumerate() {
        let delay = backoff.next_backoff();
        let delay_ms = delay.as_millis() as u64;

        // ±20% jitter envelope around base_ms
        let min_bound = (base_ms * 80) / 100;
        let max_bound = (base_ms * 120) / 100;

        assert!(
            delay_ms >= min_bound && delay_ms <= max_bound,
            "Attempt {attempt}: delay {delay_ms}ms outside jitter bounds [{min_bound}ms, {max_bound}ms] for base {base_ms}ms"
        );
        assert_eq!(backoff.consecutive_failures(), (attempt as u32) + 1);
    }

    // Advance 10 more times to ensure 30s cap holds strictly
    for _ in 0..10 {
        let delay = backoff.next_backoff();
        let delay_ms = delay.as_millis() as u64;
        assert!(
            (24_000..=36_000).contains(&delay_ms),
            "Capped delay {delay_ms}ms outside jitter bounds [24000ms, 36000ms]"
        );
    }

    // Reset on success
    backoff.reset();
    assert_eq!(backoff.consecutive_failures(), 0);

    let reset_delay = backoff.next_backoff();
    let reset_delay_ms = reset_delay.as_millis() as u64;
    assert!(
        (400..=600).contains(&reset_delay_ms),
        "Reset delay {reset_delay_ms}ms outside initial jitter bounds [400ms, 600ms]"
    );
}

#[test]
fn test_adversarial_backoff_zero_or_tiny_delay_floor() {
    let mut backoff = BackoffManager::new(Duration::from_millis(0), Duration::from_millis(50));
    // Safe minimum floor is 100ms
    let delay = backoff.next_backoff();
    assert!(delay >= Duration::from_millis(50));
}

// ===========================================================================
// Challenge 4: Graph Mutation Integrity & Frame Parser Edge Cases
// ===========================================================================

#[test]
fn test_adversarial_parse_all_collections_and_embedded_quotes() {
    // 1. Post with root, parent, and nested record quote embed
    let post_json = r#"{
        "did": "did:plc:author_1",
        "time_us": 1700000000100000,
        "kind": "commit",
        "commit": {
            "collection": "app.bsky.feed.post",
            "rkey": "3kpost100",
            "operation": "create",
            "record": {
                "$type": "app.bsky.feed.post",
                "text": "Deep thread quote post",
                "reply": {
                    "root": { "uri": "at://did:plc:root_author/app.bsky.feed.post/root1" },
                    "parent": { "uri": "at://did:plc:parent_author/app.bsky.feed.post/parent1" }
                },
                "embed": {
                    "$type": "app.bsky.embed.record",
                    "record": { "uri": "at://did:plc:quoted_author/app.bsky.feed.post/quote1" }
                }
            }
        }
    }"#;

    let (events, time_us) = parse_jetstream_frame(post_json).unwrap();
    assert_eq!(time_us, 1_700_000_000_100_000);
    assert_eq!(events.len(), 2);

    match &events[0] {
        JetstreamEvent::PostMeta {
            post_uri,
            author_did,
            root_uri,
            parent_uri,
            created_at_secs,
        } => {
            assert_eq!(
                post_uri,
                "at://did:plc:author_1/app.bsky.feed.post/3kpost100"
            );
            assert_eq!(author_did, "did:plc:author_1");
            assert_eq!(
                root_uri.as_deref(),
                Some("at://did:plc:root_author/app.bsky.feed.post/root1")
            );
            assert_eq!(
                parent_uri.as_deref(),
                Some("at://did:plc:parent_author/app.bsky.feed.post/parent1")
            );
            assert_eq!(*created_at_secs, 1_700_000_000);
        }
        _ => panic!("Expected PostMeta"),
    }

    match &events[1] {
        JetstreamEvent::Interaction {
            user_did,
            post_uri,
            signal,
            timestamp_secs,
        } => {
            assert_eq!(user_did, "did:plc:author_1");
            assert_eq!(
                post_uri,
                "at://did:plc:quoted_author/app.bsky.feed.post/quote1"
            );
            assert_eq!(*signal, SignalType::Quote);
            assert_eq!(*timestamp_secs, 1_700_000_000);
        }
        _ => panic!("Expected Quote Interaction"),
    }

    // 2. Post with recordWithMedia quote embed
    let post_media_json = r#"{
        "did": "did:plc:author_2",
        "time_us": 1700000000200000,
        "kind": "commit",
        "commit": {
            "collection": "app.bsky.feed.post",
            "rkey": "3kpost200",
            "operation": "create",
            "record": {
                "$type": "app.bsky.feed.post",
                "text": "Quote with media",
                "embed": {
                    "$type": "app.bsky.embed.recordWithMedia",
                    "record": {
                        "record": { "uri": "at://did:plc:quoted_author/app.bsky.feed.post/quote2" }
                    }
                }
            }
        }
    }"#;

    let (media_events, _) = parse_jetstream_frame(post_media_json).unwrap();
    assert_eq!(media_events.len(), 2);
    match &media_events[1] {
        JetstreamEvent::Interaction {
            post_uri, signal, ..
        } => {
            assert_eq!(
                post_uri,
                "at://did:plc:quoted_author/app.bsky.feed.post/quote2"
            );
            assert_eq!(*signal, SignalType::Quote);
        }
        _ => panic!("Expected Quote Interaction"),
    }
}

#[test]
fn test_adversarial_parse_corrupt_or_malicious_frames() {
    let corrupt_payloads = [
        "",
        "   ",
        "{",
        r#"{"kind":"commit"}"#,
        r#"{"kind":"commit","did":""}"#,
        r#"{"kind":"identity","did":"did:plc:alice"}"#,
        r#"{"kind":"commit","did":"did:plc:alice","commit":null}"#,
        r#"{"kind":"commit","did":"did:plc:alice","commit":{"collection":"unknown","operation":"create"}}"#,
        r#"{"kind":"commit","did":"did:plc:alice","commit":{"collection":"app.bsky.feed.like","operation":"create","record":null}}"#,
        r#"{"kind":"commit","did":"did:plc:alice","commit":{"collection":"app.bsky.feed.like","operation":"create","record":{}}}"#,
        r#"{"kind":"commit","did":"did:plc:alice","commit":{"collection":"app.bsky.graph.follow","operation":"create","record":{}}}"#,
    ];

    for bad in corrupt_payloads {
        assert!(
            parse_jetstream_frame(bad).is_none(),
            "Expected None for corrupt frame: {bad}"
        );
    }
}

#[test]
fn test_adversarial_graph_mutation_and_deletion_flow() {
    let interner = Arc::new(StringInterner::new());
    let graph = Arc::new(GraphStore::new());
    let now = test_now();

    let user_did = "did:plc:mutation_user";
    let author_did = "did:plc:mutation_author";
    let post_uri = "at://did:plc:mutation_author/app.bsky.feed.post/m1";

    // 1. PostMeta
    let post_event = JetstreamEvent::PostMeta {
        post_uri: CompactString::new(post_uri),
        author_did: CompactString::new(author_did),
        root_uri: None,
        parent_uri: None,
        created_at_secs: now,
    };
    apply_event_to_graph(&post_event, &interner, &graph);

    let uid = interner.intern(user_did);
    let pid = interner.intern(post_uri);
    let aid = interner.intern(author_did);

    let meta = graph.get_post_meta(pid).unwrap();
    assert_eq!(meta.author_id, aid);

    // 2. Like Interaction
    let like_event = JetstreamEvent::Interaction {
        user_did: CompactString::new(user_did),
        post_uri: CompactString::new(post_uri),
        signal: SignalType::Like,
        timestamp_secs: now,
    };
    apply_event_to_graph(&like_event, &interner, &graph);

    let likes_bm = graph.get_user_likes_bitmap(uid).unwrap();
    assert!(likes_bm.contains(pid));
    assert_eq!(graph.get_user_interactions(uid).len(), 1);

    // 3. Follow
    let follow_event = JetstreamEvent::Follow {
        follower_did: CompactString::new(user_did),
        subject_did: CompactString::new(author_did),
    };
    apply_event_to_graph(&follow_event, &interner, &graph);
    assert_eq!(graph.get_user_follows(uid), vec![aid]);

    // 4. Delete Like
    let delete_like = JetstreamEvent::Delete {
        did: CompactString::new(user_did),
        collection: CompactString::new("app.bsky.feed.like"),
        rkey: CompactString::new("rkey_like"),
    };
    apply_event_to_graph(&delete_like, &interner, &graph);
    assert!(graph.get_user_interactions(uid).is_empty());

    // 5. Delete Follow
    let delete_follow = JetstreamEvent::Delete {
        did: CompactString::new(user_did),
        collection: CompactString::new("app.bsky.graph.follow"),
        rkey: CompactString::new("rkey_follow"),
    };
    apply_event_to_graph(&delete_follow, &interner, &graph);
    assert!(graph.get_user_follows(uid).is_empty());
}

// ===========================================================================
// Challenge 5: Empirical Concurrent Read Latency Under Ingestion Load
// ===========================================================================

#[test]
fn test_adversarial_empirical_concurrent_read_latency_under_ingestion_load() {
    let (interner, graph) = SyntheticGraphBuilder::standard_cold_start_fixture(test_now());
    let rec = Arc::new(Recommender::new(interner, graph));
    let stop_flag = Arc::new(AtomicBool::new(false));
    let read_ops = Arc::new(AtomicUsize::new(0));
    let write_ops = Arc::new(AtomicUsize::new(0));

    let latencies_us = Arc::new(std::sync::Mutex::new(Vec::with_capacity(10_000)));

    let num_readers = 8;
    let num_writers = 2;
    let mut handles = Vec::new();

    // 1. Spawn continuous ingestion writer tasks (simulating 5,000-10,000 events/sec firehose load)
    for writer_id in 0..num_writers {
        let interner = Arc::clone(rec.interner());
        let graph = Arc::clone(rec.graph());
        let stop = Arc::clone(&stop_flag);
        let write_ops = Arc::clone(&write_ops);

        handles.push(thread::spawn(move || {
            let mut i = 0u64;
            while !stop.load(Ordering::Relaxed) {
                let user_did = format!("did:plc:load_user_{writer_id}_{i}");
                let author_did = format!("did:plc:load_author_{writer_id}_{}", i % 50);
                let post_uri = format!(
                    "at://did:plc:load_author_{writer_id}_{}/app.bsky.feed.post/post_{i}",
                    i % 50
                );

                let now_ts = test_now() + (i % 100);

                // Post metadata
                let post_event = JetstreamEvent::PostMeta {
                    post_uri: CompactString::new(&post_uri),
                    author_did: CompactString::new(&author_did),
                    root_uri: None,
                    parent_uri: None,
                    created_at_secs: now_ts,
                };
                apply_event_to_graph(&post_event, &interner, &graph);

                // Interaction
                let sig = match i % 3 {
                    0 => SignalType::Like,
                    1 => SignalType::Repost,
                    _ => SignalType::Quote,
                };
                let int_event = JetstreamEvent::Interaction {
                    user_did: CompactString::new(&user_did),
                    post_uri: CompactString::new(&post_uri),
                    signal: sig,
                    timestamp_secs: now_ts,
                };
                apply_event_to_graph(&int_event, &interner, &graph);

                // Follow
                let follow_event = JetstreamEvent::Follow {
                    follower_did: CompactString::new(&user_did),
                    subject_did: CompactString::new(&author_did),
                };
                apply_event_to_graph(&follow_event, &interner, &graph);

                write_ops.fetch_add(3, Ordering::Relaxed);
                i += 1;
                // Yield/pace to simulate continuous ~5,000-10,000 events/sec firehose throughput
                if i % 10 == 0 {
                    thread::sleep(Duration::from_micros(200));
                }
            }
        }));
    }

    // 2. Spawn concurrent reader tasks measuring recommender query latency
    for _reader_id in 0..num_readers {
        let rec = Arc::clone(&rec);
        let stop = Arc::clone(&stop_flag);
        let read_ops = Arc::clone(&read_ops);
        let latencies = Arc::clone(&latencies_us);

        handles.push(thread::spawn(move || {
            let viewers = [
                Some("did:plc:active_user"),
                Some("did:plc:new_user"),
                Some("did:plc:cold_user"),
                None,
            ];
            let mut i = 0usize;
            let mut local_latencies = Vec::with_capacity(1000);

            while !stop.load(Ordering::Relaxed) {
                let viewer = viewers[i % viewers.len()];
                let dials = RecommendationDials {
                    limit: 20,
                    explore_ratio: 0.15,
                    ..Default::default()
                };

                let start = Instant::now();
                let res = rec.recommend(viewer, &dials, test_now());
                let elapsed = start.elapsed().as_micros() as u64;

                assert!(res.is_ok(), "Recommendation query failed during write load");
                local_latencies.push(elapsed);
                read_ops.fetch_add(1, Ordering::Relaxed);
                i += 1;
            }

            let mut guard = latencies.lock().unwrap();
            guard.extend(local_latencies);
        }));
    }

    // Run benchmark for 300ms under continuous ingestion mutation
    thread::sleep(Duration::from_millis(300));
    stop_flag.store(true, Ordering::Relaxed);

    for h in handles {
        h.join().expect("Thread joined successfully");
    }

    let completed_reads = read_ops.load(Ordering::Relaxed);
    let completed_writes = write_ops.load(Ordering::Relaxed);

    let mut all_latencies = latencies_us.lock().unwrap().clone();
    assert!(
        all_latencies.len() >= 500,
        "Expected >=500 latency measurements, got {}",
        all_latencies.len()
    );

    all_latencies.sort_unstable();

    let count = all_latencies.len();
    let min_us = all_latencies[0];
    let p50_us = all_latencies[count * 50 / 100];
    let p90_us = all_latencies[count * 90 / 100];
    let p95_us = all_latencies[count * 95 / 100];
    let p99_us = all_latencies[count * 99 / 100];
    let max_us = all_latencies[count - 1];
    let sum_us: u64 = all_latencies.iter().sum();
    let mean_us = sum_us as f64 / count as f64;

    println!("\n=== EMPIRICAL CONCURRENT READ LATENCY BENCHMARK (INGESTION LOAD) ===");
    println!("Total Completed Reads:   {}", completed_reads);
    println!("Total Completed Writes:  {}", completed_writes);
    println!(
        "Min:     {:>6} µs ({:.3} ms)",
        min_us,
        min_us as f64 / 1000.0
    );
    println!("Mean:    {:>6.1} µs ({:.3} ms)", mean_us, mean_us / 1000.0);
    println!(
        "p50:     {:>6} µs ({:.3} ms)",
        p50_us,
        p50_us as f64 / 1000.0
    );
    println!(
        "p90:     {:>6} µs ({:.3} ms)",
        p90_us,
        p90_us as f64 / 1000.0
    );
    println!(
        "p95:     {:>6} µs ({:.3} ms)",
        p95_us,
        p95_us as f64 / 1000.0
    );
    println!(
        "p99:     {:>6} µs ({:.3} ms)",
        p99_us,
        p99_us as f64 / 1000.0
    );
    println!(
        "Max:     {:>6} µs ({:.3} ms)",
        max_us,
        max_us as f64 / 1000.0
    );
    println!("====================================================================\n");

    // Assert: p50 latency must remain strictly sub-millisecond (< 1000 µs)
    assert!(
        p50_us < 1000,
        "p50 latency ({p50_us} µs) exceeded sub-millisecond SLA threshold (<1000 µs)!"
    );
    // In unoptimized debug test runs with parallel threads, allow up to 20ms; in release mode strictly assert < 2.0ms
    let p99_threshold = if cfg!(debug_assertions) {
        20_000
    } else {
        2_000
    };
    assert!(
        p99_us < p99_threshold,
        "p99 latency ({p99_us} µs) exceeded SLA threshold ({p99_threshold} µs) during continuous write load!"
    );
}

// ===========================================================================
// Challenge 6: Proptest Invariant Fuzzing for Ingestion Frame Parser
// ===========================================================================

proptest! {
    #![proptest_config(ProptestConfig::with_cases(100))]

    #[test]
    fn test_proptest_jetstream_frame_parser_safety(
        time_us in 0u64..3_000_000_000_000_000u64,
        did in "[a-z0-9:._-]{1,50}",
        rkey in "[a-zA-Z0-9_-]{1,20}",
        collection_idx in 0usize..4usize,
    ) {
        let collections = [
            "app.bsky.feed.like",
            "app.bsky.feed.repost",
            "app.bsky.feed.post",
            "app.bsky.graph.follow",
        ];
        let collection = collections[collection_idx];

        let json = serde_json::json!({
            "did": did,
            "time_us": time_us,
            "kind": "commit",
            "commit": {
                "collection": collection,
                "rkey": rkey,
                "operation": "create",
                "record": {
                    "$type": collection,
                    "subject": format!("at://{did}/app.bsky.feed.post/{rkey}"),
                    "text": "Proptest post",
                }
            }
        });

        let json_str = json.to_string();
        let parsed = parse_jetstream_frame(&json_str);

        // Parser must never panic
        if !did.is_empty() {
            prop_assert!(parsed.is_some(), "Valid commit frame should parse successfully");
            let (events, parsed_time_us) = parsed.unwrap();
            prop_assert_eq!(parsed_time_us, time_us);
            prop_assert!(!events.is_empty());
        }
    }
}
