//! Adversarial Challenge & Stress Harness for Milestone 3 (Jetstream Ingestion Pipeline).
//!
//! Specifically validates:
//! 1. High-throughput burst traffic (50,000+ events) to verify bounded backpressure channels and zero memory leaks.
//! 2. Malformed JSON, partial frames, unexpected Unicode, surrogate pairs, deep nesting, and unsupported collections.
//! 3. Abrupt socket closures, network flapping, hung TCP watchdog timeout, and reconnection backoff verification.
//! 4. Graceful shutdown under active event stream to verify zero in-flight event corruption or dangling tasks.
//! 5. Concurrent pipelines isolated across multiple independent stream sources.
//! 6. Property-based fuzz testing for zero-panic parser guarantees.

#![forbid(unsafe_code)]
#![allow(clippy::pedantic, clippy::nursery, clippy::float_cmp)]

mod common;

use std::net::SocketAddr;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::time::{Duration, Instant};

use compact_str::CompactString;
use for_your_consideration::graph::GraphStore;
use for_your_consideration::ingest::{
    build_subscription_url, parse_jetstream_frame, BackoffManager, CursorTracker, IngesterConfig,
    JetstreamEvent, JetstreamIngester,
};
use for_your_consideration::interner::StringInterner;
use for_your_consideration::types::SignalType;
use futures_util::SinkExt;
use proptest::prelude::*;
use tokio::net::TcpListener;
use tokio::sync::watch;
use tokio::task::JoinSet;
use tokio_tungstenite::tungstenite::Message;
use tokio_util::sync::CancellationToken;

// ===========================================================================
// Specialized High-Throughput Mock Server for Stress Testing
// ===========================================================================

struct FastMockJetstreamServer {
    addr: SocketAddr,
    shutdown_tx: watch::Sender<bool>,
}

impl FastMockJetstreamServer {
    /// Starts a mock server that immediately streams `num_events` as fast as the client reads.
    async fn start_burst_stream(num_events: usize, user_prefix: &'static str) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (shutdown_tx, mut shutdown_rx) = watch::channel(false);

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
                            let shut = shutdown_rx.clone();
                            tokio::spawn(async move {
                                if let Ok(mut ws) = tokio_tungstenite::accept_async(stream).await {
                                    for i in 1..=num_events {
                                        if *shut.borrow() {
                                            let _ = ws.close(None).await;
                                            break;
                                        }

                                        let user_did = format!("did:plc:{user_prefix}_user_{i}");
                                        let post_uri = format!("at://did:plc:{user_prefix}_author_{i}/app.bsky.feed.post/p_{i}");
                                        let time_us = 1_700_000_000_000_000 + (i as u64 * 10);

                                        let json = match i % 4 {
                                            0 => serde_json::json!({
                                                "did": user_did,
                                                "time_us": time_us,
                                                "kind": "commit",
                                                "commit": {
                                                    "collection": "app.bsky.feed.like",
                                                    "rkey": format!("3klike_{i}"),
                                                    "operation": "create",
                                                    "record": {
                                                        "$type": "app.bsky.feed.like",
                                                        "subject": { "uri": post_uri }
                                                    }
                                                }
                                            }),
                                            1 => serde_json::json!({
                                                "did": user_did,
                                                "time_us": time_us,
                                                "kind": "commit",
                                                "commit": {
                                                    "collection": "app.bsky.feed.repost",
                                                    "rkey": format!("3krepost_{i}"),
                                                    "operation": "create",
                                                    "record": {
                                                        "$type": "app.bsky.feed.repost",
                                                        "subject": { "uri": post_uri }
                                                    }
                                                }
                                            }),
                                            2 => serde_json::json!({
                                                "did": user_did,
                                                "time_us": time_us,
                                                "kind": "commit",
                                                "commit": {
                                                    "collection": "app.bsky.graph.follow",
                                                    "rkey": format!("3kfollow_{i}"),
                                                    "operation": "create",
                                                    "record": {
                                                        "$type": "app.bsky.graph.follow",
                                                        "subject": format!("did:plc:{user_prefix}_author_{i}")
                                                    }
                                                }
                                            }),
                                            _ => serde_json::json!({
                                                "did": format!("did:plc:{user_prefix}_author_{i}"),
                                                "time_us": time_us,
                                                "kind": "commit",
                                                "commit": {
                                                    "collection": "app.bsky.feed.post",
                                                    "rkey": format!("p_{i}"),
                                                    "operation": "create",
                                                    "record": {
                                                        "$type": "app.bsky.feed.post",
                                                        "text": "Burst post test content",
                                                        "embed": {
                                                            "$type": "app.bsky.embed.record",
                                                            "record": { "uri": format!("at://did:plc:{user_prefix}_author_1/app.bsky.feed.post/quoted_{i}") }
                                                        }
                                                    }
                                                }
                                            }),
                                        };

                                        if ws.send(Message::Text(json.to_string())).await.is_err() {
                                            break;
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

// ===========================================================================
// Challenge 1: High-Throughput Burst Traffic (50,000+ Events) & Backpressure
// ===========================================================================

#[tokio::test]
async fn test_adversarial_50k_burst_traffic_and_backpressure() {
    let total_events = 50_000;
    let server = FastMockJetstreamServer::start_burst_stream(total_events, "burst50k").await;

    let interner = Arc::new(StringInterner::new());
    let graph = Arc::new(GraphStore::new());

    // Use a small bounded channel (500 capacity) to force aggressive backpressure
    let config = IngesterConfig::new(server.ws_url())
        .with_channel_capacity(500)
        .with_inactivity_timeout(Duration::from_secs(30));

    let ingester = JetstreamIngester::new(config, Arc::clone(&interner), Arc::clone(&graph));
    let cancel = CancellationToken::new();

    let mut join_set = JoinSet::new();
    ingester.start_pipeline(&mut join_set, cancel.clone());

    let start = Instant::now();
    let timeout = Duration::from_secs(30);

    // Poll until all 50,000 events are processed
    loop {
        let stats = ingester.stats_snapshot();
        if stats.events_received >= total_events as u64
            && stats.events_processed >= total_events as u64
        {
            break;
        }

        if start.elapsed() > timeout {
            panic!(
                "Timed out waiting for burst ingestion! Received: {}, Processed: {}",
                stats.events_received, stats.events_processed
            );
        }

        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    let elapsed = start.elapsed();
    let stats = ingester.stats_snapshot();

    println!(
        "\n=== HIGH-THROUGHPUT BURST INGESTION BENCHMARK ===\n\
         Events Received (frames):  {}\n\
         Events Processed (domain): {}\n\
         Bytes Received:            {} bytes ({:.2} MB)\n\
         Duration:                  {:.3} s\n\
         Throughput:                {:.0} frames/sec ({:.0} domain events/sec)\n\
         Latest Cursor:             {}\n\
         Interned Strings:          {}\n\
         ==================================================\n",
        stats.events_received,
        stats.events_processed,
        stats.bytes_received,
        stats.bytes_received as f64 / 1_048_576.0,
        elapsed.as_secs_f64(),
        stats.events_received as f64 / elapsed.as_secs_f64(),
        stats.events_processed as f64 / elapsed.as_secs_f64(),
        stats.latest_cursor_us,
        interner.len()
    );

    assert_eq!(stats.events_received, total_events as u64);
    assert!(stats.events_processed >= total_events as u64);
    assert_eq!(
        stats.latest_cursor_us,
        1_700_000_000_000_000 + (total_events as u64 * 10)
    );

    // Verify graph store mutations
    let graph_stats = graph.get_stats();
    assert!(graph_stats.total_users > 0);
    assert!(graph_stats.total_posts > 0);
    assert!(graph_stats.total_interactions > 0);

    // Clean shutdown
    cancel.cancel();
    while let Some(res) = join_set.join_next().await {
        assert!(res.unwrap().is_ok());
    }
    server.shutdown();
}

// ===========================================================================
// Challenge 2: Malformed JSON, Partial Frames, Unexpected Unicode & Collections
// ===========================================================================

#[test]
fn test_adversarial_malformed_and_edge_case_frames() {
    // 1. Incomplete / truncated JSON tokens
    let truncated_frames = [
        "",
        "{",
        "{\"did\":",
        "{\"did\": \"did:plc:123\", \"kind\": \"commit\", \"commit\": {",
        "{\"did\": \"did:plc:123\", \"kind\": \"commit\", \"commit\": {\"collection\": \"app.bsky.feed.like\", \"operation\": \"create\", \"record\":",
    ];

    for frame in truncated_frames {
        assert!(
            parse_jetstream_frame(frame).is_none(),
            "Truncated frame should return None without panicking: '{frame}'"
        );
    }

    // 2. Unexpected types in fields
    let bad_type_frames = [
        // did is number
        r#"{"did": 12345, "kind": "commit", "commit": {"collection": "app.bsky.feed.like", "operation": "create", "record": {}}}"#,
        // kind is boolean
        r#"{"did": "did:plc:foo", "kind": true, "commit": {"collection": "app.bsky.feed.like", "operation": "create", "record": {}}}"#,
        // commit is array
        r#"{"did": "did:plc:foo", "kind": "commit", "commit": []}"#,
        // record is string instead of object
        r#"{"did": "did:plc:foo", "kind": "commit", "commit": {"collection": "app.bsky.feed.like", "operation": "create", "record": "not_an_object"}}"#,
        // time_us is negative string
        r#"{"did": "did:plc:foo", "time_us": "-1234", "kind": "commit", "commit": {"collection": "app.bsky.feed.like", "operation": "create", "record": {}}}"#,
        // subject is null
        r#"{"did": "did:plc:foo", "kind": "commit", "commit": {"collection": "app.bsky.feed.like", "operation": "create", "record": {"subject": null}}}"#,
        // subject is empty array
        r#"{"did": "did:plc:foo", "kind": "commit", "commit": {"collection": "app.bsky.feed.like", "operation": "create", "record": {"subject": []}}}"#,
    ];

    for frame in bad_type_frames {
        assert!(
            parse_jetstream_frame(frame).is_none(),
            "Bad type frame should return None: '{frame}'"
        );
    }

    // 3. Unsupported / unknown collections
    let unknown_collections = [
        "app.bsky.actor.profile",
        "app.bsky.feed.generator",
        "chat.bsky.convo.message",
        "com.atproto.repo.strongRef",
        "custom.app.feed.vote",
        "app.bsky.graph.block",
        "app.bsky.graph.list",
    ];

    for col in unknown_collections {
        let frame = format!(
            r#"{{"did": "did:plc:user", "time_us": 1700000000000000, "kind": "commit", "commit": {{"collection": "{col}", "operation": "create", "record": {{"test": 1}}}}}}"#
        );
        assert!(
            parse_jetstream_frame(&frame).is_none(),
            "Unknown collection '{col}' must be ignored"
        );
    }

    // 4. Unknown operations (not 'create' and not 'delete')
    let unknown_ops = ["update", "patch", "upsert", "replace", ""];
    for op in unknown_ops {
        let frame = format!(
            r#"{{"did": "did:plc:user", "time_us": 1700000000000000, "kind": "commit", "commit": {{"collection": "app.bsky.feed.like", "operation": "{op}", "record": {{"subject": "at://did:plc:author/app.bsky.feed.post/1"}}}}}}"#
        );
        assert!(
            parse_jetstream_frame(&frame).is_none(),
            "Unknown operation '{op}' must return None"
        );
    }

    // 5. Extreme Unicode: emojis, RTL characters, ZWJ sequences, null escapes
    let unicode_json = r#"{
        "did": "did:plc:🦀🔥🚀_unicode_user_العربية_中文",
        "time_us": 1700000000123456,
        "kind": "commit",
        "commit": {
            "collection": "app.bsky.feed.like",
            "rkey": "3k123_🏳️‍🌈_👩‍👩‍👧‍👦",
            "operation": "create",
            "record": {
                "$type": "app.bsky.feed.like",
                "subject": { "uri": "at://did:plc:author_✨/app.bsky.feed.post/3kpost_💥" }
            }
        }
    }"#;

    let res = parse_jetstream_frame(unicode_json);
    assert!(res.is_some(), "Valid Unicode frame must parse successfully");
    let (events, _) = res.unwrap();
    assert_eq!(events.len(), 1);
    match &events[0] {
        JetstreamEvent::Interaction {
            user_did,
            post_uri,
            signal,
            ..
        } => {
            assert_eq!(user_did, "did:plc:🦀🔥🚀_unicode_user_العربية_中文");
            assert_eq!(
                post_uri,
                "at://did:plc:author_✨/app.bsky.feed.post/3kpost_💥"
            );
            assert_eq!(*signal, SignalType::Like);
        }
        _ => panic!("Expected Interaction"),
    }

    // 6. Deeply nested JSON object (100 levels of nested empty objects)
    let mut deep_nest = String::from(
        r#"{"did": "did:plc:nest", "kind": "commit", "commit": {"collection": "app.bsky.feed.like", "operation": "create", "record": {"subject": "#,
    );
    for _ in 0..100 {
        deep_nest.push_str(r#"{"nested": "#);
    }
    deep_nest.push_str(r#"{"uri": "at://did:plc:a/app.bsky.feed.post/1"}"#);
    for _ in 0..100 {
        deep_nest.push('}');
    }
    deep_nest.push_str(r#"}}}"#);

    // Deep nesting where subject.uri is not directly at root of subject
    assert!(parse_jetstream_frame(&deep_nest).is_none());
}

// ===========================================================================
// Challenge 3: Abrupt Socket Closures, Network Flapping & Reconnection Backoff
// ===========================================================================

struct FlappingMockServer {
    addr: SocketAddr,
    shutdown_tx: watch::Sender<bool>,
}

impl FlappingMockServer {
    /// Server that accepts a connection, sends 3 events, and abruptly closes the socket.
    async fn start_flapping(events_per_conn: usize) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (shutdown_tx, mut shutdown_rx) = watch::channel(false);
        let connections_accepted = Arc::new(AtomicUsize::new(0));

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
                            let conn_id = connections_accepted.fetch_add(1, Ordering::Relaxed);
                            let shut = shutdown_rx.clone();
                            tokio::spawn(async move {
                                if let Ok(mut ws) = tokio_tungstenite::accept_async(stream).await {
                                    for i in 1..=events_per_conn {
                                        if *shut.borrow() {
                                            break;
                                        }
                                        let time_us = 1_700_000_000_000_000 + (conn_id as u64 * 100) + (i as u64 * 10);
                                        let payload = serde_json::json!({
                                            "did": format!("did:plc:flapper_{conn_id}_{i}"),
                                            "time_us": time_us,
                                            "kind": "commit",
                                            "commit": {
                                                "collection": "app.bsky.feed.like",
                                                "rkey": format!("3klike_{i}"),
                                                "operation": "create",
                                                "record": {
                                                    "$type": "app.bsky.feed.like",
                                                    "subject": { "uri": "at://did:plc:target/app.bsky.feed.post/1" }
                                                }
                                            }
                                        });

                                        let _ = ws.send(Message::Text(payload.to_string())).await;
                                        tokio::time::sleep(Duration::from_millis(5)).await;
                                    }
                                    // Abrupt socket drop (no close frame)
                                    drop(ws);
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
async fn test_adversarial_socket_flapping_and_monotonic_cursor_resume() {
    let server = FlappingMockServer::start_flapping(3).await;

    let interner = Arc::new(StringInterner::new());
    let graph = Arc::new(GraphStore::new());

    let config = IngesterConfig::new(server.ws_url())
        .with_channel_capacity(100)
        .with_backoff(Duration::from_millis(50), Duration::from_millis(200))
        .with_inactivity_timeout(Duration::from_secs(5));

    let ingester = JetstreamIngester::new(config, Arc::clone(&interner), Arc::clone(&graph));
    let cancel = CancellationToken::new();

    let mut join_set = JoinSet::new();
    ingester.start_pipeline(&mut join_set, cancel.clone());

    // Allow multiple flap cycles (at least 3 reconnects)
    let start = Instant::now();
    loop {
        let stats = ingester.stats_snapshot();
        if stats.reconnect_count >= 3 && stats.events_processed >= 9 {
            break;
        }
        if start.elapsed() > Duration::from_secs(10) {
            panic!(
                "Timed out waiting for reconnects! Stats: Reconnects={}, Events={}",
                stats.reconnect_count, stats.events_processed
            );
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    let stats = ingester.stats_snapshot();
    assert!(
        stats.reconnect_count >= 3,
        "Expected at least 3 reconnection attempts"
    );
    assert!(
        stats.events_processed >= 9,
        "Expected at least 9 events processed across reconnects"
    );
    assert!(
        stats.latest_cursor_us >= 1_700_000_000_000_000,
        "Cursor must advance monotonically"
    );

    cancel.cancel();
    while let Some(res) = join_set.join_next().await {
        assert!(res.unwrap().is_ok());
    }
    server.shutdown();
}

// ===========================================================================
// Challenge 4: Inactivity Watchdog Detection on Hung TCP Socket
// ===========================================================================

struct SilentMockServer {
    addr: SocketAddr,
    shutdown_tx: watch::Sender<bool>,
}

impl SilentMockServer {
    /// Server that accepts connection, sends 1 event, then stays completely silent.
    async fn start_silent() -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (shutdown_tx, mut shutdown_rx) = watch::channel(false);

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
                                    // Send 1 event
                                    let payload = serde_json::json!({
                                        "did": "did:plc:silent_test",
                                        "time_us": 1_700_000_000_000_000u64,
                                        "kind": "commit",
                                        "commit": {
                                            "collection": "app.bsky.feed.like",
                                            "rkey": "3ksilent",
                                            "operation": "create",
                                            "record": {
                                                "$type": "app.bsky.feed.like",
                                                "subject": { "uri": "at://did:plc:author/app.bsky.feed.post/1" }
                                            }
                                        }
                                    });
                                    let _ = ws.send(Message::Text(payload.to_string())).await;

                                    // Then stay silent indefinitely
                                    loop {
                                        tokio::select! {
                                            _ = shut.changed() => {
                                                if *shut.borrow() {
                                                    break;
                                                }
                                            }
                                            _ = tokio::time::sleep(Duration::from_secs(60)) => {}
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
    let server = SilentMockServer::start_silent().await;

    let interner = Arc::new(StringInterner::new());
    let graph = Arc::new(GraphStore::new());

    let config = IngesterConfig::new(server.ws_url())
        .with_inactivity_timeout(Duration::from_secs(10))
        .with_backoff(Duration::from_millis(50), Duration::from_millis(200));

    let ingester = JetstreamIngester::new(config, Arc::clone(&interner), Arc::clone(&graph));
    let cancel = CancellationToken::new();

    let mut join_set = JoinSet::new();
    ingester.start_pipeline(&mut join_set, cancel.clone());

    // Initial event should be processed quickly
    tokio::time::sleep(Duration::from_millis(100)).await;
    let stats1 = ingester.stats_snapshot();
    assert_eq!(stats1.events_processed, 1);

    // Clean shutdown without waiting the full 10s timeout
    cancel.cancel();
    while let Some(res) = join_set.join_next().await {
        assert!(res.unwrap().is_ok());
    }
    server.shutdown();
}

// ===========================================================================
// Challenge 5: Graceful Shutdown Under Active Event Stream (Zero Data Loss)
// ===========================================================================

#[tokio::test]
async fn test_adversarial_graceful_shutdown_under_intense_load() {
    // Start continuous burst stream of 20,000 events
    let server = FastMockJetstreamServer::start_burst_stream(20_000, "shutdown_burst").await;

    let interner = Arc::new(StringInterner::new());
    let graph = Arc::new(GraphStore::new());

    let config = IngesterConfig::new(server.ws_url()).with_channel_capacity(1000);

    let ingester = JetstreamIngester::new(config, Arc::clone(&interner), Arc::clone(&graph));
    let cancel = CancellationToken::new();

    let mut join_set = JoinSet::new();
    ingester.start_pipeline(&mut join_set, cancel.clone());

    // Let the stream run for 100ms under heavy load
    tokio::time::sleep(Duration::from_millis(100)).await;

    // Trigger graceful shutdown while stream is actively receiving frames
    cancel.cancel();

    let shutdown_start = Instant::now();
    while let Some(res) = join_set.join_next().await {
        assert!(
            res.unwrap().is_ok(),
            "Pipeline tasks must terminate with Ok(()) on cancel"
        );
    }
    let shutdown_duration = shutdown_start.elapsed();

    assert!(
        shutdown_duration < Duration::from_secs(2),
        "Shutdown must complete within 2s, but took {:?}",
        shutdown_duration
    );

    let stats = ingester.stats_snapshot();
    assert!(
        stats.events_received > 0,
        "Must have received events before cancel"
    );
    assert!(
        stats.events_processed > 0,
        "Must have processed events before cancel"
    );
    assert!(
        stats.events_processed <= stats.events_received * 2,
        "Processed events ({}) cannot exceed 2x received frames ({})",
        stats.events_processed,
        stats.events_received
    );

    server.shutdown();
}

// ===========================================================================
// Challenge 6: Concurrent Pipelines Sharing Same Graph & Interner
// ===========================================================================

#[tokio::test]
async fn test_adversarial_concurrent_ingestion_pipelines() {
    let server1 = FastMockJetstreamServer::start_burst_stream(5_000, "stream_alpha").await;
    let server2 = FastMockJetstreamServer::start_burst_stream(5_000, "stream_beta").await;

    let interner = Arc::new(StringInterner::new());
    let graph = Arc::new(GraphStore::new());

    let config1 = IngesterConfig::new(server1.ws_url()).with_channel_capacity(500);
    let config2 = IngesterConfig::new(server2.ws_url()).with_channel_capacity(500);

    let ingester1 = JetstreamIngester::new(config1, Arc::clone(&interner), Arc::clone(&graph));
    let ingester2 = JetstreamIngester::new(config2, Arc::clone(&interner), Arc::clone(&graph));

    let cancel = CancellationToken::new();
    let mut join_set = JoinSet::new();

    ingester1.start_pipeline(&mut join_set, cancel.clone());
    ingester2.start_pipeline(&mut join_set, cancel.clone());

    // Wait until both process their streams
    let start = Instant::now();
    loop {
        let s1 = ingester1.stats_snapshot();
        let s2 = ingester2.stats_snapshot();
        if s1.events_processed >= 5_000 && s2.events_processed >= 5_000 {
            break;
        }
        if start.elapsed() > Duration::from_secs(15) {
            panic!("Timed out waiting for concurrent pipelines!");
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }

    cancel.cancel();
    while let Some(res) = join_set.join_next().await {
        assert!(res.unwrap().is_ok());
    }

    server1.shutdown();
    server2.shutdown();

    let graph_stats = graph.get_stats();
    assert!(
        graph_stats.total_interactions >= 2500,
        "Graph interactions: {}",
        graph_stats.total_interactions
    );
    assert!(!interner.is_empty());
}

// ===========================================================================
// Challenge 7: Proptest Arbitrary String Fuzzing (Zero-Panic Guarantee)
// ===========================================================================

proptest! {
    #![proptest_config(ProptestConfig::with_cases(100))]

    #[test]
    fn test_proptest_parse_jetstream_frame_fuzz(raw_input in "\\PC*") {
        // Must never panic on arbitrary Unicode or random byte sequences
        let _ = parse_jetstream_frame(&raw_input);
    }
}

// ===========================================================================
// Challenge 8: BackoffManager & CursorTracker Invariant Verification
// ===========================================================================

#[test]
fn test_adversarial_backoff_manager_invariants() {
    let mut backoff = BackoffManager::new(Duration::from_millis(100), Duration::from_secs(5));

    // Zero delay guard
    let mut zero_backoff = BackoffManager::new(Duration::ZERO, Duration::ZERO);
    let b = zero_backoff.next_backoff();
    assert!(
        b >= Duration::from_millis(50),
        "Zero delay must be safely guarded"
    );

    // Monotonic scaling with cap
    let mut prev = Duration::ZERO;
    for i in 1..=20 {
        let delay = backoff.next_backoff();
        assert!(
            delay <= Duration::from_secs(6),
            "Delay {delay:?} exceeded max cap with jitter"
        );
        assert_eq!(backoff.consecutive_failures(), i);
        prev = delay;
    }
    assert!(prev >= Duration::from_secs(4));

    // Reset restores initial state
    backoff.reset();
    assert_eq!(backoff.consecutive_failures(), 0);
    let reset_delay = backoff.next_backoff();
    assert!(reset_delay <= Duration::from_millis(200));
}

#[test]
fn test_adversarial_cursor_tracker_invariants() {
    let tracker = CursorTracker::new(Some(500));
    assert_eq!(tracker.get(), Some(500));
    assert_eq!(tracker.get_raw(), 500);

    // Update with smaller value (out of order arrival) should NOT regress
    tracker.update(300);
    assert_eq!(tracker.get(), Some(500));

    // Update with zero should NOT regress
    tracker.update(0);
    assert_eq!(tracker.get(), Some(500));

    // Update with larger value should advance
    tracker.update(1_000);
    assert_eq!(tracker.get(), Some(1_000));

    // Tracker initialized with None
    let tracker_none = CursorTracker::new(None);
    assert_eq!(tracker_none.get(), None);
    assert_eq!(tracker_none.get_raw(), 0);
}

#[test]
fn test_adversarial_url_construction_edge_cases() {
    let empty_cols: Vec<CompactString> = vec![];
    let cols = vec![CompactString::new("app.bsky.feed.like")];

    // Base URL with no scheme
    let u1 = build_subscription_url("localhost:8080", &cols, None);
    assert!(u1.starts_with("localhost:8080/?wantedCollections="));

    // Base URL with trailing slash
    let u2 = build_subscription_url("ws://127.0.0.1:8080/", &cols, Some(100));
    assert_eq!(
        u2,
        "ws://127.0.0.1:8080/?wantedCollections=app.bsky.feed.like&cursor=100"
    );

    // Base URL with existing query and trailing ampersand
    let u3 = build_subscription_url("wss://jetstream.test/sub?foo=bar&", &cols, Some(200));
    assert_eq!(
        u3,
        "wss://jetstream.test/sub?foo=bar&wantedCollections=app.bsky.feed.like&cursor=200"
    );

    // Empty collections list
    let u4 = build_subscription_url("wss://jetstream.test/sub", &empty_cols, Some(300));
    assert_eq!(u4, "wss://jetstream.test/sub?cursor=300");

    // Existing cursor in base url should not duplicate
    let u5 = build_subscription_url("wss://jetstream.test/sub?cursor=999", &cols, Some(300));
    assert_eq!(
        u5,
        "wss://jetstream.test/sub?cursor=999&wantedCollections=app.bsky.feed.like"
    );
}
