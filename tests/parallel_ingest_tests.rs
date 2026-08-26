#![forbid(unsafe_code)]
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    missing_docs,
    clippy::pedantic,
    clippy::nursery
)]

//! Integration tests for parallel multi-stream Jetstream collection slicing and monotonic cursor watermarking.

use std::sync::atomic::Ordering;
use std::sync::Arc;
use std::time::Duration;

use compact_str::CompactString;
use for_your_consideration::prelude::*;
use futures_util::SinkExt;
use tokio::sync::mpsc;
use tokio::task::JoinSet;
use tokio_tungstenite::tungstenite::Message;
use tokio_util::sync::CancellationToken;

struct MockJetstreamEndpoint {
    addr: std::net::SocketAddr,
    event_tx: mpsc::Sender<String>,
    shutdown_tx: tokio::sync::watch::Sender<bool>,
}

impl MockJetstreamEndpoint {
    async fn start() -> Self {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let (event_tx, event_rx) = mpsc::channel::<String>(200);
        let (shutdown_tx, mut shutdown_rx) = tokio::sync::watch::channel(false);
        let shared_rx = Arc::new(tokio::sync::Mutex::new(event_rx));

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
                            let rx = Arc::clone(&shared_rx);
                            let mut shut = shutdown_rx.clone();
                            tokio::spawn(async move {
                                if let Ok(mut ws) = tokio_tungstenite::accept_async(stream).await {
                                    loop {
                                        tokio::select! {
                                            _ = shut.changed() => {
                                                if *shut.borrow() {
                                                    let _ = ws.close(None).await;
                                                    break;
                                                }
                                            }
                                            msg = async {
                                                let mut guard = rx.lock().await;
                                                guard.recv().await
                                            } => {
                                                match msg {
                                                    Some(json) => {
                                                        if ws.send(Message::Text(json)).await.is_err() {
                                                            break;
                                                        }
                                                    }
                                                    None => break,
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

        Self {
            addr,
            event_tx,
            shutdown_tx,
        }
    }

    fn ws_url(&self) -> String {
        format!("ws://{}", self.addr)
    }

    async fn send_like(&self, user_did: &str, post_uri: &str, time_us: u64) {
        let payload = serde_json::json!({
            "did": user_did,
            "time_us": time_us,
            "kind": "commit",
            "commit": {
                "collection": "app.bsky.feed.like",
                "rkey": "3klike_test",
                "operation": "create",
                "record": {
                    "$type": "app.bsky.feed.like",
                    "subject": {
                        "uri": post_uri,
                        "cid": "bafyreih3..."
                    },
                    "createdAt": "2026-08-21T18:00:00Z"
                }
            }
        });
        let _ = self.event_tx.send(payload.to_string()).await;
    }

    async fn send_repost(&self, user_did: &str, post_uri: &str, time_us: u64) {
        let payload = serde_json::json!({
            "did": user_did,
            "time_us": time_us,
            "kind": "commit",
            "commit": {
                "collection": "app.bsky.feed.repost",
                "rkey": "3krepost_test",
                "operation": "create",
                "record": {
                    "$type": "app.bsky.feed.repost",
                    "subject": {
                        "uri": post_uri,
                        "cid": "bafyreirepost..."
                    },
                    "createdAt": "2026-08-21T18:00:00Z"
                }
            }
        });
        let _ = self.event_tx.send(payload.to_string()).await;
    }

    async fn send_post(&self, author_did: &str, rkey: &str, time_us: u64) {
        let payload = serde_json::json!({
            "did": author_did,
            "time_us": time_us,
            "kind": "commit",
            "commit": {
                "collection": "app.bsky.feed.post",
                "rkey": rkey,
                "operation": "create",
                "record": {
                    "$type": "app.bsky.feed.post",
                    "text": "Multi-stream test post",
                    "createdAt": "2026-08-21T18:00:00Z"
                }
            }
        });
        let _ = self.event_tx.send(payload.to_string()).await;
    }

    async fn send_follow(&self, follower_did: &str, subject_did: &str, time_us: u64) {
        let payload = serde_json::json!({
            "did": follower_did,
            "time_us": time_us,
            "kind": "commit",
            "commit": {
                "collection": "app.bsky.graph.follow",
                "rkey": "3kfollow_test",
                "operation": "create",
                "record": {
                    "$type": "app.bsky.graph.follow",
                    "subject": subject_did,
                    "createdAt": "2026-08-21T18:00:00Z"
                }
            }
        });
        let _ = self.event_tx.send(payload.to_string()).await;
    }

    fn shutdown(&self) {
        let _ = self.shutdown_tx.send(true);
    }
}

#[tokio::test]
async fn test_parallel_multi_stream_slicing_e2e() {
    let ep0 = MockJetstreamEndpoint::start().await; // like slice
    let ep1 = MockJetstreamEndpoint::start().await; // repost slice
    let ep2 = MockJetstreamEndpoint::start().await; // post slice
    let ep3 = MockJetstreamEndpoint::start().await; // follow slice

    let interner = Arc::new(StringInterner::new());
    let graph = Arc::new(GraphStore::new());

    let endpoints = vec![
        CompactString::new(ep0.ws_url()),
        CompactString::new(ep1.ws_url()),
        CompactString::new(ep2.ws_url()),
        CompactString::new(ep3.ws_url()),
    ];

    let start_cursor = 1_700_000_000_000_000u64;
    let config = IngesterConfig::default()
        .with_endpoints(endpoints)
        .with_parallel_slicing(true)
        .with_initial_cursor(Some(start_cursor))
        .with_channel_capacity(500)
        .with_inactivity_timeout(Duration::from_secs(10));

    let ingester = JetstreamIngester::new(config, Arc::clone(&interner), Arc::clone(&graph));
    let cancel = CancellationToken::new();

    let mut join_set = JoinSet::new();
    ingester.start_pipeline(&mut join_set, cancel.clone());

    tokio::time::sleep(Duration::from_millis(60)).await;

    // Send events on each endpoint
    ep0.send_like(
        "did:plc:user_like",
        "at://did:plc:author1/app.bsky.feed.post/1",
        start_cursor + 100_000,
    )
    .await;
    ep1.send_repost(
        "did:plc:user_repost",
        "at://did:plc:author2/app.bsky.feed.post/2",
        start_cursor + 200_000,
    )
    .await;
    ep2.send_post("did:plc:author3", "p3", start_cursor + 150_000)
        .await;
    ep3.send_follow(
        "did:plc:follower",
        "did:plc:followed",
        start_cursor + 120_000,
    )
    .await;

    tokio::time::sleep(Duration::from_millis(150)).await;
    cancel.cancel();

    while let Some(res) = join_set.join_next().await {
        assert!(res.unwrap().is_ok());
    }

    // Verify graph state
    let uid_like = interner.lookup_id("did:plc:user_like").unwrap();
    let uid_repost = interner.lookup_id("did:plc:user_repost").unwrap();
    let uid_author3 = interner.lookup_id("did:plc:author3").unwrap();
    let uid_follower = interner.lookup_id("did:plc:follower").unwrap();
    let uid_followed = interner.lookup_id("did:plc:followed").unwrap();

    assert_eq!(graph.get_user_interactions(uid_like).len(), 1);
    assert_eq!(graph.get_user_interactions(uid_repost).len(), 1);
    let p3_id = interner
        .lookup_id("at://did:plc:author3/app.bsky.feed.post/p3")
        .unwrap();
    assert_eq!(graph.get_post_meta(p3_id).unwrap().author_id, uid_author3);
    assert_eq!(graph.get_user_follows(uid_follower), vec![uid_followed]);

    // Check stats & unified low watermark
    let snapshot = ingester.stats_snapshot();
    assert_eq!(snapshot.active_slices, 4);
    assert!(snapshot.events_received >= 4);
    assert!(snapshot.events_processed >= 4);
    // Minimum across slices: start_cursor + 100_000
    assert_eq!(snapshot.latest_cursor_us, start_cursor + 100_000);

    ep0.shutdown();
    ep1.shutdown();
    ep2.shutdown();
    ep3.shutdown();
}

#[test]
fn test_multi_stream_cursor_tracker_asymmetric_slicing() {
    let tracker = CursorTracker::new(Some(10_000));
    tracker.set_slice_count(4);

    // Initial state
    assert_eq!(tracker.get_raw(), 10_000);

    // Slices advance unevenly
    tracker.update_slice(0, 20_000);
    assert_eq!(tracker.get_raw(), 10_000);

    tracker.update_slice(1, 15_000);
    assert_eq!(tracker.get_raw(), 10_000);

    tracker.update_slice(2, 18_000);
    assert_eq!(tracker.get_raw(), 10_000);

    tracker.update_slice(3, 12_000);
    // Now min(20000, 15000, 18000, 12000) = 12000
    assert_eq!(tracker.get_raw(), 12_000);

    // Slice 3 catches up to 16000 -> min becomes 15000 (slice 1)
    tracker.update_slice(3, 16_000);
    assert_eq!(tracker.get_raw(), 15_000);

    // Slice 1 catches up to 22000 -> min becomes 16000 (slice 3)
    tracker.update_slice(1, 22_000);
    assert_eq!(tracker.get_raw(), 16_000);
}

#[test]
fn test_multi_stream_stats_active_slices_and_snapshot() {
    let stats = IngestionStats::new(Some(1_000_000));
    stats.set_slice_count(4);

    stats.events_received.fetch_add(10, Ordering::Relaxed);
    stats.events_processed.fetch_add(8, Ordering::Relaxed);

    stats.update_slice_cursor(0, 1_100_000);
    stats.update_slice_cursor(1, 1_080_000);
    stats.update_slice_cursor(2, 1_050_000);
    stats.update_slice_cursor(3, 1_060_000);

    let snap = stats.snapshot();
    assert_eq!(snap.active_slices, 4);
    assert_eq!(snap.events_received, 10);
    assert_eq!(snap.events_processed, 8);
    assert_eq!(snap.latest_cursor_us, 1_050_000);
}

#[tokio::test]
async fn test_parallel_ingest_single_stream_fallback() {
    let ep = MockJetstreamEndpoint::start().await;
    let interner = Arc::new(StringInterner::new());
    let graph = Arc::new(GraphStore::new());

    let config = IngesterConfig::new(ep.ws_url())
        .with_parallel_slicing(false)
        .with_initial_cursor(Some(1_700_000_000_000_000))
        .with_collections(vec![
            CompactString::new("app.bsky.feed.like"),
            CompactString::new("app.bsky.feed.post"),
        ])
        .with_channel_capacity(100)
        .with_inactivity_timeout(Duration::from_secs(10));

    let ingester = JetstreamIngester::new(config, Arc::clone(&interner), Arc::clone(&graph));
    let cancel = CancellationToken::new();

    let mut join_set = JoinSet::new();
    ingester.start_pipeline(&mut join_set, cancel.clone());

    tokio::time::sleep(Duration::from_millis(50)).await;

    // Send like and post on single stream
    ep.send_like(
        "did:plc:single_user",
        "at://did:plc:author/app.bsky.feed.post/single_post",
        1_700_000_000_100_000,
    )
    .await;
    ep.send_post("did:plc:author", "single_post", 1_700_000_000_200_000)
        .await;

    tokio::time::sleep(Duration::from_millis(150)).await;
    cancel.cancel();

    while let Some(res) = join_set.join_next().await {
        assert!(res.unwrap().is_ok());
    }

    let snap = ingester.stats_snapshot();
    assert_eq!(snap.active_slices, 1);
    assert!(snap.events_processed >= 2);
    assert_eq!(snap.latest_cursor_us, 1_700_000_000_200_000);

    ep.shutdown();
}

#[tokio::test]
async fn test_parallel_ingest_lag_catchup_and_watermark_ratchet() {
    let ep0 = MockJetstreamEndpoint::start().await; // fast slice 0
    let ep1 = MockJetstreamEndpoint::start().await; // fast slice 1
    let ep2 = MockJetstreamEndpoint::start().await; // fast slice 2
    let ep3 = MockJetstreamEndpoint::start().await; // laggy slice 3

    let interner = Arc::new(StringInterner::new());
    let graph = Arc::new(GraphStore::new());

    let endpoints = vec![
        CompactString::new(ep0.ws_url()),
        CompactString::new(ep1.ws_url()),
        CompactString::new(ep2.ws_url()),
        CompactString::new(ep3.ws_url()),
    ];

    let base_cursor = 2_000_000_000_000_000u64;
    let config = IngesterConfig::default()
        .with_endpoints(endpoints)
        .with_parallel_slicing(true)
        .with_initial_cursor(Some(base_cursor))
        .with_channel_capacity(500)
        .with_inactivity_timeout(Duration::from_secs(10));

    let ingester = JetstreamIngester::new(config, Arc::clone(&interner), Arc::clone(&graph));
    let cancel = CancellationToken::new();

    let mut join_set = JoinSet::new();
    ingester.start_pipeline(&mut join_set, cancel.clone());

    tokio::time::sleep(Duration::from_millis(50)).await;

    // Fast streams advance by +500_000, +600_000, +700_000
    ep0.send_like(
        "did:plc:fast0",
        "at://did:plc:target/app.bsky.feed.post/1",
        base_cursor + 500_000,
    )
    .await;
    ep1.send_repost(
        "did:plc:fast1",
        "at://did:plc:target/app.bsky.feed.post/2",
        base_cursor + 600_000,
    )
    .await;
    ep2.send_post("did:plc:fast2", "p3", base_cursor + 700_000)
        .await;

    // Laggy stream only advances by +50_000
    ep3.send_follow("did:plc:lag3", "did:plc:target", base_cursor + 50_000)
        .await;

    tokio::time::sleep(Duration::from_millis(150)).await;

    // Watermark must be locked to min (base_cursor + 50_000)
    let snap1 = ingester.stats_snapshot();
    assert_eq!(snap1.latest_cursor_us, base_cursor + 50_000);

    // Laggy stream now catches up to +800_000
    ep3.send_follow("did:plc:lag3", "did:plc:target2", base_cursor + 800_000)
        .await;

    tokio::time::sleep(Duration::from_millis(150)).await;

    // Watermark must now ratchet up to min(500k, 600k, 700k, 800k) = base_cursor + 500_000
    let snap2 = ingester.stats_snapshot();
    assert_eq!(snap2.latest_cursor_us, base_cursor + 500_000);

    cancel.cancel();
    while let Some(res) = join_set.join_next().await {
        assert!(res.unwrap().is_ok());
    }

    ep0.shutdown();
    ep1.shutdown();
    ep2.shutdown();
    ep3.shutdown();
}

#[test]
fn test_multi_stream_cursor_tracker_concurrent_monotonicity() {
    use std::sync::atomic::AtomicBool;
    use std::thread;

    let tracker = Arc::new(CursorTracker::new(Some(1_000_000)));
    tracker.set_slice_count(4);

    let running = Arc::new(AtomicBool::new(true));
    let mut handles = Vec::new();

    // 8 writer threads updating slices
    for t_idx in 0..8 {
        let tr = Arc::clone(&tracker);
        let run = Arc::clone(&running);
        handles.push(thread::spawn(move || {
            let slice_idx = t_idx % 4;
            let mut val = 1_000_000 + (t_idx as u64 * 100);
            while run.load(Ordering::Relaxed) {
                val += 10;
                tr.update_slice(slice_idx, val);
            }
        }));
    }

    // 4 reader threads checking strict monotonicity
    for _ in 0..4 {
        let tr = Arc::clone(&tracker);
        let run = Arc::clone(&running);
        handles.push(thread::spawn(move || {
            let mut last_seen = tr.get_raw();
            while run.load(Ordering::Relaxed) {
                let current = tr.get_raw();
                assert!(
                    current >= last_seen,
                    "Monotonicity violation: current {current} < last_seen {last_seen}"
                );
                last_seen = current;
            }
        }));
    }

    thread::sleep(Duration::from_millis(100));
    running.store(false, Ordering::Relaxed);

    for h in handles {
        h.join().unwrap();
    }

    assert!(tracker.get_raw() > 1_000_000);
}

#[test]
fn test_multi_stream_stats_concurrent_monotonicity() {
    use std::sync::atomic::AtomicBool;
    use std::thread;

    let stats = Arc::new(IngestionStats::new(Some(5_000_000)));
    stats.set_slice_count(4);

    let running = Arc::new(AtomicBool::new(true));
    let mut handles = Vec::new();

    // 8 writer threads updating slice cursors
    for t_idx in 0..8 {
        let st = Arc::clone(&stats);
        let run = Arc::clone(&running);
        handles.push(thread::spawn(move || {
            let slice_idx = t_idx % 4;
            let mut val = 5_000_000 + (t_idx as u64 * 50);
            while run.load(Ordering::Relaxed) {
                val += 25;
                st.update_slice_cursor(slice_idx, val);
            }
        }));
    }

    // 4 reader threads checking strict monotonicity
    for _ in 0..4 {
        let st = Arc::clone(&stats);
        let run = Arc::clone(&running);
        handles.push(thread::spawn(move || {
            let mut last_seen = st.latest_cursor_us.load(Ordering::Relaxed);
            while run.load(Ordering::Relaxed) {
                let current = st.latest_cursor_us.load(Ordering::Relaxed);
                assert!(
                    current >= last_seen,
                    "Monotonicity violation: current {current} < last_seen {last_seen}"
                );
                last_seen = current;
            }
        }));
    }

    thread::sleep(Duration::from_millis(100));
    running.store(false, Ordering::Relaxed);

    for h in handles {
        h.join().unwrap();
    }

    assert!(stats.latest_cursor_us.load(Ordering::Relaxed) > 5_000_000);
}
