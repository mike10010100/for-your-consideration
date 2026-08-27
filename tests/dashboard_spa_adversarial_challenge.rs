#![forbid(unsafe_code)]

//! Empirical Adversarial Challenge Test Suite for Milestone 3 (Embedded Web Dashboard SPA):
//!
//! 1. High-concurrency request throughput for `GET /` and `GET /dashboard` (500+ concurrent requests).
//! 2. Asset size and binary footprint verification (compact embedded asset, zero CDN leakage, offline airgapped).
//! 3. End-to-end client interaction flow simulation:
//!    - SPA HTML delivery (`GET /` & `GET /dashboard`)
//!    - Live graph telemetry polling (`GET /api/telemetry`)
//!    - Handle / DID Taste Twins search (`GET /api/taste-twins`)
//!    - Algorithmic dials tuning & instant feed preview (`GET /api/feed-preview`)
//!    - 3-step Graph Proof Chain explainer modal (`GET /api/explain`)
//! 4. Adversarial dial inputs, extreme parameter ranges, and HTTP method rejection.
//! 5. DOM XSS and special character safety audit for client-side rendering.

use std::fmt::Write;
use std::sync::Arc;
use std::time::Instant;

use axum::body::Body;
use axum::http::{Method, Request, StatusCode};
use for_your_consideration::prelude::*;
use for_your_consideration::server::DASHBOARD_HTML;
use http_body_util::BodyExt;
use tower::ServiceExt;

/// Simple percent encoder for query parameters.
fn simple_percent_encode(input: &str) -> String {
    let mut out = String::with_capacity(input.len() * 3);
    for b in input.bytes() {
        match b {
            b'a'..=b'z' | b'A'..=b'Z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                out.push(b as char);
            }
            _ => {
                let _ = write!(out, "%{b:02X}");
            }
        }
    }
    out
}

/// Creates a populated test environment with interconnected users, posts, topics, and signals.
fn create_test_fixture() -> (
    AppState,
    Arc<StringInterner>,
    Arc<GraphStore>,
    Arc<Recommender>,
) {
    let interner = Arc::new(StringInterner::new());
    let graph = Arc::new(GraphStore::new());
    let recommender = Arc::new(Recommender::new(Arc::clone(&interner), Arc::clone(&graph)));

    let now = BLUESKY_EPOCH_SECS + 1_000_000;

    // Intern user DIDs
    let alice = interner.intern("did:plc:alice");
    let bob = interner.intern("did:plc:bob");
    let carol = interner.intern("did:plc:carol");
    let dave = interner.intern("did:plc:dave");
    let eve = interner.intern("did:plc:eve");

    let art_seed = interner.intern("did:plc:seed_art_creator");
    let tech_seed = interner.intern("did:plc:seed_tech_creator");
    let sci_seed = interner.intern("did:plc:seed_sci_creator");
    let news_seed = interner.intern("did:plc:seed_news_creator");
    let culture_seed = interner.intern("did:plc:seed_culture_creator");

    // Create categorized posts
    let art_post1 =
        interner.intern("at://did:plc:seed_art_creator/app.bsky.feed.post/canvas_oil_1");
    let art_post2 =
        interner.intern("at://did:plc:seed_art_creator/app.bsky.feed.post/watercolor_2");
    let tech_post1 =
        interner.intern("at://did:plc:seed_tech_creator/app.bsky.feed.post/rust_tokio_1");
    let tech_post2 =
        interner.intern("at://did:plc:seed_tech_creator/app.bsky.feed.post/simd_bitmaps_2");
    let sci_post1 =
        interner.intern("at://did:plc:seed_sci_creator/app.bsky.feed.post/exoplanet_transit_1");
    let news_post1 =
        interner.intern("at://did:plc:seed_news_creator/app.bsky.feed.post/breaking_tech_news_1");
    let culture_post1 =
        interner.intern("at://did:plc:seed_culture_creator/app.bsky.feed.post/indie_film_review_1");

    graph.record_post_meta(art_post1, art_seed, None, None, now - 1800);
    graph.record_post_meta(art_post2, art_seed, None, None, now - 3600);
    graph.record_post_meta(tech_post1, tech_seed, None, None, now - 900);
    graph.record_post_meta(tech_post2, tech_seed, None, None, now - 7200);
    graph.record_post_meta(sci_post1, sci_seed, None, None, now - 1200);
    graph.record_post_meta(news_post1, news_seed, None, None, now - 300);
    graph.record_post_meta(culture_post1, culture_seed, None, None, now - 5400);

    // 10 historical likes for Alice to activate Tier 1 personalized graph walk
    for i in 1..=10 {
        let p = interner.intern(&format!(
            "at://did:plc:seed_tech_creator/app.bsky.feed.post/alice_hist_{i}"
        ));
        graph.record_post_meta(p, tech_seed, None, None, now - 20_000);
        graph.record_interaction(alice, p, SignalType::Like, now - 15_000);
    }

    // Shared likes between Alice and Bob (Co-interactor Taste Twins)
    graph.record_interaction(alice, tech_post1, SignalType::Like, now - 800);
    graph.record_interaction(alice, art_post1, SignalType::Like, now - 800);

    graph.record_interaction(bob, tech_post1, SignalType::Like, now - 700);
    graph.record_interaction(bob, art_post1, SignalType::Like, now - 700);
    graph.record_interaction(bob, tech_post2, SignalType::Like, now - 600);
    graph.record_interaction(bob, art_post2, SignalType::Repost, now - 500);

    // Follow relationships
    graph.record_follow(alice, carol);
    graph.record_follow(carol, dave);
    graph.record_follow(dave, eve);

    graph.record_interaction(carol, sci_post1, SignalType::Like, now - 400);
    graph.record_interaction(dave, news_post1, SignalType::Like, now - 250);
    graph.record_interaction(eve, culture_post1, SignalType::Like, now - 150);

    // Baseline interactions so candidate posts meet default engagement floor (min_likes: 3)
    let u1 = interner.intern("did:plc:mock_emp_user_1");
    let u2 = interner.intern("did:plc:mock_emp_user_2");
    for &p in &[tech_post2, art_post2, sci_post1, news_post1, culture_post1] {
        graph.record_interaction(u1, p, SignalType::Like, now - 350);
        graph.record_interaction(u2, p, SignalType::Like, now - 350);
    }

    // Snapshot Tracker
    let snap_config = SnapshotConfig {
        path: std::path::PathBuf::from("target/challenger_m3_snapshot.bin"),
        interval_secs: 300,
    };
    let snapshot_tracker = Arc::new(SnapshotStatusTracker::new(&snap_config));
    snapshot_tracker.record_load(12.4);

    // Ingestion Tracker
    let stats = Arc::new(IngestionStats::new(Some(1_700_000_000_000_000)));
    stats
        .events_received
        .store(24500, std::sync::atomic::Ordering::Relaxed);
    stats
        .events_processed
        .store(24450, std::sync::atomic::Ordering::Relaxed);
    stats
        .bytes_received
        .store(3_500_000, std::sync::atomic::Ordering::Relaxed);
    stats
        .last_activity_timestamp
        .store(now, std::sync::atomic::Ordering::Relaxed);
    let ingestion_tracker = Arc::new(IngestionTracker::new(stats));

    let state = AppState::new(
        Arc::clone(&recommender),
        "did:web:feed.challenger.test",
        "feed.challenger.test",
    )
    .with_snapshot_tracker(snapshot_tracker)
    .with_ingestion_tracker(ingestion_tracker);

    (state, interner, graph, recommender)
}

#[tokio::test]
async fn test_empirical_spa_high_concurrency_throughput_500_clients() {
    let (state, _, _, _) = create_test_fixture();
    let router = create_xrpc_router(state);

    let total_concurrent_requests = 500;
    let mut handles = Vec::with_capacity(total_concurrent_requests);
    let start_all = Instant::now();

    for i in 0..total_concurrent_requests {
        let app = router.clone();
        let handle = tokio::spawn(async move {
            let req_start = Instant::now();
            let uri = if i % 2 == 0 { "/" } else { "/dashboard" };
            let req = Request::builder()
                .method(Method::GET)
                .uri(uri)
                .body(Body::empty())
                .unwrap();

            let resp = app.oneshot(req).await.unwrap();
            let elapsed_us = req_start.elapsed().as_micros() as u64;

            assert_eq!(
                resp.status(),
                StatusCode::OK,
                "Concurrent request #{i} must return HTTP 200 OK"
            );

            let content_type = resp
                .headers()
                .get("content-type")
                .unwrap()
                .to_str()
                .unwrap();
            assert!(
                content_type.starts_with("text/html"),
                "Content type must be text/html"
            );

            let body_bytes = resp.into_body().collect().await.unwrap().to_bytes();
            assert_eq!(
                body_bytes.len(),
                DASHBOARD_HTML.len(),
                "Served HTML length must match static embedded constant exactly"
            );

            elapsed_us
        });
        handles.push(handle);
    }

    let mut latencies_us = Vec::with_capacity(total_concurrent_requests);
    for h in handles {
        let lat = h.await.expect("Tokio task must not panic");
        latencies_us.push(lat);
    }

    let total_duration = start_all.elapsed();
    latencies_us.sort_unstable();

    let p50 = latencies_us[latencies_us.len() / 2];
    let p95 = latencies_us[(latencies_us.len() * 95) / 100];
    let p99 = latencies_us[(latencies_us.len() * 99) / 100];
    let max = *latencies_us.last().unwrap();
    let rps = (total_concurrent_requests as f64) / total_duration.as_secs_f64();

    println!("\n=== EMPIRICAL SPA HIGH CONCURRENCY BENCHMARK (500 requests) ===");
    println!("Total Duration:  {:?}", total_duration);
    println!("Throughput:      {:.1} requests/sec", rps);
    println!(
        "p50 Latency:     {} µs ({:.2} ms)",
        p50,
        p50 as f64 / 1000.0
    );
    println!(
        "p95 Latency:     {} µs ({:.2} ms)",
        p95,
        p95 as f64 / 1000.0
    );
    println!(
        "p99 Latency:     {} µs ({:.2} ms)",
        p99,
        p99 as f64 / 1000.0
    );
    println!(
        "Max Latency:     {} µs ({:.2} ms)",
        max,
        max as f64 / 1000.0
    );
    println!("===============================================================\n");

    // Static HTML serving from memory should be near-instantaneous
    assert!(
        p99 < 50_000,
        "p99 latency for static in-memory HTML serving must be < 50ms, got: {} µs",
        p99
    );
}

#[tokio::test]
async fn test_empirical_spa_asset_size_and_binary_footprint() {
    let raw_bytes = DASHBOARD_HTML.as_bytes();
    let raw_len = raw_bytes.len();

    println!("\n=== EMBEDDED SPA ASSET FOOTPRINT AUDIT ===");
    println!(
        "Raw HTML size: {} bytes ({:.2} KB)",
        raw_len,
        raw_len as f64 / 1024.0
    );

    // 1. Compactness check (< 120 KB uncompressed)
    assert!(
        raw_len < 120 * 1024,
        "Dashboard SPA HTML must be compact (< 120 KB), actual size: {} bytes",
        raw_len
    );
    assert!(
        raw_len > 10 * 1024,
        "Dashboard SPA HTML must contain full CSS/JS/HTML UI (> 10 KB), actual size: {} bytes",
        raw_len
    );

    // 2. HTML5 Standard Verification
    assert!(DASHBOARD_HTML.starts_with("<!DOCTYPE html>"));
    assert!(DASHBOARD_HTML.contains("<html lang=\"en\">"));
    assert!(DASHBOARD_HTML.contains("<meta charset=\"UTF-8\">"));
    assert!(DASHBOARD_HTML
        .contains("<meta name=\"viewport\" content=\"width=device-width, initial-scale=1.0\">"));
    assert!(DASHBOARD_HTML.contains("<title>For You Feed"));

    // 3. Airgap & Zero External CDN / Remote Script Audit
    let forbidden_external_resources = [
        "src=\"http",
        "src=\"https",
        "href=\"http",
        "href=\"https",
        "url(\"http",
        "url('http",
        "cdn.jsdelivr.net",
        "unpkg.com",
        "cdnjs.cloudflare.com",
        "fonts.googleapis.com",
        "fonts.gstatic.com",
        "code.jquery.com",
        "stackpath.bootstrapcdn.com",
        "tailwind",
        "bootstrap",
    ];

    for pattern in &forbidden_external_resources {
        assert!(
            !DASHBOARD_HTML.contains(pattern),
            "Forbidden external network dependency found in embedded SPA: '{pattern}'"
        );
    }

    // 4. Verify no eval() or document.write() in scripts
    assert!(
        !DASHBOARD_HTML.contains("eval("),
        "Unsafe eval() found in dashboard script"
    );
    assert!(
        !DASHBOARD_HTML.contains("document.write("),
        "Unsafe document.write() found in dashboard script"
    );

    // 5. Verify HTML escaping utility exists in client JS
    assert!(
        DASHBOARD_HTML.contains("function escapeHtml("),
        "Client JS must include escapeHtml sanitization utility"
    );
    assert!(DASHBOARD_HTML.contains(".replace(/&/g, '&amp;')"));
    assert!(DASHBOARD_HTML.contains(".replace(/</g, '&lt;')"));
    assert!(DASHBOARD_HTML.contains(".replace(/>/g, '&gt;')"));
    assert!(DASHBOARD_HTML.contains(".replace(/\"/g, '&quot;')"));
    assert!(DASHBOARD_HTML.contains(".replace(/'/g, '&#039;')"));
}

#[tokio::test]
async fn test_empirical_end_to_end_client_interaction_flow() {
    let (state, _, _, _) = create_test_fixture();
    let app = create_xrpc_router(state);

    // =========================================================================
    // Step 1: Initial Page Load (GET / and GET /dashboard)
    // =========================================================================
    let req_page = Request::builder().uri("/").body(Body::empty()).unwrap();
    let resp_page = app.clone().oneshot(req_page).await.unwrap();
    assert_eq!(resp_page.status(), StatusCode::OK);
    let html_body = resp_page.into_body().collect().await.unwrap().to_bytes();
    let html_str = String::from_utf8(html_body.to_vec()).unwrap();
    assert!(html_str.contains("id=\"telemetry\""));
    assert!(html_str.contains("id=\"taste-twins\""));
    assert!(html_str.contains("id=\"dials\""));
    assert!(html_str.contains("id=\"feed-preview\""));
    assert!(html_str.contains("id=\"proof-modal\""));

    // =========================================================================
    // Step 2: Client Telemetry Polling (GET /api/telemetry)
    // =========================================================================
    let req_telemetry = Request::builder()
        .uri("/api/telemetry")
        .body(Body::empty())
        .unwrap();
    let resp_telemetry = app.clone().oneshot(req_telemetry).await.unwrap();
    assert_eq!(resp_telemetry.status(), StatusCode::OK);

    let telemetry_bytes = resp_telemetry
        .into_body()
        .collect()
        .await
        .unwrap()
        .to_bytes();
    let telemetry_data: serde_json::Value =
        serde_json::from_slice(&telemetry_bytes).expect("Telemetry response must be valid JSON");

    // Validate structure consumed by dashboard.html fetchTelemetry()
    assert!(telemetry_data.get("graph").is_some());
    let graph_obj = &telemetry_data["graph"];
    assert!(graph_obj["total_edges"].as_u64().unwrap() > 0);
    assert!(graph_obj["total_nodes"].as_u64().unwrap() > 0);
    assert!(graph_obj["total_users"].as_u64().unwrap() > 0);
    assert!(graph_obj["total_posts"].as_u64().unwrap() > 0);
    assert!(graph_obj["total_follows"].as_u64().unwrap() > 0);

    assert!(telemetry_data.get("interner").is_some());
    assert!(
        telemetry_data["interner"]["total_interned_strings"]
            .as_u64()
            .unwrap()
            > 0
    );

    assert!(telemetry_data.get("ingestion").is_some());
    assert!(
        telemetry_data["ingestion"]["events_processed"]
            .as_u64()
            .unwrap()
            > 0
    );

    assert!(telemetry_data.get("snapshot").is_some());
    let snap_status = telemetry_data["snapshot"]["status"].as_str().unwrap();
    assert!(
        snap_status == "hydrated" || snap_status == "persisted",
        "Snapshot status must be valid ('hydrated' or 'persisted'), got: {snap_status}"
    );

    assert!(telemetry_data.get("uptime_seconds").is_some());

    // =========================================================================
    // Step 3: Handle / DID Taste Twins Search (GET /api/taste-twins)
    // =========================================================================
    let req_twins = Request::builder()
        .uri("/api/taste-twins?did=did:plc:alice&limit=10")
        .body(Body::empty())
        .unwrap();
    let resp_twins = app.clone().oneshot(req_twins).await.unwrap();
    assert_eq!(resp_twins.status(), StatusCode::OK);

    let twins_bytes = resp_twins.into_body().collect().await.unwrap().to_bytes();
    let twins_data: serde_json::Value =
        serde_json::from_slice(&twins_bytes).expect("Taste twins response must be valid JSON");

    assert_eq!(twins_data["viewer_did"].as_str().unwrap(), "did:plc:alice");
    assert!(twins_data["total_liked_posts"].as_u64().unwrap() > 0);

    let twins_arr = twins_data["twins"]
        .as_array()
        .expect("twins must be an array");
    assert!(!twins_arr.is_empty(), "Alice must have taste twins (Bob)");

    let top_twin = &twins_arr[0];
    assert_eq!(top_twin["user_did"].as_str().unwrap(), "did:plc:bob");
    let sim_score = top_twin["similarity_score"].as_f64().unwrap();
    assert!(
        (0.0..=1.0).contains(&sim_score),
        "Similarity score must be in [0.0, 1.0], got {sim_score}"
    );
    assert!(top_twin["shared_posts_count"].as_u64().unwrap() >= 2);

    // =========================================================================
    // Step 4: Algorithmic Dials Manipulation & Feed Preview (GET /api/feed-preview)
    // =========================================================================
    // Configuration A: Balanced dials
    let req_preview_a = Request::builder()
        .uri("/api/feed-preview?viewer=did:plc:alice&freshness=36&discovery=0.15&art=1.0&tech=1.0&science=1.0&news=1.0&culture=1.0&limit=15&explain=true")
        .body(Body::empty())
        .unwrap();
    let resp_preview_a = app.clone().oneshot(req_preview_a).await.unwrap();
    assert_eq!(resp_preview_a.status(), StatusCode::OK);

    let preview_bytes_a = resp_preview_a
        .into_body()
        .collect()
        .await
        .unwrap()
        .to_bytes();
    let preview_data_a: serde_json::Value =
        serde_json::from_slice(&preview_bytes_a).expect("Feed preview must be valid JSON");

    let items_a = preview_data_a["items"]
        .as_array()
        .expect("items must be an array");
    assert!(
        !items_a.is_empty(),
        "Feed preview must generate candidates for Alice"
    );
    #[cfg(not(debug_assertions))]
    assert!(
        preview_data_a["query_latency_us"].as_u64().unwrap() < 2000,
        "Query latency must be sub-2ms (< 2000µs) in release mode"
    );
    #[cfg(debug_assertions)]
    assert!(
        preview_data_a["query_latency_us"].as_u64().unwrap() < 50_000,
        "Query latency abnormal debug spike"
    );

    // Verify mathematical score breakdowns on each candidate item
    for item in items_a {
        let uri = item["uri"].as_str().unwrap();
        assert!(!uri.is_empty(), "Candidate post URI must not be empty");
        let breakdown = &item["score_breakdown"];
        assert!(breakdown["time_decay"].as_f64().unwrap() > 0.0);
        assert!(breakdown["taste_similarity"].as_f64().unwrap() >= 0.0);
        assert!(breakdown["topic_boost"].as_f64().unwrap() >= 0.0);
        assert!(breakdown["fatigue_penalty"].as_f64().unwrap() > 0.0);
        assert!(breakdown["final_score"].as_f64().unwrap() > 0.0);
    }

    // Configuration B: Art Boosted (art=5.0, others=0.0)
    let req_preview_b = Request::builder()
        .uri("/api/feed-preview?viewer=did:plc:alice&freshness=24&discovery=0.0&art=5.0&tech=0.0&science=0.0&news=0.0&culture=0.0&limit=10")
        .body(Body::empty())
        .unwrap();
    let resp_preview_b = app.clone().oneshot(req_preview_b).await.unwrap();
    assert_eq!(resp_preview_b.status(), StatusCode::OK);
    let preview_bytes_b = resp_preview_b
        .into_body()
        .collect()
        .await
        .unwrap()
        .to_bytes();
    let preview_data_b: serde_json::Value = serde_json::from_slice(&preview_bytes_b).unwrap();
    let items_b = preview_data_b["items"].as_array().unwrap();

    if !items_b.is_empty() {
        let top_topic = items_b[0]["topic"].as_str().unwrap();
        assert_eq!(
            top_topic.to_lowercase(),
            "art",
            "Top candidate under 5.0x Art boost must be Art category"
        );
    }

    // =========================================================================
    // Step 5: Graph Proof Chain Explainer Modal (GET /api/explain)
    // =========================================================================
    let target_post_uri = items_a[0]["uri"].as_str().unwrap();
    let req_explain = Request::builder()
        .uri(format!(
            "/api/explain?viewer=did:plc:alice&uri={}",
            simple_percent_encode(target_post_uri)
        ))
        .body(Body::empty())
        .unwrap();
    let resp_explain = app.clone().oneshot(req_explain).await.unwrap();
    assert_eq!(resp_explain.status(), StatusCode::OK);

    let explain_bytes = resp_explain.into_body().collect().await.unwrap().to_bytes();
    let explain_data: serde_json::Value =
        serde_json::from_slice(&explain_bytes).expect("Explain response must be valid JSON");

    let summary = explain_data["summary"]
        .as_str()
        .expect("summary must be string");
    assert!(!summary.is_empty(), "Proof summary must not be empty");

    let steps = explain_data["steps"]
        .as_array()
        .expect("steps must be array");
    assert!(
        !steps.is_empty() && steps.len() <= 4,
        "Proof steps count must be between 1 and 4, got {}",
        steps.len()
    );

    for (idx, step) in steps.iter().enumerate() {
        assert!(
            step["step_type"].is_string(),
            "Step #{idx} missing step_type"
        );
        assert!(step["node_id"].is_string(), "Step #{idx} missing node_id");
        assert!(
            step["description"].is_string(),
            "Step #{idx} missing description"
        );
    }
}

#[tokio::test]
async fn test_empirical_adversarial_and_extreme_dial_inputs() {
    let (state, _, _, _) = create_test_fixture();
    let app = create_xrpc_router(state);

    // 1. Extreme negative and oversized freshness / discovery dials
    let extreme_urls = [
        "/api/feed-preview?freshness=0&discovery=0",
        "/api/feed-preview?freshness=-999&discovery=-5.0",
        "/api/feed-preview?freshness=1000000&discovery=100.0",
        "/api/feed-preview?art=-10.0&tech=1000.0&science=0&news=-0.5&culture=500.0",
        "/api/feed-preview?limit=0",
        "/api/feed-preview?limit=10000",
    ];

    for url in &extreme_urls {
        let req = Request::builder().uri(*url).body(Body::empty()).unwrap();
        let resp = app.clone().oneshot(req).await.unwrap();
        assert_eq!(
            resp.status(),
            StatusCode::OK,
            "Server must safely saturate and handle extreme dial input: {url}"
        );
        let bytes = resp.into_body().collect().await.unwrap().to_bytes();
        let json: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert!(json.get("items").is_some());
    }

    // 2. Taste twins with nonexistent / malformed DIDs
    let malformed_taste_urls = [
        "/api/taste-twins?did=did:plc:completely_unknown_user_9999",
        "/api/taste-twins?did=invalid_did_format",
        "/api/taste-twins?handle=nonexistent.user.test",
        "/api/taste-twins?did=%3Cscript%3Ealert(1)%3C/script%3E",
    ];

    for url in &malformed_taste_urls {
        let req = Request::builder().uri(*url).body(Body::empty()).unwrap();
        let resp = app.clone().oneshot(req).await.unwrap();
        assert_eq!(
            resp.status(),
            StatusCode::OK,
            "Nonexistent DID/handle must return empty twins gracefully (HTTP 200), url: {url}"
        );
        let bytes = resp.into_body().collect().await.unwrap().to_bytes();
        let json: serde_json::Value = serde_json::from_slice(&bytes).unwrap();
        assert_eq!(json["twins"].as_array().unwrap().len(), 0);
    }

    // 3. Missing required parameters
    // Taste twins missing all params
    let req_missing_twins = Request::builder()
        .uri("/api/taste-twins")
        .body(Body::empty())
        .unwrap();
    let resp_missing_twins = app.clone().oneshot(req_missing_twins).await.unwrap();
    assert_eq!(
        resp_missing_twins.status(),
        StatusCode::BAD_REQUEST,
        "Missing did and handle parameter must return 400 Bad Request"
    );

    // Explain missing uri param
    let req_missing_explain = Request::builder()
        .uri("/api/explain?viewer=did:plc:alice")
        .body(Body::empty())
        .unwrap();
    let resp_missing_explain = app.clone().oneshot(req_missing_explain).await.unwrap();
    assert_eq!(
        resp_missing_explain.status(),
        StatusCode::BAD_REQUEST,
        "Missing uri parameter must return 400 Bad Request"
    );

    // 4. Unsupported HTTP Methods
    let unsupported_requests = [
        (Method::POST, "/"),
        (Method::PUT, "/dashboard"),
        (Method::DELETE, "/api/telemetry"),
        (Method::PATCH, "/api/feed-preview"),
    ];

    for (method, uri) in &unsupported_requests {
        let req = Request::builder()
            .method(method.clone())
            .uri(*uri)
            .body(Body::empty())
            .unwrap();
        let resp = app.clone().oneshot(req).await.unwrap();
        assert_eq!(
            resp.status(),
            StatusCode::METHOD_NOT_ALLOWED,
            "Unsupported HTTP method {method} on {uri} must return 405 Method Not Allowed"
        );
    }
}

#[tokio::test]
async fn test_empirical_spa_dom_xss_and_injection_resilience() {
    let (state, interner, graph, _) = create_test_fixture();
    let app = create_xrpc_router(state);

    let now = BLUESKY_EPOCH_SECS + 1_000_000;

    // Inject posts with special characters, quotes, ampersands, angle brackets
    let xss_author = interner.intern("did:plc:author_with_<script>_&_\"quotes\"");
    let xss_post = interner.intern(
        "at://did:plc:author_with_<script>_&_\"quotes\"/app.bsky.feed.post/post_'onload'=alert(1)",
    );

    graph.record_post_meta(xss_post, xss_author, None, None, now - 100);

    // Query explain with XSS URI
    let req_xss_explain = Request::builder()
        .uri(format!(
            "/api/explain?viewer=did:plc:alice&uri={}",
            simple_percent_encode("at://did:plc:author_with_<script>_&_\"quotes\"/app.bsky.feed.post/post_'onload'=alert(1)")
        ))
        .body(Body::empty())
        .unwrap();

    let resp = app.oneshot(req_xss_explain).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let bytes = resp.into_body().collect().await.unwrap().to_bytes();
    let json: serde_json::Value = serde_json::from_slice(&bytes).unwrap();

    let summary = json["summary"].as_str().unwrap();
    assert!(!summary.is_empty());

    // Verify valid JSON deserialization succeeded without corrupting node IDs
    let steps = json["steps"].as_array().unwrap();
    assert!(!steps.is_empty());
}
