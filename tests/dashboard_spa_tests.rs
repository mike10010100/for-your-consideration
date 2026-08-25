#![forbid(unsafe_code)]

//! Comprehensive test suite for the Embedded Web Dashboard SPA (Milestone 3):
//! - `GET /`
//! - `GET /dashboard`
//!
//! Validates HTTP 200 OK responses, HTML5 doctype and metadata, zero external CDN dependencies,
//! presence of all 4 required components (#telemetry, #taste-twins, #dials, #feed-preview, #proof-modal),
//! CORS headers, HEAD/OPTIONS support, and high-concurrency throughput.

use std::sync::Arc;

use axum::body::Body;
use axum::http::{Method, Request, StatusCode};
use for_your_consideration::prelude::*;
use http_body_util::BodyExt;
use tower::ServiceExt;

/// Helper to create a test application state.
fn create_test_state() -> AppState {
    let interner = Arc::new(StringInterner::new());
    let graph = Arc::new(GraphStore::new());
    let recommender = Arc::new(Recommender::new(interner, graph));
    AppState::new(recommender, "did:web:feed.example.com", "feed.example.com")
}

#[tokio::test]
async fn test_dashboard_root_serves_html_with_200_ok() {
    let state = create_test_state();
    let app = create_xrpc_router(state);

    let req = Request::builder()
        .method(Method::GET)
        .uri("/")
        .body(Body::empty())
        .unwrap();

    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let content_type = resp
        .headers()
        .get("content-type")
        .expect("content-type header must be present")
        .to_str()
        .unwrap();
    assert!(
        content_type.starts_with("text/html"),
        "Expected text/html content-type, got: {content_type}"
    );

    let body = resp.into_body().collect().await.unwrap().to_bytes();
    assert!(!body.is_empty(), "Dashboard HTML body must not be empty");

    let html = String::from_utf8(body.to_vec()).expect("HTML must be valid UTF-8");
    assert!(
        html.contains("<!DOCTYPE html>"),
        "HTML must include doctype"
    );
    assert!(
        html.contains("<title>For You Feed"),
        "HTML must include title"
    );
}

#[tokio::test]
async fn test_dashboard_alias_serves_identical_html() {
    let state = create_test_state();
    let app = create_xrpc_router(state);

    // Request /
    let req_root = Request::builder().uri("/").body(Body::empty()).unwrap();
    let resp_root = app.clone().oneshot(req_root).await.unwrap();
    let body_root = resp_root.into_body().collect().await.unwrap().to_bytes();

    // Request /dashboard
    let req_dash = Request::builder()
        .uri("/dashboard")
        .body(Body::empty())
        .unwrap();
    let resp_dash = app.oneshot(req_dash).await.unwrap();
    assert_eq!(resp_dash.status(), StatusCode::OK);
    let body_dash = resp_dash.into_body().collect().await.unwrap().to_bytes();

    assert_eq!(
        body_root, body_dash,
        "GET / and GET /dashboard must return identical HTML content"
    );
}

#[tokio::test]
async fn test_dashboard_spa_html_structure_and_four_main_components() {
    let state = create_test_state();
    let app = create_xrpc_router(state);

    let req = Request::builder().uri("/").body(Body::empty()).unwrap();
    let resp = app.oneshot(req).await.unwrap();
    let body = resp.into_body().collect().await.unwrap().to_bytes();
    let html = String::from_utf8(body.to_vec()).unwrap();

    // HTML5 Standards & Meta Tags
    assert!(html.contains("<meta charset=\"UTF-8\">"));
    assert!(html.contains("<meta name=\"viewport\""));
    assert!(html.contains("<style>"));
    assert!(html.contains("<script>"));

    // 4 Main Components Required by Milestone 3:
    // Component 1: Live Graph Telemetry Dashboard
    assert!(
        html.contains("id=\"telemetry\""),
        "Must contain #telemetry section"
    );
    // Component 2: Handle / DID Taste Twins Explorer
    assert!(
        html.contains("id=\"taste-twins\""),
        "Must contain #taste-twins section"
    );
    // Component 3: Live Algorithmic Dials & Feed Preview
    assert!(html.contains("id=\"dials\""), "Must contain #dials section");
    assert!(
        html.contains("id=\"feed-preview\""),
        "Must contain #feed-preview section"
    );
    // Component 4: Graph Proof Chain Explainer Modal
    assert!(
        html.contains("id=\"proof-modal\""),
        "Must contain #proof-modal element"
    );
}

#[tokio::test]
async fn test_dashboard_telemetry_component_elements() {
    let state = create_test_state();
    let app = create_xrpc_router(state);

    let req = Request::builder().uri("/").body(Body::empty()).unwrap();
    let resp = app.oneshot(req).await.unwrap();
    let body = resp.into_body().collect().await.unwrap().to_bytes();
    let html = String::from_utf8(body.to_vec()).unwrap();

    // Telemetry IDs
    assert!(html.contains("id=\"stat-total-edges\""));
    assert!(html.contains("id=\"stat-interned-strings\""));
    assert!(html.contains("id=\"stat-total-nodes\""));
    assert!(html.contains("id=\"stat-users-count\""));
    assert!(html.contains("id=\"stat-posts-count\""));
    assert!(html.contains("id=\"stat-ingestion-velocity\""));
    assert!(html.contains("id=\"stat-events-processed\""));
    assert!(html.contains("id=\"stat-snapshot-status\""));
    assert!(html.contains("id=\"stat-snapshot-load-ms\""));
    assert!(html.contains("id=\"stat-snapshot-size\""));
    assert!(html.contains("id=\"uptime-display\""));
    assert!(html.contains("id=\"telemetry-velocity-badge\""));
    assert!(html.contains("id=\"velocity-sparkline\""));
    assert!(html.contains("id=\"btn-toggle-polling\""));
}

#[tokio::test]
async fn test_dashboard_taste_twins_component_elements() {
    let state = create_test_state();
    let app = create_xrpc_router(state);

    let req = Request::builder().uri("/").body(Body::empty()).unwrap();
    let resp = app.oneshot(req).await.unwrap();
    let body = resp.into_body().collect().await.unwrap().to_bytes();
    let html = String::from_utf8(body.to_vec()).unwrap();

    // Taste Twins Explorer Search & Results
    assert!(html.contains("id=\"taste-twins-input\""));
    assert!(html.contains("id=\"taste-twins-btn\""));
    assert!(html.contains("id=\"taste-twins-results\""));
    assert!(html.contains("setTasteTwinsQuery"));
}

#[tokio::test]
async fn test_dashboard_algorithmic_dials_and_feed_preview_elements() {
    let state = create_test_state();
    let app = create_xrpc_router(state);

    let req = Request::builder().uri("/").body(Body::empty()).unwrap();
    let resp = app.oneshot(req).await.unwrap();
    let body = resp.into_body().collect().await.unwrap().to_bytes();
    let html = String::from_utf8(body.to_vec()).unwrap();

    // Dials Inputs & Badges
    assert!(html.contains("id=\"dial-viewer-input\""));
    assert!(html.contains("id=\"slider-freshness\""));
    assert!(html.contains("id=\"badge-freshness\""));
    assert!(html.contains("id=\"slider-discovery\""));
    assert!(html.contains("id=\"badge-discovery\""));

    // 5 Topic Multipliers
    assert!(html.contains("id=\"slider-topic-art\""));
    assert!(html.contains("id=\"badge-topic-art\""));
    assert!(html.contains("id=\"slider-topic-tech\""));
    assert!(html.contains("id=\"badge-topic-tech\""));
    assert!(html.contains("id=\"slider-topic-science\""));
    assert!(html.contains("id=\"badge-topic-science\""));
    assert!(html.contains("id=\"slider-topic-news\""));
    assert!(html.contains("id=\"badge-topic-news\""));
    assert!(html.contains("id=\"slider-topic-culture\""));
    assert!(html.contains("id=\"badge-topic-culture\""));
    assert!(html.contains("id=\"btn-reset-dials\""));

    // Feed Preview
    assert!(html.contains("id=\"feed-preview-latency\""));
    assert!(html.contains("id=\"feed-candidate-count\""));
    assert!(html.contains("id=\"feed-preview-items\""));
}

#[tokio::test]
async fn test_dashboard_proof_modal_component_elements() {
    let state = create_test_state();
    let app = create_xrpc_router(state);

    let req = Request::builder().uri("/").body(Body::empty()).unwrap();
    let resp = app.oneshot(req).await.unwrap();
    let body = resp.into_body().collect().await.unwrap().to_bytes();
    let html = String::from_utf8(body.to_vec()).unwrap();

    // Proof Modal IDs & Visualization
    assert!(html.contains("id=\"proof-modal\""));
    assert!(html.contains("id=\"proof-modal-content\""));
    assert!(html.contains("id=\"proof-modal-close\""));
    assert!(html.contains("id=\"proof-chain-summary\""));
    assert!(html.contains("id=\"proof-chain-svg\""));
    assert!(html.contains("id=\"proof-chain-steps\""));
    assert!(html.contains("openProofExplainer"));
}

#[tokio::test]
async fn test_dashboard_zero_external_dependencies_offline() {
    let state = create_test_state();
    let app = create_xrpc_router(state);

    let req = Request::builder().uri("/").body(Body::empty()).unwrap();
    let resp = app.oneshot(req).await.unwrap();
    let body = resp.into_body().collect().await.unwrap().to_bytes();
    let html = String::from_utf8(body.to_vec()).unwrap();

    // Verify zero external CDN links / scripts / fonts
    let external_cdn_indicators = [
        "src=\"http",
        "href=\"http",
        "url(\"http",
        "url('http",
        "cdn.jsdelivr.net",
        "unpkg.com",
        "cdnjs.cloudflare.com",
        "fonts.googleapis.com",
        "fonts.gstatic.com",
        "code.jquery.com",
        "stackpath.bootstrapcdn.com",
    ];

    for cdn in &external_cdn_indicators {
        assert!(
            !html.contains(cdn),
            "Dashboard must be 100% offline & zero-dependency; found external reference: {cdn}"
        );
    }
}

#[tokio::test]
async fn test_dashboard_cors_and_http_methods() {
    let state = create_test_state();
    let app = create_xrpc_router(state);

    // OPTIONS preflight request
    let req_options = Request::builder()
        .method(Method::OPTIONS)
        .uri("/")
        .header("Origin", "http://example.com")
        .header("Access-Control-Request-Method", "GET")
        .body(Body::empty())
        .unwrap();

    let resp_options = app.clone().oneshot(req_options).await.unwrap();
    assert_eq!(resp_options.status(), StatusCode::OK);
    assert_eq!(
        resp_options
            .headers()
            .get("access-control-allow-origin")
            .unwrap(),
        "*"
    );

    // HEAD request
    let req_head = Request::builder()
        .method(Method::HEAD)
        .uri("/")
        .body(Body::empty())
        .unwrap();

    let resp_head = app.oneshot(req_head).await.unwrap();
    assert_eq!(resp_head.status(), StatusCode::OK);
    assert!(resp_head
        .headers()
        .get("content-type")
        .unwrap()
        .to_str()
        .unwrap()
        .starts_with("text/html"));
}

#[tokio::test]
async fn test_dashboard_concurrent_requests_throughput() {
    let state = create_test_state();
    let router = create_xrpc_router(state);

    let mut handles = Vec::new();
    for i in 0..50 {
        let app = router.clone();
        let handle = tokio::spawn(async move {
            let uri = if i % 2 == 0 { "/" } else { "/dashboard" };
            let req = Request::builder().uri(uri).body(Body::empty()).unwrap();
            let resp = app.oneshot(req).await.unwrap();
            assert_eq!(resp.status(), StatusCode::OK);
            let body = resp.into_body().collect().await.unwrap().to_bytes();
            assert!(!body.is_empty());
        });
        handles.push(handle);
    }

    for h in handles {
        h.await.unwrap();
    }
}
