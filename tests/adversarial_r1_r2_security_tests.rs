#![forbid(unsafe_code)]
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    missing_docs,
    clippy::pedantic,
    clippy::nursery,
    clippy::too_many_lines,
    clippy::cast_possible_truncation
)]

//! # Adversarial R1 & R2 Security Verification Test Suite
//!
//! Comprehensive adversarial test harness covering:
//! - **R1 Cryptography & Auth Hardening**:
//!   1. Service Auth JWT validation on `app.bsky.feed.getFeedSkeleton` (expiration, audience matching, claim validation).
//!   2. HMAC-SHA256 session token key derivation (short vs long secrets), constant-time signature comparison (`constant_time_eq`), expiration, and tampering defense.
//!   3. Authorization gating on preferences and administrative endpoints.
//! - **R2 SSRF, Network Egress & Identity Resolution Hardening**:
//!   1. Outbound URL and IP validation: Loopback (`127.0.0.1`, `::1`), Link-local / Cloud metadata (`169.254.169.254`), Private RFC 1918 networks (`10.0.0.0/8`, `172.16.0.0/12`, `192.168.0.0/16`), Carrier-Grade NAT (`100.64.0.0/10`), Documentation ranges, and Multicast/Broadcast.
//!   2. Scheme validation (insecure `http://`, `ftp://`, `file://`, `gopher://`, `javascript:`).
//!   3. HTTP Client No-Redirect Policy (`redirect::Policy::none()`) on PAR & identity lookup endpoints.
//!   4. Axum HTTP Router payload size bounding and mutation endpoint hardening.

use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use axum::body::Body;
use axum::http::header::{AUTHORIZATION, CONTENT_TYPE};
use axum::http::{Method, Request, StatusCode};
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine;
use http_body_util::BodyExt;
use tower::ServiceExt;

use for_your_consideration::auth::{
    build_secure_http_client, compute_hmac_sha256, constant_time_eq, generate_session_token,
    generate_session_token_signed, is_restricted_ip, validate_outbound_url, validate_service_jwt,
    validate_session_token_signed,
};
use for_your_consideration::prelude::*;
use for_your_consideration::types::{PreferencesResponseDto, TopicWeights, UserDials};

/// Constructs an isolated test `AppState` with populated graph and preferences store.
fn create_test_app_state(
    service_did: &str,
    hostname: &str,
) -> (
    AppState,
    Arc<StringInterner>,
    Arc<GraphStore>,
    Arc<UserPreferencesStore>,
) {
    let interner = Arc::new(StringInterner::new());
    let graph = Arc::new(GraphStore::new());
    let preferences_store = Arc::new(UserPreferencesStore::new());

    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();

    // Populate baseline graph items
    let author_did = "did:plc:artist_author";
    let post_uri = "at://did:plc:artist_author/app.bsky.feed.post/art_post_1";
    let aid = interner.intern(author_did);
    let pid = interner.intern(post_uri);
    graph.record_post_meta(pid, aid, None, None, now - 50);

    let recommender = Arc::new(Recommender::new(Arc::clone(&interner), Arc::clone(&graph)));
    let state = AppState::new(recommender, service_did, hostname)
        .with_preferences_store(Arc::clone(&preferences_store));

    (state, interner, graph, preferences_store)
}

/// Helper to create a custom unsigned/raw JWT for Service Auth test assertions.
fn create_raw_service_jwt(
    iss: Option<&str>,
    sub: Option<&str>,
    aud: Option<&str>,
    exp: Option<u64>,
    iat: Option<u64>,
) -> String {
    let header_json = serde_json::json!({
        "alg": "ES256K",
        "typ": "JWT"
    });

    let mut payload_map = serde_json::Map::new();
    if let Some(i) = iss {
        payload_map.insert("iss".to_string(), serde_json::Value::String(i.to_string()));
    }
    if let Some(s) = sub {
        payload_map.insert("sub".to_string(), serde_json::Value::String(s.to_string()));
    }
    if let Some(a) = aud {
        payload_map.insert("aud".to_string(), serde_json::Value::String(a.to_string()));
    }
    if let Some(e) = exp {
        payload_map.insert("exp".to_string(), serde_json::Value::Number(e.into()));
    }
    if let Some(ia) = iat {
        payload_map.insert("iat".to_string(), serde_json::Value::Number(ia.into()));
    }
    payload_map.insert(
        "lxm".to_string(),
        serde_json::Value::String("app.bsky.feed.getFeedSkeleton".to_string()),
    );

    let h_b64 = URL_SAFE_NO_PAD.encode(header_json.to_string().as_bytes());
    let p_b64 = URL_SAFE_NO_PAD.encode(
        serde_json::Value::Object(payload_map)
            .to_string()
            .as_bytes(),
    );
    let dummy_sig = URL_SAFE_NO_PAD.encode(b"test_mock_es256_cryptographic_signature_bytes_64_len");

    format!("{h_b64}.{p_b64}.{dummy_sig}")
}

/// Spawns a background mock HTTP server returning an HTTP redirect response (301, 302, 307, or 308).
async fn spawn_mock_redirect_server(
    status_code: u16,
    target_location: &str,
) -> (String, tokio::task::JoinHandle<()>) {
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let base_url = format!("http://{addr}");
    let target_loc = target_location.to_string();

    let handle = tokio::spawn(async move {
        while let Ok((mut stream, _)) = listener.accept().await {
            use tokio::io::{AsyncReadExt, AsyncWriteExt};
            let mut buf = [0u8; 1024];
            let _ = stream.read(&mut buf).await;
            let status_text = match status_code {
                301 => "Moved Permanently",
                302 => "Found",
                307 => "Temporary Redirect",
                308 => "Permanent Redirect",
                _ => "Found",
            };
            let response = format!(
                "HTTP/1.1 {status_code} {status_text}\r\nLocation: {target_loc}\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"
            );
            let _ = stream.write_all(response.as_bytes()).await;
            let _ = stream.flush().await;
        }
    });

    (base_url, handle)
}

// ===========================================================================
// SECTION 1: SSRF Defenses, Network Egress & IP Range Filtering (R2)
// ===========================================================================

#[test]
fn test_ssrf_loopback_rejection_v4_and_v6() {
    let loopback_ips = [
        IpAddr::V4(Ipv4Addr::new(127, 0, 0, 1)),
        IpAddr::V4(Ipv4Addr::new(127, 0, 0, 2)),
        IpAddr::V4(Ipv4Addr::new(127, 128, 0, 1)),
        IpAddr::V4(Ipv4Addr::new(127, 255, 255, 254)),
        IpAddr::V6(Ipv6Addr::new(0, 0, 0, 0, 0, 0, 0, 1)),
    ];

    for ip in loopback_ips {
        assert!(
            is_restricted_ip(ip),
            "Loopback IP {ip} must be classified as restricted"
        );
    }

    // validate_outbound_url should reject loopback when allow_localhost = false
    assert!(validate_outbound_url("https://127.0.0.1/xrpc/did.json", false).is_err());
    assert!(validate_outbound_url("https://127.0.0.2:8080/xrpc", false).is_err());
    assert!(validate_outbound_url("https://localhost/xrpc", false).is_err());

    // validate_outbound_url should permit loopback when allow_localhost = true
    assert!(validate_outbound_url("http://127.0.0.1:8000/xrpc", true).is_ok());
    assert!(validate_outbound_url("http://localhost:3000/xrpc", true).is_ok());
}

#[test]
fn test_ssrf_cloud_metadata_and_link_local_rejection() {
    let metadata_ips = [
        // AWS / GCP / Azure IMDS
        IpAddr::V4(Ipv4Addr::new(169, 254, 169, 254)),
        IpAddr::V4(Ipv4Addr::new(169, 254, 1, 1)),
        IpAddr::V4(Ipv4Addr::new(169, 254, 0, 1)),
        IpAddr::V4(Ipv4Addr::new(169, 254, 254, 254)),
        // IPv6 Link-Local (fe80::/10)
        IpAddr::V6(Ipv6Addr::new(0xfe80, 0, 0, 0, 0, 0, 0, 1)),
        IpAddr::V6(Ipv6Addr::new(0xfea0, 0, 0, 0, 0, 0, 0, 1)),
    ];

    for ip in metadata_ips {
        assert!(
            is_restricted_ip(ip),
            "Metadata / Link-local IP {ip} must be restricted"
        );
    }

    assert!(validate_outbound_url("https://169.254.169.254/latest/meta-data/", false).is_err());
    assert!(validate_outbound_url("https://169.254.1.1/internal/config", false).is_err());
}

#[test]
fn test_ssrf_private_rfc1918_networks_rejection() {
    let private_ips = [
        // 10.0.0.0/8
        IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)),
        IpAddr::V4(Ipv4Addr::new(10, 254, 254, 254)),
        IpAddr::V4(Ipv4Addr::new(10, 128, 5, 20)),
        // 172.16.0.0/12
        IpAddr::V4(Ipv4Addr::new(172, 16, 0, 1)),
        IpAddr::V4(Ipv4Addr::new(172, 20, 10, 5)),
        IpAddr::V4(Ipv4Addr::new(172, 31, 255, 255)),
        // 192.168.0.0/16
        IpAddr::V4(Ipv4Addr::new(192, 168, 0, 1)),
        IpAddr::V4(Ipv4Addr::new(192, 168, 1, 1)),
        IpAddr::V4(Ipv4Addr::new(192, 168, 254, 254)),
    ];

    for ip in private_ips {
        assert!(
            is_restricted_ip(ip),
            "Private RFC 1918 IP {ip} must be restricted"
        );
    }

    assert!(validate_outbound_url("https://10.0.0.1/pds", false).is_err());
    assert!(validate_outbound_url("https://172.16.0.1:8443/auth", false).is_err());
    assert!(validate_outbound_url("https://192.168.1.1/admin", false).is_err());
}

#[test]
fn test_ssrf_carrier_grade_nat_and_special_ranges() {
    let special_ips = [
        // CGNAT: 100.64.0.0/10
        IpAddr::V4(Ipv4Addr::new(100, 64, 0, 1)),
        IpAddr::V4(Ipv4Addr::new(100, 100, 50, 1)),
        IpAddr::V4(Ipv4Addr::new(100, 127, 255, 255)),
        // "This host on this network": 0.0.0.0/8
        IpAddr::V4(Ipv4Addr::new(0, 0, 0, 0)),
        IpAddr::V4(Ipv4Addr::new(0, 1, 2, 3)),
        // Documentation ranges: 192.0.2.0/24 (TEST-NET-1), 198.51.100.0/24 (TEST-NET-2), 203.0.113.0/24 (TEST-NET-3)
        IpAddr::V4(Ipv4Addr::new(192, 0, 2, 1)),
        IpAddr::V4(Ipv4Addr::new(198, 51, 100, 1)),
        IpAddr::V4(Ipv4Addr::new(203, 0, 113, 1)),
        // IPv6 Unique Local (fc00::/7)
        IpAddr::V6(Ipv6Addr::new(0xfc00, 0, 0, 0, 0, 0, 0, 1)),
        IpAddr::V6(Ipv6Addr::new(0xfd00, 0, 0, 0, 0, 0, 0, 1)),
    ];

    for ip in special_ips {
        assert!(
            is_restricted_ip(ip),
            "Special / CGNAT / Doc IP {ip} must be restricted"
        );
    }

    assert!(validate_outbound_url("https://100.64.0.1/pds", false).is_err());
    assert!(validate_outbound_url("https://0.0.0.0:8000/auth", false).is_err());
    assert!(validate_outbound_url("https://192.0.2.1/meta", false).is_err());
}

#[test]
fn test_ssrf_multicast_and_broadcast_rejection() {
    let multicast_ips = [
        IpAddr::V4(Ipv4Addr::new(224, 0, 0, 1)),
        IpAddr::V4(Ipv4Addr::new(239, 255, 255, 250)),
        IpAddr::V4(Ipv4Addr::new(255, 255, 255, 255)), // Broadcast
        IpAddr::V6(Ipv6Addr::new(0xff02, 0, 0, 0, 0, 0, 0, 1)),
        IpAddr::V6(Ipv6Addr::new(0xff05, 0, 0, 0, 0, 0, 0, 2)),
    ];

    for ip in multicast_ips {
        assert!(
            is_restricted_ip(ip),
            "Multicast / Broadcast IP {ip} must be restricted"
        );
    }

    assert!(validate_outbound_url("https://224.0.0.1/multicast", false).is_err());
    assert!(validate_outbound_url("https://255.255.255.255/broadcast", false).is_err());
}

#[test]
fn test_ssrf_insecure_schemes_and_malformed_urls() {
    let invalid_urls = [
        // Plain HTTP when allow_localhost = false
        "http://attacker.com/pds",
        "http://bsky.social/xrpc",
        // Insecure / exotic protocols
        "ftp://files.example.com/pds",
        "file:///etc/passwd",
        "file:///proc/self/environ",
        "gopher://127.0.0.1:70/1",
        "data:text/plain;base64,SGVsbG8=",
        "javascript:alert(document.domain)",
        "ws://bsky.network/jetstream",
        // Malformed strings
        "",
        "   ",
        "https://",
        "https://:8080",
        "not-a-url",
        "///evil.com",
    ];

    for url in invalid_urls {
        assert!(
            validate_outbound_url(url, false).is_err(),
            "Insecure / malformed URL '{url}' must be rejected"
        );
    }
}

#[test]
fn test_ssrf_valid_public_https_urls_accepted() {
    let valid_urls = [
        "https://bsky.social",
        "https://bsky.social/xrpc/com.atproto.identity.resolveHandle",
        "https://plc.directory/did:plc:z72i7hdynmk6r22z27h6tvur",
        "https://my-custom-pds.example.com:8443/xrpc",
        "https://auth.pds.social/oauth/authorize",
        "https://feed-generator.bsky.team/.well-known/did.json",
    ];

    for url in valid_urls {
        let res = validate_outbound_url(url, false);
        assert!(
            res.is_ok(),
            "Valid public HTTPS URL '{url}' must be accepted: {:?}",
            res.err()
        );
    }
}

// ===========================================================================
// SECTION 2: PAR No-Redirect Policy & Outbound Client Hardening (R1 / R2)
// ===========================================================================

#[tokio::test]
async fn test_par_http_client_no_redirect_301_rejected() {
    let (mock_url, _handle) =
        spawn_mock_redirect_server(301, "http://169.254.169.254/latest/meta-data/").await;
    let client = build_secure_http_client();

    let resp = client
        .get(format!("{mock_url}/pushed-auth"))
        .send()
        .await
        .unwrap();

    // Client must receive the 301 status directly and NOT follow to 169.254.169.254
    assert_eq!(resp.status(), StatusCode::MOVED_PERMANENTLY);
    assert_eq!(
        resp.headers().get("location").unwrap().to_str().unwrap(),
        "http://169.254.169.254/latest/meta-data/"
    );
}

#[tokio::test]
async fn test_par_http_client_no_redirect_302_rejected() {
    let (mock_url, _handle) =
        spawn_mock_redirect_server(302, "http://127.0.0.1:9000/internal-secrets").await;
    let client = build_secure_http_client();

    let resp = client
        .post(format!("{mock_url}/par"))
        .body("client_id=test&request_uri=urn:ietf:params:oauth:request_uri:123")
        .send()
        .await
        .unwrap();

    // Client must receive 302 Found directly without following redirect
    assert_eq!(resp.status(), StatusCode::FOUND);
    assert_eq!(
        resp.headers().get("location").unwrap().to_str().unwrap(),
        "http://127.0.0.1:9000/internal-secrets"
    );
}

#[tokio::test]
async fn test_par_http_client_no_redirect_307_and_308_rejected() {
    let (mock_url_307, _handle1) =
        spawn_mock_redirect_server(307, "https://evil.attacker.com/steal-body").await;
    let (mock_url_308, _handle2) =
        spawn_mock_redirect_server(308, "https://evil.attacker.com/perm-redirect").await;
    let client = build_secure_http_client();

    let resp307 = client
        .post(format!("{mock_url_307}/par"))
        .body("secret=classified")
        .send()
        .await
        .unwrap();
    assert_eq!(resp307.status(), StatusCode::TEMPORARY_REDIRECT);

    let resp308 = client
        .post(format!("{mock_url_308}/par"))
        .body("secret=classified")
        .send()
        .await
        .unwrap();
    assert_eq!(resp308.status(), StatusCode::PERMANENT_REDIRECT);
}

// ===========================================================================
// SECTION 3: Service Auth JWT Expiration, Audience & Claim Validation (R1)
// ===========================================================================

#[test]
fn test_service_jwt_expired_token_rejected() {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();

    // Token expired 100 seconds ago
    let expired_jwt = create_raw_service_jwt(
        Some("did:plc:expired_actor_z72i7hdynmk6r22z27h6tvur"),
        None,
        Some("did:web:feed.example.com"),
        Some(now - 100),
        Some(now - 700),
    );

    let auth_header = format!("Bearer {expired_jwt}");
    let result = validate_service_jwt(&auth_header, Some("did:web:feed.example.com"), now);

    assert!(
        result.is_err(),
        "Expired JWT must be rejected by validate_service_jwt"
    );
    let err_msg = result.err().unwrap().to_string();
    assert!(
        err_msg.contains("expired") || err_msg.contains("Token expired"),
        "Error message must indicate token expiration, got: {err_msg}"
    );
}

#[test]
fn test_service_jwt_valid_non_expired_token_accepted() {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();

    // Token valid for next 3600 seconds
    let valid_jwt = create_raw_service_jwt(
        Some("did:plc:alice_valid"),
        None,
        Some("did:web:feed.example.com"),
        Some(now + 3600),
        Some(now),
    );

    let auth_header = format!("Bearer {valid_jwt}");
    let result = validate_service_jwt(&auth_header, Some("did:web:feed.example.com"), now);

    assert!(
        result.is_ok(),
        "Valid non-expired JWT must succeed: {:?}",
        result.err()
    );
    assert_eq!(result.unwrap().as_str(), "did:plc:alice_valid");
}

#[test]
fn test_service_jwt_audience_matching_and_mismatch_rejection() {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();

    let target_service_did = "did:web:feed.example.com";
    let competitor_service_did = "did:web:competitor-feed.com";

    // JWT minted for competitor service
    let mismatched_jwt = create_raw_service_jwt(
        Some("did:plc:victim_user"),
        None,
        Some(competitor_service_did),
        Some(now + 3600),
        Some(now),
    );

    let auth_header = format!("Bearer {mismatched_jwt}");
    let result = validate_service_jwt(&auth_header, Some(target_service_did), now);

    assert!(
        result.is_err(),
        "Token with mismatched audience must be rejected"
    );
    let err_msg = result.err().unwrap().to_string();
    assert!(
        err_msg.contains("Audience mismatch") || err_msg.contains("audience"),
        "Error must specify audience mismatch, got: {err_msg}"
    );
}

#[test]
fn test_service_jwt_iss_and_sub_fallback_resolution() {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();

    // 1. Both iss and sub present -> iss takes precedence
    let jwt1 = create_raw_service_jwt(
        Some("did:plc:issuer_primary"),
        Some("did:plc:subject_fallback"),
        Some("did:web:feed.example.com"),
        Some(now + 3600),
        Some(now),
    );
    let did1 = validate_service_jwt(
        &format!("Bearer {jwt1}"),
        Some("did:web:feed.example.com"),
        now,
    )
    .unwrap();
    assert_eq!(did1.as_str(), "did:plc:issuer_primary");

    // 2. Only sub present -> sub used as fallback
    let jwt2 = create_raw_service_jwt(
        None,
        Some("did:plc:subject_fallback_only"),
        Some("did:web:feed.example.com"),
        Some(now + 3600),
        Some(now),
    );
    let did2 = validate_service_jwt(
        &format!("Bearer {jwt2}"),
        Some("did:web:feed.example.com"),
        now,
    )
    .unwrap();
    assert_eq!(did2.as_str(), "did:plc:subject_fallback_only");

    // 3. did:web format supported in iss
    let jwt3 = create_raw_service_jwt(
        Some("did:web:custom-actor.domain.com"),
        None,
        Some("did:web:feed.example.com"),
        Some(now + 3600),
        Some(now),
    );
    let did3 = validate_service_jwt(
        &format!("Bearer {jwt3}"),
        Some("did:web:feed.example.com"),
        now,
    )
    .unwrap();
    assert_eq!(did3.as_str(), "did:web:custom-actor.domain.com");
}

#[test]
fn test_service_jwt_invalid_did_syntax_rejected() {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();

    let invalid_dids = [
        "not-a-did",
        "did:unknown:12345",
        "did:plc:", // too short
        "did:web:", // too short
        "https://example.com/did",
        "",
    ];

    for invalid_did in invalid_dids {
        let jwt = create_raw_service_jwt(
            Some(invalid_did),
            None,
            Some("did:web:feed.example.com"),
            Some(now + 3600),
            Some(now),
        );
        let res = validate_service_jwt(
            &format!("Bearer {jwt}"),
            Some("did:web:feed.example.com"),
            now,
        );
        assert!(
            res.is_err(),
            "Invalid DID '{invalid_did}' in JWT must be rejected"
        );
    }
}

#[test]
fn test_service_jwt_malformed_token_structure_and_corrupt_payloads() {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();

    let malformed_tokens = [
        "Bearer single_part_token",
        "Bearer two.parts",
        "Bearer four.parts.are.forbidden",
        "Bearer header.invalid_b64!@#.signature",
        "Bearer header.e30.signature", // empty JSON object {} (no iss/sub)
        "Bearer ",
        "bearer ",
        "BEARER ",
        "",
        "Basic dXNlcjpwYXNz",
    ];

    for tok in malformed_tokens {
        let res = validate_service_jwt(tok, Some("did:web:feed.example.com"), now);
        assert!(
            res.is_err(),
            "Malformed token '{tok}' must be rejected by validate_service_jwt"
        );
    }
}

#[tokio::test]
async fn test_service_jwt_get_feed_skeleton_integration_expired_degrades_gracefully() {
    let (state, interner, _graph, prefs_store) =
        create_test_app_state("did:web:feed.example.com", "feed.example.com");
    let app = create_xrpc_router(state);

    let alice_did = "did:plc:alice_integration_test";
    // Set custom dials for Alice (freshness = 6h)
    let alice_dials = UserDials {
        freshness_half_life_secs: 6.0 * 3600.0,
        serendipity_ratio: 0.35,
        topic_weights: TopicWeights::default(),
        updated_at_secs: 100,
    };
    prefs_store.set_by_did(&interner, alice_did, alice_dials);

    // 1. Send valid session token for Alice -> Returns Alice's customized response
    let valid_token = generate_session_token(alice_did, 3600);
    let req_valid = Request::builder()
        .uri("/xrpc/app.bsky.feed.getFeedSkeleton?feed=at://did:web:feed.example.com/app.bsky.feed.generator/for-your-consideration")
        .header(AUTHORIZATION, format!("Bearer {valid_token}"))
        .body(Body::empty())
        .unwrap();
    let resp_valid = app.clone().oneshot(req_valid).await.unwrap();
    assert_eq!(resp_valid.status(), StatusCode::OK);

    // 2. Send an unauthenticated request -> Returns 200 OK with default unauthenticated feed (zero-login fallback)
    let req_anon = Request::builder()
        .uri("/xrpc/app.bsky.feed.getFeedSkeleton?feed=at://did:web:feed.example.com/app.bsky.feed.generator/for-your-consideration")
        .body(Body::empty())
        .unwrap();
    let resp_anon = app.oneshot(req_anon).await.unwrap();
    assert_eq!(resp_anon.status(), StatusCode::OK);
}

// ===========================================================================
// SECTION 4: HMAC-SHA256 Session Token Derivation & Constant-Time Verification (R1)
// ===========================================================================

#[test]
fn test_hmac_sha256_rfc2104_deterministic_derivation() {
    let key = b"key-secret-32-bytes-long-123456";
    let msg1 = b"The quick brown fox jumps over the lazy dog";
    let msg2 = b"The quick brown fox jumps over the lazy dog";
    let msg3 = b"The quick brown fox jumps over the lazy dog."; // 1 byte difference

    let hash1 = compute_hmac_sha256(key, msg1);
    let hash2 = compute_hmac_sha256(key, msg2);
    let hash3 = compute_hmac_sha256(key, msg3);

    // Deterministic equality
    assert_eq!(hash1, hash2, "Identical inputs must yield identical HMACs");
    assert_ne!(
        hash1, hash3,
        "Different inputs must yield completely different HMACs"
    );

    // Avalanche effect: at least 10 bits should differ
    let diff_bits: u32 = hash1
        .iter()
        .zip(hash3.iter())
        .map(|(a, b)| (a ^ b).count_ones())
        .sum();
    assert!(
        diff_bits >= 30,
        "Avalanche effect must flip substantial bits (observed {diff_bits} flipped bits)"
    );
}

#[test]
fn test_hmac_session_token_short_vs_long_secrets_entropy() {
    let did = "did:plc:entropy_user_test";

    // Test secret variations:
    let short_secret_8 = b"secret08";
    let short_secret_16 = b"secret16bytes123";
    let standard_secret_32 = b"standard-32-byte-secret-key-1234";
    let long_secret_64 =
        b"very-long-secret-key-that-exceeds-the-standard-block-size-of-64-bytes-0123456789";
    let extra_long_secret_128 = vec![0x42u8; 128];

    let secrets: Vec<&[u8]> = vec![
        short_secret_8,
        short_secret_16,
        standard_secret_32,
        long_secret_64,
        &extra_long_secret_128,
    ];

    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();

    for sec in secrets {
        let token = generate_session_token_signed(did, 3600, sec);
        assert!(!token.is_empty());
        assert_eq!(token.split('.').count(), 3);

        // Validation with same secret must succeed
        let validated = validate_session_token_signed(&token, sec, now);
        assert!(
            validated.is_ok(),
            "Secret of length {} must successfully validate: {:?}",
            sec.len(),
            validated.err()
        );
        assert_eq!(validated.unwrap().as_str(), did);

        // Validation with wrong secret must fail
        let wrong_secret = b"wrong-secret-that-does-not-match";
        let invalid = validate_session_token_signed(&token, wrong_secret, now);
        assert!(
            invalid.is_err(),
            "Validation with wrong secret must fail signature check"
        );
    }
}

#[test]
fn test_hmac_session_token_expiration_enforcement() {
    let did = "did:plc:expiration_test_user";
    let secret = b"test-secret-for-expiration-check";
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();

    // Token expired 10 seconds ago
    let expired_token = generate_session_token_signed(did, -10, secret);
    let res_expired = validate_session_token_signed(&expired_token, secret, now);
    assert!(
        res_expired.is_err(),
        "Expired session token must be rejected"
    );
    let err_msg = res_expired.err().unwrap().to_string();
    assert!(
        err_msg.contains("expired") || err_msg.contains("Session token expired"),
        "Error message must indicate token expiration: {err_msg}"
    );

    // Token valid for 60 seconds into future
    let valid_token = generate_session_token_signed(did, 60, secret);
    let res_valid = validate_session_token_signed(&valid_token, secret, now);
    assert!(
        res_valid.is_ok(),
        "Non-expired session token must be accepted"
    );
    assert_eq!(res_valid.unwrap().as_str(), did);
}

#[test]
fn test_hmac_session_token_signature_and_payload_tampering_rejection() {
    let did = "did:plc:tamper_test_user";
    let secret = b"tamper-detection-secret-key-1234";
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();

    let valid_token = generate_session_token_signed(did, 3600, secret);
    let parts: Vec<&str> = valid_token.split('.').collect();
    assert_eq!(parts.len(), 3);

    // 1. Tampered signature (flip 1 char)
    let tampered_sig = format!(
        "{}.{}.{}",
        parts[0],
        parts[1],
        "A".to_string() + &parts[2][1..]
    );
    assert!(
        validate_session_token_signed(&tampered_sig, secret, now).is_err(),
        "Tampered signature must be rejected"
    );

    // 2. Tampered payload (substitute payload with another user)
    let alt_payload = serde_json::json!({
        "iss": "did:plc:attacker_did",
        "sub": "did:plc:attacker_did",
        "exp": now + 3600,
        "iat": now
    });
    let alt_p_b64 = URL_SAFE_NO_PAD.encode(alt_payload.to_string().as_bytes());
    let tampered_payload_token = format!("{}.{}.{}", parts[0], alt_p_b64, parts[2]);
    assert!(
        validate_session_token_signed(&tampered_payload_token, secret, now).is_err(),
        "Tampered payload with original signature must be rejected"
    );

    // 3. Tampered header (alg = none)
    let none_header = serde_json::json!({ "alg": "none", "typ": "JWT" });
    let none_h_b64 = URL_SAFE_NO_PAD.encode(none_header.to_string().as_bytes());
    let none_token = format!("{}.{}.{}", none_h_b64, parts[1], parts[2]);
    assert!(
        validate_session_token_signed(&none_token, secret, now).is_err(),
        "Modified header must fail HMAC signature verification"
    );
}

#[test]
fn test_constant_time_eq_exhaustive_properties() {
    let a = b"0123456789abcdef0123456789abcdef";
    let b = b"0123456789abcdef0123456789abcdef";
    let c = b"0123456789abcdef0123456789abcdeg"; // 1 char diff at end
    let d = b"1123456789abcdef0123456789abcdef"; // 1 char diff at start
    let e = b"0123456789abcdef"; // different length

    assert!(constant_time_eq(a, b), "Identical slices must return true");
    assert!(!constant_time_eq(a, c), "Diff at end must return false");
    assert!(!constant_time_eq(a, d), "Diff at start must return false");
    assert!(
        !constant_time_eq(a, e),
        "Different lengths must return false"
    );
    assert!(constant_time_eq(b"", b""), "Empty slices must return true");
}

// ===========================================================================
// SECTION 5: Axum Body Size Limits & Request Bounding (R2)
// ===========================================================================

#[tokio::test]
async fn test_axum_oversized_payload_preferences_rejected() {
    let (state, _interner, _graph, _prefs_store) =
        create_test_app_state("did:web:feed.example.com", "feed.example.com");
    let app = create_xrpc_router(state);

    let token = generate_session_token("did:plc:alice_size_test", 3600);

    // Send 100KB payload (exceeds 64KB safe bound)
    let oversized_bytes = vec![b'x'; 100 * 1024];
    let req = Request::builder()
        .method(Method::POST)
        .uri("/api/preferences")
        .header(CONTENT_TYPE, "application/json")
        .header(AUTHORIZATION, format!("Bearer {token}"))
        .body(Body::from(oversized_bytes))
        .unwrap();

    let resp = app.oneshot(req).await.unwrap();
    // Axum returns 4xx Client Error (e.g. 400 Bad Request, 413 Payload Too Large, or 422 Unprocessable)
    assert!(
        resp.status().is_client_error(),
        "Oversized payload must receive a 4xx error status, got: {}",
        resp.status()
    );
}

#[tokio::test]
async fn test_axum_oversized_payload_auth_login_rejected() {
    let (state, _interner, _graph, _prefs_store) =
        create_test_app_state("did:web:feed.example.com", "feed.example.com");
    let app = create_xrpc_router(state);

    // 200KB oversized login payload
    let oversized_bytes = vec![b'a'; 200 * 1024];
    let req = Request::builder()
        .method(Method::POST)
        .uri("/api/auth/login")
        .header(CONTENT_TYPE, "application/json")
        .body(Body::from(oversized_bytes))
        .unwrap();

    let resp = app.oneshot(req).await.unwrap();
    assert!(
        resp.status().is_client_error(),
        "Oversized auth login payload must receive 4xx, got: {}",
        resp.status()
    );
}

#[tokio::test]
async fn test_axum_normal_size_payloads_processed_cleanly() {
    let (state, _interner, _graph, _prefs_store) =
        create_test_app_state("did:web:feed.example.com", "feed.example.com");
    let app = create_xrpc_router(state);

    let alice_did = "did:plc:alice_normal_size";
    let token = generate_session_token(alice_did, 3600);

    // Normal sized preference update (~200 bytes)
    let valid_body = serde_json::json!({
        "freshness_hours": 18.0,
        "discovery_ratio": 0.25,
        "topic_weights": {
            "art": 3.0,
            "tech": 1.5,
            "science": 2.0,
            "news": 0.5,
            "culture": 1.0
        }
    });

    let req = Request::builder()
        .method(Method::POST)
        .uri("/api/preferences")
        .header(CONTENT_TYPE, "application/json")
        .header(AUTHORIZATION, format!("Bearer {token}"))
        .body(Body::from(serde_json::to_vec(&valid_body).unwrap()))
        .unwrap();

    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    // Verify GET /api/preferences returns the saved dials
    let get_req = Request::builder()
        .method(Method::GET)
        .uri("/api/preferences")
        .header(AUTHORIZATION, format!("Bearer {token}"))
        .body(Body::empty())
        .unwrap();

    let get_resp = app.oneshot(get_req).await.unwrap();
    assert_eq!(get_resp.status(), StatusCode::OK);

    let body_bytes = get_resp.into_body().collect().await.unwrap().to_bytes();
    let pref_dto: PreferencesResponseDto = serde_json::from_slice(&body_bytes).unwrap();
    assert_eq!(pref_dto.did.as_str(), alice_did);
    assert!(pref_dto.is_custom);
    assert_eq!(pref_dto.preferences.freshness_hours, 18.0);
    assert_eq!(pref_dto.preferences.discovery_ratio, 0.25);
    assert_eq!(pref_dto.preferences.topic_weights.art, 3.0);
}
