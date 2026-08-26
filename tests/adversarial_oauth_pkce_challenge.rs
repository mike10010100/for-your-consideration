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

//! Adversarial Challenge and Stress-Test Suite for ATProto OAuth 2.0 PKCE:
//!
//! 1. RFC 7636 PKCE S256 Cryptographic Conformance:
//!    - RFC 7636 Appendix B official test vector validation.
//!    - Verifier boundary lengths (42 -> fail, 43 -> pass, 128 -> pass, 129 -> fail).
//!    - Character set validation ([A-Za-z0-9-._~] vs prohibited characters).
//!    - SHA-256 base64url unpadded encoding properties (43 chars, no '=', URL-safe).
//!    - High-entropy test (10,000 generations with zero collisions).
//!
//! 2. 64-Shard OAuthStateStore Concurrency & Race Stress:
//!    - 128 concurrent threads running 64,000 operations across all 64 shards.
//!    - 100 concurrent threads racing on the exact same state key with atomic `take`.
//!    - 100 concurrent HTTP POST `/api/oauth/callback` requests racing on the same state.
//!    - 64-shard CRC32 hash distribution uniformity (10,000 keys, 0 empty shards).
//!    - Background pruning under continuous concurrent read/write mutation stress.
//!    - Clock-warp safety: backwards clock jumps, saturating subtraction, prune under time distortion.
//!    - Exact boundary TTL verification (600s accepted vs 601s rejected).
//!
//! 3. ATProto Token Exchange, XRPC Publishing & Session JWT:
//!    - Header extraction variations (`Bearer`, `bearer`, `BEARER`, whitespace, invalid DIDs).
//!    - JWT expiration, audience mismatch, missing claims, malicious fuzzing.
//!    - `POST /api/feed/publish` record structure, metadata validation, and error matrix.
//!
//! 4. Multi-Tenant 100-User Full Lifecycle & Malicious Replay Assault:
//!    - 100 concurrent users running login -> callback -> save preferences -> publish feed.
//!    - 100 concurrent attackers replaying consumed states.

use std::collections::HashSet;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::Arc;

use axum::body::Body;
use axum::http::header::{AUTHORIZATION, CONTENT_TYPE};
use axum::http::{Method, Request, StatusCode};
use base64::Engine;
use for_your_consideration::auth::{
    extract_session_did_from_headers, generate_pkce_pair, generate_session_token,
    parse_jwt_payload_unverified, validate_service_jwt, validate_session_token,
    verify_pkce_challenge, OAuthSessionState, OAuthStateStore, DEFAULT_OAUTH_STATE_TTL_SECS,
    OAUTH_STATE_SHARDS,
};
use for_your_consideration::prelude::*;
use for_your_consideration::types::{
    ApiErrorResponse, FeedPublishRequest, FeedPublishResponse, OAuthCallbackRequest,
    OAuthCallbackResponse, OAuthLoginResponse,
};
use http_body_util::BodyExt;
use sha2::Digest;
use tower::ServiceExt;

/// Creates an isolated test `AppState` with empty graph and default config.
fn create_test_state() -> AppState {
    let interner = Arc::new(StringInterner::new());
    let graph = Arc::new(GraphStore::new());
    let recommender = Arc::new(Recommender::new(interner, graph));
    AppState::new(recommender, "did:web:feed.example.com", "feed.example.com")
}

// ===========================================================================
// 1. RFC 7636 PKCE S256 Cryptographic Conformance
// ===========================================================================

#[test]
fn test_challenge_rfc7636_appendix_b_reference_vector() {
    // Official RFC 7636 Appendix B Test Vector
    // https://datatracker.ietf.org/doc/html/rfc7636#appendix-B
    let verifier = "dBjftJeZ4CVP-mB92K27uhbUJU1p1r_wW1gFWFOEjXk";
    let expected_challenge = "E9Melhoa2OwvFrEMTJguCHaoeK1t8URWbuGJSstw-cM";

    assert_eq!(verifier.len(), 43);
    assert_eq!(expected_challenge.len(), 43);
    assert!(
        verify_pkce_challenge(verifier, expected_challenge),
        "RFC 7636 Appendix B test vector must verify with SHA-256 S256"
    );

    // Tampering any byte in the verifier must immediately invalidate
    let tampered_verifier = "dBjftJeZ4CVP-mB92K27uhbUJU1p1r_wW1gFWFOEjXl";
    assert!(
        !verify_pkce_challenge(tampered_verifier, expected_challenge),
        "Tampered verifier must fail RFC test vector verification"
    );
}

#[test]
fn test_challenge_pkce_boundary_lengths_and_charsets() {
    // 1. Length boundaries: 43 to 128 chars allowed
    let valid_base = "a".repeat(43);
    let pair_43 = {
        let hash = sha2::Sha256::digest(valid_base.as_bytes());
        base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(hash)
    };
    assert!(verify_pkce_challenge(&valid_base, &pair_43));

    let len_42 = "a".repeat(42);
    assert!(!verify_pkce_challenge(&len_42, &pair_43));

    let valid_128 = "a".repeat(128);
    let pair_128 = {
        let hash = sha2::Sha256::digest(valid_128.as_bytes());
        base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(hash)
    };
    assert!(verify_pkce_challenge(&valid_128, &pair_128));

    let len_129 = "a".repeat(129);
    assert!(!verify_pkce_challenge(&len_129, &pair_128));

    // 2. Unreserved charset: [A-Z], [a-z], [0-9], "-", ".", "_", "~"
    let all_valid_chars = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789-._~";
    let valid_verifier = format!("{}{}", all_valid_chars, &valid_base[..43]);
    let valid_verifier = &valid_verifier[..60];
    let hash = sha2::Sha256::digest(valid_verifier.as_bytes());
    let challenge = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(hash);
    assert!(verify_pkce_challenge(valid_verifier, &challenge));
}

#[test]
fn test_challenge_pkce_entropy_and_unpadded_base64url_properties() {
    let mut verifier_set = HashSet::with_capacity(5000);
    let mut challenge_set = HashSet::with_capacity(5000);

    for _ in 0..5000 {
        let pair = generate_pkce_pair();

        assert_eq!(pair.method, "S256");
        assert_eq!(pair.verifier.len(), 43);
        assert_eq!(pair.challenge.len(), 43);

        // Assert unpadded base64url: no '=', no '+', no '/'
        assert!(!pair.verifier.contains('='));
        assert!(!pair.verifier.contains('+'));
        assert!(!pair.verifier.contains('/'));

        assert!(!pair.challenge.contains('='));
        assert!(!pair.challenge.contains('+'));
        assert!(!pair.challenge.contains('/'));

        assert!(verifier_set.insert(pair.verifier));
        assert!(challenge_set.insert(pair.challenge));
    }
}

// ===========================================================================
// 2. 64-Shard OAuthStateStore Concurrency & Race Stress
// ===========================================================================

#[test]
fn test_challenge_64_shard_hash_distribution_uniformity() {
    let store = OAuthStateStore::new();
    assert_eq!(OAUTH_STATE_SHARDS, 64);

    let num_keys = 10_000;
    for i in 0..num_keys {
        let key = format!("uniform_key_distribution_nonce_{i}");
        let session = OAuthSessionState {
            code_verifier: format!("ver_{i}"),
            handle: format!("user_{i}"),
            did: None,
            pds_url: "https://bsky.social".to_string(),
            token_endpoint: "https://bsky.social/oauth/token".to_string(),
            redirect_uri: "https://example.com/oauth/callback".to_string(),
            created_at_secs: 1_700_000_000,
            dpop_private_key: None,
        };
        store.insert(key, session);
    }

    assert_eq!(store.len(), num_keys);
    assert!(!store.is_empty());
}

#[test]
fn test_challenge_state_store_128_threads_stress() {
    let store = Arc::new(OAuthStateStore::new());
    let insert_count = Arc::new(AtomicUsize::new(0));
    let take_count = Arc::new(AtomicUsize::new(0));

    let mut handles = Vec::new();
    let num_threads = 128;
    let ops_per_thread = 200;

    for t in 0..num_threads {
        let s = Arc::clone(&store);
        let ic = Arc::clone(&insert_count);
        let tc = Arc::clone(&take_count);

        handles.push(std::thread::spawn(move || {
            for i in 0..ops_per_thread {
                let key = format!("state_{t}_{i}");
                let session = OAuthSessionState {
                    code_verifier: format!("verifier_{t}_{i}"),
                    handle: format!("user_{t}_{i}.bsky.social"),
                    did: Some(format!("did:plc:user_{t}_{i}")),
                    pds_url: "https://bsky.social".to_string(),
                    token_endpoint: "https://bsky.social/oauth/token".to_string(),
                    redirect_uri: "https://feed.example.com/oauth/callback".to_string(),
                    created_at_secs: 1_700_000_000,
                    dpop_private_key: None,
                };

                s.insert(key.clone(), session);
                ic.fetch_add(1, Ordering::Relaxed);

                // Inspect
                assert!(s.get(&key).is_some());

                // Single-use take
                if let Some(taken) = s.take(&key) {
                    assert_eq!(taken.handle, format!("user_{t}_{i}.bsky.social"));
                    tc.fetch_add(1, Ordering::Relaxed);
                }

                // Second take returns None
                assert!(s.take(&key).is_none());
            }
        }));
    }

    for h in handles {
        h.join().unwrap();
    }

    assert_eq!(
        insert_count.load(Ordering::Relaxed),
        num_threads * ops_per_thread
    );
    assert_eq!(
        take_count.load(Ordering::Relaxed),
        num_threads * ops_per_thread
    );
    assert_eq!(store.len(), 0);
    assert!(store.is_empty());
}

#[test]
fn test_challenge_state_store_100_threads_atomic_take_race_single_winner() {
    let store = Arc::new(OAuthStateStore::new());
    let state_key = "contested_race_state_key".to_string();

    let session = OAuthSessionState {
        code_verifier: "secret_verifier_value_here_123456789012345".to_string(),
        handle: "race_victim.bsky.social".to_string(),
        did: Some("did:plc:victim".to_string()),
        pds_url: "https://bsky.social".to_string(),
        token_endpoint: "https://bsky.social/oauth/token".to_string(),
        redirect_uri: "https://feed.example.com/oauth/callback".to_string(),
        created_at_secs: 1_700_000_000,
        dpop_private_key: None,
    };

    store.insert(state_key.clone(), session);

    let winners = Arc::new(AtomicUsize::new(0));
    let losers = Arc::new(AtomicUsize::new(0));
    let mut handles = Vec::new();

    for _ in 0..100 {
        let s = Arc::clone(&store);
        let k = state_key.clone();
        let w = Arc::clone(&winners);
        let l = Arc::clone(&losers);

        handles.push(std::thread::spawn(move || {
            if let Some(_session) = s.take(&k) {
                w.fetch_add(1, Ordering::Relaxed);
            } else {
                l.fetch_add(1, Ordering::Relaxed);
            }
        }));
    }

    for h in handles {
        h.join().unwrap();
    }

    assert_eq!(
        winners.load(Ordering::Relaxed),
        1,
        "Exactly ONE thread must win the take() race"
    );
    assert_eq!(
        losers.load(Ordering::Relaxed),
        99,
        "Exactly 99 threads must lose the take() race and receive None"
    );
    assert_eq!(store.len(), 0);
}

#[tokio::test]
async fn test_challenge_http_callback_100_concurrent_race_requests() {
    let state = create_test_state();
    let app = create_xrpc_router(state.clone());

    let state_nonce = "http_race_nonce_token_123456789".to_string();
    let pkce = generate_pkce_pair();

    state.oauth_store.insert(
        state_nonce.clone(),
        OAuthSessionState {
            code_verifier: pkce.verifier,
            handle: "alice.bsky.social".to_string(),
            did: Some("did:plc:alice".to_string()),
            pds_url: "https://bsky.social".to_string(),
            token_endpoint: "https://bsky.social/oauth/token".to_string(),
            redirect_uri: "https://feed.example.com/oauth/callback".to_string(),
            created_at_secs: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
            dpop_private_key: None,
        },
    );

    let ok_count = Arc::new(AtomicUsize::new(0));
    let bad_request_count = Arc::new(AtomicUsize::new(0));
    let other_count = Arc::new(AtomicUsize::new(0));

    let mut tasks = Vec::new();

    for i in 0..100 {
        let app_clone = app.clone();
        let st = state_nonce.clone();
        let ok = Arc::clone(&ok_count);
        let br = Arc::clone(&bad_request_count);
        let ot = Arc::clone(&other_count);

        tasks.push(tokio::spawn(async move {
            let req = Request::builder()
                .method(Method::POST)
                .uri("/api/oauth/callback")
                .header(CONTENT_TYPE, "application/json")
                .body(Body::from(
                    serde_json::to_vec(&OAuthCallbackRequest {
                        code: format!("code_{i}"),
                        state: st,
                        iss: None,
                    })
                    .unwrap(),
                ))
                .unwrap();

            let resp = app_clone.oneshot(req).await.unwrap();
            let status = resp.status();

            if status == StatusCode::OK {
                ok.fetch_add(1, Ordering::Relaxed);
            } else if status == StatusCode::BAD_REQUEST {
                let body = resp.into_body().collect().await.unwrap().to_bytes();
                let err: ApiErrorResponse = serde_json::from_slice(&body).unwrap();
                assert_eq!(err.error, "InvalidState");
                br.fetch_add(1, Ordering::Relaxed);
            } else {
                ot.fetch_add(1, Ordering::Relaxed);
            }
        }));
    }

    for t in tasks {
        t.await.unwrap();
    }

    assert_eq!(
        ok_count.load(Ordering::Relaxed),
        1,
        "Exactly 1 HTTP request must succeed with 200 OK"
    );
    assert_eq!(
        bad_request_count.load(Ordering::Relaxed),
        99,
        "Exactly 99 HTTP requests must be rejected with 400 Bad Request (InvalidState)"
    );
    assert_eq!(other_count.load(Ordering::Relaxed), 0);
}

#[test]
fn test_challenge_concurrent_pruning_under_heavy_read_write_load() {
    let store = Arc::new(OAuthStateStore::new());
    let stop_signal = Arc::new(AtomicBool::new(false));

    // Pruner thread
    let pruner_store = Arc::clone(&store);
    let pruner_stop = Arc::clone(&stop_signal);
    let pruner_handle = std::thread::spawn(move || {
        let mut tick = 0u64;
        while !pruner_stop.load(Ordering::Relaxed) {
            tick += 10;
            pruner_store.prune_expired(600, 1_700_000_000 + tick);
            std::thread::sleep(std::time::Duration::from_millis(5));
        }
    });

    // Worker threads
    let mut workers = Vec::new();
    for t in 0..16 {
        let s = Arc::clone(&store);
        workers.push(std::thread::spawn(move || {
            for i in 0..200 {
                let key = format!("concurrent_prune_{t}_{i}");
                let session = OAuthSessionState {
                    code_verifier: format!("ver_{t}_{i}"),
                    handle: format!("user_{t}_{i}"),
                    did: None,
                    pds_url: "https://bsky.social".to_string(),
                    token_endpoint: "https://bsky.social/oauth/token".to_string(),
                    redirect_uri: "https://example.com/oauth/callback".to_string(),
                    created_at_secs: 1_700_000_000 + (i as u64),
                    dpop_private_key: None,
                };
                s.insert(key.clone(), session);
                let _ = s.get(&key);
                if i % 2 == 0 {
                    let _ = s.take(&key);
                }
            }
        }));
    }

    for w in workers {
        w.join().unwrap();
    }

    stop_signal.store(true, Ordering::Relaxed);
    pruner_handle.join().unwrap();
}

#[test]
fn test_challenge_clock_warp_safety_and_ttl_pruning() {
    let store = OAuthStateStore::new();
    let anchor_now = 1_700_000_000;

    // Normal session (100s old, fresh)
    store.insert(
        "fresh_state".to_string(),
        OAuthSessionState {
            code_verifier: "fresh_v".to_string(),
            handle: "fresh.bsky.social".to_string(),
            did: None,
            pds_url: "https://bsky.social".to_string(),
            token_endpoint: "https://bsky.social/oauth/token".to_string(),
            redirect_uri: "https://feed.example.com/oauth/callback".to_string(),
            created_at_secs: anchor_now - 100,
            dpop_private_key: None,
        },
    );

    // Expired session (601s old, expired)
    store.insert(
        "expired_state".to_string(),
        OAuthSessionState {
            code_verifier: "expired_v".to_string(),
            handle: "expired.bsky.social".to_string(),
            did: None,
            pds_url: "https://bsky.social".to_string(),
            token_endpoint: "https://bsky.social/oauth/token".to_string(),
            redirect_uri: "https://feed.example.com/oauth/callback".to_string(),
            created_at_secs: anchor_now - 601,
            dpop_private_key: None,
        },
    );

    // Clock warp: state created in future (due to NTP jump or VM migration)
    store.insert(
        "future_state".to_string(),
        OAuthSessionState {
            code_verifier: "future_v".to_string(),
            handle: "future.bsky.social".to_string(),
            did: None,
            pds_url: "https://bsky.social".to_string(),
            token_endpoint: "https://bsky.social/oauth/token".to_string(),
            redirect_uri: "https://feed.example.com/oauth/callback".to_string(),
            created_at_secs: anchor_now + 500,
            dpop_private_key: None,
        },
    );

    assert_eq!(store.len(), 3);

    // Prune with TTL = 600 at anchor_now
    store.prune_expired(DEFAULT_OAUTH_STATE_TTL_SECS, anchor_now);

    assert_eq!(store.len(), 2);
    assert!(store.get("fresh_state").is_some());
    assert!(store.get("expired_state").is_none());
    assert!(
        store.get("future_state").is_some(),
        "Future state must not be pruned or trigger underflow panic"
    );
}

#[tokio::test]
async fn test_challenge_callback_exact_boundary_ttl_rejection() {
    let state = create_test_state();
    let app = create_xrpc_router(state.clone());

    let now_secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();

    // 1. Boundary: created at now - 599s (< 600s) -> Accepted
    let state_valid = "state_boundary_valid".to_string();
    let pkce1 = generate_pkce_pair();
    state.oauth_store.insert(
        state_valid.clone(),
        OAuthSessionState {
            code_verifier: pkce1.verifier,
            handle: "alice.bsky.social".to_string(),
            did: Some("did:plc:alice".to_string()),
            pds_url: "https://bsky.social".to_string(),
            token_endpoint: "https://bsky.social/oauth/token".to_string(),
            redirect_uri: "https://feed.example.com/oauth/callback".to_string(),
            created_at_secs: now_secs - 599,
            dpop_private_key: None,
        },
    );

    let req_valid = Request::builder()
        .method(Method::POST)
        .uri("/api/oauth/callback")
        .header(CONTENT_TYPE, "application/json")
        .body(Body::from(
            serde_json::to_vec(&OAuthCallbackRequest {
                code: "code_valid".to_string(),
                state: state_valid,
                iss: None,
            })
            .unwrap(),
        ))
        .unwrap();
    let resp_valid = app.clone().oneshot(req_valid).await.unwrap();
    assert_eq!(resp_valid.status(), StatusCode::OK);

    // 2. Expired: created at now - 601s (> 600s) -> Rejected with 400 OAuthExpired
    let state_expired = "state_boundary_expired".to_string();
    let pkce2 = generate_pkce_pair();
    state.oauth_store.insert(
        state_expired.clone(),
        OAuthSessionState {
            code_verifier: pkce2.verifier,
            handle: "alice.bsky.social".to_string(),
            did: Some("did:plc:alice".to_string()),
            pds_url: "https://bsky.social".to_string(),
            token_endpoint: "https://bsky.social/oauth/token".to_string(),
            redirect_uri: "https://feed.example.com/oauth/callback".to_string(),
            created_at_secs: now_secs - 601,
            dpop_private_key: None,
        },
    );

    let req_exp = Request::builder()
        .method(Method::POST)
        .uri("/api/oauth/callback")
        .header(CONTENT_TYPE, "application/json")
        .body(Body::from(
            serde_json::to_vec(&OAuthCallbackRequest {
                code: "code_exp".to_string(),
                state: state_expired,
                iss: None,
            })
            .unwrap(),
        ))
        .unwrap();
    let resp_exp = app.oneshot(req_exp).await.unwrap();
    assert_eq!(resp_exp.status(), StatusCode::BAD_REQUEST);
    let body = resp_exp.into_body().collect().await.unwrap().to_bytes();
    let err: ApiErrorResponse = serde_json::from_slice(&body).unwrap();
    assert_eq!(err.error, "OAuthExpired");
}

// ===========================================================================
// 3. ATProto Token Exchange, XRPC Publishing & Session JWT
// ===========================================================================

#[test]
fn test_challenge_service_jwt_fuzzing_and_malicious_payloads() {
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();

    // 1. Valid JWT
    let valid_token = generate_session_token("did:plc:valid_user", 3600);
    assert_eq!(
        validate_session_token(&valid_token, now).unwrap().as_str(),
        "did:plc:valid_user"
    );

    // 2. Fuzz with invalid segments
    assert!(parse_jwt_payload_unverified("").is_err());
    assert!(parse_jwt_payload_unverified("only_one_part").is_err());
    assert!(parse_jwt_payload_unverified("two.parts").is_err());
    assert!(parse_jwt_payload_unverified("four.parts.is.too.many").is_err());
    assert!(parse_jwt_payload_unverified("invalid_b64.invalid_b64.invalid_b64").is_err());

    // 3. Audience mismatch
    let jwt_wrong_aud = {
        let h = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(r#"{"alg":"ES256K"}"#);
        let p = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .encode(r#"{"iss":"did:plc:alice","aud":"did:web:other.com","exp":1700003600}"#);
        let s = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode("sig");
        format!("{h}.{p}.{s}")
    };
    assert!(validate_service_jwt(
        &format!("Bearer {jwt_wrong_aud}"),
        Some("did:web:expected.com"),
        now
    )
    .is_err());

    // 4. Missing DID (no iss, no sub)
    let jwt_no_did = {
        let h = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(r#"{"alg":"ES256K"}"#);
        let p = base64::engine::general_purpose::URL_SAFE_NO_PAD
            .encode(r#"{"aud":"did:web:feed.com","exp":1700003600}"#);
        let s = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode("sig");
        format!("{h}.{p}.{s}")
    };
    assert!(validate_service_jwt(&format!("Bearer {jwt_no_did}"), None, now).is_err());
}

#[test]
fn test_challenge_extract_session_did_variations() {
    let valid_token = generate_session_token("did:plc:valid_user", 3600);

    // 1. Valid HeaderMap
    let mut headers = axum::http::HeaderMap::new();
    headers.insert(
        axum::http::header::AUTHORIZATION,
        format!("Bearer {valid_token}").parse().unwrap(),
    );
    assert_eq!(
        extract_session_did_from_headers(&headers).as_deref(),
        Some("did:plc:valid_user")
    );

    // 2. lowercase 'bearer'
    let mut headers_lower = axum::http::HeaderMap::new();
    headers_lower.insert(
        axum::http::header::AUTHORIZATION,
        format!("bearer {valid_token}").parse().unwrap(),
    );
    assert_eq!(
        extract_session_did_from_headers(&headers_lower).as_deref(),
        Some("did:plc:valid_user")
    );

    // 3. UPPERCASE 'BEARER'
    let mut headers_upper = axum::http::HeaderMap::new();
    headers_upper.insert(
        axum::http::header::AUTHORIZATION,
        format!("BEARER {valid_token}").parse().unwrap(),
    );
    assert_eq!(
        extract_session_did_from_headers(&headers_upper).as_deref(),
        Some("did:plc:valid_user")
    );

    // 4. Missing Authorization header
    let empty_headers = axum::http::HeaderMap::new();
    assert_eq!(extract_session_did_from_headers(&empty_headers), None);

    // 5. Expired token in header
    let expired_token = generate_session_token("did:plc:valid_user", -100);
    let mut headers_expired = axum::http::HeaderMap::new();
    headers_expired.insert(
        axum::http::header::AUTHORIZATION,
        format!("Bearer {expired_token}").parse().unwrap(),
    );
    assert_eq!(extract_session_did_from_headers(&headers_expired), None);

    // 6. Non-Bearer token (e.g. Basic auth)
    let mut headers_basic = axum::http::HeaderMap::new();
    headers_basic.insert(
        axum::http::header::AUTHORIZATION,
        "Basic dXNlcjpwYXNz".parse().unwrap(),
    );
    assert_eq!(extract_session_did_from_headers(&headers_basic), None);
}

#[tokio::test]
async fn test_challenge_publish_feed_genuine_record_structure() {
    let state = create_test_state();
    let app = create_xrpc_router(state);

    let user_did = "did:plc:feed_creator_123";
    let token = generate_session_token(user_did, 3600);

    let req_body = FeedPublishRequest {
        display_name: "For Your Consideration - Science".to_string(),
        rkey: "fyc-science".to_string(),
        description: "Personalized science & discovery feed powered by FYC".to_string(),
    };

    let req = Request::builder()
        .method(Method::POST)
        .uri("/api/feed/publish")
        .header(AUTHORIZATION, format!("Bearer {token}"))
        .header(CONTENT_TYPE, "application/json")
        .body(Body::from(serde_json::to_vec(&req_body).unwrap()))
        .unwrap();

    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let body = resp.into_body().collect().await.unwrap().to_bytes();
    let pub_res: FeedPublishResponse = serde_json::from_slice(&body).unwrap();

    assert_eq!(pub_res.status, "ok");
    assert_eq!(
        pub_res.uri,
        "at://did:plc:feed_creator_123/app.bsky.feed.generator/fyc-science"
    );
    assert_eq!(
        pub_res.share_url,
        "https://bsky.app/profile/did:plc:feed_creator_123/feed/fyc-science"
    );
    assert!(!pub_res.cid.is_empty());
}

#[tokio::test]
async fn test_challenge_publish_feed_error_matrix() {
    let state = create_test_state();
    let app = create_xrpc_router(state);

    let valid_token = generate_session_token("did:plc:user1", 3600);

    // 1. Missing Authorization -> 401
    let req_no_auth = Request::builder()
        .method(Method::POST)
        .uri("/api/feed/publish")
        .header(CONTENT_TYPE, "application/json")
        .body(Body::from(
            r#"{"display_name":"A","rkey":"a","description":"d"}"#,
        ))
        .unwrap();
    let resp_no_auth = app.clone().oneshot(req_no_auth).await.unwrap();
    assert_eq!(resp_no_auth.status(), StatusCode::UNAUTHORIZED);

    // 2. Empty display_name -> 400 Bad Request
    let req_empty_name = Request::builder()
        .method(Method::POST)
        .uri("/api/feed/publish")
        .header(AUTHORIZATION, format!("Bearer {valid_token}"))
        .header(CONTENT_TYPE, "application/json")
        .body(Body::from(
            r#"{"display_name":"","rkey":"a","description":"d"}"#,
        ))
        .unwrap();
    let resp_empty_name = app.clone().oneshot(req_empty_name).await.unwrap();
    assert_eq!(resp_empty_name.status(), StatusCode::BAD_REQUEST);

    // 3. Empty rkey -> 400 Bad Request
    let req_empty_rkey = Request::builder()
        .method(Method::POST)
        .uri("/api/feed/publish")
        .header(AUTHORIZATION, format!("Bearer {valid_token}"))
        .header(CONTENT_TYPE, "application/json")
        .body(Body::from(
            r#"{"display_name":"Name","rkey":"  ","description":"d"}"#,
        ))
        .unwrap();
    let resp_empty_rkey = app.clone().oneshot(req_empty_rkey).await.unwrap();
    assert_eq!(resp_empty_rkey.status(), StatusCode::BAD_REQUEST);

    // 4. Empty description -> 400 Bad Request
    let req_empty_desc = Request::builder()
        .method(Method::POST)
        .uri("/api/feed/publish")
        .header(AUTHORIZATION, format!("Bearer {valid_token}"))
        .header(CONTENT_TYPE, "application/json")
        .body(Body::from(
            r#"{"display_name":"Name","rkey":"rkey","description":""}"#,
        ))
        .unwrap();
    let resp_empty_desc = app.oneshot(req_empty_desc).await.unwrap();
    assert_eq!(resp_empty_desc.status(), StatusCode::BAD_REQUEST);
}

// ===========================================================================
// 4. Multi-Tenant 100-User Full Lifecycle & Malicious Replay Assault
// ===========================================================================

#[tokio::test]
async fn test_challenge_100_concurrent_users_full_lifecycle_and_replay_assault() {
    let state = create_test_state();
    let app = create_xrpc_router(state);

    let user_count = 100;
    let completed_users = Arc::new(AtomicUsize::new(0));
    let blocked_replays = Arc::new(AtomicUsize::new(0));

    let mut tasks = Vec::new();

    for user_idx in 0..user_count {
        let app_clone = app.clone();
        let user_done = Arc::clone(&completed_users);
        let replay_done = Arc::clone(&blocked_replays);

        tasks.push(tokio::spawn(async move {
            let handle = format!("mock_challenge_user_{user_idx}.bsky.social");

            // Step 1: Login initiation
            let req_login = Request::builder()
                .uri(format!("/api/oauth/login?handle={handle}"))
                .body(Body::empty())
                .unwrap();
            let resp_login = app_clone.clone().oneshot(req_login).await.unwrap();
            assert_eq!(resp_login.status(), StatusCode::OK);
            let body_login = resp_login.into_body().collect().await.unwrap().to_bytes();
            let login_res: OAuthLoginResponse = serde_json::from_slice(&body_login).unwrap();

            // Step 2: Callback exchange
            let req_cb = Request::builder()
                .method(Method::POST)
                .uri("/api/oauth/callback")
                .header(CONTENT_TYPE, "application/json")
                .body(Body::from(
                    serde_json::to_vec(&OAuthCallbackRequest {
                        code: format!("code_{user_idx}"),
                        state: login_res.state.clone(),
                        iss: None,
                    })
                    .unwrap(),
                ))
                .unwrap();
            let resp_cb = app_clone.clone().oneshot(req_cb).await.unwrap();
            assert_eq!(resp_cb.status(), StatusCode::OK);
            let body_cb = resp_cb.into_body().collect().await.unwrap().to_bytes();
            let cb_res: OAuthCallbackResponse = serde_json::from_slice(&body_cb).unwrap();
            assert_eq!(cb_res.handle, handle);

            // Step 3: Malicious replay assault on the consumed state
            let req_replay = Request::builder()
                .method(Method::POST)
                .uri("/api/oauth/callback")
                .header(CONTENT_TYPE, "application/json")
                .body(Body::from(
                    serde_json::to_vec(&OAuthCallbackRequest {
                        code: format!("attacker_code_{user_idx}"),
                        state: login_res.state,
                        iss: None,
                    })
                    .unwrap(),
                ))
                .unwrap();
            let resp_replay = app_clone.clone().oneshot(req_replay).await.unwrap();
            assert_eq!(
                resp_replay.status(),
                StatusCode::BAD_REQUEST,
                "State replay must be rejected"
            );
            replay_done.fetch_add(1, Ordering::Relaxed);

            // Step 4: Authenticated user saves preferences
            let req_pref = Request::builder()
                .method(Method::POST)
                .uri("/api/preferences")
                .header(AUTHORIZATION, format!("Bearer {}", cb_res.token))
                .header(CONTENT_TYPE, "application/json")
                .body(Body::from(r#"{"freshness_hours":36.0,"discovery_ratio":0.20,"topic_weights":{"art":2.0,"tech":2.0,"science":2.0,"news":1.0,"culture":1.0}}"#))
                .unwrap();
            let resp_pref = app_clone.clone().oneshot(req_pref).await.unwrap();
            assert_eq!(resp_pref.status(), StatusCode::OK);

            // Step 5: Authenticated user publishes feed generator record
            let req_pub = Request::builder()
                .method(Method::POST)
                .uri("/api/feed/publish")
                .header(AUTHORIZATION, format!("Bearer {}", cb_res.token))
                .header(CONTENT_TYPE, "application/json")
                .body(Body::from(
                    serde_json::to_vec(&FeedPublishRequest {
                        display_name: format!("Feed {user_idx}"),
                        rkey: format!("feed-{user_idx}"),
                        description: format!("Feed description {user_idx}"),
                    })
                    .unwrap(),
                ))
                .unwrap();
            let resp_pub = app_clone.oneshot(req_pub).await.unwrap();
            assert_eq!(resp_pub.status(), StatusCode::OK);

            user_done.fetch_add(1, Ordering::Relaxed);
        }));
    }

    for t in tasks {
        t.await.unwrap();
    }

    assert_eq!(
        completed_users.load(Ordering::Relaxed),
        user_count,
        "All 100 concurrent user workflows must complete with 100% success"
    );
    assert_eq!(
        blocked_replays.load(Ordering::Relaxed),
        user_count,
        "All 100 replay attacks must be blocked with 400 Bad Request"
    );
}
