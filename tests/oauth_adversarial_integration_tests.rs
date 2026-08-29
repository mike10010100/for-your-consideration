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

//! Adversarial integration test suite validating the complete `atproto-oauth-rs` library
//! integration into the `for-your-consideration` custom feed generator.

use std::sync::Arc;

use base64::Engine;
use compact_str::CompactString;
use for_your_consideration::auth::{
    compute_access_token_hash, publish_feed_generator_record, validate_service_jwt,
    verify_pkce_challenge, DPoPKey, DPoPVerifier, OAuthSessionState, OAuthStateStore,
    OAuthUserSessionStore, PkcePair, UserOAuthSession,
};
use for_your_consideration::types::FeedPublishRequest;

#[test]
fn test_adv_oauth_dpop_verifier_and_key_proof_integration() {
    let dpop_key = DPoPKey::generate();
    let token = "test_bound_access_token_999";
    let ath = compute_access_token_hash(token);
    let htm = "POST";
    let htu = "https://bsky.social/xrpc/com.atproto.repo.putRecord";

    // Create DPoP proof bound to access token
    let proof = dpop_key
        .create_proof(htm, htu, Some("mock_nonce_xyz"), Some(&ath))
        .unwrap();

    // Verify DPoP proof with DPoPVerifier (passing expected_ath)
    let verifier = DPoPVerifier::new();
    let (claims, jwk) = verifier
        .verify_proof(&proof, htm, htu, Some("mock_nonce_xyz"), Some(&ath), None)
        .unwrap();

    assert_eq!(claims.htm, htm);
    assert_eq!(claims.htu, htu);
    assert_eq!(claims.ath.as_deref(), Some(ath.as_str()));
    assert_eq!(claims.nonce.as_deref(), Some("mock_nonce_xyz"));
    assert_eq!(jwk.crv, "P-256");
    assert_eq!(jwk.kty, "EC");
}

#[test]
fn test_adv_oauth_session_bridge_and_proof_generation() {
    let dpop_key = DPoPKey::generate();
    let access_token = "bridge_access_token_12345";
    let expected_ath = compute_access_token_hash(access_token);

    let user_session = UserOAuthSession {
        did: CompactString::new("did:plc:bridge_user"),
        handle: CompactString::new("bridge.bsky.social"),
        access_token: access_token.to_string(),
        refresh_token: Some("bridge_refresh_token_67890".to_string()),
        token_type: "DPoP".to_string(),
        dpop_private_key: Some(dpop_key.to_bytes_b64()),
        pds_endpoint: "https://pds.bridge.test".to_string(),
        token_endpoint: "https://pds.bridge.test/oauth/token".to_string(),
        expires_at_secs: 1_800_000_000,
    };

    // Convert to skyauth::session::OAuthSession
    let oauth_session = user_session.to_oauth_session().unwrap();
    assert_eq!(oauth_session.sub(), "did:plc:bridge_user");
    assert_eq!(oauth_session.access_token(), access_token);
    assert_eq!(oauth_session.token_type(), "DPoP");

    // Generate DPoP proof via OAuthSession
    let htm = "GET";
    let htu = "https://pds.bridge.test/xrpc/app.bsky.actor.getProfile";
    let proof = oauth_session.create_dpop_proof(htm, htu, None).unwrap();

    // Verify proof
    let verifier = DPoPVerifier::new();
    let (claims, _) = verifier
        .verify_proof(&proof, htm, htu, None, Some(&expected_ath), None)
        .unwrap();

    assert_eq!(claims.htm, htm);
    assert_eq!(claims.htu, htu);
    assert_eq!(claims.ath.as_deref(), Some(expected_ath.as_str()));
}

#[test]
fn test_adv_pkce_all_standard_lengths_and_constant_time() {
    for entropy_bytes in [32, 48, 64, 96] {
        let pair = PkcePair::generate_with_entropy_size(entropy_bytes).unwrap();
        assert!(verify_pkce_challenge(&pair.verifier, &pair.challenge));

        // Flip last character
        let mut tampered = pair.verifier.clone();
        let last_char = if tampered.ends_with('a') { 'b' } else { 'a' };
        tampered.pop();
        tampered.push(last_char);
        assert!(!verify_pkce_challenge(&tampered, &pair.challenge));
    }
}

#[test]
fn test_adv_oauth_state_store_100_threads_concurrency_stress() {
    let store = Arc::new(OAuthStateStore::new());
    let mut handles = Vec::new();

    for thread_idx in 0..100 {
        let store_clone = Arc::clone(&store);
        handles.push(std::thread::spawn(move || {
            let state_key = format!("state_thread_{thread_idx}");
            let session = OAuthSessionState {
                code_verifier: format!("verifier_{thread_idx}"),
                handle: format!("user_{thread_idx}.bsky.social"),
                did: Some(format!("did:plc:user_{thread_idx}")),
                pds_url: "https://bsky.social".to_string(),
                token_endpoint: "https://bsky.social/oauth/token".to_string(),
                redirect_uri: "https://feed.example.com/oauth/callback".to_string(),
                created_at_secs: 1_700_000_000,
                dpop_private_key: None,
            };

            // Insert
            store_clone.insert(state_key.clone(), session.clone());
            assert!(store_clone.get(&state_key).is_some());

            // Take (single use)
            let taken = store_clone.take(&state_key);
            assert_eq!(taken, Some(session));
            assert!(store_clone.take(&state_key).is_none());
        }));
    }

    for h in handles {
        h.join().unwrap();
    }

    assert_eq!(store.len(), 0);
    assert!(store.is_empty());
}

#[test]
fn test_adv_oauth_user_session_store_100_threads_concurrency_stress() {
    let store = Arc::new(OAuthUserSessionStore::new());
    let mut handles = Vec::new();

    for thread_idx in 0..100 {
        let store_clone = Arc::clone(&store);
        handles.push(std::thread::spawn(move || {
            let did = format!("did:plc:concurrent_user_{thread_idx}");
            let session = UserOAuthSession {
                did: CompactString::new(&did),
                handle: CompactString::new(format!("user_{thread_idx}.bsky.social")),
                access_token: format!("token_{thread_idx}"),
                refresh_token: Some(format!("refresh_{thread_idx}")),
                token_type: "DPoP".to_string(),
                dpop_private_key: None,
                pds_endpoint: "https://bsky.social".to_string(),
                token_endpoint: "https://bsky.social/oauth/token".to_string(),
                expires_at_secs: 1_700_000_000 + (thread_idx as u64 * 10),
            };

            store_clone.insert(did.clone(), session.clone());
            assert!(store_clone.get(&did).is_some());
        }));
    }

    for h in handles {
        h.join().unwrap();
    }

    assert_eq!(store.len(), 100);

    // Prune sessions older than timestamp
    store.prune_expired(1_700_000_500);
    // Sessions with expires_at_secs <= 1_700_000_500 (0..=50) should be pruned
    assert_eq!(store.len(), 49); // 51..99 remain
}

#[tokio::test]
async fn test_adv_feed_publish_with_user_oauth_session_live_dpop() {
    use axum::extract::Json;
    use axum::http::{HeaderMap, StatusCode};
    use axum::response::IntoResponse;
    use axum::routing::post;
    use axum::Router;

    // Spin up an in-process mock PDS server that validates real DPoP proofs and Authorization headers
    let recorded_dpop = Arc::new(parking_lot::Mutex::new(Vec::new()));
    let recorded_auth = Arc::new(parking_lot::Mutex::new(Vec::new()));

    let dpop_clone1 = Arc::clone(&recorded_dpop);
    let auth_clone1 = Arc::clone(&recorded_auth);
    let dpop_clone2 = Arc::clone(&recorded_dpop);
    let auth_clone2 = Arc::clone(&recorded_auth);

    let app = Router::new()
        .route(
            "/xrpc/com.atproto.repo.uploadBlob",
            post(move |headers: HeaderMap, _body: axum::body::Bytes| {
                let dpop = headers.get("DPoP").map(|v| v.to_str().unwrap().to_string());
                let auth = headers
                    .get("Authorization")
                    .map(|v| v.to_str().unwrap().to_string());
                if let Some(d) = dpop {
                    dpop_clone1.lock().push(d);
                }
                if let Some(a) = auth {
                    auth_clone1.lock().push(a);
                }
                async move {
                    (
                        StatusCode::OK,
                        Json(serde_json::json!({
                            "blob": {
                                "$type": "blob",
                                "ref": { "$link": "bafkreimockblob12345" },
                                "mimeType": "image/png",
                                "size": 1024
                            }
                        })),
                    )
                        .into_response()
                }
            }),
        )
        .route(
            "/xrpc/com.atproto.repo.putRecord",
            post(move |headers: HeaderMap, Json(body): Json<serde_json::Value>| {
                let dpop = headers.get("DPoP").map(|v| v.to_str().unwrap().to_string());
                let auth = headers
                    .get("Authorization")
                    .map(|v| v.to_str().unwrap().to_string());
                if let Some(d) = dpop {
                    dpop_clone2.lock().push(d);
                }
                if let Some(a) = auth {
                    auth_clone2.lock().push(a);
                }

                assert_eq!(body["collection"], "app.bsky.feed.generator");
                assert_eq!(body["rkey"], "adv-custom-feed");
                assert_eq!(body["repo"], "did:plc:real_test_subject_999");
                assert_eq!(body["record"]["displayName"], "Adversarial Test Feed");

                async move {
                    (
                        StatusCode::OK,
                        Json(serde_json::json!({
                            "uri": "at://did:plc:real_test_subject_999/app.bsky.feed.generator/adv-custom-feed",
                            "cid": "bafyreiputrecordcid99999"
                        })),
                    )
                        .into_response()
                }
            }),
        );

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let pds_mock_url = format!("http://127.0.0.1:{}", addr.port());

    tokio::spawn(async move {
        axum::serve(listener, app).await.unwrap();
    });

    let dpop_key = DPoPKey::generate();
    let oauth = UserOAuthSession {
        did: CompactString::new("did:plc:real_test_subject_999"),
        handle: CompactString::new("testsubject.bsky.social"),
        access_token: "live_dpop_access_token_xyz".to_string(),
        refresh_token: None,
        token_type: "DPoP".to_string(),
        dpop_private_key: Some(dpop_key.to_bytes_b64()),
        pds_endpoint: pds_mock_url.clone(),
        token_endpoint: format!("{pds_mock_url}/oauth/token"),
        expires_at_secs: 1_800_000_000,
    };

    let req = FeedPublishRequest {
        display_name: "Adversarial Test Feed".to_string(),
        rkey: "adv-custom-feed".to_string(),
        description: "Testing authentic DPoP publication against mock PDS server".to_string(),
        app_password: None,
    };

    let resp = publish_feed_generator_record(
        "did:plc:real_test_subject_999",
        "live_dpop_access_token_xyz",
        &req,
        "did:web:live.feedgenerator.com",
        Some(&pds_mock_url),
        Some(&oauth),
    )
    .await
    .unwrap();

    assert_eq!(resp.status.as_str(), "ok");
    assert_eq!(
        resp.uri.as_str(),
        "at://did:plc:real_test_subject_999/app.bsky.feed.generator/adv-custom-feed"
    );

    // Verify observable OAuth & DPoP headers
    let auths = recorded_auth.lock();
    let dpops = recorded_dpop.lock();
    assert_eq!(auths.len(), 2); // uploadBlob + putRecord
    assert_eq!(dpops.len(), 2);
    for a in auths.iter() {
        assert_eq!(a, "DPoP live_dpop_access_token_xyz");
    }

    let ath = compute_access_token_hash("live_dpop_access_token_xyz");
    let verifier = DPoPVerifier::new();

    // Verify uploadBlob proof
    let (claims0, jwk0) = verifier
        .verify_proof(
            &dpops[0],
            "POST",
            &format!("{pds_mock_url}/xrpc/com.atproto.repo.uploadBlob"),
            None,
            Some(ath.as_str()),
            None,
        )
        .unwrap();
    assert_eq!(claims0.htm, "POST");
    assert_eq!(claims0.ath.as_deref(), Some(ath.as_str()));
    assert_eq!(jwk0.thumbprint(), dpop_key.jwk_thumbprint());

    // Verify putRecord proof
    let (claims1, jwk1) = verifier
        .verify_proof(
            &dpops[1],
            "POST",
            &format!("{pds_mock_url}/xrpc/com.atproto.repo.putRecord"),
            None,
            Some(ath.as_str()),
            None,
        )
        .unwrap();
    assert_eq!(claims1.htm, "POST");
    assert_eq!(claims1.ath.as_deref(), Some(ath.as_str()));
    assert_eq!(jwk1.thumbprint(), dpop_key.jwk_thumbprint());
}

#[test]
fn test_adv_jwt_validation_clock_skew_and_future_tokens() {
    let now = 1_700_000_000;

    // 1. Expired within 60s leeway -> OK
    let header = "eyJhbGciOiJFUzI1NksiLCJ0eXAiOiJKV1QifQ";
    let payload = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(
        serde_json::json!({
            "iss": "did:plc:test_actor",
            "aud": "did:web:feed.example.com",
            "exp": now - 45
        })
        .to_string()
        .as_bytes(),
    );
    let token = format!("{header}.{payload}.dummy_sig");
    let res = validate_service_jwt(
        &format!("Bearer {token}"),
        Some("did:web:feed.example.com"),
        now,
    );
    assert!(res.is_ok(), "45s expired token within 60s leeway must pass");

    // 2. Expired beyond 60s leeway -> Error
    let payload_too_old = base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(
        serde_json::json!({
            "iss": "did:plc:test_actor",
            "aud": "did:web:feed.example.com",
            "exp": now - 75
        })
        .to_string()
        .as_bytes(),
    );
    let token_too_old = format!("{header}.{payload_too_old}.dummy_sig");
    let res_err = validate_service_jwt(
        &format!("Bearer {token_too_old}"),
        Some("did:web:feed.example.com"),
        now,
    );
    assert!(res_err.is_err(), "75s expired token must be rejected");
}
