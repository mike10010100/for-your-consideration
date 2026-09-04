#![forbid(unsafe_code)]

use std::sync::Arc;

use axum::body::Body;
use axum::http::header::{AUTHORIZATION, CONTENT_TYPE};
use axum::http::{Method, Request, StatusCode};
use http_body_util::BodyExt;
use tower::ServiceExt;

use for_your_consideration::prelude::*;

fn create_test_state() -> (
    AppState,
    Arc<StringInterner>,
    Arc<GraphStore>,
    Arc<UserPreferencesStore>,
) {
    let interner = Arc::new(StringInterner::new());
    let graph = Arc::new(GraphStore::new());
    let preferences_store = Arc::new(UserPreferencesStore::new());

    let now = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs();

    // Populate some graph state
    let uid = interner.intern("did:plc:alice");
    let aid1 = interner.intern("did:plc:author_art");
    let aid2 = interner.intern("did:plc:author_tech");
    let pid1 = interner.intern("at://did:plc:author_art/app.bsky.feed.post/art1");
    let pid2 = interner.intern("at://did:plc:author_tech/app.bsky.feed.post/tech1");

    graph.record_post_meta(pid1, aid1, None, None, now);
    graph.record_post_meta(pid2, aid2, None, None, now);

    graph.record_interaction(uid, pid1, SignalType::Like, now);
    graph.record_interaction(uid, pid2, SignalType::Repost, now);

    let recommender = Arc::new(Recommender::new(Arc::clone(&interner), Arc::clone(&graph)));
    let state = AppState::new(recommender, "did:web:feed.example.com", "feed.example.com")
        .with_preferences_store(Arc::clone(&preferences_store));

    (state, interner, graph, preferences_store)
}

#[tokio::test]
async fn test_xrpc_3_tier_precedence_hierarchy() {
    let (state, interner, _graph, prefs_store) = create_test_state();
    let app = create_xrpc_router(state);

    let user_did = "did:plc:alice";
    let token = generate_session_token(user_did, 3600);

    // Set custom dials for alice in store (freshness = 12h, discovery = 0.35, art = 3.0)
    let custom_dials = UserDials {
        freshness_half_life_secs: 12.0 * 3600.0,
        serendipity_ratio: 0.35,
        topic_weights: TopicWeights {
            art: 3.0,
            tech: 1.0,
            science: 1.0,
            news: 1.0,
            culture: 1.0,
        },
        include_replies: false,
        min_likes: 3,
        updated_at_secs: 100,
    };
    prefs_store.set_by_did(&interner, user_did, custom_dials);

    // 1. Authenticated query with NO query params -> Uses Persisted Dials (Tier 2)
    let req1 = Request::builder()
        .uri("/xrpc/app.bsky.feed.getFeedSkeleton?feed=at://did:web:feed.example.com/app.bsky.feed.generator/for-your-consideration")
        .header(AUTHORIZATION, format!("Bearer {token}"))
        .body(Body::empty())
        .unwrap();
    let resp1 = app.clone().oneshot(req1).await.unwrap();
    assert_eq!(resp1.status(), StatusCode::OK);

    // 2. Authenticated query with explicit ?freshness=realtime and ?art=0.5 -> Query overrides persisted (Tier 1)
    let req2 = Request::builder()
        .uri("/xrpc/app.bsky.feed.getFeedSkeleton?feed=at://did:web:feed.example.com/app.bsky.feed.generator/for-your-consideration&freshness=realtime&art=0.5")
        .header(AUTHORIZATION, format!("Bearer {token}"))
        .body(Body::empty())
        .unwrap();
    let resp2 = app.clone().oneshot(req2).await.unwrap();
    assert_eq!(resp2.status(), StatusCode::OK);

    // 3. Unauthenticated query -> Uses Defaults (Tier 3) with zero errors
    let req3 = Request::builder()
        .uri("/xrpc/app.bsky.feed.getFeedSkeleton?feed=at://did:web:feed.example.com/app.bsky.feed.generator/for-your-consideration")
        .body(Body::empty())
        .unwrap();
    let resp3 = app.oneshot(req3).await.unwrap();
    assert_eq!(resp3.status(), StatusCode::OK);
}

#[tokio::test]
async fn test_rest_auth_and_preferences_lifecycle() {
    let (state, _interner, _graph, _prefs_store) = create_test_state();
    let app = create_xrpc_router(state);

    // 1. Login to get token
    let login_payload = LoginRequestBody {
        identifier: "alice.bsky.social".to_string(),
        password: "valid-app-password".to_string(),
        pds_url: None,
    };
    let login_req = Request::builder()
        .method(Method::POST)
        .uri("/api/auth/login")
        .header(CONTENT_TYPE, "application/json")
        .body(Body::from(serde_json::to_vec(&login_payload).unwrap()))
        .unwrap();
    let login_resp = app.clone().oneshot(login_req).await.unwrap();
    assert_eq!(login_resp.status(), StatusCode::OK);

    let login_body = login_resp.into_body().collect().await.unwrap().to_bytes();
    let login_data: LoginSuccessResponse = serde_json::from_slice(&login_body).unwrap();
    let token = login_data.token;
    assert!(!token.is_empty());

    // 2. GET /api/preferences without token -> 401
    let unauth_req = Request::builder()
        .method(Method::GET)
        .uri("/api/preferences")
        .body(Body::empty())
        .unwrap();
    let unauth_resp = app.clone().oneshot(unauth_req).await.unwrap();
    assert_eq!(unauth_resp.status(), StatusCode::UNAUTHORIZED);

    // 3. GET /api/preferences with token -> 200 defaults
    let get_req1 = Request::builder()
        .method(Method::GET)
        .uri("/api/preferences")
        .header(AUTHORIZATION, format!("Bearer {token}"))
        .body(Body::empty())
        .unwrap();
    let get_resp1 = app.clone().oneshot(get_req1).await.unwrap();
    assert_eq!(get_resp1.status(), StatusCode::OK);

    let get_body1 = get_resp1.into_body().collect().await.unwrap().to_bytes();
    let prefs_data1: PreferencesResponseDto = serde_json::from_slice(&get_body1).unwrap();
    assert!(!prefs_data1.is_custom);
    assert_eq!(prefs_data1.preferences.freshness_hours, 36.0);

    // 4. POST /api/preferences with custom values -> 200
    let save_payload = SavePreferencesRequestBody {
        freshness_hours: 8.0,
        discovery_ratio: 0.25,
        topic_weights: Some(TopicWeights {
            art: 2.5,
            tech: 1.5,
            science: 3.0,
            news: 0.2,
            culture: 1.0,
        }),
        include_replies: Some(false),
        min_likes: Some(3),
    };
    let save_req = Request::builder()
        .method(Method::POST)
        .uri("/api/preferences")
        .header(AUTHORIZATION, format!("Bearer {token}"))
        .header(CONTENT_TYPE, "application/json")
        .body(Body::from(serde_json::to_vec(&save_payload).unwrap()))
        .unwrap();
    let save_resp = app.clone().oneshot(save_req).await.unwrap();
    assert_eq!(save_resp.status(), StatusCode::OK);

    // 5. GET /api/preferences -> 200 custom
    let get_req2 = Request::builder()
        .method(Method::GET)
        .uri("/api/preferences")
        .header(AUTHORIZATION, format!("Bearer {token}"))
        .body(Body::empty())
        .unwrap();
    let get_resp2 = app.clone().oneshot(get_req2).await.unwrap();
    assert_eq!(get_resp2.status(), StatusCode::OK);

    let get_body2 = get_resp2.into_body().collect().await.unwrap().to_bytes();
    let prefs_data2: PreferencesResponseDto = serde_json::from_slice(&get_body2).unwrap();
    assert!(prefs_data2.is_custom);
    assert_eq!(prefs_data2.preferences.freshness_hours, 8.0);
    assert_eq!(prefs_data2.preferences.discovery_ratio, 0.25);
    assert_eq!(prefs_data2.preferences.topic_weights.science, 3.0);

    // 6. DELETE /api/preferences -> 200 reset to defaults
    let del_req = Request::builder()
        .method(Method::DELETE)
        .uri("/api/preferences")
        .header(AUTHORIZATION, format!("Bearer {token}"))
        .body(Body::empty())
        .unwrap();
    let del_resp = app.clone().oneshot(del_req).await.unwrap();
    assert_eq!(del_resp.status(), StatusCode::OK);

    // 7. GET /api/preferences -> 200 defaults
    let get_req3 = Request::builder()
        .method(Method::GET)
        .uri("/api/preferences")
        .header(AUTHORIZATION, format!("Bearer {token}"))
        .body(Body::empty())
        .unwrap();
    let get_resp3 = app.oneshot(get_req3).await.unwrap();
    assert_eq!(get_resp3.status(), StatusCode::OK);

    let get_body3 = get_resp3.into_body().collect().await.unwrap().to_bytes();
    let prefs_data3: PreferencesResponseDto = serde_json::from_slice(&get_body3).unwrap();
    assert!(!prefs_data3.is_custom);
}

#[tokio::test]
async fn test_cors_methods_allowed() {
    let (state, _interner, _graph, _prefs_store) = create_test_state();
    let app = create_xrpc_router(state);

    // CORS preflight OPTIONS request on /api/preferences
    let req = Request::builder()
        .method(Method::OPTIONS)
        .uri("/api/preferences")
        .header("Origin", "https://bsky.app")
        .header("Access-Control-Request-Method", "POST")
        .header(
            "Access-Control-Request-Headers",
            "authorization, content-type",
        )
        .body(Body::empty())
        .unwrap();

    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(
        resp.headers().get("access-control-allow-origin").unwrap(),
        "*"
    );
    let allow_methods = resp
        .headers()
        .get("access-control-allow-methods")
        .unwrap()
        .to_str()
        .unwrap();
    assert!(allow_methods.contains("POST"));
    assert!(allow_methods.contains("DELETE"));
}
