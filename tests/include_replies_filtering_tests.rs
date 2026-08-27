#![forbid(unsafe_code)]
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic, missing_docs)]

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use axum::body::Body;
use axum::http::header::{AUTHORIZATION, CONTENT_TYPE};
use axum::http::{Method, Request, StatusCode};
use http_body_util::BodyExt;
use tower::ServiceExt;

use for_your_consideration::prelude::*;

fn current_timestamp() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

fn create_reply_test_environment() -> (
    AppState,
    Arc<StringInterner>,
    Arc<GraphStore>,
    Arc<UserPreferencesStore>,
    Arc<Recommender>,
    u32, // pid_root
    u32, // pid_reply
) {
    let interner = Arc::new(StringInterner::new());
    let graph = Arc::new(GraphStore::new());
    let preferences_store = Arc::new(UserPreferencesStore::new());

    let now = current_timestamp();

    let viewer_id = interner.intern("did:plc:alice_viewer");
    let co_user_id = interner.intern("did:plc:bob_twin");
    let seed_author_id = interner.intern("did:plc:author_seed");
    let root_author_id = interner.intern("did:plc:author_root");
    let reply_author_id = interner.intern("did:plc:author_reply");

    let seed_post = interner.intern("at://did:plc:author_seed/app.bsky.feed.post/seed_root");
    let root_post = interner.intern("at://did:plc:author_root/app.bsky.feed.post/cand_root");
    let other_root = interner.intern("at://did:plc:someone_else/app.bsky.feed.post/other_root");
    let reply_post = interner.intern("at://did:plc:author_reply/app.bsky.feed.post/cand_reply");

    // seed_post is a root post
    graph.record_post_meta(seed_post, seed_author_id, None, None, now - 100);
    // root_post is a root post
    graph.record_post_meta(root_post, root_author_id, None, None, now - 50);
    // other_root post metadata
    let other_author = interner.intern("did:plc:someone_else");
    graph.record_post_meta(other_root, other_author, None, None, now - 200);
    // reply_post is a reply to other_root
    graph.record_post_meta(
        reply_post,
        reply_author_id,
        Some(other_root),
        Some(other_root),
        now - 30,
    );

    // Alice likes seed_post
    for i in 0..12 {
        let dummy_post = interner.intern(&format!(
            "at://did:plc:author_seed/app.bsky.feed.post/dummy_{i}"
        ));
        graph.record_post_meta(dummy_post, seed_author_id, None, None, now - 100);
        graph.record_interaction(viewer_id, dummy_post, SignalType::Like, now - 100);
        graph.record_interaction(co_user_id, dummy_post, SignalType::Like, now - 100);
    }
    graph.record_interaction(viewer_id, seed_post, SignalType::Like, now - 100);
    graph.record_interaction(co_user_id, seed_post, SignalType::Like, now - 100);

    // Bob likes root_post and reply_post
    graph.record_interaction(co_user_id, root_post, SignalType::Like, now - 50);
    graph.record_interaction(co_user_id, reply_post, SignalType::Like, now - 30);

    // Baseline interactions so candidate posts meet default engagement floor (min_likes: 3)
    let u1 = interner.intern("did:plc:mock_reply_user_1");
    let u2 = interner.intern("did:plc:mock_reply_user_2");
    for &p in &[root_post, reply_post] {
        graph.record_interaction(u1, p, SignalType::Like, now - 40);
        graph.record_interaction(u2, p, SignalType::Like, now - 40);
    }

    let recommender = Arc::new(Recommender::new(Arc::clone(&interner), Arc::clone(&graph)));
    let state = AppState::new(
        Arc::clone(&recommender),
        "did:web:feed.example.com",
        "feed.example.com",
    )
    .with_preferences_store(Arc::clone(&preferences_store));

    (
        state,
        interner,
        graph,
        preferences_store,
        recommender,
        root_post,
        reply_post,
    )
}

#[test]
fn test_dials_defaults_and_builder() {
    let rec_dials = RecommendationDials::default();
    assert!(
        !rec_dials.include_replies,
        "RecommendationDials default must be false (root posts only)"
    );

    let rec_dials_with = rec_dials.with_include_replies(true);
    assert!(rec_dials_with.include_replies);

    let user_dials = UserDials::default();
    assert!(
        !user_dials.include_replies,
        "UserDials default must be false (root posts only)"
    );

    let user_dials_with = user_dials.with_include_replies(true);
    assert!(user_dials_with.include_replies);

    let converted_rec: RecommendationDials = user_dials_with.to_recommendation_dials();
    assert!(converted_rec.include_replies);

    let from_rec: UserDials = UserDials::from_recommendation_dials(&converted_rec, 12345);
    assert!(from_rec.include_replies);
}

#[test]
fn test_recommend_filters_replies_by_default() {
    let (_state, interner, _graph, _prefs, recommender, root_pid, reply_pid) =
        create_reply_test_environment();
    let root_uri = interner.lookup_str(root_pid).unwrap();
    let reply_uri = interner.lookup_str(reply_pid).unwrap();

    let now = current_timestamp();

    // 1. Default (include_replies = false / root posts only)
    let dials_root_only = RecommendationDials {
        include_replies: false,
        limit: 50,
        ..Default::default()
    };
    let recs_root = recommender
        .recommend(Some("did:plc:alice_viewer"), &dials_root_only, now)
        .unwrap();

    let uris: Vec<&str> = recs_root.posts.iter().map(|p| p.uri.as_str()).collect();
    assert!(
        uris.contains(&root_uri.as_str()),
        "Root post should be present"
    );
    assert!(
        !uris.contains(&reply_uri.as_str()),
        "Reply post MUST be filtered out when include_replies = false"
    );

    // 2. Include replies (include_replies = true)
    let dials_with_replies = RecommendationDials {
        include_replies: true,
        limit: 50,
        ..Default::default()
    };
    let recs_all = recommender
        .recommend(Some("did:plc:alice_viewer"), &dials_with_replies, now)
        .unwrap();

    let uris_all: Vec<&str> = recs_all.posts.iter().map(|p| p.uri.as_str()).collect();
    assert!(
        uris_all.contains(&root_uri.as_str()),
        "Root post should be present"
    );
    assert!(
        uris_all.contains(&reply_uri.as_str()),
        "Reply post SHOULD be present when include_replies = true"
    );
}

#[test]
fn test_recommend_preview_filters_replies_by_default() {
    let (_state, interner, _graph, _prefs, recommender, root_pid, reply_pid) =
        create_reply_test_environment();
    let root_uri = interner.lookup_str(root_pid).unwrap();
    let reply_uri = interner.lookup_str(reply_pid).unwrap();

    let now = current_timestamp();

    // 1. Preview with default (include_replies = false)
    let dials_root_only = RecommendationDials {
        include_replies: false,
        limit: 50,
        explain: true,
        ..Default::default()
    };
    let preview_root = recommender
        .recommend_preview_at(Some("did:plc:alice_viewer"), &dials_root_only, now)
        .unwrap();

    let uris: Vec<&str> = preview_root.items.iter().map(|c| c.uri.as_str()).collect();
    assert!(
        uris.contains(&root_uri.as_str()),
        "Root post candidate present"
    );
    assert!(
        !uris.contains(&reply_uri.as_str()),
        "Reply candidate must be excluded when include_replies = false"
    );

    // 2. Preview with include_replies = true
    let dials_with_replies = RecommendationDials {
        include_replies: true,
        limit: 50,
        explain: true,
        ..Default::default()
    };
    let preview_all = recommender
        .recommend_preview_at(Some("did:plc:alice_viewer"), &dials_with_replies, now)
        .unwrap();

    let uris_all: Vec<&str> = preview_all.items.iter().map(|c| c.uri.as_str()).collect();
    assert!(
        uris_all.contains(&root_uri.as_str()),
        "Root post candidate present"
    );
    assert!(
        uris_all.contains(&reply_uri.as_str()),
        "Reply candidate must be included when include_replies = true"
    );
}

#[tokio::test]
async fn test_xrpc_get_feed_skeleton_reply_parameters() {
    let (state, interner, _graph, _prefs, _recommender, root_pid, reply_pid) =
        create_reply_test_environment();
    let app = create_xrpc_router(state);
    let root_uri = interner.lookup_str(root_pid).unwrap();
    let reply_uri = interner.lookup_str(reply_pid).unwrap();

    let token = generate_session_token("did:plc:alice_viewer", 3600);

    // 1. Default request with NO query parameters -> replies excluded
    let req_default = Request::builder()
        .uri("/xrpc/app.bsky.feed.getFeedSkeleton?feed=at://did:web:feed.example.com/app.bsky.feed.generator/for-your-consideration")
        .header(AUTHORIZATION, format!("Bearer {token}"))
        .body(Body::empty())
        .unwrap();
    let resp_default = app.clone().oneshot(req_default).await.unwrap();
    assert_eq!(resp_default.status(), StatusCode::OK);
    let body_default = resp_default.into_body().collect().await.unwrap().to_bytes();
    let skel_default: FeedSkeletonResponse = serde_json::from_slice(&body_default).unwrap();
    let uris_default: Vec<&str> = skel_default.feed.iter().map(|p| p.post.as_str()).collect();
    assert!(uris_default.contains(&root_uri.as_str()));
    assert!(!uris_default.contains(&reply_uri.as_str()));

    // 2. Query with ?replies=all -> replies included
    let req_all = Request::builder()
        .uri("/xrpc/app.bsky.feed.getFeedSkeleton?feed=at://did:web:feed.example.com/app.bsky.feed.generator/for-your-consideration&replies=all")
        .header(AUTHORIZATION, format!("Bearer {token}"))
        .body(Body::empty())
        .unwrap();
    let resp_all = app.clone().oneshot(req_all).await.unwrap();
    assert_eq!(resp_all.status(), StatusCode::OK);
    let body_all = resp_all.into_body().collect().await.unwrap().to_bytes();
    let skel_all: FeedSkeletonResponse = serde_json::from_slice(&body_all).unwrap();
    let uris_all: Vec<&str> = skel_all.feed.iter().map(|p| p.post.as_str()).collect();
    assert!(uris_all.contains(&root_uri.as_str()));
    assert!(uris_all.contains(&reply_uri.as_str()));

    // 3. Query with ?include_replies=true -> replies included
    let req_bool = Request::builder()
        .uri("/xrpc/app.bsky.feed.getFeedSkeleton?feed=at://did:web:feed.example.com/app.bsky.feed.generator/for-your-consideration&include_replies=true")
        .header(AUTHORIZATION, format!("Bearer {token}"))
        .body(Body::empty())
        .unwrap();
    let resp_bool = app.clone().oneshot(req_bool).await.unwrap();
    assert_eq!(resp_bool.status(), StatusCode::OK);
    let body_bool = resp_bool.into_body().collect().await.unwrap().to_bytes();
    let skel_bool: FeedSkeletonResponse = serde_json::from_slice(&body_bool).unwrap();
    let uris_bool: Vec<&str> = skel_bool.feed.iter().map(|p| p.post.as_str()).collect();
    assert!(uris_bool.contains(&root_uri.as_str()));
    assert!(uris_bool.contains(&reply_uri.as_str()));

    // 4. Query with ?replies=root -> replies excluded
    let req_root = Request::builder()
        .uri("/xrpc/app.bsky.feed.getFeedSkeleton?feed=at://did:web:feed.example.com/app.bsky.feed.generator/for-your-consideration&replies=root")
        .header(AUTHORIZATION, format!("Bearer {token}"))
        .body(Body::empty())
        .unwrap();
    let resp_root = app.oneshot(req_root).await.unwrap();
    assert_eq!(resp_root.status(), StatusCode::OK);
    let body_root = resp_root.into_body().collect().await.unwrap().to_bytes();
    let skel_root: FeedSkeletonResponse = serde_json::from_slice(&body_root).unwrap();
    let uris_root: Vec<&str> = skel_root.feed.iter().map(|p| p.post.as_str()).collect();
    assert!(uris_root.contains(&root_uri.as_str()));
    assert!(!uris_root.contains(&reply_uri.as_str()));
}

#[tokio::test]
async fn test_user_preference_persistence_of_include_replies() {
    let (state, interner, _graph, prefs_store, _recommender, root_pid, reply_pid) =
        create_reply_test_environment();
    let app = create_xrpc_router(state);
    let root_uri = interner.lookup_str(root_pid).unwrap();
    let reply_uri = interner.lookup_str(reply_pid).unwrap();

    let user_did = "did:plc:alice_viewer";
    let token = generate_session_token(user_did, 3600);

    // 1. Initial GET /api/preferences -> include_replies is false
    let req_get1 = Request::builder()
        .method(Method::GET)
        .uri("/api/preferences")
        .header(AUTHORIZATION, format!("Bearer {token}"))
        .body(Body::empty())
        .unwrap();
    let resp_get1 = app.clone().oneshot(req_get1).await.unwrap();
    assert_eq!(resp_get1.status(), StatusCode::OK);
    let body_get1 = resp_get1.into_body().collect().await.unwrap().to_bytes();
    let prefs1: PreferencesResponseDto = serde_json::from_slice(&body_get1).unwrap();
    assert!(!prefs1.is_custom);
    assert!(!prefs1.preferences.include_replies);

    // 2. Save preference with include_replies = true
    let save_body = SavePreferencesRequestBody {
        freshness_hours: 24.0,
        discovery_ratio: 0.15,
        topic_weights: Some(TopicWeights::default()),
        include_replies: Some(true),
        min_likes: Some(3),
    };
    let req_save = Request::builder()
        .method(Method::POST)
        .uri("/api/preferences")
        .header(AUTHORIZATION, format!("Bearer {token}"))
        .header(CONTENT_TYPE, "application/json")
        .body(Body::from(serde_json::to_vec(&save_body).unwrap()))
        .unwrap();
    let resp_save = app.clone().oneshot(req_save).await.unwrap();
    assert_eq!(resp_save.status(), StatusCode::OK);

    // 3. Verify in preferences store
    let saved_dials = prefs_store.get_by_did(&interner, user_did).unwrap();
    assert!(saved_dials.include_replies);
    assert_eq!(saved_dials.min_likes, 3);

    // 4. GET /api/preferences -> returns include_replies: true
    let req_get2 = Request::builder()
        .method(Method::GET)
        .uri("/api/preferences")
        .header(AUTHORIZATION, format!("Bearer {token}"))
        .body(Body::empty())
        .unwrap();
    let resp_get2 = app.clone().oneshot(req_get2).await.unwrap();
    assert_eq!(resp_get2.status(), StatusCode::OK);
    let body_get2 = resp_get2.into_body().collect().await.unwrap().to_bytes();
    let prefs2: PreferencesResponseDto = serde_json::from_slice(&body_get2).unwrap();
    assert!(prefs2.is_custom);
    assert!(prefs2.preferences.include_replies);

    // 5. XRPC query without query params now automatically applies saved include_replies: true
    let req_xrpc = Request::builder()
        .uri("/xrpc/app.bsky.feed.getFeedSkeleton?feed=at://did:web:feed.example.com/app.bsky.feed.generator/for-your-consideration")
        .header(AUTHORIZATION, format!("Bearer {token}"))
        .body(Body::empty())
        .unwrap();
    let resp_xrpc = app.oneshot(req_xrpc).await.unwrap();
    assert_eq!(resp_xrpc.status(), StatusCode::OK);
    let body_xrpc = resp_xrpc.into_body().collect().await.unwrap().to_bytes();
    let skel_xrpc: FeedSkeletonResponse = serde_json::from_slice(&body_xrpc).unwrap();
    let uris: Vec<&str> = skel_xrpc.feed.iter().map(|p| p.post.as_str()).collect();
    assert!(uris.contains(&root_uri.as_str()));
    assert!(
        uris.contains(&reply_uri.as_str()),
        "Saved preferences (include_replies: true) should promote replies"
    );
}

#[test]
fn test_snapshot_v3_preferences_round_trip() {
    let interner = Arc::new(StringInterner::new());
    let graph = Arc::new(GraphStore::new());
    let preferences_save = Arc::new(UserPreferencesStore::new());

    let user_a = interner.intern("did:plc:user_a");
    let user_b = interner.intern("did:plc:user_b");

    let dials_a = UserDials {
        freshness_half_life_secs: 12.0 * 3600.0,
        serendipity_ratio: 0.25,
        topic_weights: TopicWeights {
            art: 2.0,
            tech: 1.0,
            science: 1.0,
            news: 1.0,
            culture: 1.0,
        },
        include_replies: true,
        min_likes: 3,
        updated_at_secs: 1000,
    };

    let dials_b = UserDials {
        freshness_half_life_secs: 48.0 * 3600.0,
        serendipity_ratio: 0.10,
        topic_weights: TopicWeights::default(),
        include_replies: false,
        min_likes: 3,
        updated_at_secs: 2000,
    };

    preferences_save.set(user_a, dials_a);
    preferences_save.set(user_b, dials_b);

    let temp_file = format!(
        "/tmp/snapshot_test_v3_{}_{}.bin",
        std::process::id(),
        current_timestamp()
    );

    save_snapshot_with_preferences(&temp_file, &interner, &graph, &preferences_save, 0)
        .expect("Snapshot save must succeed");

    let preferences_load = Arc::new(UserPreferencesStore::new());
    let loaded = load_snapshot_with_preferences(&temp_file, &interner, &graph, &preferences_load)
        .expect("Snapshot load must succeed")
        .expect("Snapshot must exist");

    assert_eq!(loaded.header.format_version, SNAPSHOT_FORMAT_VERSION);
    assert_eq!(loaded.header.num_preferences, 2);

    let loaded_a = preferences_load
        .get(user_a)
        .expect("User A must be restored");
    assert!(loaded_a.include_replies);
    assert_eq!(loaded_a.freshness_half_life_secs, 12.0 * 3600.0);

    let loaded_b = preferences_load
        .get(user_b)
        .expect("User B must be restored");
    assert!(!loaded_b.include_replies);
    assert_eq!(loaded_b.freshness_half_life_secs, 48.0 * 3600.0);

    let _ = std::fs::remove_file(temp_file);
}
