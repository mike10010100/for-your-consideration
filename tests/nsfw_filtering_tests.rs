#![forbid(unsafe_code)]
#![allow(clippy::unwrap_used, clippy::expect_used, clippy::panic, missing_docs)]

//! End-to-end verification of the per-user "No NSFW posts" feed config toggle.
//!
//! Covers:
//! 1. Author self-label parsing (`com.atproto.label.defs#selfLabels`) during ingestion.
//! 2. `is_nsfw` propagation through `PostMeta` and snapshot persistence.
//! 3. Candidate filtering across `recommend` and `recommend_preview_at`.
//! 4. HTTP override precedence (`?no_nsfw=` / `?safe_mode=`) on the skeleton + preview endpoints.
//! 5. Preference persistence round-trip of the `no_nsfw` dial.

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use axum::body::Body;
use axum::http::header::{AUTHORIZATION, CONTENT_TYPE};
use axum::http::{Method, Request, StatusCode};
use http_body_util::BodyExt;
use tower::ServiceExt;

use for_your_consideration::ingest::{
    apply_event_to_graph, has_nsfw_self_label, parse_jetstream_frame,
};
use for_your_consideration::prelude::*;

fn current_timestamp() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

struct NsfwEnvironment {
    state: AppState,
    interner: Arc<StringInterner>,
    preferences: Arc<UserPreferencesStore>,
    recommender: Arc<Recommender>,
    viewer_did: String,
    clean_pid: u32,
    nsfw_pid: u32,
}

fn create_nsfw_environment() -> NsfwEnvironment {
    let interner = Arc::new(StringInterner::new());
    let graph = Arc::new(GraphStore::new());
    let preferences = Arc::new(UserPreferencesStore::new());

    let now = current_timestamp();
    let viewer_did = "did:plc:nsfw_viewer".to_string();

    let viewer_id = interner.intern(&viewer_did);
    let twin_id = interner.intern("did:plc:nsfw_twin");
    let seed_author = interner.intern("did:plc:seed_author");
    let clean_author = interner.intern("did:plc:clean_author");
    let nsfw_author = interner.intern("did:plc:nsfw_author");

    let seed_post = interner.intern("at://did:plc:seed_author/app.bsky.feed.post/seed");
    let clean_post = interner.intern("at://did:plc:clean_author/app.bsky.feed.post/clean");
    let nsfw_post = interner.intern("at://did:plc:nsfw_author/app.bsky.feed.post/adult");

    // 12 shared seed likes -> Tier 1 eligibility (likes >= 10, twin overlap >= 2).
    for i in 0..12 {
        let dummy = interner.intern(&format!(
            "at://did:plc:seed_author/app.bsky.feed.post/dummy_{i}"
        ));
        graph.record_post_meta(dummy, seed_author, None, None, now - 100);
        graph.record_interaction(viewer_id, dummy, SignalType::Like, now - 100);
        graph.record_interaction(twin_id, dummy, SignalType::Like, now - 100);
    }
    graph.record_post_meta(seed_post, seed_author, None, None, now - 90);
    graph.record_interaction(viewer_id, seed_post, SignalType::Like, now - 90);
    graph.record_interaction(twin_id, seed_post, SignalType::Like, now - 90);

    // Clean candidate: twin-liked root post.
    graph.record_post_meta(clean_post, clean_author, None, None, now - 50);
    graph.record_interaction(twin_id, clean_post, SignalType::Like, now - 50);

    // NSFW candidate: twin-liked root post flagged by author self-label.
    graph.record_post_meta_with_nsfw(nsfw_post, nsfw_author, None, None, now - 40, true);
    graph.record_interaction(twin_id, nsfw_post, SignalType::Like, now - 40);

    // Baseline engagement so both candidates clear the default min_likes floor.
    let u1 = interner.intern("did:plc:floor_user_1");
    let u2 = interner.intern("did:plc:floor_user_2");
    for &p in &[clean_post, nsfw_post] {
        graph.record_interaction(u1, p, SignalType::Like, now - 45);
        graph.record_interaction(u2, p, SignalType::Like, now - 45);
    }

    let recommender = Arc::new(Recommender::new(Arc::clone(&interner), Arc::clone(&graph)));
    let state = AppState::new(
        Arc::clone(&recommender),
        "did:web:feed.example.com",
        "feed.example.com",
    )
    .with_preferences_store(Arc::clone(&preferences));

    NsfwEnvironment {
        state,
        interner,
        preferences,
        recommender,
        viewer_did,
        clean_pid: clean_post,
        nsfw_pid: nsfw_post,
    }
}

#[test]
fn test_nsfw_self_label_parsing_variants() {
    let object_form = serde_json::json!({
        "labels": { "values": [ { "val": "porn" } ] }
    });
    assert!(has_nsfw_self_label(&object_form));

    let string_form = serde_json::json!({
        "labels": { "values": [ " Sexual " ] }
    });
    assert!(has_nsfw_self_label(&string_form));

    let safe = serde_json::json!({
        "labels": { "values": [ { "val": "spoiler" } ] }
    });
    assert!(!has_nsfw_self_label(&safe));

    let no_labels = serde_json::json!({ "text": "hello" });
    assert!(!has_nsfw_self_label(&no_labels));
}

#[test]
fn test_ingest_post_with_nsfw_self_label_sets_post_meta() {
    let interner = StringInterner::new();
    let graph = GraphStore::new();

    let json = serde_json::json!({
        "did": "did:plc:author",
        "time_us": 1_700_000_000_000_000u64,
        "kind": "commit",
        "commit": {
            "rev": "rev1",
            "operation": "create",
            "collection": "app.bsky.feed.post",
            "rkey": "adult1",
            "record": {
                "$type": "app.bsky.feed.post",
                "text": "explicit content",
                "createdAt": "2023-11-14T22:13:20.000Z",
                "labels": { "$type": "com.atproto.label.defs#selfLabels", "values": [ { "val": "porn" } ] }
            }
        }
    })
    .to_string();

    let (events, _) = parse_jetstream_frame(&json).expect("frame must parse");
    for event in &events {
        apply_event_to_graph(event, &interner, &graph);
    }

    let pid = interner
        .lookup_id("at://did:plc:author/app.bsky.feed.post/adult1")
        .expect("post interned");
    let meta = graph.get_post_meta(pid).expect("post meta recorded");
    assert!(meta.is_nsfw(), "NSFW self-label must set is_nsfw");
}

#[test]
fn test_post_meta_nsfw_flag_round_trips_graph_and_snapshot() {
    let interner = Arc::new(StringInterner::new());
    let graph = Arc::new(GraphStore::new());
    let preferences = Arc::new(UserPreferencesStore::new());

    let author = interner.intern("did:plc:snap_author");
    let clean = interner.intern("at://did:plc:snap_author/app.bsky.feed.post/clean");
    let adult = interner.intern("at://did:plc:snap_author/app.bsky.feed.post/adult");
    let now = current_timestamp();

    graph.record_post_meta(clean, author, None, None, now);
    graph.record_post_meta_with_nsfw(adult, author, None, None, now, true);

    assert!(!graph.get_post_meta(clean).unwrap().is_nsfw());
    assert!(graph.get_post_meta(adult).unwrap().is_nsfw());

    let path = format!(
        "/tmp/nsfw_snapshot_{}_{}.bin",
        std::process::id(),
        current_timestamp()
    );
    save_snapshot_with_preferences(&path, &interner, &graph, &preferences, now).unwrap();

    let loaded_interner = Arc::new(StringInterner::new());
    let loaded_graph = Arc::new(GraphStore::new());
    let loaded_prefs = Arc::new(UserPreferencesStore::new());
    let loaded =
        load_snapshot_with_preferences(&path, &loaded_interner, &loaded_graph, &loaded_prefs)
            .unwrap()
            .unwrap();
    assert_eq!(loaded.header.format_version, SNAPSHOT_FORMAT_VERSION);

    let loaded_clean = loaded_interner
        .lookup_id("at://did:plc:snap_author/app.bsky.feed.post/clean")
        .unwrap();
    let loaded_adult = loaded_interner
        .lookup_id("at://did:plc:snap_author/app.bsky.feed.post/adult")
        .unwrap();
    assert!(!loaded_graph.get_post_meta(loaded_clean).unwrap().is_nsfw());
    assert!(loaded_graph.get_post_meta(loaded_adult).unwrap().is_nsfw());

    let _ = std::fs::remove_file(path);
}

#[test]
fn test_recommend_honors_no_nsfw_dial() {
    let env = create_nsfw_environment();
    let clean_uri = env.interner.lookup_str(env.clean_pid).unwrap();
    let nsfw_uri = env.interner.lookup_str(env.nsfw_pid).unwrap();
    let now = current_timestamp();

    // Default: NSFW allowed.
    let permissive = RecommendationDials {
        limit: 50,
        ..Default::default()
    };
    let recs = env
        .recommender
        .recommend(Some(&env.viewer_did), &permissive, now)
        .unwrap();
    let uris: Vec<&str> = recs.posts.iter().map(|p| p.uri.as_str()).collect();
    assert!(uris.contains(&clean_uri.as_str()));
    assert!(
        uris.contains(&nsfw_uri.as_str()),
        "NSFW post must appear when no_nsfw=false"
    );

    // no_nsfw = true: NSFW suppressed, clean retained.
    let safe = RecommendationDials {
        no_nsfw: true,
        limit: 50,
        ..Default::default()
    };
    let recs_safe = env
        .recommender
        .recommend(Some(&env.viewer_did), &safe, now)
        .unwrap();
    let uris_safe: Vec<&str> = recs_safe.posts.iter().map(|p| p.uri.as_str()).collect();
    assert!(uris_safe.contains(&clean_uri.as_str()));
    assert!(
        !uris_safe.contains(&nsfw_uri.as_str()),
        "NSFW post must be filtered when no_nsfw=true"
    );
}

#[test]
fn test_recommend_preview_honors_no_nsfw_dial() {
    let env = create_nsfw_environment();
    let clean_uri = env.interner.lookup_str(env.clean_pid).unwrap();
    let nsfw_uri = env.interner.lookup_str(env.nsfw_pid).unwrap();
    let now = current_timestamp();

    let safe = RecommendationDials {
        no_nsfw: true,
        limit: 50,
        explain: true,
        ..Default::default()
    };
    let preview = env
        .recommender
        .recommend_preview_at(Some(&env.viewer_did), &safe, now)
        .unwrap();
    let uris: Vec<&str> = preview.items.iter().map(|c| c.uri.as_str()).collect();
    assert!(uris.contains(&clean_uri.as_str()));
    assert!(!uris.contains(&nsfw_uri.as_str()));
}

#[tokio::test]
async fn test_xrpc_skeleton_no_nsfw_query_override() {
    let env = create_nsfw_environment();
    let clean_uri = env.interner.lookup_str(env.clean_pid).unwrap();
    let nsfw_uri = env.interner.lookup_str(env.nsfw_pid).unwrap();
    let app = create_xrpc_router(env.state);
    let token = generate_session_token(&env.viewer_did, 3600);
    let feed = "at://did:web:feed.example.com/app.bsky.feed.generator/for-your-consideration";

    // no_nsfw=true suppresses the NSFW post.
    let req = Request::builder()
        .uri(format!(
            "/xrpc/app.bsky.feed.getFeedSkeleton?feed={feed}&no_nsfw=true"
        ))
        .header(AUTHORIZATION, format!("Bearer {token}"))
        .body(Body::empty())
        .unwrap();
    let resp = app.clone().oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = resp.into_body().collect().await.unwrap().to_bytes();
    let skel: FeedSkeletonResponse = serde_json::from_slice(&body).unwrap();
    let uris: Vec<&str> = skel.feed.iter().map(|p| p.post.as_str()).collect();
    assert!(uris.contains(&clean_uri.as_str()));
    assert!(!uris.contains(&nsfw_uri.as_str()));

    // safe_mode=on alias also suppresses the NSFW post.
    let req_alias = Request::builder()
        .uri(format!(
            "/xrpc/app.bsky.feed.getFeedSkeleton?feed={feed}&safe_mode=on"
        ))
        .header(AUTHORIZATION, format!("Bearer {token}"))
        .body(Body::empty())
        .unwrap();
    let resp_alias = app.clone().oneshot(req_alias).await.unwrap();
    assert_eq!(resp_alias.status(), StatusCode::OK);
    let body_alias = resp_alias.into_body().collect().await.unwrap().to_bytes();
    let skel_alias: FeedSkeletonResponse = serde_json::from_slice(&body_alias).unwrap();
    assert!(!skel_alias
        .feed
        .iter()
        .any(|p| p.post.as_str() == nsfw_uri.as_str()));

    // Explicit no_nsfw=false allows the NSFW post again.
    let req_off = Request::builder()
        .uri(format!(
            "/xrpc/app.bsky.feed.getFeedSkeleton?feed={feed}&no_nsfw=false"
        ))
        .header(AUTHORIZATION, format!("Bearer {token}"))
        .body(Body::empty())
        .unwrap();
    let resp_off = app.oneshot(req_off).await.unwrap();
    let body_off = resp_off.into_body().collect().await.unwrap().to_bytes();
    let skel_off: FeedSkeletonResponse = serde_json::from_slice(&body_off).unwrap();
    assert!(skel_off
        .feed
        .iter()
        .any(|p| p.post.as_str() == nsfw_uri.as_str()));
}

#[tokio::test]
async fn test_xrpc_preview_no_nsfw_query_override() {
    let env = create_nsfw_environment();
    let nsfw_uri = env.interner.lookup_str(env.nsfw_pid).unwrap();
    let app = create_xrpc_router(env.state);

    let req = Request::builder()
        .uri(format!(
            "/api/feed-preview?viewer={}&no_nsfw=true&limit=50",
            env.viewer_did
        ))
        .body(Body::empty())
        .unwrap();
    let resp = app.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let body = resp.into_body().collect().await.unwrap().to_bytes();
    let preview: FeedPreviewResponse = serde_json::from_slice(&body).unwrap();
    assert!(!preview
        .items
        .iter()
        .any(|c| c.uri.as_str() == nsfw_uri.as_str()));
}

#[tokio::test]
async fn test_preferences_round_trip_persists_no_nsfw() {
    let env = create_nsfw_environment();
    let app = create_xrpc_router(env.state);
    let token = generate_session_token(&env.viewer_did, 3600);

    // Default preferences -> no_nsfw false.
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
    assert!(!prefs1.preferences.no_nsfw);
    assert!(!prefs1.dials.as_ref().unwrap().no_nsfw);

    // Save with no_nsfw = true.
    let save_body = SavePreferencesRequestBody {
        freshness_hours: 24.0,
        discovery_ratio: 0.15,
        topic_weights: Some(TopicWeights::default()),
        include_replies: None,
        no_nsfw: Some(true),
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

    // Persisted store reflects it.
    let saved = env
        .preferences
        .get_by_did(&env.interner, &env.viewer_did)
        .unwrap();
    assert!(saved.no_nsfw);

    // Serialized response reflects it.
    let req_get2 = Request::builder()
        .method(Method::GET)
        .uri("/api/preferences")
        .header(AUTHORIZATION, format!("Bearer {token}"))
        .body(Body::empty())
        .unwrap();
    let resp_get2 = app.oneshot(req_get2).await.unwrap();
    let body_get2 = resp_get2.into_body().collect().await.unwrap().to_bytes();
    let prefs2: PreferencesResponseDto = serde_json::from_slice(&body_get2).unwrap();
    assert!(prefs2.is_custom);
    assert!(prefs2.preferences.no_nsfw);
    assert!(prefs2.dials.as_ref().unwrap().no_nsfw);
}
