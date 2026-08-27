#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::suboptimal_flops,
    clippy::cast_lossless,
    clippy::cast_precision_loss,
    missing_docs
)]

//! Comprehensive AT Protocol Lexicon & Dynamic Schema Compliance Invariant Tests.
//!
//! Directly loads and validates raw `serde_json::Value` XRPC responses against
//! the official upstream Bluesky Lexicon JSON specifications:
//! - `app.bsky.feed.getFeedSkeleton`
//! - `app.bsky.feed.defs#skeletonFeedPost`
//! - `app.bsky.feed.describeFeedGenerator`
//! - `/.well-known/did.json` (W3C DID Document)
//!
//! Dynamically extracts required properties, allowed field names, and format constraints
//! from the official Lexicon ASTs to guarantee 100% protocol alignment.

mod common;

use std::collections::HashSet;
use std::sync::Arc;

use axum::body::to_bytes;
use axum::http::{Request, StatusCode};
use for_your_consideration::prelude::*;
use proptest::prelude::*;
use serde_json::Value;
use tower::ServiceExt;

use crate::common::SyntheticGraphBuilder;

const GET_FEED_SKELETON_LEXICON_STR: &str =
    include_str!("../lexicons/app/bsky/feed/getFeedSkeleton.json");
const DEFS_LEXICON_STR: &str = include_str!("../lexicons/app/bsky/feed/defs.json");
const DESCRIBE_FEED_GEN_LEXICON_STR: &str =
    include_str!("../lexicons/app/bsky/feed/describeFeedGenerator.json");

/// Dynamic validator that checks responses against official `ATProto` Lexicon JSON schemas.
#[derive(Debug)]
struct OfficialLexiconValidator {
    get_feed_skeleton: Value,
    defs: Value,
    describe_feed_generator: Value,
}

impl OfficialLexiconValidator {
    fn new() -> Self {
        Self {
            get_feed_skeleton: serde_json::from_str(GET_FEED_SKELETON_LEXICON_STR)
                .expect("Valid getFeedSkeleton.json"),
            defs: serde_json::from_str(DEFS_LEXICON_STR).expect("Valid defs.json"),
            describe_feed_generator: serde_json::from_str(DESCRIBE_FEED_GEN_LEXICON_STR)
                .expect("Valid describeFeedGenerator.json"),
        }
    }

    /// Dynamically validates a `getFeedSkeleton` JSON response against official Lexicon rules.
    fn validate_feed_skeleton_response(&self, response_json: &Value) {
        let root_obj = response_json
            .as_object()
            .expect("Root getFeedSkeleton response must be a JSON object");

        // Extract top-level properties from getFeedSkeleton schema
        let schema = &self.get_feed_skeleton["defs"]["main"]["output"]["schema"];
        let allowed_top_keys: HashSet<&str> = schema["properties"]
            .as_object()
            .expect("Schema properties object")
            .keys()
            .map(String::as_str)
            .collect();

        let required_top_keys: Vec<&str> = schema["required"]
            .as_array()
            .expect("Schema required array")
            .iter()
            .filter_map(Value::as_str)
            .collect();

        // 1. Verify required top-level fields
        for req in required_top_keys {
            assert!(
                root_obj.contains_key(req),
                "Response missing required top-level property '{req}' defined in official Lexicon"
            );
        }

        // 2. Verify allowed top-level fields
        for key in root_obj.keys() {
            assert!(
                allowed_top_keys.contains(key.as_str()),
                "Response contains property '{key}' not defined in official getFeedSkeleton Lexicon. Allowed: {allowed_top_keys:?}"
            );
        }

        // 3. Extract item properties from defs#skeletonFeedPost
        let item_schema = &self.defs["defs"]["skeletonFeedPost"];
        let allowed_item_keys: HashSet<&str> = item_schema["properties"]
            .as_object()
            .expect("defs#skeletonFeedPost properties object")
            .keys()
            .map(String::as_str)
            .collect();

        let required_item_keys: Vec<&str> = item_schema["required"]
            .as_array()
            .expect("defs#skeletonFeedPost required array")
            .iter()
            .filter_map(Value::as_str)
            .collect();

        let feed_array = root_obj["feed"]
            .as_array()
            .expect("'feed' must be a JSON array");

        for (idx, item) in feed_array.iter().enumerate() {
            let item_obj = item
                .as_object()
                .unwrap_or_else(|| panic!("Item #{idx} in feed must be a JSON object"));

            // Check required item properties
            for req in &required_item_keys {
                assert!(
                    item_obj.contains_key(*req),
                    "Feed item #{idx} missing required property '{req}' from defs#skeletonFeedPost"
                );
            }

            // Check allowed item properties
            for key in item_obj.keys() {
                assert!(
                    allowed_item_keys.contains(key.as_str()),
                    "Feed item #{idx} contains property '{key}' not allowed in defs#skeletonFeedPost. Allowed: {allowed_item_keys:?}"
                );
            }

            // Verify post is valid AT-URI format
            let post_uri = item_obj["post"].as_str().expect("post must be string");
            assert!(
                post_uri.starts_with("at://did:"),
                "post uri '{post_uri}' must start with at://did:"
            );
            assert!(
                post_uri.contains("/app.bsky.feed.post/"),
                "post uri '{post_uri}' must contain /app.bsky.feed.post/"
            );

            // Verify feedContext if present
            if let Some(ctx_val) = item_obj.get("feedContext") {
                assert!(
                    ctx_val.is_string(),
                    "feedContext must be a string if present"
                );
            }

            // Explicitly assert NO snake_case feed_context
            assert!(
                !item_obj.contains_key("feed_context"),
                "FATAL: Feed item #{idx} contains snake_case 'feed_context', breaking Bluesky schema!"
            );
        }
    }

    /// Dynamically validates a `describeFeedGenerator` response against official Lexicon rules.
    fn validate_describe_feed_generator_response(&self, response_json: &Value) {
        let root_obj = response_json
            .as_object()
            .expect("Root describeFeedGenerator response must be a JSON object");

        let schema = &self.describe_feed_generator["defs"]["main"]["output"]["schema"];
        let allowed_top_keys: HashSet<&str> = schema["properties"]
            .as_object()
            .expect("describeFeedGenerator schema properties")
            .keys()
            .map(String::as_str)
            .collect();

        let required_top_keys: Vec<&str> = schema["required"]
            .as_array()
            .expect("describeFeedGenerator schema required")
            .iter()
            .filter_map(Value::as_str)
            .collect();

        for req in required_top_keys {
            assert!(
                root_obj.contains_key(req),
                "describeFeedGenerator missing required property '{req}'"
            );
        }

        for key in root_obj.keys() {
            assert!(
                allowed_top_keys.contains(key.as_str()),
                "describeFeedGenerator has disallowed property '{key}'"
            );
        }

        let feeds = root_obj["feeds"].as_array().expect("feeds is an array");
        assert!(
            !feeds.is_empty(),
            "describeFeedGenerator feeds array must not be empty"
        );

        let feed_schema = &self.describe_feed_generator["defs"]["feed"];
        let allowed_feed_keys: HashSet<&str> = feed_schema["properties"]
            .as_object()
            .expect("feed schema properties")
            .keys()
            .map(String::as_str)
            .collect();

        for feed in feeds {
            let feed_obj = feed.as_object().expect("feed item is an object");
            for key in feed_obj.keys() {
                assert!(
                    allowed_feed_keys.contains(key.as_str()),
                    "feed item contains disallowed key '{key}'"
                );
            }
            let uri = feed_obj["uri"].as_str().expect("feed uri is a string");
            assert!(
                uri.starts_with("at://did:"),
                "feed uri '{uri}' must start with at://did:"
            );
            assert!(
                uri.contains("/app.bsky.feed.generator/"),
                "feed uri '{uri}' must contain /app.bsky.feed.generator/"
            );
        }

        if let Some(links) = root_obj.get("links") {
            let links_obj = links.as_object().expect("links must be an object");
            let links_schema = &self.describe_feed_generator["defs"]["links"];
            let allowed_link_keys: HashSet<&str> = links_schema["properties"]
                .as_object()
                .expect("links schema properties")
                .keys()
                .map(String::as_str)
                .collect();

            for key in links_obj.keys() {
                assert!(
                    allowed_link_keys.contains(key.as_str()),
                    "links object contains disallowed key '{key}'"
                );
            }
        }
    }
}

/// Recursively inspects a JSON AST and collects all keys.
fn collect_all_keys(val: &Value, keys: &mut Vec<String>) {
    match val {
        Value::Object(map) => {
            for (k, v) in map {
                keys.push(k.clone());
                collect_all_keys(v, keys);
            }
        }
        Value::Array(arr) => {
            for item in arr {
                collect_all_keys(item, keys);
            }
        }
        _ => {}
    }
}

/// Helper creating a populated test application state.
fn setup_test_app() -> (axum::Router, Arc<GraphStore>, Arc<StringInterner>, u64) {
    let interner = Arc::new(StringInterner::new());
    let graph = Arc::new(GraphStore::new());
    let preferences_store = Arc::new(UserPreferencesStore::new());
    let recommender = Arc::new(Recommender::new(Arc::clone(&interner), Arc::clone(&graph)));
    let snapshot_tracker = Arc::new(SnapshotStatusTracker::new(&SnapshotConfig::default()));
    let now_secs = 1_724_000_000u64;

    let app_state = AppState::new(recommender, "did:web:feed.example.com", "feed.example.com")
        .with_preferences_store(preferences_store)
        .with_snapshot_tracker(snapshot_tracker);

    SyntheticGraphBuilder::new()
        .add_user("did:plc:alice")
        .add_user("did:plc:bob")
        .add_user("did:plc:carol")
        .add_post(
            "at://did:plc:alice/app.bsky.feed.post/post1",
            "did:plc:alice",
            None::<&str>,
            None::<&str>,
            now_secs - 300,
        )
        .add_post(
            "at://did:plc:bob/app.bsky.feed.post/post2",
            "did:plc:bob",
            None::<&str>,
            None::<&str>,
            now_secs - 200,
        )
        .add_post(
            "at://did:plc:carol/app.bsky.feed.post/post3",
            "did:plc:carol",
            None::<&str>,
            None::<&str>,
            now_secs - 100,
        )
        .add_interaction(
            "did:plc:alice",
            "at://did:plc:bob/app.bsky.feed.post/post2",
            SignalType::Like,
            now_secs - 150,
        )
        .add_interaction(
            "did:plc:bob",
            "at://did:plc:alice/app.bsky.feed.post/post1",
            SignalType::Like,
            now_secs - 120,
        )
        .add_interaction(
            "did:plc:carol",
            "at://did:plc:bob/app.bsky.feed.post/post2",
            SignalType::Like,
            now_secs - 90,
        )
        .add_follow("did:plc:alice", "did:plc:bob")
        .populate(&interner, &graph);

    let router = create_xrpc_router(app_state);
    (router, graph, interner, now_secs)
}

#[tokio::test]
async fn test_dynamic_lexicon_schema_validation_for_get_feed_skeleton() {
    let (router, _graph, _interner, _now) = setup_test_app();
    let validator = OfficialLexiconValidator::new();

    let uri = "/xrpc/app.bsky.feed.getFeedSkeleton?feed=at://did:web:feed.example.com/app.bsky.feed.generator/fyc&explain=true&limit=10";
    let req = Request::builder()
        .method("GET")
        .uri(uri)
        .body(axum::body::Body::empty())
        .unwrap();

    let resp = router.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let body_bytes = to_bytes(resp.into_body(), 64 * 1024).await.unwrap();
    let root: Value = serde_json::from_slice(&body_bytes).expect("Valid JSON response");

    // Dynamic assertion against official Lexicon AST
    validator.validate_feed_skeleton_response(&root);

    // Global check: Zero snake_case keys in payload
    let mut all_keys = Vec::new();
    collect_all_keys(&root, &mut all_keys);
    for key in all_keys {
        assert!(
            !key.contains('_'),
            "Forbidden snake_case key '{key}' in getFeedSkeleton payload"
        );
    }
}

#[tokio::test]
async fn test_dynamic_lexicon_schema_validation_for_describe_feed_generator() {
    let (router, _graph, _interner, _now) = setup_test_app();
    let validator = OfficialLexiconValidator::new();

    let req = Request::builder()
        .method("GET")
        .uri("/xrpc/app.bsky.feed.describeFeedGenerator")
        .body(axum::body::Body::empty())
        .unwrap();

    let resp = router.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let body_bytes = to_bytes(resp.into_body(), 64 * 1024).await.unwrap();
    let root: Value = serde_json::from_slice(&body_bytes).expect("Valid JSON response");

    // Dynamic assertion against official Lexicon AST
    validator.validate_describe_feed_generator_response(&root);

    let mut all_keys = Vec::new();
    collect_all_keys(&root, &mut all_keys);
    for key in all_keys {
        assert!(
            !key.contains('_'),
            "Forbidden snake_case key '{key}' in describeFeedGenerator payload"
        );
    }
}

#[tokio::test]
async fn test_schema_well_known_did_doc_strict_w3c_compliance() {
    let (router, _graph, _interner, _now) = setup_test_app();

    let req = Request::builder()
        .method("GET")
        .uri("/.well-known/did.json")
        .body(axum::body::Body::empty())
        .unwrap();

    let resp = router.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::OK);

    let body_bytes = to_bytes(resp.into_body(), 64 * 1024).await.unwrap();
    let root: Value = serde_json::from_slice(&body_bytes).expect("Valid JSON response");
    let root_obj = root.as_object().expect("DID doc is a JSON object");

    assert!(
        root_obj.contains_key("@context"),
        "Missing '@context' in DID document"
    );

    let id = root_obj["id"].as_str().expect("DID doc id is a string");
    assert!(id.starts_with("did:"), "DID doc id must start with did:");

    let services = root_obj["service"].as_array().expect("service is an array");
    for s in services {
        let s_obj = s.as_object().expect("service item is an object");
        assert_eq!(
            s_obj.get("type").and_then(Value::as_str),
            Some("BskyFeedGenerator"),
            "service.type must be BskyFeedGenerator"
        );
        assert!(
            s_obj.contains_key("serviceEndpoint"),
            "service must contain 'serviceEndpoint' in camelCase"
        );
        assert!(
            !s_obj.contains_key("service_endpoint"),
            "service must NOT contain snake_case 'service_endpoint'"
        );
    }
}

#[tokio::test]
async fn test_schema_xrpc_standard_error_payload_invariants() {
    let (router, _graph, _interner, _now) = setup_test_app();

    let req = Request::builder()
        .method("GET")
        .uri("/xrpc/app.bsky.feed.getFeedSkeleton")
        .body(axum::body::Body::empty())
        .unwrap();

    let resp = router.oneshot(req).await.unwrap();
    assert_eq!(resp.status(), StatusCode::BAD_REQUEST);

    let body_bytes = to_bytes(resp.into_body(), 64 * 1024).await.unwrap();
    let root: Value = serde_json::from_slice(&body_bytes).expect("Valid error JSON response");
    let root_obj = root.as_object().expect("Error response is a JSON object");

    for key in root_obj.keys() {
        assert!(
            key == "error" || key == "message",
            "XRPC error object only allows 'error' and 'message' fields, found '{key}'"
        );
    }
    assert!(
        root_obj["error"].is_string(),
        "'error' identifier must be a string"
    );
    assert!(
        root_obj["message"].is_string(),
        "'message' description must be a string"
    );
}

proptest! {
    #![proptest_config(ProptestConfig::with_cases(20))]
    #[test]
    fn prop_get_feed_skeleton_raw_json_dynamic_lexicon_compliance(
        limit in 1usize..50,
        explain in proptest::bool::ANY,
        include_replies in proptest::bool::ANY,
        min_likes in 0u32..20
    ) {
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();

        rt.block_on(async move {
            let (router, _graph, _interner, _now) = setup_test_app();
            let validator = OfficialLexiconValidator::new();

            let uri = format!(
                "/xrpc/app.bsky.feed.getFeedSkeleton?feed=at://did:web:feed.example.com/app.bsky.feed.generator/fyc&limit={limit}&explain={explain}&replies={include_replies}&min_likes={min_likes}"
            );

            let req = Request::builder()
                .method("GET")
                .uri(&uri)
                .body(axum::body::Body::empty())
                .unwrap();

            let resp = router.oneshot(req).await.unwrap();
            prop_assert_eq!(resp.status(), StatusCode::OK);

            let body_bytes = to_bytes(resp.into_body(), 64 * 1024).await.unwrap();
            let root: Value = serde_json::from_slice(&body_bytes).expect("Valid JSON response");

            // Validate against official schema
            validator.validate_feed_skeleton_response(&root);

            let mut all_keys = Vec::new();
            collect_all_keys(&root, &mut all_keys);

            for key in all_keys {
                prop_assert!(
                    !key.contains('_'),
                    "Proptest found forbidden snake_case key '{}' in getFeedSkeleton JSON response for uri '{}'",
                    key,
                    uri
                );
            }

            Ok(())
        })?;
    }
}
