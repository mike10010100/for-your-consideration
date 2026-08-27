#![allow(dead_code, unused_imports, clippy::pedantic, clippy::nursery)]

use std::collections::HashSet;
use std::net::SocketAddr;
use std::sync::Arc;

use axum::extract::{Query, State};
use axum::http::{HeaderMap, StatusCode};
use axum::response::IntoResponse;
use axum::routing::get;
use axum::{Json, Router};
use compact_str::CompactString;
use for_your_consideration::prelude::*;
use futures_util::SinkExt;
use serde::Deserialize;
use tokio::net::TcpListener;
use tokio::sync::{mpsc, Mutex};
use tokio_tungstenite::tungstenite::Message;

// ---------------------------------------------------------------------------
// 1. Synthetic Graph Builder & Pre-canned Fixtures
// ---------------------------------------------------------------------------

/// Helper for constructing deterministic synthetic graphs with controlled topologies.
#[derive(Debug, Default)]
pub struct SyntheticGraphBuilder {
    users: Vec<CompactString>,
    posts: Vec<CompactString>,
    interactions: Vec<(CompactString, CompactString, SignalType, u64)>,
    follows: Vec<(CompactString, CompactString)>,
    post_metas: Vec<(
        CompactString,
        CompactString,
        Option<CompactString>,
        Option<CompactString>,
        u64,
    )>,
}

impl SyntheticGraphBuilder {
    #[must_use]
    pub fn new() -> Self {
        Self::default()
    }

    /// Adds a user DID if not already present.
    pub fn add_user(mut self, user_did: impl Into<CompactString>) -> Self {
        let u = user_did.into();
        if !self.users.contains(&u) {
            self.users.push(u);
        }
        self
    }

    /// Adds a post AT-URI with metadata.
    pub fn add_post(
        mut self,
        post_uri: impl Into<CompactString>,
        author_did: impl Into<CompactString>,
        root_uri: Option<impl Into<CompactString>>,
        parent_uri: Option<impl Into<CompactString>>,
        created_at: u64,
    ) -> Self {
        let p = post_uri.into();
        let a = author_did.into();
        let r = root_uri.map(Into::into);
        let par = parent_uri.map(Into::into);

        if !self.posts.contains(&p) {
            self.posts.push(p.clone());
        }
        if !self.users.contains(&a) {
            self.users.push(a.clone());
        }
        self.post_metas.push((p, a, r, par, created_at));
        self
    }

    /// Records an interaction between a user and a post.
    pub fn add_interaction(
        mut self,
        user_did: impl Into<CompactString>,
        post_uri: impl Into<CompactString>,
        signal: SignalType,
        timestamp: u64,
    ) -> Self {
        let u = user_did.into();
        let p = post_uri.into();
        if !self.users.contains(&u) {
            self.users.push(u.clone());
        }
        if !self.posts.contains(&p) {
            self.posts.push(p.clone());
        }
        self.interactions.push((u, p, signal, timestamp));
        self
    }

    /// Records a directed follow relationship.
    pub fn add_follow(
        mut self,
        follower_did: impl Into<CompactString>,
        followed_did: impl Into<CompactString>,
    ) -> Self {
        let f = follower_did.into();
        let t = followed_did.into();
        if !self.users.contains(&f) {
            self.users.push(f.clone());
        }
        if !self.users.contains(&t) {
            self.users.push(t.clone());
        }
        self.follows.push((f, t));
        self
    }

    /// Builds and populates the given `StringInterner` and `GraphStore`.
    pub fn populate(self, interner: &StringInterner, graph: &GraphStore) {
        // Intern all users and posts
        for u in &self.users {
            interner.intern(u);
        }
        for p in &self.posts {
            interner.intern(p);
        }

        // Populate post metadata
        for (p, a, r, par, ts) in self.post_metas {
            let pid = interner.intern(&p);
            let aid = interner.intern(&a);
            let rid = r.as_ref().map(|root| interner.intern(root));
            let paid = par.as_ref().map(|parent| interner.intern(parent));
            graph.record_post_meta(pid, aid, rid, paid, ts);
        }

        // Populate interactions
        for (u, p, sig, ts) in self.interactions {
            let uid = interner.intern(&u);
            let pid = interner.intern(&p);
            graph.record_interaction(uid, pid, sig, ts);
        }

        // Populate follows
        for (f, t) in self.follows {
            let fid = interner.intern(&f);
            let tid = interner.intern(&t);
            graph.record_follow(fid, tid);
        }
    }

    /// Creates standard cold-start test fixture:
    /// - `did:plc:active_user`: has 15 likes on various posts (Tier 1 candidate)
    /// - `did:plc:new_user`: has 2 likes and follows 3 accounts (Tier 2 candidate)
    /// - `did:plc:cold_user`: has 0 likes and 0 follows (Tier 3 candidate)
    /// - Global high-velocity trending posts with timestamps in the last 2 hours.
    pub fn standard_cold_start_fixture(now_secs: u64) -> (Arc<StringInterner>, Arc<GraphStore>) {
        let interner = Arc::new(StringInterner::new());
        let graph = Arc::new(GraphStore::new());

        let mut builder = Self::new();

        // Authors
        let author_a = "did:plc:author_alpha";
        let author_b = "did:plc:author_beta";

        // Global trending posts (recent within 6h)
        for i in 1..=10 {
            let p_uri = format!("at://{author_a}/app.bsky.feed.post/trending_{i}");
            builder = builder.add_post(
                p_uri.clone(),
                author_a,
                None::<&str>,
                None::<&str>,
                now_secs - 3600,
            );
            // Many users interacting to create high velocity
            for u in 1..=8 {
                let user = format!("did:plc:trend_user_{u}");
                builder = builder.add_interaction(
                    user,
                    p_uri.clone(),
                    SignalType::Like,
                    now_secs - (3600 - u as u64 * 60),
                );
            }
        }

        // Active user: 15 likes on author B posts
        let active_user = "did:plc:active_user";
        for i in 1..=15 {
            let p_uri = format!("at://{author_b}/app.bsky.feed.post/active_post_{i}");
            builder = builder.add_post(
                p_uri.clone(),
                author_b,
                None::<&str>,
                None::<&str>,
                now_secs - 7200,
            );
            builder = builder.add_interaction(
                active_user,
                p_uri.clone(),
                SignalType::Like,
                now_secs - 7000,
            );

            // Co-interactors on these posts
            let co_user = format!("did:plc:co_interactor_{}", (i % 3) + 1);
            builder =
                builder.add_interaction(co_user.clone(), p_uri, SignalType::Like, now_secs - 6800);

            // Co-interactors also like some candidate recommendation posts
            let cand_author = format!("did:plc:author_gamma_{}", (i % 8) + 1);
            let cand_uri = format!("at://{cand_author}/app.bsky.feed.post/candidate_post_{i}");
            builder = builder.add_post(
                cand_uri.clone(),
                cand_author,
                None::<&str>,
                None::<&str>,
                now_secs - 5000,
            );
            builder = builder.add_interaction(
                co_user,
                cand_uri.clone(),
                SignalType::Repost,
                now_secs - 4800,
            );
            builder = builder.add_interaction(
                "did:plc:base_liker_1",
                cand_uri.clone(),
                SignalType::Like,
                now_secs - 4800,
            );
            builder = builder.add_interaction(
                "did:plc:base_liker_2",
                cand_uri,
                SignalType::Like,
                now_secs - 4800,
            );
        }

        // New user: 2 likes, follows author A and co_interactor_1
        let new_user = "did:plc:new_user";
        let new_p1 = format!("at://{author_a}/app.bsky.feed.post/trending_1");
        let new_p2 = format!("at://{author_a}/app.bsky.feed.post/trending_2");
        builder = builder.add_interaction(new_user, new_p1, SignalType::Like, now_secs - 1000);
        builder = builder.add_interaction(new_user, new_p2, SignalType::Like, now_secs - 900);
        builder = builder.add_follow(new_user, author_a);
        builder = builder.add_follow(new_user, "did:plc:co_interactor_1");

        // Cold user
        builder = builder.add_user("did:plc:cold_user");

        builder.populate(&interner, &graph);
        (interner, graph)
    }
}

// ---------------------------------------------------------------------------
// 2. Reference Algorithmic Recommender Engine for E2E Tests
// ---------------------------------------------------------------------------

/// Re-export of crate production [`Recommender`] as [`TestRecommender`].
pub type TestRecommender = for_your_consideration::recommender::Recommender;

// ---------------------------------------------------------------------------
// 3. Mock Jetstream WebSocket Server
// ---------------------------------------------------------------------------

/// In-process WebSocket server for simulating Bluesky Jetstream events.
pub struct MockJetstreamServer {
    pub addr: SocketAddr,
    event_tx: mpsc::Sender<String>,
    shutdown_tx: tokio::sync::watch::Sender<bool>,
}

impl MockJetstreamServer {
    /// Spawns a mock Jetstream server on an ephemeral OS port.
    pub async fn start() -> Result<Self> {
        let listener = TcpListener::bind("127.0.0.1:0")
            .await
            .map_err(FeedError::Io)?;
        let addr = listener.local_addr().map_err(FeedError::Io)?;
        let (event_tx, event_rx) = mpsc::channel::<String>(1000);
        let (shutdown_tx, mut shutdown_rx) = tokio::sync::watch::channel(false);

        let shared_rx = Arc::new(Mutex::new(event_rx));

        tokio::spawn(async move {
            loop {
                tokio::select! {
                    _ = shutdown_rx.changed() => {
                        if *shutdown_rx.borrow() {
                            break;
                        }
                    }
                    accept_res = listener.accept() => {
                        if let Ok((stream, _)) = accept_res {
                            let rx = Arc::clone(&shared_rx);
                            let mut shut = shutdown_rx.clone();
                            tokio::spawn(async move {
                                if let Ok(mut ws) = tokio_tungstenite::accept_async(stream).await {
                                    loop {
                                        tokio::select! {
                                            _ = shut.changed() => {
                                                if *shut.borrow() {
                                                    let _ = ws.close(None).await;
                                                    break;
                                                }
                                            }
                                            msg = async {
                                                let mut guard = rx.lock().await;
                                                guard.recv().await
                                            } => {
                                                match msg {
                                                    Some(json) => {
                                                        if ws.send(Message::Text(json)).await.is_err() {
                                                            break;
                                                        }
                                                    }
                                                    None => break,
                                                }
                                            }
                                        }
                                    }
                                }
                            });
                        }
                    }
                }
            }
        });

        Ok(Self {
            addr,
            event_tx,
            shutdown_tx,
        })
    }

    /// Returns the WebSocket URL (e.g. `ws://127.0.0.1:12345`).
    #[must_use]
    pub fn ws_url(&self) -> String {
        format!("ws://{}", self.addr)
    }

    /// Broadcasts a raw JSON event string to connected clients.
    pub async fn send_event_json(&self, json: &str) {
        let _ = self.event_tx.send(json.to_string()).await;
    }

    /// Sends a structured Jetstream `app.bsky.feed.like` event.
    pub async fn send_like(&self, user_did: &str, post_uri: &str, time_us: u64) {
        let payload = serde_json::json!({
            "did": user_did,
            "time_us": time_us,
            "kind": "commit",
            "commit": {
                "collection": "app.bsky.feed.like",
                "rkey": "3k12345",
                "operation": "create",
                "record": {
                    "$type": "app.bsky.feed.like",
                    "subject": {
                        "uri": post_uri,
                        "cid": "bafyreih3..."
                    },
                    "createdAt": "2026-08-21T18:00:00Z"
                }
            }
        });
        self.send_event_json(&payload.to_string()).await;
    }

    /// Sends a structured Jetstream `app.bsky.feed.repost` event.
    pub async fn send_repost(&self, user_did: &str, post_uri: &str, time_us: u64) {
        let payload = serde_json::json!({
            "did": user_did,
            "time_us": time_us,
            "kind": "commit",
            "commit": {
                "collection": "app.bsky.feed.repost",
                "rkey": "3k67890",
                "operation": "create",
                "record": {
                    "$type": "app.bsky.feed.repost",
                    "subject": {
                        "uri": post_uri,
                        "cid": "bafyreih3..."
                    },
                    "createdAt": "2026-08-21T18:00:00Z"
                }
            }
        });
        self.send_event_json(&payload.to_string()).await;
    }

    /// Sends a structured Jetstream `app.bsky.feed.post` event.
    pub async fn send_post(
        &self,
        author_did: &str,
        _post_uri: &str,
        root_uri: Option<&str>,
        parent_uri: Option<&str>,
        time_us: u64,
    ) {
        let reply_json = match (root_uri, parent_uri) {
            (Some(r), Some(p)) => serde_json::json!({
                "root": { "uri": r, "cid": "bafyroot..." },
                "parent": { "uri": p, "cid": "bafyparent..." }
            }),
            _ => serde_json::Value::Null,
        };

        let mut record = serde_json::json!({
            "$type": "app.bsky.feed.post",
            "text": "Hello Bluesky!",
            "createdAt": "2026-08-21T18:00:00Z"
        });

        if !reply_json.is_null() {
            record["reply"] = reply_json;
        }

        let payload = serde_json::json!({
            "did": author_did,
            "time_us": time_us,
            "kind": "commit",
            "commit": {
                "collection": "app.bsky.feed.post",
                "rkey": "3kpost123",
                "operation": "create",
                "record": record
            }
        });
        self.send_event_json(&payload.to_string()).await;
    }

    /// Sends a structured Jetstream `app.bsky.graph.follow` event.
    pub async fn send_follow(&self, follower_did: &str, subject_did: &str, time_us: u64) {
        let payload = serde_json::json!({
            "did": follower_did,
            "time_us": time_us,
            "kind": "commit",
            "commit": {
                "collection": "app.bsky.graph.follow",
                "rkey": "3kfollow123",
                "operation": "create",
                "record": {
                    "$type": "app.bsky.graph.follow",
                    "subject": subject_did,
                    "createdAt": "2026-08-21T18:00:00Z"
                }
            }
        });
        self.send_event_json(&payload.to_string()).await;
    }

    /// Sends a structured Jetstream commit `delete` event.
    pub async fn send_delete(&self, did: &str, collection: &str, rkey: &str, time_us: u64) {
        let payload = serde_json::json!({
            "did": did,
            "time_us": time_us,
            "kind": "commit",
            "commit": {
                "collection": collection,
                "rkey": rkey,
                "operation": "delete"
            }
        });
        self.send_event_json(&payload.to_string()).await;
    }

    /// Shuts down the mock server.
    pub fn shutdown(&self) {
        let _ = self.shutdown_tx.send(true);
    }
}

// ---------------------------------------------------------------------------
// 4. Mock XRPC Router & Service Auth
// ---------------------------------------------------------------------------

/// App state for Axum XRPC tests.
#[derive(Clone)]
pub struct TestAppState {
    pub recommender: TestRecommender,
    pub service_did: CompactString,
    pub hostname: CompactString,
}

/// Query parameters for `app.bsky.feed.getFeedSkeleton`.
#[derive(Debug, Deserialize)]
pub struct FeedSkeletonQuery {
    pub feed: Option<String>,
    pub limit: Option<usize>,
    pub cursor: Option<String>,
    pub freshness: Option<String>,
    pub discovery: Option<String>,
    pub explain: Option<bool>,
    #[serde(alias = "engagement_floor", default)]
    pub min_likes: Option<String>,
}

/// Builds the test Axum router with all required XRPC and discovery endpoints.
pub fn create_test_xrpc_router(state: TestAppState) -> Router {
    Router::new()
        .route(
            "/xrpc/app.bsky.feed.getFeedSkeleton",
            get(handle_get_feed_skeleton),
        )
        .route("/.well-known/did.json", get(handle_get_did_doc))
        .route("/healthz", get(handle_get_healthz))
        .with_state(state)
}

async fn handle_get_feed_skeleton(
    State(state): State<TestAppState>,
    headers: HeaderMap,
    Query(query): Query<FeedSkeletonQuery>,
) -> impl IntoResponse {
    let _feed_uri = match query.feed {
        Some(f) if !f.is_empty() => f,
        _ => {
            return (
                StatusCode::BAD_REQUEST,
                Json(serde_json::json!({
                    "error": "InvalidRequest",
                    "message": "Missing required 'feed' parameter"
                })),
            )
                .into_response();
        }
    };

    // Extract viewer DID from Auth header
    let viewer_did = extract_viewer_did_from_headers(&headers);

    let mut dials = RecommendationDials::from_query(
        query.freshness.as_deref(),
        query.discovery.as_deref(),
        query.explain,
        query.limit,
        query.cursor,
    );
    if let Some(ref raw_min_likes) = query.min_likes {
        dials.min_likes = RecommendationDials::parse_engagement_floor(Some(raw_min_likes.as_str()));
    }

    let now_secs = chrono_like_now();

    match state
        .recommender
        .recommend(viewer_did.as_deref(), &dials, now_secs)
    {
        Ok(rec) => {
            let skeleton = FeedSkeletonResponse {
                feed: rec
                    .posts
                    .into_iter()
                    .map(|p| SkeletonFeedPost {
                        post: p.uri,
                        reason: None,
                        feed_context: p.explain,
                    })
                    .collect(),
                cursor: rec.cursor,
            };
            (StatusCode::OK, Json(skeleton)).into_response()
        }
        Err(err) => (
            StatusCode::INTERNAL_SERVER_ERROR,
            Json(serde_json::json!({
                "error": "InternalServerError",
                "message": err.to_string()
            })),
        )
            .into_response(),
    }
}

async fn handle_get_did_doc(State(state): State<TestAppState>) -> impl IntoResponse {
    let doc = serde_json::json!({
        "@context": ["https://www.w3.org/ns/did/v1"],
        "id": state.service_did.as_str(),
        "service": [{
            "id": "#bsky_fg",
            "type": "BskyFeedGenerator",
            "serviceEndpoint": format!("https://{}", state.hostname.as_str())
        }]
    });
    (StatusCode::OK, Json(doc))
}

async fn handle_get_healthz(State(state): State<TestAppState>) -> impl IntoResponse {
    let stats = state.recommender.graph.get_stats();
    let resp = serde_json::json!({
        "status": "ok",
        "nodes": stats.total_users + stats.total_posts,
        "edges": stats.total_interactions,
        "interned_strings": state.recommender.interner.len(),
    });
    (StatusCode::OK, Json(resp))
}

pub use for_your_consideration::auth::{extract_viewer_did, extract_viewer_did_from_headers};

/// Generates a valid or invalid mock ATProto service auth JWT token.
#[must_use]
pub fn generate_mock_jwt(issuer_did: &str, audience_did: &str, valid: bool) -> String {
    use base64::engine::general_purpose::URL_SAFE_NO_PAD;
    use base64::Engine;

    let header = serde_json::json!({
        "alg": "ES256K",
        "typ": "JWT"
    });
    let header_b64 = URL_SAFE_NO_PAD.encode(header.to_string());

    let payload = if valid {
        serde_json::json!({
            "iss": issuer_did,
            "aud": audience_did,
            "exp": chrono_like_now() + 3600,
            "jti": "mock_jti_12345"
        })
    } else {
        serde_json::json!({
            "iss": issuer_did,
            "aud": audience_did,
            "exp": chrono_like_now() - 3600, // Expired
        })
    };
    let payload_b64 = URL_SAFE_NO_PAD.encode(payload.to_string());

    let sig_b64 = URL_SAFE_NO_PAD.encode("mock_signature_bytes");
    format!("{header_b64}.{payload_b64}.{sig_b64}")
}

// ---------------------------------------------------------------------------
// 5. Utility & Assertion Helpers
// ---------------------------------------------------------------------------

/// Returns current timestamp in seconds.
#[must_use]
pub fn chrono_like_now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .unwrap_or_default()
        .as_secs()
}

/// Asserts that a string is a valid canonical AT-URI (`at://did:.../app.bsky.feed.post/...`).
pub fn assert_valid_at_uri(uri: &str) {
    assert!(
        uri.starts_with("at://did:"),
        "URI must start with at://did: but was {uri}"
    );
    assert!(
        uri.contains("/app.bsky.feed.post/"),
        "URI must contain collection path but was {uri}"
    );
}

/// Asserts that all post URIs in a feed are unique (no duplicates).
pub fn assert_feed_unique_posts(feed: &[SkeletonFeedPost]) {
    let mut seen = HashSet::new();
    for item in feed {
        assert!(
            seen.insert(item.post.clone()),
            "Duplicate post URI found in feed: {}",
            item.post
        );
    }
}

/// Asserts author diversity: no author appears more than `max_per_author` times.
pub fn assert_author_diversity(
    feed: &[SkeletonFeedPost],
    interner: &StringInterner,
    graph: &GraphStore,
    max_per_author: usize,
) {
    let mut counts = std::collections::HashMap::new();
    for item in feed {
        if let Some(pid) = interner.lookup_id(&item.post) {
            if let Some(meta) = graph.get_post_meta(pid) {
                let cnt = counts.entry(meta.author_id).or_insert(0);
                *cnt += 1;
                assert!(
                    *cnt <= max_per_author,
                    "Author ID {} exceeded diversity limit ({}) with count {}",
                    meta.author_id,
                    max_per_author,
                    *cnt
                );
            }
        }
    }
}
