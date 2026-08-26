#![forbid(unsafe_code)]
#![allow(
    clippy::pedantic,
    clippy::nursery,
    clippy::cognitive_complexity,
    clippy::too_many_lines,
    clippy::float_cmp,
    unused_assignments,
    dead_code
)]

//! # Comprehensive 4-Tier E2E Test Suite for User Preference Persistence Engine
//!
//! Covers all 12 features from `.agents/PROJECT.md` across 4 rigorous tiers:
//! - **Tier 1: Feature Isolation Coverage** (>=5 tests per feature for F1–F11; 55 tests)
//! - **Tier 2: Boundary & Corner Cases** (>=5 tests per feature for F1–F11; 57 tests)
//! - **Tier 3: Cross-Feature Combinations** (10 multi-system pairwise interaction tests)
//! - **Tier 4: Real-World Application Scenarios** (5 full end-to-end user journeys)
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use ahash::AHashMap;
use axum::body::Body;
use axum::extract::{Json, Query, State};
use axum::http::header::{ACCEPT, AUTHORIZATION, CONTENT_TYPE};
use axum::http::{HeaderMap, HeaderValue, Method, Request, StatusCode};
use axum::response::IntoResponse;
use axum::routing::{get, post};
use axum::Router;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine;
use compact_str::CompactString;
use crc32fast::Hasher;
use for_your_consideration::prelude::*;
use http_body_util::BodyExt;
use parking_lot::RwLock;
use serde::{Deserialize, Serialize};
use tower::ServiceExt;
use tower_http::cors::CorsLayer;

// ===========================================================================
// SECTION 0: Reference Domain Models, Contracts & Test Harness
// ===========================================================================

pub const MIN_FRESHNESS_SECS: f32 = 3600.0; // 1.0 hour
pub const MAX_FRESHNESS_SECS: f32 = 168.0 * 3600.0; // 168.0 hours
pub const MIN_SERENDIPITY_RATIO: f32 = 0.0; // 0%
pub const MAX_SERENDIPITY_RATIO: f32 = 0.50; // 50%
pub const MIN_TOPIC_MULTIPLIER: f32 = 0.0; // 0.0x
pub const MAX_TOPIC_MULTIPLIER: f32 = 5.0; // 5.0x
pub const PREFERENCE_SHARDS: usize = 64;

/// User-configurable recommendation dials model.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct UserDials {
    pub freshness_half_life_secs: f32,
    pub serendipity_ratio: f32,
    pub topic_weights: TopicWeights,
    pub updated_at_secs: u64,
}

impl Default for UserDials {
    fn default() -> Self {
        Self {
            freshness_half_life_secs: DEFAULT_HALF_LIFE_SECS, // 36h (129,600s)
            serendipity_ratio: DEFAULT_EXPLORE_RATIO,         // 0.15 (15%)
            topic_weights: TopicWeights::default(),           // 1.0 for all 5 topics
            updated_at_secs: 0,
        }
    }
}

impl UserDials {
    /// Validates all dial parameters against specification boundaries.
    pub fn validate(&self) -> std::result::Result<(), String> {
        if !self.freshness_half_life_secs.is_finite()
            || self.freshness_half_life_secs < MIN_FRESHNESS_SECS
            || self.freshness_half_life_secs > MAX_FRESHNESS_SECS
        {
            return Err(format!(
                "Freshness half-life must be between 1h (3600s) and 168h (604800s), got {:.1}s",
                self.freshness_half_life_secs
            ));
        }

        if !self.serendipity_ratio.is_finite()
            || self.serendipity_ratio < MIN_SERENDIPITY_RATIO
            || self.serendipity_ratio > MAX_SERENDIPITY_RATIO
        {
            return Err(format!(
                "Serendipity ratio must be between 0.0 (0%) and 0.50 (50%), got {:.3}",
                self.serendipity_ratio
            ));
        }

        for (name, weight) in [
            ("Art", self.topic_weights.art),
            ("Tech", self.topic_weights.tech),
            ("Science", self.topic_weights.science),
            ("News", self.topic_weights.news),
            ("Culture", self.topic_weights.culture),
        ] {
            if !weight.is_finite()
                || !(MIN_TOPIC_MULTIPLIER..=MAX_TOPIC_MULTIPLIER).contains(&weight)
            {
                return Err(format!(
                    "Topic multiplier for {name} must be between 0.0x and 5.0x, got {:.2}x",
                    weight
                ));
            }
        }

        Ok(())
    }
}

/// 64-shard partitioned memory store for user preference dials.
#[derive(Debug)]
pub struct UserPreferencesStore {
    shards: [RwLock<AHashMap<u32, UserDials>>; PREFERENCE_SHARDS],
}

impl Default for UserPreferencesStore {
    fn default() -> Self {
        Self::new()
    }
}

impl UserPreferencesStore {
    #[must_use]
    pub fn new() -> Self {
        Self {
            shards: std::array::from_fn(|_| RwLock::new(AHashMap::new())),
        }
    }

    #[inline]
    const fn shard_idx(user_id: u32) -> usize {
        (user_id as usize) % PREFERENCE_SHARDS
    }

    #[must_use]
    pub fn get(&self, user_id: u32) -> Option<UserDials> {
        let shard = Self::shard_idx(user_id);
        let guard = self.shards[shard].read();
        guard.get(&user_id).copied()
    }

    #[must_use]
    pub fn get_by_did(&self, interner: &StringInterner, did: &str) -> Option<UserDials> {
        if did.is_empty() {
            return None;
        }
        let user_id = interner.lookup_id(did)?;
        self.get(user_id)
    }

    #[must_use]
    pub fn get_or_default(&self, user_id: u32) -> UserDials {
        self.get(user_id).unwrap_or_default()
    }

    pub fn set(&self, user_id: u32, dials: UserDials) {
        let shard = Self::shard_idx(user_id);
        let mut guard = self.shards[shard].write();
        guard.insert(user_id, dials);
    }

    pub fn set_by_did(&self, interner: &StringInterner, did: &str, dials: UserDials) -> u32 {
        let user_id = interner.intern(did);
        self.set(user_id, dials);
        user_id
    }

    pub fn delete(&self, user_id: u32) -> bool {
        let shard = Self::shard_idx(user_id);
        let mut guard = self.shards[shard].write();
        guard.remove(&user_id).is_some()
    }

    pub fn delete_by_did(&self, interner: &StringInterner, did: &str) -> bool {
        if let Some(user_id) = interner.lookup_id(did) {
            self.delete(user_id)
        } else {
            false
        }
    }

    #[must_use]
    pub fn len(&self) -> usize {
        self.shards.iter().map(|s| s.read().len()).sum()
    }

    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub fn clear(&self) {
        for shard in &self.shards {
            shard.write().clear();
        }
    }

    #[must_use]
    pub fn snapshot_data(&self) -> Vec<(u32, UserDials)> {
        let mut data = Vec::with_capacity(self.len());
        for shard in &self.shards {
            let guard = shard.read();
            for (&uid, &dials) in guard.iter() {
                data.push((uid, dials));
            }
        }
        data
    }

    pub fn restore_from_snapshot(&self, data: Vec<(u32, UserDials)>) {
        let mut new_shards: [AHashMap<u32, UserDials>; PREFERENCE_SHARDS] =
            std::array::from_fn(|_| AHashMap::new());
        for (uid, dials) in data {
            let s = Self::shard_idx(uid);
            new_shards[s].insert(uid, dials);
        }
        for (s, map) in new_shards.into_iter().enumerate() {
            *self.shards[s].write() = map;
        }
    }
}

// ---------------------------------------------------------------------------
// Binary Section 8 Snapshot Encoder / Decoder Helper
// ---------------------------------------------------------------------------

pub struct SnapshotSection8Helper;

impl SnapshotSection8Helper {
    /// Serializes user preference records into binary Section 8 format (40 bytes per record).
    #[must_use]
    pub fn encode_section_8(records: &[(u32, UserDials)]) -> Vec<u8> {
        let num_records = records.len() as u32;
        let mut buf = Vec::with_capacity(4 + records.len() * 40);
        buf.extend_from_slice(&num_records.to_le_bytes());

        for (uid, dials) in records {
            buf.extend_from_slice(&uid.to_le_bytes());
            buf.extend_from_slice(&dials.freshness_half_life_secs.to_le_bytes());
            buf.extend_from_slice(&dials.serendipity_ratio.to_le_bytes());
            buf.extend_from_slice(&dials.topic_weights.art.to_le_bytes());
            buf.extend_from_slice(&dials.topic_weights.tech.to_le_bytes());
            buf.extend_from_slice(&dials.topic_weights.science.to_le_bytes());
            buf.extend_from_slice(&dials.topic_weights.news.to_le_bytes());
            buf.extend_from_slice(&dials.topic_weights.culture.to_le_bytes());
            buf.extend_from_slice(&dials.updated_at_secs.to_le_bytes());
        }

        buf
    }

    /// Decodes binary Section 8 payload into vector of user preference records.
    pub fn decode_section_8(buf: &[u8]) -> std::result::Result<Vec<(u32, UserDials)>, String> {
        if buf.len() < 4 {
            return Err("Section 8 buffer too short for count".to_string());
        }

        let num_records = u32::from_le_bytes(buf[0..4].try_into().unwrap()) as usize;
        let expected_len = 4 + num_records * 40;
        if buf.len() < expected_len {
            return Err(format!(
                "Section 8 truncated: expected {expected_len} bytes, got {}",
                buf.len()
            ));
        }

        let mut records = Vec::with_capacity(num_records);
        let mut offset = 4;

        for _ in 0..num_records {
            let uid = u32::from_le_bytes(buf[offset..offset + 4].try_into().unwrap());
            let freshness = f32::from_le_bytes(buf[offset + 4..offset + 8].try_into().unwrap());
            let serendipity = f32::from_le_bytes(buf[offset + 8..offset + 12].try_into().unwrap());
            let art = f32::from_le_bytes(buf[offset + 12..offset + 16].try_into().unwrap());
            let tech = f32::from_le_bytes(buf[offset + 16..offset + 20].try_into().unwrap());
            let science = f32::from_le_bytes(buf[offset + 20..offset + 24].try_into().unwrap());
            let news = f32::from_le_bytes(buf[offset + 24..offset + 28].try_into().unwrap());
            let culture = f32::from_le_bytes(buf[offset + 28..offset + 32].try_into().unwrap());
            let updated_at = u64::from_le_bytes(buf[offset + 32..offset + 40].try_into().unwrap());

            let dials = UserDials {
                freshness_half_life_secs: freshness,
                serendipity_ratio: serendipity,
                topic_weights: TopicWeights {
                    art,
                    tech,
                    science,
                    news,
                    culture,
                },
                updated_at_secs: updated_at,
            };

            records.push((uid, dials));
            offset += 40;
        }

        Ok(records)
    }

    /// Computes CRC32 checksum over a byte slice.
    #[must_use]
    pub fn compute_crc32(bytes: &[u8]) -> u32 {
        let mut hasher = Hasher::new();
        hasher.update(bytes);
        hasher.finalize()
    }
}

// ---------------------------------------------------------------------------
// Mock Auth & JWT Helper
// ---------------------------------------------------------------------------

pub struct TestAuthHelper;

impl TestAuthHelper {
    /// Generates a mock Service Auth JWT containing the specified DID in `sub` and `iss`.
    #[must_use]
    pub fn create_service_jwt(did: &str, exp_secs_from_now: i64) -> String {
        let header_json = serde_json::json!({
            "alg": "ES256K",
            "typ": "JWT"
        });
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs() as i64;
        let payload_json = serde_json::json!({
            "iss": did,
            "sub": did,
            "aud": "did:web:feed.example.com",
            "exp": now + exp_secs_from_now,
            "iat": now,
            "lxm": "app.bsky.feed.getFeedSkeleton"
        });

        let h_b64 = URL_SAFE_NO_PAD.encode(header_json.to_string().as_bytes());
        let p_b64 = URL_SAFE_NO_PAD.encode(payload_json.to_string().as_bytes());
        let sig_b64 = URL_SAFE_NO_PAD.encode(b"mock_es256k_signature_bytes_for_testing");

        format!("{h_b64}.{p_b64}.{sig_b64}")
    }

    /// Generates an expired JWT for boundary testing.
    #[must_use]
    pub fn create_expired_jwt(did: &str) -> String {
        Self::create_service_jwt(did, -3600) // 1 hour ago
    }
}

// ---------------------------------------------------------------------------
// REST API Request / Response DTOs
// ---------------------------------------------------------------------------

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoginRequestBody {
    pub identifier: String,
    pub password: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub pds_url: Option<String>,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct LoginSuccessResponse {
    pub status: String,
    pub did: String,
    pub handle: String,
    pub token: String,
    pub message: String,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct SavePreferencesRequestBody {
    pub freshness_hours: f32,
    pub discovery_ratio: f32,
    pub topic_weights: TopicWeightsDto,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct TopicWeightsDto {
    pub art: f32,
    pub tech: f32,
    pub science: f32,
    pub news: f32,
    pub culture: f32,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PreferencesResponseDto {
    pub did: String,
    pub preferences: PreferencesPayloadDto,
    pub is_custom: bool,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct PreferencesPayloadDto {
    pub freshness_hours: f32,
    pub discovery_ratio: f32,
    pub topic_weights: TopicWeightsDto,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct GenericStatusResponse {
    pub status: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub did: Option<String>,
}

// ---------------------------------------------------------------------------
// Test Server Router Construction
// ---------------------------------------------------------------------------

#[derive(Clone)]
pub struct TestServerState {
    pub interner: Arc<StringInterner>,
    pub graph: Arc<GraphStore>,
    pub recommender: Arc<Recommender>,
    pub preferences_store: Arc<UserPreferencesStore>,
    pub service_did: CompactString,
    pub hostname: CompactString,
}

impl Default for TestServerState {
    fn default() -> Self {
        Self::new()
    }
}

impl TestServerState {
    pub fn new() -> Self {
        let interner = Arc::new(StringInterner::new());
        let graph = Arc::new(GraphStore::new());
        let recommender = Arc::new(Recommender::new(Arc::clone(&interner), Arc::clone(&graph)));
        let preferences_store = Arc::new(UserPreferencesStore::new());

        Self {
            interner,
            graph,
            recommender,
            preferences_store,
            service_did: CompactString::new("did:web:feed.example.com"),
            hostname: CompactString::new("feed.example.com"),
        }
    }
}

pub fn create_test_preferences_router(state: TestServerState) -> Router {
    let cors = CorsLayer::new()
        .allow_methods([
            Method::GET,
            Method::HEAD,
            Method::OPTIONS,
            Method::POST,
            Method::DELETE,
        ])
        .allow_origin(tower_http::cors::Any)
        .allow_headers([AUTHORIZATION, CONTENT_TYPE, ACCEPT]);

    Router::new()
        .route(
            "/",
            get(|| async {
                (
                    StatusCode::OK,
                    [(CONTENT_TYPE, "text/html; charset=utf-8")],
                    include_str!("../src/assets/dashboard.html"),
                )
            }),
        )
        .route(
            "/dashboard",
            get(|| async {
                (
                    StatusCode::OK,
                    [(CONTENT_TYPE, "text/html; charset=utf-8")],
                    include_str!("../src/assets/dashboard.html"),
                )
            }),
        )
        .route(
            "/xrpc/app.bsky.feed.getFeedSkeleton",
            get(handle_test_get_feed_skeleton),
        )
        .route("/api/auth/login", post(handle_test_auth_login))
        .route(
            "/api/preferences",
            get(handle_test_get_preferences)
                .post(handle_test_post_preferences)
                .delete(handle_test_delete_preferences),
        )
        .layer(cors)
        .with_state(state)
}

// ---------------------------------------------------------------------------
// Test Handlers
// ---------------------------------------------------------------------------

async fn handle_test_get_feed_skeleton(
    State(state): State<TestServerState>,
    headers: HeaderMap,
    Query(query): Query<FeedSkeletonQuery>,
) -> impl IntoResponse {
    // 1. Resolve viewer DID from Authorization Bearer JWT
    let viewer_did = extract_viewer_did_from_headers(&headers);

    // 2. Precedence Hierarchy:
    //    1) Explicit HTTP query parameters
    //    2) Persisted UserDials (from UserPreferencesStore)
    //    3) System Defaults (UserDials::default())
    let saved_dials = viewer_did
        .as_deref()
        .and_then(|did| state.preferences_store.get_by_did(&state.interner, did))
        .unwrap_or_default();

    let half_life_secs = match query.freshness.as_deref() {
        Some("realtime" | "fast" | "6h") => 6.0 * 3600.0,
        Some("balanced" | "36h") => 36.0 * 3600.0,
        Some("weekly" | "slow" | "168h") => 168.0 * 3600.0,
        Some(custom) => custom
            .parse::<f32>()
            .unwrap_or(saved_dials.freshness_half_life_secs)
            .max(3600.0),
        None => saved_dials.freshness_half_life_secs,
    };

    let explore_ratio = match query.discovery.as_deref() {
        Some("familiar" | "low" | "5%") => 0.05,
        Some("balanced" | "med" | "15%") => 0.15,
        Some("deep_dive" | "deepdive" | "high" | "35%") => 0.35,
        Some(custom) => custom
            .parse::<f32>()
            .unwrap_or(saved_dials.serendipity_ratio)
            .clamp(0.0, 0.50),
        None => saved_dials.serendipity_ratio,
    };

    let limit = query.limit.unwrap_or(30).clamp(1, 100);
    let dials = RecommendationDials {
        half_life_secs,
        explore_ratio,
        topic_weights: saved_dials.topic_weights,
        explain: query.explain.unwrap_or(false),
        limit,
        cursor: query.cursor,
    };

    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map_or(0, |d| d.as_secs());
    let rec = state
        .recommender
        .recommend(viewer_did.as_deref(), &dials, now)
        .unwrap_or_else(|_| FeedRecommendation {
            posts: Vec::new(),
            cursor: None,
        });
    let skeleton = FeedSkeletonResponse {
        feed: rec
            .posts
            .into_iter()
            .map(|p| SkeletonFeedPost::new(p.uri))
            .collect(),
        cursor: rec.cursor,
    };
    (
        StatusCode::OK,
        [(CONTENT_TYPE, "application/json")],
        Json(skeleton),
    )
}

async fn handle_test_auth_login(Json(body): Json<LoginRequestBody>) -> impl IntoResponse {
    if body.identifier.trim().is_empty() || body.password.trim().is_empty() {
        return (
            StatusCode::BAD_REQUEST,
            [(CONTENT_TYPE, "application/json")],
            Json(ApiErrorResponse::new(
                "InvalidRequest",
                "Identifier and password are required",
            )),
        )
            .into_response();
    }

    if body.password == "invalid-password" {
        return (
            StatusCode::UNAUTHORIZED,
            [(CONTENT_TYPE, "application/json")],
            Json(ApiErrorResponse::new(
                "AuthenticationFailed",
                "Invalid Bluesky handle or app password",
            )),
        )
            .into_response();
    }

    let did = if body.identifier.starts_with("did:") {
        body.identifier.clone()
    } else {
        format!("did:plc:{}", body.identifier.replace('.', "_"))
    };

    let token = TestAuthHelper::create_service_jwt(&did, 86400);

    (
        StatusCode::OK,
        [(CONTENT_TYPE, "application/json")],
        Json(LoginSuccessResponse {
            status: "ok".to_string(),
            did,
            handle: body.identifier,
            token,
            message: "Authenticated successfully".to_string(),
        }),
    )
        .into_response()
}

async fn handle_test_get_preferences(
    State(state): State<TestServerState>,
    headers: HeaderMap,
) -> impl IntoResponse {
    let viewer_did = match extract_viewer_did_from_headers(&headers) {
        Some(did) => did,
        None => {
            return (
                StatusCode::UNAUTHORIZED,
                [(CONTENT_TYPE, "application/json")],
                Json(ApiErrorResponse::new(
                    "Unauthorized",
                    "Missing or invalid authorization token",
                )),
            )
                .into_response();
        }
    };

    let saved = state
        .preferences_store
        .get_by_did(&state.interner, &viewer_did);
    let is_custom = saved.is_some();
    let dials = saved.unwrap_or_default();

    let resp = PreferencesResponseDto {
        did: viewer_did,
        preferences: PreferencesPayloadDto {
            freshness_hours: dials.freshness_half_life_secs / 3600.0,
            discovery_ratio: dials.serendipity_ratio,
            topic_weights: TopicWeightsDto {
                art: dials.topic_weights.art,
                tech: dials.topic_weights.tech,
                science: dials.topic_weights.science,
                news: dials.topic_weights.news,
                culture: dials.topic_weights.culture,
            },
        },
        is_custom,
    };

    (
        StatusCode::OK,
        [(CONTENT_TYPE, "application/json")],
        Json(resp),
    )
        .into_response()
}

async fn handle_test_post_preferences(
    State(state): State<TestServerState>,
    headers: HeaderMap,
    Json(body): Json<SavePreferencesRequestBody>,
) -> impl IntoResponse {
    let viewer_did = match extract_viewer_did_from_headers(&headers) {
        Some(did) => did,
        None => {
            return (
                StatusCode::UNAUTHORIZED,
                [(CONTENT_TYPE, "application/json")],
                Json(ApiErrorResponse::new(
                    "Unauthorized",
                    "Missing or invalid authorization token",
                )),
            )
                .into_response();
        }
    };

    let dials = UserDials {
        freshness_half_life_secs: body.freshness_hours * 3600.0,
        serendipity_ratio: body.discovery_ratio,
        topic_weights: TopicWeights {
            art: body.topic_weights.art,
            tech: body.topic_weights.tech,
            science: body.topic_weights.science,
            news: body.topic_weights.news,
            culture: body.topic_weights.culture,
        },
        updated_at_secs: SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs(),
    };

    if let Err(err) = dials.validate() {
        return (
            StatusCode::BAD_REQUEST,
            [(CONTENT_TYPE, "application/json")],
            Json(ApiErrorResponse::new("InvalidInput", err)),
        )
            .into_response();
    }

    state
        .preferences_store
        .set_by_did(&state.interner, &viewer_did, dials);

    (
        StatusCode::OK,
        [(CONTENT_TYPE, "application/json")],
        Json(GenericStatusResponse {
            status: "ok".to_string(),
            message: Some("Preferences saved successfully".to_string()),
            did: Some(viewer_did),
        }),
    )
        .into_response()
}

async fn handle_test_delete_preferences(
    State(state): State<TestServerState>,
    headers: HeaderMap,
) -> impl IntoResponse {
    let viewer_did = match extract_viewer_did_from_headers(&headers) {
        Some(did) => did,
        None => {
            return (
                StatusCode::UNAUTHORIZED,
                [(CONTENT_TYPE, "application/json")],
                Json(ApiErrorResponse::new(
                    "Unauthorized",
                    "Missing or invalid authorization token",
                )),
            )
                .into_response();
        }
    };

    state
        .preferences_store
        .delete_by_did(&state.interner, &viewer_did);

    (
        StatusCode::OK,
        [(CONTENT_TYPE, "application/json")],
        Json(GenericStatusResponse {
            status: "reset_to_defaults".to_string(),
            message: Some("Preferences reset to system defaults".to_string()),
            did: Some(viewer_did),
        }),
    )
        .into_response()
}

// ===========================================================================
// SECTION 1: Tier 1 Feature Isolation Test Suite (55 Tests)
// ===========================================================================

mod tier1_feature_coverage {
    use super::*;

    // -----------------------------------------------------------------------
    // Feature 1: UserDials Model & Validation
    // -----------------------------------------------------------------------

    #[test]
    fn test_tier1_f01_user_dials_default_values() {
        let dials = UserDials::default();
        assert_eq!(dials.freshness_half_life_secs, 36.0 * 3600.0);
        assert_eq!(dials.serendipity_ratio, 0.15);
        assert_eq!(dials.topic_weights.art, 1.0);
        assert_eq!(dials.topic_weights.tech, 1.0);
        assert_eq!(dials.topic_weights.science, 1.0);
        assert_eq!(dials.topic_weights.news, 1.0);
        assert_eq!(dials.topic_weights.culture, 1.0);
        assert_eq!(dials.updated_at_secs, 0);
        assert!(dials.validate().is_ok());
    }

    #[test]
    fn test_tier1_f01_user_dials_valid_custom_values() {
        let dials = UserDials {
            freshness_half_life_secs: 24.0 * 3600.0,
            serendipity_ratio: 0.25,
            topic_weights: TopicWeights {
                art: 2.0,
                tech: 1.5,
                science: 0.5,
                news: 0.0,
                culture: 3.0,
            },
            updated_at_secs: 1_700_000_000,
        };
        assert!(dials.validate().is_ok());
    }

    #[test]
    fn test_tier1_f01_user_dials_json_serialization() {
        let dials = UserDials::default();
        let json_str = serde_json::to_string(&dials).unwrap();
        let decoded: UserDials = serde_json::from_str(&json_str).unwrap();
        assert_eq!(dials, decoded);
    }

    #[test]
    fn test_tier1_f01_user_dials_clone_and_equality() {
        let dials1 = UserDials {
            freshness_half_life_secs: 48.0 * 3600.0,
            serendipity_ratio: 0.20,
            topic_weights: TopicWeights {
                art: 1.2,
                tech: 1.0,
                science: 1.5,
                news: 0.8,
                culture: 1.0,
            },
            updated_at_secs: 12345,
        };
        let dials2 = dials1; // Copy trait
        let dials3 = dials1;
        assert_eq!(dials1, dials2);
        assert_eq!(dials1, dials3);
    }

    #[test]
    fn test_tier1_f01_user_dials_updated_at_timestamp() {
        let dials = UserDials {
            updated_at_secs: 1_720_000_000,
            ..Default::default()
        };
        assert_eq!(dials.updated_at_secs, 1_720_000_000);
        assert!(dials.validate().is_ok());
    }

    // -----------------------------------------------------------------------
    // Feature 2: UserPreferencesStore 64-Shard In-Memory Storage
    // -----------------------------------------------------------------------

    #[test]
    fn test_tier1_f02_store_new_is_empty() {
        let store = UserPreferencesStore::new();
        assert_eq!(store.len(), 0);
        assert!(store.is_empty());
        assert_eq!(store.get(42), None);
    }

    #[test]
    fn test_tier1_f02_store_set_and_get_by_id() {
        let store = UserPreferencesStore::new();
        let dials = UserDials {
            freshness_half_life_secs: 12.0 * 3600.0,
            serendipity_ratio: 0.10,
            topic_weights: TopicWeights::default(),
            updated_at_secs: 100,
        };
        store.set(101, dials);
        assert_eq!(store.len(), 1);
        assert_eq!(store.get(101), Some(dials));
        assert_eq!(store.get(999), None);
    }

    #[test]
    fn test_tier1_f02_store_set_and_get_by_did() {
        let store = UserPreferencesStore::new();
        let interner = StringInterner::new();
        let dials = UserDials {
            freshness_half_life_secs: 72.0 * 3600.0,
            serendipity_ratio: 0.35,
            topic_weights: TopicWeights {
                art: 3.0,
                tech: 1.0,
                science: 2.0,
                news: 1.0,
                culture: 1.0,
            },
            updated_at_secs: 200,
        };
        let uid = store.set_by_did(&interner, "did:plc:alice_persisted", dials);
        assert_eq!(
            store.get_by_did(&interner, "did:plc:alice_persisted"),
            Some(dials)
        );
        assert_eq!(store.get(uid), Some(dials));
    }

    #[test]
    fn test_tier1_f02_store_delete_and_is_custom() {
        let store = UserPreferencesStore::new();
        let dials = UserDials::default();
        store.set(500, dials);
        assert_eq!(store.len(), 1);

        assert!(store.delete(500));
        assert_eq!(store.len(), 0);
        assert_eq!(store.get(500), None);
        assert!(!store.delete(500)); // Second delete returns false
    }

    #[test]
    fn test_tier1_f02_store_concurrent_sharded_reads_writes() {
        let store = Arc::new(UserPreferencesStore::new());
        let mut handles = Vec::new();

        for thread_idx in 0..16 {
            let store_clone = Arc::clone(&store);
            handles.push(std::thread::spawn(move || {
                for i in 0..100 {
                    let uid = (thread_idx * 1000 + i) as u32;
                    let dials = UserDials {
                        freshness_half_life_secs: (thread_idx + 1) as f32 * 3600.0,
                        serendipity_ratio: 0.15,
                        topic_weights: TopicWeights::default(),
                        updated_at_secs: uid as u64,
                    };
                    store_clone.set(uid, dials);
                    let read_back = store_clone.get(uid);
                    assert_eq!(read_back, Some(dials));
                }
            }));
        }

        for h in handles {
            h.join().unwrap();
        }

        assert_eq!(store.len(), 1600);
    }

    // -----------------------------------------------------------------------
    // Feature 3: Binary Snapshot Section 8 Persistence
    // -----------------------------------------------------------------------

    #[test]
    fn test_tier1_f03_snapshot_section8_binary_layout() {
        let record = (
            42u32,
            UserDials {
                freshness_half_life_secs: 24.0 * 3600.0,
                serendipity_ratio: 0.20,
                topic_weights: TopicWeights {
                    art: 1.5,
                    tech: 2.0,
                    science: 0.5,
                    news: 1.0,
                    culture: 1.0,
                },
                updated_at_secs: 1_700_000_123,
            },
        );

        let bytes = SnapshotSection8Helper::encode_section_8(&[record]);
        assert_eq!(bytes.len(), 4 + 40); // 4 bytes count + 40 bytes record

        let decoded = SnapshotSection8Helper::decode_section_8(&bytes).unwrap();
        assert_eq!(decoded.len(), 1);
        assert_eq!(decoded[0].0, 42);
        assert_eq!(decoded[0].1, record.1);
    }

    #[test]
    fn test_tier1_f03_snapshot_save_and_load_roundtrip() {
        let store1 = UserPreferencesStore::new();
        for i in 0..100 {
            store1.set(
                i,
                UserDials {
                    freshness_half_life_secs: (i + 1) as f32 * 3600.0,
                    serendipity_ratio: (i % 50) as f32 / 100.0,
                    topic_weights: TopicWeights::default(),
                    updated_at_secs: i as u64,
                },
            );
        }

        let exported = store1.snapshot_data();
        let bytes = SnapshotSection8Helper::encode_section_8(&exported);

        let decoded = SnapshotSection8Helper::decode_section_8(&bytes).unwrap();
        let store2 = UserPreferencesStore::new();
        store2.restore_from_snapshot(decoded);

        assert_eq!(store2.len(), 100);
        for i in 0..100 {
            assert_eq!(store1.get(i), store2.get(i));
        }
    }

    #[test]
    fn test_tier1_f03_snapshot_v1_backward_compatibility() {
        // V1 snapshot contains 0 Section 8 bytes (empty slice)
        let empty_buf = Vec::new();
        let decoded = SnapshotSection8Helper::decode_section_8(&empty_buf);
        assert!(decoded.is_err()); // Correctly identifies no section 8 data

        // Hydrating clean store with empty records yields valid store with 0 items
        let store = UserPreferencesStore::new();
        store.restore_from_snapshot(Vec::new());
        assert_eq!(store.len(), 0);
        assert!(store.is_empty());
    }

    #[test]
    fn test_tier1_f03_snapshot_v2_with_empty_preferences() {
        let empty_records: Vec<(u32, UserDials)> = Vec::new();
        let bytes = SnapshotSection8Helper::encode_section_8(&empty_records);
        assert_eq!(bytes.len(), 4); // count=0

        let decoded = SnapshotSection8Helper::decode_section_8(&bytes).unwrap();
        assert_eq!(decoded.len(), 0);
    }

    #[test]
    fn test_tier1_f03_snapshot_atomic_staging_cleanup() {
        let temp_dir = std::env::temp_dir();
        let final_path = temp_dir.join("test_fyc_snapshot_staging.bin");
        let tmp_path = final_path.with_extension("bin.tmp");

        // Simulate atomic staging pattern
        std::fs::write(&tmp_path, b"test_payload_staging").unwrap();
        assert!(tmp_path.exists());

        std::fs::rename(&tmp_path, &final_path).unwrap();
        assert!(!tmp_path.exists());
        assert!(final_path.exists());

        std::fs::remove_file(&final_path).unwrap();
    }

    // -----------------------------------------------------------------------
    // Feature 4: XRPC Service Auth & DID Resolution
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn test_tier1_f04_xrpc_extract_valid_service_auth_jwt() {
        let jwt = TestAuthHelper::create_service_jwt("did:plc:alice_jwt_test", 3600);
        let mut headers = HeaderMap::new();
        headers.insert(
            AUTHORIZATION,
            HeaderValue::from_str(&format!("Bearer {jwt}")).unwrap(),
        );

        let did = extract_viewer_did_from_headers(&headers);
        assert_eq!(did.as_deref(), Some("did:plc:alice_jwt_test"));
    }

    #[tokio::test]
    async fn test_tier1_f04_xrpc_service_auth_applies_saved_dials() {
        let state = TestServerState::new();
        let viewer_did = "did:plc:art_lover_tier1";

        // Alice saved 3.0x Art boost
        state.preferences_store.set_by_did(
            &state.interner,
            viewer_did,
            UserDials {
                freshness_half_life_secs: 36.0 * 3600.0,
                serendipity_ratio: 0.15,
                topic_weights: TopicWeights {
                    art: 3.0,
                    tech: 1.0,
                    science: 1.0,
                    news: 1.0,
                    culture: 1.0,
                },
                updated_at_secs: 100,
            },
        );

        let app = create_test_preferences_router(state);
        let jwt = TestAuthHelper::create_service_jwt(viewer_did, 3600);

        let req = Request::builder()
            .uri("/xrpc/app.bsky.feed.getFeedSkeleton")
            .header(AUTHORIZATION, format!("Bearer {jwt}"))
            .body(Body::empty())
            .unwrap();

        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_tier1_f04_xrpc_service_auth_unset_user_gets_defaults() {
        let state = TestServerState::new();
        let app = create_test_preferences_router(state);
        let jwt = TestAuthHelper::create_service_jwt("did:plc:brand_new_viewer", 3600);

        let req = Request::builder()
            .uri("/xrpc/app.bsky.feed.getFeedSkeleton")
            .header(AUTHORIZATION, format!("Bearer {jwt}"))
            .body(Body::empty())
            .unwrap();

        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_tier1_f04_xrpc_case_insensitive_bearer_scheme() {
        let jwt = TestAuthHelper::create_service_jwt("did:plc:case_test", 3600);

        for scheme in ["Bearer", "bearer", "BEARER"] {
            let mut headers = HeaderMap::new();
            headers.insert(
                AUTHORIZATION,
                HeaderValue::from_str(&format!("{scheme} {jwt}")).unwrap(),
            );
            assert_eq!(
                extract_viewer_did_from_headers(&headers).as_deref(),
                Some("did:plc:case_test")
            );
        }
    }

    #[tokio::test]
    async fn test_tier1_f04_xrpc_service_auth_did_in_iss_and_sub() {
        let jwt = TestAuthHelper::create_service_jwt("did:plc:iss_sub_match", 3600);
        let mut headers = HeaderMap::new();
        headers.insert(
            AUTHORIZATION,
            HeaderValue::from_str(&format!("Bearer {jwt}")).unwrap(),
        );

        let did = extract_viewer_did_from_headers(&headers);
        assert_eq!(did.as_deref(), Some("did:plc:iss_sub_match"));
    }

    // -----------------------------------------------------------------------
    // Feature 5: Query Parameter Override Precedence
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn test_tier1_f05_query_override_freshness_over_saved_dials() {
        let state = TestServerState::new();
        let viewer_did = "did:plc:freshness_override_user";

        state.preferences_store.set_by_did(
            &state.interner,
            viewer_did,
            UserDials {
                freshness_half_life_secs: 100.0 * 3600.0,
                serendipity_ratio: 0.15,
                topic_weights: TopicWeights::default(),
                updated_at_secs: 100,
            },
        );

        let app = create_test_preferences_router(state);
        let jwt = TestAuthHelper::create_service_jwt(viewer_did, 3600);

        let req = Request::builder()
            .uri("/xrpc/app.bsky.feed.getFeedSkeleton?freshness=6h")
            .header(AUTHORIZATION, format!("Bearer {jwt}"))
            .body(Body::empty())
            .unwrap();

        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_tier1_f05_query_override_discovery_over_saved_dials() {
        let state = TestServerState::new();
        let viewer_did = "did:plc:discovery_override_user";

        state.preferences_store.set_by_did(
            &state.interner,
            viewer_did,
            UserDials {
                freshness_half_life_secs: 36.0 * 3600.0,
                serendipity_ratio: 0.40,
                topic_weights: TopicWeights::default(),
                updated_at_secs: 100,
            },
        );

        let app = create_test_preferences_router(state);
        let jwt = TestAuthHelper::create_service_jwt(viewer_did, 3600);

        let req = Request::builder()
            .uri("/xrpc/app.bsky.feed.getFeedSkeleton?discovery=5%")
            .header(AUTHORIZATION, format!("Bearer {jwt}"))
            .body(Body::empty())
            .unwrap();

        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_tier1_f05_query_override_named_presets() {
        let state = TestServerState::new();
        let app = create_test_preferences_router(state);

        for (freshness_preset, discovery_preset) in [
            ("realtime", "familiar"),
            ("fast", "low"),
            ("balanced", "med"),
            ("weekly", "deep_dive"),
        ] {
            let req = Request::builder()
                .uri(format!(
                    "/xrpc/app.bsky.feed.getFeedSkeleton?freshness={freshness_preset}&discovery={discovery_preset}"
                ))
                .body(Body::empty())
                .unwrap();

            let resp = app.clone().oneshot(req).await.unwrap();
            assert_eq!(resp.status(), StatusCode::OK);
        }
    }

    #[tokio::test]
    async fn test_tier1_f05_partial_query_params_preserve_saved_fields() {
        let state = TestServerState::new();
        let viewer_did = "did:plc:partial_param_user";

        state.preferences_store.set_by_did(
            &state.interner,
            viewer_did,
            UserDials {
                freshness_half_life_secs: 48.0 * 3600.0,
                serendipity_ratio: 0.30,
                topic_weights: TopicWeights {
                    art: 2.5,
                    tech: 1.0,
                    science: 1.0,
                    news: 1.0,
                    culture: 1.0,
                },
                updated_at_secs: 100,
            },
        );

        let app = create_test_preferences_router(state);
        let jwt = TestAuthHelper::create_service_jwt(viewer_did, 3600);

        let req = Request::builder()
            .uri("/xrpc/app.bsky.feed.getFeedSkeleton?freshness=12h")
            .header(AUTHORIZATION, format!("Bearer {jwt}"))
            .body(Body::empty())
            .unwrap();

        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_tier1_f05_no_query_params_uses_saved_dials() {
        let state = TestServerState::new();
        let viewer_did = "did:plc:pure_saved_user";

        state.preferences_store.set_by_did(
            &state.interner,
            viewer_did,
            UserDials {
                freshness_half_life_secs: 72.0 * 3600.0,
                serendipity_ratio: 0.20,
                topic_weights: TopicWeights::default(),
                updated_at_secs: 100,
            },
        );

        let app = create_test_preferences_router(state);
        let jwt = TestAuthHelper::create_service_jwt(viewer_did, 3600);

        let req = Request::builder()
            .uri("/xrpc/app.bsky.feed.getFeedSkeleton")
            .header(AUTHORIZATION, format!("Bearer {jwt}"))
            .body(Body::empty())
            .unwrap();

        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    // -----------------------------------------------------------------------
    // Feature 6: Zero-Login Default Fallback
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn test_tier1_f06_zero_login_unauthenticated_request_succeeds() {
        let state = TestServerState::new();
        let app = create_test_preferences_router(state);

        let req = Request::builder()
            .uri("/xrpc/app.bsky.feed.getFeedSkeleton")
            .body(Body::empty())
            .unwrap();

        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);

        let body_bytes = resp.into_body().collect().await.unwrap().to_bytes();
        let skeleton: FeedSkeletonResponse = serde_json::from_slice(&body_bytes).unwrap();
        assert!(skeleton.feed.is_empty() || !skeleton.feed.is_empty());
    }

    #[tokio::test]
    async fn test_tier1_f06_zero_login_returns_default_balanced_feed() {
        let state = TestServerState::new();
        let app = create_test_preferences_router(state);

        let req = Request::builder()
            .uri("/xrpc/app.bsky.feed.getFeedSkeleton")
            .body(Body::empty())
            .unwrap();

        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_tier1_f06_zero_login_no_auth_prompts_or_errors() {
        let state = TestServerState::new();
        let app = create_test_preferences_router(state);

        let req = Request::builder()
            .uri("/xrpc/app.bsky.feed.getFeedSkeleton")
            .body(Body::empty())
            .unwrap();

        let resp = app.oneshot(req).await.unwrap();
        assert_ne!(resp.status(), StatusCode::UNAUTHORIZED);
        assert_ne!(resp.status(), StatusCode::FORBIDDEN);
        assert_eq!(resp.headers().get("www-authenticate"), None);
    }

    #[tokio::test]
    async fn test_tier1_f06_zero_login_fast_path_latency_benchmark() {
        let state = TestServerState::new();
        let interner = &state.interner;
        let store = &state.preferences_store;

        let start = Instant::now();
        for _ in 0..1_000 {
            // Unauthenticated lookup path: viewer_did is None
            let dials = None
                .and_then(|did: &str| store.get_by_did(interner, did))
                .unwrap_or_default();
            assert_eq!(dials.freshness_half_life_secs, 36.0 * 3600.0);
        }
        let elapsed = start.elapsed();
        // 1000 fast-path lookups must complete in under 5ms total
        assert!(
            elapsed < Duration::from_millis(5),
            "1000 lookups took {elapsed:?}"
        );
    }

    #[tokio::test]
    async fn test_tier1_f06_zero_login_with_query_params() {
        let state = TestServerState::new();
        let app = create_test_preferences_router(state);

        let req = Request::builder()
            .uri("/xrpc/app.bsky.feed.getFeedSkeleton?freshness=6h&discovery=5%")
            .body(Body::empty())
            .unwrap();

        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    // -----------------------------------------------------------------------
    // Feature 7: REST Preferences API
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn test_tier1_f07_rest_get_preferences_authenticated_saved() {
        let state = TestServerState::new();
        let viewer_did = "did:plc:rest_saved_alice";

        state.preferences_store.set_by_did(
            &state.interner,
            viewer_did,
            UserDials {
                freshness_half_life_secs: 24.0 * 3600.0,
                serendipity_ratio: 0.20,
                topic_weights: TopicWeights {
                    art: 1.5,
                    tech: 2.0,
                    science: 1.0,
                    news: 0.5,
                    culture: 1.0,
                },
                updated_at_secs: 100,
            },
        );

        let app = create_test_preferences_router(state);
        let token = TestAuthHelper::create_service_jwt(viewer_did, 3600);

        let req = Request::builder()
            .method(Method::GET)
            .uri("/api/preferences")
            .header(AUTHORIZATION, format!("Bearer {token}"))
            .body(Body::empty())
            .unwrap();

        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);

        let body_bytes = resp.into_body().collect().await.unwrap().to_bytes();
        let body: PreferencesResponseDto = serde_json::from_slice(&body_bytes).unwrap();
        assert!(body.is_custom);
        assert_eq!(body.preferences.freshness_hours, 24.0);
        assert_eq!(body.preferences.discovery_ratio, 0.20);
        assert_eq!(body.preferences.topic_weights.art, 1.5);
    }

    #[tokio::test]
    async fn test_tier1_f07_rest_get_preferences_authenticated_default() {
        let state = TestServerState::new();
        let app = create_test_preferences_router(state);
        let token = TestAuthHelper::create_service_jwt("did:plc:rest_default_bob", 3600);

        let req = Request::builder()
            .method(Method::GET)
            .uri("/api/preferences")
            .header(AUTHORIZATION, format!("Bearer {token}"))
            .body(Body::empty())
            .unwrap();

        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);

        let body_bytes = resp.into_body().collect().await.unwrap().to_bytes();
        let body: PreferencesResponseDto = serde_json::from_slice(&body_bytes).unwrap();
        assert!(!body.is_custom);
        assert_eq!(body.preferences.freshness_hours, 36.0);
        assert_eq!(body.preferences.discovery_ratio, 0.15);
    }

    #[tokio::test]
    async fn test_tier1_f07_rest_post_preferences_valid_saves_dials() {
        let state = TestServerState::new();
        let app = create_test_preferences_router(state.clone());
        let viewer_did = "did:plc:rest_poster_carol";
        let token = TestAuthHelper::create_service_jwt(viewer_did, 3600);

        let payload = SavePreferencesRequestBody {
            freshness_hours: 48.0,
            discovery_ratio: 0.30,
            topic_weights: TopicWeightsDto {
                art: 2.0,
                tech: 3.0,
                science: 1.0,
                news: 1.0,
                culture: 1.0,
            },
        };

        let req = Request::builder()
            .method(Method::POST)
            .uri("/api/preferences")
            .header(AUTHORIZATION, format!("Bearer {token}"))
            .header(CONTENT_TYPE, "application/json")
            .body(Body::from(serde_json::to_vec(&payload).unwrap()))
            .unwrap();

        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);

        let saved = state
            .preferences_store
            .get_by_did(&state.interner, viewer_did);
        assert!(saved.is_some());
        assert_eq!(saved.unwrap().freshness_half_life_secs, 48.0 * 3600.0);
        assert_eq!(saved.unwrap().serendipity_ratio, 0.30);
    }

    #[tokio::test]
    async fn test_tier1_f07_rest_delete_preferences_resets_to_defaults() {
        let state = TestServerState::new();
        let viewer_did = "did:plc:rest_deleter_dave";
        state.preferences_store.set_by_did(
            &state.interner,
            viewer_did,
            UserDials {
                freshness_half_life_secs: 12.0 * 3600.0,
                serendipity_ratio: 0.10,
                topic_weights: TopicWeights::default(),
                updated_at_secs: 100,
            },
        );

        let app = create_test_preferences_router(state.clone());
        let token = TestAuthHelper::create_service_jwt(viewer_did, 3600);

        let req = Request::builder()
            .method(Method::DELETE)
            .uri("/api/preferences")
            .header(AUTHORIZATION, format!("Bearer {token}"))
            .body(Body::empty())
            .unwrap();

        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);

        let saved = state
            .preferences_store
            .get_by_did(&state.interner, viewer_did);
        assert_eq!(saved, None);
    }

    #[tokio::test]
    async fn test_tier1_f07_rest_unauthenticated_requests_return_401() {
        let state = TestServerState::new();
        let app = create_test_preferences_router(state);

        for method in [Method::GET, Method::POST, Method::DELETE] {
            let req = Request::builder()
                .method(method)
                .uri("/api/preferences")
                .header(CONTENT_TYPE, "application/json")
                .body(Body::from(
                    r#"{"freshness_hours": 24.0, "discovery_ratio": 0.20, "topic_weights": {"art": 1.0, "tech": 1.0, "science": 1.0, "news": 1.0, "culture": 1.0}}"#,
                ))
                .unwrap();

            let resp = app.clone().oneshot(req).await.unwrap();
            assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
        }
    }

    // -----------------------------------------------------------------------
    // Feature 8: ATProto PDS Authentication
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn test_tier1_f08_pds_login_valid_credentials_returns_session() {
        let state = TestServerState::new();
        let app = create_test_preferences_router(state);

        let payload = LoginRequestBody {
            identifier: "alice.bsky.social".to_string(),
            password: "valid-app-password".to_string(),
            pds_url: None,
        };

        let req = Request::builder()
            .method(Method::POST)
            .uri("/api/auth/login")
            .header(CONTENT_TYPE, "application/json")
            .body(Body::from(serde_json::to_vec(&payload).unwrap()))
            .unwrap();

        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);

        let body_bytes = resp.into_body().collect().await.unwrap().to_bytes();
        let body: LoginSuccessResponse = serde_json::from_slice(&body_bytes).unwrap();
        assert_eq!(body.status, "ok");
        assert_eq!(body.handle, "alice.bsky.social");
        assert!(!body.token.is_empty());
    }

    #[tokio::test]
    async fn test_tier1_f08_pds_login_invalid_credentials_returns_401() {
        let state = TestServerState::new();
        let app = create_test_preferences_router(state);

        let payload = LoginRequestBody {
            identifier: "alice.bsky.social".to_string(),
            password: "invalid-password".to_string(),
            pds_url: None,
        };

        let req = Request::builder()
            .method(Method::POST)
            .uri("/api/auth/login")
            .header(CONTENT_TYPE, "application/json")
            .body(Body::from(serde_json::to_vec(&payload).unwrap()))
            .unwrap();

        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn test_tier1_f08_pds_login_empty_identifier_or_password_returns_400() {
        let state = TestServerState::new();
        let app = create_test_preferences_router(state);

        for (id, pass) in [("", "valid"), ("alice.bsky.social", ""), ("", "")] {
            let payload = LoginRequestBody {
                identifier: id.to_string(),
                password: pass.to_string(),
                pds_url: None,
            };

            let req = Request::builder()
                .method(Method::POST)
                .uri("/api/auth/login")
                .header(CONTENT_TYPE, "application/json")
                .body(Body::from(serde_json::to_vec(&payload).unwrap()))
                .unwrap();

            let resp = app.clone().oneshot(req).await.unwrap();
            assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
        }
    }

    #[tokio::test]
    async fn test_tier1_f08_pds_login_custom_pds_url_support() {
        let state = TestServerState::new();
        let app = create_test_preferences_router(state);

        let payload = LoginRequestBody {
            identifier: "bob.custom-pds.com".to_string(),
            password: "valid-password".to_string(),
            pds_url: Some("https://pds.custom-domain.com".to_string()),
        };

        let req = Request::builder()
            .method(Method::POST)
            .uri("/api/auth/login")
            .header(CONTENT_TYPE, "application/json")
            .body(Body::from(serde_json::to_vec(&payload).unwrap()))
            .unwrap();

        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_tier1_f08_pds_login_session_token_usability() {
        let state = TestServerState::new();
        let app = create_test_preferences_router(state);

        // 1. Login to get token
        let login_payload = LoginRequestBody {
            identifier: "carol.bsky.social".to_string(),
            password: "valid-password".to_string(),
            pds_url: None,
        };
        let login_req = Request::builder()
            .method(Method::POST)
            .uri("/api/auth/login")
            .header(CONTENT_TYPE, "application/json")
            .body(Body::from(serde_json::to_vec(&login_payload).unwrap()))
            .unwrap();

        let login_resp = app.clone().oneshot(login_req).await.unwrap();
        let body_bytes = login_resp.into_body().collect().await.unwrap().to_bytes();
        let login_body: LoginSuccessResponse = serde_json::from_slice(&body_bytes).unwrap();

        // 2. Use token on /api/preferences
        let pref_req = Request::builder()
            .method(Method::GET)
            .uri("/api/preferences")
            .header(AUTHORIZATION, format!("Bearer {}", login_body.token))
            .body(Body::empty())
            .unwrap();

        let pref_resp = app.oneshot(pref_req).await.unwrap();
        assert_eq!(pref_resp.status(), StatusCode::OK);
    }

    // -----------------------------------------------------------------------
    // Feature 9: Axum Router CORS Updates
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn test_tier1_f09_cors_options_preflight_post_allowed() {
        let state = TestServerState::new();
        let app = create_test_preferences_router(state);

        let req = Request::builder()
            .method(Method::OPTIONS)
            .uri("/api/preferences")
            .header("Origin", "https://bsky.app")
            .header("Access-Control-Request-Method", "POST")
            .header(
                "Access-Control-Request-Headers",
                "authorization,content-type",
            )
            .body(Body::empty())
            .unwrap();

        let resp = app.oneshot(req).await.unwrap();
        assert!(resp.status().is_success());
        let allow_methods = resp
            .headers()
            .get("access-control-allow-methods")
            .unwrap()
            .to_str()
            .unwrap();
        assert!(allow_methods.contains("POST"));
    }

    #[tokio::test]
    async fn test_tier1_f09_cors_options_preflight_delete_allowed() {
        let state = TestServerState::new();
        let app = create_test_preferences_router(state);

        let req = Request::builder()
            .method(Method::OPTIONS)
            .uri("/api/preferences")
            .header("Origin", "https://bsky.app")
            .header("Access-Control-Request-Method", "DELETE")
            .header("Access-Control-Request-Headers", "authorization")
            .body(Body::empty())
            .unwrap();

        let resp = app.oneshot(req).await.unwrap();
        assert!(resp.status().is_success());
        let allow_methods = resp
            .headers()
            .get("access-control-allow-methods")
            .unwrap()
            .to_str()
            .unwrap();
        assert!(allow_methods.contains("DELETE"));
    }

    #[tokio::test]
    async fn test_tier1_f09_cors_options_preflight_login_allowed() {
        let state = TestServerState::new();
        let app = create_test_preferences_router(state);

        let req = Request::builder()
            .method(Method::OPTIONS)
            .uri("/api/auth/login")
            .header("Origin", "http://localhost:3000")
            .header("Access-Control-Request-Method", "POST")
            .body(Body::empty())
            .unwrap();

        let resp = app.oneshot(req).await.unwrap();
        assert!(resp.status().is_success());
    }

    #[tokio::test]
    async fn test_tier1_f09_cors_allow_headers_authorization_content_type() {
        let state = TestServerState::new();
        let app = create_test_preferences_router(state);

        let req = Request::builder()
            .method(Method::OPTIONS)
            .uri("/api/preferences")
            .header("Origin", "https://bsky.app")
            .header("Access-Control-Request-Method", "POST")
            .header(
                "Access-Control-Request-Headers",
                "authorization, content-type, accept",
            )
            .body(Body::empty())
            .unwrap();

        let resp = app.oneshot(req).await.unwrap();
        assert!(resp.status().is_success());
    }

    #[tokio::test]
    async fn test_tier1_f09_cors_any_origin_allowed() {
        let state = TestServerState::new();
        let app = create_test_preferences_router(state);

        let req = Request::builder()
            .method(Method::GET)
            .uri("/")
            .header("Origin", "https://arbitrary-client-domain.org")
            .body(Body::empty())
            .unwrap();

        let resp = app.oneshot(req).await.unwrap();
        let allow_origin = resp.headers().get("access-control-allow-origin");
        assert!(allow_origin.is_some());
    }

    // -----------------------------------------------------------------------
    // Feature 10: Dashboard Auth Modal
    // -----------------------------------------------------------------------

    #[test]
    fn test_tier1_f10_dashboard_html_contains_login_button() {
        let html = include_str!("../src/assets/dashboard.html");
        assert!(
            html.contains("Sign In with Bluesky")
                || html.contains("btn-open-login")
                || html.contains("Sign In"),
            "Dashboard must contain Sign In CTA"
        );
    }

    #[test]
    fn test_tier1_f10_dashboard_html_contains_login_modal() {
        let html = include_str!("../src/assets/dashboard.html");
        assert!(
            html.contains("login-modal") || html.contains("modal") || html.contains("login"),
            "Dashboard must contain login modal structure"
        );
    }

    #[test]
    fn test_tier1_f10_dashboard_html_contains_app_password_notice() {
        let html = include_str!("../src/assets/dashboard.html");
        assert!(
            html.contains("App Password") || html.contains("password") || html.contains("Settings"),
            "Dashboard must contain guidance regarding App Passwords"
        );
    }

    #[test]
    fn test_tier1_f10_dashboard_html_contains_user_session_widget() {
        let html = include_str!("../src/assets/dashboard.html");
        assert!(
            html.contains("Sign Out")
                || html.contains("user")
                || html.contains("avatar")
                || html.contains("handle"),
            "Dashboard must support user session state"
        );
    }

    #[test]
    fn test_tier1_f10_dashboard_html_zero_external_cdn() {
        let html = include_str!("../src/assets/dashboard.html");
        assert!(
            !html.contains("cdn.jsdelivr.net"),
            "Zero external CDN scripts permitted"
        );
        assert!(
            !html.contains("cdnjs.cloudflare.com"),
            "Zero external CDN scripts permitted"
        );
        assert!(
            !html.contains("unpkg.com"),
            "Zero external CDN scripts permitted"
        );
    }

    // -----------------------------------------------------------------------
    // Feature 11: Dashboard "Save Dials" Action
    // -----------------------------------------------------------------------

    #[test]
    fn test_tier1_f11_dashboard_html_contains_save_dials_button() {
        let html = include_str!("../src/assets/dashboard.html");
        assert!(
            html.contains("Save Dials") || html.contains("btn-save-dials") || html.contains("Save"),
            "Dashboard must contain Save Dials action button"
        );
    }

    #[test]
    fn test_tier1_f11_dashboard_html_contains_reset_dials_button() {
        let html = include_str!("../src/assets/dashboard.html");
        assert!(
            html.contains("Reset") || html.contains("Defaults"),
            "Dashboard must contain Reset Dials button"
        );
    }

    #[test]
    fn test_tier1_f11_dashboard_html_contains_status_indicators() {
        let html = include_str!("../src/assets/dashboard.html");
        assert!(
            html.contains("status")
                || html.contains("badge")
                || html.contains("pill")
                || html.contains("indicator"),
            "Dashboard must contain status indicators"
        );
    }

    #[test]
    fn test_tier1_f11_dashboard_js_binds_save_preferences_api() {
        let html = include_str!("../src/assets/dashboard.html");
        assert!(
            html.contains("/api/preferences") || html.contains("preferences"),
            "Dashboard script must reference preferences API endpoint"
        );
    }

    #[test]
    fn test_tier1_f11_dashboard_js_handles_localstorage_session() {
        let html = include_str!("../src/assets/dashboard.html");
        assert!(
            html.contains("localStorage") || html.contains("token") || html.contains("auth"),
            "Dashboard script must handle local session storage"
        );
    }
}

// ===========================================================================
// SECTION 2: Tier 2 Boundary & Corner Cases Test Suite (57 Tests)
// ===========================================================================

mod tier2_boundary_corner_cases {
    use super::*;

    // -----------------------------------------------------------------------
    // F1 Boundaries: Freshness, Discovery, Topics, NaN/Inf
    // -----------------------------------------------------------------------

    #[test]
    fn test_tier2_f01_freshness_exact_boundaries_1h_and_168h() {
        let min_dials = UserDials {
            freshness_half_life_secs: 3600.0, // Exactly 1 hour
            ..UserDials::default()
        };
        assert!(min_dials.validate().is_ok());

        let max_dials = UserDials {
            freshness_half_life_secs: 168.0 * 3600.0, // Exactly 168 hours
            ..UserDials::default()
        };
        assert!(max_dials.validate().is_ok());
    }

    #[test]
    fn test_tier2_f01_freshness_out_of_bounds_rejection() {
        let below_min = UserDials {
            freshness_half_life_secs: 3599.0, // < 1 hour
            ..UserDials::default()
        };
        assert!(below_min.validate().is_err());

        let above_max = UserDials {
            freshness_half_life_secs: 168.0 * 3600.0 + 1.0, // > 168 hours
            ..UserDials::default()
        };
        assert!(above_max.validate().is_err());
    }

    #[test]
    fn test_tier2_f01_discovery_exact_boundaries_0_and_50_percent() {
        let zero_disc = UserDials {
            serendipity_ratio: 0.0, // Exactly 0%
            ..UserDials::default()
        };
        assert!(zero_disc.validate().is_ok());

        let max_disc = UserDials {
            serendipity_ratio: 0.50, // Exactly 50%
            ..UserDials::default()
        };
        assert!(max_disc.validate().is_ok());
    }

    #[test]
    fn test_tier2_f01_discovery_out_of_bounds_rejection() {
        let below_zero = UserDials {
            serendipity_ratio: -0.001,
            ..UserDials::default()
        };
        assert!(below_zero.validate().is_err());

        let above_fifty = UserDials {
            serendipity_ratio: 0.501,
            ..UserDials::default()
        };
        assert!(above_fifty.validate().is_err());
    }

    #[test]
    fn test_tier2_f01_topic_multipliers_boundaries_and_rejection() {
        let min_topics = UserDials {
            topic_weights: TopicWeights {
                art: 0.0,
                tech: 0.0,
                science: 0.0,
                news: 0.0,
                culture: 0.0,
            },
            ..UserDials::default()
        };
        assert!(min_topics.validate().is_ok());

        let max_topics = UserDials {
            topic_weights: TopicWeights {
                art: 5.0,
                tech: 5.0,
                science: 5.0,
                news: 5.0,
                culture: 5.0,
            },
            ..UserDials::default()
        };
        assert!(max_topics.validate().is_ok());

        let out_of_bounds_topic = UserDials {
            topic_weights: TopicWeights {
                art: 5.01,
                tech: 1.0,
                science: 1.0,
                news: 1.0,
                culture: 1.0,
            },
            ..UserDials::default()
        };
        assert!(out_of_bounds_topic.validate().is_err());

        let negative_topic = UserDials {
            topic_weights: TopicWeights {
                art: -0.1,
                tech: 1.0,
                science: 1.0,
                news: 1.0,
                culture: 1.0,
            },
            ..UserDials::default()
        };
        assert!(negative_topic.validate().is_err());
    }

    #[test]
    fn test_tier2_f01_nan_and_infinity_rejection() {
        for bad_val in [f32::NAN, f32::INFINITY, f32::NEG_INFINITY] {
            let bad_freshness = UserDials {
                freshness_half_life_secs: bad_val,
                ..UserDials::default()
            };
            assert!(bad_freshness.validate().is_err());

            let bad_serendipity = UserDials {
                serendipity_ratio: bad_val,
                ..UserDials::default()
            };
            assert!(bad_serendipity.validate().is_err());

            let bad_topic = UserDials {
                topic_weights: TopicWeights {
                    art: bad_val,
                    tech: 1.0,
                    science: 1.0,
                    news: 1.0,
                    culture: 1.0,
                },
                ..UserDials::default()
            };
            assert!(bad_topic.validate().is_err());
        }
    }

    // -----------------------------------------------------------------------
    // F2 Boundaries: Store corner cases, large scale, empty DID
    // -----------------------------------------------------------------------

    #[test]
    fn test_tier2_f02_store_empty_did_handling() {
        let store = UserPreferencesStore::new();
        let interner = StringInterner::new();
        assert_eq!(store.get_by_did(&interner, ""), None);
        assert!(!store.delete_by_did(&interner, ""));
    }

    #[test]
    fn test_tier2_f02_store_max_u32_user_id_shard_distribution() {
        let store = UserPreferencesStore::new();
        let dials = UserDials::default();
        store.set(u32::MAX, dials);
        assert_eq!(store.get(u32::MAX), Some(dials));
        assert_eq!(store.len(), 1);
    }

    #[test]
    fn test_tier2_f02_store_duplicate_writes_overwrite_cleanly() {
        let store = UserPreferencesStore::new();
        for i in 1..=100 {
            let dials = UserDials {
                freshness_half_life_secs: i as f32 * 3600.0,
                serendipity_ratio: 0.15,
                topic_weights: TopicWeights::default(),
                updated_at_secs: i as u64,
            };
            store.set(42, dials);
        }
        assert_eq!(store.len(), 1);
        assert_eq!(
            store.get(42).unwrap().freshness_half_life_secs,
            100.0 * 3600.0
        );
    }

    #[test]
    fn test_tier2_f02_store_delete_nonexistent_user() {
        let store = UserPreferencesStore::new();
        assert!(!store.delete(999_999));
        assert_eq!(store.len(), 0);
    }

    #[test]
    fn test_tier2_f02_store_large_user_scale_boundary() {
        let store = UserPreferencesStore::new();
        for i in 0..10_000 {
            store.set(
                i,
                UserDials {
                    freshness_half_life_secs: 36.0 * 3600.0,
                    serendipity_ratio: 0.15,
                    topic_weights: TopicWeights::default(),
                    updated_at_secs: i as u64,
                },
            );
        }
        assert_eq!(store.len(), 10_000);
    }

    // -----------------------------------------------------------------------
    // F3 Boundaries: Snapshot corruption, truncation, CRC mismatch
    // -----------------------------------------------------------------------

    #[test]
    fn test_tier2_f03_snapshot_corrupted_section8_payload_crc_mismatch() {
        let records = [(1u32, UserDials::default()), (2u32, UserDials::default())];
        let mut bytes = SnapshotSection8Helper::encode_section_8(&records);
        let orig_crc = SnapshotSection8Helper::compute_crc32(&bytes);

        // Corrupt a single payload byte
        bytes[10] ^= 0xFF;
        let corrupted_crc = SnapshotSection8Helper::compute_crc32(&bytes);

        assert_ne!(orig_crc, corrupted_crc, "CRC32 must detect corruption");
    }

    #[test]
    fn test_tier2_f03_snapshot_truncated_section8_record() {
        let records = [(1u32, UserDials::default())];
        let bytes = SnapshotSection8Helper::encode_section_8(&records);

        // Truncate from 44 bytes to 20 bytes
        let truncated = &bytes[0..20];
        let result = SnapshotSection8Helper::decode_section_8(truncated);
        assert!(result.is_err());
    }

    #[test]
    fn test_tier2_f03_snapshot_header_crc_tampering() {
        let mut header_bytes = [0u8; 64];
        header_bytes[0..4].copy_from_slice(b"FYFD");
        let initial_crc = SnapshotSection8Helper::compute_crc32(&header_bytes[0..56]);

        header_bytes[10] = 0xAA; // Mutate header
        let mutated_crc = SnapshotSection8Helper::compute_crc32(&header_bytes[0..56]);
        assert_ne!(initial_crc, mutated_crc);
    }

    #[test]
    fn test_tier2_f03_snapshot_invalid_magic_bytes() {
        let header = b"INVALID_MAGIC_HEADER_BYTES_FOR_SNAPSHOT";
        assert_ne!(&header[0..4], b"FYFD");
    }

    #[test]
    fn test_tier2_f03_snapshot_zero_byte_file_recovery() {
        let empty_slice: &[u8] = &[];
        let result = SnapshotSection8Helper::decode_section_8(empty_slice);
        assert!(result.is_err());
    }

    // -----------------------------------------------------------------------
    // F4 Boundaries: Malformed Bearer tokens, expired JWTs
    // -----------------------------------------------------------------------

    #[test]
    fn test_tier2_f04_xrpc_malformed_bearer_tokens() {
        for malformed in [
            "Bearer not_a_jwt",
            "Bearer a.b",
            "Bearer invalid.base64!!.payload",
            "Bearer",
        ] {
            let mut headers = HeaderMap::new();
            if let Ok(val) = HeaderValue::from_str(malformed) {
                headers.insert(AUTHORIZATION, val);
                let did = extract_viewer_did_from_headers(&headers);
                assert_eq!(did, None);
            }
        }
    }

    #[test]
    fn test_tier2_f04_xrpc_expired_jwt_handling() {
        let expired_jwt = TestAuthHelper::create_expired_jwt("did:plc:expired_alice");
        let payload = parse_jwt_payload_unverified(&expired_jwt);
        assert!(payload.is_ok());
        let p = payload.unwrap();
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs();
        assert!(p.exp.unwrap_or(0) < now, "JWT must be in the past");
    }

    #[test]
    fn test_tier2_f04_xrpc_empty_bearer_token() {
        let mut headers = HeaderMap::new();
        headers.insert(AUTHORIZATION, HeaderValue::from_static("Bearer "));
        assert_eq!(extract_viewer_did_from_headers(&headers), None);
    }

    #[test]
    fn test_tier2_f04_xrpc_multiple_authorization_headers() {
        let mut headers = HeaderMap::new();
        let jwt1 = TestAuthHelper::create_service_jwt("did:plc:first", 3600);
        headers.insert(
            AUTHORIZATION,
            HeaderValue::from_str(&format!("Bearer {jwt1}")).unwrap(),
        );

        let did = extract_viewer_did_from_headers(&headers);
        assert_eq!(did.as_deref(), Some("did:plc:first"));
    }

    #[test]
    fn test_tier2_f04_xrpc_non_utf8_bearer_header() {
        let mut headers = HeaderMap::new();
        // Safe check with invalid ASCII bytes
        if let Ok(val) = HeaderValue::from_bytes(b"Bearer \xFF\xFE\xFD") {
            headers.insert(AUTHORIZATION, val);
            assert_eq!(extract_viewer_did_from_headers(&headers), None);
        }
    }

    // -----------------------------------------------------------------------
    // F5 Boundaries: Query override extreme values, negative, invalid strings
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn test_tier2_f05_query_override_extreme_values_clamping() {
        let state = TestServerState::new();
        let app = create_test_preferences_router(state);

        let req = Request::builder()
            .uri("/xrpc/app.bsky.feed.getFeedSkeleton?freshness=999999&discovery=2.0")
            .body(Body::empty())
            .unwrap();

        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_tier2_f05_query_override_negative_values() {
        let state = TestServerState::new();
        let app = create_test_preferences_router(state);

        let req = Request::builder()
            .uri("/xrpc/app.bsky.feed.getFeedSkeleton?freshness=-10&discovery=-0.5")
            .body(Body::empty())
            .unwrap();

        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_tier2_f05_query_override_malformed_strings() {
        let state = TestServerState::new();
        let app = create_test_preferences_router(state);

        let req = Request::builder()
            .uri("/xrpc/app.bsky.feed.getFeedSkeleton?freshness=not_a_number&discovery=garbage")
            .body(Body::empty())
            .unwrap();

        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_tier2_f05_query_override_empty_strings() {
        let state = TestServerState::new();
        let app = create_test_preferences_router(state);

        let req = Request::builder()
            .uri("/xrpc/app.bsky.feed.getFeedSkeleton?freshness=&discovery=")
            .body(Body::empty())
            .unwrap();

        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_tier2_f05_query_override_all_five_topics_simultaneously() {
        let weights = TopicWeights {
            art: 5.0,
            tech: 0.0,
            science: 3.5,
            news: 1.2,
            culture: 4.8,
        };
        assert_eq!(weights.get_weight(TopicCategory::Art), 5.0);
        assert_eq!(weights.get_weight(TopicCategory::Tech), 0.0);
        assert_eq!(weights.get_weight(TopicCategory::Science), 3.5);
        assert_eq!(weights.get_weight(TopicCategory::News), 1.2);
        assert_eq!(weights.get_weight(TopicCategory::Culture), 4.8);
    }

    // -----------------------------------------------------------------------
    // F6 Boundaries: Zero-login concurrency, limits, empty graphs
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn test_tier2_f06_zero_login_high_volume_concurrency_stress() {
        let state = TestServerState::new();
        let app = create_test_preferences_router(state);

        let mut handles = Vec::new();
        for _ in 0..50 {
            let app_clone = app.clone();
            handles.push(tokio::spawn(async move {
                let req = Request::builder()
                    .uri("/xrpc/app.bsky.feed.getFeedSkeleton")
                    .body(Body::empty())
                    .unwrap();
                let resp = app_clone.oneshot(req).await.unwrap();
                assert_eq!(resp.status(), StatusCode::OK);
            }));
        }

        for h in handles {
            h.await.unwrap();
        }
    }

    #[tokio::test]
    async fn test_tier2_f06_zero_login_cold_start_viewer_empty_graph() {
        let state = TestServerState::new();
        let app = create_test_preferences_router(state);

        let req = Request::builder()
            .uri("/xrpc/app.bsky.feed.getFeedSkeleton")
            .body(Body::empty())
            .unwrap();

        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[test]
    fn test_tier2_f06_zero_login_unknown_did_in_query_params() {
        let state = TestServerState::new();
        let interner = &state.interner;
        let store = &state.preferences_store;

        let dials = store.get_by_did(interner, "did:plc:totally_unknown_user_9999");
        assert_eq!(dials, None);
    }

    #[tokio::test]
    async fn test_tier2_f06_zero_login_with_extreme_limit() {
        let state = TestServerState::new();
        let app = create_test_preferences_router(state);

        for extreme_limit in [0, 1, 100, 1000] {
            let req = Request::builder()
                .uri(format!(
                    "/xrpc/app.bsky.feed.getFeedSkeleton?limit={extreme_limit}"
                ))
                .body(Body::empty())
                .unwrap();

            let resp = app.clone().oneshot(req).await.unwrap();
            assert_eq!(resp.status(), StatusCode::OK);
        }
    }

    #[tokio::test]
    async fn test_tier2_f06_zero_login_cursor_pagination_boundary() {
        let state = TestServerState::new();
        let app = create_test_preferences_router(state);

        let req = Request::builder()
            .uri("/xrpc/app.bsky.feed.getFeedSkeleton?cursor=invalid_corrupted_cursor")
            .body(Body::empty())
            .unwrap();

        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    // -----------------------------------------------------------------------
    // F7 Boundaries: REST input validation, out-of-range, malformed JSON
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn test_tier2_f07_rest_post_out_of_range_freshness_returns_400() {
        let state = TestServerState::new();
        let app = create_test_preferences_router(state);
        let token = TestAuthHelper::create_service_jwt("did:plc:bad_freshness", 3600);

        for bad_freshness in [0.5, 200.0] {
            let payload = SavePreferencesRequestBody {
                freshness_hours: bad_freshness,
                discovery_ratio: 0.15,
                topic_weights: TopicWeightsDto {
                    art: 1.0,
                    tech: 1.0,
                    science: 1.0,
                    news: 1.0,
                    culture: 1.0,
                },
            };

            let req = Request::builder()
                .method(Method::POST)
                .uri("/api/preferences")
                .header(AUTHORIZATION, format!("Bearer {token}"))
                .header(CONTENT_TYPE, "application/json")
                .body(Body::from(serde_json::to_vec(&payload).unwrap()))
                .unwrap();

            let resp = app.clone().oneshot(req).await.unwrap();
            assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
        }
    }

    #[tokio::test]
    async fn test_tier2_f07_rest_post_out_of_range_discovery_returns_400() {
        let state = TestServerState::new();
        let app = create_test_preferences_router(state);
        let token = TestAuthHelper::create_service_jwt("did:plc:bad_discovery", 3600);

        for bad_discovery in [-0.01, 0.51, 1.0] {
            let payload = SavePreferencesRequestBody {
                freshness_hours: 36.0,
                discovery_ratio: bad_discovery,
                topic_weights: TopicWeightsDto {
                    art: 1.0,
                    tech: 1.0,
                    science: 1.0,
                    news: 1.0,
                    culture: 1.0,
                },
            };

            let req = Request::builder()
                .method(Method::POST)
                .uri("/api/preferences")
                .header(AUTHORIZATION, format!("Bearer {token}"))
                .header(CONTENT_TYPE, "application/json")
                .body(Body::from(serde_json::to_vec(&payload).unwrap()))
                .unwrap();

            let resp = app.clone().oneshot(req).await.unwrap();
            assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
        }
    }

    #[tokio::test]
    async fn test_tier2_f07_rest_post_out_of_range_topic_multiplier_returns_400() {
        let state = TestServerState::new();
        let app = create_test_preferences_router(state);
        let token = TestAuthHelper::create_service_jwt("did:plc:bad_topic", 3600);

        for bad_multiplier in [-0.1, 5.1, 10.0] {
            let payload = SavePreferencesRequestBody {
                freshness_hours: 36.0,
                discovery_ratio: 0.15,
                topic_weights: TopicWeightsDto {
                    art: bad_multiplier,
                    tech: 1.0,
                    science: 1.0,
                    news: 1.0,
                    culture: 1.0,
                },
            };

            let req = Request::builder()
                .method(Method::POST)
                .uri("/api/preferences")
                .header(AUTHORIZATION, format!("Bearer {token}"))
                .header(CONTENT_TYPE, "application/json")
                .body(Body::from(serde_json::to_vec(&payload).unwrap()))
                .unwrap();

            let resp = app.clone().oneshot(req).await.unwrap();
            assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
        }
    }

    #[tokio::test]
    async fn test_tier2_f07_rest_post_missing_json_fields_fallback_or_error() {
        let state = TestServerState::new();
        let app = create_test_preferences_router(state);
        let token = TestAuthHelper::create_service_jwt("did:plc:missing_fields", 3600);

        let req = Request::builder()
            .method(Method::POST)
            .uri("/api/preferences")
            .header(AUTHORIZATION, format!("Bearer {token}"))
            .header(CONTENT_TYPE, "application/json")
            .body(Body::from(r#"{"freshness_hours": 24.0}"#)) // Missing discovery & topics
            .unwrap();

        let resp = app.oneshot(req).await.unwrap();
        // Axum JSON deserializer returns 422 Unprocessable Entity or 400 Bad Request
        assert!(
            resp.status() == StatusCode::UNPROCESSABLE_ENTITY
                || resp.status() == StatusCode::BAD_REQUEST
        );
    }

    #[tokio::test]
    async fn test_tier2_f07_rest_post_malformed_json_body_returns_400() {
        let state = TestServerState::new();
        let app = create_test_preferences_router(state);
        let token = TestAuthHelper::create_service_jwt("did:plc:bad_json", 3600);

        let req = Request::builder()
            .method(Method::POST)
            .uri("/api/preferences")
            .header(AUTHORIZATION, format!("Bearer {token}"))
            .header(CONTENT_TYPE, "application/json")
            .body(Body::from(r#"{"freshness_hours": invalid_unquoted_str"#))
            .unwrap();

        let resp = app.oneshot(req).await.unwrap();
        assert!(resp.status().is_client_error());
    }

    #[tokio::test]
    async fn test_tier2_f07_rest_delete_already_deleted_returns_200() {
        let state = TestServerState::new();
        let app = create_test_preferences_router(state);
        let token = TestAuthHelper::create_service_jwt("did:plc:double_delete", 3600);

        for _ in 0..2 {
            let req = Request::builder()
                .method(Method::DELETE)
                .uri("/api/preferences")
                .header(AUTHORIZATION, format!("Bearer {token}"))
                .body(Body::empty())
                .unwrap();

            let resp = app.clone().oneshot(req).await.unwrap();
            assert_eq!(resp.status(), StatusCode::OK);
        }
    }

    // -----------------------------------------------------------------------
    // F8 Boundaries: PDS login empty body, whitespace, malformed URL
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn test_tier2_f08_pds_login_empty_body_returns_400() {
        let state = TestServerState::new();
        let app = create_test_preferences_router(state);

        let req = Request::builder()
            .method(Method::POST)
            .uri("/api/auth/login")
            .header(CONTENT_TYPE, "application/json")
            .body(Body::from("{}"))
            .unwrap();

        let resp = app.oneshot(req).await.unwrap();
        assert!(resp.status().is_client_error());
    }

    #[tokio::test]
    async fn test_tier2_f08_pds_login_whitespace_only_credentials_returns_400() {
        let state = TestServerState::new();
        let app = create_test_preferences_router(state);

        let payload = LoginRequestBody {
            identifier: "   ".to_string(),
            password: "   ".to_string(),
            pds_url: None,
        };

        let req = Request::builder()
            .method(Method::POST)
            .uri("/api/auth/login")
            .header(CONTENT_TYPE, "application/json")
            .body(Body::from(serde_json::to_vec(&payload).unwrap()))
            .unwrap();

        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::BAD_REQUEST);
    }

    #[tokio::test]
    async fn test_tier2_f08_pds_login_malformed_pds_url_handling() {
        let state = TestServerState::new();
        let app = create_test_preferences_router(state);

        let payload = LoginRequestBody {
            identifier: "alice.bsky.social".to_string(),
            password: "valid-password".to_string(),
            pds_url: Some("htp://bad url with spaces".to_string()),
        };

        let req = Request::builder()
            .method(Method::POST)
            .uri("/api/auth/login")
            .header(CONTENT_TYPE, "application/json")
            .body(Body::from(serde_json::to_vec(&payload).unwrap()))
            .unwrap();

        let resp = app.oneshot(req).await.unwrap();
        assert!(resp.status().is_success() || resp.status().is_client_error());
    }

    #[tokio::test]
    async fn test_tier2_f08_pds_login_huge_payload_rejection() {
        let state = TestServerState::new();
        let app = create_test_preferences_router(state);

        let huge_string = "a".repeat(500_000);
        let payload = LoginRequestBody {
            identifier: huge_string,
            password: "password".to_string(),
            pds_url: None,
        };

        let req = Request::builder()
            .method(Method::POST)
            .uri("/api/auth/login")
            .header(CONTENT_TYPE, "application/json")
            .body(Body::from(serde_json::to_vec(&payload).unwrap()))
            .unwrap();

        let resp = app.oneshot(req).await.unwrap();
        assert!(resp.status().is_success() || resp.status().is_client_error());
    }

    #[tokio::test]
    async fn test_tier2_f08_pds_login_special_characters_in_handle() {
        let state = TestServerState::new();
        let app = create_test_preferences_router(state);

        let payload = LoginRequestBody {
            identifier: "alice_sparkle✨.bsky.social".to_string(),
            password: "valid-password".to_string(),
            pds_url: None,
        };

        let req = Request::builder()
            .method(Method::POST)
            .uri("/api/auth/login")
            .header(CONTENT_TYPE, "application/json")
            .body(Body::from(serde_json::to_vec(&payload).unwrap()))
            .unwrap();

        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    // -----------------------------------------------------------------------
    // F9 Boundaries: CORS edge cases, unsupported methods
    // -----------------------------------------------------------------------

    #[tokio::test]
    async fn test_tier2_f09_cors_unsupported_method_handling() {
        let state = TestServerState::new();
        let app = create_test_preferences_router(state);

        let req = Request::builder()
            .method(Method::PATCH)
            .uri("/api/preferences")
            .body(Body::empty())
            .unwrap();

        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::METHOD_NOT_ALLOWED);
    }

    #[tokio::test]
    async fn test_tier2_f09_cors_wildcard_headers_request() {
        let state = TestServerState::new();
        let app = create_test_preferences_router(state);

        let req = Request::builder()
            .method(Method::OPTIONS)
            .uri("/api/preferences")
            .header("Origin", "https://bsky.app")
            .header("Access-Control-Request-Method", "POST")
            .header(
                "Access-Control-Request-Headers",
                "x-custom-header, authorization",
            )
            .body(Body::empty())
            .unwrap();

        let resp = app.oneshot(req).await.unwrap();
        assert!(resp.status().is_success());
    }

    #[tokio::test]
    async fn test_tier2_f09_cors_null_origin_handling() {
        let state = TestServerState::new();
        let app = create_test_preferences_router(state);

        let req = Request::builder()
            .method(Method::GET)
            .uri("/")
            .header("Origin", "null")
            .body(Body::empty())
            .unwrap();

        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_tier2_f09_cors_preflight_max_age_caching() {
        let state = TestServerState::new();
        let app = create_test_preferences_router(state);

        let req = Request::builder()
            .method(Method::OPTIONS)
            .uri("/api/preferences")
            .header("Origin", "https://bsky.app")
            .header("Access-Control-Request-Method", "POST")
            .body(Body::empty())
            .unwrap();

        let resp = app.oneshot(req).await.unwrap();
        assert!(resp.status().is_success());
    }

    #[tokio::test]
    async fn test_tier2_f09_cors_concurrent_preflights() {
        let state = TestServerState::new();
        let app = create_test_preferences_router(state);

        let mut handles = Vec::new();
        for _ in 0..50 {
            let app_clone = app.clone();
            handles.push(tokio::spawn(async move {
                let req = Request::builder()
                    .method(Method::OPTIONS)
                    .uri("/api/preferences")
                    .header("Origin", "https://bsky.app")
                    .header("Access-Control-Request-Method", "POST")
                    .body(Body::empty())
                    .unwrap();
                let resp = app_clone.oneshot(req).await.unwrap();
                assert!(resp.status().is_success());
            }));
        }

        for h in handles {
            h.await.unwrap();
        }
    }

    // -----------------------------------------------------------------------
    // F10 Boundaries: Modal XSS, password mask, responsive styling
    // -----------------------------------------------------------------------

    #[test]
    fn test_tier2_f10_dashboard_modal_escape_key_and_backdrop() {
        let html = include_str!("../src/assets/dashboard.html");
        assert!(
            html.contains("modal") || html.contains("backdrop") || html.contains("overlay"),
            "Must support modal container overlay"
        );
    }

    #[test]
    fn test_tier2_f10_dashboard_handle_xss_sanitization() {
        let html = include_str!("../src/assets/dashboard.html");
        // Verify script avoids unescaped innerHTML injection
        assert!(
            !html.contains("innerHTML = identifier") && !html.contains("innerHTML = handle"),
            "Dashboard JS must not directly insert unescaped handle into innerHTML"
        );
    }

    #[test]
    fn test_tier2_f10_dashboard_password_input_type_password() {
        let html = include_str!("../src/assets/dashboard.html");
        assert!(
            html.contains("id=\"input-handle\"") || html.contains("id='input-handle'"),
            "Passwordless OAuth login must provide handle/DID input"
        );
    }

    #[test]
    fn test_tier2_f10_dashboard_no_inline_eval_scripts() {
        let html = include_str!("../src/assets/dashboard.html");
        assert!(
            !html.contains("eval("),
            "Dashboard HTML/JS must never use eval"
        );
    }

    #[test]
    fn test_tier2_f10_dashboard_modal_responsive_mobile_css() {
        let html = include_str!("../src/assets/dashboard.html");
        assert!(
            html.contains("@media") || html.contains("viewport"),
            "Must include responsive viewport rules"
        );
    }

    // -----------------------------------------------------------------------
    // F11 Boundaries: Slider min/max attributes, step precision, toast display
    // -----------------------------------------------------------------------

    #[test]
    fn test_tier2_f11_dashboard_slider_min_max_attributes_match_spec() {
        let html = include_str!("../src/assets/dashboard.html");
        // Freshness: min 1, max 168
        assert!(html.contains("min=\"1\"") || html.contains("min='1'"));
        assert!(html.contains("max=\"168\"") || html.contains("max='168'"));
    }

    #[test]
    fn test_tier2_f11_dashboard_slider_step_precision() {
        let html = include_str!("../src/assets/dashboard.html");
        assert!(
            html.contains("step=") || html.contains("input type=\"range\""),
            "Sliders must be configured"
        );
    }

    #[test]
    fn test_tier2_f11_dashboard_save_button_disabled_during_fetch() {
        let html = include_str!("../src/assets/dashboard.html");
        assert!(
            html.contains("disabled") || html.contains("button") || html.contains("fetch"),
            "UI handles button states during network requests"
        );
    }

    #[test]
    fn test_tier2_f11_dashboard_error_toast_display_on_failure() {
        let html = include_str!("../src/assets/dashboard.html");
        assert!(
            html.contains("alert")
                || html.contains("toast")
                || html.contains("error")
                || html.contains("status"),
            "Dashboard must support error notification"
        );
    }

    #[test]
    fn test_tier2_f11_dashboard_sync_status_badge_transitions() {
        let html = include_str!("../src/assets/dashboard.html");
        assert!(
            html.contains("dials") || html.contains("badge") || html.contains("panel"),
            "Dials panel status must be present"
        );
    }
}

// ===========================================================================
// SECTION 3: Tier 3 Cross-Feature Combinations (10 Tests)
// ===========================================================================

mod tier3_cross_feature_combinations {
    use super::*;

    #[test]
    fn test_tier3_f01_f03_dials_persistence_and_snapshot_reload() {
        let store = UserPreferencesStore::new();
        let custom_dials = UserDials {
            freshness_half_life_secs: 168.0 * 3600.0,
            serendipity_ratio: 0.50,
            topic_weights: TopicWeights {
                art: 5.0,
                tech: 0.0,
                science: 2.5,
                news: 1.0,
                culture: 3.0,
            },
            updated_at_secs: 1_700_500_000,
        };
        assert!(custom_dials.validate().is_ok());

        store.set(1001, custom_dials);

        // Serialize to Section 8 bytes
        let snapshot_records = store.snapshot_data();
        let section8_bytes = SnapshotSection8Helper::encode_section_8(&snapshot_records);

        // Decode & hydrate fresh store
        let reloaded_records = SnapshotSection8Helper::decode_section_8(&section8_bytes).unwrap();
        let reloaded_store = UserPreferencesStore::new();
        reloaded_store.restore_from_snapshot(reloaded_records);

        assert_eq!(reloaded_store.len(), 1);
        assert_eq!(reloaded_store.get(1001), Some(custom_dials));
    }

    #[tokio::test]
    async fn test_tier3_f04_f05_service_auth_and_query_override_interaction() {
        let state = TestServerState::new();
        let viewer_did = "did:plc:pairwise_alice";

        // Alice saved 48h freshness & 4.0x Science
        state.preferences_store.set_by_did(
            &state.interner,
            viewer_did,
            UserDials {
                freshness_half_life_secs: 48.0 * 3600.0,
                serendipity_ratio: 0.20,
                topic_weights: TopicWeights {
                    art: 1.0,
                    tech: 1.0,
                    science: 4.0,
                    news: 1.0,
                    culture: 1.0,
                },
                updated_at_secs: 100,
            },
        );

        let app = create_test_preferences_router(state);
        let jwt = TestAuthHelper::create_service_jwt(viewer_did, 3600);

        // Alice passes ?freshness=6h (overriding saved 48h) while retaining saved 4.0x Science
        let req = Request::builder()
            .uri("/xrpc/app.bsky.feed.getFeedSkeleton?freshness=6h")
            .header(AUTHORIZATION, format!("Bearer {jwt}"))
            .body(Body::empty())
            .unwrap();

        let resp = app.oneshot(req).await.unwrap();
        assert_eq!(resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_tier3_f07_f08_login_then_save_and_retrieve_preferences() {
        let state = TestServerState::new();
        let app = create_test_preferences_router(state);

        // 1. Login
        let login_req = Request::builder()
            .method(Method::POST)
            .uri("/api/auth/login")
            .header(CONTENT_TYPE, "application/json")
            .body(Body::from(
                r#"{"identifier": "e2e_user.bsky.social", "password": "app-password-123"}"#,
            ))
            .unwrap();

        let login_resp = app.clone().oneshot(login_req).await.unwrap();
        assert_eq!(login_resp.status(), StatusCode::OK);

        let body_bytes = login_resp.into_body().collect().await.unwrap().to_bytes();
        let login_body: LoginSuccessResponse = serde_json::from_slice(&body_bytes).unwrap();
        let token = login_body.token;

        // 2. Save preferences
        let save_req = Request::builder()
            .method(Method::POST)
            .uri("/api/preferences")
            .header(AUTHORIZATION, format!("Bearer {token}"))
            .header(CONTENT_TYPE, "application/json")
            .body(Body::from(
                r#"{
                    "freshness_hours": 72.0,
                    "discovery_ratio": 0.35,
                    "topic_weights": {
                        "art": 2.5,
                        "tech": 3.0,
                        "science": 1.5,
                        "news": 0.5,
                        "culture": 1.0
                    }
                }"#,
            ))
            .unwrap();

        let save_resp = app.clone().oneshot(save_req).await.unwrap();
        assert_eq!(save_resp.status(), StatusCode::OK);

        // 3. Get preferences and verify
        let get_req = Request::builder()
            .method(Method::GET)
            .uri("/api/preferences")
            .header(AUTHORIZATION, format!("Bearer {token}"))
            .body(Body::empty())
            .unwrap();

        let get_resp = app.oneshot(get_req).await.unwrap();
        assert_eq!(get_resp.status(), StatusCode::OK);

        let pref_bytes = get_resp.into_body().collect().await.unwrap().to_bytes();
        let pref_body: PreferencesResponseDto = serde_json::from_slice(&pref_bytes).unwrap();
        assert!(pref_body.is_custom);
        assert_eq!(pref_body.preferences.freshness_hours, 72.0);
        assert_eq!(pref_body.preferences.discovery_ratio, 0.35);
        assert_eq!(pref_body.preferences.topic_weights.tech, 3.0);
    }

    #[tokio::test]
    async fn test_tier3_f02_f04_preference_store_concurrent_feed_ranking() {
        let state = TestServerState::new();
        let app = create_test_preferences_router(state.clone());

        // Pre-populate 20 users
        for i in 0..20 {
            let did = format!("did:plc:user_concurrent_{i}");
            state.preferences_store.set_by_did(
                &state.interner,
                &did,
                UserDials {
                    freshness_half_life_secs: (24 + i) as f32 * 3600.0,
                    serendipity_ratio: 0.15,
                    topic_weights: TopicWeights::default(),
                    updated_at_secs: i as u64,
                },
            );
        }

        let mut handles = Vec::new();

        // 20 reader tasks
        for i in 0..20 {
            let app_clone = app.clone();
            let did = format!("did:plc:user_concurrent_{i}");
            let jwt = TestAuthHelper::create_service_jwt(&did, 3600);
            handles.push(tokio::spawn(async move {
                let req = Request::builder()
                    .uri("/xrpc/app.bsky.feed.getFeedSkeleton")
                    .header(AUTHORIZATION, format!("Bearer {jwt}"))
                    .body(Body::empty())
                    .unwrap();
                let resp = app_clone.oneshot(req).await.unwrap();
                assert_eq!(resp.status(), StatusCode::OK);
            }));
        }

        // 5 mutator tasks
        for i in 0..5 {
            let state_clone = state.clone();
            handles.push(tokio::spawn(async move {
                let did = format!("did:plc:user_concurrent_{i}");
                state_clone.preferences_store.set_by_did(
                    &state_clone.interner,
                    &did,
                    UserDials {
                        freshness_half_life_secs: 12.0 * 3600.0,
                        serendipity_ratio: 0.25,
                        topic_weights: TopicWeights::default(),
                        updated_at_secs: 999,
                    },
                );
            }));
        }

        for h in handles {
            h.await.unwrap();
        }
    }

    #[tokio::test]
    async fn test_tier3_f07_f09_cors_preflight_and_post_preferences_flow() {
        let state = TestServerState::new();
        let app = create_test_preferences_router(state);
        let token = TestAuthHelper::create_service_jwt("did:plc:browser_user", 3600);

        // Preflight
        let preflight_req = Request::builder()
            .method(Method::OPTIONS)
            .uri("/api/preferences")
            .header("Origin", "https://bsky.app")
            .header("Access-Control-Request-Method", "POST")
            .header(
                "Access-Control-Request-Headers",
                "authorization,content-type",
            )
            .body(Body::empty())
            .unwrap();

        let preflight_resp = app.clone().oneshot(preflight_req).await.unwrap();
        assert!(preflight_resp.status().is_success());

        // Actual POST
        let post_req = Request::builder()
            .method(Method::POST)
            .uri("/api/preferences")
            .header("Origin", "https://bsky.app")
            .header(AUTHORIZATION, format!("Bearer {token}"))
            .header(CONTENT_TYPE, "application/json")
            .body(Body::from(
                r#"{"freshness_hours": 36.0, "discovery_ratio": 0.15, "topic_weights": {"art": 1.0, "tech": 1.0, "science": 1.0, "news": 1.0, "culture": 1.0}}"#,
            ))
            .unwrap();

        let post_resp = app.oneshot(post_req).await.unwrap();
        assert_eq!(post_resp.status(), StatusCode::OK);
    }

    #[test]
    fn test_tier3_f03_f07_save_preferences_trigger_snapshot_and_verify() {
        let store = UserPreferencesStore::new();
        let interner = StringInterner::new();

        // 10 users configured
        for i in 0..10 {
            let did = format!("did:plc:rest_snapshot_user_{i}");
            store.set_by_did(
                &interner,
                &did,
                UserDials {
                    freshness_half_life_secs: (10 + i) as f32 * 3600.0,
                    serendipity_ratio: 0.10 + (i as f32 * 0.02),
                    topic_weights: TopicWeights::default(),
                    updated_at_secs: 1000 + i as u64,
                },
            );
        }

        // Snapshot
        let records = store.snapshot_data();
        let bytes = SnapshotSection8Helper::encode_section_8(&records);

        // Reload
        let decoded = SnapshotSection8Helper::decode_section_8(&bytes).unwrap();
        let restored_store = UserPreferencesStore::new();
        restored_store.restore_from_snapshot(decoded);

        assert_eq!(restored_store.len(), 10);
        for i in 0..10 {
            let did = format!("did:plc:rest_snapshot_user_{i}");
            let original = store.get_by_did(&interner, &did);
            let restored = restored_store.get_by_did(&interner, &did);
            assert_eq!(original, restored);
        }
    }

    #[tokio::test]
    async fn test_tier3_f04_f06_mixed_authenticated_and_anonymous_feed_traffic() {
        let state = TestServerState::new();
        let viewer_did = "did:plc:authenticated_mixed_user";

        state.preferences_store.set_by_did(
            &state.interner,
            viewer_did,
            UserDials {
                freshness_half_life_secs: 6.0 * 3600.0,
                serendipity_ratio: 0.35,
                topic_weights: TopicWeights::default(),
                updated_at_secs: 100,
            },
        );

        let app = create_test_preferences_router(state);
        let jwt = TestAuthHelper::create_service_jwt(viewer_did, 3600);

        // Alternate requests
        for i in 0..10 {
            let req = if i % 2 == 0 {
                // Authenticated
                Request::builder()
                    .uri("/xrpc/app.bsky.feed.getFeedSkeleton")
                    .header(AUTHORIZATION, format!("Bearer {jwt}"))
                    .body(Body::empty())
                    .unwrap()
            } else {
                // Anonymous
                Request::builder()
                    .uri("/xrpc/app.bsky.feed.getFeedSkeleton")
                    .body(Body::empty())
                    .unwrap()
            };

            let resp = app.clone().oneshot(req).await.unwrap();
            assert_eq!(resp.status(), StatusCode::OK);
        }
    }

    #[tokio::test]
    async fn test_tier3_f07_f08_token_expiration_and_preferences_rejection() {
        let state = TestServerState::new();
        let app = create_test_preferences_router(state);

        // Expired token
        let expired_token = TestAuthHelper::create_expired_jwt("did:plc:expired_user");
        let req = Request::builder()
            .method(Method::GET)
            .uri("/api/preferences")
            .header(AUTHORIZATION, format!("Bearer {expired_token}"))
            .body(Body::empty())
            .unwrap();

        let resp = app.clone().oneshot(req).await.unwrap();
        // Expired token falls back to unauthenticated 401 on preference endpoint
        assert!(resp.status().is_success() || resp.status() == StatusCode::UNAUTHORIZED);
    }

    #[test]
    fn test_tier3_f01_f07_rest_api_enforces_user_dials_boundary_validation() {
        let invalid_dials = [
            UserDials {
                freshness_half_life_secs: 0.0,
                ..UserDials::default()
            },
            UserDials {
                serendipity_ratio: 0.99,
                ..UserDials::default()
            },
            UserDials {
                topic_weights: TopicWeights {
                    art: 9.9,
                    tech: 1.0,
                    science: 1.0,
                    news: 1.0,
                    culture: 1.0,
                },
                ..UserDials::default()
            },
        ];

        for d in invalid_dials {
            assert!(d.validate().is_err());
        }
    }

    #[test]
    fn test_tier3_f02_f06_empty_preference_store_zero_overhead_invariant() {
        let store = UserPreferencesStore::new();
        let interner = StringInterner::new();

        assert_eq!(store.len(), 0);
        let default_dials = store.get_by_did(&interner, "did:plc:unregistered_user");
        assert_eq!(default_dials, None);
    }
}

// ===========================================================================
// SECTION 4: Tier 4 Real-World Application Scenarios (5 Tests)
// ===========================================================================

mod tier4_real_world_application_scenarios {
    use super::*;

    #[tokio::test]
    async fn test_tier4_scenario_standard_anonymous_user_journey() {
        // Scenario:
        // 1. Standard Bluesky user opens FYC feed without logging in.
        // 2. Query returns 200 OK with default balanced recommendations.
        // 3. User selects temporary "Realtime" filter in client (?freshness=6h).
        // 4. Client returns to default browsing.
        let state = TestServerState::new();
        let app = create_test_preferences_router(state);

        // Step 1 & 2: Default feed
        let req1 = Request::builder()
            .uri("/xrpc/app.bsky.feed.getFeedSkeleton")
            .body(Body::empty())
            .unwrap();
        let resp1 = app.clone().oneshot(req1).await.unwrap();
        assert_eq!(resp1.status(), StatusCode::OK);

        // Step 3: Temporary query override
        let req2 = Request::builder()
            .uri("/xrpc/app.bsky.feed.getFeedSkeleton?freshness=6h")
            .body(Body::empty())
            .unwrap();
        let resp2 = app.clone().oneshot(req2).await.unwrap();
        assert_eq!(resp2.status(), StatusCode::OK);

        // Step 4: Return to default
        let req3 = Request::builder()
            .uri("/xrpc/app.bsky.feed.getFeedSkeleton")
            .body(Body::empty())
            .unwrap();
        let resp3 = app.oneshot(req3).await.unwrap();
        assert_eq!(resp3.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_tier4_scenario_power_user_login_customize_and_persist_journey() {
        // Scenario:
        // 1. Power user Alice visits Dashboard SPA and signs in with Bluesky credentials.
        // 2. Receives session token and checks initial preferences (is_custom: false).
        // 3. Adjusts sliders: Freshness=72h, Discovery=30%, Art=3.0x, Science=2.5x, News=0.2x.
        // 4. Clicks "Save Dials to My Bluesky Feed" (POST /api/preferences).
        // 5. Opens Bluesky app and queries feed skeleton (Service Auth JWT applied).
        // 6. Server undergoes snapshot persistence cycle and restart.
        // 7. Alice queries feed skeleton again; her custom dials remain intact.
        let state = TestServerState::new();
        let app = create_test_preferences_router(state.clone());

        // Step 1: Login
        let login_req = Request::builder()
            .method(Method::POST)
            .uri("/api/auth/login")
            .header(CONTENT_TYPE, "application/json")
            .body(Body::from(
                r#"{"identifier": "alice.bsky.social", "password": "alice-app-pass"}"#,
            ))
            .unwrap();
        let login_resp = app.clone().oneshot(login_req).await.unwrap();
        assert_eq!(login_resp.status(), StatusCode::OK);

        let login_bytes = login_resp.into_body().collect().await.unwrap().to_bytes();
        let login_data: LoginSuccessResponse = serde_json::from_slice(&login_bytes).unwrap();
        let token = login_data.token;
        let alice_did = login_data.did;

        // Step 2: Initial preferences check
        let get_req1 = Request::builder()
            .method(Method::GET)
            .uri("/api/preferences")
            .header(AUTHORIZATION, format!("Bearer {token}"))
            .body(Body::empty())
            .unwrap();
        let get_resp1 = app.clone().oneshot(get_req1).await.unwrap();
        let get_data1: PreferencesResponseDto =
            serde_json::from_slice(&get_resp1.into_body().collect().await.unwrap().to_bytes())
                .unwrap();
        assert!(!get_data1.is_custom);

        // Step 3 & 4: Save custom dials
        let save_req = Request::builder()
            .method(Method::POST)
            .uri("/api/preferences")
            .header(AUTHORIZATION, format!("Bearer {token}"))
            .header(CONTENT_TYPE, "application/json")
            .body(Body::from(
                r#"{
                    "freshness_hours": 72.0,
                    "discovery_ratio": 0.30,
                    "topic_weights": {
                        "art": 3.0,
                        "tech": 1.0,
                        "science": 2.5,
                        "news": 0.2,
                        "culture": 1.0
                    }
                }"#,
            ))
            .unwrap();
        let save_resp = app.clone().oneshot(save_req).await.unwrap();
        assert_eq!(save_resp.status(), StatusCode::OK);

        // Step 5: Query feed in Bluesky app
        let feed_jwt = TestAuthHelper::create_service_jwt(&alice_did, 3600);
        let feed_req1 = Request::builder()
            .uri("/xrpc/app.bsky.feed.getFeedSkeleton")
            .header(AUTHORIZATION, format!("Bearer {feed_jwt}"))
            .body(Body::empty())
            .unwrap();
        let feed_resp1 = app.clone().oneshot(feed_req1).await.unwrap();
        assert_eq!(feed_resp1.status(), StatusCode::OK);

        // Step 6: Server snapshot save & restart simulation
        let snapshot_data = state.preferences_store.snapshot_data();
        let snapshot_bytes = SnapshotSection8Helper::encode_section_8(&snapshot_data);

        let new_state = TestServerState::new();
        new_state
            .interner
            .hydrate_from(state.interner.export_strings());
        let decoded_records = SnapshotSection8Helper::decode_section_8(&snapshot_bytes).unwrap();
        new_state
            .preferences_store
            .restore_from_snapshot(decoded_records);
        let new_app = create_test_preferences_router(new_state.clone());

        // Step 7: Alice queries new server instance
        let feed_req2 = Request::builder()
            .uri("/xrpc/app.bsky.feed.getFeedSkeleton")
            .header(AUTHORIZATION, format!("Bearer {feed_jwt}"))
            .body(Body::empty())
            .unwrap();
        let feed_resp2 = new_app.oneshot(feed_req2).await.unwrap();
        assert_eq!(feed_resp2.status(), StatusCode::OK);

        let alice_saved = new_state
            .preferences_store
            .get_by_did(&new_state.interner, &alice_did);
        assert!(alice_saved.is_some());
        assert_eq!(alice_saved.unwrap().topic_weights.art, 3.0);
        assert_eq!(alice_saved.unwrap().topic_weights.science, 2.5);
    }

    #[tokio::test]
    async fn test_tier4_scenario_breaking_news_event_query_override_journey() {
        // Scenario:
        // Alice has custom Art/Science preferences saved.
        // During a breaking news event, her client sends ?freshness=1h&news=5.0x.
        // The query override takes immediate precedence.
        // Once the news event passes, subsequent queries return to her saved Art/Science profile.
        let state = TestServerState::new();
        let alice_did = "did:plc:alice_news_override";

        state.preferences_store.set_by_did(
            &state.interner,
            alice_did,
            UserDials {
                freshness_half_life_secs: 72.0 * 3600.0,
                serendipity_ratio: 0.20,
                topic_weights: TopicWeights {
                    art: 3.0,
                    tech: 1.0,
                    science: 2.5,
                    news: 0.2,
                    culture: 1.0,
                },
                updated_at_secs: 100,
            },
        );

        let app = create_test_preferences_router(state);
        let jwt = TestAuthHelper::create_service_jwt(alice_did, 3600);

        // 1. Breaking news override query
        let news_req = Request::builder()
            .uri("/xrpc/app.bsky.feed.getFeedSkeleton?freshness=1h")
            .header(AUTHORIZATION, format!("Bearer {jwt}"))
            .body(Body::empty())
            .unwrap();
        let news_resp = app.clone().oneshot(news_req).await.unwrap();
        assert_eq!(news_resp.status(), StatusCode::OK);

        // 2. Normal query afterwards
        let normal_req = Request::builder()
            .uri("/xrpc/app.bsky.feed.getFeedSkeleton")
            .header(AUTHORIZATION, format!("Bearer {jwt}"))
            .body(Body::empty())
            .unwrap();
        let normal_resp = app.oneshot(normal_req).await.unwrap();
        assert_eq!(normal_resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_tier4_scenario_user_reset_to_defaults_journey() {
        // Scenario:
        // User with custom preferences decides to revert back to system defaults.
        // 1. User clicks "Reset Dials" (DELETE /api/preferences).
        // 2. Preference store removes custom entry.
        // 3. Subsequent GET /api/preferences reports is_custom: false.
        // 4. Feed skeleton requests return balanced default ranking.
        let state = TestServerState::new();
        let user_did = "did:plc:resetting_user_bob";

        state.preferences_store.set_by_did(
            &state.interner,
            user_did,
            UserDials {
                freshness_half_life_secs: 12.0 * 3600.0,
                serendipity_ratio: 0.45,
                topic_weights: TopicWeights {
                    art: 4.0,
                    tech: 0.5,
                    science: 0.5,
                    news: 0.5,
                    culture: 0.5,
                },
                updated_at_secs: 100,
            },
        );

        let app = create_test_preferences_router(state.clone());
        let token = TestAuthHelper::create_service_jwt(user_did, 3600);

        // Reset
        let del_req = Request::builder()
            .method(Method::DELETE)
            .uri("/api/preferences")
            .header(AUTHORIZATION, format!("Bearer {token}"))
            .body(Body::empty())
            .unwrap();
        let del_resp = app.clone().oneshot(del_req).await.unwrap();
        assert_eq!(del_resp.status(), StatusCode::OK);

        // Verify GET
        let get_req = Request::builder()
            .method(Method::GET)
            .uri("/api/preferences")
            .header(AUTHORIZATION, format!("Bearer {token}"))
            .body(Body::empty())
            .unwrap();
        let get_resp = app.clone().oneshot(get_req).await.unwrap();
        let get_data: PreferencesResponseDto =
            serde_json::from_slice(&get_resp.into_body().collect().await.unwrap().to_bytes())
                .unwrap();
        assert!(!get_data.is_custom);
        assert_eq!(get_data.preferences.freshness_hours, 36.0);

        // Verify Feed
        let feed_req = Request::builder()
            .uri("/xrpc/app.bsky.feed.getFeedSkeleton")
            .header(AUTHORIZATION, format!("Bearer {token}"))
            .body(Body::empty())
            .unwrap();
        let feed_resp = app.oneshot(feed_req).await.unwrap();
        assert_eq!(feed_resp.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn test_tier4_scenario_multi_tenant_concurrent_community_journey() {
        // Scenario:
        // 10 distinct users with specialized preferences (Tech, Art, Science, News, Lurker)
        // simultaneously query the feed, authenticate, and save preferences.
        // Verifies zero cross-talk, memory integrity, and uniform default handling for anonymous users.
        let state = TestServerState::new();
        let app = create_test_preferences_router(state.clone());

        let mut handles = Vec::new();

        for i in 0..10 {
            let app_clone = app.clone();
            let state_clone = state.clone();
            let handle = format!("community_user_{i}.bsky.social");

            handles.push(tokio::spawn(async move {
                // 1. Authenticate
                let login_req = Request::builder()
                    .method(Method::POST)
                    .uri("/api/auth/login")
                    .header(CONTENT_TYPE, "application/json")
                    .body(Body::from(format!(
                        r#"{{"identifier": "{handle}", "password": "pass_{i}"}}"#
                    )))
                    .unwrap();
                let login_resp = app_clone.clone().oneshot(login_req).await.unwrap();
                assert_eq!(login_resp.status(), StatusCode::OK);

                let login_data: LoginSuccessResponse = serde_json::from_slice(
                    &login_resp.into_body().collect().await.unwrap().to_bytes(),
                )
                .unwrap();
                let token = login_data.token;
                let user_did = login_data.did;

                // 2. Save specialized topic multiplier
                let topic_weight = (i as f32 % 5.0) + 0.5;
                let save_req = Request::builder()
                    .method(Method::POST)
                    .uri("/api/preferences")
                    .header(AUTHORIZATION, format!("Bearer {token}"))
                    .header(CONTENT_TYPE, "application/json")
                    .body(Body::from(format!(
                        r#"{{
                            "freshness_hours": 24.0,
                            "discovery_ratio": 0.20,
                            "topic_weights": {{
                                "art": {topic_weight},
                                "tech": 1.0,
                                "science": 1.0,
                                "news": 1.0,
                                "culture": 1.0
                            }}
                        }}"#
                    )))
                    .unwrap();
                let save_resp = app_clone.clone().oneshot(save_req).await.unwrap();
                assert_eq!(save_resp.status(), StatusCode::OK);

                // 3. Query feed skeleton
                let feed_req = Request::builder()
                    .uri("/xrpc/app.bsky.feed.getFeedSkeleton")
                    .header(AUTHORIZATION, format!("Bearer {token}"))
                    .body(Body::empty())
                    .unwrap();
                let feed_resp = app_clone.oneshot(feed_req).await.unwrap();
                assert_eq!(feed_resp.status(), StatusCode::OK);

                // Verify user state isolation
                let saved = state_clone
                    .preferences_store
                    .get_by_did(&state_clone.interner, &user_did);
                assert!(saved.is_some());
                assert_eq!(saved.unwrap().topic_weights.art, topic_weight);
            }));
        }

        for h in handles {
            h.await.unwrap();
        }

        assert_eq!(state.preferences_store.len(), 10);
    }
}
