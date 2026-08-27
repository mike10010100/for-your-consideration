#![forbid(unsafe_code)]
#![deny(
    clippy::all,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::todo,
    clippy::unimplemented,
    missing_docs,
    rust_2018_idioms
)]

//! # `for-your-consideration`
//!
//! A high-performance, single-box custom feed generator for AT Protocol and Bluesky.
//!
//! ## Key Modules
//! - [`types`]: Core domain types, compact 8-byte edge representation, and `ATProto` skeleton models.
//! - [`interner`]: 32-bit bidirectional string interner with double-checked locking.
//! - [`graph`]: In-memory multi-signal graph store with Roaring Bitmaps, time decay, and BM25 popularity dampening.
//! - [`recommender`]: Multi-signal, time-decayed 3-step random walk with candidate scoring, anti-fatigue, and serendipity.
//! - [`ingest`]: Real-time Jetstream WebSocket firehose ingestion pipeline.
//! - [`auth`]: AT Protocol service auth JWT parser and viewer DID extraction.
//! - [`server`]: Axum HTTP XRPC web server (`getFeedSkeleton`, DID doc, healthz).
//! - [`snapshot`]: Atomic binary disk persistence with CRC32 integrity verification.
//! - [`error`]: Domain error definitions.
//!
//! ## Example
//!
//! ```rust
//! use for_your_consideration::prelude::*;
//!
//! let interner = StringInterner::new();
//! let user_id = interner.intern("did:plc:alice");
//! let post_id = interner.intern("at://did:plc:bob/app.bsky.feed.post/3k... ");
//!
//! let graph = GraphStore::new();
//! graph.record_interaction(user_id, post_id, SignalType::Like, 1_700_000_000);
//!
//! assert_eq!(graph.get_user_interactions(user_id).len(), 1);
//! ```

/// AT Protocol service authentication and viewer DID extraction.
pub mod auth;
/// Domain error types and result aliases.
pub mod error;
/// In-memory multi-signal interaction graph store and Roaring Bitmaps.
pub mod graph;
/// Real-time Jetstream WebSocket firehose ingestion pipeline.
pub mod ingest;
/// Bidirectional 32-bit string interner.
pub mod interner;
/// Sharded in-memory user preference dials store.
pub mod preferences;
/// Multi-signal 3-hop recommender engine.
pub mod recommender;
/// Axum HTTP XRPC and REST web server.
pub mod server;
/// Atomic binary snapshot persistence with CRC32 verification.
pub mod snapshot;
/// Core domain types, compact edges, and DTOs.
pub mod types;

/// Convenient prelude re-exporting core data structures, algorithms, and graph primitives.
pub mod prelude {
    pub use crate::auth::{
        authenticate_pds_session, build_secure_http_client, compute_access_token_hash,
        exchange_oauth_code, extract_session_did_from_headers, extract_viewer_did,
        extract_viewer_did_from_headers, generate_pkce_pair, generate_session_token,
        is_restricted_ip, is_valid_did, parse_jwt_payload_unverified, percent_encode_query_param,
        publish_feed_generator_record, resolve_identity_pds, validate_outbound_url,
        validate_outbound_url_async, validate_service_jwt, validate_session_token,
        verify_pkce_challenge, OAuthSessionState, OAuthStateStore, OAuthUserSessionStore,
        PkceChallengePair, ResolvedPdsIdentity, ServiceJwtPayload, UserOAuthSession,
        DEFAULT_OAUTH_STATE_TTL_SECS, OAUTH_STATE_SHARDS,
    };
    pub use crate::error::{FeedError, Result};
    pub use crate::graph::{
        calculate_bayesian_confidence, calculate_bayesian_shrinkage, calculate_consensus_boost,
        calculate_popularity_dampener, calculate_social_proof_factor, calculate_time_decay,
        GraphSnapshotData, GraphStats, GraphStore, DEFAULT_BAYESIAN_BETA,
        DEFAULT_CONSENSUS_BOOST_MU, DEFAULT_HALF_LIFE_SECS, DEFAULT_SOCIAL_PROOF_ALPHA,
        DEFAULT_SOCIAL_PROOF_LAMBDA, MIN_SHARED_OVERLAP, NUM_SHARDS, SIX_HOURS_SECS,
        SOCIAL_PROOF_PLATEAU_THRESHOLD,
    };
    pub use crate::ingest::{
        apply_event_to_graph, build_jetstream_url, build_subscription_url,
        is_event_matching_collections, normalize_jetstream_endpoint, parse_jetstream_frame,
        parse_jetstream_json, run_reader_reconnect_loop, run_slice_reader_reconnect_loop,
        BackoffManager, BackoffPolicy, CursorTracker, IngestEvent, IngesterConfig, IngestionStats,
        IngestionStatsSnapshot, IngestionTracker, JetstreamClient, JetstreamConfig, JetstreamEvent,
        JetstreamIngester, DEFAULT_CHANNEL_CAPACITY, DEFAULT_INACTIVITY_TIMEOUT_SECS,
        DEFAULT_INITIAL_BACKOFF_MS, DEFAULT_JETSTREAM_ENDPOINTS, DEFAULT_JETSTREAM_URL,
        DEFAULT_MAX_BACKOFF_SECS, DEFAULT_PING_INTERVAL_SECS, MAX_STREAM_SLICES,
    };
    pub use crate::interner::StringInterner;
    pub use crate::preferences::{shard_idx, UserPreferencesStore, PREFERENCE_SHARDS};
    pub use crate::recommender::{
        classify_post, deterministic_topic_fallback, match_creator_seed, match_uri_keywords,
        ImpressionEntry, ImpressionStore, Recommender, ViewerImpressionHistory,
        DEFAULT_MAX_IMPRESSIONS_PER_USER, FATIGUE_MIN_FLOOR, FATIGUE_TAU_SECS, FATIGUE_WINDOW_SECS,
        HARD_SUPPRESSION_WINDOW_SECS, IMPRESSION_SHARDS,
    };
    pub use crate::server::{
        create_xrpc_router, handle_delete_preferences, handle_get_client_metadata,
        handle_get_explain, handle_get_feed_preview, handle_get_feed_skeleton, handle_get_healthz,
        handle_get_oauth_login, handle_get_preferences, handle_get_taste_twins,
        handle_get_telemetry, handle_post_auth_login, handle_post_feed_publish,
        handle_post_oauth_callback, handle_post_preferences, serve_xrpc, AppState,
        FeedSkeletonQuery, DEFAULT_FEED_RKEY,
    };
    pub use crate::snapshot::{
        load_snapshot, load_snapshot_with_preferences, save_snapshot,
        save_snapshot_with_preferences, LoadedSnapshot, SnapshotConfig, SnapshotHeader,
        SnapshotStatusTracker, HEADER_SIZE, SNAPSHOT_FORMAT_VERSION, SNAPSHOT_FORMAT_VERSION_V1,
        SNAPSHOT_FORMAT_VERSION_V2, SNAPSHOT_FORMAT_VERSION_V3, SNAPSHOT_MAGIC,
    };
    pub use crate::types::{
        ApiErrorResponse, CompactEdge, DeletePreferencesResponse, ExplainQuery, FeedPreviewItem,
        FeedPreviewQuery, FeedPreviewResponse, FeedPublishRequest, FeedPublishResponse,
        FeedRecommendation, FeedSkeletonResponse, GenericStatusResponse, GetPreferencesResponse,
        GraphProofChain, GraphTelemetryInfo, ImpressionTelemetryInfo, IngestionVelocityInfo,
        InternerTelemetryInfo, LoginRequest, LoginRequestBody, LoginResponse, LoginSuccessResponse,
        OAuthCallbackRequest, OAuthCallbackResponse, OAuthClientMetadata, OAuthLoginQuery,
        OAuthLoginResponse, PostMeta, PreferencesPayloadDto, PreferencesResponseDto,
        ProofChainStep, RecommendationDials, RecommendationSource, SavePreferencesRequestBody,
        ScoreBreakdown, ScoredPost, SetPreferencesRequest, SetPreferencesResponse, SharedPostInfo,
        SignalType, SkeletonFeedPost, SkeletonReason, SnapshotStatusInfo, TasteTwinItem,
        TasteTwinsQuery, TasteTwinsResponse, TelemetryResponse, TopicCategory, TopicWeights,
        UserDials, UserDialsResponse, BLUESKY_EPOCH_SECS, CURATED_MIN_LIKES, DEFAULT_EXPLORE_RATIO,
        DEFAULT_MIN_LIKES, DEFAULT_PAGE_LIMIT, DISCOVERY_MAX, DISCOVERY_MIN, EMERGING_MIN_LIKES,
        FRESHNESS_MAX_HOURS, FRESHNESS_MIN_HOURS, MAX_ENGAGEMENT_FLOOR, MAX_FRESHNESS_SECS,
        MAX_PAGE_LIMIT, MAX_SERENDIPITY_RATIO, MAX_TOPIC_MULTIPLIER, MIN_ENGAGEMENT_FLOOR,
        MIN_FRESHNESS_SECS, MIN_SERENDIPITY_RATIO, MIN_TOPIC_MULTIPLIER, NUM_TOPIC_CATEGORIES,
        TOPIC_CATEGORIES, TOPIC_MAX, TOPIC_MIN,
    };
}

pub use prelude::*;
