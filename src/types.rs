use compact_str::CompactString;
use serde::{Deserialize, Serialize};

/// Custom epoch for relative timestamp packing: 2023-01-01T00:00:00Z.
/// 29 bits from this epoch can represent ~17 years (until ~2040).
pub const BLUESKY_EPOCH_SECS: u64 = 1_672_531_200;

/// Bitmask and shift constants for packing [`CompactEdge`].
const SIGNAL_MASK: u32 = 0b111;
const TIMESTAMP_SHIFT: u32 = 3;
const MAX_RELATIVE_SECS: u32 = (1 << 29) - 1;

/// Interaction signal types supported by the AT Protocol feed generator.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[repr(u8)]
pub enum SignalType {
    /// A like interaction on a post (weight = 1.0x).
    Like = 0b001,
    /// A quote post interaction (weight = 2.0x).
    Quote = 0b010,
    /// A repost interaction (weight = 3.0x).
    Repost = 0b011,
}

impl SignalType {
    /// Returns the algorithmic weight associated with this signal type.
    #[must_use]
    pub const fn weight(&self) -> f32 {
        match self {
            Self::Like => 1.0,
            Self::Quote => 2.0,
            Self::Repost => 3.0,
        }
    }

    /// Converts a raw 3-bit representation to [`SignalType`].
    #[must_use]
    pub const fn from_u8(val: u8) -> Option<Self> {
        match val & 0b111 {
            0b001 => Some(Self::Like),
            0b010 => Some(Self::Quote),
            0b011 => Some(Self::Repost),
            _ => None,
        }
    }

    /// Returns the static string representation of this signal.
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Like => "like",
            Self::Quote => "quote",
            Self::Repost => "repost",
        }
    }

    /// Returns the past-tense verb representation of this signal for explanations.
    #[must_use]
    pub const fn past_tense_verb(&self) -> &'static str {
        match self {
            Self::Like => "liked",
            Self::Quote => "quoted",
            Self::Repost => "reposted",
        }
    }
}

impl std::fmt::Display for SignalType {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// An 8-byte packed edge structure representing an interaction in the graph.
///
/// Layout:
/// - `target`: 32-bit destination node ID (post ID or user ID).
/// - `packed`: 32-bit value where the top 29 bits encode relative seconds
///   since [`BLUESKY_EPOCH_SECS`], and the bottom 3 bits encode [`SignalType`].
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct CompactEdge {
    /// The target node identifier (e.g. `post_id` for user->post or `user_id` for post->user).
    pub target: u32,
    /// Packed payload containing 29-bit timestamp and 3-bit signal.
    pub packed: u32,
}

impl CompactEdge {
    /// Creates a new [`CompactEdge`] with the specified target, signal, and absolute unix timestamp in seconds.
    #[must_use]
    pub fn new(target: u32, signal: SignalType, timestamp_secs: u64) -> Self {
        let rel_secs = timestamp_secs.saturating_sub(BLUESKY_EPOCH_SECS);
        let clamped_rel = if rel_secs > u64::from(MAX_RELATIVE_SECS) {
            MAX_RELATIVE_SECS
        } else {
            clamped_to_u32(rel_secs)
        };

        let sig_code = (signal as u32) & SIGNAL_MASK;
        let packed = (clamped_rel << TIMESTAMP_SHIFT) | sig_code;
        Self { target, packed }
    }

    /// Returns the target node ID.
    #[must_use]
    pub const fn target(&self) -> u32 {
        self.target
    }

    /// Returns the signal type of this edge.
    #[must_use]
    pub const fn signal(&self) -> SignalType {
        match SignalType::from_u8((self.packed & SIGNAL_MASK) as u8) {
            Some(sig) => sig,
            None => SignalType::Like,
        }
    }

    /// Returns the relative seconds since [`BLUESKY_EPOCH_SECS`].
    #[must_use]
    pub const fn relative_timestamp_secs(&self) -> u32 {
        self.packed >> TIMESTAMP_SHIFT
    }

    /// Returns the absolute unix timestamp in seconds.
    #[must_use]
    pub const fn timestamp_secs(&self) -> u64 {
        BLUESKY_EPOCH_SECS + (self.packed >> TIMESTAMP_SHIFT) as u64
    }

    /// Returns the algorithmic weight of this edge's signal type.
    #[must_use]
    pub const fn weight(&self) -> f32 {
        self.signal().weight()
    }
}

const fn clamped_to_u32(val: u64) -> u32 {
    val as u32
}

/// Metadata associated with an indexed post in the graph.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct PostMeta {
    /// The interned user ID of the post's author.
    pub author_id: u32,
    /// The interned post ID of the root post if this post is a reply/thread child.
    pub root_id: Option<u32>,
    /// The interned post ID of the direct parent post if this post is a reply.
    pub parent_id: Option<u32>,
    /// Unix timestamp (in seconds) when the post was created.
    pub created_at: u64,
}

impl PostMeta {
    /// Creates a new [`PostMeta`].
    #[must_use]
    pub const fn new(
        author_id: u32,
        root_id: Option<u32>,
        parent_id: Option<u32>,
        created_at: u64,
    ) -> Self {
        Self {
            author_id,
            root_id,
            parent_id,
            created_at,
        }
    }

    /// Returns `true` if this post is a top-level root post (not a reply).
    #[must_use]
    pub const fn is_root(&self) -> bool {
        self.root_id.is_none() && self.parent_id.is_none()
    }

    /// Returns `true` if this post is a reply.
    #[must_use]
    pub const fn is_reply(&self) -> bool {
        self.parent_id.is_some()
    }
}

/// Topic biasing weights configurable via feed query parameters or UI dials.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct TopicWeights {
    /// Weight multiplier for Art category (0.0 to 5.0, default 1.0).
    pub art: f32,
    /// Weight multiplier for Tech category (0.0 to 5.0, default 1.0).
    pub tech: f32,
    /// Weight multiplier for Science category (0.0 to 5.0, default 1.0).
    pub science: f32,
    /// Weight multiplier for News category (0.0 to 5.0, default 1.0).
    pub news: f32,
    /// Weight multiplier for Culture category (0.0 to 5.0, default 1.0).
    pub culture: f32,
}

impl Default for TopicWeights {
    fn default() -> Self {
        Self {
            art: 1.0,
            tech: 1.0,
            science: 1.0,
            news: 1.0,
            culture: 1.0,
        }
    }
}

impl TopicWeights {
    /// Returns the multiplier for a given topic category.
    #[must_use]
    pub const fn get_weight(&self, category: TopicCategory) -> f32 {
        match category {
            TopicCategory::Art => self.art,
            TopicCategory::Tech => self.tech,
            TopicCategory::Science => self.science,
            TopicCategory::News => self.news,
            TopicCategory::Culture => self.culture,
            TopicCategory::General => 1.0,
        }
    }
}

/// User-controlled recommendation dials passed via feed query parameters.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct RecommendationDials {
    /// Half-life decay parameter $\tau$ in seconds (e.g. 6h = 21,600s, 36h = 129,600s, 168h = 604,800s).
    pub half_life_secs: f32,
    /// Epsilon-greedy exploration ratio $\epsilon \in [0.0, 1.0]$.
    pub explore_ratio: f32,
    /// Topic biasing weights for 5 primary categories.
    pub topic_weights: TopicWeights,
    /// Whether to generate structured explanation traces for each post.
    pub explain: bool,
    /// Maximum number of posts to return per page.
    pub limit: usize,
    /// Opaque pagination cursor.
    pub cursor: Option<String>,
}

/// Default half-life is 36 hours (129,600 seconds).
pub const DEFAULT_HALF_LIFE_SECS: f32 = 36.0 * 3600.0;
/// Default exploration ratio is 15% (0.15).
pub const DEFAULT_EXPLORE_RATIO: f32 = 0.15;
/// Default page limit is 30 items.
pub const DEFAULT_PAGE_LIMIT: usize = 30;
/// Maximum allowable limit.
pub const MAX_PAGE_LIMIT: usize = 100;

impl Default for RecommendationDials {
    fn default() -> Self {
        Self {
            half_life_secs: DEFAULT_HALF_LIFE_SECS,
            explore_ratio: DEFAULT_EXPLORE_RATIO,
            topic_weights: TopicWeights::default(),
            explain: false,
            limit: DEFAULT_PAGE_LIMIT,
            cursor: None,
        }
    }
}

impl RecommendationDials {
    /// Parses query parameters into [`RecommendationDials`] with safe fallback defaults.
    #[must_use]
    pub fn from_query(
        freshness: Option<&str>,
        discovery: Option<&str>,
        explain: Option<bool>,
        limit: Option<usize>,
        cursor: Option<String>,
    ) -> Self {
        let half_life_secs = match freshness {
            Some("realtime" | "fast" | "6h") => 6.0 * 3600.0,
            Some("balanced" | "36h") => 36.0 * 3600.0,
            Some("weekly" | "slow" | "168h") => 168.0 * 3600.0,
            Some(custom) => custom
                .parse::<f32>()
                .unwrap_or(DEFAULT_HALF_LIFE_SECS)
                .max(3600.0),
            None => DEFAULT_HALF_LIFE_SECS,
        };

        let explore_ratio = match discovery {
            Some("familiar" | "low" | "5%") => 0.05,
            Some("balanced" | "med" | "15%") => 0.15,
            Some("deep_dive" | "deepdive" | "high" | "35%") => 0.35,
            Some(custom) => custom
                .parse::<f32>()
                .unwrap_or(DEFAULT_EXPLORE_RATIO)
                .clamp(0.0, 1.0),
            None => DEFAULT_EXPLORE_RATIO,
        };

        let limit = limit.unwrap_or(DEFAULT_PAGE_LIMIT).clamp(1, MAX_PAGE_LIMIT);
        let explain = explain.unwrap_or(false);

        Self {
            half_life_secs,
            explore_ratio,
            topic_weights: TopicWeights::default(),
            explain,
            limit,
            cursor,
        }
    }

    /// Sets custom topic weights on these dials.
    #[must_use]
    pub const fn with_topic_weights(mut self, topic_weights: TopicWeights) -> Self {
        self.topic_weights = topic_weights;
        self
    }
}

/// Minimum allowable freshness half-life in hours (1.0 hour).
pub const FRESHNESS_MIN_HOURS: f32 = 1.0;
/// Maximum allowable freshness half-life in hours (168.0 hours / 7 days).
pub const FRESHNESS_MAX_HOURS: f32 = 168.0;
/// Minimum allowable serendipity discovery ratio (0.0 / 0%).
pub const DISCOVERY_MIN: f32 = 0.0;
/// Maximum allowable serendipity discovery ratio (0.50 / 50%).
pub const DISCOVERY_MAX: f32 = 0.50;
/// Minimum allowable topic category multiplier (0.0x).
pub const TOPIC_MIN: f32 = 0.0;
/// Maximum allowable topic category multiplier (5.0x).
pub const TOPIC_MAX: f32 = 5.0;

/// Minimum allowable freshness half-life in seconds (3,600s).
pub const MIN_FRESHNESS_SECS: f32 = FRESHNESS_MIN_HOURS * 3600.0;
/// Maximum allowable freshness half-life in seconds (604,800s).
pub const MAX_FRESHNESS_SECS: f32 = FRESHNESS_MAX_HOURS * 3600.0;
/// Minimum serendipity exploration ratio.
pub const MIN_SERENDIPITY_RATIO: f32 = DISCOVERY_MIN;
/// Maximum serendipity exploration ratio.
pub const MAX_SERENDIPITY_RATIO: f32 = DISCOVERY_MAX;
/// Minimum topic category multiplier.
pub const MIN_TOPIC_MULTIPLIER: f32 = TOPIC_MIN;
/// Maximum topic category multiplier.
pub const MAX_TOPIC_MULTIPLIER: f32 = TOPIC_MAX;

/// User-configurable recommendation dials persisted per viewer account.
#[derive(Debug, Clone, Copy, PartialEq, Serialize, Deserialize)]
pub struct UserDials {
    /// Half-life time decay parameter in seconds (range: 1h [3,600s] to 168h [604,800s], default: 36h [129,600s]).
    pub freshness_half_life_secs: f32,
    /// Serendipity exploration ratio (range: 0.0 [0%] to 0.50 [50%], default: 0.15 [15%]).
    pub serendipity_ratio: f32,
    /// Topic bias multipliers for the 5 primary categories (range: 0.0x to 5.0x each, default: 1.0x).
    pub topic_weights: TopicWeights,
    /// Unix timestamp in seconds when preferences were last saved or updated.
    pub updated_at_secs: u64,
}

impl Default for UserDials {
    fn default() -> Self {
        Self {
            freshness_half_life_secs: DEFAULT_HALF_LIFE_SECS,
            serendipity_ratio: DEFAULT_EXPLORE_RATIO,
            topic_weights: TopicWeights::default(),
            updated_at_secs: 0,
        }
    }
}

impl UserDials {
    /// Validates all dial boundaries against strict specification limits.
    pub fn validate(&self) -> Result<(), String> {
        if !self.freshness_half_life_secs.is_finite()
            || self.freshness_half_life_secs < MIN_FRESHNESS_SECS
            || self.freshness_half_life_secs > MAX_FRESHNESS_SECS
        {
            return Err(format!(
                "Freshness half-life must be between {:.1}h ({}s) and {:.1}h ({}s), got {:.1}s",
                FRESHNESS_MIN_HOURS,
                MIN_FRESHNESS_SECS as u32,
                FRESHNESS_MAX_HOURS,
                MAX_FRESHNESS_SECS as u32,
                self.freshness_half_life_secs
            ));
        }

        if !self.serendipity_ratio.is_finite()
            || self.serendipity_ratio < MIN_SERENDIPITY_RATIO
            || self.serendipity_ratio > MAX_SERENDIPITY_RATIO
        {
            return Err(format!(
                "Discovery ratio must be between {:.2} ({}%) and {:.2} ({}%), got {:.3}",
                DISCOVERY_MIN,
                (DISCOVERY_MIN * 100.0) as u32,
                DISCOVERY_MAX,
                (DISCOVERY_MAX * 100.0) as u32,
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
                    "Topic multiplier for {name} must be between {:.1}x and {:.1}x, got {:.2}x",
                    MIN_TOPIC_MULTIPLIER, MAX_TOPIC_MULTIPLIER, weight
                ));
            }
        }

        Ok(())
    }

    /// Returns freshness half-life in hours.
    #[must_use]
    pub const fn freshness_half_life_hours(&self) -> f32 {
        self.freshness_half_life_secs / 3600.0
    }

    /// Returns discovery / serendipity exploration ratio.
    #[must_use]
    pub const fn discovery_ratio(&self) -> f32 {
        self.serendipity_ratio
    }

    /// Constructs [`UserDials`] from hours, discovery ratio, topic weights, and updated timestamp.
    #[must_use]
    pub const fn from_hours(
        freshness_half_life_hours: f32,
        discovery_ratio: f32,
        topic_weights: TopicWeights,
        updated_at_secs: u64,
    ) -> Self {
        Self {
            freshness_half_life_secs: freshness_half_life_hours * 3600.0,
            serendipity_ratio: discovery_ratio,
            topic_weights,
            updated_at_secs,
        }
    }

    /// Converts [`UserDials`] into [`RecommendationDials`] with default page limits and cursor.
    #[must_use]
    pub const fn to_recommendation_dials(&self) -> RecommendationDials {
        RecommendationDials {
            half_life_secs: self.freshness_half_life_secs,
            explore_ratio: self.serendipity_ratio,
            topic_weights: self.topic_weights,
            explain: false,
            limit: DEFAULT_PAGE_LIMIT,
            cursor: None,
        }
    }

    /// Constructs [`UserDials`] from [`RecommendationDials`].
    #[must_use]
    pub const fn from_recommendation_dials(
        dials: &RecommendationDials,
        updated_at_secs: u64,
    ) -> Self {
        Self {
            freshness_half_life_secs: dials.half_life_secs,
            serendipity_ratio: dials.explore_ratio,
            topic_weights: dials.topic_weights,
            updated_at_secs,
        }
    }

    /// Applies these custom user dials onto an existing [`RecommendationDials`] instance.
    pub const fn apply_to_recommendation_dials(&self, dials: &mut RecommendationDials) {
        dials.half_life_secs = self.freshness_half_life_secs;
        dials.explore_ratio = self.serendipity_ratio;
        dials.topic_weights = self.topic_weights;
    }
}

/// Request payload for `POST /api/auth/login`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LoginRequestBody {
    /// Bluesky handle or DID identifier (e.g. "alice.bsky.social").
    pub identifier: String,
    /// Bluesky App Password or password.
    pub password: String,
    /// Optional custom PDS URL (defaults to `<https://bsky.social>`).
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub pds_url: Option<String>,
}

/// Alias for [`LoginRequestBody`].
pub type LoginRequest = LoginRequestBody;

/// Successful response payload for `POST /api/auth/login`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct LoginSuccessResponse {
    /// Operation status string (e.g. "ok").
    pub status: String,
    /// Authenticated user DID.
    pub did: String,
    /// Authenticated user handle.
    pub handle: String,
    /// Scoped authentication token.
    pub token: String,
    /// Human-readable success message.
    pub message: String,
}

/// Alias for [`LoginSuccessResponse`].
pub type LoginResponse = LoginSuccessResponse;

/// User preference dials representation for JSON REST API responses.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct UserDialsResponse {
    /// Freshness time-decay half-life in hours.
    pub freshness_half_life_hours: f32,
    /// Serendipitous discovery / exploration ratio (0.0 - 0.5).
    pub discovery_ratio: f32,
    /// Granular topic category multipliers.
    pub topics: TopicWeights,
    /// Timestamp in seconds since unix epoch when dials were last updated.
    pub updated_at_secs: u64,
}

impl From<UserDials> for UserDialsResponse {
    fn from(dials: UserDials) -> Self {
        Self {
            freshness_half_life_hours: dials.freshness_half_life_hours(),
            discovery_ratio: dials.discovery_ratio(),
            topics: dials.topic_weights,
            updated_at_secs: dials.updated_at_secs,
        }
    }
}

/// Preferences representation payload within [`PreferencesResponseDto`].
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PreferencesPayloadDto {
    /// Freshness half-life in hours.
    #[serde(alias = "freshness_half_life_hours")]
    pub freshness_hours: f32,
    /// Exploration discovery ratio.
    pub discovery_ratio: f32,
    /// 5-channel topic weight multipliers.
    #[serde(alias = "topics")]
    pub topic_weights: TopicWeights,
}

impl From<UserDials> for PreferencesPayloadDto {
    fn from(dials: UserDials) -> Self {
        Self {
            freshness_hours: dials.freshness_half_life_hours(),
            discovery_ratio: dials.discovery_ratio(),
            topic_weights: dials.topic_weights,
        }
    }
}

/// Response payload for `GET /api/preferences`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct PreferencesResponseDto {
    /// Viewer DID.
    pub did: String,
    /// Active preference settings.
    pub preferences: PreferencesPayloadDto,
    /// Whether these preferences are custom-saved or system defaults.
    pub is_custom: bool,
    /// Detailed dial breakdown if present.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dials: Option<UserDialsResponse>,
}

/// Alias for [`PreferencesResponseDto`].
pub type GetPreferencesResponse = PreferencesResponseDto;

/// Request payload for `POST /api/preferences`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SavePreferencesRequestBody {
    /// Target freshness half-life in hours (1.0 to 168.0).
    #[serde(alias = "freshness_half_life_hours")]
    pub freshness_hours: f32,
    /// Target discovery exploration ratio (0.0 to 0.5).
    pub discovery_ratio: f32,
    /// Optional topic weight multipliers (0.0 to 5.0).
    #[serde(alias = "topics", default)]
    pub topic_weights: Option<TopicWeights>,
}

/// Alias for [`SavePreferencesRequestBody`].
pub type SetPreferencesRequest = SavePreferencesRequestBody;

/// Generic status response payload for mutations.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct GenericStatusResponse {
    /// Operation status string (e.g. "ok").
    pub status: String,
    /// Optional descriptive message.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message: Option<String>,
    /// Optional affected user DID.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub did: Option<String>,
    /// Optional updated preferences summary.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub preferences: Option<PreferencesPayloadDto>,
    /// Optional detailed dial representation.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub dials: Option<UserDialsResponse>,
}

/// Alias for [`GenericStatusResponse`].
pub type SetPreferencesResponse = GenericStatusResponse;
/// Alias for [`GenericStatusResponse`].
pub type DeletePreferencesResponse = GenericStatusResponse;

/// The origin source of a recommended post.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub enum RecommendationSource {
    /// Active user 3-step co-interaction random walk.
    Tier1InteractionWalk,
    /// Follow graph traversal fallback.
    Tier2FollowWalk,
    /// Curated 6-hour high-velocity sliding pool for cold start / unauthenticated.
    Tier3VelocityPool,
    /// Epsilon-greedy serendipitous exploration candidate.
    ExplorationSerendipity,
}

impl RecommendationSource {
    /// Returns static string representation.
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Tier1InteractionWalk => "tier1_interaction_walk",
            Self::Tier2FollowWalk => "tier2_follow_walk",
            Self::Tier3VelocityPool => "tier3_velocity_pool",
            Self::ExplorationSerendipity => "exploration_serendipity",
        }
    }
}

impl std::fmt::Display for RecommendationSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// Topic category for post classification and cold-start onboarding diversity clustering.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[repr(u8)]
pub enum TopicCategory {
    /// Visual arts, illustration, digital art, photography, design.
    Art = 0,
    /// Software engineering, programming, AI/ML, distributed systems, technology.
    Tech = 1,
    /// Natural sciences, astronomy, physics, biology, space exploration, mathematics.
    Science = 2,
    /// Journalism, global news, breaking events, current affairs, economics.
    News = 3,
    /// Books, literature, music, cinema, history, philosophy, human culture.
    Culture = 4,
    /// General or unclassified topic category.
    General = 5,
}

/// The 5 core topic diversity categories used for balanced cold-start candidate interleaving.
pub const TOPIC_CATEGORIES: [TopicCategory; 5] = [
    TopicCategory::Art,
    TopicCategory::Tech,
    TopicCategory::Science,
    TopicCategory::News,
    TopicCategory::Culture,
];

/// Total number of distinct primary topic categories in [`TOPIC_CATEGORIES`].
pub const NUM_TOPIC_CATEGORIES: usize = 5;

impl TopicCategory {
    /// Returns the static string identifier of this topic category.
    #[must_use]
    pub const fn as_str(&self) -> &'static str {
        match self {
            Self::Art => "art",
            Self::Tech => "tech",
            Self::Science => "science",
            Self::News => "news",
            Self::Culture => "culture",
            Self::General => "general",
        }
    }

    /// Converts a raw discriminant byte to [`TopicCategory`].
    #[must_use]
    pub const fn from_u8(val: u8) -> Option<Self> {
        match val {
            0 => Some(Self::Art),
            1 => Some(Self::Tech),
            2 => Some(Self::Science),
            3 => Some(Self::News),
            4 => Some(Self::Culture),
            5 => Some(Self::General),
            _ => None,
        }
    }

    /// Parses a case-insensitive string slice into a [`TopicCategory`].
    #[must_use]
    pub fn from_str_name(s: &str) -> Option<Self> {
        match s.trim().to_ascii_lowercase().as_str() {
            "art" | "arts" | "illustration" | "photography" | "design" => Some(Self::Art),
            "tech" | "technology" | "software" | "programming" | "code" | "ai" => Some(Self::Tech),
            "science" | "physics" | "astronomy" | "biology" | "space" => Some(Self::Science),
            "news" | "press" | "journalism" | "breaking" | "worldnews" => Some(Self::News),
            "culture" | "books" | "music" | "film" | "movies" | "history" | "philosophy" => {
                Some(Self::Culture)
            }
            "general" | "misc" | "other" => Some(Self::General),
            _ => None,
        }
    }

    /// Maps the topic category to a 0-based bucket index in `0..5` for round-robin diversity partitioning.
    #[must_use]
    pub const fn to_index(&self) -> usize {
        match self {
            Self::Tech => 1,
            Self::Science => 2,
            Self::News => 3,
            Self::Culture => 4,
            Self::Art | Self::General => 0,
        }
    }
}

impl std::fmt::Display for TopicCategory {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

/// An individual candidate post evaluated and scored by the recommendation engine.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ScoredPost {
    /// Interned post ID.
    pub post_id: u32,
    /// Canonical AT-URI of the post.
    pub uri: CompactString,
    /// Interned author user ID.
    pub author_id: u32,
    /// Final composite score after time-decay, cosine similarity, and popularity dampening.
    pub score: f32,
    /// Algorithmic tier or mechanism that produced this candidate.
    pub source: RecommendationSource,
    /// Optional human-readable explanation trace.
    pub explain: Option<String>,
}

/// Internal result of recommendation generation containing scored posts and an optional pagination cursor.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FeedRecommendation {
    /// Ordered list of recommended posts.
    pub posts: Vec<ScoredPost>,
    /// Opaque pagination cursor.
    pub cursor: Option<String>,
}

/// AT Protocol XRPC `app.bsky.feed.getFeedSkeleton` response schema.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FeedSkeletonResponse {
    /// The ordered list of post skeletons.
    pub feed: Vec<SkeletonFeedPost>,
    /// Opaque cursor to pass to subsequent page requests.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub cursor: Option<String>,
}

/// An individual post item within the feed skeleton response.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SkeletonFeedPost {
    /// The canonical AT-URI of the post (e.g. `at://did:plc:.../app.bsky.feed.post/...`).
    pub post: CompactString,
    /// Optional reason for post inclusion (e.g. repost).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reason: Option<SkeletonReason>,
    /// Optional context string describing feed generation metadata.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub feed_context: Option<String>,
}

impl SkeletonFeedPost {
    /// Creates a basic skeleton item with no repost reason.
    #[must_use]
    pub fn new(post: impl Into<CompactString>) -> Self {
        Self {
            post: post.into(),
            reason: None,
            feed_context: None,
        }
    }

    /// Creates a skeleton item with a repost reason.
    #[must_use]
    pub fn with_repost(
        post: impl Into<CompactString>,
        repost_uri: impl Into<CompactString>,
    ) -> Self {
        Self {
            post: post.into(),
            reason: Some(SkeletonReason::Repost {
                repost: repost_uri.into(),
            }),
            feed_context: None,
        }
    }
}

/// Reasons explaining why a post appears in the feed skeleton.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "$type")]
pub enum SkeletonReason {
    /// Post was reposted into the feed.
    #[serde(rename = "app.bsky.feed.defs#skeletonReasonRepost")]
    Repost {
        /// AT-URI of the repost record.
        repost: CompactString,
    },
}

/// Granular breakdown of individual mathematical scoring factors for transparency.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct ScoreBreakdown {
    /// Exponential time decay factor.
    pub time_decay: f32,
    /// Taste similarity or base affinity score.
    pub taste_similarity: f32,
    /// Topic bias multiplier.
    pub topic_boost: f32,
    /// Fatigue penalty multiplier (1.0 if unseen, < 1.0 if decayed).
    pub fatigue_penalty: f32,
    /// Final composite score after applying all factors.
    pub final_score: f32,
}

/// An individual transition step in the graph proof chain.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ProofChainStep {
    /// Step transition type (e.g. "`viewer_interaction`", "`taste_similarity`", "`recommendation_signal`").
    pub step_type: CompactString,
    /// Node identifier (DID, AT-URI, or topic name).
    pub node_id: CompactString,
    /// Human-readable description of this transition step.
    pub description: String,
}

/// A verifiable graph proof explaining why a post was recommended.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GraphProofChain {
    /// The sequence of graph transition steps.
    pub steps: Vec<ProofChainStep>,
    /// Concise human-readable summary.
    pub summary: String,
}

/// A rich candidate post item returned by the interactive preview endpoint.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FeedPreviewItem {
    /// Canonical AT-URI of the post.
    pub uri: CompactString,
    /// DID of the author.
    pub author_did: CompactString,
    /// Topic category classification.
    pub topic: TopicCategory,
    /// Source tier description (e.g. "Tier 1: 3-Step Interaction Walk").
    pub tier: String,
    /// Granular score breakdown.
    pub score_breakdown: ScoreBreakdown,
    /// Structured 3-step proof chain if available.
    pub proof_chain: Option<GraphProofChain>,
}

/// Response payload for `GET /api/feed-preview`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct FeedPreviewResponse {
    /// Viewer DID or handle queried.
    pub viewer_did: CompactString,
    /// Scored preview items with score breakdowns and proof chains.
    pub items: Vec<FeedPreviewItem>,
    /// Total number of candidates evaluated in the pipeline.
    pub total_candidates: usize,
    /// Backend recommendation query latency in microseconds.
    pub query_latency_us: u64,
}

/// Information about a post liked by both the viewer and a taste twin.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct SharedPostInfo {
    /// Canonical AT-URI of the post.
    pub uri: CompactString,
    /// Author DID of the post.
    pub author_did: CompactString,
    /// Topic category.
    pub category: TopicCategory,
    /// Unix timestamp when created.
    pub created_at: u64,
}

/// An individual taste twin user with similarity metrics and shared interactions.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TasteTwinItem {
    /// DID of the taste twin user.
    pub user_did: CompactString,
    /// Cosine similarity score in range [0.0, 1.0].
    pub similarity_score: f32,
    /// Count of shared liked posts.
    pub shared_posts_count: usize,
    /// Top inferred interest categories.
    pub top_interests: Vec<TopicCategory>,
    /// Sample of shared liked posts.
    pub shared_posts: Vec<SharedPostInfo>,
}

/// Response payload for `GET /api/taste-twins`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TasteTwinsResponse {
    /// Canonical DID of the queried viewer.
    pub viewer_did: CompactString,
    /// Total count of posts liked by the viewer in the graph.
    pub total_liked_posts: usize,
    /// Ranked list of top taste twins.
    pub twins: Vec<TasteTwinItem>,
    /// Query execution latency in microseconds.
    pub query_latency_us: u64,
}

/// Point-in-time snapshot status information for telemetry and dashboard reporting.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct SnapshotStatusInfo {
    /// Status description: "clean", "hydrated", "persisted", or error details.
    pub status: String,
    /// Unix timestamp in seconds when the snapshot was last saved/loaded.
    pub last_saved_secs: u64,
    /// Number of seconds elapsed since the last save.
    pub last_saved_ago_secs: u64,
    /// Duration of the last load in milliseconds.
    pub last_load_duration_ms: f64,
    /// Duration of the last save in milliseconds.
    pub last_save_duration_ms: f64,
    /// Periodic snapshot checkpoint interval in seconds.
    pub interval_secs: u64,
    /// Path to the snapshot binary file.
    pub file_path: String,
    /// Size of the snapshot file on disk in bytes.
    pub file_size_bytes: u64,
    /// Snapshot binary format version.
    pub format_version: u16,
}

impl Default for SnapshotStatusInfo {
    fn default() -> Self {
        Self {
            status: "clean".to_string(),
            last_saved_secs: 0,
            last_saved_ago_secs: 0,
            last_load_duration_ms: 0.0,
            last_save_duration_ms: 0.0,
            interval_secs: 300,
            file_path: "snapshot.bin".to_string(),
            file_size_bytes: 0,
            format_version: 1,
        }
    }
}

/// Real-time Jetstream ingestion metrics, backfill hydration progress, and instantaneous event velocity.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct IngestionVelocityInfo {
    /// Total raw events received over the WebSocket stream.
    pub events_received: u64,
    /// Total events parsed and applied to the in-memory graph.
    pub events_processed: u64,
    /// Total raw network bytes received.
    pub bytes_received: u64,
    /// Total number of reconnection attempts triggered.
    pub reconnect_count: u64,
    /// Highest monotonic Jetstream cursor (`time_us`) processed.
    pub latest_cursor_us: u64,
    /// Unix timestamp in seconds of the most recent event or heartbeat.
    pub last_activity_timestamp: u64,
    /// Instantaneous ingestion velocity in events per second.
    pub velocity_events_per_sec: f32,
    /// Initial cursor timestamp (`time_us`) when backfill / hydration began, if any.
    pub initial_cursor_us: Option<u64>,
    /// Target realtime timestamp (`time_us`) when backfill was initiated.
    pub target_cursor_us: Option<u64>,
    /// Current stream lag in seconds behind wall-clock time (`now_secs - cursor_secs`).
    pub lag_seconds: u64,
    /// Backfill hydration completion percentage (0.0% to 100.0%).
    pub backfill_progress_percent: f32,
    /// Whether the ingester has caught up to the live stream head (lag <= 60s).
    pub is_live: bool,
    /// Estimated time remaining in seconds until hydration completes.
    pub eta_seconds: Option<u64>,
    /// Speedup factor relative to real-time (e.g. 45.2x).
    pub speedup_factor: f32,
}

impl Default for IngestionVelocityInfo {
    fn default() -> Self {
        Self {
            events_received: 0,
            events_processed: 0,
            bytes_received: 0,
            reconnect_count: 0,
            latest_cursor_us: 0,
            last_activity_timestamp: 0,
            velocity_events_per_sec: 0.0,
            initial_cursor_us: None,
            target_cursor_us: None,
            lag_seconds: 0,
            backfill_progress_percent: 100.0,
            is_live: true,
            eta_seconds: None,
            speedup_factor: 1.0,
        }
    }
}

/// Graph size and topology telemetry.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GraphTelemetryInfo {
    /// Total unique nodes in the graph (users + posts).
    pub total_nodes: usize,
    /// Total unique user accounts.
    pub total_users: usize,
    /// Total unique posts.
    pub total_posts: usize,
    /// Total interaction edges (likes, reposts, quotes).
    pub total_edges: usize,
    /// Total directed follow relationships.
    pub total_follows: usize,
    /// Total post metadata entries.
    pub post_metadata_entries: usize,
    /// Number of active posts in the 6-hour sliding velocity pool.
    pub active_velocity_posts: usize,
}

/// String interner dictionary telemetry.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct InternerTelemetryInfo {
    /// Total interned compact strings (DIDs, AT-URIs, handles).
    pub total_interned_strings: usize,
}

/// Impression store anti-fatigue memory telemetry.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ImpressionTelemetryInfo {
    /// Total active viewers tracked in the 64-shard LRU impression cache.
    pub total_tracked_viewers: usize,
    /// Immediate hard suppression window duration in seconds (default: 1800s = 30m).
    pub hard_suppression_window_secs: u64,
    /// Exponential soft fatigue decay window duration in seconds (default: 21600s = 6h).
    pub fatigue_decay_window_secs: u64,
}

/// Live in-memory RAM usage and footprint telemetry.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct MemoryTelemetryInfo {
    /// Estimated heap memory used by the bipartite graph (bytes).
    pub graph_bytes: usize,
    /// Estimated heap memory used by the string interner (bytes).
    pub interner_bytes: usize,
    /// Estimated heap memory used by the 64-shard impression store (bytes).
    pub impression_bytes: usize,
    /// Total estimated domain heap memory (bytes).
    pub total_estimated_bytes: usize,
    /// Formatted human-readable memory string (e.g. "24.5 MB").
    pub formatted_total: String,
}

/// Formats a byte size into human-readable representation (B, KB, MB, GB).
#[must_use]
pub fn format_memory_bytes(bytes: usize) -> String {
    if bytes < 1024 {
        format!("{bytes} B")
    } else if bytes < 1024 * 1024 {
        format!("{:.1} KB", bytes as f64 / 1024.0)
    } else if bytes < 1024 * 1024 * 1024 {
        format!("{:.1} MB", bytes as f64 / (1024.0 * 1024.0))
    } else {
        format!("{:.2} GB", bytes as f64 / (1024.0 * 1024.0 * 1024.0))
    }
}

/// Comprehensive live telemetry response for `GET /api/telemetry`.
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct TelemetryResponse {
    /// Overall engine status ("ok", "degraded", etc.).
    pub status: String,
    /// Server process uptime in seconds.
    pub uptime_seconds: u64,
    /// Live in-memory graph statistics.
    pub graph: GraphTelemetryInfo,
    /// String interner compact table statistics.
    pub interner: InternerTelemetryInfo,
    /// In-memory heap footprint and RAM telemetry.
    pub memory: MemoryTelemetryInfo,
    /// Real-time Jetstream firehose ingestion velocity and cursor stats.
    pub ingestion: IngestionVelocityInfo,
    /// Binary snapshot persistence and hydration status.
    pub snapshot: SnapshotStatusInfo,
    /// Sliding LRU impression anti-fatigue memory stats.
    pub impression_store: ImpressionTelemetryInfo,
    /// Optional administrator DID authorized to publish the official feed generator.
    #[serde(skip_serializing_if = "Option::is_none", default)]
    pub admin_did: Option<String>,
}

/// Query parameters for `GET /api/taste-twins`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct TasteTwinsQuery {
    /// DID of the viewer to find taste twins for (e.g. `did:plc:alice`).
    pub did: Option<String>,
    /// Handle of the viewer to find taste twins for (e.g. `alice.bsky.social`).
    pub handle: Option<String>,
    /// Maximum number of twins to return (default: 10, max: 50).
    pub limit: Option<usize>,
}

impl TasteTwinsQuery {
    /// Extracts the target identifier (DID or handle), stripping leading `@` and whitespace.
    #[must_use]
    pub fn target_identifier(&self) -> Option<&str> {
        self.did
            .as_deref()
            .or(self.handle.as_deref())
            .map(str::trim)
            .filter(|s| !s.is_empty())
    }

    /// Returns the sanitized limit clamped to `1..=50`.
    #[must_use]
    pub fn limit_or_default(&self) -> usize {
        self.limit.unwrap_or(10).clamp(1, 50)
    }
}

/// Query parameters for `GET /api/feed-preview`.
#[derive(Debug, Clone, Default, PartialEq, Serialize, Deserialize)]
pub struct FeedPreviewQuery {
    /// Viewer DID or handle (optional; cold-start Tier 3 if omitted).
    pub viewer: Option<String>,
    /// Alternative parameter name for viewer DID.
    pub did: Option<String>,
    /// Alternative parameter name for viewer handle.
    pub handle: Option<String>,
    /// Time-decay freshness dial (e.g. "realtime", "balanced", "weekly" or half-life secs).
    pub freshness: Option<String>,
    /// Discovery / serendipity exploration dial (e.g. "familiar", "balanced", "`deep_dive`").
    pub discovery: Option<String>,

    /// Topic bias multiplier for Art domain.
    pub art: Option<f32>,
    /// Topic bias multiplier for Tech domain.
    pub tech: Option<f32>,
    /// Topic bias multiplier for Science domain.
    pub science: Option<f32>,
    /// Topic bias multiplier for News domain.
    pub news: Option<f32>,
    /// Topic bias multiplier for Culture domain.
    pub culture: Option<f32>,
    /// Maximum number of items to return (default: 30, max: 100).
    pub limit: Option<usize>,
    /// Whether to generate structured 3-step graph proof chains.
    pub explain: Option<bool>,
}

impl FeedPreviewQuery {
    /// Extracts the viewer identifier (DID or handle), if provided.
    #[must_use]
    pub fn viewer_identifier(&self) -> Option<&str> {
        self.viewer
            .as_deref()
            .or(self.did.as_deref())
            .or(self.handle.as_deref())
            .map(str::trim)
            .filter(|s| !s.is_empty())
    }

    /// Converts this query into [`RecommendationDials`] with custom topic weights.
    #[must_use]
    pub fn to_dials(&self) -> RecommendationDials {
        let base_dials = RecommendationDials::from_query(
            self.freshness.as_deref(),
            self.discovery.as_deref(),
            self.explain,
            self.limit,
            None,
        );

        let topic_weights = TopicWeights {
            art: self.art.unwrap_or(1.0).max(0.0),
            tech: self.tech.unwrap_or(1.0).max(0.0),
            science: self.science.unwrap_or(1.0).max(0.0),
            news: self.news.unwrap_or(1.0).max(0.0),
            culture: self.culture.unwrap_or(1.0).max(0.0),
        };

        base_dials.with_topic_weights(topic_weights)
    }
}

/// Query parameters for `GET /api/explain`.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ExplainQuery {
    /// Viewer DID or handle (optional).
    pub viewer: Option<String>,
    /// Alternative parameter name for viewer DID.
    pub did: Option<String>,
    /// Canonical AT-URI of the post to explain (required).
    pub uri: Option<String>,
    /// Alternative parameter name for post URI.
    pub post: Option<String>,
}

impl ExplainQuery {
    /// Extracts the viewer identifier, if provided.
    #[must_use]
    pub fn viewer_identifier(&self) -> Option<&str> {
        self.viewer
            .as_deref()
            .or(self.did.as_deref())
            .map(str::trim)
            .filter(|s| !s.is_empty())
    }

    /// Extracts the canonical target post AT-URI.
    #[must_use]
    pub fn post_uri(&self) -> Option<&str> {
        self.uri
            .as_deref()
            .or(self.post.as_deref())
            .map(str::trim)
            .filter(|s| !s.is_empty())
    }
}

/// Standardized JSON error response payload for REST endpoints.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ApiErrorResponse {
    /// Short error code or classification.
    pub error: String,
    /// Human-readable descriptive explanation of the error.
    pub message: String,
}

impl ApiErrorResponse {
    /// Creates a new [`ApiErrorResponse`].
    #[must_use]
    pub fn new(error: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            error: error.into(),
            message: message.into(),
        }
    }
}

/// AT Protocol OAuth Client Metadata document adhering to RFC 7591 / RFC 8414 and `ATProto` OAuth specification.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OAuthClientMetadata {
    /// Canonical URL identifying the client (serves as the client's `client_id`).
    pub client_id: CompactString,
    /// Human-readable client name displayed on user consent screens.
    pub client_name: CompactString,
    /// Client homepage or landing page URL.
    pub client_uri: CompactString,
    /// Whitelist of allowed redirect URIs for OAuth authorization code callbacks.
    pub redirect_uris: Vec<CompactString>,
    /// Supported grant types (e.g. `["authorization_code", "refresh_token"]`).
    pub grant_types: Vec<CompactString>,
    /// Supported response types (e.g. `["code"]`).
    pub response_types: Vec<CompactString>,
    /// Requested OAuth scopes (e.g. `"atproto transition:generic"`).
    pub scope: CompactString,
    /// Authentication method for token endpoint (typically `"none"` for public web clients).
    pub token_endpoint_auth_method: CompactString,
    /// Client application type (e.g. `"web"` or `"native"`).
    pub application_type: CompactString,
    /// Whether access tokens must be bound to `DPoP` keys.
    pub dpop_bound_access_tokens: bool,
}

impl OAuthClientMetadata {
    /// Creates standard client metadata for a given hostname and optional service DID.
    #[must_use]
    pub fn new_for_host(hostname: &str) -> Self {
        let clean_host = hostname.trim_end_matches('/');
        let is_localhost = clean_host.starts_with("localhost")
            || clean_host.starts_with("127.0.0.1")
            || clean_host.starts_with("0.0.0.0");

        let scheme = if is_localhost { "http" } else { "https" };
        let client_id = if is_localhost {
            CompactString::new("http://127.0.0.1:3000/oauth/client-metadata.json")
        } else {
            format!("{scheme}://{clean_host}/oauth/client-metadata.json").into()
        };

        let client_uri = format!("{scheme}://{clean_host}").into();

        let redirect_uris = if is_localhost {
            vec![CompactString::new("http://127.0.0.1:3000/oauth/callback")]
        } else {
            vec![format!("{scheme}://{clean_host}/oauth/callback").into()]
        };

        Self {
            client_id,
            client_name: CompactString::new("For Your Consideration"),
            client_uri,
            redirect_uris,
            grant_types: vec![
                CompactString::new("authorization_code"),
                CompactString::new("refresh_token"),
            ],
            response_types: vec![CompactString::new("code")],
            scope: CompactString::new("atproto transition:generic"),
            token_endpoint_auth_method: CompactString::new("none"),
            application_type: CompactString::new("web"),
            dpop_bound_access_tokens: true,
        }
    }
}

/// Query parameters for `GET /api/oauth/login`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OAuthLoginQuery {
    /// Bluesky handle or DID identifier of the authenticating user (e.g. "alice.bsky.social").
    pub handle: Option<String>,
    /// Optional custom redirect URI for testing or alternative callbacks.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub redirect_uri: Option<String>,
}

/// Response payload for `GET /api/oauth/login`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OAuthLoginResponse {
    /// Operation status string (e.g. "ok").
    pub status: CompactString,
    /// Constructed PDS authorization URL for the user to visit.
    pub authorization_url: String,
    /// Secure single-use state nonce generated for this login session.
    pub state: String,
}

/// Request body for `POST /api/oauth/callback`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OAuthCallbackRequest {
    /// Authorization code returned from the PDS authorization server.
    pub code: String,
    /// Single-use state nonce returned from the PDS authorization server.
    pub state: String,
    /// Optional issuer identifier returned in the callback.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub iss: Option<String>,
}

/// Response payload for `POST /api/oauth/callback`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct OAuthCallbackResponse {
    /// Operation status string (e.g. "ok").
    pub status: CompactString,
    /// Authenticated user DID (e.g. `did:plc:...`).
    pub did: CompactString,
    /// Authenticated user handle (e.g. `alice.bsky.social`).
    pub handle: CompactString,
    /// Scoped session JWT token for API calls.
    pub token: String,
}

/// Request body for `POST /api/feed/publish`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FeedPublishRequest {
    /// Human-readable display name for the custom feed generator (e.g. "For Your Consideration").
    pub display_name: String,
    /// Record key identifier for the feed (e.g. "for-your-consideration" or "fyc").
    pub rkey: String,
    /// Descriptive summary of the custom feed generator algorithm.
    pub description: String,
}

/// Response payload for `POST /api/feed/publish`.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct FeedPublishResponse {
    /// Operation status string (e.g. "ok").
    pub status: CompactString,
    /// Canonical AT-URI of the published `app.bsky.feed.generator` record.
    pub uri: CompactString,
    /// Content identifier (CID) of the committed record.
    pub cid: CompactString,
    /// Web share URL for adding the feed in the Bluesky app.
    pub share_url: CompactString,
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic, missing_docs)]
mod tests {
    use super::*;

    #[test]
    fn test_compact_edge_size_and_packing() {
        assert_eq!(std::mem::size_of::<CompactEdge>(), 8);

        let target = 123_456;
        let ts = BLUESKY_EPOCH_SECS + 500_000;
        let edge = CompactEdge::new(target, SignalType::Quote, ts);

        assert_eq!(edge.target(), target);
        assert_eq!(edge.signal(), SignalType::Quote);
        assert_eq!(edge.relative_timestamp_secs(), 500_000);
        assert_eq!(edge.timestamp_secs(), ts);
        assert_eq!(edge.weight(), 2.0);
    }

    #[test]
    fn test_compact_edge_saturation() {
        let edge_before_epoch = CompactEdge::new(42, SignalType::Like, 1_000_000);
        assert_eq!(edge_before_epoch.relative_timestamp_secs(), 0);
        assert_eq!(edge_before_epoch.timestamp_secs(), BLUESKY_EPOCH_SECS);

        let edge_future =
            CompactEdge::new(42, SignalType::Repost, BLUESKY_EPOCH_SECS + 1_000_000_000);
        assert_eq!(edge_future.relative_timestamp_secs(), MAX_RELATIVE_SECS);
        assert_eq!(edge_future.signal(), SignalType::Repost);
    }

    #[test]
    fn test_signal_weights() {
        assert_eq!(SignalType::Like.weight(), 1.0);
        assert_eq!(SignalType::Quote.weight(), 2.0);
        assert_eq!(SignalType::Repost.weight(), 3.0);
    }

    #[test]
    fn test_post_meta_root_and_reply() {
        let root = PostMeta::new(10, None, None, 1_700_000_000);
        assert!(root.is_root());
        assert!(!root.is_reply());

        let reply = PostMeta::new(11, Some(100), Some(101), 1_700_000_010);
        assert!(!reply.is_root());
        assert!(reply.is_reply());
    }

    #[test]
    fn test_recommendation_dials_from_query() {
        let dials = RecommendationDials::from_query(
            Some("realtime"),
            Some("deep_dive"),
            Some(true),
            Some(50),
            Some("cursor123".to_string()),
        );
        assert_eq!(dials.half_life_secs, 6.0 * 3600.0);
        assert_eq!(dials.explore_ratio, 0.35);
        assert!(dials.explain);
        assert_eq!(dials.limit, 50);
        assert_eq!(dials.cursor.as_deref(), Some("cursor123"));
    }

    #[test]
    fn test_feed_skeleton_serialization() {
        let skeleton = FeedSkeletonResponse {
            feed: vec![
                SkeletonFeedPost::new("at://did:plc:alice/app.bsky.feed.post/123"),
                SkeletonFeedPost::with_repost(
                    "at://did:plc:bob/app.bsky.feed.post/456",
                    "at://did:plc:carol/app.bsky.feed.repost/789",
                ),
            ],
            cursor: Some("opaque_cursor".to_string()),
        };

        let json = serde_json::to_string(&skeleton).unwrap();
        assert!(json.contains("app.bsky.feed.defs#skeletonReasonRepost"));
        assert!(json.contains("at://did:plc:alice/app.bsky.feed.post/123"));
    }

    #[test]
    fn test_topic_category_variants_and_helpers() {
        assert_eq!(TOPIC_CATEGORIES.len(), 5);
        assert_eq!(NUM_TOPIC_CATEGORIES, 5);

        assert_eq!(TopicCategory::Art.as_str(), "art");
        assert_eq!(TopicCategory::Tech.as_str(), "tech");
        assert_eq!(TopicCategory::Science.as_str(), "science");
        assert_eq!(TopicCategory::News.as_str(), "news");
        assert_eq!(TopicCategory::Culture.as_str(), "culture");
        assert_eq!(TopicCategory::General.as_str(), "general");

        assert_eq!(TopicCategory::from_u8(0), Some(TopicCategory::Art));
        assert_eq!(TopicCategory::from_u8(1), Some(TopicCategory::Tech));
        assert_eq!(TopicCategory::from_u8(2), Some(TopicCategory::Science));
        assert_eq!(TopicCategory::from_u8(3), Some(TopicCategory::News));
        assert_eq!(TopicCategory::from_u8(4), Some(TopicCategory::Culture));
        assert_eq!(TopicCategory::from_u8(5), Some(TopicCategory::General));
        assert_eq!(TopicCategory::from_u8(6), None);

        assert_eq!(TopicCategory::Art.to_index(), 0);
        assert_eq!(TopicCategory::Tech.to_index(), 1);
        assert_eq!(TopicCategory::Science.to_index(), 2);
        assert_eq!(TopicCategory::News.to_index(), 3);
        assert_eq!(TopicCategory::Culture.to_index(), 4);

        assert_eq!(
            TopicCategory::from_str_name("ART"),
            Some(TopicCategory::Art)
        );
        assert_eq!(
            TopicCategory::from_str_name("technology"),
            Some(TopicCategory::Tech)
        );
        assert_eq!(
            TopicCategory::from_str_name("biology"),
            Some(TopicCategory::Science)
        );
        assert_eq!(
            TopicCategory::from_str_name("journalism"),
            Some(TopicCategory::News)
        );
        assert_eq!(
            TopicCategory::from_str_name("books"),
            Some(TopicCategory::Culture)
        );
        assert_eq!(TopicCategory::from_str_name("unknown_topic"), None);

        let json = serde_json::to_string(&TopicCategory::Science).unwrap();
        let parsed: TopicCategory = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed, TopicCategory::Science);
    }

    #[test]
    fn test_topic_weights_defaults_and_get_weight() {
        let weights = TopicWeights::default();
        assert_eq!(weights.art, 1.0);
        assert_eq!(weights.tech, 1.0);
        assert_eq!(weights.science, 1.0);
        assert_eq!(weights.news, 1.0);
        assert_eq!(weights.culture, 1.0);

        assert_eq!(weights.get_weight(TopicCategory::Art), 1.0);
        assert_eq!(weights.get_weight(TopicCategory::Tech), 1.0);
        assert_eq!(weights.get_weight(TopicCategory::Science), 1.0);
        assert_eq!(weights.get_weight(TopicCategory::News), 1.0);
        assert_eq!(weights.get_weight(TopicCategory::Culture), 1.0);
        assert_eq!(weights.get_weight(TopicCategory::General), 1.0);

        let custom = TopicWeights {
            art: 2.5,
            tech: 0.0,
            science: 1.5,
            news: 0.5,
            culture: 3.0,
        };
        assert_eq!(custom.get_weight(TopicCategory::Art), 2.5);
        assert_eq!(custom.get_weight(TopicCategory::Tech), 0.0);
        assert_eq!(custom.get_weight(TopicCategory::Science), 1.5);
        assert_eq!(custom.get_weight(TopicCategory::News), 0.5);
        assert_eq!(custom.get_weight(TopicCategory::Culture), 3.0);
        assert_eq!(custom.get_weight(TopicCategory::General), 1.0);
    }

    #[test]
    fn test_feed_preview_response_serialization() {
        let breakdown = ScoreBreakdown {
            time_decay: 0.95,
            taste_similarity: 0.85,
            topic_boost: 1.5,
            fatigue_penalty: 1.0,
            final_score: 1.21125,
        };

        let proof_step = ProofChainStep {
            step_type: "viewer_interaction".into(),
            node_id: "at://did:plc:alice/post/1".into(),
            description: "You liked this post".to_string(),
        };

        let proof_chain = GraphProofChain {
            steps: vec![proof_step],
            summary: "Recommended because of taste match".to_string(),
        };

        let preview_item = FeedPreviewItem {
            uri: "at://did:plc:author/post/100".into(),
            author_did: "did:plc:author".into(),
            topic: TopicCategory::Tech,
            tier: "Tier 1: 3-Step Interaction Walk".to_string(),
            score_breakdown: breakdown.clone(),
            proof_chain: Some(proof_chain),
        };

        let resp = FeedPreviewResponse {
            viewer_did: "did:plc:viewer".into(),
            items: vec![preview_item],
            total_candidates: 42,
            query_latency_us: 1500,
        };

        let json = serde_json::to_string(&resp).unwrap();
        let parsed: FeedPreviewResponse = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.viewer_did, "did:plc:viewer");
        assert_eq!(parsed.items.len(), 1);
        assert_eq!(parsed.total_candidates, 42);
        assert_eq!(parsed.items[0].topic, TopicCategory::Tech);
        assert_eq!(parsed.items[0].score_breakdown, breakdown);
    }

    #[test]
    fn test_taste_twins_response_serialization() {
        let shared_post = SharedPostInfo {
            uri: "at://did:plc:author/post/1".into(),
            author_did: "did:plc:author".into(),
            category: TopicCategory::Science,
            created_at: 1_700_000_000,
        };

        let twin = TasteTwinItem {
            user_did: "did:plc:twin1".into(),
            similarity_score: 0.88,
            shared_posts_count: 5,
            top_interests: vec![TopicCategory::Science, TopicCategory::Tech],
            shared_posts: vec![shared_post],
        };

        let resp = TasteTwinsResponse {
            viewer_did: "did:plc:viewer".into(),
            total_liked_posts: 20,
            twins: vec![twin],
            query_latency_us: 320,
        };

        let json = serde_json::to_string(&resp).unwrap();
        let parsed: TasteTwinsResponse = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.viewer_did, "did:plc:viewer");
        assert_eq!(parsed.total_liked_posts, 20);
        assert_eq!(parsed.twins.len(), 1);
        assert_eq!(parsed.twins[0].user_did, "did:plc:twin1");
        assert_eq!(parsed.twins[0].shared_posts.len(), 1);
    }

    #[test]
    fn test_telemetry_response_serialization() {
        let telemetry = TelemetryResponse {
            status: "ok".to_string(),
            uptime_seconds: 3600,
            graph: GraphTelemetryInfo {
                total_nodes: 100,
                total_users: 40,
                total_posts: 60,
                total_edges: 250,
                total_follows: 30,
                post_metadata_entries: 60,
                active_velocity_posts: 15,
            },
            interner: InternerTelemetryInfo {
                total_interned_strings: 100,
            },
            memory: MemoryTelemetryInfo {
                graph_bytes: 50000,
                interner_bytes: 20000,
                impression_bytes: 10000,
                total_estimated_bytes: 80000,
                formatted_total: "78.1 KB".to_string(),
            },
            ingestion: IngestionVelocityInfo {
                events_received: 1000,
                events_processed: 950,
                bytes_received: 50000,
                reconnect_count: 1,
                latest_cursor_us: 1_700_000_000_000_000,
                last_activity_timestamp: 1_700_000_000,
                velocity_events_per_sec: 42.5,
                initial_cursor_us: Some(1_699_000_000_000_000),
                target_cursor_us: Some(1_700_000_000_000_000),
                lag_seconds: 0,
                backfill_progress_percent: 100.0,
                is_live: true,
                eta_seconds: Some(0),
                speedup_factor: 35.0,
            },
            snapshot: SnapshotStatusInfo {
                status: "persisted".to_string(),
                last_saved_secs: 1_700_000_000,
                last_saved_ago_secs: 120,
                last_load_duration_ms: 15.2,
                last_save_duration_ms: 8.4,
                interval_secs: 300,
                file_path: "snapshot.bin".to_string(),
                file_size_bytes: 10240,
                format_version: 1,
            },

            impression_store: ImpressionTelemetryInfo {
                total_tracked_viewers: 25,
                hard_suppression_window_secs: 1800,
                fatigue_decay_window_secs: 21600,
            },
            admin_did: Some("did:plc:admin".to_string()),
        };

        let json = serde_json::to_string(&telemetry).unwrap();
        let parsed: TelemetryResponse = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.status, "ok");
        assert_eq!(parsed.uptime_seconds, 3600);
        assert_eq!(parsed.graph.total_nodes, 100);
        assert_eq!(parsed.ingestion.events_processed, 950);
        assert_eq!(parsed.snapshot.status, "persisted");
        assert_eq!(parsed.impression_store.total_tracked_viewers, 25);
    }

    #[test]
    fn test_taste_twins_query_helpers() {
        let q1 = TasteTwinsQuery {
            did: Some("did:plc:alice".to_string()),
            handle: None,
            limit: Some(25),
        };
        assert_eq!(q1.target_identifier(), Some("did:plc:alice"));
        assert_eq!(q1.limit_or_default(), 25);

        let q2 = TasteTwinsQuery {
            did: None,
            handle: Some("  @bob.bsky.social  ".to_string()),
            limit: None,
        };
        assert_eq!(q2.target_identifier(), Some("@bob.bsky.social"));
        assert_eq!(q2.limit_or_default(), 10);

        let q3 = TasteTwinsQuery {
            did: Some("   ".to_string()),
            handle: None,
            limit: Some(100),
        };
        assert_eq!(q3.target_identifier(), None);
        assert_eq!(q3.limit_or_default(), 50);
    }

    #[test]
    fn test_feed_preview_query_helpers() {
        let q = FeedPreviewQuery {
            viewer: Some("did:plc:viewer".to_string()),
            did: None,
            handle: None,
            freshness: Some("realtime".to_string()),
            discovery: Some("deep_dive".to_string()),
            art: Some(2.5),
            tech: Some(0.5),
            science: None,
            news: None,
            culture: None,
            limit: Some(20),
            explain: Some(true),
        };
        assert_eq!(q.viewer_identifier(), Some("did:plc:viewer"));

        let dials = q.to_dials();
        assert_eq!(dials.half_life_secs, 6.0 * 3600.0);
        assert_eq!(dials.explore_ratio, 0.35);
        assert_eq!(dials.limit, 20);
        assert!(dials.explain);
        assert_eq!(dials.topic_weights.art, 2.5);
        assert_eq!(dials.topic_weights.tech, 0.5);
        assert_eq!(dials.topic_weights.science, 1.0);
    }

    #[test]
    fn test_explain_query_and_api_error_response() {
        let q = ExplainQuery {
            viewer: Some("did:plc:alice".to_string()),
            did: None,
            uri: Some("at://did:plc:author/post/1".to_string()),
            post: None,
        };
        assert_eq!(q.viewer_identifier(), Some("did:plc:alice"));
        assert_eq!(q.post_uri(), Some("at://did:plc:author/post/1"));

        let err = ApiErrorResponse::new("InvalidRequest", "Missing parameter");
        let json = serde_json::to_string(&err).unwrap();
        let parsed: ApiErrorResponse = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed.error, "InvalidRequest");
        assert_eq!(parsed.message, "Missing parameter");
    }

    #[test]
    fn test_user_dials_default_and_validation() {
        let default_dials = UserDials::default();
        assert_eq!(default_dials.freshness_half_life_secs, 36.0 * 3600.0);
        assert_eq!(default_dials.freshness_half_life_hours(), 36.0);
        assert_eq!(default_dials.discovery_ratio(), 0.15);
        assert_eq!(default_dials.topic_weights.art, 1.0);
        assert!(default_dials.validate().is_ok());

        // Boundary minimums
        let min_dials = UserDials::from_hours(
            FRESHNESS_MIN_HOURS,
            DISCOVERY_MIN,
            TopicWeights {
                art: TOPIC_MIN,
                tech: TOPIC_MIN,
                science: TOPIC_MIN,
                news: TOPIC_MIN,
                culture: TOPIC_MIN,
            },
            1_700_000_000,
        );
        assert!(min_dials.validate().is_ok());
        assert_eq!(min_dials.freshness_half_life_hours(), 1.0);
        assert_eq!(min_dials.discovery_ratio(), 0.0);

        // Boundary maximums
        let max_dials = UserDials::from_hours(
            FRESHNESS_MAX_HOURS,
            DISCOVERY_MAX,
            TopicWeights {
                art: TOPIC_MAX,
                tech: TOPIC_MAX,
                science: TOPIC_MAX,
                news: TOPIC_MAX,
                culture: TOPIC_MAX,
            },
            1_700_000_000,
        );
        assert!(max_dials.validate().is_ok());
        assert_eq!(max_dials.freshness_half_life_hours(), 168.0);
        assert_eq!(max_dials.discovery_ratio(), 0.50);

        // Invalid freshness too low
        let mut invalid_freshness = default_dials;
        invalid_freshness.freshness_half_life_secs = 3599.0;
        assert!(invalid_freshness.validate().is_err());

        // Invalid freshness too high
        invalid_freshness.freshness_half_life_secs = MAX_FRESHNESS_SECS + 1.0;
        assert!(invalid_freshness.validate().is_err());

        // Invalid freshness NaN
        invalid_freshness.freshness_half_life_secs = f32::NAN;
        assert!(invalid_freshness.validate().is_err());

        // Invalid discovery too high
        let mut invalid_discovery = default_dials;
        invalid_discovery.serendipity_ratio = 0.51;
        assert!(invalid_discovery.validate().is_err());

        // Invalid discovery negative
        invalid_discovery.serendipity_ratio = -0.01;
        assert!(invalid_discovery.validate().is_err());

        // Invalid topic weight too high
        let mut invalid_topic = default_dials;
        invalid_topic.topic_weights.tech = 5.01;
        assert!(invalid_topic.validate().is_err());

        // Invalid topic weight negative
        invalid_topic.topic_weights.tech = -0.1;
        assert!(invalid_topic.validate().is_err());
    }

    #[test]
    fn test_user_dials_conversions() {
        let dials = UserDials::from_hours(
            24.0,
            0.20,
            TopicWeights {
                art: 1.5,
                tech: 2.0,
                science: 0.5,
                news: 1.0,
                culture: 1.2,
            },
            1_724_000_000,
        );

        let rec_dials = dials.to_recommendation_dials();
        assert_eq!(rec_dials.half_life_secs, 24.0 * 3600.0);
        assert_eq!(rec_dials.explore_ratio, 0.20);
        assert_eq!(rec_dials.topic_weights.art, 1.5);
        assert_eq!(rec_dials.limit, DEFAULT_PAGE_LIMIT);

        let from_rec = UserDials::from_recommendation_dials(&rec_dials, 1_724_000_000);
        assert_eq!(from_rec, dials);

        let mut existing_rec = RecommendationDials::default();
        dials.apply_to_recommendation_dials(&mut existing_rec);
        assert_eq!(existing_rec.half_life_secs, 24.0 * 3600.0);
        assert_eq!(existing_rec.explore_ratio, 0.20);
        assert_eq!(existing_rec.topic_weights.tech, 2.0);

        let resp: UserDialsResponse = dials.into();
        assert_eq!(resp.freshness_half_life_hours, 24.0);
        assert_eq!(resp.discovery_ratio, 0.20);
        assert_eq!(resp.updated_at_secs, 1_724_000_000);
    }

    #[test]
    fn test_oauth_client_metadata_new_and_serialization() {
        let meta = OAuthClientMetadata::new_for_host("feed.example.com");
        assert_eq!(
            meta.client_id,
            "https://feed.example.com/oauth/client-metadata.json"
        );
        assert_eq!(meta.client_uri, "https://feed.example.com");
        assert_eq!(meta.client_name, "For Your Consideration");
        assert_eq!(meta.scope, "atproto transition:generic");
        assert_eq!(meta.response_types, vec![CompactString::new("code")]);
        assert_eq!(
            meta.grant_types,
            vec![
                CompactString::new("authorization_code"),
                CompactString::new("refresh_token")
            ]
        );
        assert_eq!(meta.application_type, "web");
        assert_eq!(meta.token_endpoint_auth_method, "none");
        assert!(meta.dpop_bound_access_tokens);

        let json = serde_json::to_string(&meta).unwrap();
        let parsed: OAuthClientMetadata = serde_json::from_str(&json).unwrap();
        assert_eq!(parsed, meta);

        // Localhost host variant
        let local_meta = OAuthClientMetadata::new_for_host("localhost:3000");
        assert_eq!(
            local_meta.client_id,
            "http://127.0.0.1:3000/oauth/client-metadata.json"
        );
    }

    #[test]
    fn test_oauth_dtos_serialization() {
        let login_query = OAuthLoginQuery {
            handle: Some("alice.bsky.social".to_string()),
            redirect_uri: Some("https://example.com/callback".to_string()),
        };
        let lq_json = serde_json::to_string(&login_query).unwrap();
        let lq_parsed: OAuthLoginQuery = serde_json::from_str(&lq_json).unwrap();
        assert_eq!(lq_parsed, login_query);

        let login_resp = OAuthLoginResponse {
            status: "ok".into(),
            authorization_url: "https://auth.example.com/oauth/authorize?foo=bar".to_string(),
            state: "state_nonce_123".to_string(),
        };
        let lr_json = serde_json::to_string(&login_resp).unwrap();
        let lr_parsed: OAuthLoginResponse = serde_json::from_str(&lr_json).unwrap();
        assert_eq!(lr_parsed, login_resp);

        let cb_req = OAuthCallbackRequest {
            code: "auth_code_xyz".to_string(),
            state: "state_nonce_123".to_string(),
            iss: Some("https://pds.example.com".to_string()),
        };
        let cb_json = serde_json::to_string(&cb_req).unwrap();
        let cb_parsed: OAuthCallbackRequest = serde_json::from_str(&cb_json).unwrap();
        assert_eq!(cb_parsed, cb_req);

        let cb_resp = OAuthCallbackResponse {
            status: "ok".into(),
            did: "did:plc:alice".into(),
            handle: "alice.bsky.social".into(),
            token: "jwt.token.val".to_string(),
        };
        let cbr_json = serde_json::to_string(&cb_resp).unwrap();
        let cbr_parsed: OAuthCallbackResponse = serde_json::from_str(&cbr_json).unwrap();
        assert_eq!(cbr_parsed, cb_resp);

        let pub_req = FeedPublishRequest {
            display_name: "For Your Consideration".to_string(),
            rkey: "for-your-consideration".to_string(),
            description: "Personalized recommendation feed".to_string(),
        };
        let pr_json = serde_json::to_string(&pub_req).unwrap();
        let pr_parsed: FeedPublishRequest = serde_json::from_str(&pr_json).unwrap();
        assert_eq!(pr_parsed, pub_req);

        let pub_resp = FeedPublishResponse {
            status: "ok".into(),
            uri: "at://did:plc:alice/app.bsky.feed.generator/for-your-consideration".into(),
            cid: "bafyreig123".into(),
            share_url: "https://bsky.app/profile/did:plc:alice/feed/for-your-consideration".into(),
        };
        let presp_json = serde_json::to_string(&pub_resp).unwrap();
        let presp_parsed: FeedPublishResponse = serde_json::from_str(&presp_json).unwrap();
        assert_eq!(presp_parsed, pub_resp);
    }
}
