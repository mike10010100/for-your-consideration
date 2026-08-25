#![forbid(unsafe_code)]

//! Algorithmic recommendation engine for AT Protocol / Bluesky custom feeds.
//!
//! Implements:
//! - 3-tier cold-start hierarchy:
//!   - **Tier 1**: 3-step random walk graph traversal for active users ($\ge 10$ interactions).
//!   - **Tier 2**: 2-step follow-graph traversal for new users ($< 10$ interactions + follows).
//!   - **Tier 3**: Curated 6-hour high-velocity sliding pool for cold start / unauthenticated users.
//! - Cascading fallback between tiers when higher tiers yield empty candidate pools.
//! - Multi-factor candidate scoring:
//!   - Exponential half-life time decay ($W(e) = W_{\text{signal}} \cdot e^{-\Delta t / \tau}$).
//!   - RoaringBitmap-accelerated Cosine taste similarity.
//!   - BM25 inverse degree popularity dampening ($\frac{1}{\sqrt{|\text{GlobalInteractions}(p)| + 1}}$).
//! - Anti-fatigue filtering:
//!   - Seen / liked / interacted deduplication via user's `RoaringBitmap`.
//!   - Self-authored post exclusion.
//!   - Conversation thread / reply tree dampening (maximum 1 post per conversation root).
//!   - Author diversity constraint (maximum 2 posts per author per page).
//! - Serendipity exploration: 85/15 $\epsilon$-greedy blending with user-controllable dials.
//! - Deterministic, resilient cursor pagination and optional structured explainability traces.

use std::collections::VecDeque;
use std::sync::Arc;

use ahash::{AHashMap, AHashSet};
use compact_str::CompactString;
use parking_lot::RwLock;
use roaring::RoaringBitmap;

use crate::error::Result;
use crate::graph::{calculate_popularity_dampener, calculate_time_decay, GraphStore};
use crate::interner::StringInterner;
use crate::types::{
    CompactEdge, FeedPreviewItem, FeedPreviewResponse, FeedRecommendation, GraphProofChain,
    PostMeta, ProofChainStep, RecommendationDials, RecommendationSource, ScoreBreakdown,
    ScoredPost, SharedPostInfo, SignalType, TasteTwinItem, TasteTwinsResponse, TopicCategory,
    BLUESKY_EPOCH_SECS, DEFAULT_PAGE_LIMIT, NUM_TOPIC_CATEGORIES, TOPIC_CATEGORIES,
};

/// Number of parallel lock shards for impression memory.
pub const IMPRESSION_SHARDS: usize = 64;

/// Default maximum number of served post impressions tracked per user.
pub const DEFAULT_MAX_IMPRESSIONS_PER_USER: usize = 1_000;

/// Minimum score multiplier floor for immediately viewed posts (15% of original score).
pub const FATIGUE_MIN_FLOOR: f32 = 0.15;

/// Backward-compatibility alias for previous 30m window constant.
pub const HARD_SUPPRESSION_WINDOW_SECS: u64 = 30 * 60;

/// Soft fatigue window: posts served within the last 6 hours (21,600s) receive smooth exponential damping.
pub const FATIGUE_WINDOW_SECS: u64 = 6 * 3600;

/// Characteristic time constant (tau) for soft fatigue exponential recovery: 2 hours (7,200s).
pub const FATIGUE_TAU_SECS: f32 = 2.0 * 3600.0;

/// An individual post impression record with its served timestamp.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ImpressionEntry {
    /// Interned 32-bit post ID.
    pub post_id: u32,
    /// Unix timestamp in seconds when the post was served to the viewer.
    pub served_at_secs: u64,
}

/// Bounded sliding impression history for a single viewer.
#[derive(Debug, Clone)]
pub struct ViewerImpressionHistory {
    /// `RoaringBitmap` for ultrafast $O(1)$ set containment test.
    pub post_ids: RoaringBitmap,
    /// Map from `post_id` to the most recent served timestamp.
    pub timestamps: AHashMap<u32, u64>,
    /// Sliding FIFO queue for bounded capacity eviction.
    pub queue: VecDeque<ImpressionEntry>,
    /// Maximum capacity of impressions before oldest entries are evicted.
    pub max_capacity: usize,
}

impl Default for ViewerImpressionHistory {
    fn default() -> Self {
        Self::new(DEFAULT_MAX_IMPRESSIONS_PER_USER)
    }
}

impl ViewerImpressionHistory {
    /// Creates a new impression history with the specified maximum capacity.
    #[must_use]
    pub fn new(max_capacity: usize) -> Self {
        let initial_cap = max_capacity.min(128);
        Self {
            post_ids: RoaringBitmap::new(),
            timestamps: AHashMap::with_capacity(initial_cap),
            queue: VecDeque::with_capacity(initial_cap),
            max_capacity,
        }
    }

    /// Records an impression for a given post ID at the specified unix timestamp.
    pub fn record_impression(&mut self, post_id: u32, timestamp_secs: u64) {
        self.post_ids.insert(post_id);
        self.timestamps.insert(post_id, timestamp_secs);
        self.queue.push_back(ImpressionEntry {
            post_id,
            served_at_secs: timestamp_secs,
        });

        // Bounded capacity eviction
        while self.queue.len() > self.max_capacity {
            if let Some(oldest) = self.queue.pop_front() {
                if let Some(&latest_ts) = self.timestamps.get(&oldest.post_id) {
                    if latest_ts <= oldest.served_at_secs {
                        self.timestamps.remove(&oldest.post_id);
                        self.post_ids.remove(oldest.post_id);
                    }
                }
            }
        }
    }

    /// Returns the timestamp when the post was served, if present.
    #[must_use]
    pub fn get_served_timestamp(&self, post_id: u32) -> Option<u64> {
        if !self.post_ids.contains(post_id) {
            return None;
        }
        self.timestamps.get(&post_id).copied()
    }

    /// Checks if a post ID exists in this impression history.
    #[must_use]
    pub fn contains(&self, post_id: u32) -> bool {
        self.post_ids.contains(post_id)
    }

    /// Returns the number of distinct post IDs tracked.
    #[must_use]
    pub fn len(&self) -> usize {
        self.post_ids.len() as usize
    }

    /// Returns true if no impressions are recorded.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.post_ids.is_empty()
    }

    /// Prunes entries served before the given cutoff timestamp.
    pub fn prune_older_than(&mut self, cutoff_secs: u64) {
        while let Some(front) = self.queue.front() {
            if front.served_at_secs < cutoff_secs {
                if let Some(oldest) = self.queue.pop_front() {
                    if let Some(&latest_ts) = self.timestamps.get(&oldest.post_id) {
                        if latest_ts <= oldest.served_at_secs {
                            self.timestamps.remove(&oldest.post_id);
                            self.post_ids.remove(oldest.post_id);
                        }
                    }
                }
            } else {
                break;
            }
        }
    }
}

/// Single lock shard storing impression histories for a subset of viewers.
#[derive(Debug, Default)]
struct ImpressionShard {
    viewers: AHashMap<u32, ViewerImpressionHistory>,
}

/// 64-shard partitioned in-memory sliding LRU impression cache.
#[derive(Debug)]
pub struct ImpressionStore {
    shards: [RwLock<ImpressionShard>; IMPRESSION_SHARDS],
    max_impressions_per_user: usize,
}

impl Default for ImpressionStore {
    fn default() -> Self {
        Self::new(DEFAULT_MAX_IMPRESSIONS_PER_USER)
    }
}

impl ImpressionStore {
    /// Creates a new 64-shard impression store with bounded per-user capacity.
    #[must_use]
    pub fn new(max_impressions_per_user: usize) -> Self {
        Self {
            shards: std::array::from_fn(|_| RwLock::new(ImpressionShard::default())),
            max_impressions_per_user,
        }
    }

    /// Shard index calculation based on viewer ID.
    #[inline]
    const fn shard_index(viewer_id: u32) -> usize {
        (viewer_id as usize) % IMPRESSION_SHARDS
    }

    /// Records served post impressions for a viewer at the given timestamp.
    pub fn record_impressions(&self, viewer_id: u32, post_ids: &[u32], timestamp_secs: u64) {
        if post_ids.is_empty() {
            return;
        }
        let shard_idx = Self::shard_index(viewer_id);
        let mut guard = self.shards[shard_idx].write();
        let max_cap = self.max_impressions_per_user;
        let history = guard
            .viewers
            .entry(viewer_id)
            .or_insert_with(|| ViewerImpressionHistory::new(max_cap));

        for &pid in post_ids {
            history.record_impression(pid, timestamp_secs);
        }
    }

    /// Evaluates the smooth continuous fatigue penalty for a candidate post for a specific viewer.
    ///
    /// Formula:
    /// $$\text{Multiplier}(\Delta t) = \text{MIN\_FLOOR} + (1.0 - \text{MIN\_FLOOR}) \times \left(1.0 - \exp\left(-\frac{\Delta t}{\tau_{\text{fatigue}}}\right)\right)$$
    ///
    /// - Immediately upon view ($\Delta t = 0$): Multiplier is `0.15` (dampens score by 85%, pushing it down rank).
    /// - At $\Delta t = 30\text{m}$: Multiplier $\approx 0.338$.
    /// - At $\Delta t = 2\text{h}$: Multiplier $\approx 0.687$.
    /// - After $\Delta t \ge 6\text{h}$ (or if never served): Multiplier is `1.0` (fully recovered).
    #[must_use]
    pub fn evaluate_fatigue_penalty(
        &self,
        viewer_id: u32,
        post_id: u32,
        now_secs: u64,
    ) -> Option<f32> {
        let shard_idx = Self::shard_index(viewer_id);
        let guard = self.shards[shard_idx].read();
        let Some(history) = guard.viewers.get(&viewer_id) else {
            return Some(1.0);
        };
        let Some(served_ts) = history.get_served_timestamp(post_id) else {
            return Some(1.0);
        };

        let dt = now_secs.saturating_sub(served_ts);
        if dt >= FATIGUE_WINDOW_SECS {
            Some(1.0)
        } else {
            let recovery = 1.0 - (-((dt as f32) / FATIGUE_TAU_SECS)).exp();
            let multiplier = (1.0 - FATIGUE_MIN_FLOOR).mul_add(recovery, FATIGUE_MIN_FLOOR);
            Some(multiplier.clamp(FATIGUE_MIN_FLOOR, 1.0))
        }
    }

    /// Checks if a post ID is in the viewer's impression history.
    #[must_use]
    pub fn contains_impression(&self, viewer_id: u32, post_id: u32) -> bool {
        let shard_idx = Self::shard_index(viewer_id);
        let guard = self.shards[shard_idx].read();
        guard
            .viewers
            .get(&viewer_id)
            .is_some_and(|h| h.contains(post_id))
    }

    /// Returns the total number of distinct impressions recorded for a viewer.
    #[must_use]
    pub fn get_viewer_impression_count(&self, viewer_id: u32) -> usize {
        let shard_idx = Self::shard_index(viewer_id);
        let guard = self.shards[shard_idx].read();
        guard
            .viewers
            .get(&viewer_id)
            .map_or(0, ViewerImpressionHistory::len)
    }

    /// Returns the number of distinct viewers with recorded impression histories across all shards.
    #[must_use]
    pub fn total_viewers(&self) -> usize {
        self.shards.iter().map(|s| s.read().viewers.len()).sum()
    }

    /// Prunes expired impressions older than 6 hours across all shards.
    pub fn prune_expired(&self, now_secs: u64) {
        let cutoff = now_secs.saturating_sub(FATIGUE_WINDOW_SECS);
        for shard in &self.shards {
            let mut guard = shard.write();
            for history in guard.viewers.values_mut() {
                history.prune_older_than(cutoff);
            }
        }
    }

    /// Clears all impression histories across all shards.
    pub fn clear(&self) {
        for shard in &self.shards {
            shard.write().viewers.clear();
        }
    }

    /// Returns the estimated heap memory footprint in bytes.
    #[must_use]
    pub fn estimated_size_bytes(&self) -> usize {
        let mut total = std::mem::size_of::<Self>();
        for shard in &self.shards {
            let guard = shard.read();
            total += guard.viewers.capacity() * 48;
            for viewer in guard.viewers.values() {
                total += viewer.post_ids.serialized_size();
                total += viewer.timestamps.capacity()
                    * (std::mem::size_of::<u32>() + std::mem::size_of::<u64>() + 16);
                total += viewer.queue.capacity() * std::mem::size_of::<ImpressionEntry>();
            }
        }
        total
    }
}

/// Internal candidate evaluation structure used during recommendation preview scoring.
#[derive(Debug, Clone)]
struct CandidateEvaluation {
    post_id: u32,
    uri: CompactString,
    author_did: CompactString,
    author_id: u32,
    topic: TopicCategory,
    tier: String,
    score_breakdown: ScoreBreakdown,
}

/// High-performance graph traversal and multi-signal recommendation engine.
#[derive(Debug, Clone)]
pub struct Recommender {
    /// Bidirectional string interner.
    pub interner: Arc<StringInterner>,
    /// In-memory multi-signal graph store.
    pub graph: Arc<GraphStore>,
    /// In-memory sliding LRU impression cache.
    pub impression_store: Arc<ImpressionStore>,
}

impl Recommender {
    /// Creates a new [`Recommender`] instance wrapping the provided interner and graph store.
    #[must_use]
    pub fn new(interner: Arc<StringInterner>, graph: Arc<GraphStore>) -> Self {
        Self {
            interner,
            graph,
            impression_store: Arc::new(ImpressionStore::default()),
        }
    }

    /// Creates a new [`Recommender`] instance with a custom impression store.
    #[must_use]
    pub const fn with_impression_store(
        interner: Arc<StringInterner>,
        graph: Arc<GraphStore>,
        impression_store: Arc<ImpressionStore>,
    ) -> Self {
        Self {
            interner,
            graph,
            impression_store,
        }
    }

    /// Returns a reference to the shared string interner.
    #[must_use]
    pub const fn interner(&self) -> &Arc<StringInterner> {
        &self.interner
    }

    /// Returns a reference to the shared graph store.
    #[must_use]
    pub const fn graph(&self) -> &Arc<GraphStore> {
        &self.graph
    }

    /// Returns a reference to the shared impression store.
    #[must_use]
    pub const fn impression_store(&self) -> &Arc<ImpressionStore> {
        &self.impression_store
    }

    /// Records served post impressions for a viewer DID.
    pub fn record_impressions(
        &self,
        viewer_did: Option<&str>,
        post_ids: &[u32],
        timestamp_secs: u64,
    ) {
        if let Some(did) = viewer_did {
            let viewer_id = self.interner.intern(did);
            self.impression_store
                .record_impressions(viewer_id, post_ids, timestamp_secs);
        }
    }

    /// Records served post impressions for a viewer DID (alias for [`Recommender::record_impressions`]).
    pub fn record_impressions_by_did(
        &self,
        viewer_did: Option<&str>,
        post_ids: &[u32],
        timestamp_secs: u64,
    ) {
        self.record_impressions(viewer_did, post_ids, timestamp_secs);
    }

    /// Records served post impressions directly for a numeric viewer ID.
    pub fn record_impressions_for_user(
        &self,
        viewer_id: u32,
        post_ids: &[u32],
        timestamp_secs: u64,
    ) {
        self.impression_store
            .record_impressions(viewer_id, post_ids, timestamp_secs);
    }

    /// Evaluates recommendations for a viewer DID given algorithmic dials at an explicit unix timestamp in seconds.
    ///
    /// Executes the full 4-phase recommendation pipeline:
    /// 1. Cold-start tier selection and routing with cascading fallbacks.
    /// 2. Graph traversal and candidate scoring.
    /// 3. Anti-fatigue filtering (seen/self deduplication, impression fatigue suppression/decay, thread dampening, author diversity).
    /// 4. Serendipity blending, explainability traces, and cursor pagination.
    pub fn recommend(
        &self,
        viewer_did: Option<&str>,
        dials: &RecommendationDials,
        now_secs: u64,
    ) -> Result<FeedRecommendation> {
        let viewer_id = viewer_did.and_then(|did| self.interner.lookup_id(did));

        let (mut candidates, source) = viewer_id.map_or_else(
            || {
                (
                    self.traverse_tier3(dials, now_secs),
                    RecommendationSource::Tier3VelocityPool,
                )
            },
            |uid| {
                let user_likes = self.graph.get_user_likes_bitmap(uid);
                let likes_count = user_likes.as_ref().map_or(0, roaring::RoaringBitmap::len);

                if likes_count >= 10 {
                    // Tier 1: 3-step random walk
                    let t1_candidates = self.traverse_tier1(uid, dials, now_secs);
                    if t1_candidates.is_empty() {
                        // Cascading fallback to Tier 2
                        let t2_candidates = self.traverse_tier2(uid, dials, now_secs);
                        if t2_candidates.is_empty() {
                            (
                                self.traverse_tier3(dials, now_secs),
                                RecommendationSource::Tier3VelocityPool,
                            )
                        } else {
                            (t2_candidates, RecommendationSource::Tier2FollowWalk)
                        }
                    } else {
                        (t1_candidates, RecommendationSource::Tier1InteractionWalk)
                    }
                } else if likes_count > 0 || !self.graph.get_user_follows(uid).is_empty() {
                    // Tier 2: Follow-graph walk
                    let t2_candidates = self.traverse_tier2(uid, dials, now_secs);
                    if t2_candidates.is_empty() {
                        (
                            self.traverse_tier3(dials, now_secs),
                            RecommendationSource::Tier3VelocityPool,
                        )
                    } else {
                        (t2_candidates, RecommendationSource::Tier2FollowWalk)
                    }
                } else {
                    // Tier 3: Cold start velocity pool
                    (
                        self.traverse_tier3(dials, now_secs),
                        RecommendationSource::Tier3VelocityPool,
                    )
                }
            },
        );

        // Phase 4: Anti-Fatigue Filtering
        // 1. Seen / Liked deduplication & Self-post exclusion
        let seen_bitmap = viewer_id.and_then(|uid| self.graph.get_user_likes_bitmap(uid));
        candidates.retain(|c| {
            if let Some(ref seen) = seen_bitmap {
                if seen.contains(c.post_id) {
                    return false;
                }
            }
            if let Some(uid) = viewer_id {
                if c.author_id == uid {
                    return false;
                }
            }
            true
        });

        // 2. Impression Fatigue Filtering (Smooth Continuous Score Damping)
        if let Some(uid) = viewer_id {
            for c in &mut candidates {
                if let Some(multiplier) = self
                    .impression_store
                    .evaluate_fatigue_penalty(uid, c.post_id, now_secs)
                {
                    c.score *= multiplier;
                }
            }

            // Re-sort candidates by score descending after score dampening
            candidates.sort_by(|a, b| {
                b.score
                    .partial_cmp(&a.score)
                    .unwrap_or(std::cmp::Ordering::Equal)
                    .then_with(|| a.post_id.cmp(&b.post_id))
            });
        }

        // 3. Thread / reply tree dampening: max 1 post per conversation root (highest score wins)
        let mut root_indices: AHashMap<u32, usize> = AHashMap::new();
        let mut thread_filtered: Vec<ScoredPost> = Vec::with_capacity(candidates.len());
        for cand in candidates {
            let meta = self.graph.get_post_meta(cand.post_id);
            let root = meta.and_then(|m| m.root_id).unwrap_or(cand.post_id);
            if let Some(&idx) = root_indices.get(&root) {
                if cand.score > thread_filtered[idx].score {
                    thread_filtered[idx] = cand;
                }
            } else {
                root_indices.insert(root, thread_filtered.len());
                thread_filtered.push(cand);
            }
        }

        if dials.explain {
            for cand in &mut thread_filtered {
                let meta = self.graph.get_post_meta(cand.post_id);
                let root = meta.and_then(|m| m.root_id).unwrap_or(cand.post_id);
                let prev_explain = cand.explain.take();
                cand.explain = Some(if let Some(prev) = prev_explain {
                    format!("{prev}, root_id={root}")
                } else {
                    format!(
                        "source={}, score={:.3}, root_id={root}",
                        cand.source.as_str(),
                        cand.score
                    )
                });
            }
        }

        // 4. Author diversity filtering: max 2 posts per author per page
        let mut author_counts: AHashMap<u32, usize> = AHashMap::new();
        let mut diverse_posts = Vec::with_capacity(thread_filtered.len());
        for cand in thread_filtered {
            let count = author_counts.entry(cand.author_id).or_insert(0);
            if *count < 2 {
                *count += 1;
                diverse_posts.push(cand);
            }
        }

        // Serendipity Exploration Blending
        let final_posts = if dials.explore_ratio > 0.0 && diverse_posts.len() > 2 {
            apply_serendipity(diverse_posts, dials.explore_ratio, source)
        } else {
            diverse_posts
        };

        // Cursor pagination
        let (page_posts, next_cursor) =
            paginate_posts(&final_posts, dials.limit, dials.cursor.as_deref());

        Ok(FeedRecommendation {
            posts: page_posts,
            cursor: next_cursor,
        })
    }

    /// Evaluates recommendations at the specified explicit unix timestamp.
    ///
    /// Alias for [`Recommender::recommend`].
    pub fn recommend_at(
        &self,
        viewer_did: Option<&str>,
        dials: &RecommendationDials,
        now_secs: u64,
    ) -> Result<FeedRecommendation> {
        self.recommend(viewer_did, dials, now_secs)
    }

    /// Evaluates recommendations using current system clock time.
    pub fn recommend_now(
        &self,
        viewer_did: Option<&str>,
        dials: &RecommendationDials,
    ) -> Result<FeedRecommendation> {
        let now_secs = current_time_secs();
        self.recommend(viewer_did, dials, now_secs)
    }

    /// Discovers top taste twins for a given DID or handle using `RoaringBitmap` Cosine similarity over co-interactors.
    ///
    /// Returns:
    /// - `TasteTwinsResponse` with ranked twins, shared posts, top interests, and query latency in microseconds.
    /// - If the user is unknown or has 0 interactions, returns an empty twins list gracefully.
    pub fn find_taste_twins(&self, viewer_did: &str, limit: usize) -> Result<TasteTwinsResponse> {
        let start_instant = std::time::Instant::now();
        let clean_did = viewer_did.trim().trim_start_matches('@');
        let limit = limit.clamp(1, 50);

        let Some(viewer_id) = self.interner.lookup_id(clean_did) else {
            let latency = start_instant.elapsed().as_micros() as u64;
            return Ok(TasteTwinsResponse {
                viewer_did: CompactString::new(clean_did),
                total_liked_posts: 0,
                twins: Vec::new(),
                query_latency_us: latency,
            });
        };

        let Some(viewer_bm) = self.graph.get_user_likes_bitmap(viewer_id) else {
            let latency = start_instant.elapsed().as_micros() as u64;
            return Ok(TasteTwinsResponse {
                viewer_did: self
                    .interner
                    .lookup_str(viewer_id)
                    .unwrap_or_else(|| CompactString::new(clean_did)),
                total_liked_posts: 0,
                twins: Vec::new(),
                query_latency_us: latency,
            });
        };

        let v_len = viewer_bm.len();
        if v_len == 0 {
            let latency = start_instant.elapsed().as_micros() as u64;
            return Ok(TasteTwinsResponse {
                viewer_did: self
                    .interner
                    .lookup_str(viewer_id)
                    .unwrap_or_else(|| CompactString::new(clean_did)),
                total_liked_posts: 0,
                twins: Vec::new(),
                query_latency_us: latency,
            });
        }

        let sqrt_v_len = (v_len as f32).sqrt();

        // Accumulate unique co-interactors from all posts in viewer's bitmap
        let mut co_interactors = AHashSet::new();
        for post_id in &viewer_bm {
            let p_edges = self.graph.get_post_interactions(post_id);
            for edge in p_edges {
                let co_uid = edge.target();
                if co_uid != viewer_id {
                    co_interactors.insert(co_uid);
                }
            }
        }

        if co_interactors.is_empty() {
            let latency = start_instant.elapsed().as_micros() as u64;
            return Ok(TasteTwinsResponse {
                viewer_did: self
                    .interner
                    .lookup_str(viewer_id)
                    .unwrap_or_else(|| CompactString::new(clean_did)),
                total_liked_posts: v_len as usize,
                twins: Vec::new(),
                query_latency_us: latency,
            });
        }

        // Compute SIMD Cosine similarity
        let mut candidate_twins: Vec<(u32, f32, usize, RoaringBitmap)> =
            Vec::with_capacity(co_interactors.len());

        for co_uid in co_interactors {
            if let Some(co_bm) = self.graph.get_user_likes_bitmap(co_uid) {
                let co_len = co_bm.len() as f32;
                if co_len > 0.0 {
                    let inter_len = viewer_bm.intersection_len(&co_bm);
                    if inter_len > 0 {
                        let sim = (inter_len as f32) / (sqrt_v_len * co_len.sqrt());
                        candidate_twins.push((co_uid, sim, inter_len as usize, co_bm));
                    }
                }
            }
        }

        // Rank candidate twins: similarity DESC, shared_count DESC, user_id ASC
        candidate_twins.sort_by(|a, b| {
            b.1.partial_cmp(&a.1)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| b.2.cmp(&a.2))
                .then_with(|| a.0.cmp(&b.0))
        });
        candidate_twins.truncate(limit);

        let mut twins = Vec::with_capacity(candidate_twins.len());
        for (co_uid, sim, shared_count, co_bm) in candidate_twins {
            let Some(co_did) = self.interner.lookup_str(co_uid) else {
                continue;
            };

            let shared_bm = &viewer_bm & &co_bm;
            let mut shared_posts = Vec::new();
            let mut category_counts: AHashMap<TopicCategory, usize> = AHashMap::new();

            for pid in shared_bm.iter().take(5) {
                if let Some(uri) = self.interner.lookup_str(pid) {
                    let meta = self.graph.get_post_meta(pid);
                    let author_did = meta
                        .as_ref()
                        .and_then(|m| self.interner.lookup_str(m.author_id))
                        .unwrap_or_else(|| CompactString::new("unknown"));
                    let created_at = meta.as_ref().map_or(BLUESKY_EPOCH_SECS, |m| m.created_at);
                    let category = classify_post(pid, uri.as_str(), Some(author_did.as_str()));
                    *category_counts.entry(category).or_insert(0) += 1;

                    shared_posts.push(SharedPostInfo {
                        uri,
                        author_did,
                        category,
                        created_at,
                    });
                }
            }

            // Also sample co_bm to build interest profile
            for pid in co_bm.iter().take(20) {
                if let Some(uri) = self.interner.lookup_str(pid) {
                    let meta = self.graph.get_post_meta(pid);
                    let author_did = meta.and_then(|m| self.interner.lookup_str(m.author_id));
                    let category = classify_post(pid, uri.as_str(), author_did.as_deref());
                    *category_counts.entry(category).or_insert(0) += 1;
                }
            }

            let mut sorted_categories: Vec<(TopicCategory, usize)> =
                category_counts.into_iter().collect();
            sorted_categories.sort_by_key(|b| std::cmp::Reverse(b.1));
            let top_interests: Vec<TopicCategory> = sorted_categories
                .into_iter()
                .take(3)
                .map(|(cat, _)| cat)
                .collect();

            twins.push(TasteTwinItem {
                user_did: co_did,
                similarity_score: sim,
                shared_posts_count: shared_count,
                top_interests,
                shared_posts,
            });
        }

        let latency = start_instant.elapsed().as_micros() as u64;
        Ok(TasteTwinsResponse {
            viewer_did: self
                .interner
                .lookup_str(viewer_id)
                .unwrap_or_else(|| CompactString::new(clean_did)),
            total_liked_posts: v_len as usize,
            twins,
            query_latency_us: latency,
        })
    }

    /// Extracts the 3-step proof chain explaining why a post was recommended for a viewer:
    /// `You -> Interacted Post -> Taste Twin (@user) -> Recommended Post`.
    pub fn explain_recommendation(
        &self,
        viewer_did: &str,
        post_uri: &str,
    ) -> Result<GraphProofChain> {
        let clean_viewer = viewer_did.trim().trim_start_matches('@');
        let clean_uri = post_uri.trim();

        let post_id = self.interner.lookup_id(clean_uri);
        let Some(pid) = post_id else {
            return Ok(GraphProofChain {
                steps: vec![ProofChainStep {
                    step_type: "unindexed_post".into(),
                    node_id: CompactString::new(clean_uri),
                    description: format!("Post '{clean_uri}' is not yet indexed in the graph"),
                }],
                summary: format!("Post '{clean_uri}' not found in the current graph store."),
            });
        };

        let meta = self.graph.get_post_meta(pid);
        let author_did = meta
            .and_then(|m| self.interner.lookup_str(m.author_id))
            .unwrap_or_else(|| CompactString::new("unknown"));
        let topic = classify_post(pid, clean_uri, Some(author_did.as_str()));

        let viewer_id = self.interner.lookup_id(clean_viewer);
        if let Some(vid) = viewer_id {
            let post_edges = self.graph.get_post_interactions(pid);
            let viewer_bm = self.graph.get_user_likes_bitmap(vid);
            let mut best_twin: Option<(u32, f32, SignalType, u64)> = None;

            for edge in &post_edges {
                let co_user = edge.target();
                if co_user != vid {
                    let sim = self.graph.compute_cosine_similarity(vid, co_user);
                    if sim > 0.0 {
                        let score = sim * edge.weight();
                        if best_twin
                            .as_ref()
                            .is_none_or(|(_, best_s, _, _)| score > *best_s)
                        {
                            best_twin = Some((co_user, sim, edge.signal(), edge.timestamp_secs()));
                        }
                    }
                }
            }

            if let Some((twin_id, sim, twin_sig, _twin_ts)) = best_twin {
                let twin_did = self
                    .interner
                    .lookup_str(twin_id)
                    .unwrap_or_else(|| CompactString::new("did:plc:twin"));
                let twin_bm = self.graph.get_user_likes_bitmap(twin_id);

                let seed_pid = match (&viewer_bm, &twin_bm) {
                    (Some(v_bm), Some(t_bm)) => {
                        let common = v_bm & t_bm;
                        common.iter().next()
                    }
                    _ => None,
                };

                let (seed_uri, seed_sig) = seed_pid.map_or_else(
                    || (CompactString::new("shared_interest"), SignalType::Like),
                    |spid| {
                        let s_uri = self
                            .interner
                            .lookup_str(spid)
                            .unwrap_or_else(|| CompactString::new("seed_post"));
                        let v_edges = self.graph.get_user_interactions(vid);
                        let v_sig = v_edges
                            .iter()
                            .find(|e| e.target() == spid)
                            .map_or(SignalType::Like, CompactEdge::signal);
                        (s_uri, v_sig)
                    },
                );

                let steps = vec![
                    ProofChainStep {
                        step_type: "viewer_interaction".into(),
                        node_id: seed_uri.clone(),
                        description: format!("You {} seed post '{seed_uri}'", seed_sig.past_tense_verb()),
                    },
                    ProofChainStep {
                        step_type: "taste_similarity".into(),
                        node_id: twin_did.clone(),
                        description: format!(
                            "Taste twin @{twin_did} has a {:.1}% taste match with you based on shared likes",
                            sim * 100.0
                        ),
                    },
                    ProofChainStep {
                        step_type: "recommendation_signal".into(),
                        node_id: CompactString::new(clean_uri),
                        description: format!(
                            "Taste twin @{twin_did} {} this {} post",
                            twin_sig.past_tense_verb(),
                            topic.as_str()
                        ),
                    },
                ];

                let summary = format!(
                    "Recommended because you {} an earlier post, and taste twin @{twin_did} ({:.0}% taste match) {} this {} post.",
                    seed_sig.past_tense_verb(),
                    sim * 100.0,
                    twin_sig.past_tense_verb(),
                    topic.as_str()
                );

                return Ok(GraphProofChain { steps, summary });
            }

            // Tier 2 follow proof chain check
            let follows = self.graph.get_user_follows(vid);
            for edge in &post_edges {
                let co_user = edge.target();
                if follows.contains(&co_user) {
                    let f_did = self
                        .interner
                        .lookup_str(co_user)
                        .unwrap_or_else(|| CompactString::new("did:plc:followed"));
                    let f_sig = edge.signal();
                    let steps = vec![
                        ProofChainStep {
                            step_type: "follow_graph".into(),
                            node_id: f_did.clone(),
                            description: format!("You follow @{f_did}"),
                        },
                        ProofChainStep {
                            step_type: "followed_interaction".into(),
                            node_id: CompactString::new(clean_uri),
                            description: format!(
                                "Followed user @{f_did} {} this post",
                                f_sig.past_tense_verb()
                            ),
                        },
                        ProofChainStep {
                            step_type: "follow_affinity_boost".into(),
                            node_id: CompactString::new(clean_uri),
                            description: "Boosted by 1.5x follow graph affinity multiplier"
                                .to_string(),
                        },
                    ];
                    let summary = format!(
                        "Recommended because you follow @{f_did}, who {} this {} post.",
                        f_sig.past_tense_verb(),
                        topic.as_str()
                    );
                    return Ok(GraphProofChain { steps, summary });
                }
            }
        }

        // Tier 3 Cold-Start / Topic Velocity proof chain
        let steps = vec![
            ProofChainStep {
                step_type: "cold_start_onboarding".into(),
                node_id: CompactString::new(topic.as_str()),
                description: format!("Classified under {} topic domain", topic.as_str()),
            },
            ProofChainStep {
                step_type: "velocity_trending".into(),
                node_id: CompactString::new(clean_uri),
                description: "High interaction velocity in the last 6-hour sliding window"
                    .to_string(),
            },
            ProofChainStep {
                step_type: "topic_diversity_interleaving".into(),
                node_id: CompactString::new(clean_uri),
                description: "Selected via balanced topic diversity round-robin interleaving"
                    .to_string(),
            },
        ];
        let summary = format!(
            "Recommended as a trending high-velocity post in the {} topic domain.",
            topic.as_str()
        );
        Ok(GraphProofChain { steps, summary })
    }

    /// Evaluates recommendations in preview mode with transparent mathematical score breakdowns
    /// and graph proof chains, evaluating fatigue penalties in a read-only manner (without mutating impressions).
    pub fn recommend_preview(
        &self,
        viewer_did: Option<&str>,
        dials: &RecommendationDials,
    ) -> Result<FeedPreviewResponse> {
        let latest_graph_ts = self.graph.get_latest_interaction_timestamp();
        let sys_time = current_time_secs();
        let now_secs = if latest_graph_ts > sys_time
            || (latest_graph_ts > BLUESKY_EPOCH_SECS
                && sys_time.saturating_sub(latest_graph_ts) > crate::graph::SIX_HOURS_SECS)
        {
            latest_graph_ts
        } else {
            sys_time
        };
        self.recommend_preview_at(viewer_did, dials, now_secs)
    }

    /// Evaluates preview recommendations at an explicit timestamp.
    pub fn recommend_preview_at(
        &self,
        viewer_did: Option<&str>,
        dials: &RecommendationDials,
        now_secs: u64,
    ) -> Result<FeedPreviewResponse> {
        let start_instant = std::time::Instant::now();
        let clean_viewer = viewer_did.map(|d| d.trim().trim_start_matches('@'));
        let viewer_id = clean_viewer.and_then(|d| self.interner.lookup_id(d));

        let mut candidate_evals: Vec<CandidateEvaluation> = Vec::new();

        if let Some(uid) = viewer_id {
            let user_likes = self.graph.get_user_likes_bitmap(uid);
            let likes_count = user_likes.as_ref().map_or(0, RoaringBitmap::len);

            if likes_count >= 10 {
                // Tier 1 Preview Walk
                let user_interactions = self.graph.get_user_interactions(uid);
                let mut co_interactor_weights: AHashMap<u32, f32> = AHashMap::new();
                for edge in &user_interactions {
                    let post_id = edge.target();
                    let post_interactions = self.graph.get_post_interactions(post_id);
                    for p_edge in post_interactions {
                        let co_user = p_edge.target();
                        if co_user != uid {
                            let sim = self.graph.compute_cosine_similarity(uid, co_user);
                            *co_interactor_weights.entry(co_user).or_insert(0.0) += sim.max(0.1);
                        }
                    }
                }

                let mut cand_details: AHashMap<u32, (f32, f32, f32)> = AHashMap::new();
                for (&co_user, &co_sim) in &co_interactor_weights {
                    let co_interactions = self.graph.get_user_interactions(co_user);
                    for c_edge in co_interactions {
                        let cand_pid = c_edge.target();
                        let decay = calculate_time_decay(
                            c_edge.signal(),
                            c_edge.timestamp_secs(),
                            now_secs,
                            dials.half_life_secs,
                        );
                        let dampener = calculate_popularity_dampener(
                            self.graph.get_post_interaction_count(cand_pid),
                        );
                        let entry = cand_details
                            .entry(cand_pid)
                            .or_insert((0.0, decay, dampener));
                        entry.0 += co_sim;
                    }
                }

                for (pid, (sim, decay, dampener)) in cand_details {
                    let Some(uri) = self.interner.lookup_str(pid) else {
                        continue;
                    };
                    let Some(meta) = self.graph.get_post_meta(pid) else {
                        continue;
                    };
                    let author_did = self
                        .interner
                        .lookup_str(meta.author_id)
                        .unwrap_or_else(|| CompactString::new("unknown"));
                    let topic = classify_post(pid, uri.as_str(), Some(author_did.as_str()));
                    let topic_boost = dials.topic_weights.get_weight(topic);

                    let fatigue_penalty = self
                        .impression_store
                        .evaluate_fatigue_penalty(uid, pid, now_secs)
                        .unwrap_or(0.0);

                    let taste_similarity = sim * dampener;
                    let base_score = taste_similarity * decay * topic_boost;
                    let final_score = base_score * fatigue_penalty;

                    let breakdown = ScoreBreakdown {
                        time_decay: decay,
                        taste_similarity,
                        topic_boost,
                        fatigue_penalty,
                        final_score,
                    };

                    candidate_evals.push(CandidateEvaluation {
                        post_id: pid,
                        uri,
                        author_did,
                        author_id: meta.author_id,
                        topic,
                        tier: "Tier 1: 3-Step Interaction Walk".to_string(),
                        score_breakdown: breakdown,
                    });
                }
            } else if likes_count > 0 || !self.graph.get_user_follows(uid).is_empty() {
                // Tier 2 Preview Walk
                let follows = self.graph.get_user_follows(uid);
                let mut cand_details: AHashMap<u32, (f32, f32)> = AHashMap::new();
                for followed_id in follows {
                    let followed_interactions = self.graph.get_user_interactions(followed_id);
                    for edge in followed_interactions {
                        let cand_pid = edge.target();
                        let decay = calculate_time_decay(
                            edge.signal(),
                            edge.timestamp_secs(),
                            now_secs,
                            dials.half_life_secs,
                        );
                        let dampener = calculate_popularity_dampener(
                            self.graph.get_post_interaction_count(cand_pid),
                        );
                        cand_details.insert(cand_pid, (decay, dampener));
                    }
                }

                for (pid, (decay, dampener)) in cand_details {
                    let Some(uri) = self.interner.lookup_str(pid) else {
                        continue;
                    };
                    let Some(meta) = self.graph.get_post_meta(pid) else {
                        continue;
                    };
                    let author_did = self
                        .interner
                        .lookup_str(meta.author_id)
                        .unwrap_or_else(|| CompactString::new("unknown"));
                    let topic = classify_post(pid, uri.as_str(), Some(author_did.as_str()));
                    let topic_boost = dials.topic_weights.get_weight(topic);

                    let fatigue_penalty = self
                        .impression_store
                        .evaluate_fatigue_penalty(uid, pid, now_secs)
                        .unwrap_or(0.0);

                    let taste_similarity = 1.5 * dampener;
                    let base_score = taste_similarity * decay * topic_boost;
                    let final_score = base_score * fatigue_penalty;

                    let breakdown = ScoreBreakdown {
                        time_decay: decay,
                        taste_similarity,
                        topic_boost,
                        fatigue_penalty,
                        final_score,
                    };

                    candidate_evals.push(CandidateEvaluation {
                        post_id: pid,
                        uri,
                        author_did,
                        author_id: meta.author_id,
                        topic,
                        tier: "Tier 2: 2-Step Follow Walk".to_string(),
                        score_breakdown: breakdown,
                    });
                }
            }
        }

        // If no candidate evals from tier 1 or tier 2, evaluate Tier 3
        if candidate_evals.is_empty() {
            let pool_ids = self.graph.get_velocity_pool_candidates_at(now_secs, 100);
            for (idx, pid) in pool_ids.into_iter().enumerate() {
                let Some(uri) = self.interner.lookup_str(pid) else {
                    continue;
                };
                let meta = self
                    .graph
                    .get_post_meta(pid)
                    .unwrap_or_else(|| PostMeta::new(0, None, None, now_secs));
                let author_did = self
                    .interner
                    .lookup_str(meta.author_id)
                    .unwrap_or_else(|| CompactString::new("unknown"));
                let topic = classify_post(pid, uri.as_str(), Some(author_did.as_str()));
                let topic_boost = dials.topic_weights.get_weight(topic);

                let fatigue_penalty = viewer_id.map_or(1.0, |uid| {
                    self.impression_store
                        .evaluate_fatigue_penalty(uid, pid, now_secs)
                        .unwrap_or(0.0)
                });

                let taste_similarity = 100.0 / (idx as f32 + 1.0);
                let time_decay = 1.0;
                let final_score = taste_similarity * topic_boost * fatigue_penalty;

                let breakdown = ScoreBreakdown {
                    time_decay,
                    taste_similarity,
                    topic_boost,
                    fatigue_penalty,
                    final_score,
                };

                candidate_evals.push(CandidateEvaluation {
                    post_id: pid,
                    uri,
                    author_did,
                    author_id: meta.author_id,
                    topic,
                    tier: "Tier 3: Topic Velocity Pool".to_string(),
                    score_breakdown: breakdown,
                });
            }
        }

        let total_candidates = candidate_evals.len();

        // 1. Seen/liked deduplication & self-post exclusion & hard suppression filter
        let seen_bitmap = viewer_id.and_then(|uid| self.graph.get_user_likes_bitmap(uid));
        candidate_evals.retain(|c| {
            if let Some(ref seen) = seen_bitmap {
                if seen.contains(c.post_id) {
                    return false;
                }
            }
            if let Some(uid) = viewer_id {
                if c.author_id == uid {
                    return false;
                }
            }
            if c.score_breakdown.fatigue_penalty <= 0.0 {
                return false;
            }
            true
        });

        // 2. Sort descending by final_score
        candidate_evals.sort_by(|a, b| {
            b.score_breakdown
                .final_score
                .partial_cmp(&a.score_breakdown.final_score)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| a.post_id.cmp(&b.post_id))
        });

        // 3. Conversation tree root dampening (max 1 per tree root)
        let mut root_indices: AHashMap<u32, usize> = AHashMap::new();
        let mut thread_filtered: Vec<CandidateEvaluation> =
            Vec::with_capacity(candidate_evals.len());
        for cand in candidate_evals {
            let meta = self.graph.get_post_meta(cand.post_id);
            let root = meta.and_then(|m| m.root_id).unwrap_or(cand.post_id);
            if let Some(&idx) = root_indices.get(&root) {
                if cand.score_breakdown.final_score
                    > thread_filtered[idx].score_breakdown.final_score
                {
                    thread_filtered[idx] = cand;
                }
            } else {
                root_indices.insert(root, thread_filtered.len());
                thread_filtered.push(cand);
            }
        }

        // 4. Author diversity (max 2 per author)
        let mut author_counts: AHashMap<u32, usize> = AHashMap::new();
        let mut diverse_evals = Vec::with_capacity(thread_filtered.len());
        for cand in thread_filtered {
            let count = author_counts.entry(cand.author_id).or_insert(0);
            if *count < 2 {
                *count += 1;
                diverse_evals.push(cand);
            }
        }

        let limit = if dials.limit == 0 {
            DEFAULT_PAGE_LIMIT
        } else {
            dials.limit
        };
        diverse_evals.truncate(limit);

        let mut items = Vec::with_capacity(diverse_evals.len());
        for cand in diverse_evals {
            let proof_chain = if dials.explain {
                let did_str = clean_viewer.unwrap_or("");
                self.explain_recommendation(did_str, cand.uri.as_str()).ok()
            } else {
                None
            };

            items.push(FeedPreviewItem {
                uri: cand.uri,
                author_did: cand.author_did,
                topic: cand.topic,
                tier: cand.tier,
                score_breakdown: cand.score_breakdown,
                proof_chain,
            });
        }

        let query_latency_us = start_instant.elapsed().as_micros() as u64;
        Ok(FeedPreviewResponse {
            viewer_did: CompactString::new(clean_viewer.unwrap_or("")),
            items,
            total_candidates,
            query_latency_us,
        })
    }

    /// Performs the Tier 1 3-step co-interaction random walk:
    /// `Viewer -> Interacted Posts -> Top Co-Interactors (Cosine Taste Similarity) -> Candidate Posts`.
    #[must_use]
    pub fn traverse_tier1(
        &self,
        viewer_id: u32,
        dials: &RecommendationDials,
        now_secs: u64,
    ) -> Vec<ScoredPost> {
        let user_interactions = self.graph.get_user_interactions(viewer_id);
        let mut co_interactor_weights: AHashMap<u32, f32> = AHashMap::new();

        // Step 1 & 2: Viewer -> Seed Posts -> Co-interactors
        for edge in &user_interactions {
            let post_id = edge.target();
            let post_interactions = self.graph.get_post_interactions(post_id);
            for p_edge in post_interactions {
                let co_user = p_edge.target();
                if co_user != viewer_id {
                    let sim = self.graph.compute_cosine_similarity(viewer_id, co_user);
                    *co_interactor_weights.entry(co_user).or_insert(0.0) += sim.max(0.1);
                }
            }
        }

        // Step 3: Co-interactors -> Candidate Posts
        let mut candidate_scores: AHashMap<u32, f32> = AHashMap::new();
        for (&co_user, &co_sim) in &co_interactor_weights {
            let co_interactions = self.graph.get_user_interactions(co_user);
            for c_edge in co_interactions {
                let cand_pid = c_edge.target();
                let decay = calculate_time_decay(
                    c_edge.signal(),
                    c_edge.timestamp_secs(),
                    now_secs,
                    dials.half_life_secs,
                );
                let dampener =
                    calculate_popularity_dampener(self.graph.get_post_interaction_count(cand_pid));
                let score = co_sim * decay * dampener;
                *candidate_scores.entry(cand_pid).or_insert(0.0) += score;
            }
        }

        let mut scored: Vec<ScoredPost> = candidate_scores
            .into_iter()
            .filter_map(|(pid, raw_score)| {
                let uri = self.interner.lookup_str(pid)?;
                let meta = self.graph.get_post_meta(pid)?;
                let author_str = self.interner.lookup_str(meta.author_id);
                let category = classify_post(pid, uri.as_str(), author_str.as_deref());
                let topic_boost = dials.topic_weights.get_weight(category);
                let score = raw_score * topic_boost;
                Some(ScoredPost {
                    post_id: pid,
                    uri,
                    author_id: meta.author_id,
                    score,
                    source: RecommendationSource::Tier1InteractionWalk,
                    explain: None,
                })
            })
            .collect();

        scored.sort_by(|a, b| {
            b.score
                .partial_cmp(&a.score)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| a.post_id.cmp(&b.post_id))
        });
        scored
    }

    /// Performs the Tier 2 2-step follow-graph traversal:
    /// `Viewer -> Followed Accounts -> Followed Interactions`.
    #[must_use]
    pub fn traverse_tier2(
        &self,
        viewer_id: u32,
        dials: &RecommendationDials,
        now_secs: u64,
    ) -> Vec<ScoredPost> {
        let follows = self.graph.get_user_follows(viewer_id);
        let mut candidate_scores: AHashMap<u32, f32> = AHashMap::new();

        for followed_id in follows {
            let followed_interactions = self.graph.get_user_interactions(followed_id);
            for edge in followed_interactions {
                let cand_pid = edge.target();
                let decay = calculate_time_decay(
                    edge.signal(),
                    edge.timestamp_secs(),
                    now_secs,
                    dials.half_life_secs,
                );
                let dampener =
                    calculate_popularity_dampener(self.graph.get_post_interaction_count(cand_pid));
                let score = decay * dampener * 1.5; // Follow boost multiplier
                *candidate_scores.entry(cand_pid).or_insert(0.0) += score;
            }
        }

        let mut scored: Vec<ScoredPost> = candidate_scores
            .into_iter()
            .filter_map(|(pid, raw_score)| {
                let uri = self.interner.lookup_str(pid)?;
                let meta = self.graph.get_post_meta(pid)?;
                let author_str = self.interner.lookup_str(meta.author_id);
                let category = classify_post(pid, uri.as_str(), author_str.as_deref());
                let topic_boost = dials.topic_weights.get_weight(category);
                let score = raw_score * topic_boost;
                Some(ScoredPost {
                    post_id: pid,
                    uri,
                    author_id: meta.author_id,
                    score,
                    source: RecommendationSource::Tier2FollowWalk,
                    explain: None,
                })
            })
            .collect();

        scored.sort_by(|a, b| {
            b.score
                .partial_cmp(&a.score)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| a.post_id.cmp(&b.post_id))
        });
        scored
    }

    /// Classifies a post into its [`TopicCategory`] using creator seeds, URI keywords, or deterministic hash fallback.
    #[must_use]
    pub fn classify_post_by_id(&self, post_id: u32, uri: &str, author_id: u32) -> TopicCategory {
        let author_str = self.interner.lookup_str(author_id);
        classify_post(post_id, uri, author_str.as_deref())
    }

    /// Retrieves candidates from the Tier 3 global 6-hour high-velocity sliding pool
    /// with topic diversity clustering and balanced round-robin interleaving.
    #[must_use]
    pub fn traverse_tier3(&self, dials: &RecommendationDials, now_secs: u64) -> Vec<ScoredPost> {
        let pool_ids = self.graph.get_velocity_pool_candidates_at(now_secs, 100);

        let mut buckets: [Vec<ScoredPost>; NUM_TOPIC_CATEGORIES] = Default::default();

        for (idx, pid) in pool_ids.into_iter().enumerate() {
            let Some(uri) = self.interner.lookup_str(pid) else {
                continue;
            };
            let meta = self
                .graph
                .get_post_meta(pid)
                .unwrap_or_else(|| PostMeta::new(0, None, None, now_secs));
            let author_str = self.interner.lookup_str(meta.author_id);
            let category = classify_post(pid, uri.as_str(), author_str.as_deref());

            let topic_boost = dials.topic_weights.get_weight(category);
            let score = (100.0 / (idx as f32 + 1.0)) * topic_boost;
            let explain = if dials.explain {
                Some(format!(
                    "source={}, score={score:.3}, topic={}",
                    RecommendationSource::Tier3VelocityPool.as_str(),
                    category.as_str()
                ))
            } else {
                None
            };

            let post = ScoredPost {
                post_id: pid,
                uri,
                author_id: meta.author_id,
                score,
                source: RecommendationSource::Tier3VelocityPool,
                explain,
            };

            let bucket_idx = category.to_index();
            buckets[bucket_idx].push(post);
        }

        interleave_topic_buckets(&buckets)
    }
}

/// Seed creator mapping entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CreatorSeedEntry {
    /// Author DID, handle, or identifier pattern.
    pub author_pattern: &'static str,
    /// Associated topic domain category.
    pub category: TopicCategory,
}

/// Curated high-signal starter creator seeds mapped to topic domains.
pub static CURATED_CREATOR_SEEDS: &[CreatorSeedEntry] = &[
    // Art
    CreatorSeedEntry {
        author_pattern: "did:plc:art_seed",
        category: TopicCategory::Art,
    },
    CreatorSeedEntry {
        author_pattern: "did:plc:artist",
        category: TopicCategory::Art,
    },
    CreatorSeedEntry {
        author_pattern: "did:plc:painter",
        category: TopicCategory::Art,
    },
    CreatorSeedEntry {
        author_pattern: "did:plc:illustrator",
        category: TopicCategory::Art,
    },
    CreatorSeedEntry {
        author_pattern: "did:plc:photographer",
        category: TopicCategory::Art,
    },
    CreatorSeedEntry {
        author_pattern: "did:plc:designer",
        category: TopicCategory::Art,
    },
    CreatorSeedEntry {
        author_pattern: "art.bsky.social",
        category: TopicCategory::Art,
    },
    CreatorSeedEntry {
        author_pattern: "illustration.bsky.social",
        category: TopicCategory::Art,
    },
    CreatorSeedEntry {
        author_pattern: "photography.bsky.social",
        category: TopicCategory::Art,
    },
    CreatorSeedEntry {
        author_pattern: "design.bsky.social",
        category: TopicCategory::Art,
    },
    CreatorSeedEntry {
        author_pattern: "generativeart.bsky.social",
        category: TopicCategory::Art,
    },
    CreatorSeedEntry {
        author_pattern: "sketchbook.bsky.social",
        category: TopicCategory::Art,
    },
    // Tech
    CreatorSeedEntry {
        author_pattern: "did:plc:tech_seed",
        category: TopicCategory::Tech,
    },
    CreatorSeedEntry {
        author_pattern: "did:plc:developer",
        category: TopicCategory::Tech,
    },
    CreatorSeedEntry {
        author_pattern: "did:plc:coder",
        category: TopicCategory::Tech,
    },
    CreatorSeedEntry {
        author_pattern: "did:plc:engineer",
        category: TopicCategory::Tech,
    },
    CreatorSeedEntry {
        author_pattern: "did:plc:programmer",
        category: TopicCategory::Tech,
    },
    CreatorSeedEntry {
        author_pattern: "did:plc:rustacean",
        category: TopicCategory::Tech,
    },
    CreatorSeedEntry {
        author_pattern: "did:plc:tech",
        category: TopicCategory::Tech,
    },
    CreatorSeedEntry {
        author_pattern: "tech.bsky.social",
        category: TopicCategory::Tech,
    },
    CreatorSeedEntry {
        author_pattern: "rustlang.bsky.social",
        category: TopicCategory::Tech,
    },
    CreatorSeedEntry {
        author_pattern: "linux.bsky.social",
        category: TopicCategory::Tech,
    },
    CreatorSeedEntry {
        author_pattern: "opensource.bsky.social",
        category: TopicCategory::Tech,
    },
    CreatorSeedEntry {
        author_pattern: "dev.bsky.social",
        category: TopicCategory::Tech,
    },
    CreatorSeedEntry {
        author_pattern: "software.bsky.social",
        category: TopicCategory::Tech,
    },
    CreatorSeedEntry {
        author_pattern: "github.bsky.social",
        category: TopicCategory::Tech,
    },
    // Science
    CreatorSeedEntry {
        author_pattern: "did:plc:science_seed",
        category: TopicCategory::Science,
    },
    CreatorSeedEntry {
        author_pattern: "did:plc:scientist",
        category: TopicCategory::Science,
    },
    CreatorSeedEntry {
        author_pattern: "did:plc:astronomy",
        category: TopicCategory::Science,
    },
    CreatorSeedEntry {
        author_pattern: "did:plc:biology",
        category: TopicCategory::Science,
    },
    CreatorSeedEntry {
        author_pattern: "did:plc:physics",
        category: TopicCategory::Science,
    },
    CreatorSeedEntry {
        author_pattern: "did:plc:space",
        category: TopicCategory::Science,
    },
    CreatorSeedEntry {
        author_pattern: "did:plc:nature",
        category: TopicCategory::Science,
    },
    CreatorSeedEntry {
        author_pattern: "did:plc:science",
        category: TopicCategory::Science,
    },
    CreatorSeedEntry {
        author_pattern: "science.bsky.social",
        category: TopicCategory::Science,
    },
    CreatorSeedEntry {
        author_pattern: "nature.bsky.social",
        category: TopicCategory::Science,
    },
    CreatorSeedEntry {
        author_pattern: "physics.bsky.social",
        category: TopicCategory::Science,
    },
    CreatorSeedEntry {
        author_pattern: "space.bsky.social",
        category: TopicCategory::Science,
    },
    CreatorSeedEntry {
        author_pattern: "climate.bsky.social",
        category: TopicCategory::Science,
    },
    CreatorSeedEntry {
        author_pattern: "nasa.bsky.social",
        category: TopicCategory::Science,
    },
    CreatorSeedEntry {
        author_pattern: "arxiv.bsky.social",
        category: TopicCategory::Science,
    },
    // News
    CreatorSeedEntry {
        author_pattern: "did:plc:news_seed",
        category: TopicCategory::News,
    },
    CreatorSeedEntry {
        author_pattern: "did:plc:journalist",
        category: TopicCategory::News,
    },
    CreatorSeedEntry {
        author_pattern: "did:plc:reporter",
        category: TopicCategory::News,
    },
    CreatorSeedEntry {
        author_pattern: "did:plc:times",
        category: TopicCategory::News,
    },
    CreatorSeedEntry {
        author_pattern: "did:plc:press",
        category: TopicCategory::News,
    },
    CreatorSeedEntry {
        author_pattern: "did:plc:breaking",
        category: TopicCategory::News,
    },
    CreatorSeedEntry {
        author_pattern: "did:plc:news",
        category: TopicCategory::News,
    },
    CreatorSeedEntry {
        author_pattern: "news.bsky.social",
        category: TopicCategory::News,
    },
    CreatorSeedEntry {
        author_pattern: "journalism.bsky.social",
        category: TopicCategory::News,
    },
    CreatorSeedEntry {
        author_pattern: "breaking.bsky.social",
        category: TopicCategory::News,
    },
    CreatorSeedEntry {
        author_pattern: "press.bsky.social",
        category: TopicCategory::News,
    },
    CreatorSeedEntry {
        author_pattern: "worldnews.bsky.social",
        category: TopicCategory::News,
    },
    CreatorSeedEntry {
        author_pattern: "reuters.bsky.social",
        category: TopicCategory::News,
    },
    CreatorSeedEntry {
        author_pattern: "apnews.bsky.social",
        category: TopicCategory::News,
    },
    // Culture
    CreatorSeedEntry {
        author_pattern: "did:plc:culture_seed",
        category: TopicCategory::Culture,
    },
    CreatorSeedEntry {
        author_pattern: "did:plc:writer",
        category: TopicCategory::Culture,
    },
    CreatorSeedEntry {
        author_pattern: "did:plc:author",
        category: TopicCategory::Culture,
    },
    CreatorSeedEntry {
        author_pattern: "did:plc:music",
        category: TopicCategory::Culture,
    },
    CreatorSeedEntry {
        author_pattern: "did:plc:cinema",
        category: TopicCategory::Culture,
    },
    CreatorSeedEntry {
        author_pattern: "did:plc:film",
        category: TopicCategory::Culture,
    },
    CreatorSeedEntry {
        author_pattern: "did:plc:books",
        category: TopicCategory::Culture,
    },
    CreatorSeedEntry {
        author_pattern: "did:plc:culture",
        category: TopicCategory::Culture,
    },
    CreatorSeedEntry {
        author_pattern: "culture.bsky.social",
        category: TopicCategory::Culture,
    },
    CreatorSeedEntry {
        author_pattern: "books.bsky.social",
        category: TopicCategory::Culture,
    },
    CreatorSeedEntry {
        author_pattern: "film.bsky.social",
        category: TopicCategory::Culture,
    },
    CreatorSeedEntry {
        author_pattern: "music.bsky.social",
        category: TopicCategory::Culture,
    },
    CreatorSeedEntry {
        author_pattern: "philosophy.bsky.social",
        category: TopicCategory::Culture,
    },
    CreatorSeedEntry {
        author_pattern: "history.bsky.social",
        category: TopicCategory::Culture,
    },
    CreatorSeedEntry {
        author_pattern: "cinema.bsky.social",
        category: TopicCategory::Culture,
    },
];

/// Matches an author DID or handle against curated starter creator seeds and domain keywords.
#[must_use]
pub fn match_creator_seed(author: &str) -> Option<TopicCategory> {
    let lower = author.to_ascii_lowercase();

    // 1. Direct seed pattern lookup
    for seed in CURATED_CREATOR_SEEDS {
        if lower == seed.author_pattern || lower.starts_with(seed.author_pattern) {
            return Some(seed.category);
        }
    }

    // 2. Tokenized author handle / DID matching
    for token in lower.split(|c: char| !c.is_alphanumeric()) {
        if token.is_empty() {
            continue;
        }
        match token {
            "art" | "artist" | "painter" | "illustrator" | "photographer" | "drawing"
            | "mastoart" | "designer" | "creative" => return Some(TopicCategory::Art),
            "tech" | "developer" | "coder" | "engineer" | "programmer" | "rustacean" | "linux"
            | "software" | "github" | "compiler" | "hacker" => return Some(TopicCategory::Tech),
            "science" | "scientist" | "astronomy" | "biology" | "physics" | "space" | "nature"
            | "climate" | "quantum" | "chemistry" | "nasa" | "arxiv" => {
                return Some(TopicCategory::Science)
            }
            "news" | "journalist" | "reporter" | "journalism" | "breaking" | "press" | "times"
            | "tribune" | "gazette" | "reuters" | "chronicle" => return Some(TopicCategory::News),
            "culture" | "writer" | "books" | "book" | "music" | "musician" | "cinema" | "film"
            | "movie" | "history" | "philosophy" | "literature" | "poetry" => {
                return Some(TopicCategory::Culture)
            }
            _ => {}
        }
    }

    None
}

/// Matches a post URI or hashtag string against topic domain keywords.
#[must_use]
pub fn match_uri_keywords(uri: &str) -> Option<TopicCategory> {
    let lower = uri.to_ascii_lowercase();

    for token in lower.split(|c: char| !c.is_alphanumeric()) {
        if token.is_empty() {
            continue;
        }
        match token {
            "art" | "arts" | "artwork" | "illustration" | "illustrator" | "drawing"
            | "painting" | "photo" | "photography" | "photographer" | "sketch" | "design"
            | "generativeart" | "mastoart" | "conceptart" | "3dart" | "digitalart"
            | "watercolor" | "creative" | "gallery" | "portrait" => {
                return Some(TopicCategory::Art)
            }
            "tech" | "technology" | "programming" | "coding" | "coder" | "rust" | "rustlang"
            | "python" | "golang" | "typescript" | "javascript" | "linux" | "ai" | "ml"
            | "machinelearning" | "software" | "dev" | "cloud" | "crypto" | "webdev"
            | "computerscience" | "opensource" | "backend" | "frontend" | "fullstack"
            | "kernel" | "database" => return Some(TopicCategory::Tech),
            "science" | "scientific" | "physics" | "physicist" | "astronomy" | "astrophysics"
            | "biology" | "biologist" | "chemistry" | "chemist" | "space" | "nasa" | "climate"
            | "genetics" | "neuroscience" | "quantum" | "ecology" | "geology" | "paleontology"
            | "evolution" | "telescope" | "cosmos" => return Some(TopicCategory::Science),
            "news" | "breaking" | "breakingnews" | "press" | "journalism" | "journalist"
            | "politics" | "political" | "election" | "worldnews" | "headline" | "economy"
            | "economics" | "investigative" | "report" | "reporting" | "daily" | "gazette" => {
                return Some(TopicCategory::News)
            }
            "culture" | "cultural" | "book" | "books" | "booksky" | "literature" | "literary"
            | "novel" | "music" | "musician" | "film" | "films" | "movie" | "movies" | "cinema"
            | "history" | "historical" | "philosophy" | "philosophical" | "gaming" | "poetry"
            | "poet" | "theatre" | "humanities" => return Some(TopicCategory::Culture),
            _ => {}
        }
    }

    None
}

/// Deterministic topic hash partition fallback for unclassified posts.
///
/// Ensures uniform and deterministic mapping across the 5 primary topic categories:
/// [`TopicCategory::Art`], [`TopicCategory::Tech`], [`TopicCategory::Science`],
/// [`TopicCategory::News`], [`TopicCategory::Culture`].
#[must_use]
pub fn deterministic_topic_fallback(post_id: u32, uri: &str) -> TopicCategory {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for &byte in uri.as_bytes() {
        hash ^= u64::from(byte);
        hash = hash.wrapping_mul(0x0100_0000_01b3);
    }
    hash ^= u64::from(post_id);
    hash = hash.wrapping_mul(0x0100_0000_01b3);

    let index = (hash % (NUM_TOPIC_CATEGORIES as u64)) as usize;
    TOPIC_CATEGORIES[index]
}

/// Classifies a post into a [`TopicCategory`] using creator seeds, URI/hashtag keywords,
/// or deterministic hash fallback.
#[must_use]
pub fn classify_post(post_id: u32, uri: &str, author_str: Option<&str>) -> TopicCategory {
    if let Some(author) = author_str {
        if let Some(category) = match_creator_seed(author) {
            return category;
        }
    }

    if let Some(category) = match_uri_keywords(uri) {
        return category;
    }

    deterministic_topic_fallback(post_id, uri)
}

/// Interleaves candidates round-robin across topic buckets with graceful backfill.
///
/// Ensures single-topic viral spikes cannot monopolize the top positions in the feed,
/// while guaranteeing representation from all non-empty topic domains.
#[must_use]
pub fn interleave_topic_buckets(
    buckets: &[Vec<ScoredPost>; NUM_TOPIC_CATEGORIES],
) -> Vec<ScoredPost> {
    let total_candidates: usize = buckets.iter().map(Vec::len).sum();
    if total_candidates == 0 {
        return Vec::new();
    }

    let mut result = Vec::with_capacity(total_candidates);
    let max_len = buckets.iter().map(Vec::len).max().unwrap_or(0);

    for round in 0..max_len {
        for bucket in buckets {
            if round < bucket.len() {
                result.push(bucket[round].clone());
            }
        }
    }

    result
}

/// Blends top exploitation candidates with exploration serendipity candidates.
fn apply_serendipity(
    posts: Vec<ScoredPost>,
    explore_ratio: f32,
    _primary_source: RecommendationSource,
) -> Vec<ScoredPost> {
    let total = posts.len();
    let explore_count = ((total as f32) * explore_ratio).round() as usize;
    if explore_count == 0 || explore_count >= total {
        return posts;
    }

    let exploit_count = total - explore_count;
    let mut result = Vec::with_capacity(total);

    // Exploit top items
    for p in posts.iter().take(exploit_count) {
        result.push(p.clone());
    }

    // Explore remaining items with adjusted source tag
    for p in posts.iter().skip(exploit_count) {
        let mut expl = p.clone();
        expl.source = RecommendationSource::ExplorationSerendipity;
        result.push(expl);
    }

    result
}

/// Paginates candidate posts using stable, deterministic offset cursors.
fn paginate_posts(
    posts: &[ScoredPost],
    limit: usize,
    cursor: Option<&str>,
) -> (Vec<ScoredPost>, Option<String>) {
    let start_idx = cursor.map_or(0, |c| {
        c.parse::<usize>().unwrap_or_else(|_| {
            // Resilient Base64 decode fallback
            use base64::engine::general_purpose::URL_SAFE_NO_PAD;
            use base64::Engine;
            URL_SAFE_NO_PAD
                .decode(c)
                .ok()
                .and_then(|bytes| String::from_utf8(bytes).ok())
                .and_then(|s| s.parse::<usize>().ok())
                .unwrap_or(0)
        })
    });

    if start_idx >= posts.len() {
        return (Vec::new(), None);
    }

    let effective_limit = if limit == 0 {
        DEFAULT_PAGE_LIMIT
    } else {
        limit
    };

    let end_idx = (start_idx + effective_limit).min(posts.len());
    let page = posts[start_idx..end_idx].to_vec();

    let next_cursor = if end_idx < posts.len() {
        Some(end_idx.to_string())
    } else {
        None
    };

    (page, next_cursor)
}

/// Helper for defensive system time computation.
fn current_time_secs() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(BLUESKY_EPOCH_SECS, |d| d.as_secs())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::SignalType;
    use ahash::AHashSet;

    #[test]
    fn test_recommender_creation_and_accessors() {
        let interner = Arc::new(StringInterner::new());
        let graph = Arc::new(GraphStore::new());
        let rec = Recommender::new(Arc::clone(&interner), Arc::clone(&graph));

        assert_eq!(rec.interner().len(), 0);
        assert_eq!(rec.graph().stats().total_users, 0);
    }

    #[test]
    fn test_tier1_traversal_and_scoring() {
        let interner = Arc::new(StringInterner::new());
        let graph = Arc::new(GraphStore::new());

        let viewer = interner.intern("did:plc:viewer");
        let co_user = interner.intern("did:plc:co_user");
        let author = interner.intern("did:plc:author");

        let seed_post = interner.intern("at://did:plc:author/app.bsky.feed.post/seed");
        let cand_post = interner.intern("at://did:plc:author/app.bsky.feed.post/cand");

        let now = BLUESKY_EPOCH_SECS + 10_000;

        graph.record_post_meta(seed_post, author, None, None, now - 500);
        graph.record_post_meta(cand_post, author, None, None, now - 300);

        // Viewer likes seed post
        graph.record_interaction(viewer, seed_post, SignalType::Like, now - 200);
        // Co-user likes seed post and cand post
        graph.record_interaction(co_user, seed_post, SignalType::Like, now - 180);
        graph.record_interaction(co_user, cand_post, SignalType::Repost, now - 150);

        let rec = Recommender::new(interner, graph);
        let dials = RecommendationDials::default();
        let candidates = rec.traverse_tier1(viewer, &dials, now);

        assert_eq!(candidates.len(), 2);
        // Candidate post should be discovered
        assert!(candidates.iter().any(|c| c.post_id == cand_post));
    }

    #[test]
    fn test_tier2_follow_walk_traversal() {
        let interner = Arc::new(StringInterner::new());
        let graph = Arc::new(GraphStore::new());

        let viewer = interner.intern("did:plc:viewer");
        let followed = interner.intern("did:plc:followed");
        let author = interner.intern("did:plc:author");
        let post = interner.intern("at://did:plc:author/app.bsky.feed.post/1");

        let now = BLUESKY_EPOCH_SECS + 10_000;
        graph.record_post_meta(post, author, None, None, now - 100);
        graph.record_follow(viewer, followed);
        graph.record_interaction(followed, post, SignalType::Like, now - 50);

        let rec = Recommender::new(interner, graph);
        let dials = RecommendationDials::default();
        let candidates = rec.traverse_tier2(viewer, &dials, now);

        assert_eq!(candidates.len(), 1);
        assert_eq!(candidates[0].post_id, post);
        assert_eq!(candidates[0].source, RecommendationSource::Tier2FollowWalk);
    }

    #[test]
    fn test_author_diversity_enforced() {
        let interner = Arc::new(StringInterner::new());
        let graph = Arc::new(GraphStore::new());

        let viewer = interner.intern("did:plc:viewer");
        let followed = interner.intern("did:plc:followed");
        let dominant_author = interner.intern("did:plc:dominant_author");

        let now = BLUESKY_EPOCH_SECS + 10_000;
        graph.record_follow(viewer, followed);

        // Dominant author has 5 posts liked by followed
        for i in 1..=5 {
            let pid = interner.intern(&format!("at://did:plc:dominant_author/post/{i}"));
            graph.record_post_meta(pid, dominant_author, None, None, now - 100);
            graph.record_interaction(followed, pid, SignalType::Like, now - 50);
        }

        let rec = Recommender::new(interner, graph);
        let dials = RecommendationDials::default();
        let res = rec.recommend(Some("did:plc:viewer"), &dials, now).unwrap();

        // Max 2 posts from dominant_author allowed
        let author_posts_count = res
            .posts
            .iter()
            .filter(|p| p.author_id == dominant_author)
            .count();
        assert_eq!(author_posts_count, 2);
    }

    #[test]
    fn test_thread_dampening_enforced() {
        let interner = Arc::new(StringInterner::new());
        let graph = Arc::new(GraphStore::new());

        let viewer = interner.intern("did:plc:viewer");
        let followed = interner.intern("did:plc:followed");
        let author1 = interner.intern("did:plc:author1");
        let author2 = interner.intern("did:plc:author2");

        let root_post = interner.intern("at://did:plc:author1/post/root");
        let reply_post = interner.intern("at://did:plc:author2/post/reply");

        let now = BLUESKY_EPOCH_SECS + 10_000;
        graph.record_post_meta(root_post, author1, None, None, now - 100);
        graph.record_post_meta(
            reply_post,
            author2,
            Some(root_post),
            Some(root_post),
            now - 50,
        );

        graph.record_follow(viewer, followed);
        graph.record_interaction(followed, root_post, SignalType::Like, now - 40);
        graph.record_interaction(followed, reply_post, SignalType::Like, now - 30);

        let rec = Recommender::new(interner, graph);
        let dials = RecommendationDials::default();
        let res = rec.recommend(Some("did:plc:viewer"), &dials, now).unwrap();

        // Only 1 post from the root conversation tree
        assert_eq!(res.posts.len(), 1);
    }

    #[test]
    fn test_pagination_and_cursor() {
        let interner = Arc::new(StringInterner::new());
        let graph = Arc::new(GraphStore::new());

        let viewer = interner.intern("did:plc:viewer");
        let followed = interner.intern("did:plc:followed");

        let now = BLUESKY_EPOCH_SECS + 10_000;
        graph.record_follow(viewer, followed);

        for i in 1..=10 {
            let author = interner.intern(&format!("did:plc:author_{i}"));
            let pid = interner.intern(&format!("at://did:plc:author_{i}/post/1"));
            graph.record_post_meta(pid, author, None, None, now - 100);
            graph.record_interaction(followed, pid, SignalType::Like, now - 50);
        }

        let rec = Recommender::new(interner, graph);
        let dials = RecommendationDials {
            limit: 4,
            ..Default::default()
        };

        let page1 = rec.recommend(Some("did:plc:viewer"), &dials, now).unwrap();
        assert_eq!(page1.posts.len(), 4);
        assert_eq!(page1.cursor.as_deref(), Some("4"));

        let dials2 = RecommendationDials {
            limit: 4,
            cursor: page1.cursor,
            ..Default::default()
        };
        let page2 = rec.recommend(Some("did:plc:viewer"), &dials2, now).unwrap();
        assert_eq!(page2.posts.len(), 4);
        assert_eq!(page2.cursor.as_deref(), Some("8"));

        let dials3 = RecommendationDials {
            limit: 4,
            cursor: page2.cursor,
            ..Default::default()
        };
        let page3 = rec.recommend(Some("did:plc:viewer"), &dials3, now).unwrap();
        assert_eq!(page3.posts.len(), 2);
        assert_eq!(page3.cursor, None);
    }

    #[test]
    fn test_explainability_trace_populated() {
        let interner = Arc::new(StringInterner::new());
        let graph = Arc::new(GraphStore::new());

        let viewer = interner.intern("did:plc:viewer");
        let followed = interner.intern("did:plc:followed");
        let author = interner.intern("did:plc:author");
        let post = interner.intern("at://did:plc:author/post/1");

        let now = BLUESKY_EPOCH_SECS + 10_000;
        graph.record_post_meta(post, author, None, None, now - 100);
        graph.record_follow(viewer, followed);
        graph.record_interaction(followed, post, SignalType::Like, now - 50);

        let rec = Recommender::new(interner, graph);
        let dials = RecommendationDials {
            explain: true,
            ..Default::default()
        };

        let res = rec.recommend(Some("did:plc:viewer"), &dials, now).unwrap();
        assert_eq!(res.posts.len(), 1);
        let explain = res.posts[0].explain.as_ref().unwrap();
        assert!(explain.contains("source=tier2_follow_walk"));
        assert!(explain.contains("score="));
        assert!(explain.contains("root_id="));
    }

    #[test]
    fn test_impression_store_hard_suppression_and_decay_evaluation() {
        let store = ImpressionStore::new(100);
        let viewer_id = 42;
        let post_id = 100;
        let served_at = 1_000_000;

        store.record_impressions(viewer_id, &[post_id], served_at);

        // Immediately viewed (dt = 0 to 100s): smoothly dampened with minimum floor of 0.15
        let mult_100s = store
            .evaluate_fatigue_penalty(viewer_id, post_id, served_at + 100)
            .unwrap();
        let expected_100s = (1.0 - FATIGUE_MIN_FLOOR).mul_add(
            1.0 - (-100.0f32 / FATIGUE_TAU_SECS).exp(),
            FATIGUE_MIN_FLOOR,
        );
        assert!((mult_100s - expected_100s).abs() < 1e-4);
        assert!(mult_100s >= FATIGUE_MIN_FLOOR);

        // At 30m (1800s): smooth exponential recovery
        let mult_30m = store
            .evaluate_fatigue_penalty(viewer_id, post_id, served_at + 1800)
            .unwrap();
        let expected_30m = (1.0 - FATIGUE_MIN_FLOOR).mul_add(
            1.0 - (-1800.0f32 / FATIGUE_TAU_SECS).exp(),
            FATIGUE_MIN_FLOOR,
        );
        assert!((mult_30m - expected_30m).abs() < 1e-4);

        // At 2h (7200s): soft decay at tau
        let mult_2h = store
            .evaluate_fatigue_penalty(viewer_id, post_id, served_at + 7200)
            .unwrap();
        let expected_2h =
            (1.0 - FATIGUE_MIN_FLOOR).mul_add(1.0 - (-1.0f32).exp(), FATIGUE_MIN_FLOOR); // 0.15 + 0.85 * (1 - 1/e) ≈ 0.6873
        assert!((mult_2h - expected_2h).abs() < 1e-4);

        // At 6h (21600s): full recovery
        let mult_6h = store
            .evaluate_fatigue_penalty(viewer_id, post_id, served_at + 21600)
            .unwrap();
        assert_eq!(mult_6h, 1.0);

        // Unseen post receives 1.0
        assert_eq!(
            store.evaluate_fatigue_penalty(viewer_id, 999, served_at + 100),
            Some(1.0)
        );
    }

    #[test]
    fn test_recommender_record_impressions_and_anti_fatigue_recommendation() {
        let interner = Arc::new(StringInterner::new());
        let graph = Arc::new(GraphStore::new());

        let viewer = interner.intern("did:plc:viewer");
        let followed = interner.intern("did:plc:followed");
        let author = interner.intern("did:plc:author");

        let p1 = interner.intern("at://did:plc:author/post/1");
        let p2 = interner.intern("at://did:plc:author/post/2");

        let now = BLUESKY_EPOCH_SECS + 10_000;
        graph.record_post_meta(p1, author, None, None, now - 100);
        graph.record_post_meta(p2, author, None, None, now - 100);
        graph.record_follow(viewer, followed);
        graph.record_interaction(followed, p1, SignalType::Like, now - 50);
        graph.record_interaction(followed, p2, SignalType::Like, now - 50);

        let rec = Recommender::new(interner, graph);
        let dials = RecommendationDials::default();

        // Initial recommendation returns both posts
        let initial = rec.recommend(Some("did:plc:viewer"), &dials, now).unwrap();
        assert_eq!(initial.posts.len(), 2);

        // Record impression for p1
        rec.record_impressions(Some("did:plc:viewer"), &[p1], now);

        // Immediate refresh (at now + 60s): p1 is smoothly dampened, so fresh p2 ranks #1 and p1 is ranked #2
        let refresh = rec
            .recommend(Some("did:plc:viewer"), &dials, now + 60)
            .unwrap();
        assert_eq!(refresh.posts.len(), 2);
        assert_eq!(refresh.posts[0].post_id, p2); // fresh p2 ranked first
        assert_eq!(refresh.posts[1].post_id, p1); // seen p1 pushed down to second

        // 2 hours later (now + 7200): p1 has recovered more score but is still ranked below fresh p2
        let later = rec
            .recommend(Some("did:plc:viewer"), &dials, now + 7200)
            .unwrap();
        assert_eq!(later.posts.len(), 2);
        assert_eq!(later.posts[0].post_id, p2); // fresh p2 ranked first
        assert_eq!(later.posts[1].post_id, p1); // recovering p1 ranked second
    }

    #[test]
    fn test_viewer_impression_history_capacity_bounding() {
        let mut history = ViewerImpressionHistory::new(3);
        history.record_impression(1, 100);
        history.record_impression(2, 200);
        history.record_impression(3, 300);
        assert_eq!(history.len(), 3);
        assert!(history.contains(1));

        // 4th impression evicts post 1
        history.record_impression(4, 400);
        assert_eq!(history.len(), 3);
        assert!(!history.contains(1));
        assert!(history.contains(2));
        assert!(history.contains(3));
        assert!(history.contains(4));
    }

    #[test]
    fn test_creator_seed_matching() {
        assert_eq!(
            match_creator_seed("did:plc:art_seed"),
            Some(TopicCategory::Art)
        );
        assert_eq!(
            match_creator_seed("tech.bsky.social"),
            Some(TopicCategory::Tech)
        );
        assert_eq!(
            match_creator_seed("science.bsky.social"),
            Some(TopicCategory::Science)
        );
        assert_eq!(
            match_creator_seed("news.bsky.social"),
            Some(TopicCategory::News)
        );
        assert_eq!(
            match_creator_seed("culture.bsky.social"),
            Some(TopicCategory::Culture)
        );
        assert_eq!(match_creator_seed("did:plc:random_user_12345"), None);
    }

    #[test]
    fn test_uri_keyword_matching() {
        assert_eq!(
            match_uri_keywords("at://did:plc:user/app.bsky.feed.post/my_cool_drawing_art"),
            Some(TopicCategory::Art)
        );
        assert_eq!(
            match_uri_keywords("at://did:plc:user/app.bsky.feed.post/rust_compiler_optimization"),
            Some(TopicCategory::Tech)
        );
        assert_eq!(
            match_uri_keywords("at://did:plc:user/app.bsky.feed.post/quantum_physics_paper"),
            Some(TopicCategory::Science)
        );
        assert_eq!(
            match_uri_keywords("at://did:plc:user/app.bsky.feed.post/breakingnews_election_update"),
            Some(TopicCategory::News)
        );
        assert_eq!(
            match_uri_keywords("at://did:plc:user/app.bsky.feed.post/booksky_novel_review"),
            Some(TopicCategory::Culture)
        );
        assert_eq!(
            match_uri_keywords("at://did:plc:user/app.bsky.feed.post/3k1234567"),
            None
        );
    }

    #[test]
    fn test_deterministic_topic_fallback_consistency_and_distribution() {
        let uri = "at://did:plc:anon/app.bsky.feed.post/3kgeneric123";
        let cat1 = deterministic_topic_fallback(42, uri);
        let cat2 = deterministic_topic_fallback(42, uri);
        assert_eq!(cat1, cat2);

        // Verify that across various posts, all 5 categories are hit
        let mut counts = [0usize; 5];
        for pid in 1..=500 {
            let sample_uri = format!("at://did:plc:anon/app.bsky.feed.post/post_{pid}");
            let cat = deterministic_topic_fallback(pid, &sample_uri);
            counts[cat.to_index()] += 1;
        }

        for &c in &counts {
            assert!(
                c > 50,
                "Bucket count {c} should be well-represented across 500 posts"
            );
        }
    }

    #[test]
    fn test_interleave_topic_buckets_round_robin_and_backfill() {
        let mut buckets: [Vec<ScoredPost>; NUM_TOPIC_CATEGORIES] = Default::default();

        // 1 Art, 3 Tech, 1 Science, 0 News, 0 Culture
        buckets[TopicCategory::Art.to_index()].push(ScoredPost {
            post_id: 1,
            uri: "at://did:plc:art/post/1".into(),
            author_id: 10,
            score: 100.0,
            source: RecommendationSource::Tier3VelocityPool,
            explain: None,
        });

        for i in 1..=3 {
            buckets[TopicCategory::Tech.to_index()].push(ScoredPost {
                post_id: 10 + i,
                uri: format!("at://did:plc:tech/post/{i}").into(),
                author_id: 20,
                score: 90.0,
                source: RecommendationSource::Tier3VelocityPool,
                explain: None,
            });
        }

        buckets[TopicCategory::Science.to_index()].push(ScoredPost {
            post_id: 30,
            uri: "at://did:plc:sci/post/1".into(),
            author_id: 30,
            score: 80.0,
            source: RecommendationSource::Tier3VelocityPool,
            explain: None,
        });

        let interleaved = interleave_topic_buckets(&buckets);
        assert_eq!(interleaved.len(), 5);

        // Round 0: Art(1), Tech(11), Science(30)
        assert_eq!(interleaved[0].post_id, 1);
        assert_eq!(interleaved[1].post_id, 11);
        assert_eq!(interleaved[2].post_id, 30);

        // Round 1: Tech(12)
        assert_eq!(interleaved[3].post_id, 12);

        // Round 2: Tech(13)
        assert_eq!(interleaved[4].post_id, 13);
    }

    #[test]
    fn test_traverse_tier3_topic_diversity_interleaving() {
        let interner = Arc::new(StringInterner::new());
        let graph = Arc::new(GraphStore::new());
        let now = BLUESKY_EPOCH_SECS + 10_000;

        let art_author = interner.intern("did:plc:art_seed");
        let tech_author = interner.intern("did:plc:tech_seed");
        let sci_author = interner.intern("did:plc:science_seed");

        let art_post = interner.intern("at://did:plc:art_seed/post/1");
        let tech_post = interner.intern("at://did:plc:tech_seed/post/1");
        let sci_post = interner.intern("at://did:plc:science_seed/post/1");

        graph.record_post_meta(art_post, art_author, None, None, now - 100);
        graph.record_post_meta(tech_post, tech_author, None, None, now - 100);
        graph.record_post_meta(sci_post, sci_author, None, None, now - 100);

        // Simulate interaction velocity
        let liker = interner.intern("did:plc:liker");
        graph.record_interaction(liker, art_post, SignalType::Like, now - 50);
        graph.record_interaction(liker, tech_post, SignalType::Like, now - 40);
        graph.record_interaction(liker, sci_post, SignalType::Like, now - 30);

        let rec = Recommender::new(interner, graph);
        let dials = RecommendationDials {
            explain: true,
            ..Default::default()
        };

        let tier3_candidates = rec.traverse_tier3(&dials, now);
        assert_eq!(tier3_candidates.len(), 3);

        // Verify that candidates from Art, Tech, Science are all present
        let mut categories_found = AHashSet::new();
        for cand in &tier3_candidates {
            if let Some(ref exp) = cand.explain {
                for cat in &TOPIC_CATEGORIES {
                    if exp.contains(&format!("topic={}", cat.as_str())) {
                        categories_found.insert(*cat);
                    }
                }
            }
        }

        assert!(categories_found.contains(&TopicCategory::Art));
        assert!(categories_found.contains(&TopicCategory::Tech));
        assert!(categories_found.contains(&TopicCategory::Science));
    }
}
