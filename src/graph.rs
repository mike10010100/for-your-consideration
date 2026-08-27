use std::sync::atomic::{AtomicU64, Ordering};

use ahash::{AHashMap, AHashSet};
use parking_lot::RwLock;
use roaring::RoaringBitmap;
use serde::{Deserialize, Serialize};

use crate::types::{CompactEdge, PostMeta, SignalType};

#[derive(Debug, Clone)]
struct VelocityCandidateCache {
    current_time_secs: u64,
    limit: usize,
    mutation_count: u64,
    candidates: Vec<u32>,
}

const RING_BUFFER_CAPACITY: usize = 65_536;

#[derive(Debug, Clone)]
struct RecentActiveRingBuffer {
    buffer: Vec<(u64, u32)>,
    head: usize,
    count: usize,
}

impl Default for RecentActiveRingBuffer {
    fn default() -> Self {
        Self {
            buffer: vec![(0, 0); RING_BUFFER_CAPACITY],
            head: 0,
            count: 0,
        }
    }
}

impl RecentActiveRingBuffer {
    #[inline]
    fn push(&mut self, ts: u64, pid: u32) {
        self.buffer[self.head] = (ts, pid);
        self.head = (self.head + 1) % RING_BUFFER_CAPACITY;
        if self.count < RING_BUFFER_CAPACITY {
            self.count += 1;
        }
    }

    #[inline]
    const fn clear(&mut self) {
        self.head = 0;
        self.count = 0;
    }

    #[inline]
    const fn is_empty(&self) -> bool {
        self.count == 0
    }

    fn collect_candidates(
        &self,
        window_start: u64,
        current_time_secs: u64,
        out: &mut AHashSet<u32>,
    ) {
        if self.count == 0 {
            return;
        }
        for i in 0..self.count {
            let (ts, pid) = self.buffer[i];
            if ts >= window_start && ts <= current_time_secs {
                out.insert(pid);
            }
        }
    }
}

/// Number of parallel lock shards to minimize contention under high write/read concurrency.
pub const NUM_SHARDS: usize = 64;

/// 6 hours in seconds (used for Tier 3 cold-start high-velocity window).
pub const SIX_HOURS_SECS: u64 = 6 * 3600;

/// Default half-life in seconds (36 hours).
pub const DEFAULT_HALF_LIFE_SECS: f32 = 36.0 * 3600.0;

/// Returns the shard index for a given 32-bit identifier.
#[inline]
const fn shard_idx(id: u32) -> usize {
    (id as usize) & (NUM_SHARDS - 1)
}

/// Computes exponential time decay for an interaction edge.
///
/// Formula:
/// $$W(e) = W_{\text{signal}} \cdot e^{-\frac{\Delta t}{\tau}}$$
///
/// where $\Delta t = t_{\text{current}} - t_{\text{event}}$ (saturating), and $\tau$ is `half_life_secs`.
#[must_use]
pub fn calculate_time_decay(
    signal: SignalType,
    event_time_secs: u64,
    current_time_secs: u64,
    half_life_secs: f32,
) -> f32 {
    let dt = current_time_secs.saturating_sub(event_time_secs) as f32;
    let tau = if half_life_secs <= 0.0 {
        DEFAULT_HALF_LIFE_SECS
    } else {
        half_life_secs
    };
    let decay = (-dt / tau).exp();
    signal.weight() * decay
}

/// Default sublinear social proof alpha growth parameter ($\alpha = 0.15$).
pub const DEFAULT_SOCIAL_PROOF_ALPHA: f32 = 0.15;

/// Default soft viral plateau lambda taper parameter ($\lambda = 0.10$).
pub const DEFAULT_SOCIAL_PROOF_LAMBDA: f32 = 0.10;

/// Interaction threshold where the social proof curve transitions to the soft viral plateau ($N = 500$).
pub const SOCIAL_PROOF_PLATEAU_THRESHOLD: usize = 500;

/// Default multi-curator consensus boost parameter ($\mu = 0.45$).
pub const DEFAULT_CONSENSUS_BOOST_MU: f32 = 0.45;

/// Computes the continuous social proof quality curve factor for a candidate post.
///
/// Formula:
/// $$S(N) = \left(\frac{N + 1.0}{N + 3.0}\right) \times \left(1.0 + \alpha \cdot \ln(1 + \min(N, 500))\right) \times \frac{1.0}{1.0 + \lambda \cdot \ln\left(1 + \frac{\max(0, N - 500)}{500}\right)}$$
///
/// where $\alpha = 0.15$, $\lambda = 0.10$.
///
/// Numerical invariants:
/// - $N = 0 \implies S(0) \approx 0.333$ (unvetted noise moderation baseline)
/// - $N = 3 \implies S(3) \approx 0.806$ (rapid quality ramp for early community signal)
/// - $N = 10 \implies S(10) \approx 1.150$
/// - $N = 50 \implies S(50) \approx 1.529$ (strong boost for validated posts)
/// - $N = 500 \implies S(500) \approx 1.924$ (peak quality plateau)
/// - $N = 5000 \implies S(5000) \approx 1.570$ (smooth logarithmic taper preventing viral monopoly)
#[must_use]
pub fn calculate_social_proof_factor(global_interactions_count: usize) -> f32 {
    let n = global_interactions_count as f32;
    let baseline_ratio = (n + 1.0) / (n + 3.0);

    let growth_n = if global_interactions_count <= SOCIAL_PROOF_PLATEAU_THRESHOLD {
        n
    } else {
        SOCIAL_PROOF_PLATEAU_THRESHOLD as f32
    };
    let growth_factor = DEFAULT_SOCIAL_PROOF_ALPHA.mul_add(growth_n.ln_1p(), 1.0);

    let taper_excess = if global_interactions_count > SOCIAL_PROOF_PLATEAU_THRESHOLD {
        (global_interactions_count - SOCIAL_PROOF_PLATEAU_THRESHOLD) as f32
    } else {
        0.0
    };
    let taper_factor = 1.0
        / DEFAULT_SOCIAL_PROOF_LAMBDA.mul_add(
            (taper_excess / (SOCIAL_PROOF_PLATEAU_THRESHOLD as f32)).ln_1p(),
            1.0,
        );

    baseline_ratio * growth_factor * taper_factor
}

/// Computes the social proof quality curve factor for a candidate post.
///
/// Alias for [`calculate_social_proof_factor`] maintained for backward compatibility.
#[must_use]
#[inline]
pub fn calculate_popularity_dampener(global_interactions_count: usize) -> f32 {
    calculate_social_proof_factor(global_interactions_count)
}

/// Computes the multi-curator consensus multiplier for a candidate post endorsed by $k$ taste twins.
///
/// Formula:
/// $$\text{ConsensusBoost}(k) = 1.0 + \mu \cdot \ln(k), \quad \mu = 0.45$$
///
/// where for $k \le 1$, returns `1.0`.
///
/// Numerical profile:
/// - $k \le 1 \implies 1.000$ (single-curator baseline)
/// - $k = 2 \implies 1.0 + 0.45 \cdot \ln(2) \approx 1.312$ (+31.2% boost)
/// - $k = 3 \implies 1.0 + 0.45 \cdot \ln(3) \approx 1.494$ (+49.4% boost)
/// - $k = 10 \implies 1.0 + 0.45 \cdot \ln(10) \approx 2.036$ (+103.6% boost)
#[must_use]
pub fn calculate_consensus_boost(curator_count: usize) -> f32 {
    if curator_count <= 1 {
        1.0
    } else {
        DEFAULT_CONSENSUS_BOOST_MU.mul_add((curator_count as f32).ln(), 1.0)
    }
}

/// Default Bayesian shrinkage smoothing parameter beta ($\beta = 3.0$).
pub const DEFAULT_BAYESIAN_BETA: f32 = 3.0;

/// Minimum shared interaction overlap required to qualify as a valid co-interactor / taste twin.
pub const MIN_SHARED_OVERLAP: usize = 2;

/// Computes Bayesian statistical confidence shrinkage factor for a given shared interaction count.
///
/// Formula:
/// $$\text{Shrinkage}(S, \beta) = \frac{S}{S + \beta}$$
///
/// where $S = \text{shared\_count}$, and $\beta$ is a smoothing parameter (default 3.0).
/// When $S = 0$, returns `0.0`. If $\beta \le 0.0$, defaults to [`DEFAULT_BAYESIAN_BETA`].
#[must_use]
pub fn calculate_bayesian_shrinkage(shared_count: usize, beta: f32) -> f32 {
    if shared_count == 0 {
        0.0
    } else {
        let s = shared_count as f32;
        let b = if beta <= 0.0 {
            DEFAULT_BAYESIAN_BETA
        } else {
            beta
        };
        s / (s + b)
    }
}

/// Computes Bayesian shrunk taste similarity confidence:
///
/// Formula:
/// $$\text{Confidence}(u, v) = \text{Cosine}(u, v) \times \frac{|\text{SharedLikes}(u, v)|}{|\text{SharedLikes}(u, v)| + \beta}$$
#[must_use]
pub fn calculate_bayesian_confidence(raw_cosine: f32, shared_count: usize, beta: f32) -> f32 {
    raw_cosine * calculate_bayesian_shrinkage(shared_count, beta)
}

/// High-performance multi-signal in-memory bipartite graph store.
///
/// Manages:
/// - Forward user->post interactions (`Vec<CompactEdge>`)
/// - Reverse post->user interactions (`Vec<CompactEdge>`)
/// - User interaction sets using [`RoaringBitmap`] for rapid Jaccard/Cosine taste similarity
/// - Directed follow relationships (`follower_id -> Vec<u32>`)
/// - Post metadata index (`post_id -> PostMeta`)
/// - Author post index (`author_id -> Vec<u32>`)
/// - 6-hour high-velocity sliding pool for cold start
#[derive(Debug)]
pub struct GraphStore {
    /// Forward adjacency: `user_id -> Vec<CompactEdge>`
    user_interactions: [RwLock<AHashMap<u32, Vec<CompactEdge>>>; NUM_SHARDS],
    /// Reverse adjacency: `post_id -> Vec<CompactEdge>`
    post_interactions: [RwLock<AHashMap<u32, Vec<CompactEdge>>>; NUM_SHARDS],
    /// User interaction bitmap for rapid set intersections and similarity
    user_likes_bitmaps: [RwLock<AHashMap<u32, RoaringBitmap>>; NUM_SHARDS],
    /// Follow graph: `follower_id -> Vec<u32>`
    follows: [RwLock<AHashMap<u32, Vec<u32>>>; NUM_SHARDS],
    /// Post metadata: `post_id -> PostMeta`
    post_metadata: [RwLock<AHashMap<u32, PostMeta>>; NUM_SHARDS],
    /// Author posts: `author_id -> Vec<u32>`
    author_posts: [RwLock<AHashMap<u32, Vec<u32>>>; NUM_SHARDS],
    /// Active recent posts tracker for the 6-hour sliding pool: `post_id -> latest_timestamp`
    active_recent_posts: [RwLock<AHashMap<u32, u64>>; NUM_SHARDS],
    /// Monotonic mutation counter for cache invalidation
    mutation_counter: AtomicU64,
    /// Cached velocity pool candidates
    velocity_cache: RwLock<Option<VelocityCandidateCache>>,
    /// Chronological circular ring buffer of recent post interactions: `(timestamp_secs, post_id)`
    recent_active_log: RwLock<RecentActiveRingBuffer>,
}

impl Default for GraphStore {
    fn default() -> Self {
        Self::new()
    }
}

impl GraphStore {
    /// Creates a new, empty [`GraphStore`] with sharded concurrency.
    #[must_use]
    pub fn new() -> Self {
        Self {
            user_interactions: std::array::from_fn(|_| RwLock::new(AHashMap::new())),
            post_interactions: std::array::from_fn(|_| RwLock::new(AHashMap::new())),
            user_likes_bitmaps: std::array::from_fn(|_| RwLock::new(AHashMap::new())),
            follows: std::array::from_fn(|_| RwLock::new(AHashMap::new())),
            post_metadata: std::array::from_fn(|_| RwLock::new(AHashMap::new())),
            author_posts: std::array::from_fn(|_| RwLock::new(AHashMap::new())),
            active_recent_posts: std::array::from_fn(|_| RwLock::new(AHashMap::new())),
            mutation_counter: AtomicU64::new(0),
            velocity_cache: RwLock::new(None),
            recent_active_log: RwLock::new(RecentActiveRingBuffer::default()),
        }
    }

    /// Records a multi-signal interaction from a user on a post.
    ///
    /// Updates forward adjacency, reverse adjacency, the user's Roaring Bitmap,
    /// and the 6-hour high-velocity active tracker.
    pub fn record_interaction(
        &self,
        user_id: u32,
        post_id: u32,
        signal: SignalType,
        timestamp: u64,
    ) {
        let forward_edge = CompactEdge::new(post_id, signal, timestamp);
        let reverse_edge = CompactEdge::new(user_id, signal, timestamp);

        // 1. Forward adjacency
        {
            let u_shard = shard_idx(user_id);
            let mut guard = self.user_interactions[u_shard].write();
            let edges = guard.entry(user_id).or_default();
            // Avoid duplicate exact interaction if already present
            if !edges
                .iter()
                .any(|e| e.target == post_id && e.signal() == signal)
            {
                edges.push(forward_edge);
            }
        }

        // 2. Reverse adjacency
        {
            let p_shard = shard_idx(post_id);
            let mut guard = self.post_interactions[p_shard].write();
            let edges = guard.entry(post_id).or_default();
            if !edges
                .iter()
                .any(|e| e.target == user_id && e.signal() == signal)
            {
                edges.push(reverse_edge);
            }
        }

        // 3. User interaction Roaring Bitmap
        {
            let u_shard = shard_idx(user_id);
            let mut guard = self.user_likes_bitmaps[u_shard].write();
            guard.entry(user_id).or_default().insert(post_id);
        }

        // 4. Active recent posts tracking for velocity pool
        {
            let p_shard = shard_idx(post_id);
            let mut guard = self.active_recent_posts[p_shard].write();
            let entry = guard.entry(post_id).or_insert(timestamp);
            if timestamp > *entry {
                *entry = timestamp;
            }
        }

        self.mutation_counter.fetch_add(1, Ordering::Relaxed);
        self.recent_active_log.write().push(timestamp, post_id);
    }

    /// Removes an interaction (e.g. like deletion or un-repost).
    pub fn remove_interaction(&self, user_id: u32, post_id: u32, signal: SignalType) {
        // Forward
        {
            let u_shard = shard_idx(user_id);
            let mut guard = self.user_interactions[u_shard].write();
            if let Some(edges) = guard.get_mut(&user_id) {
                edges.retain(|e| !(e.target == post_id && e.signal() == signal));
            }
        }

        // Reverse
        {
            let p_shard = shard_idx(post_id);
            let mut guard = self.post_interactions[p_shard].write();
            if let Some(edges) = guard.get_mut(&post_id) {
                edges.retain(|e| !(e.target == user_id && e.signal() == signal));
            }
        }

        // Bitmap: check if user has other remaining interactions on this post
        {
            let u_shard = shard_idx(user_id);
            let forward_guard = self.user_interactions[u_shard].read();
            let has_other = forward_guard
                .get(&user_id)
                .is_some_and(|edges| edges.iter().any(|e| e.target == post_id));

            if !has_other {
                let mut bm_guard = self.user_likes_bitmaps[u_shard].write();
                if let Some(bm) = bm_guard.get_mut(&user_id) {
                    bm.remove(post_id);
                }
            }
        }

        self.mutation_counter.fetch_add(1, Ordering::Relaxed);
    }

    /// Records a directed follow relationship (`follower_id -> followed_id`).
    pub fn record_follow(&self, follower_id: u32, followed_id: u32) {
        let shard = shard_idx(follower_id);
        let mut guard = self.follows[shard].write();
        let list = guard.entry(follower_id).or_default();
        if !list.contains(&followed_id) {
            list.push(followed_id);
        }
        self.mutation_counter.fetch_add(1, Ordering::Relaxed);
    }

    /// Removes a follow relationship (`follower_id -> followed_id`).
    pub fn remove_follow(&self, follower_id: u32, followed_id: u32) {
        let shard = shard_idx(follower_id);
        let mut guard = self.follows[shard].write();
        if let Some(list) = guard.get_mut(&follower_id) {
            list.retain(|&id| id != followed_id);
        }
        self.mutation_counter.fetch_add(1, Ordering::Relaxed);
    }

    /// Records metadata for a post and updates the author posts index.
    pub fn record_post_meta(
        &self,
        post_id: u32,
        author_id: u32,
        root_id: Option<u32>,
        parent_id: Option<u32>,
        created_at: u64,
    ) {
        let meta = PostMeta::new(author_id, root_id, parent_id, created_at);

        // 1. Post metadata
        {
            let p_shard = shard_idx(post_id);
            let mut guard = self.post_metadata[p_shard].write();
            guard.insert(post_id, meta);
        }

        // 2. Author posts
        {
            let a_shard = shard_idx(author_id);
            let mut guard = self.author_posts[a_shard].write();
            let posts = guard.entry(author_id).or_default();
            if !posts.contains(&post_id) {
                posts.push(post_id);
            }
        }
        self.mutation_counter.fetch_add(1, Ordering::Relaxed);
    }

    /// Retrieves a cloned [`RoaringBitmap`] of all post IDs interacted with by the user.
    #[must_use]
    pub fn get_user_likes_bitmap(&self, user_id: u32) -> Option<RoaringBitmap> {
        let shard = shard_idx(user_id);
        let guard = self.user_likes_bitmaps[shard].read();
        guard.get(&user_id).cloned()
    }

    /// Executes a closure with a borrowed slice of forward interactions for the specified user,
    /// avoiding heap vector allocations on the query hot path.
    pub fn with_user_interactions<F, R>(&self, user_id: u32, f: F) -> R
    where
        F: FnOnce(&[CompactEdge]) -> R,
    {
        let shard = shard_idx(user_id);
        let guard = self.user_interactions[shard].read();
        let slice = guard.get(&user_id).map_or(&[][..], |v| &v[..]);
        f(slice)
    }

    /// Executes a closure with a borrowed slice of reverse interactions for the specified post,
    /// avoiding heap vector allocations on the query hot path.
    pub fn with_post_interactions<F, R>(&self, post_id: u32, f: F) -> R
    where
        F: FnOnce(&[CompactEdge]) -> R,
    {
        let shard = shard_idx(post_id);
        let guard = self.post_interactions[shard].read();
        let slice = guard.get(&post_id).map_or(&[][..], |v| &v[..]);
        f(slice)
    }

    /// Executes a closure with a reference to the user's likes [`RoaringBitmap`], if present,
    /// eliminating bitmap cloning overhead on the query hot path.
    pub fn with_user_likes_bitmap<F, R>(&self, user_id: u32, f: F) -> R
    where
        F: FnOnce(Option<&RoaringBitmap>) -> R,
    {
        let shard = shard_idx(user_id);
        let guard = self.user_likes_bitmaps[shard].read();
        f(guard.get(&user_id))
    }

    /// Executes a closure with a borrowed slice of followed user IDs for the specified user.
    pub fn with_user_follows<F, R>(&self, user_id: u32, f: F) -> R
    where
        F: FnOnce(&[u32]) -> R,
    {
        let shard = shard_idx(user_id);
        let guard = self.follows[shard].read();
        let slice = guard.get(&user_id).map_or(&[][..], |v| &v[..]);
        f(slice)
    }

    /// Executes a closure with a borrowed slice of authored post IDs for the specified author.
    pub fn with_author_posts<F, R>(&self, author_id: u32, f: F) -> R
    where
        F: FnOnce(&[u32]) -> R,
    {
        let shard = shard_idx(author_id);
        let guard = self.author_posts[shard].read();
        let slice = guard.get(&author_id).map_or(&[][..], |v| &v[..]);
        f(slice)
    }

    /// Retrieves all forward interactions recorded for a user.
    #[must_use]
    pub fn get_user_interactions(&self, user_id: u32) -> Vec<CompactEdge> {
        let shard = shard_idx(user_id);
        let guard = self.user_interactions[shard].read();
        guard.get(&user_id).cloned().unwrap_or_default()
    }

    /// Retrieves all reverse interactions recorded for a post.
    #[must_use]
    pub fn get_post_interactions(&self, post_id: u32) -> Vec<CompactEdge> {
        let shard = shard_idx(post_id);
        let guard = self.post_interactions[shard].read();
        guard.get(&post_id).cloned().unwrap_or_default()
    }

    /// Retrieves the count of interactions on a post.
    #[must_use]
    pub fn get_post_interaction_count(&self, post_id: u32) -> usize {
        let shard = shard_idx(post_id);
        let guard = self.post_interactions[shard].read();
        guard.get(&post_id).map_or(0, Vec::len)
    }

    /// Retrieves all user IDs followed by the specified follower.
    #[must_use]
    pub fn get_user_follows(&self, user_id: u32) -> Vec<u32> {
        let shard = shard_idx(user_id);
        let guard = self.follows[shard].read();
        guard.get(&user_id).cloned().unwrap_or_default()
    }

    /// Retrieves metadata for the given post.
    #[must_use]
    pub fn get_post_meta(&self, post_id: u32) -> Option<PostMeta> {
        let shard = shard_idx(post_id);
        let guard = self.post_metadata[shard].read();
        guard.get(&post_id).cloned()
    }

    /// Retrieves all post IDs authored by the given user.
    #[must_use]
    pub fn get_author_posts(&self, author_id: u32) -> Vec<u32> {
        let shard = shard_idx(author_id);
        let guard = self.author_posts[shard].read();
        guard.get(&author_id).cloned().unwrap_or_default()
    }

    /// Computes Jaccard taste similarity between two users based on their Roaring Bitmaps.
    ///
    /// $$J(A, B) = \frac{|A \cap B|}{|A \cup B|}$$
    #[must_use]
    pub fn compute_jaccard_similarity(&self, user_a: u32, user_b: u32) -> f32 {
        if user_a == user_b {
            return 1.0;
        }

        let shard_a = shard_idx(user_a);
        let shard_b = shard_idx(user_b);

        if shard_a == shard_b {
            let guard = self.user_likes_bitmaps[shard_a].read();
            match (guard.get(&user_a), guard.get(&user_b)) {
                (Some(a), Some(b)) => {
                    let union_len = a.union_len(b);
                    if union_len == 0 {
                        0.0
                    } else {
                        let inter_len = a.intersection_len(b);
                        inter_len as f32 / union_len as f32
                    }
                }
                _ => 0.0,
            }
        } else {
            let (first, second) = if shard_a < shard_b {
                (shard_a, shard_b)
            } else {
                (shard_b, shard_a)
            };
            let guard_first = self.user_likes_bitmaps[first].read();
            let guard_second = self.user_likes_bitmaps[second].read();
            let bm_a = if shard_a == first {
                guard_first.get(&user_a)
            } else {
                guard_second.get(&user_a)
            };
            let bm_b = if shard_b == first {
                guard_first.get(&user_b)
            } else {
                guard_second.get(&user_b)
            };

            match (bm_a, bm_b) {
                (Some(a), Some(b)) => {
                    let union_len = a.union_len(b);
                    if union_len == 0 {
                        0.0
                    } else {
                        let inter_len = a.intersection_len(b);
                        inter_len as f32 / union_len as f32
                    }
                }
                _ => 0.0,
            }
        }
    }

    /// Computes Cosine taste similarity between two users based on their Roaring Bitmaps.
    ///
    /// $$\text{Cosine}(A, B) = \frac{|A \cap B|}{\sqrt{|A| \cdot |B|}}$$
    #[must_use]
    pub fn compute_cosine_similarity(&self, user_a: u32, user_b: u32) -> f32 {
        if user_a == user_b {
            return 1.0;
        }

        let shard_a = shard_idx(user_a);
        let shard_b = shard_idx(user_b);

        if shard_a == shard_b {
            let guard = self.user_likes_bitmaps[shard_a].read();
            match (guard.get(&user_a), guard.get(&user_b)) {
                (Some(a), Some(b)) => {
                    let len_a = a.len() as f32;
                    let len_b = b.len() as f32;
                    if len_a == 0.0 || len_b == 0.0 {
                        0.0
                    } else {
                        let inter_len = a.intersection_len(b) as f32;
                        inter_len / (len_a * len_b).sqrt()
                    }
                }
                _ => 0.0,
            }
        } else {
            let (first, second) = if shard_a < shard_b {
                (shard_a, shard_b)
            } else {
                (shard_b, shard_a)
            };
            let guard_first = self.user_likes_bitmaps[first].read();
            let guard_second = self.user_likes_bitmaps[second].read();
            let bm_a = if shard_a == first {
                guard_first.get(&user_a)
            } else {
                guard_second.get(&user_a)
            };
            let bm_b = if shard_b == first {
                guard_first.get(&user_b)
            } else {
                guard_second.get(&user_b)
            };

            match (bm_a, bm_b) {
                (Some(a), Some(b)) => {
                    let len_a = a.len() as f32;
                    let len_b = b.len() as f32;
                    if len_a == 0.0 || len_b == 0.0 {
                        0.0
                    } else {
                        let inter_len = a.intersection_len(b) as f32;
                        inter_len / (len_a * len_b).sqrt()
                    }
                }
                _ => 0.0,
            }
        }
    }

    /// Computes Bayesian confidence-shrunk Cosine taste similarity between two users based on their Roaring Bitmaps.
    ///
    /// Formula:
    /// $$\text{Confidence}(A, B) = \text{Cosine}(A, B) \times \frac{|A \cap B|}{|A \cap B| + \beta}$$
    ///
    /// If `user_a == user_b`, returns `1.0`.
    #[must_use]
    pub fn compute_bayesian_cosine_similarity(&self, user_a: u32, user_b: u32, beta: f32) -> f32 {
        if user_a == user_b {
            return 1.0;
        }

        let shard_a = shard_idx(user_a);
        let shard_b = shard_idx(user_b);

        if shard_a == shard_b {
            let guard = self.user_likes_bitmaps[shard_a].read();
            match (guard.get(&user_a), guard.get(&user_b)) {
                (Some(a), Some(b)) => {
                    let len_a = a.len() as f32;
                    let len_b = b.len() as f32;
                    if len_a == 0.0 || len_b == 0.0 {
                        0.0
                    } else {
                        let inter_len = a.intersection_len(b);
                        let raw_cosine = (inter_len as f32) / (len_a * len_b).sqrt();
                        calculate_bayesian_confidence(raw_cosine, inter_len as usize, beta)
                    }
                }
                _ => 0.0,
            }
        } else {
            let (first, second) = if shard_a < shard_b {
                (shard_a, shard_b)
            } else {
                (shard_b, shard_a)
            };
            let guard_first = self.user_likes_bitmaps[first].read();
            let guard_second = self.user_likes_bitmaps[second].read();
            let bm_a = if shard_a == first {
                guard_first.get(&user_a)
            } else {
                guard_second.get(&user_a)
            };
            let bm_b = if shard_b == first {
                guard_first.get(&user_b)
            } else {
                guard_second.get(&user_b)
            };

            match (bm_a, bm_b) {
                (Some(a), Some(b)) => {
                    let len_a = a.len() as f32;
                    let len_b = b.len() as f32;
                    if len_a == 0.0 || len_b == 0.0 {
                        0.0
                    } else {
                        let inter_len = a.intersection_len(b);
                        let raw_cosine = (inter_len as f32) / (len_a * len_b).sqrt();
                        calculate_bayesian_confidence(raw_cosine, inter_len as usize, beta)
                    }
                }
                _ => 0.0,
            }
        }
    }

    /// Retrieves candidate post IDs from the 6-hour high-velocity pool using the latest known timestamp.
    #[must_use]
    pub fn get_velocity_pool_candidates(&self, limit: usize) -> Vec<u32> {
        let max_time = self.get_latest_interaction_timestamp();
        self.get_velocity_pool_candidates_at(max_time, limit)
    }

    /// Retrieves candidate post IDs from the 6-hour high-velocity pool evaluated at `current_time_secs`.
    ///
    /// Scores each candidate post by summing decayed signal weights of all interactions
    /// that occurred within the 6-hour window ($[t_{\text{current}} - 21600, t_{\text{current}}]$),
    /// multiplied by the social proof quality curve.
    ///
    /// Utilizes a sliding active log and fast memoized cache for sub-millisecond query evaluation.
    #[must_use]
    pub fn get_velocity_pool_candidates_at(
        &self,
        current_time_secs: u64,
        limit: usize,
    ) -> Vec<u32> {
        if limit == 0 {
            return Vec::new();
        }

        let current_mutation = self.mutation_counter.load(Ordering::Relaxed);

        // 1. Fast path: check velocity cache
        {
            let cache_guard = self.velocity_cache.read();
            if let Some(ref cache) = *cache_guard {
                if cache.current_time_secs == current_time_secs
                    && cache.mutation_count == current_mutation
                    && cache.limit >= limit
                {
                    if cache.candidates.len() <= limit {
                        return cache.candidates.clone();
                    }
                    return cache.candidates[..limit].to_vec();
                }
            }
        }

        // 2. Slow path: evaluate velocity pool for candidate posts in sliding window
        let window_start = current_time_secs.saturating_sub(SIX_HOURS_SECS);

        let mut candidate_pids = AHashSet::new();
        {
            let log_guard = self.recent_active_log.read();
            if log_guard.is_empty() {
                // Fallback: scan active_recent_posts shards if log is empty
                for shard in &self.active_recent_posts {
                    let guard = shard.read();
                    for (&pid, &ts) in guard.iter() {
                        if ts >= window_start && ts <= current_time_secs {
                            candidate_pids.insert(pid);
                        }
                    }
                }
            } else {
                log_guard.collect_candidates(window_start, current_time_secs, &mut candidate_pids);
            }
        }

        let mut scored_candidates: Vec<(u32, f32)> = Vec::with_capacity(candidate_pids.len());

        for pid in candidate_pids {
            let p_shard = shard_idx(pid);
            let post_guard = self.post_interactions[p_shard].read();
            if let Some(edges) = post_guard.get(&pid) {
                let mut post_score = 0.0f32;
                let mut recent_count = 0usize;

                for edge in edges {
                    let edge_ts = edge.timestamp_secs();
                    if edge_ts >= window_start && edge_ts <= current_time_secs {
                        let decay_weight = calculate_time_decay(
                            edge.signal(),
                            edge_ts,
                            current_time_secs,
                            SIX_HOURS_SECS as f32,
                        );
                        post_score += decay_weight;
                        recent_count += 1;
                    }
                }

                if recent_count > 0 {
                    let social_proof = calculate_social_proof_factor(edges.len());
                    let final_score = post_score * social_proof;
                    scored_candidates.push((pid, final_score));
                }
            }
        }

        // Sort descending by score, tie-break by post_id ascending
        scored_candidates.sort_by(|a, b| {
            b.1.partial_cmp(&a.1)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| a.0.cmp(&b.0))
        });

        let all_result: Vec<u32> = scored_candidates.into_iter().map(|(pid, _)| pid).collect();

        // Update cache (store up to max(limit, 100))
        {
            let mut cache_guard = self.velocity_cache.write();
            *cache_guard = Some(VelocityCandidateCache {
                current_time_secs,
                limit: limit.max(all_result.len()),
                mutation_count: current_mutation,
                candidates: all_result.clone(),
            });
        }

        if all_result.len() > limit {
            all_result[..limit].to_vec()
        } else {
            all_result
        }
    }

    /// Returns the latest interaction timestamp recorded in the graph.
    #[must_use]
    pub fn get_latest_interaction_timestamp(&self) -> u64 {
        let mut max_ts = crate::types::BLUESKY_EPOCH_SECS;
        for shard in &self.active_recent_posts {
            let guard = shard.read();
            for &ts in guard.values() {
                if ts > max_ts {
                    max_ts = ts;
                }
            }
        }
        max_ts
    }

    /// Prunes stale interaction edges and metadata older than `cutoff_timestamp_secs`.
    pub fn prune_older_than(&self, cutoff_timestamp_secs: u64) {
        // Prune forward interactions and remove empty entries
        for shard in &self.user_interactions {
            let mut guard = shard.write();
            for edges in guard.values_mut() {
                edges.retain(|e| e.timestamp_secs() >= cutoff_timestamp_secs);
            }
            guard.retain(|_, edges| !edges.is_empty());
        }

        // Prune reverse interactions and remove empty entries
        for shard in &self.post_interactions {
            let mut guard = shard.write();
            for edges in guard.values_mut() {
                edges.retain(|e| e.timestamp_secs() >= cutoff_timestamp_secs);
            }
            guard.retain(|_, edges| !edges.is_empty());
        }

        // Prune active recent posts
        for shard in &self.active_recent_posts {
            let mut guard = shard.write();
            guard.retain(|_, &mut ts| ts >= cutoff_timestamp_secs);
        }

        // Prune stale post metadata entries
        for shard in &self.post_metadata {
            let mut guard = shard.write();
            guard.retain(|_, meta| meta.created_at >= cutoff_timestamp_secs);
        }

        self.mutation_counter.fetch_add(1, Ordering::Relaxed);
        *self.velocity_cache.write() = None;
    }

    /// Returns aggregated statistics for the graph store.
    #[must_use]
    pub fn stats(&self) -> GraphStats {
        let mut total_users = 0;
        let mut total_interactions = 0;
        let mut total_posts = 0;
        let mut total_follows = 0;
        let mut total_metadata_entries = 0;
        let mut active_velocity_posts = 0;

        for shard in &self.user_interactions {
            let guard = shard.read();
            total_users += guard.len();
            for edges in guard.values() {
                total_interactions += edges.len();
            }
        }

        for shard in &self.post_interactions {
            let guard = shard.read();
            total_posts += guard.len();
        }

        for shard in &self.follows {
            let guard = shard.read();
            for list in guard.values() {
                total_follows += list.len();
            }
        }

        for shard in &self.post_metadata {
            let guard = shard.read();
            total_metadata_entries += guard.len();
        }

        for shard in &self.active_recent_posts {
            let guard = shard.read();
            active_velocity_posts += guard.len();
        }

        GraphStats {
            total_users,
            total_posts,
            total_interactions,
            total_follows,
            total_metadata_entries,
            active_velocity_posts,
        }
    }

    /// Returns aggregated statistics for the graph store.
    ///
    /// Alias for [`GraphStore::stats`].
    #[must_use]
    pub fn get_stats(&self) -> GraphStats {
        self.stats()
    }

    /// Returns the estimated heap memory footprint in bytes across all 64 shards.
    #[must_use]
    pub fn estimated_size_bytes(&self) -> usize {
        let mut total = std::mem::size_of::<Self>();
        for shard in 0..NUM_SHARDS {
            let u_guard = self.user_interactions[shard].read();
            total += u_guard.capacity() * 32;
            for edges in u_guard.values() {
                total += edges.capacity() * std::mem::size_of::<CompactEdge>();
            }

            let p_guard = self.post_interactions[shard].read();
            total += p_guard.capacity() * 32;
            for edges in p_guard.values() {
                total += edges.capacity() * std::mem::size_of::<CompactEdge>();
            }

            let b_guard = self.user_likes_bitmaps[shard].read();
            total += b_guard.capacity() * 32;
            for bm in b_guard.values() {
                total += bm.serialized_size();
            }

            let f_guard = self.follows[shard].read();
            total += f_guard.capacity() * 32;
            for follows in f_guard.values() {
                total += follows.capacity() * 4;
            }

            let m_guard = self.post_metadata[shard].read();
            total += m_guard.capacity() * (32 + std::mem::size_of::<PostMeta>());

            let a_guard = self.author_posts[shard].read();
            total += a_guard.capacity() * 32;
            for posts in a_guard.values() {
                total += posts.capacity() * 4;
            }

            let r_guard = self.active_recent_posts[shard].read();
            total += r_guard.capacity() * (32 + std::mem::size_of::<(u32, u64)>());
        }
        total
    }

    /// Clears all graph data (useful for test setup/teardown).
    pub fn clear(&self) {
        for shard in &self.user_interactions {
            shard.write().clear();
        }
        for shard in &self.post_interactions {
            shard.write().clear();
        }
        for shard in &self.user_likes_bitmaps {
            shard.write().clear();
        }
        for shard in &self.follows {
            shard.write().clear();
        }
        for shard in &self.post_metadata {
            shard.write().clear();
        }
        for shard in &self.author_posts {
            shard.write().clear();
        }
        for shard in &self.active_recent_posts {
            shard.write().clear();
        }
        self.mutation_counter.fetch_add(1, Ordering::Relaxed);
        self.recent_active_log.write().clear();
        *self.velocity_cache.write() = None;
    }

    /// Extracts a complete snapshot of all graph data across all 64 shards.
    #[must_use]
    pub fn snapshot_data(&self) -> GraphSnapshotData {
        let mut user_interactions = Vec::new();
        for shard in &self.user_interactions {
            let guard = shard.read();
            for (&uid, edges) in guard.iter() {
                if !edges.is_empty() {
                    user_interactions.push((uid, edges.clone()));
                }
            }
        }

        let mut post_interactions = Vec::new();
        for shard in &self.post_interactions {
            let guard = shard.read();
            for (&pid, edges) in guard.iter() {
                if !edges.is_empty() {
                    post_interactions.push((pid, edges.clone()));
                }
            }
        }

        let mut user_likes_bitmaps = Vec::new();
        for shard in &self.user_likes_bitmaps {
            let guard = shard.read();
            for (&uid, bm) in guard.iter() {
                if !bm.is_empty() {
                    user_likes_bitmaps.push((uid, bm.clone()));
                }
            }
        }

        let mut follows = Vec::new();
        for shard in &self.follows {
            let guard = shard.read();
            for (&fid, list) in guard.iter() {
                if !list.is_empty() {
                    follows.push((fid, list.clone()));
                }
            }
        }

        let mut post_metadata = Vec::new();
        for shard in &self.post_metadata {
            let guard = shard.read();
            for (&pid, meta) in guard.iter() {
                post_metadata.push((pid, meta.clone()));
            }
        }

        let mut active_recent_posts = Vec::new();
        for shard in &self.active_recent_posts {
            let guard = shard.read();
            for (&pid, &ts) in guard.iter() {
                active_recent_posts.push((pid, ts));
            }
        }

        GraphSnapshotData {
            user_interactions,
            post_interactions,
            user_likes_bitmaps,
            follows,
            post_metadata,
            active_recent_posts,
        }
    }

    /// Restores graph state from raw snapshot data, resetting existing state.
    pub fn restore_from_snapshot(&self, data: GraphSnapshotData) {
        // 1. User interactions
        let mut user_shards: [AHashMap<u32, Vec<CompactEdge>>; NUM_SHARDS] =
            std::array::from_fn(|_| AHashMap::new());
        for (uid, edges) in data.user_interactions {
            let s = shard_idx(uid);
            user_shards[s].insert(uid, edges);
        }
        for (s, map) in user_shards.into_iter().enumerate() {
            *self.user_interactions[s].write() = map;
        }

        // 2. Post interactions
        let mut post_shards: [AHashMap<u32, Vec<CompactEdge>>; NUM_SHARDS] =
            std::array::from_fn(|_| AHashMap::new());
        for (pid, edges) in data.post_interactions {
            let s = shard_idx(pid);
            post_shards[s].insert(pid, edges);
        }
        for (s, map) in post_shards.into_iter().enumerate() {
            *self.post_interactions[s].write() = map;
        }

        // 3. User likes bitmaps
        let mut bm_shards: [AHashMap<u32, RoaringBitmap>; NUM_SHARDS] =
            std::array::from_fn(|_| AHashMap::new());
        for (uid, bm) in data.user_likes_bitmaps {
            let s = shard_idx(uid);
            bm_shards[s].insert(uid, bm);
        }
        for (s, map) in bm_shards.into_iter().enumerate() {
            *self.user_likes_bitmaps[s].write() = map;
        }

        // 4. Follows
        let mut follow_shards: [AHashMap<u32, Vec<u32>>; NUM_SHARDS] =
            std::array::from_fn(|_| AHashMap::new());
        for (fid, list) in data.follows {
            let s = shard_idx(fid);
            follow_shards[s].insert(fid, list);
        }
        for (s, map) in follow_shards.into_iter().enumerate() {
            *self.follows[s].write() = map;
        }

        // 5. Post metadata & Author posts
        let mut meta_shards: [AHashMap<u32, PostMeta>; NUM_SHARDS] =
            std::array::from_fn(|_| AHashMap::new());
        let mut author_shards: [AHashMap<u32, Vec<u32>>; NUM_SHARDS] =
            std::array::from_fn(|_| AHashMap::new());
        for (pid, meta) in data.post_metadata {
            let author_id = meta.author_id;
            let a_shard = shard_idx(author_id);
            let posts = author_shards[a_shard].entry(author_id).or_default();
            if !posts.contains(&pid) {
                posts.push(pid);
            }

            let p_shard = shard_idx(pid);
            meta_shards[p_shard].insert(pid, meta);
        }
        for (s, map) in meta_shards.into_iter().enumerate() {
            *self.post_metadata[s].write() = map;
        }
        for (s, map) in author_shards.into_iter().enumerate() {
            *self.author_posts[s].write() = map;
        }

        // 6. Active recent posts
        let mut active_shards: [AHashMap<u32, u64>; NUM_SHARDS] =
            std::array::from_fn(|_| AHashMap::new());
        for (pid, ts) in &data.active_recent_posts {
            let s = shard_idx(*pid);
            active_shards[s].insert(*pid, *ts);
        }
        for (s, map) in active_shards.into_iter().enumerate() {
            *self.active_recent_posts[s].write() = map;
        }

        self.mutation_counter.fetch_add(1, Ordering::Relaxed);
        {
            let mut log = self.recent_active_log.write();
            log.clear();
            for &(pid, ts) in &data.active_recent_posts {
                log.push(ts, pid);
            }
        }
        *self.velocity_cache.write() = None;
    }
}

/// Raw snapshot representation of in-memory graph state for persistence.
#[derive(Debug, Clone, Default)]
pub struct GraphSnapshotData {
    /// Forward adjacency: `(user_id, Vec<CompactEdge>)`
    pub user_interactions: Vec<(u32, Vec<CompactEdge>)>,
    /// Reverse adjacency: `(post_id, Vec<CompactEdge>)`
    pub post_interactions: Vec<(u32, Vec<CompactEdge>)>,
    /// User interaction Roaring Bitmaps: `(user_id, RoaringBitmap)`
    pub user_likes_bitmaps: Vec<(u32, RoaringBitmap)>,
    /// Follow graph: `(follower_id, Vec<u32>)`
    pub follows: Vec<(u32, Vec<u32>)>,
    /// Post metadata: `(post_id, PostMeta)`
    pub post_metadata: Vec<(u32, PostMeta)>,
    /// High-velocity 6-hour active posts: `(post_id, timestamp)`
    pub active_recent_posts: Vec<(u32, u64)>,
}

/// Aggregated graph metrics and diagnostics.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct GraphStats {
    /// Number of distinct users with interactions recorded.
    pub total_users: usize,
    /// Number of distinct posts with interactions recorded.
    pub total_posts: usize,
    /// Total count of interaction edges stored in the forward adjacency list.
    pub total_interactions: usize,
    /// Total count of directed follow edges stored.
    pub total_follows: usize,
    /// Total count of post metadata entries stored.
    pub total_metadata_entries: usize,
    /// Number of posts tracked in the high-velocity window.
    pub active_velocity_posts: usize,
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic, missing_docs)]
mod tests {
    use super::*;
    use crate::types::BLUESKY_EPOCH_SECS;

    #[test]
    fn test_record_and_get_interactions() {
        let graph = GraphStore::new();
        let u1 = 1;
        let p1 = 100;
        let ts = BLUESKY_EPOCH_SECS + 1000;

        graph.record_interaction(u1, p1, SignalType::Like, ts);
        graph.record_interaction(u1, p1, SignalType::Repost, ts + 10);

        let user_edges = graph.get_user_interactions(u1);
        assert_eq!(user_edges.len(), 2);
        assert_eq!(user_edges[0].target(), p1);
        assert_eq!(user_edges[0].signal(), SignalType::Like);
        assert_eq!(user_edges[1].signal(), SignalType::Repost);

        let post_edges = graph.get_post_interactions(p1);
        assert_eq!(post_edges.len(), 2);
        assert_eq!(post_edges[0].target(), u1);

        let bitmap = graph.get_user_likes_bitmap(u1).unwrap();
        assert!(bitmap.contains(p1));
        assert_eq!(bitmap.len(), 1);
    }

    #[test]
    fn test_remove_interaction() {
        let graph = GraphStore::new();
        let u1 = 1;
        let p1 = 100;
        let ts = BLUESKY_EPOCH_SECS + 1000;

        graph.record_interaction(u1, p1, SignalType::Like, ts);
        graph.record_interaction(u1, p1, SignalType::Repost, ts + 10);
        assert_eq!(graph.get_user_interactions(u1).len(), 2);

        graph.remove_interaction(u1, p1, SignalType::Like);
        assert_eq!(graph.get_user_interactions(u1).len(), 1);
        assert_eq!(
            graph.get_user_interactions(u1)[0].signal(),
            SignalType::Repost
        );
        assert!(graph.get_user_likes_bitmap(u1).unwrap().contains(p1));

        graph.remove_interaction(u1, p1, SignalType::Repost);
        assert_eq!(graph.get_user_interactions(u1).len(), 0);
        assert!(!graph.get_user_likes_bitmap(u1).unwrap().contains(p1));
    }

    #[test]
    fn test_follow_graph() {
        let graph = GraphStore::new();
        let u1 = 1;
        let u2 = 2;
        let u3 = 3;

        graph.record_follow(u1, u2);
        graph.record_follow(u1, u3);
        graph.record_follow(u1, u2); // idempotent

        let follows = graph.get_user_follows(u1);
        assert_eq!(follows.len(), 2);
        assert!(follows.contains(&u2));
        assert!(follows.contains(&u3));

        graph.remove_follow(u1, u2);
        let follows_after = graph.get_user_follows(u1);
        assert_eq!(follows_after, vec![u3]);
    }

    #[test]
    fn test_post_metadata_and_author_index() {
        let graph = GraphStore::new();
        let author = 42;
        let p1 = 101;
        let p2 = 102;
        let ts = BLUESKY_EPOCH_SECS + 5000;

        graph.record_post_meta(p1, author, None, None, ts);
        graph.record_post_meta(p2, author, Some(p1), Some(p1), ts + 60);

        let meta1 = graph.get_post_meta(p1).unwrap();
        assert_eq!(meta1.author_id, author);
        assert!(meta1.is_root());

        let meta2 = graph.get_post_meta(p2).unwrap();
        assert_eq!(meta2.author_id, author);
        assert_eq!(meta2.root_id, Some(p1));
        assert_eq!(meta2.parent_id, Some(p1));
        assert!(meta2.is_reply());

        let author_posts = graph.get_author_posts(author);
        assert_eq!(author_posts.len(), 2);
        assert!(author_posts.contains(&p1));
        assert!(author_posts.contains(&p2));
    }

    #[test]
    fn test_taste_similarity_calculations() {
        let graph = GraphStore::new();
        let u1 = 1;
        let u2 = 2;
        let u3 = 3;
        let ts = BLUESKY_EPOCH_SECS + 1000;

        // u1 liked [10, 20, 30, 40]
        for p in [10, 20, 30, 40] {
            graph.record_interaction(u1, p, SignalType::Like, ts);
        }

        // u2 liked [10, 20, 30, 50] (3 common, union = 5)
        for p in [10, 20, 30, 50] {
            graph.record_interaction(u2, p, SignalType::Like, ts);
        }

        // u3 liked [60, 70] (0 common)
        for p in [60, 70] {
            graph.record_interaction(u3, p, SignalType::Like, ts);
        }

        // Jaccard: 3 / 5 = 0.6
        let jaccard_1_2 = graph.compute_jaccard_similarity(u1, u2);
        assert!((jaccard_1_2 - 0.6).abs() < 1e-4);

        // Cosine: 3 / sqrt(4 * 4) = 3 / 4 = 0.75
        let cosine_1_2 = graph.compute_cosine_similarity(u1, u2);
        assert!((cosine_1_2 - 0.75).abs() < 1e-4);

        // Disjoint users
        assert_eq!(graph.compute_jaccard_similarity(u1, u3), 0.0);
        assert_eq!(graph.compute_cosine_similarity(u1, u3), 0.0);

        // Same user
        assert_eq!(graph.compute_jaccard_similarity(u1, u1), 1.0);
        assert_eq!(graph.compute_cosine_similarity(u1, u1), 1.0);
        assert_eq!(
            graph.compute_bayesian_cosine_similarity(u1, u1, DEFAULT_BAYESIAN_BETA),
            1.0
        );

        // Bayesian Cosine: 0.75 * (3 / (3 + 3)) = 0.75 * 0.5 = 0.375
        let bayesian_1_2 = graph.compute_bayesian_cosine_similarity(u1, u2, DEFAULT_BAYESIAN_BETA);
        assert!((bayesian_1_2 - 0.375).abs() < 1e-4);

        // Disjoint users bayesian
        assert_eq!(
            graph.compute_bayesian_cosine_similarity(u1, u3, DEFAULT_BAYESIAN_BETA),
            0.0
        );
    }

    #[test]
    fn test_bayesian_shrinkage_and_confidence_math() {
        // Zero overlap
        assert_eq!(calculate_bayesian_shrinkage(0, 3.0), 0.0);
        assert_eq!(calculate_bayesian_confidence(1.0, 0, 3.0), 0.0);

        // 1 overlap: 1 / (1 + 3) = 0.25 (75% penalty)
        let s1 = calculate_bayesian_shrinkage(1, 3.0);
        assert!((s1 - 0.25).abs() < 1e-5);
        let c1 = calculate_bayesian_confidence(1.0, 1, 3.0);
        assert!((c1 - 0.25).abs() < 1e-5);

        // 2 overlap: 2 / (2 + 3) = 0.40 (60% penalty)
        let s2 = calculate_bayesian_shrinkage(2, 3.0);
        assert!((s2 - 0.40).abs() < 1e-5);
        let raw2 = 0.8f32;
        let c2 = calculate_bayesian_confidence(raw2, 2, 3.0);
        let expected_c2 = raw2 * 0.40;
        assert!((c2 - expected_c2).abs() < 1e-5);

        // 3 overlap: 3 / (3 + 3) = 0.50 (50% penalty)
        let s3 = calculate_bayesian_shrinkage(3, 3.0);
        assert!((s3 - 0.50).abs() < 1e-5);
        let raw3 = 0.6f32;
        let c3 = calculate_bayesian_confidence(raw3, 3, 3.0);
        let expected_c3 = raw3 * 0.50;
        assert!((c3 - expected_c3).abs() < 1e-5);

        // High overlap (50 items): 50 / 53 ≈ 0.9434
        let s50 = calculate_bayesian_shrinkage(50, 3.0);
        assert!((s50 - (50.0 / 53.0)).abs() < 1e-5);

        // Fallback for non-positive beta
        assert!((calculate_bayesian_shrinkage(3, 0.0) - 0.50).abs() < 1e-5);
        assert!((calculate_bayesian_shrinkage(3, -1.5) - 0.50).abs() < 1e-5);
    }

    #[test]
    fn test_exponential_time_decay_math() {
        let event_ts = 1_700_000_000;
        let tau = 36.0 * 3600.0;

        // At event time: decay = exp(0) = 1.0
        let w0 = calculate_time_decay(SignalType::Like, event_ts, event_ts, tau);
        assert_eq!(w0, 1.0);

        let w0_quote = calculate_time_decay(SignalType::Quote, event_ts, event_ts, tau);
        assert_eq!(w0_quote, 2.0);

        let w0_repost = calculate_time_decay(SignalType::Repost, event_ts, event_ts, tau);
        assert_eq!(w0_repost, 3.0);

        // At event time + tau (36 hours): decay = exp(-1) = 0.36787944
        let w_tau = calculate_time_decay(SignalType::Like, event_ts, event_ts + 36 * 3600, tau);
        assert!((w_tau - (-1.0f32).exp()).abs() < 1e-5);

        // Saturated time (event in future): decay = exp(0) = 1.0
        let w_future = calculate_time_decay(SignalType::Like, event_ts + 100, event_ts, tau);
        assert_eq!(w_future, 1.0);
    }

    #[test]
    fn test_social_proof_quality_curve() {
        // N = 0: baseline moderation (1/3)
        let s0 = calculate_social_proof_factor(0);
        assert!((s0 - 1.0 / 3.0).abs() < 1e-5);
        assert!((calculate_popularity_dampener(0) - s0).abs() < f32::EPSILON);

        // N = 3: early community validation (~0.806)
        let s3 = calculate_social_proof_factor(3);
        assert!((s3 - 0.805_296).abs() < 1e-4);

        // N = 10: established post (~1.150)
        let s10 = calculate_social_proof_factor(10);
        assert!((s10 - 1.1505).abs() < 1e-3);

        // N = 50: strong community proof (~1.530)
        let s50 = calculate_social_proof_factor(50);
        assert!((s50 - 1.5298).abs() < 1e-3);

        // N = 500: peak plateau (~1.925)
        let s500 = calculate_social_proof_factor(500);
        assert!((s500 - 1.9248).abs() < 1e-3);

        // N = 5000: smooth logarithmic taper (~1.570)
        let s5000 = calculate_social_proof_factor(5000);
        assert!((s5000 - 1.5702).abs() < 1e-3);

        // Monotonic increase up to threshold (500)
        assert!(s0 < s3);
        assert!(s3 < s10);
        assert!(s10 < s50);
        assert!(s50 < s500);

        // Soft plateau / taper above threshold
        assert!(s500 > s5000);
        assert!(s5000 > 1.0); // Never collapses to zero

        // Extreme bounds check
        let s_10m = calculate_social_proof_factor(10_000_000);
        assert!(!s_10m.is_nan());
        assert!(!s_10m.is_infinite());
        assert!(s_10m > 0.0);
    }

    #[test]
    fn test_multi_curator_consensus_boost() {
        // k <= 1: single curator or empty -> 1.0
        assert_eq!(calculate_consensus_boost(0), 1.0);
        assert_eq!(calculate_consensus_boost(1), 1.0);

        // k = 2: ~1.312 (+31.2% boost)
        let b2 = calculate_consensus_boost(2);
        assert!((b2 - 1.311_916).abs() < 1e-4);

        // k = 3: ~1.494 (+49.4% boost)
        let b3 = calculate_consensus_boost(3);
        assert!((b3 - 1.494_375).abs() < 1e-4);

        // k = 10: ~2.036 (+103.6% boost)
        let b10 = calculate_consensus_boost(10);
        assert!((b10 - 2.036_163).abs() < 1e-4);

        // Monotonic growth for k >= 1
        assert!(calculate_consensus_boost(1) < calculate_consensus_boost(2));
        assert!(calculate_consensus_boost(2) < calculate_consensus_boost(3));
        assert!(calculate_consensus_boost(3) < calculate_consensus_boost(5));
        assert!(calculate_consensus_boost(5) < calculate_consensus_boost(10));
    }

    #[test]
    fn test_velocity_pool_candidates() {
        let graph = GraphStore::new();
        let base_ts = BLUESKY_EPOCH_SECS + 50_000;

        // Post 1: 5 likes recently
        for u in 1..=5 {
            graph.record_interaction(u, 101, SignalType::Like, base_ts - 100);
        }

        // Post 2: 1 like recently
        graph.record_interaction(1, 102, SignalType::Like, base_ts - 50);

        // Post 3: 10 likes 10 hours ago (outside 6-hour window)
        for u in 1..=10 {
            graph.record_interaction(u, 103, SignalType::Like, base_ts - 10 * 3600);
        }

        let candidates = graph.get_velocity_pool_candidates_at(base_ts, 10);
        assert_eq!(candidates.len(), 2);
        assert_eq!(candidates[0], 101); // Post 1 has highest velocity
        assert_eq!(candidates[1], 102); // Post 2 has second
        assert!(!candidates.contains(&103)); // Post 3 was outside window
    }

    #[test]
    fn test_prune_older_than() {
        let graph = GraphStore::new();
        let ts_old = BLUESKY_EPOCH_SECS + 100;
        let ts_new = BLUESKY_EPOCH_SECS + 10_000;

        graph.record_interaction(1, 100, SignalType::Like, ts_old);
        graph.record_interaction(1, 200, SignalType::Like, ts_new);

        assert_eq!(graph.get_user_interactions(1).len(), 2);

        graph.prune_older_than(BLUESKY_EPOCH_SECS + 5_000);

        let edges = graph.get_user_interactions(1);
        assert_eq!(edges.len(), 1);
        assert_eq!(edges[0].target(), 200);
    }

    #[test]
    fn test_stats() {
        let graph = GraphStore::new();
        let ts = BLUESKY_EPOCH_SECS + 100;

        graph.record_interaction(1, 100, SignalType::Like, ts);
        graph.record_follow(1, 2);
        graph.record_post_meta(100, 2, None, None, ts);

        let stats = graph.stats();
        assert_eq!(stats.total_users, 1);
        assert_eq!(stats.total_posts, 1);
        assert_eq!(stats.total_interactions, 1);
        assert_eq!(stats.total_follows, 1);
        assert_eq!(stats.total_metadata_entries, 1);
        assert_eq!(stats.active_velocity_posts, 1);
    }

    #[test]
    fn test_snapshot_data_and_restore() {
        let graph = GraphStore::new();
        let ts = BLUESKY_EPOCH_SECS + 500;

        graph.record_interaction(1, 100, SignalType::Like, ts);
        graph.record_interaction(1, 200, SignalType::Repost, ts + 10);
        graph.record_interaction(2, 100, SignalType::Quote, ts + 20);
        graph.record_follow(1, 2);
        graph.record_follow(2, 3);
        graph.record_post_meta(100, 10, None, None, ts);
        graph.record_post_meta(200, 10, Some(100), Some(100), ts + 5);

        let snap = graph.snapshot_data();
        assert_eq!(snap.user_interactions.len(), 2);
        assert_eq!(snap.post_interactions.len(), 2);
        assert_eq!(snap.user_likes_bitmaps.len(), 2);
        assert_eq!(snap.follows.len(), 2);
        assert_eq!(snap.post_metadata.len(), 2);
        assert_eq!(snap.active_recent_posts.len(), 2);

        let restored = GraphStore::new();
        restored.restore_from_snapshot(snap);

        let stats_orig = graph.stats();
        let stats_rest = restored.stats();
        assert_eq!(stats_orig, stats_rest);

        assert_eq!(restored.get_user_interactions(1).len(), 2);
        assert_eq!(restored.get_user_follows(1), vec![2]);
        assert_eq!(restored.get_author_posts(10).len(), 2);
        assert_eq!(restored.get_post_meta(200).unwrap().root_id, Some(100));
    }
}
