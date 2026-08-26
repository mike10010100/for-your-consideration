use ahash::AHashMap;
use parking_lot::RwLock;
use roaring::RoaringBitmap;
use serde::{Deserialize, Serialize};

use crate::types::{CompactEdge, PostMeta, SignalType};

/// Number of parallel lock shards to minimize contention under high write/read concurrency.
pub const NUM_SHARDS: usize = 64;

/// 6 hours in seconds (used for Tier 3 cold-start high-velocity window).
pub const SIX_HOURS_SECS: u64 = 6 * 3600;

/// Default half-life in seconds (36 hours).
pub const DEFAULT_HALF_LIFE_SECS: f32 = 36.0 * 3600.0;

/// Returns the shard index for a given 32-bit identifier.
#[inline]
const fn shard_idx(id: u32) -> usize {
    (id as usize) % NUM_SHARDS
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

/// Computes the BM25 inverse degree popularity dampening factor for a candidate post.
///
/// Formula:
/// $$\text{Dampener}(p) = \frac{1}{\sqrt{|\text{GlobalInteractions}(p)| + 1}}$$
///
/// Prevents mega-viral posts from dominating personalized recommendations.
#[must_use]
pub fn calculate_popularity_dampener(global_interactions_count: usize) -> f32 {
    1.0 / ((global_interactions_count as f32) + 1.0).sqrt()
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
    }

    /// Records a directed follow relationship (`follower_id -> followed_id`).
    pub fn record_follow(&self, follower_id: u32, followed_id: u32) {
        let shard = shard_idx(follower_id);
        let mut guard = self.follows[shard].write();
        let list = guard.entry(follower_id).or_default();
        if !list.contains(&followed_id) {
            list.push(followed_id);
        }
    }

    /// Removes a follow relationship (`follower_id -> followed_id`).
    pub fn remove_follow(&self, follower_id: u32, followed_id: u32) {
        let shard = shard_idx(follower_id);
        let mut guard = self.follows[shard].write();
        if let Some(list) = guard.get_mut(&follower_id) {
            list.retain(|&id| id != followed_id);
        }
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
    }

    /// Retrieves a cloned [`RoaringBitmap`] of all post IDs interacted with by the user.
    #[must_use]
    pub fn get_user_likes_bitmap(&self, user_id: u32) -> Option<RoaringBitmap> {
        let shard = shard_idx(user_id);
        let guard = self.user_likes_bitmaps[shard].read();
        guard.get(&user_id).cloned()
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

        let bm_a = self.get_user_likes_bitmap(user_a);
        let bm_b = self.get_user_likes_bitmap(user_b);

        match (bm_a, bm_b) {
            (Some(a), Some(b)) => {
                let union_len = a.union_len(&b);
                if union_len == 0 {
                    0.0
                } else {
                    let inter_len = a.intersection_len(&b);
                    inter_len as f32 / union_len as f32
                }
            }
            _ => 0.0,
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

        let bm_a = self.get_user_likes_bitmap(user_a);
        let bm_b = self.get_user_likes_bitmap(user_b);

        match (bm_a, bm_b) {
            (Some(a), Some(b)) => {
                let len_a = a.len() as f32;
                let len_b = b.len() as f32;
                if len_a == 0.0 || len_b == 0.0 {
                    0.0
                } else {
                    let inter_len = a.intersection_len(&b) as f32;
                    inter_len / (len_a * len_b).sqrt()
                }
            }
            _ => 0.0,
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
    /// divided by the BM25 inverse degree popularity dampener.
    #[must_use]
    pub fn get_velocity_pool_candidates_at(
        &self,
        current_time_secs: u64,
        limit: usize,
    ) -> Vec<u32> {
        if limit == 0 {
            return Vec::new();
        }

        let window_start = current_time_secs.saturating_sub(SIX_HOURS_SECS);

        // Collect all posts active in the sliding window
        let mut candidates: Vec<(u32, f32)> = Vec::new();

        for shard in &self.active_recent_posts {
            let guard = shard.read();
            for (&post_id, &last_ts) in guard.iter() {
                if last_ts >= window_start {
                    // Compute velocity score for this post
                    let p_shard = shard_idx(post_id);
                    let post_guard = self.post_interactions[p_shard].read();
                    if let Some(edges) = post_guard.get(&post_id) {
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
                            let dampener = calculate_popularity_dampener(edges.len());
                            let final_score = post_score * dampener;
                            candidates.push((post_id, final_score));
                        }
                    }
                }
            }
        }

        // Sort descending by score
        candidates.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
        candidates.truncate(limit);
        candidates.into_iter().map(|(pid, _)| pid).collect()
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
        for (pid, ts) in data.active_recent_posts {
            let s = shard_idx(pid);
            active_shards[s].insert(pid, ts);
        }
        for (s, map) in active_shards.into_iter().enumerate() {
            *self.active_recent_posts[s].write() = map;
        }
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
    fn test_bm25_popularity_dampener() {
        assert_eq!(calculate_popularity_dampener(0), 1.0);
        assert!((calculate_popularity_dampener(3) - 0.5).abs() < 1e-5);
        assert!((calculate_popularity_dampener(99) - 0.1).abs() < 1e-5);
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
