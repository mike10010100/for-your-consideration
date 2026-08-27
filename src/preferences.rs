#![forbid(unsafe_code)]

//! In-memory sharded storage engine for user recommendation preference dials.
//!
//! Provides thread-safe, 64-shard partitioned storage (`[parking_lot::RwLock<AHashMap<u32, UserDials>>; 64]`)
//! mapped by interned user IDs (`u32`).
//!
//! # Performance Characteristics
//!
//! - **Unauthenticated / Unset Fast Path**: Viewer lookups bypass preference shard lock acquisition
//!   entirely if the DID is not present in the [`StringInterner`], returning `None` in < 15 ns.
//! - **Partitioned Concurrency**: 64 lock shards guarantee minimal lock contention under high concurrent load.
//! - **Zero Allocation Lookups**: [`UserDials`] is `Copy` (32 bytes), avoiding heap allocations on read paths.

use ahash::AHashMap;
use parking_lot::RwLock;

use crate::interner::StringInterner;
use crate::types::UserDials;

/// Number of parallel lock shards to minimize lock contention under high concurrency.
pub const PREFERENCE_SHARDS: usize = 64;

/// Returns the shard index for a given interned 32-bit user ID.
#[inline]
#[must_use]
pub const fn shard_idx(user_id: u32) -> usize {
    (user_id as usize) & (PREFERENCE_SHARDS - 1)
}

/// High-performance, 64-shard partitioned in-memory store for user preference dials.
#[derive(Debug)]
pub struct UserPreferencesStore {
    shards: [RwLock<AHashMap<u32, UserDials>>; PREFERENCE_SHARDS],
}

impl Default for UserPreferencesStore {
    fn default() -> Self {
        Self::new()
    }
}

impl Clone for UserPreferencesStore {
    fn clone(&self) -> Self {
        let mut new_shards: [RwLock<AHashMap<u32, UserDials>>; PREFERENCE_SHARDS] =
            std::array::from_fn(|_| RwLock::new(AHashMap::new()));
        for (i, shard) in self.shards.iter().enumerate() {
            *new_shards[i].get_mut() = shard.read().clone();
        }
        Self { shards: new_shards }
    }
}

impl UserPreferencesStore {
    /// Creates a new, empty [`UserPreferencesStore`] with 64 partitioned shards.
    #[must_use]
    pub fn new() -> Self {
        Self {
            shards: std::array::from_fn(|_| RwLock::new(AHashMap::new())),
        }
    }

    /// Retrieves custom dials for a numeric user ID if set.
    #[must_use]
    pub fn get(&self, user_id: u32) -> Option<UserDials> {
        let shard = shard_idx(user_id);
        let guard = self.shards[shard].read();
        guard.get(&user_id).copied()
    }

    /// Retrieves custom dials for a user DID string, or `None` if not saved or uninterned.
    ///
    /// If the DID has never been seen or interned, this returns `None` immediately
    /// without acquiring any preference shard locks.
    #[must_use]
    pub fn get_by_did(&self, interner: &StringInterner, did: &str) -> Option<UserDials> {
        let user_id = interner.lookup_id(did)?;
        self.get(user_id)
    }

    /// Retrieves custom dials for a numeric user ID, falling back to [`UserDials::default()`].
    #[must_use]
    pub fn get_or_default(&self, user_id: u32) -> UserDials {
        self.get(user_id).unwrap_or_default()
    }

    /// Retrieves custom dials for a user DID string, falling back to [`UserDials::default()`].
    #[must_use]
    pub fn get_by_did_or_default(&self, interner: &StringInterner, did: &str) -> UserDials {
        self.get_by_did(interner, did).unwrap_or_default()
    }

    /// Saves or updates custom dials for a numeric user ID.
    pub fn set(&self, user_id: u32, dials: UserDials) {
        let shard = shard_idx(user_id);
        let mut guard = self.shards[shard].write();
        guard.insert(user_id, dials);
    }

    /// Saves or updates custom dials for a DID string, interning the DID if necessary.
    ///
    /// Returns the interned numeric user ID.
    pub fn set_by_did(&self, interner: &StringInterner, did: &str, dials: UserDials) -> u32 {
        let user_id = interner.intern(did);
        self.set(user_id, dials);
        user_id
    }

    /// Removes custom preferences for a numeric user ID, returning previous dials if present.
    pub fn remove(&self, user_id: u32) -> Option<UserDials> {
        let shard = shard_idx(user_id);
        let mut guard = self.shards[shard].write();
        guard.remove(&user_id)
    }

    /// Removes custom preferences for a DID string, returning previous dials if present.
    pub fn remove_by_did(&self, interner: &StringInterner, did: &str) -> Option<UserDials> {
        let user_id = interner.lookup_id(did)?;
        self.remove(user_id)
    }

    /// Deletes custom preferences for a numeric user ID, returning `true` if previously present.
    pub fn delete(&self, user_id: u32) -> bool {
        self.remove(user_id).is_some()
    }

    /// Deletes custom preferences for a DID string, returning `true` if previously present.
    pub fn delete_by_did(&self, interner: &StringInterner, did: &str) -> bool {
        self.remove_by_did(interner, did).is_some()
    }

    /// Returns the total count of saved user preference profiles across all shards.
    #[must_use]
    pub fn len(&self) -> usize {
        self.shards.iter().map(|s| s.read().len()).sum()
    }

    /// Returns `true` if no user preferences are saved.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Clears all preferences across all 64 shards.
    pub fn clear(&self) {
        for shard in &self.shards {
            shard.write().clear();
        }
    }

    /// Returns the estimated in-memory footprint in bytes.
    #[must_use]
    pub fn estimated_size_bytes(&self) -> usize {
        let mut total = std::mem::size_of::<Self>();
        for shard in &self.shards {
            let guard = shard.read();
            total += guard.capacity()
                * (std::mem::size_of::<u32>() + std::mem::size_of::<UserDials>() + 16);
        }
        total
    }

    /// Exports a snapshot clone of all saved preferences across all 64 shards.
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

    /// Restores preference state from snapshot data, completely replacing existing state.
    pub fn restore_from_snapshot(&self, data: Vec<(u32, UserDials)>) {
        let mut new_shards: [AHashMap<u32, UserDials>; PREFERENCE_SHARDS] =
            std::array::from_fn(|_| AHashMap::new());
        for (uid, dials) in data {
            let s = shard_idx(uid);
            new_shards[s].insert(uid, dials);
        }
        for (s, map) in new_shards.into_iter().enumerate() {
            *self.shards[s].write() = map;
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic, missing_docs)]
mod tests {
    use super::*;
    use crate::types::TopicWeights;

    #[test]
    fn test_shard_idx_bounds() {
        for id in 0..1000 {
            assert!(shard_idx(id) < PREFERENCE_SHARDS);
        }
    }

    #[test]
    fn test_store_crud_and_defaults() {
        let store = UserPreferencesStore::new();
        let interner = StringInterner::new();
        assert!(store.is_empty());
        assert_eq!(store.len(), 0);

        // Fallback for unset user
        assert_eq!(store.get(10), None);
        assert_eq!(store.get_or_default(10), UserDials::default());
        assert_eq!(store.get_by_did(&interner, "did:plc:alice"), None);
        assert_eq!(
            store.get_by_did_or_default(&interner, "did:plc:alice"),
            UserDials::default()
        );

        // Set by ID
        let dials1 = UserDials::from_hours(12.0, 0.25, TopicWeights::default(), 100);
        store.set(10, dials1);
        assert_eq!(store.len(), 1);
        assert!(!store.is_empty());
        assert_eq!(store.get(10), Some(dials1));

        // Set by DID
        let dials2 = UserDials::from_hours(48.0, 0.10, TopicWeights::default(), 200);
        let u2_id = store.set_by_did(&interner, "did:plc:bob", dials2);
        assert_eq!(store.len(), 2);
        assert_eq!(store.get_by_did(&interner, "did:plc:bob"), Some(dials2));
        assert_eq!(store.get(u2_id), Some(dials2));

        // Remove by ID
        assert_eq!(store.remove(10), Some(dials1));
        assert_eq!(store.remove(10), None);
        assert_eq!(store.len(), 1);

        // Delete by DID
        assert!(store.delete_by_did(&interner, "did:plc:bob"));
        assert!(!store.delete_by_did(&interner, "did:plc:bob"));
        assert_eq!(store.len(), 0);
        assert!(store.is_empty());
    }

    #[test]
    fn test_snapshot_export_and_restore() {
        let store = UserPreferencesStore::new();
        let mut expected = Vec::new();
        for i in 0..100 {
            let dials =
                UserDials::from_hours(i as f32 + 1.0, 0.15, TopicWeights::default(), u64::from(i));
            store.set(i, dials);
            expected.push((i, dials));
        }

        assert_eq!(store.len(), 100);
        let snap = store.snapshot_data();
        assert_eq!(snap.len(), 100);

        let restored_store = UserPreferencesStore::new();
        restored_store.restore_from_snapshot(snap);
        assert_eq!(restored_store.len(), 100);

        for (id, dials) in expected {
            assert_eq!(restored_store.get(id), Some(dials));
        }

        store.clear();
        assert!(store.is_empty());
    }

    #[test]
    fn test_clone_and_estimated_size() {
        let store = UserPreferencesStore::new();
        store.set(1, UserDials::default());
        let cloned = store.clone();
        assert_eq!(cloned.len(), 1);
        assert_eq!(cloned.get(1), Some(UserDials::default()));
        assert!(store.estimated_size_bytes() > 0);
    }
}
