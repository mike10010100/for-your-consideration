use ahash::AHashMap;
use compact_str::CompactString;
use parking_lot::RwLock;

/// Total number of independent shards for the [`StringInterner`].
pub const NUM_INTERNER_SHARDS: usize = 64;

/// Computes a deterministic shard index from a string slice using FNV-1a.
#[inline]
fn string_shard_idx(s: &str) -> usize {
    let mut hash: u64 = 0xcbf2_9ce4_8422_2325;
    for byte in s.as_bytes() {
        hash ^= u64::from(*byte);
        hash = hash.wrapping_mul(0x0100_0000_01b3);
    }
    (hash as usize) % NUM_INTERNER_SHARDS
}

/// Decodes a global `u32` ID into its `(shard_idx, local_index)` tuple.
#[inline]
const fn id_to_shard_and_local(id: u32) -> (usize, usize) {
    let shard = (id & 0x3F) as usize;
    let local = (id >> 6) as usize;
    (shard, local)
}

/// Encodes a `(shard_idx, local_index)` tuple into a global `u32` ID.
#[inline]
const fn local_to_id(shard: usize, local: usize) -> u32 {
    ((local as u32) << 6) | (shard as u32)
}

/// Fast, thread-safe bidirectional string interner mapping string identifiers
/// (such as DIDs and AT-URIs) to compact 32-bit integer IDs (`u32`).
///
/// Partitioned into 64 independent [`parking_lot::RwLock`] shards with hash-bucketed routing
/// to eliminate write lock contention during high-throughput concurrent ingestion.
#[derive(Debug)]
pub struct StringInterner {
    shards: [RwLock<InternerShard>; NUM_INTERNER_SHARDS],
}

impl Default for StringInterner {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Debug, Default)]
struct InternerShard {
    to_id: AHashMap<CompactString, u32>,
    to_str: Vec<CompactString>,
}

impl StringInterner {
    /// Creates a new, empty [`StringInterner`] with 64 independent shards.
    #[must_use]
    pub fn new() -> Self {
        Self {
            shards: std::array::from_fn(|_| RwLock::new(InternerShard::default())),
        }
    }

    /// Interns a string slice, returning its unique `u32` ID.
    ///
    /// If the string was already interned, returns the existing ID.
    /// Uses double-checked locking per shard to minimize write-lock contention.
    pub fn intern(&self, s: &str) -> u32 {
        self.get_or_intern(s)
    }

    /// Interns a string slice, returning its unique `u32` ID.
    ///
    /// Alias for [`StringInterner::intern`].
    pub fn get_or_intern(&self, s: &str) -> u32 {
        let shard_idx = string_shard_idx(s);

        // Fast path: optimistic read lock on specific shard
        {
            let guard = self.shards[shard_idx].read();
            if let Some(&id) = guard.to_id.get(s) {
                return id;
            }
        }

        // Slow path: write lock on specific shard with double-checked verification
        let mut guard = self.shards[shard_idx].write();
        if let Some(&id) = guard.to_id.get(s) {
            return id;
        }

        let compact = CompactString::new(s);
        let local_idx = guard.to_str.len();
        let id = local_to_id(shard_idx, local_idx);
        guard.to_str.push(compact.clone());
        guard.to_id.insert(compact, id);
        id
    }

    /// Looks up the `u32` ID for an existing string, if interned.
    #[must_use]
    pub fn lookup_id(&self, s: &str) -> Option<u32> {
        self.get_id(s)
    }

    /// Looks up the `u32` ID for an existing string, if interned.
    ///
    /// Alias for [`StringInterner::lookup_id`].
    #[must_use]
    pub fn get_id(&self, s: &str) -> Option<u32> {
        let shard_idx = string_shard_idx(s);
        let guard = self.shards[shard_idx].read();
        guard.to_id.get(s).copied()
    }

    /// Resolves a `u32` ID back to its string representation.
    #[must_use]
    pub fn lookup_str(&self, id: u32) -> Option<CompactString> {
        self.resolve(id)
    }

    /// Resolves a `u32` ID back to its string representation.
    ///
    /// Alias for [`StringInterner::lookup_str`].
    #[must_use]
    pub fn resolve(&self, id: u32) -> Option<CompactString> {
        let (shard_idx, local_idx) = id_to_shard_and_local(id);
        if shard_idx >= NUM_INTERNER_SHARDS {
            return None;
        }
        let guard = self.shards[shard_idx].read();
        guard.to_str.get(local_idx).cloned()
    }

    /// Returns the total number of interned strings across all shards.
    #[must_use]
    pub fn len(&self) -> usize {
        self.shards
            .iter()
            .map(|shard| shard.read().to_str.len())
            .sum()
    }

    /// Returns `true` if no strings have been interned yet.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns the estimated heap memory footprint in bytes.
    #[must_use]
    pub fn estimated_size_bytes(&self) -> usize {
        let mut total_vec_bytes = 0;
        let mut total_map_bytes = 0;

        for shard in &self.shards {
            let guard = shard.read();
            let strings_len = guard.to_str.len();
            total_vec_bytes += strings_len * std::mem::size_of::<CompactString>();
            total_map_bytes += guard.to_id.capacity()
                * (std::mem::size_of::<CompactString>() + std::mem::size_of::<u32>() + 16);
        }

        std::mem::size_of::<Self>() + total_vec_bytes + total_map_bytes
    }

    /// Clears all interned strings across all shards.
    pub fn clear(&self) {
        for shard in &self.shards {
            let mut guard = shard.write();
            guard.to_id.clear();
            guard.to_str.clear();
        }
    }

    /// Exports a snapshot clone of all interned strings in deterministic shard and index order.
    #[must_use]
    pub fn export_strings(&self) -> Vec<CompactString> {
        let mut all = Vec::with_capacity(self.len());
        for shard in &self.shards {
            let guard = shard.read();
            all.extend(guard.to_str.iter().cloned());
        }
        all
    }

    /// Creates a new [`StringInterner`] pre-populated from an ordered list of strings.
    #[must_use]
    pub fn from_exported_strings(strings: Vec<CompactString>) -> Self {
        let interner = Self::new();
        interner.hydrate_from(strings);
        interner
    }

    /// Replaces the internal state with the provided ordered list of strings.
    pub fn hydrate_from(&self, strings: Vec<CompactString>) {
        self.clear();
        for s in strings {
            self.get_or_intern(&s);
        }
    }
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic, missing_docs)]
mod tests {
    use super::*;
    use std::sync::Arc;
    use std::thread;

    #[test]
    fn test_interner_bidirectional() {
        let interner = StringInterner::new();
        assert!(interner.is_empty());
        assert_eq!(interner.len(), 0);

        let id1 = interner.intern("did:plc:alice");
        let id2 = interner.intern("did:plc:bob");
        let id1_again = interner.intern("did:plc:alice");

        assert_eq!(id1, id1_again);
        assert_ne!(id1, id2);
        assert_eq!(interner.len(), 2);
        assert!(!interner.is_empty());

        assert_eq!(interner.lookup_id("did:plc:alice"), Some(id1));
        assert_eq!(interner.lookup_id("did:plc:bob"), Some(id2));
        assert_eq!(interner.lookup_id("did:plc:charlie"), None);

        assert_eq!(interner.lookup_str(id1).as_deref(), Some("did:plc:alice"));
        assert_eq!(interner.lookup_str(id2).as_deref(), Some("did:plc:bob"));
        assert_eq!(interner.lookup_str(99999), None);

        // Test alias methods
        assert_eq!(interner.get_id("did:plc:alice"), Some(id1));
        assert_eq!(interner.resolve(id1).as_deref(), Some("did:plc:alice"));
    }

    #[test]
    fn test_interner_concurrent_access() {
        let interner = Arc::new(StringInterner::new());
        let mut handles = Vec::new();

        for i in 0..16 {
            let interner = Arc::clone(&interner);
            handles.push(thread::spawn(move || {
                for j in 0..100 {
                    let key = format!("did:plc:user_{j}");
                    let id = interner.intern(&key);
                    assert_eq!(interner.lookup_str(id).as_deref(), Some(key.as_str()));
                }
                let unique_key = format!("did:plc:thread_{i}_unique");
                interner.intern(&unique_key);
            }));
        }

        for handle in handles {
            handle.join().unwrap();
        }

        assert_eq!(interner.len(), 100 + 16);
    }

    #[test]
    fn test_interner_clear() {
        let interner = StringInterner::new();
        interner.intern("test1");
        interner.intern("test2");
        assert_eq!(interner.len(), 2);

        interner.clear();
        assert_eq!(interner.len(), 0);
        assert!(interner.is_empty());
        assert_eq!(interner.lookup_id("test1"), None);
    }

    #[test]
    fn test_interner_export_and_hydration() {
        let interner = StringInterner::new();
        let id0 = interner.intern("did:plc:alice");
        let id1 = interner.intern("did:plc:bob");
        let id2 = interner.intern("at://did:plc:alice/app.bsky.feed.post/123");

        let exported = interner.export_strings();
        assert_eq!(exported.len(), 3);

        // from_exported_strings
        let restored = StringInterner::from_exported_strings(exported.clone());
        assert_eq!(restored.len(), 3);
        assert_eq!(restored.lookup_id("did:plc:alice"), Some(id0));
        assert_eq!(restored.lookup_id("did:plc:bob"), Some(id1));
        assert_eq!(
            restored.lookup_id("at://did:plc:alice/app.bsky.feed.post/123"),
            Some(id2)
        );
        assert_eq!(restored.lookup_str(id0).as_deref(), Some("did:plc:alice"));

        // hydrate_from into existing instance
        let empty_interner = StringInterner::new();
        empty_interner.hydrate_from(exported);
        assert_eq!(empty_interner.len(), 3);
        assert_eq!(empty_interner.lookup_id("did:plc:alice"), Some(id0));
        assert_eq!(empty_interner.lookup_id("did:plc:bob"), Some(id1));
        assert_eq!(
            empty_interner.lookup_id("at://did:plc:alice/app.bsky.feed.post/123"),
            Some(id2)
        );
    }
}
