use ahash::AHashMap;
use compact_str::CompactString;
use parking_lot::RwLock;

/// Fast, thread-safe bidirectional string interner mapping string identifiers
/// (such as DIDs and AT-URIs) to compact 32-bit integer IDs (`u32`).
///
/// Uses double-checked locking with [`parking_lot::RwLock`] and [`AHashMap`]
/// for minimal lock contention under high concurrency.
#[derive(Debug, Default)]
pub struct StringInterner {
    inner: RwLock<InternerInner>,
}

#[derive(Debug, Default)]
struct InternerInner {
    to_id: AHashMap<CompactString, u32>,
    to_str: Vec<CompactString>,
}

impl StringInterner {
    /// Creates a new, empty [`StringInterner`].
    #[must_use]
    pub fn new() -> Self {
        Self {
            inner: RwLock::new(InternerInner::default()),
        }
    }

    /// Interns a string slice, returning its unique `u32` ID.
    ///
    /// If the string was already interned, returns the existing ID.
    /// Uses double-checked locking to minimize write-lock contention.
    pub fn intern(&self, s: &str) -> u32 {
        self.get_or_intern(s)
    }

    /// Interns a string slice, returning its unique `u32` ID.
    ///
    /// Alias for [`StringInterner::intern`].
    pub fn get_or_intern(&self, s: &str) -> u32 {
        // Fast path: optimistic read lock
        {
            let guard = self.inner.read();
            if let Some(&id) = guard.to_id.get(s) {
                return id;
            }
        }

        // Slow path: write lock with double-checked verification
        let mut guard = self.inner.write();
        if let Some(&id) = guard.to_id.get(s) {
            return id;
        }

        let compact = CompactString::new(s);
        let id = guard.to_str.len() as u32;
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
        let guard = self.inner.read();
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
        let guard = self.inner.read();
        guard.to_str.get(id as usize).cloned()
    }

    /// Returns the total number of interned strings.
    #[must_use]
    pub fn len(&self) -> usize {
        let guard = self.inner.read();
        guard.to_str.len()
    }

    /// Returns `true` if no strings have been interned yet.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Returns the estimated heap memory footprint in bytes.
    #[must_use]
    pub fn estimated_size_bytes(&self) -> usize {
        let guard = self.inner.read();
        let strings_len = guard.to_str.len();
        let vec_bytes = strings_len * std::mem::size_of::<CompactString>();
        let map_bytes = guard.to_id.capacity()
            * (std::mem::size_of::<CompactString>() + std::mem::size_of::<u32>() + 16);
        std::mem::size_of::<Self>() + vec_bytes + map_bytes
    }

    /// Clears all interned strings (useful for testing).
    pub fn clear(&self) {
        let mut guard = self.inner.write();
        guard.to_id.clear();
        guard.to_str.clear();
    }

    /// Exports a snapshot clone of all interned strings in index order.
    #[must_use]
    pub fn export_strings(&self) -> Vec<CompactString> {
        let guard = self.inner.read();
        guard.to_str.clone()
    }

    /// Creates a new [`StringInterner`] pre-populated from an ordered list of strings.
    #[must_use]
    pub fn from_exported_strings(strings: Vec<CompactString>) -> Self {
        let mut to_id = AHashMap::with_capacity(strings.len());
        for (idx, s) in strings.iter().enumerate() {
            to_id.insert(s.clone(), idx as u32);
        }
        Self {
            inner: RwLock::new(InternerInner {
                to_id,
                to_str: strings,
            }),
        }
    }

    /// Replaces the internal state with the provided ordered list of strings.
    pub fn hydrate_from(&self, strings: Vec<CompactString>) {
        let mut guard = self.inner.write();
        guard.to_id.clear();
        guard.to_id.reserve(strings.len());
        for (idx, s) in strings.iter().enumerate() {
            guard.to_id.insert(s.clone(), idx as u32);
        }
        guard.to_str = strings;
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

        assert_eq!(id1, 0);
        assert_eq!(id2, 1);
        assert_eq!(id1, id1_again);
        assert_eq!(interner.len(), 2);
        assert!(!interner.is_empty());

        assert_eq!(interner.lookup_id("did:plc:alice"), Some(0));
        assert_eq!(interner.lookup_id("did:plc:bob"), Some(1));
        assert_eq!(interner.lookup_id("did:plc:charlie"), None);

        assert_eq!(interner.lookup_str(0).as_deref(), Some("did:plc:alice"));
        assert_eq!(interner.lookup_str(1).as_deref(), Some("did:plc:bob"));
        assert_eq!(interner.lookup_str(2), None);

        // Test alias methods
        assert_eq!(interner.get_id("did:plc:alice"), Some(0));
        assert_eq!(interner.resolve(0).as_deref(), Some("did:plc:alice"));
    }

    #[test]
    fn test_interner_concurrent_access() {
        let interner = Arc::new(StringInterner::new());
        let mut handles = Vec::new();

        for i in 0..16 {
            let interner = Arc::clone(&interner);
            handles.push(thread::spawn(move || {
                for j in 0..100 {
                    let key = format!("did:plc:user_{}", j);
                    let id = interner.intern(&key);
                    assert_eq!(id, j as u32);
                    assert_eq!(interner.lookup_str(id).as_deref(), Some(key.as_str()));
                }
                let unique_key = format!("did:plc:thread_{}_unique", i);
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

        assert_eq!(id0, 0);
        assert_eq!(id1, 1);
        assert_eq!(id2, 2);

        let exported = interner.export_strings();
        assert_eq!(exported.len(), 3);
        assert_eq!(exported[0].as_str(), "did:plc:alice");
        assert_eq!(exported[1].as_str(), "did:plc:bob");
        assert_eq!(
            exported[2].as_str(),
            "at://did:plc:alice/app.bsky.feed.post/123"
        );

        // from_exported_strings
        let restored = StringInterner::from_exported_strings(exported.clone());
        assert_eq!(restored.len(), 3);
        assert_eq!(restored.lookup_id("did:plc:alice"), Some(0));
        assert_eq!(restored.lookup_id("did:plc:bob"), Some(1));
        assert_eq!(
            restored.lookup_id("at://did:plc:alice/app.bsky.feed.post/123"),
            Some(2)
        );
        assert_eq!(restored.lookup_str(0).as_deref(), Some("did:plc:alice"));

        // hydrate_from into existing instance
        let empty_interner = StringInterner::new();
        empty_interner.hydrate_from(exported);
        assert_eq!(empty_interner.len(), 3);
        assert_eq!(empty_interner.lookup_id("did:plc:alice"), Some(0));
        assert_eq!(empty_interner.lookup_id("did:plc:bob"), Some(1));
        assert_eq!(
            empty_interner.lookup_id("at://did:plc:alice/app.bsky.feed.post/123"),
            Some(2)
        );
    }
}
