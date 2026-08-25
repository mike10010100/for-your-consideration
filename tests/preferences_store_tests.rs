#![forbid(unsafe_code)]
#![allow(clippy::float_cmp)]

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::Duration;

use for_your_consideration::prelude::*;

#[test]
fn test_preferences_store_new_is_empty() {
    let store = UserPreferencesStore::new();
    assert!(store.is_empty());
    assert_eq!(store.len(), 0);
    assert_eq!(PREFERENCE_SHARDS, 64);
}

#[test]
fn test_preferences_store_crud_by_id() {
    let store = UserPreferencesStore::new();

    let dials1 = UserDials::from_hours(
        12.0,
        0.20,
        TopicWeights {
            art: 1.5,
            tech: 2.0,
            science: 1.0,
            news: 0.8,
            culture: 1.2,
        },
        1_700_000_000,
    );

    let dials2 = UserDials::from_hours(
        72.0,
        0.05,
        TopicWeights {
            art: 0.5,
            tech: 3.0,
            science: 2.0,
            news: 0.1,
            culture: 0.5,
        },
        1_700_000_100,
    );

    // Insert user 10 and 20
    store.set(10, dials1);
    store.set(20, dials2);
    assert_eq!(store.len(), 2);
    assert!(!store.is_empty());

    assert_eq!(store.get(10), Some(dials1));
    assert_eq!(store.get(20), Some(dials2));
    assert_eq!(store.get(30), None);

    // Overwrite user 10
    let dials1_updated = UserDials::from_hours(24.0, 0.35, TopicWeights::default(), 1_700_000_200);
    store.set(10, dials1_updated);
    assert_eq!(store.len(), 2);
    assert_eq!(store.get(10), Some(dials1_updated));

    // Remove user 10
    assert_eq!(store.remove(10), Some(dials1_updated));
    assert_eq!(store.remove(10), None);
    assert_eq!(store.len(), 1);
    assert_eq!(store.get(10), None);

    // Delete user 20
    assert!(store.delete(20));
    assert!(!store.delete(20));
    assert_eq!(store.len(), 0);
    assert!(store.is_empty());
}

#[test]
fn test_preferences_store_crud_by_did() {
    let store = UserPreferencesStore::new();
    let interner = StringInterner::new();

    let dials_alice = UserDials::from_hours(6.0, 0.30, TopicWeights::default(), 1000);
    let dials_bob = UserDials::from_hours(48.0, 0.10, TopicWeights::default(), 2000);

    let alice_id = store.set_by_did(&interner, "did:plc:alice", dials_alice);
    let bob_id = store.set_by_did(&interner, "did:plc:bob", dials_bob);

    assert_eq!(store.len(), 2);
    assert_eq!(
        store.get_by_did(&interner, "did:plc:alice"),
        Some(dials_alice)
    );
    assert_eq!(store.get_by_did(&interner, "did:plc:bob"), Some(dials_bob));
    assert_eq!(store.get(alice_id), Some(dials_alice));
    assert_eq!(store.get(bob_id), Some(dials_bob));

    // Remove alice by DID
    assert_eq!(
        store.remove_by_did(&interner, "did:plc:alice"),
        Some(dials_alice)
    );
    assert_eq!(store.get_by_did(&interner, "did:plc:alice"), None);
    assert_eq!(store.len(), 1);

    // Delete bob by DID
    assert!(store.delete_by_did(&interner, "did:plc:bob"));
    assert!(!store.delete_by_did(&interner, "did:plc:bob"));
    assert_eq!(store.len(), 0);
    assert!(store.is_empty());
}

#[test]
fn test_preferences_store_uninterned_did_fast_path() {
    let store = UserPreferencesStore::new();
    let interner = StringInterner::new();

    // Query uninterned DID without mutating interner
    let uninterned_did = "did:plc:completely_unknown_user";
    assert_eq!(store.get_by_did(&interner, uninterned_did), None);
    assert_eq!(
        store.get_by_did_or_default(&interner, uninterned_did),
        UserDials::default()
    );

    // Verify interner was not mutated
    assert_eq!(interner.lookup_id(uninterned_did), None);
    assert_eq!(store.remove_by_did(&interner, uninterned_did), None);
    assert!(!store.delete_by_did(&interner, uninterned_did));
}

#[test]
fn test_preferences_store_default_fallback() {
    let store = UserPreferencesStore::new();
    let interner = StringInterner::new();

    assert_eq!(store.get_or_default(999), UserDials::default());
    assert_eq!(
        store.get_by_did_or_default(&interner, "did:plc:nonexistent"),
        UserDials::default()
    );
}

#[test]
fn test_preferences_store_clear() {
    let store = UserPreferencesStore::new();
    for i in 0..500 {
        store.set(i, UserDials::default());
    }
    assert_eq!(store.len(), 500);
    assert!(!store.is_empty());

    store.clear();
    assert_eq!(store.len(), 0);
    assert!(store.is_empty());
    assert_eq!(store.get(100), None);
}

#[test]
fn test_preferences_store_snapshot_export_and_restore() {
    let store = UserPreferencesStore::new();
    let mut expected = Vec::new();

    for i in 0..1000 {
        let dials = UserDials::from_hours(
            1.0 + (i % 168) as f32,
            ((i % 50) as f32) / 100.0,
            TopicWeights {
                art: ((i % 50) as f32) / 10.0,
                tech: 1.0,
                science: 2.0,
                news: 0.5,
                culture: 1.5,
            },
            1_700_000_000 + u64::from(i),
        );
        store.set(i, dials);
        expected.push((i, dials));
    }

    assert_eq!(store.len(), 1000);
    let exported = store.snapshot_data();
    assert_eq!(exported.len(), 1000);

    let restored_store = UserPreferencesStore::new();
    restored_store.restore_from_snapshot(exported);
    assert_eq!(restored_store.len(), 1000);

    for (uid, dials) in expected {
        assert_eq!(restored_store.get(uid), Some(dials));
    }
}

#[test]
fn test_preferences_store_concurrent_readers_writers() {
    let store = Arc::new(UserPreferencesStore::new());
    let interner = Arc::new(StringInterner::new());
    let stop_signal = Arc::new(AtomicBool::new(false));

    // Pre-populate 200 users
    for i in 0..200 {
        let did = format!("did:plc:user_{i}");
        store.set_by_did(&interner, &did, UserDials::default());
    }

    let mut handles = Vec::new();

    // 8 Writer threads
    for thread_idx in 0..8 {
        let s = Arc::clone(&store);
        let stop = Arc::clone(&stop_signal);
        let handle = thread::spawn(move || {
            let mut counter = 0u64;
            while !stop.load(Ordering::Relaxed) {
                let user_id = (thread_idx * 100 + (counter % 100)) as u32;
                let dials = UserDials::from_hours(
                    12.0 + (counter % 24) as f32,
                    0.20,
                    TopicWeights::default(),
                    counter,
                );
                s.set(user_id, dials);
                if counter.is_multiple_of(50) {
                    s.remove(user_id);
                }
                counter += 1;
            }
        });
        handles.push(handle);
    }

    // 16 Reader threads
    for thread_idx in 0..16 {
        let s = Arc::clone(&store);
        let i_ref = Arc::clone(&interner);
        let stop = Arc::clone(&stop_signal);
        let handle = thread::spawn(move || {
            let mut reads = 0u64;
            while !stop.load(Ordering::Relaxed) {
                let target_user = (thread_idx * 50 + (reads % 200)) as u32;
                let _ = s.get(target_user);
                let _ = s.get_or_default(target_user);

                let did = format!("did:plc:user_{}", reads % 250);
                let _ = s.get_by_did(&i_ref, &did);
                let _ = s.get_by_did_or_default(&i_ref, &did);

                reads += 1;
            }
        });
        handles.push(handle);
    }

    // Run concurrency stress for 250ms
    thread::sleep(Duration::from_millis(250));
    stop_signal.store(true, Ordering::Relaxed);

    for h in handles {
        h.join().expect("Thread join failed");
    }

    // Assert store is intact and callable
    assert!(!store.is_empty());
}

#[test]
fn test_preferences_store_shard_distribution() {
    assert_eq!(PREFERENCE_SHARDS, 64);
    let mut shard_counts = [0usize; 64];

    for id in 0..6400 {
        let s = shard_idx(id);
        assert!(s < 64);
        shard_counts[s] += 1;
    }

    for count in shard_counts {
        assert_eq!(count, 100, "Partitioning must be strictly uniform");
    }
}

#[test]
fn test_preferences_store_memory_estimation_and_clone() {
    let store = UserPreferencesStore::new();
    let initial_size = store.estimated_size_bytes();
    assert!(initial_size > 0);

    for i in 0..500 {
        store.set(i, UserDials::default());
    }
    let populated_size = store.estimated_size_bytes();
    assert!(populated_size >= initial_size);

    let cloned = store.clone();
    assert_eq!(cloned.len(), 500);
    assert_eq!(cloned.get(10), Some(UserDials::default()));

    // Mutating clone does not mutate original
    cloned.clear();
    assert_eq!(cloned.len(), 0);
    assert_eq!(store.len(), 500);
}
