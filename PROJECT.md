# Project: for-your-consideration Performance and Memory Optimization

## Architecture
`for-your-consideration` is a high-throughput custom feed generator for Bluesky / AT Protocol operating under continuous firehose ingestion (~450 events/sec, >52M edges). The optimization architecture eliminates query latency bottlenecks, bounds combinatorial graph traversals, introduces sliding window candidate caching, offloads blocking snapshot persistence with shard streaming, and configures modern low-fragmentation memory allocators.

### Core Subsystems & Data Flow
1. **Graph Store (`src/graph.rs`)**:
   - 64 independent `parking_lot::RwLock` shards for forward edges, reverse edges, RoaringBitmaps, and metadata.
   - Sliding window TTL cache (`VelocityCandidateCache`) with 10-second validity and clock-warp safety.
   - Shard-by-shard streaming serialization methods (`stream_user_interactions_to`, `stream_post_interactions_to`, `stream_user_likes_bitmaps_to`, etc.) directly into `BufWriter`.
2. **Recommender Engine (`src/recommender.rs`)**:
   - Centralized defensive bounding constants: `MAX_SEED_POSTS = 50`, `MAX_POST_EDGES = 500`, `MAX_CO_INTERACTORS = 100`.
   - `recommend_preview_at`: Bounded 3-step walk with seed slicing, reverse post edge slicing, and top co-interactor selection.
   - `find_taste_twins`: Bounded seed post exploration and reverse edge slicing.
3. **Snapshot Checkpoint Persistence (`src/snapshot.rs`, `src/main.rs`)**:
   - `save_snapshot_with_preferences`: Streams shard-by-shard without creating intermediate multi-gigabyte clone vectors.
   - Offloaded from Tokio async worker threads to dedicated blocking thread pool via `tokio::task::spawn_blocking`.
4. **Allocator & Safety (`Cargo.toml`, `src/main.rs`, `src/lib.rs`)**:
   - Feature-gated `tikv-jemallocator` (`jemalloc` default) and `mimalloc` under 100% safe Rust `#[global_allocator]`.
   - Strict `#![forbid(unsafe_code)]`, zero unwraps/expects/panics, `#![deny(missing_docs)]`.

## Feature Inventory
| # | Feature | Description | Milestone | Source |
|---|---------|-------------|-----------|--------|
| 1 | Centralized Traversal Bounds | Define `MAX_SEED_POSTS` (50), `MAX_POST_EDGES` (500), `MAX_CO_INTERACTORS` (100) in `src/recommender.rs` | M1 | Survey 1 / Request R1 |
| 2 | Bounded Feed Preview Walk | Slice seed posts to 50, post edges to 500, top co-interactors to 100 in `recommend_preview_at` | M1 | Survey 1 / Request R1 |
| 3 | Bounded Taste Twins Walk | Slice seed posts to 50 and reverse edges to 500 in `find_taste_twins` | M1 | Survey 1 / Request R1 |
| 4 | Explainability Edge Slicing | Slice reverse interaction edges to 500 in `explain_recommendation` | M1 | Survey 1 / Request R1 |
| 5 | Velocity Pool TTL Cache | 10-second clock-warp safe sliding window TTL cache in `GraphStore::get_velocity_pool_candidates_at` | M2 | Survey 2 / Request R2 |
| 6 | TTL Cache Invalidation Discipline | Invalidate cache on `clear()`, `restore_from_snapshot()`, and `prune_older_than()` | M2 | Survey 2 / Request R2 |
| 7 | Non-Blocking Snapshot Checkpoints | Offload periodic snapshot persistence to `tokio::task::spawn_blocking` in `src/main.rs` | M3 | Survey 2 / Request R3 |
| 8 | Shard-by-Shard Streaming Serialization | Stream data directly from 64 shards to `BufWriter` without multi-gigabyte clone vectors | M3 | Survey 2 / Request R3 |
| 9 | Safe Allocator Configuration | Feature-gate `tikv-jemallocator` and `mimalloc` in `Cargo.toml` and `src/main.rs` | M4 | Survey 3 / Request R4 |
| 10 | Strict Safety & Repository Invariants | Maintain `#![forbid(unsafe_code)]`, zero unwrap/expect/panic, `#![deny(missing_docs)]` | M4 | Survey 3 / Request R5 |
| 11 | Complete Verification Pipeline | Pass fmt, clippy -D warnings, test suite, cargo deny, llvm-cov >= 80% | M5 | Survey 3 / Request R5 |

## Milestones
| # | Name | Scope | Dependencies | Status |
|---|------|-------|-------------|--------|
| M1 | Traversal Bounds & Query Optimization | `src/recommender.rs`, `tests/` | none | DONE |
| M2 | High-Velocity Pool TTL Cache | `src/graph.rs`, `src/types.rs`, `tests/` | none | DONE |
| M3 | Non-Blocking Streaming Snapshot Persistence | `src/snapshot.rs`, `src/graph.rs`, `src/interner.rs`, `src/preferences.rs`, `src/main.rs`, `tests/` | none | DONE |
| M4 | Heap Memory Optimization & Allocator Config | `Cargo.toml`, `src/main.rs` | none | DONE |
| M5 | E2E Testing Suite & Adversarial Hardening | Opaque-box E2E test suite (Tiers 1-4) & adversarial test suite (Tier 5) | M1, M2, M3, M4 | DONE |

## Interface Contracts

### M1: Traversal Bounds (`src/recommender.rs`)
- `pub const MAX_SEED_POSTS: usize = 50;`
- `pub const MAX_POST_EDGES: usize = 500;`
- `pub const MAX_CO_INTERACTORS: usize = 100;`
- `recommend_preview_at`: Caps seed posts, slices post interactions, selects top 100 co-interactors by Bayesian similarity.
- `find_taste_twins`: Bounded seed exploration and interaction edge slicing.

### M2: Velocity Pool TTL Cache (`src/graph.rs`)
- `pub const VELOCITY_CACHE_TTL_SECS: u64 = 10;`
- `pub struct VelocityCandidateCache { pub evaluated_at_secs: u64, pub candidates: Vec<u32> }`
- `get_velocity_pool_candidates_at(&self, current_time_secs: u64, limit: usize) -> Vec<u32>`

### M3: Streaming Snapshot Persistence (`src/snapshot.rs`, `src/graph.rs`)
- `GraphStore::stream_user_interactions_to<F>(&self, write_chunk: &mut F, total_edges: &mut u64) -> Result<()>`
- `GraphStore::stream_post_interactions_to<F>(&self, write_chunk: &mut F) -> Result<()>`
- `GraphStore::stream_user_likes_bitmaps_to<F>(&self, write_chunk: &mut F, buf: &mut Vec<u8>) -> Result<()>`
- `StringInterner::stream_strings_to<F>(&self, write_chunk: &mut F) -> Result<()>`
- `UserPreferencesStore::stream_preferences_to<F>(&self, write_chunk: &mut F) -> Result<()>`
- `save_snapshot_with_preferences`: Streams shard data directly to `BufWriter` (128 KB buffer) and computes CRC32 on the fly.
- In `src/main.rs`: `tokio::task::spawn_blocking` wraps `save_snapshot_with_preferences`.

### M4: Allocator & Feature Gates (`Cargo.toml`, `src/main.rs`)
- `[features] default = ["jemalloc"], jemalloc = ["dep:tikv-jemallocator"], mimalloc = ["dep:mimalloc"]`
- In `src/main.rs`: `#[cfg(feature = "jemalloc")] #[global_allocator] static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;`

## Code Layout
- `src/lib.rs`: Library crate root with safety invariants.
- `src/main.rs`: Binary entry point, runtime setup, global allocator, supervised task management.
- `src/graph.rs`: 64-shard storage, adjacency accessors, similarity & decay math, velocity cache, streaming snapshot methods.
- `src/recommender.rs`: Bounded collaborative filtering, candidate scoring, feed preview, taste twins, explainability.
- `src/snapshot.rs`: Streaming binary serialization, atomic checkpoint saving, CRC32 verification, deserialization.
- `src/preferences.rs`: 64-shard user preference store and streaming methods.
- `src/interner.rs`: 64-shard string interner and streaming methods.
- `src/types.rs`: Data structures, score breakdowns, candidate evaluations, response types.
- `tests/`: Integration, unit, adversarial challenger, and stress test suites.
