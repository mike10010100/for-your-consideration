# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

---

## [0.3.0] - 2026-08-29

### Added

- **`atproto-oauth-rs` Integration**: Integrated standalone, formally verified `atproto-oauth-rs` library replacing in-tree OAuth, PKCE, DPoP, and SSRF modules.
- **64-Shard User Session Storage**: Preserved 64-shard partitioned user session management with clock-warp-safe background maintenance pruning.
- **Adversarial OAuth Integration Test Suite**: Added dedicated integration tests validating multi-tenant DPoP signing, session rotation, and state store race conditions.

---

## [0.2.0] - 2026-08-28

### Added
- **JWT Clock Skew Leeway**: Implemented RFC 7519 §4.1.4 60-second clock skew leeway in `validate_service_jwt` for reliable ATProto Service Auth under distributed AppView timing drift.
- **High-Velocity Pool Sliding Window TTL Cache**: Added 10-second TTL cache for Tier 3 / cold-start candidate retrieval, dropping response times from ~42ms to <1ms.
- **Bounded Graph Traversal & Defenses**: Defensive capping for seed posts (50), post edge slicing (500), and top co-interactors (100) to ensure deterministic sub-15ms personalized traversals.
- **Dedicated Worker Thread Snapshotting**: Backgrounded snapshot persistence via `tokio::task::spawn_blocking` and streaming serialization, preventing event loop blocking during 70M+ edge checkpointing.
- **High-Performance Memory Allocator**: Integrated `tikv-jemallocator` (with fallback to `mimalloc`) under safe feature gates to eliminate malloc heap fragmentation.
- **64-Shard Partitioned State**: Partitioned `GraphStore`, `ImpressionStore`, and `UserPreferencesStore` across 64 independent `RwLock` shards for lock-free scaling.
- **Comprehensive Verification & Durability Test Suite**: 500+ unit, integration, and stress tests maintaining >= 80% line coverage and 0 unsafe code.

### Fixed
- Fixed unauthenticated fallback issue during slight AppView clock skew that previously bypassed viewer impression tracking.
- Eliminated multi-gigabyte heap allocation spikes during periodic binary snapshot checkpoints.

---

## [0.1.0] - 2026-08-26

### Added
- Initial release of **For Your Consideration** custom feed generator for AT Protocol / Bluesky.
- 3-tier recommendation pipeline (Tier 1: 3-step walk, Tier 2: follow-graph walk, Tier 3: velocity pool).
- Real-time multi-stream Jetstream firehose consumer.
- Smooth continuous anti-fatigue score damping with `ImpressionStore`.
- Built-in web dashboard and algorithm dials.
- Atomic binary snapshot serialization (`v1` - `v4`).
