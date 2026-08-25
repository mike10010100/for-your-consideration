# Project: for-your-consideration Phase 2

## Architecture
- **Language & Standards**: Safe Rust 2021 Edition, `#![forbid(unsafe_code)]`, zero unwraps/panics in production, strict `clippy::pedantic` with `-D warnings`, saturating time arithmetic.
- **Core Components**:
  - `StringInterner` (`src/interner.rs`): Bidirectional 32-bit compact string table.
  - `GraphStore` (`src/graph.rs`): 64-shard partitioned multi-signal graph store with RoaringBitmaps.
  - `SnapshotEngine` (`src/snapshot.rs`): Atomic disk persistence with CRC32 checksum, `< 50ms` boot hydration.
  - `ImpressionStore` & `Recommender` (`src/recommender.rs`): 64-shard sliding LRU impression cache, 30m hard suppression, 2–6h exponential decay, 5-cluster topic diversity (Art, Tech, Science, News, Culture), creator seeds, 3-tier candidate generation.
  - `XRPCServer` & `Lifecycle` (`src/server.rs`, `src/main.rs`): Axum HTTP service, impression tracking on `getFeedSkeleton`, periodic background checkpoints in `tokio::task::JoinSet`, graceful shutdown persistence on `SIGINT`/`SIGTERM`.

## Feature Inventory
| # | Feature | Description | Milestone | Source |
|---|---|---|---|---|
| F1 | Binary Snapshot Format | 64-byte self-describing header (`b"FYFD"`, v1), string dictionary, 64-shard graph tables | M1 (DONE) | PRD §3.5, R1 |
| F2 | CRC32 Integrity Checksum | Header and payload CRC32 validation via `crc32fast` | M1 (DONE) | PRD §3.5, R1 |
| F3 | Atomic Rename Persistence | Temporary file write (`snapshot.bin.tmp`) + `sync_all` + atomic POSIX rename to `snapshot.bin` | M1 (DONE) | PRD §3.5, R1 |
| F4 | Fast Boot Hydration (<50ms) | Instant pre-warmed state recovery with zero hashmap reallocation overhead | M1 (DONE) | PRD §3.5, R1 |
| F5 | StringInterner Snapshot Hooks | Export and hydration methods for string dictionary | M1 (DONE) | PRD §3.5, R1 |
| F6 | GraphStore Snapshot Hooks | Export and hydration methods for 64 shards of adjacency, bitmaps, follows, post metadata, and active posts | M1 (DONE) | PRD §3.5, R1 |
| F7 | Impression Store Data Structure | 64-shard `parking_lot::RwLock` sliding LRU per viewer DID (`RoaringBitmap` + `VecDeque` + `AHashMap`) | M2 (DONE) | PRD §3.2, R2 |
| F8 | Two-Tier Anti-Fatigue Filtering | 100% hard suppression for posts served in 0–30m; exponential soft score decay for posts served in 30m–6h | M2 (DONE) | PRD §3.2, R2 |
| F9 | Impression Eviction & Bounded Memory | Bounded per-user capacity (1,000 posts) and sliding window pruning | M2 (DONE) | PRD §3.2, R2 |
| F10 | Topic Diversity Clustering | 5-cluster classification (Art, Tech, Science, News, Culture) with keyword/hashtag pattern matching | M3 (DONE) | PRD §3.3, R3 |
| F11 | Curated Starter Creator Seeds | High-signal creator seeds mapped to topic clusters for unauthenticated / new users | M3 (DONE) | PRD §3.3, R3 |
| F12 | Round-Robin Diversity Interleaving | Balanced candidate selection preventing single-topic viral monopoly in Tier 3 cold-start | M3 (DONE) | PRD §3.3, R3 |
| F13 | XRPC Impression Recording Hook | Capture served post IDs in `handle_get_feed_skeleton` and record to viewer's impression history | M4 (DONE) | PRD §3.2, R4 |
| F14 | Startup Snapshot Hydration Hook | Load pre-warmed graph and interner on server startup before binding listener | M4 (DONE) | PRD §3.5, R4 |
| F15 | Periodic Snapshot JoinSet Task | Background worker running every 5 minutes in `tokio::task::JoinSet` with `CancellationToken` | M4 (DONE) | PRD §3.5, R4 |
| F16 | Graceful Shutdown Persistence | Persist latest graph and interner state to `snapshot.bin` on `SIGINT`/`SIGTERM` after task drain | M4 (DONE) | PRD §3.5, R4 |
| F17 | Production Quality & Linter Gates | `#![forbid(unsafe_code)]`, zero panics/unwraps, `clippy::pedantic` `-D warnings`, `cargo fmt`, unit/prop/doc tests | M5 (DONE) | PRD §5, R5 |
| F18 | Embedded SPA Dashboard | Responsive zero-dependency HTML5/CSS/Vanilla-JS SPA served at `/` and `/dashboard` via Axum | M3 (DONE) | PRD, R1 |
| F19 | Telemetry API (`/api/telemetry`) | Live graph telemetry: total edges, interned strings, nodes, snapshot status, ingestion velocity | M2 (DONE) | PRD, R1 |
| F20 | Taste Twins API (`/api/taste-twins`) | Co-interactor discovery, Cosine similarity over RoaringBitmaps, shared liked posts | M1 (DONE) | PRD, R2 |
| F21 | Dials & Feed Preview API (`/api/feed-preview`) | Live algorithmic dials (Freshness, Discovery, Topic weights) + read-only candidate scoring (<2ms) | M1 (DONE) | PRD, R3 |
| F22 | Graph Proof Chain Explainer (`/api/explain`) | 3-step proof chain reconstruction (`You -> Interacted Post -> Taste Twin -> Recommended Post`) | M1 (DONE) | PRD, R4 |
| F23 | Production Stability & Linter Gates | `#![forbid(unsafe_code)]`, zero unwraps/panics, strict clippy `-D warnings`, full test suite | M4 (DONE) | PRD, R5 |

## Milestones
| # | Name | Scope | Dependencies | Status |
|---|---|---|---|---|
| M1 | Core Recommender APIs & Proof Engine | `src/types.rs`, `src/recommender.rs`, `src/graph.rs`, `tests/recommender_api_tests.rs` | none | DONE |
| M2 | Telemetry & Axum REST Endpoints | `src/server.rs`, `src/main.rs`, `tests/dashboard_api_tests.rs` | M1 | DONE |
| M3 | Embedded Web Dashboard SPA | `src/assets/dashboard.html`, `src/server.rs`, `tests/dashboard_spa_tests.rs` | M2 | DONE |
| M4 | Comprehensive QA & Hardening | Full workspace, Clippy pedantic, Fmt, Doc tests, E2E tests, Auditor gate | M1, M2, M3 | DONE |

## Interface Contracts

### `src/types.rs` ↔ `src/recommender.rs`
- `TopicWeights { art: f32, tech: f32, science: f32, news: f32, culture: f32 }`
- `ScoreBreakdown { time_decay: f32, taste_similarity: f32, topic_boost: f32, fatigue_penalty: f32, final_score: f32 }`
- `FeedPreviewItem { uri: CompactString, author_did: CompactString, topic: TopicCategory, tier: String, score_breakdown: ScoreBreakdown, proof_chain: Option<GraphProofChain> }`
- `FeedPreviewResponse { viewer_did: CompactString, items: Vec<FeedPreviewItem>, total_candidates: usize, query_latency_us: u64 }`
- `TasteTwinItem { user_did: CompactString, similarity_score: f32, shared_posts_count: usize, top_interests: Vec<TopicCategory>, shared_posts: Vec<SharedPostInfo> }`
- `TasteTwinsResponse { viewer_did: CompactString, total_liked_posts: usize, twins: Vec<TasteTwinItem>, query_latency_us: u64 }`
- `GraphProofChain { steps: Vec<ProofChainStep>, summary: String }`

### `src/recommender.rs` ↔ `src/server.rs`
- `Recommender::find_taste_twins(&self, viewer_did: &str, limit: usize) -> Result<TasteTwinsResponse, FeedError>`
- `Recommender::recommend_preview(&self, viewer_did: Option<&str>, dials: &RecommendationDials) -> Result<FeedPreviewResponse, FeedError>`
- `Recommender::explain_recommendation(&self, viewer_did: &str, post_uri: &str) -> Result<GraphProofChain, FeedError>`

### `src/server.rs` ↔ Axum Handlers
- `GET /` and `GET /dashboard` -> `Html<&'static str>`
- `GET /api/telemetry` -> `Json<TelemetryResponse>`
- `GET /api/taste-twins?did=...&limit=...` -> `Json<TasteTwinsResponse>`
- `GET /api/feed-preview?viewer=...&freshness=...&discovery=...&art=...&tech=...&science=...&news=...&culture=...` -> `Json<FeedPreviewResponse>`
- `GET /api/explain?viewer=...&uri=...` -> `Json<GraphProofChain>`

## Code Layout
- `src/lib.rs`: Module exports and prelude
- `src/types.rs`: Core types, `SignalType`, `CompactEdge`, `PostMeta`, `TopicCategory`, `TopicWeights`, telemetry & preview DTOs
- `src/interner.rs`: `StringInterner` with export/hydrate
- `src/graph.rs`: 64-shard `GraphStore` with export/restore and RoaringBitmap operations
- `src/snapshot.rs`: Binary serialization, CRC32, atomic rename, hydration
- `src/recommender.rs`: Multi-signal routing, `ImpressionStore`, anti-fatigue math, topic diversity, taste twins, preview candidate generation, proof chain tracing
- `src/server.rs`: Axum XRPC server, SPA static embedding, REST endpoints for telemetry, taste twins, preview, and explainability
- `src/assets/dashboard.html`: Embedded zero-dependency HTML5/CSS/Vanilla-JS SPA
- `src/main.rs`: CLI runtime, telemetry trackers, lifecycle tasks, graceful shutdown persistence
- `src/error.rs`: Error taxonomy including `FeedError::UserNotFound`, `FeedError::PostNotFound`
- `tests/`: Unit, boundary, property, and E2E integration test suites
