# Original User Request

## 2026-08-24T16:30:22Z

Implement Phase 2 of the high-performance single-box "For You" custom feed generator for AT Protocol / Bluesky in Rust, adding **atomic disk snapshot persistence**, **impression memory with anti-repetition fatigue**, and **enhanced new-user onboarding topic diversity** per the updated PRD.

Working directory: for-your-consideration/
Integrity mode: development

Reference PRD: PRD.md
Reference Best Practices: https://github.com/mike10010100/rust-best-practices

## Requirements

### R1. Atomic Disk Snapshotting & Fast Boot Recovery (`src/snapshot.rs`)
Implement a compact binary snapshot engine with CRC32 integrity verification that periodically checkpoints `StringInterner` and `GraphStore` to `snapshot.bin` (and on graceful shutdown `SIGINT`/`SIGTERM`), and hydrates pre-warmed state in < 50 ms on boot.

### R2. Impression Memory & Anti-Repetition Fatigue (`src/recommender.rs`)
Implement an in-memory sliding LRU impression cache per viewer DID tracking served post timestamps. Enforce 100% suppression for posts served within the last 30 minutes, and exponential score dampening for posts served in the last 2–6 hours to eliminate feed repetition fatigue.

### R3. Dynamic Query Parameter Weights (`src/server.rs`, `src/types.rs`)
Support dynamic per-request dial overrides in `app.bsky.feed.getFeedSkeleton`:
- `freshness`: Half-life duration (`realtime` = 6h, `balanced` = 36h, `weekly` = 168h)
- `discovery`: Serendipity exploration ratio (`familiar` = 0.05, `balanced` = 0.15, `deep_dive` = 0.35)
- `topic_art`, `topic_tech`, `topic_science`, `topic_news`, `topic_culture`: Custom integer weights (0–100) for topic domain preferences
- `explain=true`: Returns full mathematical proof chains explaining why each candidate post was selected.

### R4. Production Validation, Clippy Pedantic & Benchmarks
- All existing and new tests must pass (`cargo test`).
- `#![forbid(unsafe_code)]` must be strictly enforced.
- Zero Clippy warnings under `cargo clippy --all-targets -- -D warnings`.
- Performance benchmark suites (`benches/snapshot_bench.rs`, `benches/fatigue_bench.rs`) verifying sub-50ms snapshot hydration and sub-2ms recommendation latency.

---

## 2026-08-25T04:20:15Z

Fix Jetstream ingest velocity regression and add live hydration progress tracking to the dashboard.

Working directory: for-your-consideration/
Reference PRD: PRD.md

## Acceptance Criteria

### Performance & Snapshots
- [ ] Snapshot hydration restores the complete graph and interner state in < 50 ms.
- [ ] Snapshot serialization is atomic (via temp file rename) and corruption-safe with CRC32 validation.

### Anti-Repetition & Quality
- [ ] Posts served to a viewer within 30 minutes are 100% suppressed on subsequent initial page requests.
- [ ] Posts served 30m–6h ago receive exponential score decay without disappearing permanently.
- [ ] Tier 3 cold-start delivers a diverse cross-section of content rather than a single spiked topic.

### Quality & Linter Gates
- [ ] `cargo clippy --all-targets --all-features -- -D warnings` passes with 0 warnings.
- [ ] `cargo fmt --all -- --check` passes cleanly.
- [ ] `cargo test` and `cargo test --doc` pass 100% of test cases.

## 2026-08-24T17:26:11Z

Build an interactive, modern web dashboard and explainability playground served directly by the Axum server (`http://localhost:3000/`) for the high-performance single-box "For You" feed generator.

Working directory: /Users/mike10010100/git/atproto-experiments/for-your-consideration
Integrity mode: development

Reference PRD: /Users/mike10010100/git/atproto-experiments/for-your-consideration/PRD.md
Reference Best Practices: https://github.com/mike10010100/rust-best-practices

## Requirements

### R1. Modern Web Dashboard at Root (`/` and `/dashboard`)
Serve a fast, responsive, zero-dependency HTML5/CSS/Vanilla-JS single-page application embedded directly into the Axum server. Display live graph telemetry (total edges, interned strings, nodes, snapshot status, and real-time ingestion velocity).

### R2. Handle / DID Taste Twins Explorer
Provide an interactive search bar where users can enter any Bluesky handle or DID. Implement an endpoint (`GET /api/taste-twins?did=...`) returning their top taste-twins in the like-graph, their Cosine similarity scores, and shared liked posts.

### R3. Live Algorithmic Dials & Interactive Feed Preview
Provide interactive UI sliders for **Freshness** (half-life τ), **Discovery / Serendipity** (ε-exploration ratio), and **Topic Biasing** (Art, Tech, Science, News, Culture). Re-render the recommendation feed dynamically as sliders are moved, displaying post URIs, score breakdowns, topic badges, and source tiers.

### R4. Graph Proof Chain Explainer ("Why am I seeing this?")
Clicking any recommended post in the UI displays an explainability card showing the exact 3-step proof chain: `You -> Interacted Post -> Taste Twin (@user) -> Recommended Post`.

### R5. Production Stability & QA Verification
Adhere strictly to mike10010100/rust-best-practices: #![forbid(unsafe_code)], zero unwraps/panics in production, strict clippy::pedantic with -D warnings, defensive time (saturating_duration_since), and 100% test pass rate.

## Acceptance Criteria

### UI & Endpoints
- [ ] Navigating to http://localhost:3000/ renders the interactive dashboard with live telemetry.
- [ ] GET /api/taste-twins returns valid JSON with top co-interactors and similarity scores.
- [ ] Moving dials dynamically updates feed candidates with sub-2ms backend query latency.
- [ ] Explainability proof chains correctly display the intermediary post and user connections.

### Quality & Linter Gates
- [ ] cargo clippy --all-targets --all-features -- -D warnings passes with 0 warnings.
- [ ] cargo fmt --all -- --check passes cleanly.
- [ ] cargo test and cargo test --doc pass 100% of test cases.

---

## 2026-08-28T01:33:53Z

Use a full multi-agent swarm team to resolve critical feed generation latency and memory bottlenecks in the AT Protocol / Bluesky custom feed generator (`for-your-consideration`), bringing query response times, preview generation, and background snapshot persistence into sub-10ms performance bounds under high-volume firehose ingestion while curbing heap RSS bloat.

Working directory: /home/mike10010100/git/for-your-consideration
Integrity mode: development

## Requirements

### R1. Recommendation Query & Preview Traversal Optimization
- Ensure feed preview generation (`recommend_preview_at` for `GET /api/feed-preview`) and live graph traversals operate with strict defensive bounds (capping seed posts to 50, slicing post edges to 500, capping top co-interactors to 100) to eliminate combinatorial fanout on graphs with >50M edges.
- Ensure taste-twin discovery (`find_taste_twins` for `GET /api/taste-twins`) bounds seed post exploration and interaction slicing to return sub-10ms response times.

### R2. High-Velocity Pool Sliding Window TTL Cache
- Implement a resilient time-windowed TTL cache (e.g. 5–10s validity) for Tier 3 / cold-start velocity pool candidate selection in `GraphStore::get_velocity_pool_candidates_at`.
- Eliminate redundant dynamic recalculation and scanning of the 65,536-entry ring buffer on every unauthenticated request during active ~450 ev/s firehose ingestion.

### R3. Non-Blocking Snapshot Checkpoint Persistence
- Offload periodic disk snapshot persistence (`save_snapshot_with_preferences`) from the core Tokio async worker thread in `src/main.rs` to dedicated blocking worker threads via `tokio::task::spawn_blocking`.
- Stream shard data during snapshot serialization to avoid allocating multi-gigabyte temporary clone vectors across 52.6M edges.

### R4. Heap Memory Optimization & Allocator Configuration
- Configure a high-performance memory allocator (e.g. `tikv-jemallocator` or `mimalloc` under safe Rust feature gates) to prevent `glibc` malloc heap fragmentation and bring Docker container RSS down from 23.2GB to < 5GB.

### R5. Non-Negotiable Repository Invariants & Verification Pipeline
- Strictly enforce all repository standards: `#![forbid(unsafe_code)]`, zero panics (`clippy::unwrap_used`, `clippy::expect_used`, `clippy::panic` denied), `#![deny(missing_docs)]`, 64-shard partitioned concurrency, and clock-warp safe math.
- Pass 100% of the verification pipeline:
  1. `cargo fmt --all -- --check`
  2. `cargo clippy --all-targets -- -D warnings`
  3. `cargo test --all-targets`
  4. `cargo deny check`
  5. `cargo llvm-cov --all-targets --fail-under-lines 80 --summary-only`

## Acceptance Criteria

### Performance & Latency
- [ ] `/api/feed-preview` response time drops from ~7.8s to < 10ms on the live 52M-edge graph.
- [ ] Tier 3 / cold-start candidate retrieval drops from ~42ms to < 1ms on cache hits.
- [ ] `/api/taste-twins` response time drops from ~745ms to < 20ms.
- [ ] Periodic snapshot execution does not block or monopolize Tokio async runtime worker threads.
- [ ] In-memory heap memory spikes during snapshot checkpoints are minimized.

### Production Integrity & Quality Gates
- [ ] Zero `#![forbid(unsafe_code)]` violations.
- [ ] Zero unwraps, expects, or panics in production code paths.
- [ ] 100% documentation coverage on all public APIs and items (`missing_docs`).
- [ ] `cargo fmt`, `cargo clippy --all-targets -- -D warnings`, and `cargo deny check` pass with 0 warnings/errors.
- [ ] Full test suite (`cargo test --all-targets`) passes 100% of unit, integration, and stress tests with >= 80% line coverage.


