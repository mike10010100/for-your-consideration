# Original User Request

## 2026-08-24T16:30:22Z

Implement Phase 2 of the high-performance single-box "For You" custom feed generator for AT Protocol / Bluesky in Rust, adding **atomic disk snapshot persistence**, **impression memory with anti-repetition fatigue**, and **enhanced new-user onboarding topic diversity** per the updated PRD.

Working directory: /Users/mike10010100/git/atproto-experiments/for-your-consideration
Integrity mode: development

Reference PRD: /Users/mike10010100/git/atproto-experiments/for-your-consideration/PRD.md
Reference Best Practices: https://github.com/mike10010100/rust-best-practices

## Requirements

### R1. Atomic Disk Snapshotting & Fast Boot Recovery (`src/snapshot.rs`)
Implement a compact binary snapshot engine with CRC32 integrity verification that periodically checkpoints `StringInterner` and `GraphStore` to `snapshot.bin` (and on graceful shutdown `SIGINT`/`SIGTERM`), and hydrates pre-warmed state in < 50 ms on boot.

### R2. Impression Memory & Anti-Repetition Fatigue (`src/recommender.rs`)
Implement an in-memory sliding LRU impression cache per viewer DID tracking served post timestamps. Enforce 100% suppression for posts served within the last 30 minutes, and exponential score dampening for posts served in the last 2–6 hours to eliminate feed repetition fatigue.

### R3. Enhanced New-User Cold-Start & Onboarding Topic Diversity
Enrich Tier 3 velocity candidate selection with topic diversity clustering (ensuring a balanced mix across art, tech, science, news, culture) and curated high-signal creator seeds for unauthenticated or brand-new Bluesky accounts.

### R4. Server & Lifecycle Integration (`src/server.rs`, `src/main.rs`)
Integrate impression recording into the `/xrpc/app.bsky.feed.getFeedSkeleton` response flow, wire automatic periodic snapshot tasks in the background `JoinSet`, and ensure clean graceful shutdown snapshotting.

### R5. Production Stability & QA Verification
Maintain strict compliance with `mike10010100/rust-best-practices`: `#![forbid(unsafe_code)]`, zero unwraps/panics in production, strict `clippy::pedantic` with `-D warnings`, defensive time (`saturating_duration_since`), and comprehensive unit/integration/property tests.

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

