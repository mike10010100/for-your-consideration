# 🌟 For Your Consideration (`FYC`)
### High-Performance AT Protocol Custom Feed Generator for Bluesky
*(An homage to the classic "For You" feed)*

[![Version](https://img.shields.io/badge/version-0.2.0-blue.svg)](CHANGELOG.md)
[![Rust Safe](https://img.shields.io/badge/Rust-Safe_2021-brightgreen.svg)](#)
[![Forbid Unsafe](https://img.shields.io/badge/%23!%5Bforbid(unsafe_code)%5D-enforced-blue.svg)](#)
[![Tests Passing](https://img.shields.io/badge/Tests-503%20passed-success.svg)](#)
[![Sub-2ms Latency](https://img.shields.io/badge/Latency-p99%20%3C%201.5ms-orange.svg)](#)

---

## 📖 Overview

**For Your Consideration** is a production-grade, single-box custom feed generator for the AT Protocol and Bluesky. Designed as an homage to the original algorithmic "For You" concept, it re-imagines personalized feed generation through:

1. **Multi-Signal Graph Collaborative Filtering**: Ingests likes ($1.0\times$), quotes ($2.0\times$), reposts ($3.0\times$), and follow relations from the global Bluesky Jetstream firehose in real-time.
2. **RoaringBitmap Cosine Taste Similarity**: Rapidly identifies "Taste Twins" across millions of users with zero GC pauses and sub-millisecond bitset intersections.
3. **Smooth Continuous Anti-Fatigue Damping ("No Groundhog Day")**: Replaces hard 100% suppression cutoffs with a continuous soft-recovery curve ($0.15\times$ floor climbing to $1.0\times$ full score over 6 hours), ensuring refreshes always bring fresh content without hiding posts forever.
4. **3-Tier Cold Start Pipeline**:
   - **Tier 1 ($\ge 10$ interactions)**: 3-step personalized graph random walk.
   - **Tier 2 ($< 10$ interactions)**: 2-step traversal over the user's follow-graph.
   - **Tier 3 (Zero history / unauthenticated)**: Curated creator starter seeds & topic-diverse velocity pools (art, tech, science, news, culture).
5. **Instant Boot Recovery & Fast Replay**: Loads pre-warmed state from atomic binary snapshots (`snapshot.bin`) in **$< 10\text{ ms}$**, or automatically catches up on 12 hours of past history via fast-forward replay ($> 5,000\text{ ev/s}$).
6. **Real-Time Web Dashboard & Simulator**: Built-in zero-dependency SPA at [`http://localhost:3000/`](http://localhost:3000/) for inspecting heap RAM usage, taste twins, algorithmic dials, and live hydration progress.

---

## 📐 Architecture Pipeline

```
Incoming Request (Viewer DID + Algorithmic Dials + Cursor)
                            │
                            ▼
┌───────────────────────────────────────────────────────────┐
│  Phase 1: Cold-Start Prior & Seed Selection               │
│  - Likes >= 10:  Tier 1: 3-Step Multi-Signal Graph Walk   │
│  - Likes < 10:   Tier 2: 2-Step Follow-Graph Walk         │
│  - 0 History:    Tier 3: Topic-Clustered Velocity Pool    │
└───────────────────────────┬───────────────────────────────┘
                            │
                            ▼
┌───────────────────────────────────────────────────────────┐
│  Phase 2: Multi-Signal Graph Traversal                    │
│  - Likes (1.0x), Quotes (2.0x), Reposts (3.0x)            │
│  - RoaringBitmap Cosine taste similarity                  │
│  - Candidate post pool aggregation                        │
└───────────────────────────┬───────────────────────────────┘
                            │
                            ▼
┌───────────────────────────────────────────────────────────┐
│  Phase 3: Multi-Factor Candidate Scoring                  │
│  - Exponential Time-Decay: W(e) = W_signal * exp(-Δt / τ) │
│  - Inverse Degree Dampening: BM25/TF-IDF viral penalty    │
│  - Taste Overlap Weighting                                │
└───────────────────────────┬───────────────────────────────┘
                            │
                            ▼
┌───────────────────────────────────────────────────────────┐
│  Phase 4: Impression Memory & Anti-Fatigue Filtering      │
│  - Liked & Self-authored post deduplication               │
│  - Continuous Soft Fatigue Decay (0.15x -> 1.0x at 6h)    │
│  - Conversation thread root dampening (max 1 post/root)   │
│  - Author diversity constraint (max 1-2 posts/author)     │
│  - 85/15 Epsilon-Greedy Serendipity exploration           │
└───────────────────────────┬───────────────────────────────┘
                            │
                            ▼
┌───────────────────────────────────────────────────────────┐
│  Phase 5: Response Delivery & Impression Recording        │
│  - Record newly served posts into viewer's Impression LRU │
│  - Return hydrated skeleton response + pagination cursor  │
└───────────────────────────────────────────────────────────┘
```

---

## 🚀 Quick Start (Local Run)

### 1. Build and Run Full Test Suite
```bash
cd for-your-consideration
cargo test
```

### 2. Start the Engine
```bash
cargo run --release
```

* **Interactive Web Dashboard**: Open [`http://localhost:3000/`](http://localhost:3000/) or [`http://localhost:3000/dashboard`](http://localhost:3000/dashboard) in your browser.
* **Telemetry API**: [`http://localhost:3000/api/telemetry`](http://localhost:3000/api/telemetry)
* **XRPC Endpoint**: `http://localhost:3000/xrpc/app.bsky.feed.getFeedSkeleton`

---

## 🌐 Public Deployment & Publishing to Bluesky

To make your feed public and pin-able in the official Bluesky app, you need to expose your server via HTTPS and register your feed generator record.

### Step 1: Run the Feed Engine

#### Option A: Docker Compose (Recommended for Permanent Box)
```bash
# Start with persistent volume for snapshot checkpoints
docker compose up -d --build

# View live stream ingest and telemetry
docker compose logs -f
```

#### Option B: Cargo Native
```bash
cargo run --release
```

---

### Step 2: Expose Public HTTPS via Cloudflare Tunnel

We provide a helper script that automates Cloudflare Tunnel setup:

```bash
./scripts/setup_tunnel.sh
```

* **Mode 1 (Quick / Ephemeral)**: Instantly exposes `http://localhost:3000` via a public `*.trycloudflare.com` URL (zero-config, free).
* **Mode 2 (Production Custom Domain)**: Authenticates and binds your custom domain (e.g. `feed.example.com`) directly to your permanent box without opening firewall ports.

Verify your DID document is accessible:
```bash
curl https://<YOUR_TUNNEL_DOMAIN>/.well-known/did.json
```

### Step 3: Publish Feed Generator Record to Bluesky

Run the included publication script with a Bluesky App Password:

```bash
BSKY_HANDLE="your-handle.com" \
BSKY_PASSWORD="xxxx-xxxx-xxxx-xxxx" \
FEED_HOSTNAME="feed.yourdomain.com" \
./scripts/publish_feed.sh
```

### Step 4: Pin in the Bluesky App!

Open your generated share link:
```
https://bsky.app/profile/<YOUR_DID>/feed/for-your-consideration
```
Tap **"Pin to Home"** to enjoy your custom, real-time personalized feed!

## 🎛️ Dynamic Algorithm Dials

Users and clients can customize recommendation parameters dynamically using URL query parameters:

* `freshness`: Adjusts the time-decay half-life $\tau$ (`realtime` = 6h, `balanced` = 36h, `weekly` = 168h).
* `discovery`: Adjusts the serendipity exploration ratio $\epsilon$ (`familiar` = 5%, `balanced` = 15%, `deep_dive` = 35%).
* `replies`: Controls post composition (`root` = Root posts only [default], `all` = Include root posts and replies).
* `topic_art`, `topic_tech`, `topic_science`, `topic_news`, `topic_culture`: Custom topic domain multipliers (0.0x–5.0x).
* `explain=true`: Returns full mathematical proof chains explaining why each candidate post was selected.

---

## 🔐 Native ATProto OAuth & Saved Dials

Users can authenticate directly on the web dashboard using native ATProto OAuth (no app passwords required):
* **Sign In with Bluesky**: PKCE S256 + DPoP token exchange against user's home PDS.
* **Persistent Algorithmic Dials**: Saved slider adjustments automatically apply to the viewer's feed when browsing the Bluesky app via authenticated Service Auth JWTs.
* **1-Click Feed Publishing**: Admin users can register or update the `app.bsky.feed.generator` record directly from the web dashboard.

---

## 🦀 Rust Best Practices & Architecture Standards

This codebase strictly follows the production-grade stability patterns, defensive concurrency rules, and QA quality gates established in [`mike10010100/rust-best-practices`](https://github.com/mike10010100/rust-best-practices):

* **Architecture Guide**: [`BEST_PRACTICES.md`](https://github.com/mike10010100/rust-best-practices/blob/main/BEST_PRACTICES.md)
* **Tooling Blueprint**: [`TOOLING.md`](https://github.com/mike10010100/rust-best-practices/blob/main/TOOLING.md)
* **AI Agent Instructions**: [`AGENTS.md`](AGENTS.md) (and [`agents.md`](https://github.com/mike10010100/rust-best-practices/blob/main/agents.md))

### Non-Negotiable Invariants:
1. **Zero Unsafe Code**: `#![forbid(unsafe_code)]` enforced crate-wide with 0 unsafe blocks.
2. **Strict Crate-Root Safety Guard**: `#![deny(clippy::all, clippy::unwrap_used, clippy::expect_used, clippy::panic, missing_docs, rust_2018_idioms)]`.
3. **Zero Production Panics**: 100% typed error handling with `Result<T, FeedError>` and graceful fallback defaults.
4. **Defensive Concurrency & Monotonic Time**: Clock-warp safe math (`saturating_duration_since`), drift-free interval scheduling, 64-shard partitioned locks, and zero lock holding across `.await` yield points.
5. **Leak-Free Async Tasks**: All background workers managed within a supervised `tokio::task::JoinSet` bound to a unified `CancellationToken`.
6. **Sub-2ms SLA**: Sub-2ms p99 query latency verified under concurrent ingestion and preference mutation stress.
7. **Strict Semantic Versioning & CHANGELOG**: Automated PR version bump enforcement (`scripts/check_version_bump.sh`) following [SemVer 2.0.0](https://semver.org/) and [Keep a Changelog](CHANGELOG.md).
