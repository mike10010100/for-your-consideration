# 🌟 For Your Consideration (`FYC`)
### High-Performance AT Protocol Custom Feed Generator for Bluesky
*(An homage to the classic "For You" feed)*

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

### Step 1: Deploy with Docker or Fly.io (or Cloudflare Tunnel)

#### Option A: Docker / Docker Compose (Recommended for VPS)
```bash
# Start with persistent volume for snapshots
docker compose up -d --build

# Inspect live container logs
docker compose logs -f
```

#### Option B: Fly.io (1-Click Cloud Deployment)
```bash
# Create volume for persistent snapshot storage
fly volumes create fyc_data --size 2 -r iad

# Deploy app
fly launch
fly deploy
```

#### Option C: Cloudflare Tunnel (Quick Local Testing)
```bash
# Free zero-config port forwarding
cloudflared tunnel --url http://localhost:3000
```
This gives you a public hostname, e.g. `feed.yourdomain.com` (or `xyz.trycloudflare.com`).

Set your environment variables:
```bash
export HOSTNAME="feed.yourdomain.com"
export SERVICE_DID="did:web:feed.yourdomain.com"
export FEED_RKEY="for-your-consideration"
cargo run --release
```

Verify your DID document is accessible:
```bash
curl https://feed.yourdomain.com/.well-known/did.json
```

### Step 2: Publish Feed Generator Record to Bluesky

Run the included publication script with a Bluesky App Password:

```bash
BSKY_HANDLE="your-handle.com" \
BSKY_PASSWORD="xxxx-xxxx-xxxx-xxxx" \
FEED_HOSTNAME="feed.yourdomain.com" \
./scripts/publish_feed.sh
```

### Step 3: Pin in the Bluesky App!

Open your generated share link:
```
https://bsky.app/profile/<YOUR_DID>/feed/for-your-consideration
```
Tap **"Pin to Home"** to enjoy your custom, real-time personalized feed!

---

## 🎛️ Dynamic Algorithm Dials

Users and clients can customize recommendation parameters dynamically using URL query parameters:

* `freshness`: Adjusts the time-decay half-life $\tau$ (`realtime` = 6h, `balanced` = 36h, `weekly` = 168h).
* `discovery`: Adjusts the serendipity exploration ratio $\epsilon$ (`familiar` = 5%, `balanced` = 15%, `deep_dive` = 35%).
* `topic_art`, `topic_tech`, `topic_science`, `topic_news`, `topic_culture`: Custom integer weights (0–100) for topic domain preferences.
* `explain=true`: Returns full mathematical proof chains explaining why each candidate post was selected.

---

## 🛡️ Invariants & Quality Standards

Strictly adheres to [`mike10010100/rust-best-practices`](https://github.com/mike10010100/rust-best-practices):
* `#![forbid(unsafe_code)]` enforced across all crates and modules.
* Zero `.unwrap()`, zero `.expect()`, zero `panic!` in production paths.
* Zero Clippy warnings (`-D warnings` with `clippy::pedantic` and `clippy::nursery`).
* All background tasks tracked in `tokio::task::JoinSet` with cooperative `CancellationToken` cancellation.
* Sub-2ms p99 recommendation latency under high write concurrency.
