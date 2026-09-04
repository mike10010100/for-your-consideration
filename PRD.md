# Product Requirements Document (PRD): High-Performance Single-Box "For You" Feed Engine

**Project Name:** `for-your-consideration`  
**Target Platform:** AT Protocol / Bluesky Custom Feeds  
**Language & Runtime:** Rust (Edition 2021), Tokio Async Runtime  
**Location:** `for-your-consideration/`  
**Design Standard:** Strict compliance with [`mike10010100/rust-best-practices`](https://github.com/mike10010100/rust-best-practices)

---

## 1. Executive Summary & Vision

`for-your-consideration` is a **production-grade, single-box custom feed generator** for AT Protocol and Bluesky. It provides personalized "For You" recommendations through an advanced **multi-signal, time-decayed graph collaborative filtering engine** combined with **impression-aware anti-fatigue filtering, serendipity exploration, cold-start fallbacks, and atomic disk persistence**.

The engine is built as a **zero-GC, lock-free, in-memory graph engine** written in safe Rust (`#![forbid(unsafe_code)]`), achieving sub-millisecond recommendation latencies, supporting 10,000+ requests/sec on single-box hardware, and recovering pre-warmed state instantly via compact binary snapshots.

---

## 2. Comprehensive Algorithmic Pipeline

```
[ Incoming Request (Viewer DID + Algorithmic Dials + Cursor) ]
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│  Phase 1: Cold-Start Prior & Seed Selection                     │
│  - If Likes >= 10:  Tier 1: 3-Step Multi-Signal Graph Walk      │
│  - If Likes < 10:   Tier 2: 2-Step Follow-Graph Walk (Seed)     │
│  - If 0 History:    Tier 3: Topic-Diverse Global Velocity Pool  │
└─────────────────────────────┬───────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│  Phase 2: Multi-Signal Graph Traversal                          │
│  - Ingest: Likes (1.0x), Quotes (2.0x), Reposts (3.0x)          │
│  - RoaringBitmap Cosine taste similarity over co-interactors    │
│  - Aggregate candidate post pool                                │
└─────────────────────────────┬───────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│  Phase 3: Multi-Factor Candidate Scoring                        │
│  - Exponential Time-Decay: W(e) = W_signal * exp(-Δt / τ)       │
│  - Inverse Degree Dampening: BM25/TF-IDF viral penalty          │
│  - Taste Overlap Weighting                                      │
└─────────────────────────────┬───────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│  Phase 4: Impression Memory & Anti-Fatigue Filtering            │
│  - Liked & Self-authored post deduplication                     │
│  - Impression Fatigue: Hard-suppress posts served in last 30m;  │
│    apply exponential dampening to posts served in last 2-6h     │
│  - Conversation thread root dampening (max 1 post/root)         │
│  - Author diversity constraint (max 1-2 posts/author)           │
│  - 85/15 Epsilon-Greedy Serendipity exploration                 │
└─────────────────────────────┬───────────────────────────────────┘
                              │
                              ▼
┌─────────────────────────────────────────────────────────────────┐
│  Phase 5: Response Delivery & Impression Recording              │
│  - Record newly served post IDs into viewer's Impression LRU    │
│  - Return hydrated skeleton response + pagination cursor        │
└─────────────────────────────────────────────────────────────────┘
```

---

## 3. Detailed Feature Specifications

### 3.1. Multi-Signal & Time-Decayed Graph Engine
- **Multi-Signal Edge Weighting**:
  - `Like` event: $1.0\times$ weight
  - `Quote` event: $2.0\times$ weight
  - `Repost` event: $3.0\times$ weight
- **Exponential Half-Life Time Decay**:
  $$W(\text{interaction}) = \text{SignalWeight} \times e^{-\frac{t_{\text{current}} - t_{\text{event}}}{\tau}}$$
  where $\tau$ defaults to 36 hours (configurable via request parameter `freshness`).
- **Inverse Degree Dampening (Anti-Viral Penalty)**:
  Candidate score is normalized by global interaction frequency:
  $$\text{Score}(p) = \sum_{u \in \text{CoInteractors}} \frac{W(u \to p) \times \text{Sim}(U_{\text{viewer}}, u)}{\sqrt{|\text{GlobalInteractions}(p)| + 1}}$$

### 3.2. Impression Memory & Anti-Repetition Filtering (No Groundhog Day)
- **Problem**: Users who scroll past posts without liking them repeatedly see the exact same posts promoted at the top of their feed across refreshes.
- **Impression Store**: A bounded in-memory sliding LRU cache of `(ViewerID -> VecDeque<(PostID, Timestamp)>)` backed by a per-user `RoaringBitmap`.
- **Two-Tier Impression Fatigue** (implemented as a single continuous curve; see `ImpressionStore::evaluate_fatigue_penalty`):
  1. **Immediate Suppression Floor (0 mins)**: Posts served right now are dampened to a **15% score floor** (`FATIGUE_MIN_FLOOR`), effectively demoting them below unseen content.
  2. **Soft Fatigue Recovery Window (0 – 6 hours)**: Served posts recover exponentially toward full score:
     $$\text{Score}_{\text{adjusted}}(p) = \text{Score}(p) \times \left(0.15 + 0.85 \times \left(1.0 - e^{-\frac{\Delta t_{\text{served}}}{\tau_{\text{fatigue}}}}\right)\right)$$
     with $\tau_{\text{fatigue}} = 2\text{h}$. At 30m the multiplier is $\approx 0.34$, at 2h $\approx 0.69$, and after 6h the post is fully recovered ($1.0\times$).
- **Result**: Every refresh surfaces genuinely fresh content while keeping the user's core taste affinity intact — no post is ever permanently hidden.

### 3.3. Enhanced New-User Cold-Start & Onboarding Experience
- **Tier 1 (Active Users, $\ge 10$ interactions)**: Full 3-step random walk over interaction graph.
- **Tier 2 (New Users, $< 10$ interactions)**: 2-step traversal over the user's follow-graph ($U_{\text{viewer}} \to \text{Follows} \to \text{Their Likes}$).
- **Tier 3 (Zero History / Unauthenticated Onboarding)**:
  - **Topic Diversity Clustering**: Instead of a flat single-topic spike, the velocity pool categorizes posts across varied domains (art, tech, science, news, culture).
  - **Curated Starter Seeds**: Verified high-signal creator seeds provide an immediate warm start for new Bluesky sign-ups.

### 3.4. Serendipity & Diversity Constraints
- **85/15 Epsilon-Greedy Exploration**: 85% high-confidence taste cluster + 15% high-velocity adjacent cluster sampling.
- **Author Diversity Limit**: Max 1–2 posts per author per 30-post skeleton page.
- **Thread Dampening**: Max 1 post per conversation reply tree root.

### 3.5. Disk Persistence & Fast Boot Snapshots (`src/snapshot.rs`)
- **Atomic Periodic Checkpoints**: Every $N$ minutes (and on graceful shutdown `SIGINT`/`SIGTERM`), the engine serializes the `StringInterner` and `GraphStore` to an atomic temporary file (`snapshot.bin.tmp`) and renames it to `snapshot.bin`.
- **Sub-50ms Cold Start Recovery**: On engine startup, if `snapshot.bin` exists:
  - Validates binary magic bytes and CRC32 checksum.
  - Hydrates the entire graph and interner in $< 50\text{ ms}$, resuming live Jetstream ingestion seamlessly without a cold-start backfill delay.
- **Format**: Zero-copy compact binary encoding (string dictionary table + roaring bitmap byte streams + edge arrays).

### 3.6. User Agency & "Algorithm Dials"
Supports URL query parameters passed in the feed request:
- `freshness`: Adjusts half-life $\tau$ (`realtime` = 6h, `balanced` = 36h, `weekly` = 168h).
- `discovery`: Controls the exploration ratio $\epsilon$ (`familiar` = 5%, `balanced` = 15%, `deep_dive` = 35%).
- `explain`: If `true`, returns structured interaction trace metadata for UI explainers.

### 3.7. Initial Startup 12-Hour Historical Replay (`BACKFILL_HOURS`)
- **Problem**: When starting up a fresh node with no existing snapshot, the in-memory graph is completely empty until live events accumulate over hours.
- **Solution**: Jetstream's time-based cursor replay (`?cursor=<microsecond_timestamp>`).
- **Mechanism**:
  - On clean startup (when no snapshot is present), the ingester calculates $t_{\text{start}} = (\text{now} - 12\text{ hours}) \times 1,000,000\text{ }\mu\text{s}$.
  - Connects to Jetstream with `?cursor={t_start}`.
  - Replays and ingests all historical likes, reposts, posts, and follows from the last 12 hours at line rate (several thousand events per second).
  - Pre-warms the bipartite graph, taste twins, and velocity pools within ~2–3 minutes before transitioning seamlessly into real-time streaming.

---

## 4. Technical Architecture

```
                          ┌────────────────────────┐
                          │   AT Protocol Relay    │
                          │      (Jetstream)       │
                          └───────────┬────────────┘
                                      │ (WebSocket JSON: likes, reposts, quotes, follows)
                                      ▼
                          ┌────────────────────────┐
                          │   Jetstream Ingester   │
                          │   (`src/ingest.rs`)    │
                          │ (Backpressure / Channel│
                          └───────────┬────────────┘
                                      │
                                      ▼
                          ┌────────────────────────┐   Atomic Save / Load
                          │    String Interner     │◄───────────────────────┐
                          │  (`src/interner.rs`)   │                        │
                          └───────────┬────────────┘                        │
                                      │                             ┌───────┴────────┐
                                      ▼                             │ Disk Snapshot  │
                          ┌────────────────────────┐                │(`src/snapshot`)│
                          │ In-Memory Graph Store  │◄───────────────┤ (snapshot.bin) │
                          │   (`src/graph.rs`)     │                └────────────────┘
                          │  (Roaring Bitmaps /    │
                          │   Weighted Edges)      │
                          └───────────┬────────────┘
                                      ▲
                                      │ 3-Step Walk Query (< 2ms)
                                      │
                          ┌───────────┴────────────┐
                          │   Recommender Engine   │◄─── [ Impression Store ]
                          │ (`src/recommender.rs`) │     (Served Post History)
                          └───────────┬────────────┘
                                      ▲
                                      │ getFeedSkeleton + JWT
                                      │
┌───────────────────────┐ ┌───────────┴────────────┐
│   Bluesky AppView /   │─┤  Axum XRPC Web Server  │
│     Mobile Client     │ │   (`src/server.rs`)    │
└───────────────────────┘ └────────────────────────┘
```

---

## 5. Rust Best Practices & Quality Standards

Strictly enforces all invariants from [`mike10010100/rust-best-practices`](https://github.com/mike10010100/rust-best-practices):

| Invariant | Standard Applied |
| :--- | :--- |
| **Safety** | `#![forbid(unsafe_code)]` in all modules. |
| **Zero Panics** | No `.unwrap()`, `.expect()`, `panic!`, `todo!`. All fallibles return `Result<T, FeedError>`. |
| **Clippy** | `-D warnings`, `clippy::pedantic`, `clippy::nursery` with 0 warnings. |
| **Defensive Time** | `now.saturating_duration_since(earlier)` everywhere. |
| **Async Locking** | Zero async lock contention: synchronous guards dropped before `.await`. |
| **Task Lifecycle** | All background workers tracked in `tokio::task::JoinSet` with `CancellationToken`. |
| **Testing** | Unit tests, boundary tests, proptest property tests, and doc-tests (`cargo test --doc`). |

---

## 6. Success Metrics & Target Verification

| Metric | Target Goal | Verification Method |
| :--- | :--- | :--- |
| **Recommendation Latency (p99)** | < 2.0 ms | Benchmark harness (`recommendation_latency`) |
| **Snapshot Recovery Time** | < 50 ms for 10M edges | Cold boot benchmark (`snapshot_recovery`) |
| **Impression Suppression** | 100% suppression in 30m window | Integration test (`tests/impression_tests.rs`) |
| **Memory Footprint** | < 500 MB for 10M+ edges | Heap profiling benchmark (`memory_footprint`) |
| **Clippy & Code Quality** | 0 warnings, 0 unwraps | `cargo clippy --all-targets -- -D warnings` |
| **Test Coverage** | 100% test pass rate | `cargo test` + `cargo test --doc` |
