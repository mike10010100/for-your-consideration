# GitHub Copilot Code Review & Engineering Guidelines

This repository (`for-your-consideration`) is a production-grade, high-performance AT Protocol / Bluesky custom feed generator written in 100% Safe Rust. When reviewing Pull Requests or generating code suggestions, strictly adhere to the following architecture and resilience invariants:

---

## 🛡️ Core Safety & Non-Negotiable Invariants

### 1. Zero Unsafe Code
- The crate root strictly enforces `#![forbid(unsafe_code)]`.
- **Block/Reject** any attempt to introduce `unsafe` blocks or dependencies that require unsafe exceptions.

### 2. Zero Production Panics & Typed Errors
- **Banned in production code**: `.unwrap()`, `.expect()`, `panic!`, `todo!`, `unimplemented!`.
- All fallible operations must return strongly typed `Result<T, FeedError>` using variants from `src/error.rs`.
- Use `?`, `match`, or `if let` for clean, defensive error propagation.

### 3. Defensive Concurrency & Lock Invariants
- **NEVER Hold Locks Across `.await` Points**: `parking_lot::RwLock` and `Mutex` guards must be dropped before executing any `.await`, network I/O, or asynchronous sleep/yield points.
- **64-Shard Partitioning**: Concurrency-sensitive structures (`GraphStore`, `ImpressionStore`, `UserPreferencesStore`, `OAuthStateStore`, `OAuthUserSessionStore`) must use 64 independent shards to prevent lock contention.

### 4. Monotonic Time & Clock-Warp Safety
- Monotonic clocks can warp under NTP adjustments or VM snapshots.
- Always compute elapsed durations using `now.saturating_duration_since(earlier)` or `.map_or(0, ...)`. Never call raw `.duration_since()` without a safe fallback.
- Recurring background tasks must use `tokio::time::interval` or anchor-relative timestamps to prevent scheduling drift.

### 5. Task Cancellation & Leak Prevention
- Background workers must be managed within a `tokio::task::JoinSet` bound to a unified `CancellationToken`.
- On shutdown, tasks must be cleanly aborted and joined.

### 6. SSRF & Network Egress Defense
- All outbound requests to user-supplied PDS/PLC endpoints must be validated via `validate_outbound_url` / `validate_outbound_url_async`.
- Block loopback (`127.0.0.1`, `::1`), private RFC 1918 networks (`10.0.0.0/8`, `172.16.0.0/12`, `192.168.0.0/16`), Carrier-Grade NAT (`100.64.0.0/10`), link-local (`169.254.0.0/16`), and cloud metadata endpoints.
- Outbound HTTP clients must use `reqwest::redirect::Policy::none()`.

### 7. 100% Documentation Coverage
- All public types, functions, modules, and fields must have descriptive documentation comments (`missing_docs` is denied).
- Bare URLs in documentation must be enclosed in angle brackets (e.g. `<https://fyc.mike10010100.com>`).
