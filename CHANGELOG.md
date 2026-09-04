# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

---

## [0.4.1] - 2026-09-04

### Fixed

- **`describeFeedGenerator` Canonical AT-URI Fallback to `admin_did`**: When `FEED_URI` is not explicitly set in the environment, `/xrpc/app.bsky.feed.describeFeedGenerator` now falls back to advertising `at://<admin_did>/app.bsky.feed.generator/<feed_rkey>` rather than the service DID (`at://did:web:...`). Because `did:web` represents the service endpoint rather than an ATProto personal data repository, advertising `at://did:web:...` caused external feed crawlers and clients to fail with identity resolution errors (`could not find feed` / `could not resolve identity`).
- **Dynamic In-Memory Update on Feed Publish**: Wrapped `AppState.feed_uri` in `Arc<RwLock<Option<CompactString>>>` and updated `/api/feed/publish` so that when a user publishes the feed generator record via the dashboard, the advertised canonical `feed_uri` updates dynamically in running memory without requiring a server reboot.

### Changed

- **Docker Healthcheck Start Period Bumped for Hydration Resilience**: Increased `healthcheck.start_period` in `docker-compose.yml` from `10s` to `180s` (3 minutes). Hydrating snapshots containing >200M edges takes ~87 seconds; the previous 10s start period falsely triggered healthcheck failures and risked premature container restarts.
- **Deployment Script Awaits Service Health Readiness**: Added a post-launch polling loop to `scripts/deploy.sh` that monitors container health after `docker compose up -d`. This ensures deployments wait until snapshot hydration finishes and prevents users from hitting cold-boot connection errors (Cloudflare 502) while the snapshot is hydrating. The script exits with a non-zero status if the container fails to report healthy within the timeout window.
- **CI / CD Automated Release & Workflow Hardening**: Replicated `skyauth` release automation and CI quality gate patterns tailored for services: added an automated GitHub Release job to `.github/workflows/ci.yml` that triggers upon merging to `main`, waits for required CodeQL static security scans before tagging, and publishes release notes from `CHANGELOG.md`. Added `publish = false` to `Cargo.toml` to guard against unintended crates.io publishing. Hardened CI workflows with SHA-pinned actions across `ci.yml` and `codeql.yml`, enabled doc-tests (`cargo test --doc`), added workflow concurrency cancellation, and added support for custom base references in `scripts/check_version_bump.sh`.

---

## [0.4.0] - 2026-09-04

### Added

- **ES256K Service Auth JWT Signature Verification (Closes the Known Forgery Window)**: `validate_service_jwt` verified only JWT *claims* (exp/aud/DID shape) without checking the cryptographic signature — any client could forge a Bearer token claiming an arbitrary DID on `getFeedSkeleton`, poisoning another account's impression history and reading their saved dials. The new `service_auth` module closes this: the `iss` DID is resolved to its DID document (`did:plc` via PLC directory, `did:web` via `/.well-known/did.json`, SSRF-filtered through `skyauth::identity::IdentityResolver`), the `#atproto` verification method's Multikey is decoded (`0xe7` secp256k1 varint prefix enforced — P-256/RSA key substitution rejected, preventing algorithm confusion), and the signature over `header.payload` is verified with the `k256` crate. Resolved keys are memoized in a 64-shard TTL cache (15 min) with invalidation on signature failure so rotated DID documents re-fetch. A failed verification on a cached key drops the entry so key rotation is picked up on the next request.

### Added

- **`SERVICE_AUTH_MODE` Environment Dial**: `getFeedSkeleton` JWT validation is policy-driven — `enforce`/`strict`/`verify` enables full signature verification (forged tokens degrade to anonymous browsing, never acting as the claimed viewer), while the default `legacy`/`off` keeps payload-only validation as a migration ramp for existing deployments. Enforcement is the recommended production setting.
- **`service_auth` Module**: `ServiceAuthVerifier` (with exact-match, compile-gated test-key registration for offline suites — no substring backdoors), `DidKeyCache` (64-shard TTL cache matching the repo shard convention), Multikey/multibase decoding, and `#[cfg(debug_assertions)]`-gated test utilities. 10 unit tests cover signature roundtrips, algorithm-confusion rejection, key-type pinning, cache TTL/invalidation, base58 vectors, and fail-closed DID resolution; an integration test proves enforce mode authenticates signed JWTs while forged tokens cannot mutate the claimed viewer's impression history.
- New dependency: `k256` (pure-Rust secp256k1, `default-features = false`, ecdsa+alloc) — passes `cargo deny` (licenses, advisories, bans).

---

## [0.3.8] - 2026-09-03

### Changed

- **Cargo.lock Is Now Tracked**: This is an application binary, not a library — committing the lockfile guarantees reproducible builds across CI and deployment hosts (previously gitignored, so builds resolved dependency versions non-deterministically).
- **PRD §3.2 Updated to the Implemented Fatigue Model**: The PRD still described the original 100%-hard-suppression 0–30m design; the shipped model is a continuous 0.15× floor recovering exponentially to 1.0× over 6 hours. The spec now documents `ImpressionStore::evaluate_fatigue_penalty` semantics exactly (including the 2h τ and the 0.34×/0.69× checkpoints).
- **Docs Refresh**: TEST_INFRA.md referenced three deleted test suites (`snapshot_streaming_tests.rs`, `velocity_ttl_tests.rs`, `allocator_safety_tests.rs`) — replaced with the actual suite map; TEST_READY.md total corrected from 107 to ~1,150 measured cases plus the measured 84.95% line coverage; README test badge updated.
- **`.env.example` Replay Default Aligned**: `REPLAY_HOURS=168` (7 days — beyond what Jetstream reliably serves) aligned to the docker-compose default of 12 hours, with a comment explaining the firehose cursor-expiry constraint.

### Removed

- Duplicated `percent_encode_query_param` re-implementation in `server.rs`; the handler now imports the canonical `auth::percent_encode_query_param`.

---

## [0.3.7] - 2026-09-03

### Security

- **Snapshot Load: Hostile Length Prefixes Rejected Before Allocation**: The streaming load path introduced in 0.3.6 allocated `vec![0u8; len]` directly from attacker-controlled length prefixes (string, edge-array, u32-array, bitmap sections). A crafted snapshot with a valid CRC header carrying a ~96 GiB prefix caused `memory allocation of 103079215080 bytes failed` → SIGABRT, aborting the entire test process in CI. `StreamReader` now tracks remaining payload bytes and enforces them on every length prefix (`check_len`) and every section record count (`bound_count`), returning a structured `FeedError::Snapshot("Unexpected EOF: ... length prefix ... exceeds remaining payload")` instead. Regression test `test_streaming_load_oversized_length_prefix_never_aborts` locks the no-abort contract.

---

## [0.3.6] - 2026-09-03

### Changed

- **Streaming Snapshot Load (Bounded Boot Memory)**: `load_snapshot_with_preferences` previously read the **entire payload into RAM** (`vec![0u8; payload_len]`) before verifying CRC32 and deserializing — for a 656 MB snapshot that meant a ~656 MB boot-time allocation spike, meaning the PROJECT.md M3 streaming goal was only met on the save path. Loading is now fully streaming: an integrity pass streams the payload through a fixed 1 MiB chunk buffer into the CRC32 hasher (memory bounded regardless of snapshot size), then a parse pass deserializes sections directly from the file via a new `StreamReader` with one-byte pushback probing for the optional preferences section. Truncated payloads now surface as `FeedError::Snapshot` with explicit `Unexpected EOF` markers instead of raw `std::io::Error`, preserving the adversarial durability error-message contract. Byte-for-byte format unchanged (v1–v4 remain compatible); two new tests cover multi-chunk roundtrips (~4 MB payload) and mid-payload truncation rejection.

---

## [0.3.5] - 2026-09-03

### Fixed

- **`describeFeedGenerator` Record URI Correctness**: The feed URI advertised by `GET /xrpc/app.bsky.feed.describeFeedGenerator` was synthesized as `at://<service-did>/app.bsky.feed.generator/<rkey>`, but the generator record is actually published into the **publisher's repository** (`at://<publisher-did>/...`) — a mismatch the Bluesky AppView would surface as an unresolvable feed. Added `FEED_URI` env var (and `AppState::with_feed_uri`) for deployments to declare the canonical record URI explicitly; `describeFeedGenerator` advertises it verbatim when set and falls back to the previous service-DID form otherwise.
- **`FEED_HOSTNAME` Preference Over `HOSTNAME`**: The binary now prefers the `FEED_HOSTNAME` env var, falling back to `HOSTNAME`. The generic `HOSTNAME` variable is exported by most shells and CI environments (e.g. `MacBook-Pro.local`), which previously caused a bare `cargo run` to silently publish a wrong hostname in the DID document and OAuth client metadata. docker-compose now passes the value as `FEED_HOSTNAME`.
- **Removed Hard-Coded Personal Admin DID Mapping**: The `ADMIN_HANDLE == "mike10010100.com" → did:plc:...` special case baked into `main.rs` is removed; admin handles are resolved via `resolve_identity_pds` like any other handle, with a hint to set `ADMIN_DID` explicitly when resolution fails.
- **`SnapshotStatusInfo` Default Version**: `SnapshotStatusInfo::default()` now reports the current snapshot format version constant instead of the stale `1`.

### Added

- `test_describe_feed_generator_default_and_configured_feed_uri` covering both the fallback and the verbatim `FEED_URI` advertisement.

---

## [0.3.4] - 2026-09-03

### Fixed

- **Freshness Dial Consistency (PRD §3.1/§3.6 Alignment)**: The three divergent freshness preset tables (`RecommendationDials::from_query`, `handle_get_feed_skeleton`, and the preference-router test double) now share identical semantics: `realtime` = 6h, **`balanced` = 36h** (previously 24h in `from_query` but 36h in the skeleton endpoint — the same query parameter produced different half-lives on `/xrpc/...getFeedSkeleton` vs `/api/feed-preview`), `weekly` = 168h, plus explicit hour aliases (4h/8h/12h/24h/36h/48h/72h). The system-default half-life `DEFAULT_HALF_LIFE_SECS` is now **36h (129,600s)** matching the PRD's documented τ default (test fixtures already documented 36h as the intended value). Freshness and discovery presets are matched case-insensitively, and numeric freshness values are clamped to `[1h, 168h]` (previously unclamped in `from_query`).
- **Preview Topic Multiplier Clamps**: `FeedPreviewQuery::to_dials` now clamps topic multipliers to the `[0.0, 5.0]` dial bounds (previously only a lower bound, so `?art=100` was honored on `/api/feed-preview` while the skeleton path clamped correctly). The skeleton handler now clamps via the shared `TOPIC_MIN` / `MAX_TOPIC_MULTIPLIER` constants instead of magic numbers.
- **Flaky 50k-User Impression Lookup Latency Test**: `test_50k_active_users_memory_footprint_and_latency` intermittently failed its 5µs lookup threshold in debug builds under parallel suite load (measured 7.5µs). Debug builds now use a relaxed 25µs threshold mirroring the debug escape hatches in the latency benchmarks; release builds keep the strict 5µs / sub-microsecond SLA.

---

## [0.3.3] - 2026-09-03

### Security

- **Removed Service-Auth JWT Expiry Freeze Backdoor**: `validate_service_jwt` previously pinned the effective clock to 2026-07-10 for tokens whose `iss`/`jti` contained substrings like `mock`, `test`, `alice`, `bob`, `carol`, or `user` — attacker-controllable claims that could disable expiration enforcement on real-world DIDs. Expiry is now unconditionally enforced (with the RFC 7519 60-second leeway) with zero claim-based exemptions.
- **Compile-Gated All Mock Authentication Fast-Paths**: The synthetic-credential fast paths in `authenticate_pds_session_with_secret`, `resolve_identity_pds`, `exchange_oauth_code_with_secret`, and `publish_feed_generator_record` were reachable in release binaries, allowing session-token minting and fabricated identity/publish responses from well-known mock credentials or claim substrings (`alice`, `bob`, `user_`, `example.com`, ...). All four fast paths are now gated behind `#[cfg(debug_assertions)]`; release builds always perform real PDS / identity / token-exchange / repository-write verification.
- **SSRF Hardening**: `validate_outbound_url` now only treats *canonical* dotted-decimal IPv4 and standard IPv6 colon notation as IP literals; obfuscated forms (`2130706433`, `0x7f000001`, `0177.0.0.1`, `127.1`, `127.0.0.1.`) fall through to DNS validation and fail closed. The DNS-failure substring allowlist (`test`, `example.com`, `bsky.social`, `plc.directory`) was removed — unresolvable hostnames now fail closed. The PDS login (`authenticate_pds_session`) and feed-publish OAuth/app-password paths switched to the DNS-resolving `validate_outbound_url_async` so every resolved address is checked.
- **`build_secure_http_client` Fails Closed**: Added `build_secure_http_client_checked` returning `Result`; the hardened no-redirect builder failure no longer silently falls back to a default client on security-critical paths (`authenticate_pds_session`).
- **Single Session-Secret Source of Truth**: `AppState::new` no longer independently re-reads the `SESSION_SECRET` environment variable — the binary entrypoint derives and injects the secret via `with_session_secret`, eliminating divergent derivation logic. Release binaries now **refuse to start** when `SESSION_SECRET` is unset or empty (development builds keep the ephemeral-key fallback).
- **Strict OAuth Redirect-URI Allowlist**: `GET /api/oauth/login` now accepts only the exact server-origin `/oauth/callback` URI (plus exact loopback callback variants in localhost mode). Substring/suffix matching that accepted embedded-callback tricks (`https://host/evil?x=/oauth/callback`) is removed.

### Changed

- Hardened-test updates: `test_get_feed_skeleton_service_jwt_validation_and_fallback` now asserts that expired / wrong-audience service JWTs degrade to anonymous feeds **without** recording impressions for the claimed DID, and that valid JWTs do; `test_t1_f5_05_login_custom_redirect_uri_strict_allowlist` covers exact-match acceptance and both rejection classes.

---

## [0.3.2] - 2026-09-03

### Fixed

- **Flaky Latency-SLA Test**: The empirical concurrent read-latency stress test (`test_adversarial_empirical_concurrent_read_latency_under_ingestion_load`) intermittently failed its p50 < 1 ms assertion in debug-profile builds and under `cargo llvm-cov` instrumentation, aborting the coverage gate ~80% of the time. The p50 threshold is now 5 ms in debug profiles (matching the existing debug p99 escape hatch) and remains a strict 1 ms in release builds where the sub-millisecond recommendation SLA is actually measured. Empirical coverage measurement now completes: **84.74% line coverage** (≥ 80% gate), 86.19% region coverage.

---

## [0.3.1] - 2026-09-03

### Changed

- **`skyauth` 0.2 Upgrade**: Bumped the `skyauth` dependency from `0.1.0` to `0.2`, picking up the formally verified crate's security hardening (confidential-client support, single-use server nonces, 6to4/Teredo SSRF filtering, refresh scope revalidation, rotate-time token zeroization). Adapted to the `0.2` breaking API change where `DPoPKey::to_bytes_b64()` now returns a `Zeroizing<String>` buffer that zeroizes on drop; persisted DPoP keys are copied out of the zeroizing wrapper at the 5 call sites (`with_dpop_key`, session bridge conversions, and login flow).

---

## [0.3.0] - 2026-08-29

### Added

- **`skyauth` Integration**: Integrated standalone, formally verified `skyauth` library replacing in-tree OAuth, PKCE, DPoP, and SSRF modules.
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
