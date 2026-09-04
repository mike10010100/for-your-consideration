# 🤖 Agent Coding & Engineering Handover Guide

Welcome, Agent! This document is designed specifically for AI coding assistants (Antigravity, Claude, Cursor, Copilot, etc.) interacting with this codebase.

---

## 🎯 Repository Standards & Reference Blueprint

This project is built following the **Production-Grade Rust Best Practices & Architecture Standards** defined in the user's reference repository:
- **Reference Repo**: [`rust-best-practices`](/Users/mike10010100/git/rust-best-practices) (or `https://github.com/mike10010100/rust-best-practices`)
- **Architecture Guide**: [`BEST_PRACTICES.md`](/Users/mike10010100/git/rust-best-practices/BEST_PRACTICES.md)
- **Tooling Blueprint**: [`TOOLING.md`](/Users/mike10010100/git/rust-best-practices/TOOLING.md)
- **Agent Blueprint**: [`agents.md`](/Users/mike10010100/git/rust-best-practices/agents.md)

When working in this repository:
- Treat every safety gate and architectural pattern from `rust-best-practices` as a strict non-negotiable requirement.
- Never lower quality gates, weaken lint rules, or bypass defensive error handling for convenience.
- Any new features, modules, or refactors must adhere to the same uncompromising resilience standard.

---

## 🛡️ Core Non-Negotiable Invariants

### 1. Zero Unsafe Code
The crate root ([`src/lib.rs`](src/lib.rs) and [`src/main.rs`](src/main.rs)) enforces:
```rust
#![forbid(unsafe_code)]
```
Never attempt to use `unsafe`, weaken this attribute, or introduce dependencies that circumvent compiler safety guarantees.

### 2. Strict Crate-Root Safety Guard
Both crate roots enforce the strict compiler lint safety guard:
```rust
#![deny(
    clippy::all,
    clippy::unwrap_used,     // Deny unwrap(), force explicit error handling
    clippy::expect_used,     // Deny expect(), force structured errors
    clippy::panic,           // Deny panic!, force error bubbling
    clippy::todo,            // Deny todo! placeholders in production
    clippy::unimplemented,   // Deny unimplemented! macros
    missing_docs,            // Enforce public API documentation
    rust_2018_idioms         // Use modern Rust idioms
)]
```

### 3. Zero Production Panics & Typed Errors
- **Banned in production**: `.unwrap()`, `.expect()`, `panic!`, `todo!`, `unimplemented!`.
- All fallible operations must return a strongly typed `Result<T, FeedError>` using variants defined in [`src/error.rs`](src/error.rs).
- Use `?`, `match`, or `if let` to propagate errors safely.
- In test modules (`#[cfg(test)]`), allow unwrap via `#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic, missing_docs)]`.

### 4. Defensive Concurrency, Locks & Time
- **Clock-Warp Safety**: Always compute elapsed time using `now.saturating_duration_since(earlier)` or `.map_or(0, ...)`. Never use raw `.duration_since()` without fallback as monotonic clocks can jump backwards under VM or NTP syncs.
- **Drift-Free Scheduling**: Recurring tasks (e.g. periodic snapshots, graph pruning) must calculate next runs relative to the previous anchor timestamp or use `tokio::time::interval`, not relative `Instant::now() + delay`.
- **Never Hold Locks Across `.await` Points**: Synchronous mutex or `RwLock` guards must always be dropped before executing any `.await`, `sleep()`, or network I/O.
- **Sharded State Partitioning**: High-concurrency structures (`GraphStore`, `ImpressionStore`, `UserPreferencesStore`) use **64 independent `RwLock` shards** to eliminate lock contention under multi-threaded load.
- **Task Leak Prevention & Cancellation**: All background tasks must be tracked in a managed `tokio::task::JoinSet` tied to a `CancellationToken`. On shutdown or timeout, tasks must be cleanly aborted and joined.

### 5. 100% Documentation Coverage
- All public structs, fields, constants, enums, modules, and functions must have descriptive documentation comments (`missing_docs` is denied).
- Bare URLs in documentation must be enclosed in angle brackets (e.g. `<https://bsky.social>`).

### 6. Semantic Versioning, CHANGELOG & Automated Release Invariants
- **Service Deployment Model (Never Publish to Crates.io)**: `for-your-consideration` is a standalone backend service, not a reusable library crate. [`Cargo.toml`](Cargo.toml) enforces `publish = false`. Never attempt to publish this service to crates.io or public cargo registries.
- **Manual CHANGELOG Curation Required**: The CI/CD automation does **not** generate or modify the changelog automatically. AI agents and human contributors are strictly required to manually curate and document all changes for every PR in [`CHANGELOG.md`](CHANGELOG.md) under `## [X.Y.Z] - YYYY-MM-DD` following [Keep a Changelog](https://keepachangelog.com/).
- **Semantic Versioning Bumps**: Every Pull Request modifying application code, features, bug fixes, or architecture **must bump the package version in [`Cargo.toml`](Cargo.toml)** according to [Semantic Versioning (SemVer 2.0.0)](https://semver.org/):
  - **Patch** (`0.4.x` → `0.4.y`): Backward-compatible bug fixes, security patches, performance tuning, and minor refactors.
  - **Minor** (`0.x.0` → `0.y.0`): New features, algorithm dials, snapshot schema changes, or significant new capabilities.
  - **Major** (`x.0.0` → `y.0.0`): Breaking public API changes or architecture overhauls.
- **Automated Version & CHANGELOG Verification**: The `./scripts/check_version_bump.sh` verification script is enforced in CI on every PR. CI fails closed if the version in `Cargo.toml` is not greater than the base branch or if the version entry is absent from `CHANGELOG.md`.
- **Automated GitHub Release Lifecycle on Merge**: When a PR is merged into `main`, GitHub Actions (`release` job in `.github/workflows/ci.yml`) automatically:
  1. Runs and requires 100% pass rate across all quality gates (`fmt-and-clippy`, `test`, `coverage`, `security-audit`, `docker-build`).
  2. Awaits successful conclusion of all required CodeQL static security scans (`rust`, `javascript-typescript`, `actions`).
  3. Checks if release tag `vX.Y.Z` already exists, and if not, automatically creates the GitHub Release tag `vX.Y.Z` titled `vX.Y.Z - Production Release` populated from the curated [`CHANGELOG.md`](CHANGELOG.md).

---

## ⚡ Mandatory Pre-Completion Checklist

Before reporting any work as complete, you **must execute and pass every step** of this verification pipeline:

```bash
# 1. Check code formatting
cargo fmt --all -- --check

# 2. Check strict clippy rules (must have 0 warnings with -D warnings)
cargo clippy --all-targets -- -D warnings

# 3. Run all unit and integration test suites
cargo test --all-targets

# 4. Run documentation tests
cargo test --doc

# 5. Dependency security & policy scan
cargo deny check

# 6. Test coverage gate (must maintain >= 80% line coverage)
cargo llvm-cov --all-targets --fail-under-lines 80 --summary-only

# 7. Verify Semantic Version bump & CHANGELOG entry
./scripts/check_version_bump.sh
```
