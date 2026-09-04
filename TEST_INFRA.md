# E2E Test Infra: for-your-consideration Performance and Memory Optimization

## Test Philosophy
- Opaque-box, requirement-driven testing validating sub-10ms query latency, bounded graph traversals, TTL cache hit behavior, non-blocking streaming snapshot checkpoints, and memory allocator safety.
- Methodology: Category-Partition + Boundary Value Analysis (BVA) + Pairwise Combinatorial Testing + Real-World Workload Testing.

## Feature Inventory & Test Coverage Goals
| # | Feature | Requirement | Tier 1 (Feature) | Tier 2 (Boundary) | Tier 3 (Cross-Feature) | Tier 4 (Workload) |
|---|---------|-------------|:----------------:|:-----------------:|:----------------------:|:-----------------:|
| 1 | Bounded Feed Preview Walk | R1 | 5 | 5 | ✓ | ✓ |
| 2 | Bounded Taste Twins Discovery | R1 | 5 | 5 | ✓ | ✓ |
| 3 | Velocity Pool TTL Cache | R2 | 5 | 5 | ✓ | ✓ |
| 4 | Non-Blocking Snapshot Checkpoints | R3 | 5 | 5 | ✓ | ✓ |
| 5 | Streaming Shard Snapshot Serialization | R3 | 5 | 5 | ✓ | ✓ |
| 6 | Heap Memory & Allocator Configuration | R4 | 5 | 5 | ✓ | ✓ |
| 7 | Safety Invariants (Zero Unsafe / Zero Panic) | R5 | 5 | 5 | ✓ | ✓ |

## Test Architecture
- Test Runner: `cargo test --all-targets`
- Lints & Verification: `cargo fmt --all -- --check`, `cargo clippy --all-targets -- -D warnings`, `cargo deny check`, `cargo llvm-cov --all-targets --fail-under-lines 80 --summary-only`
- Integration Test Directory: `tests/` (53 suites, ~1,150 test cases)
  - `tests/recommender_api_tests.rs`: API response correctness and latency thresholds.
  - `tests/preview_challenger_tests.rs`: High-load candidate matrix and bounds verification.
  - `tests/snapshot_adversarial_durability_tests.rs`: Streaming snapshot roundtrip (multi-chunk), CRC integrity, truncation rejection, concurrent-save stress, and low memory verification.
  - `tests/snapshot_v2_tests.rs` / `tests/snapshot_tests.rs`: Snapshot format roundtrips, preference persistence, and backward compatibility.
  - `tests/m2_m4_performance_and_streaming_tests.rs`: TTL cache behavior, streaming persistence under load, and allocator configuration.
  - `tests/tier5_adversarial_hardening_tests.rs`: Safety invariants (zero-unsafe / zero-panic) and hardening checks.
- Unit tests live in `#[cfg(test)]` modules inside each `src/*.rs` file.

## Coverage Thresholds
- Tier 1: >= 5 tests per feature
- Tier 2: >= 5 tests per feature (boundary and extreme conditions)
- Tier 3: Pairwise coverage of major feature combinations
- Tier 4: >= 5 realistic high-volume firehose and concurrent query scenarios
- Minimum total coverage threshold: >= 80% line coverage via `cargo llvm-cov`.
