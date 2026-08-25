#![forbid(unsafe_code)]
#![allow(clippy::too_many_lines)]

//! Synthetic recommendation query latency benchmark.
//!
//! Validates the PRD performance requirement:
//! Recommendation query latency (p99) is under 2.0 ms on synthetic test graphs.

use std::sync::Arc;
use std::time::Instant;

use compact_str::CompactString;
use for_your_consideration::prelude::*;

const NUM_USERS: usize = 10_000;
const NUM_POSTS: usize = 50_000;
const NUM_INTERACTIONS: usize = 500_000;
const P99_LATENCY_THRESHOLD_MICROS: u128 = 2_000; // 2.0 ms in release mode

fn build_benchmark_graph() -> (
    Arc<StringInterner>,
    Arc<GraphStore>,
    Arc<Recommender>,
    Vec<CompactString>,
) {
    let interner = Arc::new(StringInterner::new());
    let graph = Arc::new(GraphStore::new());
    let now = BLUESKY_EPOCH_SECS + 50_000_000;

    let mut user_dids = Vec::with_capacity(NUM_USERS);
    for i in 0..NUM_USERS {
        let did = CompactString::new(format!("did:plc:user_{i:06}"));
        interner.intern(&did);
        user_dids.push(did);
    }

    for i in 0..NUM_POSTS {
        let author_idx = i % NUM_USERS;
        let author_did = &user_dids[author_idx];
        let uri = CompactString::new(format!("at://{author_did}/app.bsky.feed.post/{i:08}"));
        let pid = interner.intern(&uri);
        let aid = interner.lookup_id(author_did).unwrap();
        let root_id = if i % 5 == 0 {
            None
        } else {
            Some(interner.intern(&format!(
                "at://{author_did}/app.bsky.feed.post/root_{}",
                i / 5
            )))
        };
        let created_at = now - (i as u64 % (86400 * 3));
        graph.record_post_meta(pid, aid, root_id, None, created_at);
    }

    // Interactions
    for i in 0..NUM_INTERACTIONS {
        let uid = (i * 17) as u32 % NUM_USERS as u32;
        let pid = (i * 31) as u32 % NUM_POSTS as u32;
        let signal = match i % 6 {
            0 => SignalType::Repost,
            1 | 2 => SignalType::Quote,
            _ => SignalType::Like,
        };
        let ts = now - (i as u64 % (86400 * 2));
        graph.record_interaction(uid, pid, signal, ts);
    }

    // Follows
    for i in 0..NUM_USERS {
        let uid = i as u32;
        for f in 1..=5 {
            let target_uid = (uid + f * 100) % NUM_USERS as u32;
            graph.record_follow(uid, target_uid);
        }
    }

    let recommender = Arc::new(Recommender::new(Arc::clone(&interner), Arc::clone(&graph)));
    (interner, graph, recommender, user_dids)
}

fn run_warmup(
    recommender: &Recommender,
    user_dids: &[CompactString],
    dials: &RecommendationDials,
    now_secs: u64,
    count: usize,
) {
    for i in 0..count {
        let viewer_did = if i % 10 == 0 {
            None
        } else {
            Some(user_dids[i % NUM_USERS].as_str())
        };
        let _ = recommender.recommend(viewer_did, dials, now_secs);
    }
}

fn execute_queries(
    recommender: &Recommender,
    user_dids: &[CompactString],
    dials: &RecommendationDials,
    now_secs: u64,
    count: usize,
) -> (Vec<u128>, std::time::Duration) {
    let mut latencies_micros: Vec<u128> = Vec::with_capacity(count);
    let bench_start = Instant::now();

    for i in 0..count {
        let viewer_did = match i % 20 {
            0 => None, // 5% unauthenticated / cold (Tier 3)
            1 | 2 => Some(user_dids[(NUM_USERS - 1) - (i % 50)].as_str()), // 10% new users (Tier 2)
            _ => Some(user_dids[i % 500].as_str()), // 85% active users (Tier 1)
        };

        let t0 = Instant::now();
        let rec = recommender.recommend(viewer_did, dials, now_secs);
        let elapsed_micros = t0.elapsed().as_micros();
        latencies_micros.push(elapsed_micros);

        assert!(rec.is_ok(), "Recommendation query failed");
    }

    (latencies_micros, bench_start.elapsed())
}

fn print_report(mut latencies: Vec<u128>, duration: std::time::Duration, is_debug: bool) {
    latencies.sort_unstable();

    let count = latencies.len();
    let min = latencies[0];
    let p50 = latencies[count * 50 / 100];
    let p90 = latencies[count * 90 / 100];
    let p95 = latencies[count * 95 / 100];
    let p99 = latencies[count * 99 / 100];
    let p999 = latencies[count * 999 / 1000];
    let max = latencies[count - 1];
    let sum: u128 = latencies.iter().sum();
    let mean = sum as f64 / count as f64;
    let throughput = count as f64 / duration.as_secs_f64();

    println!("------------------------------------------------------------");
    println!(" Latency Benchmark Results ({count} queries, debug={is_debug}):");
    println!("------------------------------------------------------------");
    println!(
        "  Min Latency:    {:>8.3} ms ({:>6} µs)",
        min as f64 / 1000.0,
        min
    );
    println!(
        "  p50 (Median):   {:>8.3} ms ({:>6} µs)",
        p50 as f64 / 1000.0,
        p50
    );
    println!(
        "  p90:            {:>8.3} ms ({:>6} µs)",
        p90 as f64 / 1000.0,
        p90
    );
    println!(
        "  p95:            {:>8.3} ms ({:>6} µs)",
        p95 as f64 / 1000.0,
        p95
    );
    println!(
        "  p99:            {:>8.3} ms ({:>6} µs)",
        p99 as f64 / 1000.0,
        p99
    );
    println!(
        "  p99.9:          {:>8.3} ms ({:>6} µs)",
        p999 as f64 / 1000.0,
        p999
    );
    println!(
        "  Max Latency:    {:>8.3} ms ({:>6} µs)",
        max as f64 / 1000.0,
        max
    );
    println!(
        "  Mean Latency:   {:>8.3} ms ({:>6.1} µs)",
        mean / 1000.0,
        mean
    );
    println!("  Throughput:     {:>8.1} queries/sec", throughput);
    println!("------------------------------------------------------------");

    if is_debug {
        println!("Note: Running in unoptimized debug test mode. Release mode (cargo bench) strictly asserts < 2.0ms SLA.");
    } else {
        println!(
            "Verification: p99 latency ({:.3} ms) <= SLA limit ({:.1} ms): {}",
            p99 as f64 / 1000.0,
            P99_LATENCY_THRESHOLD_MICROS as f64 / 1000.0,
            if p99 < P99_LATENCY_THRESHOLD_MICROS {
                "PASSED"
            } else {
                "FAILED"
            }
        );
        assert!(
            p99 < P99_LATENCY_THRESHOLD_MICROS,
            "p99 latency ({p99} µs) exceeded SLA threshold ({P99_LATENCY_THRESHOLD_MICROS} µs)"
        );
    }
}

fn main() {
    println!("============================================================");
    println!(" For-You Recommendation Engine: Latency Benchmark (p99 < 2ms)");
    println!("============================================================");

    println!("[1/3] Building synthetic graph (10k users, 50k posts, 500k edges)...");
    let start_build = Instant::now();
    let (_interner, graph, recommender, user_dids) = build_benchmark_graph();
    let stats = graph.get_stats();
    println!(
        "Graph built in {:.2?} (Nodes: {}, Edges: {}, Follows: {})",
        start_build.elapsed(),
        stats.total_users + stats.total_posts,
        stats.total_interactions,
        stats.total_follows
    );

    let now_secs = BLUESKY_EPOCH_SECS + 50_000_000;
    let dials = RecommendationDials {
        half_life_secs: 36.0 * 3600.0,
        explore_ratio: 0.15,
        explain: false,
        limit: 30,
        cursor: None,
        ..Default::default()
    };

    let is_debug = cfg!(debug_assertions);
    let warmup_queries = if is_debug { 100 } else { 1_000 };
    let bench_queries = if is_debug { 500 } else { 10_000 };

    println!("[2/3] Warming up JIT and caches ({warmup_queries} queries)...");
    run_warmup(&recommender, &user_dids, &dials, now_secs, warmup_queries);

    println!("[3/3] Running {bench_queries} recommendation queries across tiers...");
    let (latencies, duration) =
        execute_queries(&recommender, &user_dids, &dials, now_secs, bench_queries);

    print_report(latencies, duration, is_debug);
}
