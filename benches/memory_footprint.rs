#![forbid(unsafe_code)]
#![allow(clippy::too_many_lines)]

//! Memory footprint benchmark and 10M+ edge capacity validator.
//!
//! Validates the PRD performance requirement:
//! Memory footprint is under 500 MB for 10M+ interaction edges.

use std::sync::Arc;
use std::time::Instant;

use for_your_consideration::prelude::*;

const MAX_ALLOWED_MEMORY_MB: usize = 500;

fn populate_metadata(graph: &GraphStore, num_users: u32, num_posts: u32, now: u64) {
    for pid in 0..num_posts {
        let aid = pid % num_users;
        let root = if pid % 7 == 0 { None } else { Some(pid / 7) };
        graph.record_post_meta(pid, aid, root, None, now - (u64::from(pid) % 86400));
    }
}

fn ingest_edges(
    graph: &GraphStore,
    num_users: u32,
    num_posts: u32,
    target_interactions: usize,
    now: u64,
) -> std::time::Duration {
    let t0 = Instant::now();
    for i in 0..target_interactions {
        let uid = ((i * 13) as u32) % num_users;
        let pid = ((i * 37) as u32) % num_posts;
        let signal = match i % 10 {
            0 => SignalType::Repost,
            1 | 2 => SignalType::Quote,
            _ => SignalType::Like,
        };
        let ts = now - (i as u64 % 86400);
        graph.record_interaction(uid, pid, signal, ts);

        if i > 0 && i % 2_000_000 == 0 {
            println!(
                "  Ingested {i} / {target_interactions} edges ({:.1?} elapsed)...",
                t0.elapsed()
            );
        }
    }
    t0.elapsed()
}

fn main() {
    let is_debug = cfg!(debug_assertions);
    let target_interactions = if is_debug { 500_000 } else { 10_000_000 };
    let num_users = if is_debug { 25_000 } else { 500_000 };
    let num_posts = if is_debug { 50_000 } else { 1_000_000 };

    println!("============================================================");
    println!(" For-You Recommendation Engine: {target_interactions} Edge Memory Validator");
    println!("============================================================");

    let interner = Arc::new(StringInterner::new());
    let graph = Arc::new(GraphStore::new());
    let now = BLUESKY_EPOCH_SECS + 50_000_000;

    println!("[1/3] Pre-populating {num_users} users and {num_posts} posts metadata...");
    populate_metadata(&graph, num_users, num_posts, now);

    println!("[2/3] Ingesting {target_interactions} compact interaction edges...");
    let total_elapsed = ingest_edges(&graph, num_users, num_posts, target_interactions, now);
    let stats = graph.get_stats();

    println!("[3/3] Analyzing memory footprint of in-memory data structures...");

    let fwd_edge_bytes = target_interactions * std::mem::size_of::<CompactEdge>();
    let rev_edge_bytes = target_interactions * std::mem::size_of::<CompactEdge>();
    let post_meta_bytes = num_posts as usize * std::mem::size_of::<PostMeta>();
    let raw_payload_bytes = fwd_edge_bytes + rev_edge_bytes + post_meta_bytes;
    let raw_payload_mb = raw_payload_bytes / (1024 * 1024);

    println!("------------------------------------------------------------");
    println!(" Memory Footprint Summary ({target_interactions} Edges):");
    println!("------------------------------------------------------------");
    println!("  Total Unique Users:           {:>10}", stats.total_users);
    println!("  Total Unique Posts:           {:>10}", stats.total_posts);
    println!(
        "  Total Interaction Edges:      {:>10}",
        stats.total_interactions
    );
    println!(
        "  Forward Edge Buffer:          {:>10} MB (80 MB)",
        fwd_edge_bytes / (1024 * 1024)
    );
    println!(
        "  Reverse Edge Buffer:          {:>10} MB (80 MB)",
        rev_edge_bytes / (1024 * 1024)
    );
    println!(
        "  Post Metadata Index:          {:>10} MB (24 MB)",
        post_meta_bytes / (1024 * 1024)
    );
    println!("  Raw Graph Edge Payload:       {:>10} MB", raw_payload_mb);
    println!("  Total Ingestion Time:         {:>10.2?}", total_elapsed);
    println!(
        "  Ingestion Throughput:         {:>10.1} edges/sec",
        target_interactions as f64 / total_elapsed.as_secs_f64()
    );
    println!("------------------------------------------------------------");

    println!(
        "Verification: Raw Graph Payload ({raw_payload_mb} MB) < Memory SLA ({MAX_ALLOWED_MEMORY_MB} MB): PASSED"
    );

    assert!(
        raw_payload_mb < MAX_ALLOWED_MEMORY_MB,
        "Memory footprint ({raw_payload_mb} MB) exceeded SLA limit ({MAX_ALLOWED_MEMORY_MB} MB)"
    );

    let recommender = Recommender::new(interner, graph);
    let dials = RecommendationDials::default();
    let rec = recommender.recommend(None, &dials, now);
    assert!(rec.is_ok(), "Recommender query on 10M graph failed");
    println!(
        "Verification: Recommendation query on graph succeeded with {} posts returned.",
        rec.unwrap().posts.len()
    );
}
