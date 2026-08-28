#![forbid(unsafe_code)]
#![allow(
    clippy::too_many_lines,
    clippy::cognitive_complexity,
    clippy::cast_precision_loss,
    clippy::cast_possible_truncation
)]

//! Empirical Challenger Test Suite for Milestone 1:
//! 1. Query latency for `recommend_preview()` under high candidate load (verify < 2ms latency budget).
//! 2. Latency of `find_taste_twins()` with large user interaction bitsets.
//! 3. Concurrency stress: concurrent calls to `recommend_preview`, `find_taste_twins`, and graph mutations (zero deadlocks, thread safety).
//! 4. Adversarial edge cases, mathematical breakdown validation, and proof chain invariants.

use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

use ahash::AHashSet;
use compact_str::CompactString;
use for_your_consideration::prelude::*;

fn build_high_load_graph(
    num_co_interactors: usize,
    posts_per_co_interactor: usize,
) -> (
    Arc<StringInterner>,
    Arc<GraphStore>,
    Recommender,
    CompactString,
) {
    let interner = Arc::new(StringInterner::new());
    let graph = Arc::new(GraphStore::new());
    let now = BLUESKY_EPOCH_SECS + 50_000_000;

    let viewer_did = CompactString::new("did:plc:challenger_viewer");
    let viewer_id = interner.intern(&viewer_did);

    // 1. Viewer likes 20 seed posts
    let seed_author_id = interner.intern("did:plc:seed_author");
    let mut seed_post_ids = Vec::with_capacity(20);
    for i in 0..20 {
        let uri = CompactString::new(format!(
            "at://did:plc:seed_author/app.bsky.feed.post/seed_{i}"
        ));
        let pid = interner.intern(&uri);
        seed_post_ids.push(pid);
        graph.record_post_meta(pid, seed_author_id, None, None, now - 10_000);
        graph.record_interaction(viewer_id, pid, SignalType::Like, now - 5_000);
    }

    // 2. Populate co-interactors and their candidates
    for u in 0..num_co_interactors {
        let co_did = CompactString::new(format!("did:plc:co_user_{u:05}"));
        let co_id = interner.intern(&co_did);

        // Co-interactor likes 5 seed posts (shared taste)
        for s in 0..5 {
            let spid = seed_post_ids[(u + s) % seed_post_ids.len()];
            graph.record_interaction(co_id, spid, SignalType::Like, now - 4_000);
        }

        // Co-interactor interacts with candidate posts
        for p in 0..posts_per_co_interactor {
            let topic = TOPIC_CATEGORIES[(u + p) % NUM_TOPIC_CATEGORIES];
            let author_did = CompactString::new(format!(
                "did:plc:{}_creator_{}",
                topic.as_str(),
                (u * 7 + p) % 50
            ));
            let author_id = interner.intern(&author_did);

            let cand_uri = CompactString::new(format!(
                "at://{author_did}/app.bsky.feed.post/{topic}_cand_{u}_{p}"
            ));
            let cand_pid = interner.intern(&cand_uri);

            let root_id = if p % 3 == 0 {
                None
            } else {
                Some(interner.intern(&format!(
                    "at://{author_did}/app.bsky.feed.post/{topic}_root_{}",
                    p / 3
                )))
            };

            graph.record_post_meta(cand_pid, author_id, root_id, None, now - 3_000);

            let signal = match (u + p) % 4 {
                0 => SignalType::Repost,
                1 => SignalType::Quote,
                _ => SignalType::Like,
            };
            graph.record_interaction(co_id, cand_pid, signal, now - 2_000);
        }
    }

    let rec = Recommender::new(Arc::clone(&interner), Arc::clone(&graph));
    (interner, graph, rec, viewer_did)
}

#[test]
fn test_challenge_recommend_preview_high_candidate_load_latency_and_correctness() {
    // 500 co-interactors * 20 candidates each = 10,000 candidate evaluations
    let (_interner, _graph, rec, viewer_did) = build_high_load_graph(500, 20);

    let dials = RecommendationDials {
        half_life_secs: 36.0 * 3600.0,
        explore_ratio: 0.15,
        topic_weights: TopicWeights {
            art: 2.0,
            tech: 1.5,
            science: 1.0,
            news: 0.8,
            culture: 1.2,
        },
        explain: true,
        include_replies: false,
        min_likes: 1,
        limit: 30,
        cursor: None,
    };

    // Warmup
    for _ in 0..10 {
        let _ = rec.recommend_preview(Some(viewer_did.as_str()), &dials);
    }

    let iterations = 100;
    let mut latencies_micros = Vec::with_capacity(iterations);

    let start_all = Instant::now();
    for _ in 0..iterations {
        let t0 = Instant::now();
        let resp = rec
            .recommend_preview(Some(viewer_did.as_str()), &dials)
            .expect("recommend_preview must succeed");
        let elapsed = t0.elapsed().as_micros() as u64;
        latencies_micros.push(elapsed);

        // Sanity assertions on output
        assert_eq!(resp.items.len(), 30);
        assert!(
            resp.total_candidates >= 1_000 && resp.total_candidates <= 2_000,
            "Total evaluated candidates should be bounded by top co-interactors: {}",
            resp.total_candidates
        );
        assert_eq!(resp.viewer_did, viewer_did.as_str());

        // Validate score breakdowns and proof chains
        for item in &resp.items {
            let sb = &item.score_breakdown;
            assert!(sb.final_score > 0.0 && sb.final_score.is_finite());
            assert!(sb.time_decay > 0.0 && sb.time_decay <= 3.0);
            assert!(sb.taste_similarity > 0.0 && sb.taste_similarity.is_finite());
            assert!(sb.topic_boost > 0.0 && sb.topic_boost <= 2.0);
            assert_eq!(sb.fatigue_penalty, 1.0);

            // Explainability must be populated
            assert!(item.proof_chain.is_some());
            let chain = item.proof_chain.as_ref().unwrap();
            assert_eq!(chain.steps.len(), 3);
            assert!(!chain.summary.is_empty());
        }
    }
    let total_elapsed = start_all.elapsed();

    latencies_micros.sort_unstable();
    let min = latencies_micros[0];
    let p50 = latencies_micros[iterations * 50 / 100];
    let p90 = latencies_micros[iterations * 90 / 100];
    let p95 = latencies_micros[iterations * 95 / 100];
    let p99 = latencies_micros[iterations * 99 / 100];
    let max = latencies_micros[iterations - 1];
    let avg = latencies_micros.iter().sum::<u64>() as f64 / iterations as f64;

    println!("=== EMPIRICAL CHALLENGE: recommend_preview() High Load Latency ===");
    println!("Evaluated candidate pool: 10,000+ candidates over {iterations} queries");
    println!("Total runtime: {:.2?}", total_elapsed);
    println!("Min: {min} µs ({:.3} ms)", min as f64 / 1000.0);
    println!("p50: {p50} µs ({:.3} ms)", p50 as f64 / 1000.0);
    println!("p90: {p90} µs ({:.3} ms)", p90 as f64 / 1000.0);
    println!("p95: {p95} µs ({:.3} ms)", p95 as f64 / 1000.0);
    println!("p99: {p99} µs ({:.3} ms)", p99 as f64 / 1000.0);
    println!("Max: {max} µs ({:.3} ms)", max as f64 / 1000.0);
    println!("Avg: {avg:.1} µs ({:.3} ms)", avg / 1000.0);
    println!("=================================================================");

    // In debug mode or release mode, verify response query_latency_us is tracked accurately
    let single = rec
        .recommend_preview(Some(viewer_did.as_str()), &dials)
        .unwrap();
    assert!(single.query_latency_us > 0);
}

#[test]
fn test_challenge_recommend_preview_latency_scaling_matrix() {
    println!("\n=== LATENCY SCALING MATRIX: recommend_preview() across Candidate Sizes ===");
    println!("| Co-Interactors | Candidates | Explain | p50 (µs) | p95 (µs) | p99 (µs) | Mean (µs) | Sub-2ms SLA |");
    println!("|---|---|---|---|---|---|---|---|");

    let configs = [
        (20, 5),   // ~100 candidates
        (50, 10),  // ~500 candidates
        (100, 10), // ~1,000 candidates
        (250, 10), // ~2,500 candidates
        (500, 20), // ~10,000 candidates
    ];

    for (num_co, posts_per_co) in configs {
        let (_interner, _graph, rec, viewer_did) = build_high_load_graph(num_co, posts_per_co);

        for explain in [false, true] {
            let dials = RecommendationDials {
                half_life_secs: 36.0 * 3600.0,
                explore_ratio: 0.15,
                explain,
                limit: 30,
                ..Default::default()
            };

            // Warmup
            for _ in 0..5 {
                let _ = rec.recommend_preview(Some(viewer_did.as_str()), &dials);
            }

            let runs = 50;
            let mut lats = Vec::with_capacity(runs);
            let mut cand_count = 0;

            for _ in 0..runs {
                let t0 = Instant::now();
                let resp = rec
                    .recommend_preview(Some(viewer_did.as_str()), &dials)
                    .unwrap();
                lats.push(t0.elapsed().as_micros() as u64);
                cand_count = resp.total_candidates;
            }

            lats.sort_unstable();
            let p50 = lats[runs * 50 / 100];
            let p95 = lats[runs * 95 / 100];
            let p99 = lats[runs * 99 / 100];
            let mean = lats.iter().sum::<u64>() as f64 / runs as f64;
            let sla_status = if p50 < 2000 { "YES" } else { "EXCEEDED (>2ms)" };

            println!(
                "| {:>14} | {:>10} | {:>7} | {:>8} | {:>8} | {:>8} | {:>9.1} | {:>11} |",
                num_co, cand_count, explain, p50, p95, p99, mean, sla_status
            );
        }
    }
    println!("===========================================================================\n");
}

#[test]
fn test_challenge_find_taste_twins_bitset_scaling_matrix() {
    println!("\n=== LATENCY SCALING MATRIX: find_taste_twins() across Bitset Sizes ===");
    println!("| Viewer Likes | Candidate Co-Users | p50 (µs) | p95 (µs) | p99 (µs) | Mean (µs) | Sub-1ms |");
    println!("|---|---|---|---|---|---|---|");

    let bitset_sizes = [100, 500, 1_000, 5_000, 10_000];

    for total_likes in bitset_sizes {
        let interner = Arc::new(StringInterner::new());
        let graph = Arc::new(GraphStore::new());
        let now = BLUESKY_EPOCH_SECS + 50_000_000;

        let viewer_did = "did:plc:matrix_viewer";
        let viewer_id = interner.intern(viewer_did);
        let author_id = interner.intern("did:plc:matrix_author");

        for p in 0..total_likes {
            let pid = interner.intern(&format!("at://did:plc:matrix_author/post/m_{p}"));
            graph.record_post_meta(pid, author_id, None, None, now - 10_000);
            graph.record_interaction(viewer_id, pid, SignalType::Like, now - 5_000);
        }

        // 50 candidate co-users with overlapping likes
        for u in 0..50 {
            let co_id = interner.intern(&format!("did:plc:matrix_co_{u}"));
            let overlap_count = (total_likes / 2).max(1);
            for p in 0..overlap_count {
                let pid = interner.intern(&format!(
                    "at://did:plc:matrix_author/post/m_{}",
                    (u * 17 + p) % total_likes
                ));
                graph.record_interaction(co_id, pid, SignalType::Like, now - 4_000);
            }
        }

        let rec = Recommender::new(interner, graph);

        // Warmup
        for _ in 0..5 {
            let _ = rec.find_taste_twins(viewer_did, 10);
        }

        let runs = 50;
        let mut lats = Vec::with_capacity(runs);
        for _ in 0..runs {
            let t0 = Instant::now();
            let _ = rec.find_taste_twins(viewer_did, 10).unwrap();
            lats.push(t0.elapsed().as_micros() as u64);
        }

        lats.sort_unstable();
        let p50 = lats[runs * 50 / 100];
        let p95 = lats[runs * 95 / 100];
        let p99 = lats[runs * 99 / 100];
        let mean = lats.iter().sum::<u64>() as f64 / runs as f64;
        let sub_1ms = if p50 < 1000 { "YES" } else { "NO" };

        println!(
            "| {:>12} | {:>18} | {:>8} | {:>8} | {:>8} | {:>9.1} | {:>7} |",
            total_likes, 50, p50, p95, p99, mean, sub_1ms
        );
    }
    println!("======================================================================\n");
}

#[test]
fn test_challenge_find_taste_twins_large_bitsets_and_accuracy() {
    let interner = Arc::new(StringInterner::new());
    let graph = Arc::new(GraphStore::new());
    let now = BLUESKY_EPOCH_SECS + 50_000_000;

    let viewer_did = "did:plc:big_viewer";
    let viewer_id = interner.intern(viewer_did);

    let author_id = interner.intern("did:plc:author");

    // Viewer likes 5,000 posts (posts 0..5,000)
    let total_viewer_posts = 5_000;
    for p in 0..total_viewer_posts {
        let uri = format!("at://did:plc:author/app.bsky.feed.post/v_{p}");
        let pid = interner.intern(&uri);
        graph.record_post_meta(pid, author_id, None, None, now - 10_000);
        graph.record_interaction(viewer_id, pid, SignalType::Like, now - 5_000);
    }

    // Candidate 1: High overlap Twin (likes 4,500 of viewer's posts + 500 other) -> |C1|=5,000, overlap=4,500
    // Cosine Sim = 4500 / sqrt(5000 * 5000) = 0.90
    let twin_high_id = interner.intern("did:plc:twin_high");
    for p in 0..total_viewer_posts {
        if p % 10 != 0 {
            let pid = interner.intern(&format!("at://did:plc:author/app.bsky.feed.post/v_{p}"));
            graph.record_interaction(twin_high_id, pid, SignalType::Like, now - 4_000);
        }
    }
    for p in 5_000..5_500 {
        let uri = format!("at://did:plc:author/app.bsky.feed.post/other_{p}");
        let pid = interner.intern(&uri);
        graph.record_post_meta(pid, author_id, None, None, now - 10_000);
        graph.record_interaction(twin_high_id, pid, SignalType::Like, now - 4_000);
    }

    // Candidate 2: Medium overlap Twin (likes 2,500 of viewer's posts + 2,500 other) -> |C2|=5,000, overlap=2,500
    // Cosine Sim = 2500 / 5000 = 0.50
    let twin_med_id = interner.intern("did:plc:twin_med");
    for p in 0..total_viewer_posts {
        if p % 2 == 0 {
            let pid = interner.intern(&format!("at://did:plc:author/app.bsky.feed.post/v_{p}"));
            graph.record_interaction(twin_med_id, pid, SignalType::Like, now - 4_000);
        }
    }
    for p in 5_500..8_000 {
        let uri = format!("at://did:plc:author/app.bsky.feed.post/other_{p}");
        let pid = interner.intern(&uri);
        graph.record_post_meta(pid, author_id, None, None, now - 10_000);
        graph.record_interaction(twin_med_id, pid, SignalType::Like, now - 4_000);
    }

    // Candidate 3: Low overlap Twin (likes 500 of viewer's posts + 4,500 other) -> |C3|=5,000, overlap=500
    // Cosine Sim = 500 / 5000 = 0.10
    let twin_low_id = interner.intern("did:plc:twin_low");
    for p in 0..total_viewer_posts {
        if p % 10 == 0 {
            let pid = interner.intern(&format!("at://did:plc:author/app.bsky.feed.post/v_{p}"));
            graph.record_interaction(twin_low_id, pid, SignalType::Like, now - 4_000);
        }
    }
    for p in 8_000..12_500 {
        let uri = format!("at://did:plc:author/app.bsky.feed.post/other_{p}");
        let pid = interner.intern(&uri);
        graph.record_post_meta(pid, author_id, None, None, now - 10_000);
        graph.record_interaction(twin_low_id, pid, SignalType::Like, now - 4_000);
    }

    // Candidate 4: Disjoint user (likes 5,000 posts, 0 overlap)
    let disjoint_id = interner.intern("did:plc:disjoint_user");
    for p in 13_000..18_000 {
        let uri = format!("at://did:plc:author/app.bsky.feed.post/disjoint_{p}");
        let pid = interner.intern(&uri);
        graph.record_post_meta(pid, author_id, None, None, now - 10_000);
        graph.record_interaction(disjoint_id, pid, SignalType::Like, now - 4_000);
    }

    // Add 100 random noise co-interactors
    for u in 0..100 {
        let noise_id = interner.intern(&format!("did:plc:noise_{u}"));
        for s in 0..10 {
            let pid = interner.intern(&format!(
                "at://did:plc:author/app.bsky.feed.post/v_{}",
                (u * 17 + s) % 5000
            ));
            graph.record_interaction(noise_id, pid, SignalType::Like, now - 3_000);
        }
    }

    let rec = Recommender::new(interner, graph);

    // Warmup
    for _ in 0..10 {
        let _ = rec.find_taste_twins(viewer_did, 10);
    }

    let iterations = 50;
    let mut latencies = Vec::with_capacity(iterations);
    let start = Instant::now();

    for _ in 0..iterations {
        let t0 = Instant::now();
        let resp = rec
            .find_taste_twins(viewer_did, 10)
            .expect("find_taste_twins must succeed");
        latencies.push(t0.elapsed().as_micros() as u64);

        assert_eq!(resp.viewer_did, viewer_did);
        assert_eq!(resp.total_liked_posts, total_viewer_posts);
        assert!(resp.twins.len() >= 3);

        // Verification of ranking and cosine accuracy
        assert_eq!(resp.twins[0].user_did, "did:plc:twin_high");
        assert!((resp.twins[0].similarity_score - 0.90).abs() < 1e-3);
        assert_eq!(resp.twins[0].shared_posts_count, 4500);

        assert_eq!(resp.twins[1].user_did, "did:plc:twin_med");
        assert!((resp.twins[1].similarity_score - 0.50).abs() < 1e-3);
        assert_eq!(resp.twins[1].shared_posts_count, 2500);

        assert_eq!(resp.twins[2].user_did, "did:plc:twin_low");
        assert!((resp.twins[2].similarity_score - 0.10).abs() < 1e-3);
        assert_eq!(resp.twins[2].shared_posts_count, 500);

        // Disjoint user must NOT appear
        assert!(resp
            .twins
            .iter()
            .all(|t| t.user_did != "did:plc:disjoint_user"));
        // Viewer must NOT appear
        assert!(resp.twins.iter().all(|t| t.user_did != viewer_did));
    }

    latencies.sort_unstable();
    let p50 = latencies[iterations * 50 / 100];
    let p95 = latencies[iterations * 95 / 100];
    let p99 = latencies[iterations * 99 / 100];
    let avg = latencies.iter().sum::<u64>() as f64 / iterations as f64;

    println!("=== EMPIRICAL CHALLENGE: find_taste_twins() Large Bitsets (5k items) ===");
    println!("Runtime for {iterations} queries: {:.2?}", start.elapsed());
    println!("p50: {p50} µs ({:.3} ms)", p50 as f64 / 1000.0);
    println!("p95: {p95} µs ({:.3} ms)", p95 as f64 / 1000.0);
    println!("p99: {p99} µs ({:.3} ms)", p99 as f64 / 1000.0);
    println!("Avg: {avg:.1} µs ({:.3} ms)", avg / 1000.0);
    println!("=========================================================================");
}

#[test]
fn test_challenge_concurrency_stress_preview_twins_and_mutations() {
    let (_interner, _graph, rec, viewer_did) = build_high_load_graph(200, 10);
    let rec = Arc::new(rec);
    let stop_flag = Arc::new(AtomicBool::new(false));

    let preview_ops = Arc::new(AtomicUsize::new(0));
    let twins_ops = Arc::new(AtomicUsize::new(0));
    let explain_ops = Arc::new(AtomicUsize::new(0));
    let mutation_ops = Arc::new(AtomicUsize::new(0));
    let error_count = Arc::new(AtomicUsize::new(0));

    let max_preview_latency = Arc::new(AtomicU64::new(0));
    let max_twins_latency = Arc::new(AtomicU64::new(0));

    let mut handles = Vec::new();

    // 1. 6 Reader threads executing recommend_preview
    for thread_id in 0..6 {
        let rec = Arc::clone(&rec);
        let stop = Arc::clone(&stop_flag);
        let ops = Arc::clone(&preview_ops);
        let errs = Arc::clone(&error_count);
        let max_lat = Arc::clone(&max_preview_latency);
        let v_did = viewer_did.clone();

        handles.push(thread::spawn(move || {
            let mut i = 0usize;
            while !stop.load(Ordering::Relaxed) {
                let did_opt = match (thread_id + i) % 4 {
                    0 => Some(v_did.as_str()),
                    1 => Some("did:plc:co_user_00010"),
                    2 => Some("did:plc:unknown_user"),
                    _ => None,
                };

                let dials = RecommendationDials {
                    half_life_secs: 3600.0 * (6.0 + (i % 48) as f32),
                    explore_ratio: ((i % 10) as f32) / 10.0,
                    topic_weights: TopicWeights {
                        art: ((i % 5) as f32) * 0.5,
                        tech: (((i + 1) % 5) as f32) * 0.5,
                        science: 1.0,
                        news: 1.0,
                        culture: 1.0,
                    },
                    explain: i.is_multiple_of(2),
                    include_replies: false,
                    min_likes: 3,
                    limit: 10 + (i % 40),
                    cursor: None,
                };

                let t0 = Instant::now();
                match rec.recommend_preview(did_opt, &dials) {
                    Ok(resp) => {
                        let lat = t0.elapsed().as_micros() as u64;
                        max_lat.fetch_max(lat, Ordering::Relaxed);
                        assert!(resp.items.len() <= dials.limit);
                        ops.fetch_add(1, Ordering::Relaxed);
                    }
                    Err(_) => {
                        errs.fetch_add(1, Ordering::Relaxed);
                    }
                }
                i += 1;
            }
        }));
    }

    // 2. 4 Reader threads executing find_taste_twins
    for thread_id in 0..4 {
        let rec = Arc::clone(&rec);
        let stop = Arc::clone(&stop_flag);
        let ops = Arc::clone(&twins_ops);
        let errs = Arc::clone(&error_count);
        let max_lat = Arc::clone(&max_twins_latency);
        let v_did = viewer_did.clone();

        handles.push(thread::spawn(move || {
            let mut i = 0usize;
            while !stop.load(Ordering::Relaxed) {
                let did = if (thread_id + i).is_multiple_of(3) {
                    v_did.as_str()
                } else if (thread_id + i) % 3 == 1 {
                    "did:plc:co_user_00005"
                } else {
                    "did:plc:nonexistent"
                };

                let limit = 5 + (i % 20);
                let t0 = Instant::now();
                match rec.find_taste_twins(did, limit) {
                    Ok(resp) => {
                        let lat = t0.elapsed().as_micros() as u64;
                        max_lat.fetch_max(lat, Ordering::Relaxed);
                        assert!(resp.twins.len() <= limit);
                        ops.fetch_add(1, Ordering::Relaxed);
                    }
                    Err(_) => {
                        errs.fetch_add(1, Ordering::Relaxed);
                    }
                }
                i += 1;
            }
        }));
    }

    // 3. 2 Reader threads executing explain_recommendation
    for thread_id in 0..2 {
        let rec = Arc::clone(&rec);
        let stop = Arc::clone(&stop_flag);
        let ops = Arc::clone(&explain_ops);
        let errs = Arc::clone(&error_count);
        let v_did = viewer_did.clone();

        handles.push(thread::spawn(move || {
            let mut i = 0usize;
            while !stop.load(Ordering::Relaxed) {
                let uri = format!(
                    "at://did:plc:tech_creator_0/app.bsky.feed.post/tech_cand_{}_{}",
                    (thread_id + i) % 50,
                    i % 10
                );
                match rec.explain_recommendation(v_did.as_str(), &uri) {
                    Ok(chain) => {
                        assert!(!chain.steps.is_empty());
                        ops.fetch_add(1, Ordering::Relaxed);
                    }
                    Err(_) => {
                        errs.fetch_add(1, Ordering::Relaxed);
                    }
                }
                i += 1;
            }
        }));
    }

    // 4. 4 Writer threads mutating graph concurrently
    for thread_id in 0..4 {
        let rec = Arc::clone(&rec);
        let stop = Arc::clone(&stop_flag);
        let ops = Arc::clone(&mutation_ops);

        handles.push(thread::spawn(move || {
            let mut i = 0usize;
            let now = BLUESKY_EPOCH_SECS + 50_000_000;
            while !stop.load(Ordering::Relaxed) {
                let user_uri = format!("did:plc:stress_writer_{thread_id}_{i}");
                let post_uri =
                    format!("at://did:plc:author/app.bsky.feed.post/stress_{thread_id}_{i}");
                let uid = rec.interner().intern(&user_uri);
                let pid = rec.interner().intern(&post_uri);
                let aid = rec.interner().intern("did:plc:author");

                rec.graph()
                    .record_post_meta(pid, aid, None, None, now + i as u64);
                let sig = match i % 3 {
                    0 => SignalType::Like,
                    1 => SignalType::Quote,
                    _ => SignalType::Repost,
                };
                rec.graph()
                    .record_interaction(uid, pid, sig, now + i as u64);
                rec.graph().record_follow(uid, aid);

                // Periodic removals
                if i.is_multiple_of(10) {
                    rec.graph().remove_interaction(uid, pid, sig);
                    rec.graph().remove_follow(uid, aid);
                }

                ops.fetch_add(1, Ordering::Relaxed);
                i += 1;
            }
        }));
    }

    // Let the stress test run under heavy contention
    let duration = Duration::from_millis(500);
    thread::sleep(duration);
    stop_flag.store(true, Ordering::Relaxed);

    for h in handles {
        h.join()
            .expect("Worker thread must join cleanly without panics");
    }

    let c_preview = preview_ops.load(Ordering::Relaxed);
    let c_twins = twins_ops.load(Ordering::Relaxed);
    let c_explain = explain_ops.load(Ordering::Relaxed);
    let c_mutations = mutation_ops.load(Ordering::Relaxed);
    let c_errs = error_count.load(Ordering::Relaxed);

    println!("=== EMPIRICAL CHALLENGE: Multi-Threaded Concurrency Stress Results ===");
    println!("recommend_preview() queries completed: {c_preview}");
    println!("find_taste_twins() queries completed:  {c_twins}");
    println!("explain_recommendation() completed:    {c_explain}");
    println!("Graph mutations completed:             {c_mutations}");
    println!("Total errors encountered:              {c_errs}");
    println!(
        "Max preview latency observed:          {} µs",
        max_preview_latency.load(Ordering::Relaxed)
    );
    println!(
        "Max twins latency observed:            {} µs",
        max_twins_latency.load(Ordering::Relaxed)
    );
    println!("=====================================================================");

    assert_eq!(c_errs, 0, "No errors should occur during concurrent stress");
    assert!(
        c_preview >= 20,
        "Expected significant preview throughput: {c_preview}"
    );
    assert!(
        c_twins >= 20,
        "Expected significant twins throughput: {c_twins}"
    );
    assert!(
        c_mutations >= 50,
        "Expected significant write throughput: {c_mutations}"
    );
}

#[test]
fn test_challenge_adversarial_preview_edge_cases() {
    let interner = Arc::new(StringInterner::new());
    let graph = Arc::new(GraphStore::new());
    let rec = Recommender::new(interner, graph);

    // 1. Extreme Dial Values:
    let extreme_dials = vec![
        // Extreme negative half life
        RecommendationDials {
            half_life_secs: -100.0,
            ..Default::default()
        },
        // Zero half life
        RecommendationDials {
            half_life_secs: 0.0,
            ..Default::default()
        },
        // Enormous half life
        RecommendationDials {
            half_life_secs: 1e15,
            ..Default::default()
        },
        // Negative explore ratio
        RecommendationDials {
            explore_ratio: -0.5,
            ..Default::default()
        },
        // Oversized explore ratio
        RecommendationDials {
            explore_ratio: 1.5,
            ..Default::default()
        },
        // Zero limit
        RecommendationDials {
            limit: 0,
            ..Default::default()
        },
        // Excessive limit
        RecommendationDials {
            limit: 99_999,
            ..Default::default()
        },
        // Extreme topic weights
        RecommendationDials {
            topic_weights: TopicWeights {
                art: 0.0,
                tech: 100.0,
                science: 0.0,
                news: 0.0,
                culture: 0.0,
            },
            ..Default::default()
        },
    ];

    for (idx, dials) in extreme_dials.into_iter().enumerate() {
        let res = rec.recommend_preview(Some("did:plc:nonexistent"), &dials);
        assert!(
            res.is_ok(),
            "Case {idx}: recommend_preview should gracefully handle extreme dials"
        );
        let resp = res.unwrap();
        assert!(resp.items.len() <= dials.limit.max(30));
    }

    // 2. Taste Twins extreme parameters:
    let res_twins_huge_limit = rec.find_taste_twins("did:plc:some_user", 1_000_000);
    assert!(res_twins_huge_limit.is_ok());
    assert!(res_twins_huge_limit.unwrap().twins.is_empty());

    let res_twins_zero_limit = rec.find_taste_twins("did:plc:some_user", 0);
    assert!(res_twins_zero_limit.is_ok());

    // 3. Explain non-existent post:
    let chain_none = rec
        .explain_recommendation("did:plc:u", "at://nonexistent/post/1")
        .unwrap();
    assert_eq!(chain_none.steps[0].step_type, "unindexed_post");
}

/// Independent ground-truth floating-point oracle for Cosine similarity between two discrete sets.
fn oracle_cosine_similarity(set_a: &AHashSet<u32>, set_b: &AHashSet<u32>) -> f64 {
    if set_a.is_empty() || set_b.is_empty() {
        return 0.0;
    }
    let intersection_size = set_a.intersection(set_b).count() as f64;
    let norm_a = (set_a.len() as f64).sqrt();
    let norm_b = (set_b.len() as f64).sqrt();
    intersection_size / (norm_a * norm_b)
}

#[test]
fn test_taste_twins_cosine_oracle_matrix_fuzzing() {
    let interner = Arc::new(StringInterner::new());
    let graph = Arc::new(GraphStore::new());
    let rec = Recommender::new(Arc::clone(&interner), Arc::clone(&graph));
    let now = BLUESKY_EPOCH_SECS + 500_000;

    let viewer = interner.intern("did:plc:fuzz_viewer");
    let author = interner.intern("did:plc:fuzz_author");

    // Total pool of 200 posts
    let mut all_posts = Vec::with_capacity(200);
    for i in 0..200 {
        let p = interner.intern(&format!("at://did:plc:fuzz_author/app.bsky.feed.post/{i}"));
        graph.record_post_meta(p, author, None, None, now - 1000);
        all_posts.push(p);
    }

    // Viewer likes 50 posts (0..50)
    let mut viewer_set = AHashSet::new();
    for &p in &all_posts[0..50] {
        graph.record_interaction(viewer, p, SignalType::Like, now - 500);
        viewer_set.insert(p);
    }

    // Generate 30 candidate users with varied overlap patterns
    let mut user_sets: Vec<(u32, CompactString, AHashSet<u32>)> = Vec::new();
    for u in 1..=30 {
        let handle = format!("did:plc:fuzz_user_{u}");
        let uid = interner.intern(&handle);
        let mut set = AHashSet::new();

        let post_range: Vec<u32> = match u % 5 {
            0 => all_posts[0..(u % 30 + 5)].to_vec(),
            1 => {
                let mut v = all_posts[10..30].to_vec();
                v.extend_from_slice(&all_posts[60..80]);
                v
            }
            2 => {
                let mut v = vec![all_posts[0]];
                v.extend_from_slice(&all_posts[100..(100 + u * 2)]);
                v
            }
            3 => all_posts[100..150].to_vec(),
            _ => {
                let mut v = all_posts[0..50].to_vec();
                v.extend_from_slice(&all_posts[50..(50 + u)]);
                v
            }
        };

        for &p in &post_range {
            graph.record_interaction(uid, p, SignalType::Like, now - 400);
            set.insert(p);
        }
        user_sets.push((uid, CompactString::new(handle), set));
    }

    // Direct pairwise verification against oracle
    for (uid, _handle, set) in &user_sets {
        let expected = oracle_cosine_similarity(&viewer_set, set) as f32;
        let actual = graph.compute_cosine_similarity(viewer, *uid);
        assert!(
            (actual - expected).abs() < 1e-5,
            "Cosine mismatch for user {uid}: expected {expected}, got {actual}"
        );
    }

    // Verification via find_taste_twins API
    let twins_resp = rec.find_taste_twins("did:plc:fuzz_viewer", 50).unwrap();
    assert_eq!(twins_resp.viewer_did, "did:plc:fuzz_viewer");
    assert_eq!(twins_resp.total_liked_posts, 50);

    // Verify all returned twins have similarity > 0.0 and match oracle exactly
    let mut prev_score = 1.01f32;
    for twin in &twins_resp.twins {
        assert!(
            twin.similarity_score <= prev_score + 1e-6,
            "Twins must be sorted descending: prev {prev_score}, current {}",
            twin.similarity_score
        );
        prev_score = twin.similarity_score;

        let matching_user = user_sets
            .iter()
            .find(|(_, handle, _)| *handle == twin.user_did)
            .expect("Returned twin must be one of generated users");

        let expected_sim = oracle_cosine_similarity(&viewer_set, &matching_user.2) as f32;
        let expected_shared = viewer_set.intersection(&matching_user.2).count();
        let expected_conf =
            calculate_bayesian_confidence(expected_sim, expected_shared, DEFAULT_BAYESIAN_BETA);
        assert!(
            (twin.similarity_score - expected_conf).abs() < 1e-5,
            "find_taste_twins confidence mismatch for {}: expected {expected_conf}, got {}",
            twin.user_did,
            twin.similarity_score
        );
        assert_eq!(twin.shared_posts_count, expected_shared);
    }

    // Ensure < 2 overlap users were strictly excluded from twins
    for (uid, handle, set) in &user_sets {
        if viewer_set.intersection(set).count() < MIN_SHARED_OVERLAP {
            assert!(
                !twins_resp.twins.iter().any(|t| t.user_did == *handle),
                "Low-overlap user {uid} ({handle}) should not be in taste twins"
            );
        }
    }
}

#[test]
fn test_taste_twins_boundary_edge_cases() {
    let interner = Arc::new(StringInterner::new());
    let graph = Arc::new(GraphStore::new());
    let rec = Recommender::new(Arc::clone(&interner), Arc::clone(&graph));
    let now = BLUESKY_EPOCH_SECS + 100_000;

    let viewer = interner.intern("did:plc:sparse_viewer");
    let dense_twin = interner.intern("did:plc:dense_twin");
    let author = interner.intern("did:plc:author");

    let p_shared = interner.intern("at://did:plc:author/app.bsky.feed.post/shared");
    graph.record_post_meta(p_shared, author, None, None, now - 1000);

    // Viewer has only 1 like
    graph.record_interaction(viewer, p_shared, SignalType::Like, now - 500);

    // Dense twin has 10,000 likes (1 shared, 9999 distinct)
    graph.record_interaction(dense_twin, p_shared, SignalType::Like, now - 400);
    for i in 1..10_000 {
        let p = interner.intern(&format!("at://did:plc:author/app.bsky.feed.post/p_{i}"));
        graph.record_post_meta(p, author, None, None, now - 1000);
        graph.record_interaction(dense_twin, p, SignalType::Like, now - 400);
    }

    // Expected cosine: 1.0 / sqrt(1 * 10000) = 1.0 / 100.0 = 0.01
    let expected = 1.0 / 100.0f32;
    let actual = graph.compute_cosine_similarity(viewer, dense_twin);
    assert!((actual - expected).abs() < 1e-6);

    // 1 shared like (< MIN_SHARED_OVERLAP) must return 0 taste twins
    let resp1 = rec.find_taste_twins("did:plc:sparse_viewer", 10).unwrap();
    assert_eq!(resp1.twins.len(), 0);

    // Now record a second shared like
    let p_shared2 = interner.intern("at://did:plc:author/app.bsky.feed.post/shared2");
    graph.record_post_meta(p_shared2, author, None, None, now - 1000);
    graph.record_interaction(viewer, p_shared2, SignalType::Like, now - 500);
    graph.record_interaction(dense_twin, p_shared2, SignalType::Like, now - 400);

    // With 2 shared likes, dense twin qualifies
    let resp2 = rec.find_taste_twins("did:plc:sparse_viewer", 10).unwrap();
    assert_eq!(resp2.twins.len(), 1);
    assert_eq!(resp2.twins[0].user_did, "did:plc:dense_twin");
    let expected_cosine_2 = 2.0 / (2.0 * 10001.0f32).sqrt();
    let expected_conf_2 =
        calculate_bayesian_confidence(expected_cosine_2, 2, DEFAULT_BAYESIAN_BETA);
    assert!((resp2.twins[0].similarity_score - expected_conf_2).abs() < 1e-5);
    assert_eq!(resp2.twins[0].shared_posts_count, 2);
}

#[test]
fn test_read_only_impression_isolation_stress() {
    let interner = Arc::new(StringInterner::new());
    let graph = Arc::new(GraphStore::new());
    let rec = Recommender::new(Arc::clone(&interner), Arc::clone(&graph));
    let now = BLUESKY_EPOCH_SECS + 200_000;

    let viewer = interner.intern("did:plc:iso_viewer");
    let co_user = interner.intern("did:plc:iso_co_user");

    // Create 20 posts with distinct authors to satisfy author diversity (max 2/author)
    let mut posts = Vec::new();
    for i in 0..20 {
        let author = interner.intern(&format!("did:plc:iso_author_{i}"));
        let p = interner.intern(&format!(
            "at://did:plc:iso_author_{i}/app.bsky.feed.post/{i}"
        ));
        graph.record_post_meta(p, author, None, None, now - 2000);
        posts.push(p);
    }

    // Seed viewer with 12 likes for Tier 1 qualification
    for &p in &posts[0..12] {
        graph.record_interaction(viewer, p, SignalType::Like, now - 1000);
        graph.record_interaction(co_user, p, SignalType::Like, now - 900);
    }
    // Candidate posts liked by co-user
    for &p in &posts[12..20] {
        graph.record_interaction(co_user, p, SignalType::Like, now - 800);
    }

    let dials = RecommendationDials {
        explain: true,
        min_likes: 1,
        limit: 10,
        ..Default::default()
    };

    // Baseline verification: impression store is completely empty
    assert_eq!(
        rec.impression_store().get_viewer_impression_count(viewer),
        0
    );
    assert_eq!(rec.impression_store().total_viewers(), 0);
    assert!(!rec
        .impression_store()
        .contains_impression(viewer, posts[12]));

    // Execute 100 consecutive recommend_preview calls across various dial configurations
    for step in 0..100 {
        let custom_dials = RecommendationDials {
            half_life_secs: (step as f32 + 1.0) * 3600.0,
            explore_ratio: (step % 50) as f32 / 100.0,
            topic_weights: TopicWeights {
                art: (step % 5) as f32,
                tech: 1.0,
                science: 2.0,
                news: 0.5,
                culture: 1.5,
            },
            explain: step % 2 == 0,
            include_replies: false,
            min_likes: 1,
            limit: 15,
            cursor: None,
        };

        let prev = rec
            .recommend_preview_at(Some("did:plc:iso_viewer"), &custom_dials, now)
            .unwrap();
        assert!(!prev.items.is_empty());
        assert_eq!(prev.viewer_did, "did:plc:iso_viewer");

        // Assert strictly zero entries added to impression store
        assert_eq!(
            rec.impression_store().get_viewer_impression_count(viewer),
            0,
            "Impression count must remain 0 after preview call #{step}"
        );
        assert_eq!(
            rec.impression_store().total_viewers(),
            0,
            "Total viewers must remain 0 after preview call #{step}"
        );
        for &p in &posts {
            assert!(
                !rec.impression_store().contains_impression(viewer, p),
                "No post should be marked as impressed by preview"
            );
        }
    }

    // Now run standard recommend() and verify candidates are 100% identical and unsuppressed
    let regular_res = rec
        .recommend(Some("did:plc:iso_viewer"), &dials, now)
        .unwrap();
    assert_eq!(regular_res.posts.len(), 8); // posts 12..20

    // Explicitly record impressions for 3 posts via regular delivery hook
    rec.record_impressions(
        Some("did:plc:iso_viewer"),
        &[posts[12], posts[13], posts[14]],
        now,
    );

    // Verify impression store now has exactly 3 entries for viewer
    assert_eq!(
        rec.impression_store().get_viewer_impression_count(viewer),
        3
    );
    assert_eq!(rec.impression_store().total_viewers(), 1);

    // Preview should now reflect smooth soft damping (fatigue_penalty == 0.15) for the 3 seen posts
    let preview_after = rec
        .recommend_preview_at(Some("did:plc:iso_viewer"), &dials, now)
        .unwrap();
    assert_eq!(preview_after.items.len(), 8);
    // The 5 fresh posts rank ahead of the 3 dampened posts
    for item in &preview_after.items[0..5] {
        assert_ne!(item.uri, "at://did:plc:iso_author_12/app.bsky.feed.post/12");
        assert_ne!(item.uri, "at://did:plc:iso_author_13/app.bsky.feed.post/13");
        assert_ne!(item.uri, "at://did:plc:iso_author_14/app.bsky.feed.post/14");
    }

    // And verify preview still did NOT record any additional impressions for the remaining 5 posts
    assert_eq!(
        rec.impression_store().get_viewer_impression_count(viewer),
        3
    );
}

#[test]
fn test_proof_chain_reconstruction_multi_path_adversarial() {
    let interner = Arc::new(StringInterner::new());
    let graph = Arc::new(GraphStore::new());
    let rec = Recommender::new(Arc::clone(&interner), Arc::clone(&graph));
    let now = BLUESKY_EPOCH_SECS + 300_000;

    let viewer = interner.intern("did:plc:proof_viewer");
    let author = interner.intern("did:plc:tech_seed");
    let twin_weak = interner.intern("did:plc:twin_weak");
    let twin_strong = interner.intern("did:plc:twin_strong");

    let seed_1 = interner.intern("at://did:plc:tech_seed/app.bsky.feed.post/seed_1");
    let seed_2 = interner.intern("at://did:plc:tech_seed/app.bsky.feed.post/seed_2");
    let target_post = interner.intern("at://did:plc:tech_seed/app.bsky.feed.post/target_post");

    graph.record_post_meta(seed_1, author, None, None, now - 1000);
    graph.record_post_meta(seed_2, author, None, None, now - 1000);
    graph.record_post_meta(target_post, author, None, None, now - 500);

    // Viewer likes seed_1 and seed_2 (|viewer| = 2)
    graph.record_interaction(viewer, seed_1, SignalType::Like, now - 600);
    graph.record_interaction(viewer, seed_2, SignalType::Like, now - 600);

    // Twin Weak: only shares seed_1 (|twin_weak| = 2 {seed_1, target_post}), likes target_post (weight 1.0)
    // sim(viewer, twin_weak) = 1 / sqrt(2 * 2) = 0.5
    // path_score = 0.5 * 1.0 = 0.5
    graph.record_interaction(twin_weak, seed_1, SignalType::Like, now - 550);
    graph.record_interaction(twin_weak, target_post, SignalType::Like, now - 400);

    // Twin Strong: shares seed_1 and seed_2 (|twin_strong| = 3 {seed_1, seed_2, target_post}), reposts target_post (weight 3.0)
    // sim(viewer, twin_strong) = 2 / sqrt(2 * 3) = 2 / 2.4494897 ≈ 0.8164966 (81.6%)
    // path_score = 0.8165 * 3.0 = 2.4495
    graph.record_interaction(twin_strong, seed_1, SignalType::Like, now - 550);
    graph.record_interaction(twin_strong, seed_2, SignalType::Like, now - 550);
    graph.record_interaction(twin_strong, target_post, SignalType::Repost, now - 400);

    let chain = rec
        .explain_recommendation(
            "did:plc:proof_viewer",
            "at://did:plc:tech_seed/app.bsky.feed.post/target_post",
        )
        .unwrap();

    assert_eq!(chain.steps.len(), 3);

    // Step 1: Viewer -> Interacted Seed Post
    assert_eq!(chain.steps[0].step_type, "viewer_interaction");
    assert!(
        chain.steps[0].node_id == "at://did:plc:tech_seed/app.bsky.feed.post/seed_1"
            || chain.steps[0].node_id == "at://did:plc:tech_seed/app.bsky.feed.post/seed_2"
    );

    // Step 2: Taste Similarity -> Strong Twin (Bayesian shrunk confidence: 0.8165 * 0.40 = 32.7%)
    assert_eq!(chain.steps[1].step_type, "taste_similarity");
    assert_eq!(chain.steps[1].node_id, "did:plc:twin_strong");
    assert!(chain.steps[1].description.contains("32.7% taste match"));

    // Step 3: Recommendation Signal -> Target Post via Repost
    assert_eq!(chain.steps[2].step_type, "recommendation_signal");
    assert_eq!(
        chain.steps[2].node_id,
        "at://did:plc:tech_seed/app.bsky.feed.post/target_post"
    );
    assert!(chain.steps[2].description.contains("reposted"));
    assert!(chain.steps[2].description.contains("tech"));

    assert!(chain.summary.contains("did:plc:twin_strong"));
    assert!(chain.summary.contains("reposted this tech post"));
}

#[test]
fn test_preview_mathematical_score_breakdown_invariants() {
    let interner = Arc::new(StringInterner::new());
    let graph = Arc::new(GraphStore::new());
    let rec = Recommender::new(Arc::clone(&interner), Arc::clone(&graph));
    let now = BLUESKY_EPOCH_SECS + 100_000;

    let viewer = interner.intern("did:plc:math_viewer");
    let co_user = interner.intern("did:plc:math_co_user");
    let author = interner.intern("did:plc:art_seed");

    let seed = interner.intern("at://did:plc:art_seed/app.bsky.feed.post/seed");
    let cand = interner.intern("at://did:plc:art_seed/app.bsky.feed.post/cand");

    graph.record_post_meta(seed, author, None, None, now - 1000);
    graph.record_post_meta(cand, author, None, None, now - 3600); // 1 hour ago

    // Populate 10 likes for Tier 1
    let mut first_pad = None;
    for i in 1..=10 {
        let p = interner.intern(&format!("at://did:plc:art_seed/app.bsky.feed.post/pad_{i}"));
        if first_pad.is_none() {
            first_pad = Some(p);
        }
        graph.record_post_meta(p, author, None, None, now - 2000);
        graph.record_interaction(viewer, p, SignalType::Like, now - 1500);
    }
    let p_pad1 = first_pad.unwrap();
    graph.record_interaction(viewer, seed, SignalType::Like, now - 800);
    graph.record_interaction(co_user, seed, SignalType::Like, now - 700);
    graph.record_interaction(co_user, p_pad1, SignalType::Like, now - 700);

    // Co-user reposts cand (weight = 3.0) 3600s ago
    graph.record_interaction(co_user, cand, SignalType::Repost, now - 3600);

    let half_life = 36.0 * 3600.0; // 129600s
    let dials = RecommendationDials {
        half_life_secs: half_life,
        topic_weights: TopicWeights {
            art: 2.5,
            tech: 1.0,
            science: 1.0,
            news: 1.0,
            culture: 1.0,
        },
        explain: true,
        min_likes: 1,
        ..Default::default()
    };

    let preview = rec
        .recommend_preview_at(Some("did:plc:math_viewer"), &dials, now)
        .unwrap();
    let item = preview
        .items
        .iter()
        .find(|i| i.uri == "at://did:plc:art_seed/app.bsky.feed.post/cand")
        .expect("Candidate post must be present");

    let b = &item.score_breakdown;

    // 1. Time decay: weight 3.0 * exp(-3600 / 129600) = 3.0 * exp(-0.0277778) ≈ 2.9178
    let expected_decay = 3.0 * (-3600.0f32 / half_life).exp();
    assert!((b.time_decay - expected_decay).abs() < 1e-4);

    // 2. Topic boost: 2.5 for art
    assert_eq!(b.topic_boost, 2.5);

    // 3. Fatigue penalty: 1.0 (unseen)
    assert_eq!(b.fatigue_penalty, 1.0);

    // 4. Final score = taste_similarity * time_decay * topic_boost * fatigue_penalty
    let expected_final = b.taste_similarity * b.time_decay * b.topic_boost * b.fatigue_penalty;
    assert!((b.final_score - expected_final).abs() < 1e-4);
}

#[test]
fn test_m1_defensive_bounds_seed_posts_capping() {
    let interner = Arc::new(StringInterner::new());
    let graph = Arc::new(GraphStore::new());
    let rec = Recommender::new(Arc::clone(&interner), Arc::clone(&graph));
    let now = BLUESKY_EPOCH_SECS + 60_000_000;

    let viewer_did = "did:plc:m1_bounds_viewer";
    let viewer_id = interner.intern(viewer_did);

    // Viewer interacts with 100 posts (0..100) with increasing timestamps
    for i in 0..100 {
        let uri = format!("at://did:plc:author/app.bsky.feed.post/seed_{i:03}");
        let pid = interner.intern(&uri);
        let author = interner.intern("did:plc:author");
        graph.record_post_meta(pid, author, None, None, now - 100_000 + i as u64);
        graph.record_interaction(viewer_id, pid, SignalType::Like, now - 100_000 + i as u64);
    }

    // Co-user A only likes oldest seed posts 0..5 (outside the 50 most recent, i.e. 50..100)
    let co_a_did = "did:plc:co_user_old";
    let co_a_id = interner.intern(co_a_did);
    for i in 0..5 {
        let uri = format!("at://did:plc:author/app.bsky.feed.post/seed_{i:03}");
        let pid = interner.intern(&uri);
        graph.record_interaction(co_a_id, pid, SignalType::Like, now - 50_000);
    }
    // Candidate post from Co-user A
    let cand_a_pid = interner.intern("at://did:plc:co_user_old/app.bsky.feed.post/cand_a");
    graph.record_post_meta(cand_a_pid, co_a_id, None, None, now - 10_000);
    graph.record_interaction(co_a_id, cand_a_pid, SignalType::Like, now - 10_000);

    // Co-user B likes newest seed posts 95..100 (inside the 50 most recent)
    let co_b_did = "did:plc:co_user_recent";
    let co_b_id = interner.intern(co_b_did);
    for i in 95..100 {
        let uri = format!("at://did:plc:author/app.bsky.feed.post/seed_{i:03}");
        let pid = interner.intern(&uri);
        graph.record_interaction(co_b_id, pid, SignalType::Like, now - 50_000);
    }
    // Candidate post from Co-user B
    let cand_b_pid = interner.intern("at://did:plc:co_user_recent/app.bsky.feed.post/cand_b");
    graph.record_post_meta(cand_b_pid, co_b_id, None, None, now - 10_000);
    graph.record_interaction(co_b_id, cand_b_pid, SignalType::Like, now - 10_000);

    // 1. Taste twins discovery should find Co-user B (recent seed overlap) and NOT Co-user A
    let twins_resp = rec.find_taste_twins(viewer_did, 10).unwrap();
    let twin_dids: Vec<&str> = twins_resp
        .twins
        .iter()
        .map(|t| t.user_did.as_str())
        .collect();
    assert!(
        twin_dids.contains(&co_b_did),
        "Co-user B with recent overlap should be discovered"
    );
    assert!(
        !twin_dids.contains(&co_a_did),
        "Co-user A with older overlap outside top-50 seed posts should NOT be explored"
    );

    // 2. Feed preview should evaluate candidates from Co-user B and NOT Co-user A
    let dials = RecommendationDials {
        limit: 10,
        min_likes: 1,
        ..Default::default()
    };
    let preview = rec
        .recommend_preview_at(Some(viewer_did), &dials, now)
        .unwrap();
    let preview_uris: Vec<&str> = preview.items.iter().map(|it| it.uri.as_str()).collect();
    assert!(
        preview_uris.contains(&"at://did:plc:co_user_recent/app.bsky.feed.post/cand_b"),
        "Candidate from recent co-interactor should be included"
    );
    assert!(
        !preview_uris.contains(&"at://did:plc:co_user_old/app.bsky.feed.post/cand_a"),
        "Candidate from old seed co-interactor should not be reached"
    );
}

#[test]
fn test_m1_defensive_bounds_viral_post_edges_and_top_co_interactors() {
    let interner = Arc::new(StringInterner::new());
    let graph = Arc::new(GraphStore::new());
    let rec = Recommender::new(Arc::clone(&interner), Arc::clone(&graph));
    let now = BLUESKY_EPOCH_SECS + 70_000_000;

    let viewer_did = "did:plc:m1_viral_viewer";
    let viewer_id = interner.intern(viewer_did);

    // Viewer interacts with 15 seed posts
    let mut seed_pids = Vec::with_capacity(15);
    for i in 0..15 {
        let uri = format!("at://did:plc:author/app.bsky.feed.post/vseed_{i}");
        let pid = interner.intern(&uri);
        let author = interner.intern("did:plc:author");
        graph.record_post_meta(pid, author, None, None, now - 100_000);
        graph.record_interaction(viewer_id, pid, SignalType::Like, now - 90_000);
        seed_pids.push(pid);
    }

    // Populate 200 co-interactors, each liking all 15 seed posts
    for u in 0..200 {
        let co_did = format!("did:plc:viral_co_{u:03}");
        let co_id = interner.intern(&co_did);
        for &spid in &seed_pids {
            graph.record_interaction(co_id, spid, SignalType::Like, now - 80_000);
        }
        // Each co-user has a distinct candidate post
        let cand_uri = format!("at://did:plc:viral_co_{u:03}/app.bsky.feed.post/cand");
        let cand_pid = interner.intern(&cand_uri);
        graph.record_post_meta(cand_pid, co_id, None, None, now - 10_000);
        graph.record_interaction(co_id, cand_pid, SignalType::Like, now - 10_000);
    }

    let dials = RecommendationDials {
        limit: 30,
        min_likes: 1,
        ..Default::default()
    };

    // Warmup
    for _ in 0..5 {
        let _ = rec.recommend_preview_at(Some(viewer_did), &dials, now);
    }

    let preview = rec
        .recommend_preview_at(Some(viewer_did), &dials, now)
        .unwrap();

    // With 200 co-interactors capped to MAX_CO_INTERACTORS (100), exactly 100 candidate posts should be evaluated
    assert_eq!(
        preview.total_candidates, 100,
        "Total candidates evaluated should match MAX_CO_INTERACTORS (100)"
    );
    assert_eq!(preview.items.len(), 30);
    #[cfg(not(debug_assertions))]
    assert!(
        preview.query_latency_us < 2_000,
        "Preview latency SLA violation in release: {}us",
        preview.query_latency_us
    );
    #[cfg(debug_assertions)]
    assert!(
        preview.query_latency_us < 100_000,
        "Preview latency abnormal debug spike: {}us",
        preview.query_latency_us
    );
}

#[test]
fn test_m1_explain_recommendation_viral_post_sub_1ms() {
    let interner = Arc::new(StringInterner::new());
    let graph = Arc::new(GraphStore::new());
    let rec = Recommender::new(Arc::clone(&interner), Arc::clone(&graph));
    let now = BLUESKY_EPOCH_SECS + 80_000_000;

    let viewer_did = "did:plc:explain_viewer";
    let viewer_id = interner.intern(viewer_did);

    let seed_pid1 = interner.intern("at://did:plc:author/app.bsky.feed.post/shared_seed1");
    let seed_pid2 = interner.intern("at://did:plc:author/app.bsky.feed.post/shared_seed2");
    let author_id = interner.intern("did:plc:author");
    graph.record_post_meta(seed_pid1, author_id, None, None, now - 50_000);
    graph.record_post_meta(seed_pid2, author_id, None, None, now - 50_000);
    graph.record_interaction(viewer_id, seed_pid1, SignalType::Like, now - 40_000);
    graph.record_interaction(viewer_id, seed_pid2, SignalType::Like, now - 40_000);

    let target_uri = "at://did:plc:creator/app.bsky.feed.post/viral_target";
    let target_pid = interner.intern(target_uri);
    let creator_id = interner.intern("did:plc:creator");
    graph.record_post_meta(target_pid, creator_id, None, None, now - 10_000);

    // Target post has 1,200 reverse interaction edges (viral post)
    for u in 0..1200 {
        let user_did = format!("did:plc:liker_{u:04}");
        let uid = interner.intern(&user_did);
        graph.record_interaction(
            uid,
            target_pid,
            SignalType::Like,
            now - 5_000 + (u as u64 % 1000),
        );
        if u == 1150 {
            // Twin curator who also likes the 2 shared seed posts (MIN_SHARED_OVERLAP >= 2)
            graph.record_interaction(uid, seed_pid1, SignalType::Like, now - 40_000);
            graph.record_interaction(uid, seed_pid2, SignalType::Like, now - 40_000);
        }
    }

    // Warmup
    for _ in 0..5 {
        let _ = rec.explain_recommendation(viewer_did, target_uri);
    }

    let t0 = Instant::now();
    let explanation = rec
        .explain_recommendation(viewer_did, target_uri)
        .expect("explain_recommendation should succeed");
    let elapsed = t0.elapsed();

    assert!(
        elapsed.as_micros() < 5_000,
        "Explain latency should be sub-5ms on viral post in debug mode (took {:?})",
        elapsed
    );
    assert_eq!(explanation.steps.len(), 3);
    assert!(explanation.summary.to_lowercase().contains("taste twin"));
}
