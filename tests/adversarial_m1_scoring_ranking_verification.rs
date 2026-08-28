#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::float_cmp,
    clippy::suboptimal_flops,
    clippy::needless_range_loop,
    clippy::unreadable_literal,
    missing_docs
)]

use for_your_consideration::prelude::*;
use for_your_consideration::recommender::MAX_CO_INTERACTORS;
use std::collections::HashSet;
use std::sync::Arc;

// ===========================================================================
// 1. Bayesian Shrinkage & Confidence Mathematical Correctness
// ===========================================================================

#[test]
fn test_empirical_bayesian_shrinkage_and_confidence_properties() {
    let beta = DEFAULT_BAYESIAN_BETA; // 3.0
    assert_eq!(beta, 3.0);

    // 1. Zero shared count boundary
    assert_eq!(calculate_bayesian_shrinkage(0, beta), 0.0);
    assert_eq!(calculate_bayesian_confidence(1.0, 0, beta), 0.0);
    assert_eq!(calculate_bayesian_confidence(0.5, 0, beta), 0.0);

    // 2. Analytical values: S / (S + 3.0)
    let expected_s1 = 1.0 / (1.0 + 3.0); // 0.25
    let expected_s2 = 2.0 / (2.0 + 3.0); // 0.40
    let expected_s3 = 3.0 / (3.0 + 3.0); // 0.50
    let expected_s9 = 9.0 / (9.0 + 3.0); // 0.75
    let expected_s27 = 27.0 / (27.0 + 3.0); // 0.90

    assert!((calculate_bayesian_shrinkage(1, beta) - expected_s1).abs() < 1e-6);
    assert!((calculate_bayesian_shrinkage(2, beta) - expected_s2).abs() < 1e-6);
    assert!((calculate_bayesian_shrinkage(3, beta) - expected_s3).abs() < 1e-6);
    assert!((calculate_bayesian_shrinkage(9, beta) - expected_s9).abs() < 1e-6);
    assert!((calculate_bayesian_shrinkage(27, beta) - expected_s27).abs() < 1e-6);

    // 3. Strict Monotonicity in S for S in 1..1000
    let mut prev_shrinkage = 0.0;
    for s in 1..=1000 {
        let shrinkage = calculate_bayesian_shrinkage(s, beta);
        assert!(
            shrinkage > prev_shrinkage,
            "Shrinkage must be strictly increasing with S: S={s}, prev={prev_shrinkage}, cur={shrinkage}"
        );
        assert!(
            shrinkage < 1.0,
            "Shrinkage must be strictly bounded below 1.0"
        );
        prev_shrinkage = shrinkage;
    }

    // 4. Strict Concavity in S (Diminishing Marginal Gains) for S in 1..100
    // d(Shrinkage)/dS is decreasing
    for s in 1..=100 {
        let diff1 =
            calculate_bayesian_shrinkage(s + 1, beta) - calculate_bayesian_shrinkage(s, beta);
        let diff2 =
            calculate_bayesian_shrinkage(s + 2, beta) - calculate_bayesian_shrinkage(s + 1, beta);
        assert!(
            diff1 > diff2,
            "Shrinkage must be strictly concave (diminishing returns): S={s}, diff1={diff1}, diff2={diff2}"
        );
    }

    // 5. Monotonicity in Cosine for fixed S
    for s in [2, 5, 10, 50] {
        let mut prev_conf = -1.0;
        for c_int in 0..=100 {
            let cosine = c_int as f32 / 100.0;
            let conf = calculate_bayesian_confidence(cosine, s, beta);
            assert!(
                conf >= prev_conf,
                "Confidence must be monotonic in cosine: S={s}, cosine={cosine}"
            );
            prev_conf = conf;
        }
    }

    // 6. Bayesian Rank Inversion / Quality vs Noise Property:
    // A small sample with perfect cosine (S=2, C=1.0) gives 1.0 * 2/5 = 0.40.
    // A moderate sample with good cosine (S=10, C=0.75) gives 0.75 * 10/13 = 0.5769.
    // Moderate sample must outrank noisy small sample.
    let conf_noisy = calculate_bayesian_confidence(1.0, 2, beta);
    let conf_solid = calculate_bayesian_confidence(0.75, 10, beta);
    assert!(
        conf_solid > conf_noisy,
        "Bayesian shrinkage must penalize small noisy overlap (S=2, C=1.0 -> {conf_noisy}) compared to robust overlap (S=10, C=0.75 -> {conf_solid})"
    );

    // 7. Non-positive beta fallback
    assert_eq!(
        calculate_bayesian_shrinkage(3, 0.0),
        calculate_bayesian_shrinkage(3, DEFAULT_BAYESIAN_BETA)
    );
    assert_eq!(
        calculate_bayesian_shrinkage(3, -5.0),
        calculate_bayesian_shrinkage(3, DEFAULT_BAYESIAN_BETA)
    );
}

// ===========================================================================
// 2. Exponential Time Decay Monotonicity & Robustness
// ===========================================================================

#[test]
fn test_empirical_time_decay_strict_monotonicity_and_clock_skew() {
    let now = BLUESKY_EPOCH_SECS + 50_000_000;
    let tau = DEFAULT_HALF_LIFE_SECS; // 129,600.0s (36h)

    // 1. Signal weights verification
    let w_like = calculate_time_decay(SignalType::Like, now, now, tau);
    let w_repost = calculate_time_decay(SignalType::Repost, now, now, tau);
    let w_quote = calculate_time_decay(SignalType::Quote, now, now, tau);

    assert_eq!(w_like, 1.0);
    assert_eq!(w_repost, 3.0);
    assert_eq!(w_quote, 2.0);

    // 2. Exact exponential decay at dt = tau: factor is e^(-1)
    let e_inv = (-1.0f32).exp(); // ~0.36787944
    let w_tau = calculate_time_decay(SignalType::Like, now - tau as u64, now, tau);
    assert!(
        (w_tau - e_inv).abs() < 1e-5,
        "Decay at dt=tau must be e^(-1), expected {e_inv}, got {w_tau}"
    );

    // 3. Strict Monotonicity in Delta t
    let mut prev_decay = f32::INFINITY;
    for dt_hours in 0..=240 {
        let event_time = now - (dt_hours * 3600);
        let decay = calculate_time_decay(SignalType::Like, event_time, now, tau);
        assert!(
            decay < prev_decay || (dt_hours == 0 && decay == 1.0),
            "Decay must strictly decrease with elapsed time: dt={dt_hours}h, prev={prev_decay}, cur={decay}"
        );
        assert!(decay > 0.0, "Decay must remain strictly positive");
        prev_decay = decay;
    }

    // 4. Clock Skew / Future timestamps: dt < 0 must saturate safely to dt = 0
    let w_future_1s = calculate_time_decay(SignalType::Like, now + 1, now, tau);
    let w_future_1000s = calculate_time_decay(SignalType::Like, now + 1_000, now, tau);
    let w_future_1yr = calculate_time_decay(SignalType::Like, now + 31_536_000, now, tau);
    assert_eq!(
        w_future_1s, 1.0,
        "Future event must saturate to zero elapsed time (decay = 1.0)"
    );
    assert_eq!(
        w_future_1000s, 1.0,
        "Future event must saturate to zero elapsed time"
    );
    assert_eq!(
        w_future_1yr, 1.0,
        "Far future event must not panic or produce NaN"
    );

    // 5. Non-positive half-life fallback
    let w_zero_tau = calculate_time_decay(SignalType::Like, now - 3600, now, 0.0);
    let w_neg_tau = calculate_time_decay(SignalType::Like, now - 3600, now, -10.0);
    let w_default_tau =
        calculate_time_decay(SignalType::Like, now - 3600, now, DEFAULT_HALF_LIFE_SECS);
    assert_eq!(w_zero_tau, w_default_tau);
    assert_eq!(w_neg_tau, w_default_tau);
}

// ===========================================================================
// 3. select_nth_unstable_by Top-K Exact Preservation
// ===========================================================================

#[test]
fn test_empirical_select_nth_unstable_by_top_k_exact_preservation() {
    let k = MAX_CO_INTERACTORS; // 100
    assert_eq!(k, 100);

    let test_sizes = [101, 105, 150, 200, 500, 1000, 5000];

    let mut rng_state: u64 = 0x853c_49e6_748f_ea9b;
    let next_f32 = |state: &mut u64| -> f32 {
        *state ^= *state << 13;
        *state ^= *state >> 7;
        *state ^= *state << 17;
        (*state as f32) / (u64::MAX as f32)
    };

    for &size in &test_sizes {
        // Distribution 1: Uniform random scores
        {
            let items: Vec<(u32, f32)> = (0..size as u32)
                .map(|id| (id, next_f32(&mut rng_state)))
                .collect();

            // Baseline: full sort descending
            let mut exact_sorted = items.clone();
            exact_sorted.sort_by(|a, b| {
                b.1.partial_cmp(&a.1)
                    .unwrap_or(std::cmp::Ordering::Equal)
                    .then_with(|| a.0.cmp(&b.0))
            });
            let exact_top_k: HashSet<u32> = exact_sorted[..k].iter().map(|&(id, _)| id).collect();
            let threshold_score = exact_sorted[k - 1].1;
            let outside_score = exact_sorted[k].1;

            // Partition with select_nth_unstable_by
            let mut partitioned = items.clone();
            partitioned.select_nth_unstable_by(k, |a, b| {
                b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal)
            });
            partitioned.truncate(k);

            assert_eq!(partitioned.len(), k);
            for &(id, score) in &partitioned {
                assert!(
                    score >= outside_score,
                    "Every selected element score ({score}) must be >= outside score ({outside_score})"
                );
                if threshold_score > outside_score {
                    assert!(
                        exact_top_k.contains(&id),
                        "When strictly separated, selected ID {id} must be in exact top K"
                    );
                }
            }
        }

        // Distribution 2: Worst-case reverse sorted
        {
            let items: Vec<(u32, f32)> = (0..size as u32).map(|id| (id, id as f32)).collect();

            let mut partitioned = items.clone();
            partitioned.select_nth_unstable_by(k, |a, b| {
                b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal)
            });
            partitioned.truncate(k);

            assert_eq!(partitioned.len(), k);
            let min_selected = partitioned
                .iter()
                .map(|&(_, s)| s)
                .fold(f32::INFINITY, f32::min);
            let expected_min = (size - k) as f32;
            assert_eq!(
                min_selected, expected_min,
                "Reverse sorted partition must preserve exact top K elements"
            );
        }

        // Distribution 3: Heavy ties / Discrete clusters of scores
        {
            let items: Vec<(u32, f32)> = (0..size as u32)
                .map(|id| (id, (id % 10) as f32 * 0.1))
                .collect();

            let mut partitioned = items.clone();
            partitioned.select_nth_unstable_by(k, |a, b| {
                b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal)
            });
            partitioned.truncate(k);

            assert_eq!(partitioned.len(), k);
            let mut exact_sorted = items.clone();
            exact_sorted.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal));
            let min_exact_top_k = exact_sorted[k - 1].1;

            for &(_, score) in &partitioned {
                assert!(
                    score >= min_exact_top_k,
                    "Selected score {score} must be >= min exact top K score {min_exact_top_k}"
                );
            }
        }

        // Distribution 4: All identical scores
        {
            let items: Vec<(u32, f32)> = (0..size as u32).map(|id| (id, 0.75)).collect();

            let mut partitioned = items.clone();
            partitioned.select_nth_unstable_by(k, |a, b| {
                b.1.partial_cmp(&a.1).unwrap_or(std::cmp::Ordering::Equal)
            });
            partitioned.truncate(k);

            assert_eq!(partitioned.len(), k);
            for &(_, score) in &partitioned {
                assert_eq!(score, 0.75);
            }
        }
    }
}

// ===========================================================================
// 4. Consensus Boost & Social Proof Curves
// ===========================================================================

#[test]
fn test_empirical_consensus_boost_and_social_proof_curves() {
    // 1. Consensus Boost: 1.0 + 0.45 * ln(k) for k >= 2
    assert_eq!(calculate_consensus_boost(0), 1.0);
    assert_eq!(calculate_consensus_boost(1), 1.0);

    let boost_2 = calculate_consensus_boost(2);
    let boost_3 = calculate_consensus_boost(3);
    let boost_10 = calculate_consensus_boost(10);
    let boost_100 = calculate_consensus_boost(100);

    assert!((boost_2 - (1.0 + 0.45 * 2.0f32.ln())).abs() < 1e-6);
    assert!((boost_3 - (1.0 + 0.45 * 3.0f32.ln())).abs() < 1e-6);
    assert!((boost_10 - (1.0 + 0.45 * 10.0f32.ln())).abs() < 1e-6);
    assert!((boost_100 - (1.0 + 0.45 * 100.0f32.ln())).abs() < 1e-6);

    assert!(boost_2 > 1.0);
    assert!(boost_3 > boost_2);
    assert!(boost_10 > boost_3);
    assert!(boost_100 > boost_10);

    // 2. Social Proof Factor Curve:
    // N=0 -> 1/3 ~ 0.33333334
    let sp_0 = calculate_social_proof_factor(0);
    assert!((sp_0 - 1.0 / 3.0).abs() < 1e-6);

    // Strictly increasing on [0, 500]
    let mut prev_sp = sp_0;
    for &n in &[1, 3, 10, 50, 100, 250, 500] {
        let sp = calculate_social_proof_factor(n);
        assert!(
            sp > prev_sp,
            "Social proof must strictly increase up to plateau (500): N={n}, prev={prev_sp}, cur={sp}"
        );
        prev_sp = sp;
    }

    let sp_500 = calculate_social_proof_factor(500);
    assert!((sp_500 - 1.924).abs() < 0.01);

    // Strictly decreasing for N > 500 (soft viral plateau taper)
    let mut prev_sp_taper = sp_500;
    for &n in &[600, 1000, 5000, 20000, 100_000] {
        let sp = calculate_social_proof_factor(n);
        assert!(
            sp < prev_sp_taper,
            "Social proof must taper off above 500: N={n}, prev={prev_sp_taper}, cur={sp}"
        );
        assert!(
            sp > 0.5,
            "Social proof taper must remain positive and bounded"
        );
        prev_sp_taper = sp;
    }
}

// ===========================================================================
// 5. recommend_preview_at Mathematical Score Breakdown Identities (Tier 1)
// ===========================================================================

#[test]
fn test_empirical_preview_score_breakdown_mathematical_identities() {
    let interner = Arc::new(StringInterner::new());
    let graph = Arc::new(GraphStore::new());
    let rec = Recommender::new(Arc::clone(&interner), Arc::clone(&graph));
    let now = BLUESKY_EPOCH_SECS + 60_000_000;

    let viewer_did = "did:plc:viewer_math_eval";
    let viewer_id = interner.intern(viewer_did);

    // Setup 15 seed posts for viewer (Tier 1 requires >= 10 likes)
    let mut seed_pids = Vec::new();
    for i in 0..15 {
        let uri = format!("at://did:plc:author_seed/app.bsky.feed.post/seed_{i}");
        let pid = interner.intern(&uri);
        let author_id = interner.intern("did:plc:author_seed");
        graph.record_post_meta(pid, author_id, None, None, now - 1000);
        graph.record_interaction(viewer_id, pid, SignalType::Like, now - 1000 + i as u64);
        seed_pids.push(pid);
    }

    // Create 20 co-interactors with overlapping likes
    for u in 0..20 {
        let co_did = format!("did:plc:co_math_{u:02}");
        let co_id = interner.intern(&co_did);

        // Overlap on first 5 seeds
        for &seed_pid in &seed_pids[0..5] {
            graph.record_interaction(co_id, seed_pid, SignalType::Like, now - 2000);
        }

        // Recommend distinct candidate posts with multiple likes to pass engagement floor
        for p in 0..3 {
            let cand_uri = format!("at://did:plc:cand_author_{u}_{p}/app.bsky.feed.post/p_{p}");
            let cand_pid = interner.intern(&cand_uri);
            let author_id = interner.intern(&format!("did:plc:cand_author_{u}_{p}"));
            graph.record_post_meta(cand_pid, author_id, None, None, now - 500);
            // Record 3 likes from various users to meet default engagement floor
            graph.record_interaction(co_id, cand_pid, SignalType::Like, now - 500);
            let dummy_u1 = interner.intern(&format!("did:plc:dummy_{u}_{p}_1"));
            let dummy_u2 = interner.intern(&format!("did:plc:dummy_{u}_{p}_2"));
            graph.record_interaction(dummy_u1, cand_pid, SignalType::Like, now - 500);
            graph.record_interaction(dummy_u2, cand_pid, SignalType::Like, now - 500);
        }
    }

    let dials = RecommendationDials {
        limit: 50,
        min_likes: 0,
        half_life_secs: 7200.0,
        explain: true,
        topic_weights: TopicWeights::default(),
        ..Default::default()
    };

    let preview = rec
        .recommend_preview_at(Some(viewer_did), &dials, now)
        .unwrap();
    assert!(!preview.items.is_empty(), "Preview must produce items");

    for item in &preview.items {
        let b = &item.score_breakdown;
        let expected_final = b.taste_similarity * b.time_decay * b.topic_boost * b.fatigue_penalty;
        assert!(
            (b.final_score - expected_final).abs() < 1e-4,
            "Mathematical identity final_score = taste_similarity * time_decay * topic_boost * fatigue_penalty violated: final_score={}, calculated={}",
            b.final_score,
            expected_final
        );
        assert!(
            b.time_decay > 0.0 && b.time_decay <= 1.0,
            "time_decay must be in (0, 1]: got {}",
            b.time_decay
        );
        assert!(
            b.fatigue_penalty > 0.0 && b.fatigue_penalty <= 1.0,
            "fatigue_penalty must be in (0, 1]: got {}",
            b.fatigue_penalty
        );
        assert!(
            b.taste_similarity > 0.0,
            "taste_similarity must be positive: got {}",
            b.taste_similarity
        );
    }

    // Verify descending sort order
    for i in 0..preview.items.len().saturating_sub(1) {
        assert!(
            preview.items[i].score_breakdown.final_score
                >= preview.items[i + 1].score_breakdown.final_score,
            "Preview items must be strictly sorted descending by final_score: item[{i}]={} < item[{}]={}",
            preview.items[i].score_breakdown.final_score,
            i + 1,
            preview.items[i + 1].score_breakdown.final_score
        );
    }
}

// ===========================================================================
// 6. Defensive Bounds Exactness Under Viral Fanout & Top-K Ranking
// ===========================================================================

#[test]
fn test_empirical_preview_defensive_bounds_exact_scoring_under_viral_fanout() {
    let interner = Arc::new(StringInterner::new());
    let graph = Arc::new(GraphStore::new());
    let rec = Recommender::new(Arc::clone(&interner), Arc::clone(&graph));
    let now = BLUESKY_EPOCH_SECS + 70_000_000;

    let viewer_did = "did:plc:viral_bounds_viewer";
    let viewer_id = interner.intern(viewer_did);

    // 1. Create 80 seed posts for viewer (MAX_SEED_POSTS = 50)
    // Posts 0..30 are old, posts 30..80 are recent
    let mut old_pids = Vec::new();
    let mut recent_pids = Vec::new();
    for i in 0..80 {
        let uri = format!("at://did:plc:author_v/app.bsky.feed.post/seed_{i}");
        let pid = interner.intern(&uri);
        let author_id = interner.intern("did:plc:author_v");
        let ts = now - 500_000 + (i as u64 * 1000);
        graph.record_post_meta(pid, author_id, None, None, ts);
        graph.record_interaction(viewer_id, pid, SignalType::Like, ts);
        if i < 30 {
            old_pids.push(pid);
        } else {
            recent_pids.push(pid);
        }
    }
    assert_eq!(old_pids.len(), 30);
    assert_eq!(recent_pids.len(), 50);

    // 2. Create a "phantom twin" who only interacted with OLD seed posts (0..30)
    // Because seed posts are capped to last 50, this phantom twin must NOT be reached during Tier 1 walk!
    let phantom_did = "did:plc:phantom_twin";
    let phantom_id = interner.intern(phantom_did);
    for &pid in &old_pids {
        graph.record_interaction(phantom_id, pid, SignalType::Like, now - 400_000);
    }
    let phantom_cand_uri = "at://did:plc:phantom_cand_auth/app.bsky.feed.post/phantom_cand";
    let phantom_cand_pid = interner.intern(phantom_cand_uri);
    let phantom_cand_auth = interner.intern("did:plc:phantom_cand_auth");
    graph.record_post_meta(phantom_cand_pid, phantom_cand_auth, None, None, now - 100);
    graph.record_interaction(phantom_id, phantom_cand_pid, SignalType::Like, now - 100);

    // 3. Create 150 co-interactors on the 50 RECENT seed posts
    // We give them varying overlap counts from 2 up to 20 shared likes.
    for u in 0..150 {
        let co_did = format!("did:plc:recent_co_{u:03}");
        let co_id = interner.intern(&co_did);

        let shared_count = (u % 10) + 2; // 2..11 shared likes
        for s in 0..shared_count {
            graph.record_interaction(co_id, recent_pids[s], SignalType::Like, now - 50_000);
        }

        // Recommend one candidate post per co-interactor
        let cand_uri = format!("at://did:plc:recent_cand_auth_{u:03}/app.bsky.feed.post/cand");
        let cand_pid = interner.intern(&cand_uri);
        let cand_auth = interner.intern(&format!("did:plc:recent_cand_auth_{u:03}"));
        graph.record_post_meta(cand_pid, cand_auth, None, None, now - 1000);
        graph.record_interaction(co_id, cand_pid, SignalType::Like, now - 1000);
    }

    let dials = RecommendationDials {
        limit: 50,
        min_likes: 0,
        ..Default::default()
    };

    let preview = rec
        .recommend_preview_at(Some(viewer_did), &dials, now)
        .unwrap();

    // 1. Check candidate evaluation count: exactly MAX_CO_INTERACTORS (100)
    assert_eq!(
        preview.total_candidates, MAX_CO_INTERACTORS,
        "Total candidates evaluated must equal MAX_CO_INTERACTORS (100)"
    );

    // 2. Check phantom candidate from old seed posts is NOT present
    let has_phantom = preview
        .items
        .iter()
        .any(|it| it.uri.as_str() == phantom_cand_uri);
    assert!(
        !has_phantom,
        "Candidate from phantom twin on seed posts older than top 50 must be completely excluded"
    );

    // 3. Verify all returned preview items have positive final scores
    for it in &preview.items {
        assert!(
            it.score_breakdown.final_score > 0.0,
            "All preview final scores must be positive"
        );
    }
}

// ===========================================================================
// 7. Tier 2 and Tier 3 Mathematical Breakdown Verification
// ===========================================================================

#[test]
fn test_empirical_tier2_and_tier3_score_breakdowns() {
    let interner = Arc::new(StringInterner::new());
    let graph = Arc::new(GraphStore::new());
    let rec = Recommender::new(Arc::clone(&interner), Arc::clone(&graph));
    let now = BLUESKY_EPOCH_SECS + 65_000_000;

    // --- Scenario A: Tier 2 (Follow Walk) ---
    // User with 2 likes (< 10 threshold for Tier 1) and 1 followed author
    let viewer2_did = "did:plc:viewer_tier2";
    let viewer2_id = interner.intern(viewer2_did);

    let p_like1 = interner.intern("at://did:plc:a1/app.bsky.feed.post/l1");
    let p_like2 = interner.intern("at://did:plc:a1/app.bsky.feed.post/l2");
    graph.record_post_meta(
        p_like1,
        interner.intern("did:plc:a1"),
        None,
        None,
        now - 1000,
    );
    graph.record_post_meta(
        p_like2,
        interner.intern("did:plc:a1"),
        None,
        None,
        now - 1000,
    );
    graph.record_interaction(viewer2_id, p_like1, SignalType::Like, now - 1000);
    graph.record_interaction(viewer2_id, p_like2, SignalType::Like, now - 1000);

    let followed_did = "did:plc:followed_author";
    let followed_id = interner.intern(followed_did);
    graph.record_follow(viewer2_id, followed_id);

    // Followed user interacted with a candidate post
    let cand_tier2_uri = "at://did:plc:cand_tier2/app.bsky.feed.post/t2";
    let cand_tier2_pid = interner.intern(cand_tier2_uri);
    let cand_tier2_author = interner.intern("did:plc:cand_tier2");
    graph.record_post_meta(cand_tier2_pid, cand_tier2_author, None, None, now - 200);
    graph.record_interaction(followed_id, cand_tier2_pid, SignalType::Like, now - 200);

    let dials = RecommendationDials {
        min_likes: 0,
        ..Default::default()
    };

    let preview_t2 = rec
        .recommend_preview_at(Some(viewer2_did), &dials, now)
        .unwrap();
    assert_eq!(preview_t2.items.len(), 1);
    let item_t2 = &preview_t2.items[0];
    let b2 = &item_t2.score_breakdown;

    // In Tier 2:
    // taste_similarity = 1.5 * social_proof
    // final_score = taste_similarity * time_decay * topic_boost * fatigue_penalty
    let social_proof = calculate_social_proof_factor(1);
    assert!((b2.taste_similarity - 1.5 * social_proof).abs() < 1e-5);
    let expected_final_t2 =
        b2.taste_similarity * b2.time_decay * b2.topic_boost * b2.fatigue_penalty;
    assert!((b2.final_score - expected_final_t2).abs() < 1e-5);

    // --- Scenario B: Tier 3 (Cold Start / Velocity Pool) ---
    // Cold viewer with 0 likes and 0 follows
    let viewer3_did = "did:plc:cold_viewer";
    let _viewer3_id = interner.intern(viewer3_did);

    let pool_post_uri = "at://did:plc:pool_auth/app.bsky.feed.post/p1";
    let pool_post_pid = interner.intern(pool_post_uri);
    let pool_post_auth = interner.intern("did:plc:pool_auth");
    graph.record_post_meta(pool_post_pid, pool_post_auth, None, None, now - 100);
    // Add interactions to recent pool
    let active_u = interner.intern("did:plc:active_u");
    graph.record_interaction(active_u, pool_post_pid, SignalType::Like, now - 100);

    let preview_t3 = rec
        .recommend_preview_at(Some(viewer3_did), &dials, now)
        .unwrap();
    assert!(!preview_t3.items.is_empty());
    let item_t3 = &preview_t3.items[0];
    let b3 = &item_t3.score_breakdown;

    // In Tier 3:
    // taste_similarity = 100.0 / (idx + 1.0)
    // time_decay = 1.0
    // final_score = taste_similarity * topic_boost * fatigue_penalty
    assert_eq!(b3.time_decay, 1.0);
    assert!((b3.taste_similarity - 100.0).abs() < 1e-4);
    let expected_final_t3 = b3.taste_similarity * b3.topic_boost * b3.fatigue_penalty;
    assert!((b3.final_score - expected_final_t3).abs() < 1e-4);
}

// ===========================================================================
// 8. find_taste_twins Bayesian Mathematical Ranking & Thresholds
// ===========================================================================

#[test]
fn test_empirical_taste_twins_bayesian_ranking_and_bounding() {
    let interner = Arc::new(StringInterner::new());
    let graph = Arc::new(GraphStore::new());
    let rec = Recommender::new(Arc::clone(&interner), Arc::clone(&graph));
    let now = BLUESKY_EPOCH_SECS + 50_000_000;

    let viewer_did = "did:plc:twins_eval_viewer";
    let viewer_id = interner.intern(viewer_did);

    // Create 20 posts liked by viewer
    let mut viewer_pids = Vec::new();
    for i in 0..20 {
        let uri = format!("at://did:plc:auth/app.bsky.feed.post/twin_seed_{i}");
        let pid = interner.intern(&uri);
        let author_id = interner.intern("did:plc:auth");
        graph.record_post_meta(pid, author_id, None, None, now - 10_000);
        graph.record_interaction(viewer_id, pid, SignalType::Like, now - 10_000 + i as u64);
        viewer_pids.push(pid);
    }

    // Twin 1: 1 shared like (below MIN_SHARED_OVERLAP = 2) -> MUST BE EXCLUDED
    let twin1_did = "did:plc:twin_insufficient_overlap";
    let twin1_id = interner.intern(twin1_did);
    graph.record_interaction(twin1_id, viewer_pids[0], SignalType::Like, now - 5000);

    // Twin 2: 3 shared likes, 3 total likes (Cosine = 3 / sqrt(20 * 3) = 3 / 7.746 ~ 0.3873)
    // Shrinkage = 3 / (3 + 3) = 0.50 -> Conf ~ 0.1936
    let twin2_did = "did:plc:twin_moderate";
    let twin2_id = interner.intern(twin2_did);
    for &pid in &viewer_pids[0..3] {
        graph.record_interaction(twin2_id, pid, SignalType::Like, now - 5000);
    }

    // Twin 3: 10 shared likes, 10 total likes (Cosine = 10 / sqrt(20 * 10) = 10 / 14.142 ~ 0.7071)
    // Shrinkage = 10 / (10 + 3) = 10/13 ~ 0.7692 -> Conf ~ 0.5439
    let twin3_did = "did:plc:twin_strong";
    let twin3_id = interner.intern(twin3_did);
    for &pid in &viewer_pids[0..10] {
        graph.record_interaction(twin3_id, pid, SignalType::Like, now - 5000);
    }

    let twins_resp = rec.find_taste_twins(viewer_did, 10).unwrap();

    // Twin 1 must not be present
    assert!(
        !twins_resp
            .twins
            .iter()
            .any(|t| t.user_did.as_str() == twin1_did),
        "Twin with overlap < 2 must be excluded"
    );

    // Twin 3 and Twin 2 must be present
    assert_eq!(twins_resp.twins.len(), 2);
    assert_eq!(twins_resp.twins[0].user_did.as_str(), twin3_did);
    assert_eq!(twins_resp.twins[1].user_did.as_str(), twin2_did);

    // Strictly descending similarity confidence
    assert!(twins_resp.twins[0].similarity_score > twins_resp.twins[1].similarity_score);
}

// ===========================================================================
// 9. Author Diversity and Conversation Root Dampening Preservation
// ===========================================================================

#[test]
fn test_empirical_author_diversity_and_root_dampening_under_ties() {
    let interner = Arc::new(StringInterner::new());
    let graph = Arc::new(GraphStore::new());
    let rec = Recommender::new(Arc::clone(&interner), Arc::clone(&graph));
    let now = BLUESKY_EPOCH_SECS + 55_000_000;

    let viewer_did = "did:plc:diversity_viewer";
    let viewer_id = interner.intern(viewer_did);

    // Seed likes for viewer
    let mut seeds = Vec::new();
    for i in 0..12 {
        let uri = format!("at://did:plc:seed_auth/app.bsky.feed.post/s_{i}");
        let pid = interner.intern(&uri);
        graph.record_post_meta(
            pid,
            interner.intern("did:plc:seed_auth"),
            None,
            None,
            now - 5000,
        );
        graph.record_interaction(viewer_id, pid, SignalType::Like, now - 5000 + i as u64);
        seeds.push(pid);
    }

    // Co-interactor
    let co_id = interner.intern("did:plc:co_diversity");
    for &pid in &seeds[0..6] {
        graph.record_interaction(co_id, pid, SignalType::Like, now - 3000);
    }

    // 1. Same author flood: create 5 posts by single author "spammer_author"
    let spammer_did = "did:plc:spammer_author";
    let spammer_id = interner.intern(spammer_did);
    for i in 0..5 {
        let uri = format!("at://did:plc:spammer_author/app.bsky.feed.post/spam_{i}");
        let pid = interner.intern(&uri);
        graph.record_post_meta(pid, spammer_id, None, None, now - 100);
        graph.record_interaction(co_id, pid, SignalType::Like, now - 100);
    }

    // 2. Conversation root flood: create 4 replies sharing the same root_id
    let root_uri = "at://did:plc:root_author/app.bsky.feed.post/thread_root";
    let root_pid = interner.intern(root_uri);
    let thread_author = interner.intern("did:plc:thread_author");
    graph.record_post_meta(root_pid, thread_author, None, None, now - 100);

    for i in 0..4 {
        let reply_uri = format!("at://did:plc:diff_author_{i}/app.bsky.feed.post/reply_{i}");
        let reply_pid = interner.intern(&reply_uri);
        let author_i = interner.intern(&format!("did:plc:diff_author_{i}"));
        graph.record_post_meta(reply_pid, author_i, Some(root_pid), None, now - 50);
        graph.record_interaction(co_id, reply_pid, SignalType::Like, now - 50);
    }

    let dials = RecommendationDials {
        include_replies: true,
        min_likes: 0,
        limit: 20,
        ..Default::default()
    };

    let preview = rec
        .recommend_preview_at(Some(viewer_did), &dials, now)
        .unwrap();

    // Check author diversity: max 2 per author
    let mut author_counts: std::collections::HashMap<&str, usize> =
        std::collections::HashMap::new();
    for it in &preview.items {
        *author_counts.entry(it.author_did.as_str()).or_insert(0) += 1;
    }
    for (author, count) in author_counts {
        assert!(
            count <= 2,
            "Author diversity violated for author {author}: count={count} > 2"
        );
    }

    // Check conversation tree root dampening: max 1 per tree root
    let mut root_counts: std::collections::HashMap<u32, usize> = std::collections::HashMap::new();
    for it in &preview.items {
        let pid = interner.lookup_id(it.uri.as_str()).unwrap();
        let meta = graph.get_post_meta(pid);
        let root = meta.and_then(|m| m.root_id).unwrap_or(pid);
        *root_counts.entry(root).or_insert(0) += 1;
    }
    for (root, count) in root_counts {
        assert!(
            count <= 1,
            "Thread root dampening violated for root {root}: count={count} > 1"
        );
    }
}
