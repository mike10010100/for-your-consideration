#![forbid(unsafe_code)]
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    missing_docs,
    clippy::float_cmp,
    clippy::suboptimal_flops
)]

use std::sync::Arc;

use for_your_consideration::prelude::*;

// =========================================================================
// 1. Social Proof Curve ($S(N)$) Analytical & Monotonicity Stress Tests
// =========================================================================

#[test]
fn test_social_proof_monotonicity_0_to_500() {
    let mut prev_score = calculate_social_proof_factor(0);
    assert!((prev_score - 1.0 / 3.0).abs() < 1e-5);

    // Verify strict monotonic increase for every single integer from 0 to 500
    for n in 1..=500 {
        let current_score = calculate_social_proof_factor(n);
        assert!(
            current_score > prev_score,
            "S(N) must be strictly monotonic on [0, 500]: S({n}) = {current_score} vs S({}) = {prev_score}",
            n - 1
        );
        prev_score = current_score;
    }

    // Verify analytical anchor points
    let s0 = calculate_social_proof_factor(0);
    let s1 = calculate_social_proof_factor(1);
    let s3 = calculate_social_proof_factor(3);
    let s10 = calculate_social_proof_factor(10);
    let s50 = calculate_social_proof_factor(50);
    let s500 = calculate_social_proof_factor(500);

    let expected_s0 = 1.0 / 3.0;
    let expected_s1 = 0.5 * (1.0 + 0.15 * 2.0f32.ln());
    let expected_s3 = (4.0 / 6.0) * (1.0 + 0.15 * 4.0f32.ln());
    let expected_s10 = (11.0 / 13.0) * (1.0 + 0.15 * 11.0f32.ln());
    let expected_s50 = (51.0 / 53.0) * (1.0 + 0.15 * 51.0f32.ln());
    let expected_s500 = (501.0 / 503.0) * (1.0 + 0.15 * 501.0f32.ln());

    assert!((s0 - expected_s0).abs() < 1e-5);
    assert!((s1 - expected_s1).abs() < 1e-5);
    assert!((s3 - expected_s3).abs() < 1e-5);
    assert!((s10 - expected_s10).abs() < 1e-5);
    assert!((s50 - expected_s50).abs() < 1e-5);
    assert!((s500 - expected_s500).abs() < 1e-5);
}

#[test]
fn test_social_proof_soft_logarithmic_plateau_taper() {
    let s500 = calculate_social_proof_factor(500);

    // Test points above threshold: taper should be smooth and monotonically decreasing
    let test_points = [
        501, 600, 1000, 2500, 5000, 10_000, 25_000, 50_000, 100_000, 500_000, 1_000_000, 5_000_000,
        10_000_000,
    ];

    let mut prev_score = s500;
    for &n in &test_points {
        let score = calculate_social_proof_factor(n);

        // Score must decrease monotonically after 500
        assert!(
            score < prev_score,
            "S(N) must decrease for N > 500: S({n}) = {score} vs prev = {prev_score}"
        );

        // Score must remain strictly positive and non-zero
        assert!(
            score > 0.0,
            "S(N) must never collapse to <= 0: S({n}) = {score}"
        );

        // For N <= 5,000,000, score remains >= 1.0 (above neutral baseline)
        if n <= 5_000_000 {
            assert!(
                score >= 1.0,
                "S({n}) = {score} should remain >= 1.0 within normal viral scale"
            );
        }

        // Even at 10,000,000 interactions, score stays far above unvetted noise S(0) = 0.333
        assert!(
            score > 0.333,
            "Mega-viral post S({n}) = {score} must still outperform unvetted noise (0.333)"
        );

        prev_score = score;
    }

    // Verify exact values for 5k and 50k
    let s5k = calculate_social_proof_factor(5000);
    let s50k = calculate_social_proof_factor(50_000);
    let s10m = calculate_social_proof_factor(10_000_000);

    // S(5000) ≈ (5001/5003) * (1 + 0.15*ln(501)) / (1 + 0.10*ln(1 + 4500/500))
    //          ≈ 0.9996 * 1.932491 / (1 + 0.10*ln(10)) ≈ 1.931718 / 1.2302585 ≈ 1.570172
    assert!((s5k - 1.570_172).abs() < 1e-4);

    // S(50000) ≈ 1.0 * 1.932491 / (1 + 0.10*ln(100)) ≈ 1.932491 / 1.460517 ≈ 1.323155
    assert!((s50k - 1.323_155).abs() < 1e-3);

    // S(10M) ≈ 1.0 * 1.932491 / (1 + 0.10*ln(20000)) ≈ 1.932491 / 1.990349 ≈ 0.97093
    assert!((s10m - 0.970_93).abs() < 1e-3);
}

// =========================================================================
// 2. Multi-Curator Consensus Boost Analytical & Compounding Stress Tests
// =========================================================================

#[test]
fn test_consensus_boost_compounding_ratios() {
    let k_values = [0, 1, 2, 3, 5, 10, 50, 100];
    let expected_boosts = [
        1.0,                        // k = 0
        1.0,                        // k = 1
        1.0 + 0.45 * 2.0f32.ln(),   // k = 2 ≈ 1.311916
        1.0 + 0.45 * 3.0f32.ln(),   // k = 3 ≈ 1.494375
        1.0 + 0.45 * 5.0f32.ln(),   // k = 5 ≈ 1.724246
        1.0 + 0.45 * 10.0f32.ln(),  // k = 10 ≈ 2.036163
        1.0 + 0.45 * 50.0f32.ln(),  // k = 50 ≈ 2.760408
        1.0 + 0.45 * 100.0f32.ln(), // k = 100 ≈ 3.072326
    ];

    for (&k, &expected) in k_values.iter().zip(expected_boosts.iter()) {
        let actual = calculate_consensus_boost(k);
        assert!(
            (actual - expected).abs() < 1e-5,
            "ConsensusBoost({k}) = {actual}, expected {expected}"
        );
    }

    // Verify strict monotonicity for k >= 1
    for k in 1..1000 {
        let b_k = calculate_consensus_boost(k);
        let b_next = calculate_consensus_boost(k + 1);
        assert!(
            b_next > b_k,
            "ConsensusBoost must strictly increase: B({k}+1) = {b_next} vs B({k}) = {b_k}"
        );
    }

    // Verify diminishing marginal returns (concavity of logarithm):
    // Delta(k -> k+1) > Delta(k+1 -> k+2)
    for k in 1..100 {
        let delta1 = calculate_consensus_boost(k + 1) - calculate_consensus_boost(k);
        let delta2 = calculate_consensus_boost(k + 2) - calculate_consensus_boost(k + 1);
        assert!(
            delta1 > delta2,
            "ConsensusBoost marginal return must decrease (concave): delta1={delta1} vs delta2={delta2} at k={k}"
        );
    }
}

// =========================================================================
// 3. Invariant Fuzzing: Extreme Interactions, Infinity, NaN, Edge Cases
// =========================================================================

#[test]
fn test_social_proof_and_consensus_extreme_fuzzing() {
    // 1. Extreme interaction count (usize::MAX)
    let s_max = calculate_social_proof_factor(usize::MAX);
    assert!(!s_max.is_nan(), "S(usize::MAX) must not be NaN");
    assert!(!s_max.is_infinite(), "S(usize::MAX) must not be infinite");
    assert!(s_max > 0.0, "S(usize::MAX) must be strictly positive");
    assert!(s_max < 2.0, "S(usize::MAX) must be bounded");

    // 2. Extreme curator count (usize::MAX)
    let b_max = calculate_consensus_boost(usize::MAX);
    assert!(
        !b_max.is_nan(),
        "ConsensusBoost(usize::MAX) must not be NaN"
    );
    assert!(
        !b_max.is_infinite(),
        "ConsensusBoost(usize::MAX) must not be infinite"
    );
    assert!(
        b_max > 1.0,
        "ConsensusBoost(usize::MAX) must be greater than 1.0"
    );
    assert!(
        b_max < 30.0,
        "ConsensusBoost(usize::MAX) must remain realistically bounded"
    );

    // 3. Boundary points around threshold 500
    let s499 = calculate_social_proof_factor(499);
    let s500 = calculate_social_proof_factor(500);
    let s501 = calculate_social_proof_factor(501);

    assert!(s499 < s500, "S(499) must be < S(500)");
    assert!(s501 < s500, "S(501) must be < S(500)");
    // C0 continuity across boundary
    assert!(
        (s500 - s499).abs() < 0.01,
        "Continuity step at threshold must be small"
    );
    assert!(
        (s500 - s501).abs() < 0.01,
        "Continuity step at threshold must be small"
    );

    // 4. Dense sweep of interaction counts for numeric stability
    for step in [
        0,
        1,
        2,
        5,
        10,
        50,
        100,
        200,
        499,
        500,
        501,
        1000,
        10_000,
        100_000,
        1_000_000,
        100_000_000,
    ] {
        let s = calculate_social_proof_factor(step);
        assert!(s.is_finite(), "S({step}) must be finite");
        assert!(s > 0.0, "S({step}) must be positive");
    }

    // 5. Backward compatibility alias invariant
    for &n in &[0, 1, 10, 50, 500, 5000, 50000, usize::MAX] {
        assert_eq!(
            calculate_social_proof_factor(n),
            calculate_popularity_dampener(n),
            "Alias calculate_popularity_dampener({n}) must be strictly identical"
        );
    }
}

// =========================================================================
// 4. End-to-End Tournament Simulation & Mathematical Invariants
// =========================================================================

#[test]
fn test_tournament_multi_curator_and_social_proof_ranking() {
    let interner = Arc::new(StringInterner::new());
    let graph = Arc::new(GraphStore::new());
    let rec = Recommender::new(Arc::clone(&interner), Arc::clone(&graph));
    let now = BLUESKY_EPOCH_SECS + 500_000;

    let viewer = interner.intern("did:plc:tournament_viewer");
    let author_a = interner.intern("did:plc:tournament_author_a");
    let author_b = interner.intern("did:plc:tournament_author_b");
    let author_c = interner.intern("did:plc:tournament_author_c");

    let twin1 = interner.intern("did:plc:twin_1");
    let twin2 = interner.intern("did:plc:twin_2");
    let twin3 = interner.intern("did:plc:twin_3");
    let twin4 = interner.intern("did:plc:twin_4");
    let twin5 = interner.intern("did:plc:twin_5");

    let twins = [twin1, twin2, twin3, twin4, twin5];

    // Viewer seed interactions (10 posts)
    let mut seed_posts = Vec::new();
    for i in 1..=10 {
        let p = interner.intern(&format!(
            "at://did:plc:seed_author/app.bsky.feed.post/seed_{i}"
        ));
        seed_posts.push(p);
        let seed_author = interner.intern("did:plc:seed_author");
        graph.record_post_meta(p, seed_author, None, None, now - 5000);
        graph.record_interaction(viewer, p, SignalType::Like, now - 4000);
    }

    // Connect twins with identical shared overlap (2 shared likes each)
    for &twin in &twins {
        graph.record_interaction(twin, seed_posts[0], SignalType::Like, now - 3500);
        graph.record_interaction(twin, seed_posts[1], SignalType::Like, now - 3500);
    }

    // Candidate A (Author A): 5 twins endorse it (k=5), 10 total interactions
    let p_k5_n10 = interner.intern("at://did:plc:tournament_author_a/app.bsky.feed.post/k5_n10");
    graph.record_post_meta(p_k5_n10, author_a, None, None, now - 1000);
    for &twin in &twins {
        graph.record_interaction(twin, p_k5_n10, SignalType::Like, now - 100);
    }
    for u in 1..=5 {
        let other = interner.intern(&format!("did:plc:other_a_{u}"));
        graph.record_interaction(other, p_k5_n10, SignalType::Like, now - 100);
    }

    // Candidate B (Author B): 1 twin endorses it (k=1), 500 total interactions (peak viral)
    let p_k1_n500 = interner.intern("at://did:plc:tournament_author_b/app.bsky.feed.post/k1_n500");
    graph.record_post_meta(p_k1_n500, author_b, None, None, now - 1000);
    graph.record_interaction(twin1, p_k1_n500, SignalType::Like, now - 100);
    for u in 1..=499 {
        let other = interner.intern(&format!("did:plc:other_b_{u}"));
        graph.record_interaction(other, p_k1_n500, SignalType::Like, now - 100);
    }

    // Candidate C (Author C): 1 twin endorses it (k=1), 1 total interaction (unvetted)
    let p_k1_n1 = interner.intern("at://did:plc:tournament_author_c/app.bsky.feed.post/k1_n1");
    graph.record_post_meta(p_k1_n1, author_c, None, None, now - 1000);
    graph.record_interaction(twin1, p_k1_n1, SignalType::Like, now - 100);

    let dials = RecommendationDials {
        limit: 10,
        min_likes: 1,
        ..Default::default()
    };

    let result = rec
        .recommend(Some("did:plc:tournament_viewer"), &dials, now)
        .unwrap();

    assert_eq!(result.posts.len(), 3);

    // Verify ordering:
    // Candidate A has 5 curators: Affinity = 5 * conf, ConsensusBoost(5) = 1.7242, S(10) = 1.1505
    //   Score_A = 5 * conf * 1.7242 * 1.1505 ≈ 9.918 * conf
    // Candidate B has 1 curator: Affinity = 1 * conf, ConsensusBoost(1) = 1.0, S(500) = 1.9248
    //   Score_B = 1 * conf * 1.0 * 1.9248 ≈ 1.925 * conf
    // Candidate C has 1 curator: Affinity = 1 * conf, ConsensusBoost(1) = 1.0, S(1) = 0.5520
    //   Score_C = 1 * conf * 1.0 * 0.5520 ≈ 0.552 * conf

    assert_eq!(
        result.posts[0].post_id, p_k5_n10,
        "#1 must be 5-curator consensus post"
    );
    assert_eq!(
        result.posts[1].post_id, p_k1_n500,
        "#2 must be peak viral 500-interaction post"
    );
    assert_eq!(
        result.posts[2].post_id, p_k1_n1,
        "#3 must be 1-interaction unvetted post"
    );

    let score_a = result.posts[0].score;
    let score_b = result.posts[1].score;
    let score_c = result.posts[2].score;

    assert!(
        score_a > 4.5 * score_b,
        "5-curator post must substantially dominate 1-curator post"
    );
    assert!(
        score_b > 3.0 * score_c,
        "500-like post must substantially dominate 1-like post"
    );
}

// =========================================================================
// 5. Tier 2 & Tier 3 Social Proof Scaling Verification
// =========================================================================

#[test]
fn test_tier2_and_tier3_social_proof_integration() {
    let interner = Arc::new(StringInterner::new());
    let graph = Arc::new(GraphStore::new());
    let rec = Recommender::new(Arc::clone(&interner), Arc::clone(&graph));
    let now = BLUESKY_EPOCH_SECS + 500_000;

    let viewer = interner.intern("did:plc:tier2_viewer");
    let followed = interner.intern("did:plc:followed_user");
    let author1 = interner.intern("did:plc:author_tier2_1");
    let author2 = interner.intern("did:plc:author_tier2_2");

    // Viewer follows `followed`
    graph.record_follow(viewer, followed);

    // Candidate 1: 500 likes (peak social proof factor ~1.9248)
    let p_viral = interner.intern("at://did:plc:author_tier2_1/app.bsky.feed.post/p_viral");
    graph.record_post_meta(p_viral, author1, None, None, now - 2000);
    graph.record_interaction(followed, p_viral, SignalType::Like, now - 1000);
    for u in 1..=499 {
        let other = interner.intern(&format!("did:plc:other_v_{u}"));
        graph.record_interaction(other, p_viral, SignalType::Like, now - 1000);
    }

    // Candidate 2: 1 like (only followed user, unvetted factor ~0.5520)
    let p_unvetted = interner.intern("at://did:plc:author_tier2_2/app.bsky.feed.post/p_unvetted");
    graph.record_post_meta(p_unvetted, author2, None, None, now - 2000);
    graph.record_interaction(followed, p_unvetted, SignalType::Like, now - 1000);

    let dials = RecommendationDials {
        limit: 10,
        ..Default::default()
    };

    let scored_tier2 = rec.traverse_tier2(viewer, &dials, now);
    assert_eq!(scored_tier2.len(), 2);
    assert_eq!(scored_tier2[0].post_id, p_viral);
    assert_eq!(scored_tier2[1].post_id, p_unvetted);

    let ratio = scored_tier2[0].score / scored_tier2[1].score;
    let expected_ratio = calculate_social_proof_factor(500) / calculate_social_proof_factor(1);
    assert!(
        (ratio - expected_ratio).abs() < 1e-4,
        "Tier 2 score ratio {ratio} must match S(500)/S(1) = {expected_ratio}"
    );
}

// =========================================================================
// 6. Preview Breakdown Mathematical Decomposition Invariants
// =========================================================================

#[test]
fn test_preview_score_breakdown_mathematical_identity() {
    let interner = Arc::new(StringInterner::new());
    let graph = Arc::new(GraphStore::new());
    let rec = Recommender::new(Arc::clone(&interner), Arc::clone(&graph));
    let now = BLUESKY_EPOCH_SECS + 500_000;

    let viewer = interner.intern("did:plc:preview_id_viewer");
    let twin1 = interner.intern("did:plc:preview_id_twin1");
    let twin2 = interner.intern("did:plc:preview_id_twin2");
    let twin3 = interner.intern("did:plc:preview_id_twin3");
    let author = interner.intern("did:plc:preview_id_author");

    // Establish Tier 1 viewer history
    for i in 1..=10 {
        let sp = interner.intern(&format!(
            "at://did:plc:preview_id_author/app.bsky.feed.post/seed_{i}"
        ));
        graph.record_post_meta(sp, author, None, None, now - 3000);
        graph.record_interaction(viewer, sp, SignalType::Like, now - 2000);
        if i <= 3 {
            graph.record_interaction(twin1, sp, SignalType::Like, now - 1800);
            graph.record_interaction(twin2, sp, SignalType::Like, now - 1800);
            graph.record_interaction(twin3, sp, SignalType::Like, now - 1800);
        }
    }

    // Candidate target endorsed by 3 twins
    let cand = interner.intern("at://did:plc:preview_id_author/app.bsky.feed.post/target");
    graph.record_post_meta(cand, author, None, None, now - 1000);
    graph.record_interaction(twin1, cand, SignalType::Like, now - 500);
    graph.record_interaction(twin2, cand, SignalType::Like, now - 500);
    graph.record_interaction(twin3, cand, SignalType::Like, now - 500);

    let dials = RecommendationDials {
        explain: true,
        ..Default::default()
    };

    let preview = rec
        .recommend_preview_at(Some("did:plc:preview_id_viewer"), &dials, now)
        .unwrap();

    assert_eq!(preview.items.len(), 1);
    let item = &preview.items[0];
    let b = &item.score_breakdown;

    // Verify mathematical decomposition:
    // final_score = taste_similarity * time_decay * topic_boost * fatigue_penalty
    let expected_final = b.taste_similarity * b.time_decay * b.topic_boost * b.fatigue_penalty;
    assert!(
        (b.final_score - expected_final).abs() < 1e-4,
        "Decomposition must hold: final_score ({}) == taste_sim ({}) * time_decay ({}) * topic_boost ({}) * fatigue ({})",
        b.final_score,
        b.taste_similarity,
        b.time_decay,
        b.topic_boost,
        b.fatigue_penalty
    );
}
