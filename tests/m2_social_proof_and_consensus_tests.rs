#![forbid(unsafe_code)]
#![allow(
    clippy::all,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    missing_docs
)]

use std::sync::Arc;

use for_your_consideration::prelude::*;

#[test]
fn test_continuous_social_proof_curve_values_and_regimes() {
    // 1. Unvetted baseline (N = 0)
    let s0 = calculate_social_proof_factor(0);
    assert!((s0 - 1.0 / 3.0).abs() < 1e-5);
    assert_eq!(s0, calculate_popularity_dampener(0));

    // 2. Early community signal (N = 3) -> ~0.806
    let s3 = calculate_social_proof_factor(3);
    assert!((s3 - 0.805_296).abs() < 1e-4);

    // 3. Established post (N = 10) -> ~1.150
    let s10 = calculate_social_proof_factor(10);
    assert!((s10 - 1.150_502).abs() < 1e-4);

    // 4. Validated post (N = 50) -> ~1.530
    let s50 = calculate_social_proof_factor(50);
    assert!((s50 - 1.529_78).abs() < 1e-4);

    // 5. Peak plateau threshold (N = 500) -> ~1.925
    let s500 = calculate_social_proof_factor(500);
    assert!((s500 - 1.924_807).abs() < 1e-4);

    // 6. Soft viral plateau taper (N = 5000) -> ~1.570
    let s5000 = calculate_social_proof_factor(5000);
    assert!((s5000 - 1.570_18).abs() < 1e-4);

    // 7. Mega-viral post (N = 50000) -> ~1.322
    let s50k = calculate_social_proof_factor(50_000);
    assert!((s50k - 1.322).abs() < 1e-2);

    // Monotonicity up to N = 500
    assert!(s0 < s3);
    assert!(s3 < s10);
    assert!(s10 < s50);
    assert!(s50 < s500);

    // Soft taper for N > 500 (never crashing to zero)
    assert!(s500 > s5000);
    assert!(s5000 > s50k);
    assert!(s50k > 1.0);
    assert!(s50k > s0);

    // Extreme scale safety (10M interactions)
    let s10m = calculate_social_proof_factor(10_000_000);
    assert!(!s10m.is_nan());
    assert!(!s10m.is_infinite());
    assert!(s10m > 0.0);
}

#[test]
fn test_multi_curator_consensus_boost_values_and_scaling() {
    // k <= 1 -> 1.0
    assert_eq!(calculate_consensus_boost(0), 1.0);
    assert_eq!(calculate_consensus_boost(1), 1.0);

    // k = 2 -> 1.0 + 0.45 * ln(2) ≈ 1.3119 (+31.2%)
    let b2 = calculate_consensus_boost(2);
    assert!((b2 - 1.311_916).abs() < 1e-4);

    // k = 3 -> 1.0 + 0.45 * ln(3) ≈ 1.4944 (+49.4%)
    let b3 = calculate_consensus_boost(3);
    assert!((b3 - 1.494_375).abs() < 1e-4);

    // k = 5 -> 1.0 + 0.45 * ln(5) ≈ 1.7243 (+72.4%)
    let b5 = calculate_consensus_boost(5);
    assert!((b5 - 1.724_246).abs() < 1e-4);

    // k = 10 -> 1.0 + 0.45 * ln(10) ≈ 2.0362 (+103.6%)
    let b10 = calculate_consensus_boost(10);
    assert!((b10 - 2.036_163).abs() < 1e-4);

    // Monotonic scaling for k >= 1
    assert!(calculate_consensus_boost(1) < calculate_consensus_boost(2));
    assert!(calculate_consensus_boost(2) < calculate_consensus_boost(3));
    assert!(calculate_consensus_boost(3) < calculate_consensus_boost(5));
    assert!(calculate_consensus_boost(5) < calculate_consensus_boost(10));
}

#[test]
fn test_tier1_multi_curator_compounding_end_to_end() {
    let interner = Arc::new(StringInterner::new());
    let graph = Arc::new(GraphStore::new());
    let rec = Recommender::new(Arc::clone(&interner), Arc::clone(&graph));
    let now = BLUESKY_EPOCH_SECS + 100_000;

    let viewer = interner.intern("did:plc:consensus_viewer");
    let twin_a = interner.intern("did:plc:twin_a");
    let twin_b = interner.intern("did:plc:twin_b");
    let twin_c = interner.intern("did:plc:twin_c");
    let author = interner.intern("did:plc:author_m2");

    // Viewer likes 10 posts to establish Tier 1 active status
    let mut seed_posts = Vec::new();
    for i in 1..=10 {
        let sp = interner.intern(&format!(
            "at://did:plc:author_m2/app.bsky.feed.post/seed_{i}"
        ));
        seed_posts.push(sp);
        graph.record_post_meta(sp, author, None, None, now - 2000);
        graph.record_interaction(viewer, sp, SignalType::Like, now - 1500);
    }

    // Twin A, Twin B, Twin C all like seed_posts[0] and seed_posts[1] (shared_likes = 2 >= MIN_SHARED_OVERLAP)
    for &twin in &[twin_a, twin_b, twin_c] {
        graph.record_interaction(twin, seed_posts[0], SignalType::Like, now - 1400);
        graph.record_interaction(twin, seed_posts[1], SignalType::Like, now - 1400);
    }

    // Candidate 1: p_consensus is liked by all 3 twins
    let p_consensus = interner.intern("at://did:plc:author_m2/app.bsky.feed.post/cand_consensus");
    graph.record_post_meta(p_consensus, author, None, None, now - 1000);
    graph.record_interaction(twin_a, p_consensus, SignalType::Like, now - 500);
    graph.record_interaction(twin_b, p_consensus, SignalType::Like, now - 500);
    graph.record_interaction(twin_c, p_consensus, SignalType::Like, now - 500);

    // Candidate 2: p_single is liked by ONLY Twin A
    let p_single = interner.intern("at://did:plc:author_m2/app.bsky.feed.post/cand_single");
    graph.record_post_meta(p_single, author, None, None, now - 1000);
    graph.record_interaction(twin_a, p_single, SignalType::Like, now - 500);

    let dials = RecommendationDials {
        limit: 10,
        min_likes: 1,
        ..Default::default()
    };

    let result = rec
        .recommend(Some("did:plc:consensus_viewer"), &dials, now)
        .unwrap();
    assert_eq!(result.posts.len(), 2);

    // Consensus post must rank #1
    assert_eq!(result.posts[0].post_id, p_consensus);
    assert_eq!(result.posts[1].post_id, p_single);

    // Score ratio reflects 3 curators vs 1 curator with ConsensusBoost(3) = 1.4944:
    let conf_a =
        calculate_bayesian_confidence(2.0 / (10.0f32 * 4.0).sqrt(), 2, DEFAULT_BAYESIAN_BETA);
    let conf_b =
        calculate_bayesian_confidence(2.0 / (10.0f32 * 3.0).sqrt(), 2, DEFAULT_BAYESIAN_BETA);
    let conf_c =
        calculate_bayesian_confidence(2.0 / (10.0f32 * 3.0).sqrt(), 2, DEFAULT_BAYESIAN_BETA);
    let total_affinity_consensus = conf_a + conf_b + conf_c;
    let total_affinity_single = conf_a;

    let expected_ratio = (total_affinity_consensus
        * calculate_consensus_boost(3)
        * calculate_social_proof_factor(3))
        / (total_affinity_single * calculate_consensus_boost(1) * calculate_social_proof_factor(1));

    let actual_ratio = result.posts[0].score / result.posts[1].score;
    assert!(
        (actual_ratio - expected_ratio).abs() < 1e-4,
        "Expected ratio {expected_ratio:.4}, found {actual_ratio:.4}"
    );
}

#[test]
fn test_tier1_social_proof_boost_end_to_end() {
    let interner = Arc::new(StringInterner::new());
    let graph = Arc::new(GraphStore::new());
    let rec = Recommender::new(Arc::clone(&interner), Arc::clone(&graph));
    let now = BLUESKY_EPOCH_SECS + 100_000;

    let viewer = interner.intern("did:plc:proof_viewer");
    let twin = interner.intern("did:plc:proof_twin");
    let author = interner.intern("did:plc:author_proof");

    // Establish Tier 1 eligibility
    for i in 1..=10 {
        let sp = interner.intern(&format!(
            "at://did:plc:author_proof/app.bsky.feed.post/sp_{i}"
        ));
        graph.record_post_meta(sp, author, None, None, now - 2000);
        graph.record_interaction(viewer, sp, SignalType::Like, now - 1500);
        if i <= 2 {
            graph.record_interaction(twin, sp, SignalType::Like, now - 1400);
        }
    }

    // Candidate 1: p_validated has 50 interactions
    let p_validated =
        interner.intern("at://did:plc:author_proof/app.bsky.feed.post/cand_validated");
    graph.record_post_meta(p_validated, author, None, None, now - 1000);
    graph.record_interaction(twin, p_validated, SignalType::Like, now - 500);
    for u in 1..=49 {
        let other_u = interner.intern(&format!("did:plc:other_user_{u}"));
        graph.record_interaction(other_u, p_validated, SignalType::Like, now - 500);
    }

    // Candidate 2: p_unvetted has only 1 interaction (by twin)
    let p_unvetted = interner.intern("at://did:plc:author_proof/app.bsky.feed.post/cand_unvetted");
    graph.record_post_meta(p_unvetted, author, None, None, now - 1000);
    graph.record_interaction(twin, p_unvetted, SignalType::Like, now - 500);

    let dials = RecommendationDials {
        limit: 10,
        min_likes: 1,
        ..Default::default()
    };

    let result = rec
        .recommend(Some("did:plc:proof_viewer"), &dials, now)
        .unwrap();
    assert_eq!(result.posts.len(), 2);

    // Validated post must rank #1 over unvetted post
    assert_eq!(result.posts[0].post_id, p_validated);
    assert_eq!(result.posts[1].post_id, p_unvetted);

    // Ratio = S(50) / S(1) = 1.5298 / 0.5520 ≈ 2.77
    let ratio = result.posts[0].score / result.posts[1].score;
    assert!(
        ratio > 2.5,
        "Validated post with 50 likes must receive strong social proof boost over 1-like post"
    );
}

#[test]
fn test_preview_score_breakdown_multi_curator_exactness() {
    let interner = Arc::new(StringInterner::new());
    let graph = Arc::new(GraphStore::new());
    let rec = Recommender::new(Arc::clone(&interner), Arc::clone(&graph));
    let now = BLUESKY_EPOCH_SECS + 100_000;

    let viewer = interner.intern("did:plc:preview_math_viewer");
    let twin1 = interner.intern("did:plc:preview_math_twin1");
    let twin2 = interner.intern("did:plc:preview_math_twin2");
    let author = interner.intern("did:plc:preview_math_author");

    for i in 1..=10 {
        let sp = interner.intern(&format!(
            "at://did:plc:preview_math_author/app.bsky.feed.post/sp_{i}"
        ));
        graph.record_post_meta(sp, author, None, None, now - 2000);
        graph.record_interaction(viewer, sp, SignalType::Like, now - 1500);
        if i <= 2 {
            graph.record_interaction(twin1, sp, SignalType::Like, now - 1400);
            graph.record_interaction(twin2, sp, SignalType::Like, now - 1400);
        }
    }

    let cand = interner.intern("at://did:plc:preview_math_author/app.bsky.feed.post/target");
    graph.record_post_meta(cand, author, None, None, now - 1000);
    graph.record_interaction(twin1, cand, SignalType::Like, now - 500);
    graph.record_interaction(twin2, cand, SignalType::Like, now - 500);

    let dials = RecommendationDials {
        explain: true,
        min_likes: 1,
        ..Default::default()
    };

    let preview = rec
        .recommend_preview_at(Some("did:plc:preview_math_viewer"), &dials, now)
        .unwrap();
    assert_eq!(preview.items.len(), 1);

    let item = &preview.items[0];
    let b = &item.score_breakdown;

    // Verify mathematical breakdown integrity:
    // final_score = taste_similarity * time_decay * topic_boost * fatigue_penalty
    let computed_final = b.taste_similarity * b.time_decay * b.topic_boost * b.fatigue_penalty;
    assert!(
        (b.final_score - computed_final).abs() < 1e-4,
        "Score breakdown factors must mathematically multiply to final_score"
    );
}
