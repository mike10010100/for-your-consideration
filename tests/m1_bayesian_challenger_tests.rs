#![allow(
    clippy::all,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::unreadable_literal,
    clippy::suboptimal_flops,
    clippy::approx_constant,
    clippy::excessive_precision,
    clippy::doc_markdown,
    missing_docs
)]

//! Comprehensive Empirical Challenge Test Suite for Milestone 1 (M1: Bayesian Taste Shrinkage & Overlap Filtering).
//!
//! Stress-tests:
//! 1. Mathematical boundary conditions for shrinkage: S=0, S=1, S=2, S=3, S=10^6, `S=usize::MAX`, negative/zero/infinite beta.
//! 2. Property invariants (monotonicity, boundedness, strict concavity, diminishing returns, float precision boundaries).
//! 3. `GraphStore` Bayesian cosine similarity oracle against ground truth.
//! 4. Tier 1 walk co-interactor traversal: strictly excludes single-overlap (S=1) users.
//! 5. Taste Twins API (`/api/taste-twins`): strictly excludes S=1 users, ranks S>=2 by confidence.
//! 6. Explain recommendation API: strictly rejects S=1 co-interactors for taste twin proof chains.
//! 7. Proptest property-based fuzzing of Bayesian shrinkage and confidence.

use for_your_consideration::prelude::*;
use proptest::prelude::*;
use std::sync::Arc;

// ===========================================================================
// Section 1: Pure Mathematical Boundary Conditions & Extreme Inputs
// ===========================================================================

#[test]
fn test_bayesian_shrinkage_boundary_conditions() {
    // S = 0 must return exactly 0.0 regardless of beta
    assert_eq!(calculate_bayesian_shrinkage(0, 3.0), 0.0);
    assert_eq!(calculate_bayesian_shrinkage(0, 10.0), 0.0);
    assert_eq!(calculate_bayesian_shrinkage(0, 0.0), 0.0);
    assert_eq!(calculate_bayesian_shrinkage(0, -5.0), 0.0);

    // S = 1 with beta = 3.0: 1 / (1 + 3) = 0.25 (75% penalty)
    let s1 = calculate_bayesian_shrinkage(1, 3.0);
    assert!((s1 - 0.25).abs() < 1e-6, "S=1 expected 0.25, got {s1}");

    // S = 2 with beta = 3.0: 2 / (2 + 3) = 0.40 (60% penalty)
    let s2 = calculate_bayesian_shrinkage(2, 3.0);
    assert!((s2 - 0.40).abs() < 1e-6, "S=2 expected 0.40, got {s2}");

    // S = 3 with beta = 3.0: 3 / (3 + 3) = 0.50 (50% penalty)
    let s3 = calculate_bayesian_shrinkage(3, 3.0);
    assert!((s3 - 0.50).abs() < 1e-6, "S=3 expected 0.50, got {s3}");

    // S = 7 with beta = 3.0: 7 / (7 + 3) = 0.70 (30% penalty)
    let s7 = calculate_bayesian_shrinkage(7, 3.0);
    assert!((s7 - 0.70).abs() < 1e-6, "S=7 expected 0.70, got {s7}");

    // S = 27 with beta = 3.0: 27 / 30 = 0.90 (10% penalty)
    let s27 = calculate_bayesian_shrinkage(27, 3.0);
    assert!((s27 - 0.90).abs() < 1e-6, "S=27 expected 0.90, got {s27}");

    // S = 10^6: 1_000_000 / 1_000_003 ≈ 0.999997 (approaches 1.0 asymptotically)
    let s_mil = calculate_bayesian_shrinkage(1_000_000, 3.0);
    assert!(s_mil > 0.99999, "S=10^6 expected > 0.99999, got {s_mil}");
    assert!(s_mil < 1.0, "S=10^6 must be strictly < 1.0, got {s_mil}");

    // Extreme S: usize::MAX must not overflow or panic
    let s_max = calculate_bayesian_shrinkage(usize::MAX, 3.0);
    assert!(
        (s_max - 1.0).abs() < 1e-5,
        "S=usize::MAX should be ≈ 1.0, got {s_max}"
    );

    // Non-positive beta must fall back to DEFAULT_BAYESIAN_BETA (3.0)
    assert_eq!(
        calculate_bayesian_shrinkage(3, 0.0),
        calculate_bayesian_shrinkage(3, DEFAULT_BAYESIAN_BETA)
    );
    assert_eq!(
        calculate_bayesian_shrinkage(3, -0.001),
        calculate_bayesian_shrinkage(3, DEFAULT_BAYESIAN_BETA)
    );
    assert_eq!(
        calculate_bayesian_shrinkage(3, -100.0),
        calculate_bayesian_shrinkage(3, DEFAULT_BAYESIAN_BETA)
    );
}

#[test]
fn test_bayesian_confidence_calculation_properties() {
    // Confidence = raw_cosine * shrinkage
    let raw = 0.85f32;
    let conf_0 = calculate_bayesian_confidence(raw, 0, 3.0);
    assert_eq!(conf_0, 0.0);

    let conf_1 = calculate_bayesian_confidence(raw, 1, 3.0);
    assert!((conf_1 - (0.85 * 0.25)).abs() < 1e-6);

    let conf_2 = calculate_bayesian_confidence(raw, 2, 3.0);
    assert!((conf_2 - (0.85 * 0.40)).abs() < 1e-6);

    let conf_3 = calculate_bayesian_confidence(raw, 3, 3.0);
    assert!((conf_3 - (0.85 * 0.50)).abs() < 1e-6);

    // Bayesian Inversion Demonstration:
    // User A has raw cosine 0.7071 with 2 shared likes -> Confidence = 0.7071 * 0.40 = 0.28284
    // User B has raw cosine 0.6708 with 3 shared likes -> Confidence = 0.6708 * 0.50 = 0.33540
    // Despite User A having higher raw cosine, User B wins due to higher statistical confidence!
    let raw_a = 0.707_106_8_f32;
    let raw_b = 0.670_820_4_f32;
    let conf_a = calculate_bayesian_confidence(raw_a, 2, 3.0);
    let conf_b = calculate_bayesian_confidence(raw_b, 3, 3.0);

    assert!(
        conf_b > conf_a,
        "User B (3 shared, raw 0.6708 -> conf {conf_b}) must rank higher than User A (2 shared, raw 0.7071 -> conf {conf_a})"
    );
}

#[test]
fn test_bayesian_shrinkage_strict_monotonicity_and_diminishing_returns() {
    // Verify strict monotonicity for S in 0..1000
    let mut prev = calculate_bayesian_shrinkage(0, 3.0);
    for s in 1..=1000 {
        let curr = calculate_bayesian_shrinkage(s, 3.0);
        assert!(
            curr > prev,
            "Strict monotonicity violated at S={s}: curr={curr} <= prev={prev}"
        );
        prev = curr;
    }

    // Verify diminishing returns (strict concavity): Δ(S -> S+1) > Δ(S+1 -> S+2)
    for s in 0..100 {
        let delta1 =
            calculate_bayesian_shrinkage(s + 1, 3.0) - calculate_bayesian_shrinkage(s, 3.0);
        let delta2 =
            calculate_bayesian_shrinkage(s + 2, 3.0) - calculate_bayesian_shrinkage(s + 1, 3.0);
        assert!(
            delta1 > delta2,
            "Concavity violated at S={s}: delta1={delta1} <= delta2={delta2}"
        );
    }
}

// ===========================================================================
// Section 2: GraphStore Bayesian Cosine Similarity Oracle
// ===========================================================================

#[test]
fn test_graph_store_bayesian_cosine_similarity_oracle() {
    let graph = GraphStore::new();

    let u1 = 1;
    let u2 = 2;
    let u3 = 3;
    let u4 = 4;
    let u_nonexistent = 999;

    // u1 likes posts 10, 20, 30, 40 (4 posts)
    for p in [10, 20, 30, 40] {
        graph.record_interaction(u1, p, SignalType::Like, 1000);
    }

    // u2 likes posts 10, 20 (2 shared with u1; total 2 likes)
    for p in [10, 20] {
        graph.record_interaction(u2, p, SignalType::Like, 1000);
    }

    // u3 likes post 10 only (1 shared with u1; total 1 like)
    graph.record_interaction(u3, 10, SignalType::Like, 1000);

    // u4 likes posts 100, 200 (0 shared with u1; total 2 likes)
    for p in [100, 200] {
        graph.record_interaction(u4, p, SignalType::Like, 1000);
    }

    // Self-similarity must be 1.0
    assert_eq!(graph.compute_bayesian_cosine_similarity(u1, u1, 3.0), 1.0);
    assert_eq!(graph.compute_bayesian_cosine_similarity(u2, u2, 3.0), 1.0);

    // Nonexistent users must return 0.0
    assert_eq!(
        graph.compute_bayesian_cosine_similarity(u1, u_nonexistent, 3.0),
        0.0
    );
    assert_eq!(
        graph.compute_bayesian_cosine_similarity(u_nonexistent, u1, 3.0),
        0.0
    );
    assert_eq!(
        graph.compute_bayesian_cosine_similarity(u_nonexistent, u_nonexistent, 3.0),
        1.0
    );

    // u1 and u4: 0 shared -> raw cosine 0.0 -> Bayesian cosine 0.0
    assert_eq!(graph.compute_bayesian_cosine_similarity(u1, u4, 3.0), 0.0);

    // u1 and u3: 1 shared like.
    // raw cosine = 1 / sqrt(4 * 1) = 0.50.
    // Bayesian shrinkage for S=1, beta=3.0 is 1/4 = 0.25.
    // Expected Bayesian cosine = 0.50 * 0.25 = 0.125.
    let sim_1_3 = graph.compute_bayesian_cosine_similarity(u1, u3, 3.0);
    assert!(
        (sim_1_3 - 0.125).abs() < 1e-6,
        "u1-u3 expected 0.125, got {sim_1_3}"
    );

    // u1 and u2: 2 shared likes.
    // raw cosine = 2 / sqrt(4 * 2) = 2 / sqrt(8) ≈ 0.7071068.
    // Bayesian shrinkage for S=2, beta=3.0 is 2/5 = 0.40.
    // Expected Bayesian cosine = 0.7071068 * 0.40 ≈ 0.2828427.
    let sim_1_2 = graph.compute_bayesian_cosine_similarity(u1, u2, 3.0);
    let expected_1_2 = (2.0f32 / (8.0f32).sqrt()) * 0.40f32;
    assert!(
        (sim_1_2 - expected_1_2).abs() < 1e-6,
        "u1-u2 expected {expected_1_2}, got {sim_1_2}"
    );
}

// ===========================================================================
// Section 3: Tier 1 Walk Co-Interactor Single-Overlap Hard Exclusion
// ===========================================================================

#[test]
fn test_tier1_walk_strictly_excludes_single_overlap_candidate_posts() {
    let interner = Arc::new(StringInterner::new());
    let graph = Arc::new(GraphStore::new());
    let rec = Recommender::new(Arc::clone(&interner), Arc::clone(&graph));

    let viewer = "did:plc:viewer_empirical";
    let single_overlap_user = "did:plc:user_single_overlap";
    let double_overlap_user = "did:plc:user_double_overlap";
    let triple_overlap_user = "did:plc:user_triple_overlap";
    let author = "did:plc:author100";

    let v_id = interner.intern(viewer);
    let s_id = interner.intern(single_overlap_user);
    let d_id = interner.intern(double_overlap_user);
    let t_id = interner.intern(triple_overlap_user);
    let a_id = interner.intern(author);

    // Posts liked by viewer: 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 (ensure >= 10 likes for Tier 1 active status)
    let mut viewer_posts = Vec::new();
    let now = BLUESKY_EPOCH_SECS + 100_000;
    for i in 1..=10 {
        let uri = format!("at://did:plc:author100/app.bsky.feed.post/seed_{i}");
        let pid = interner.intern(&uri);
        viewer_posts.push(pid);
        graph.record_post_meta(pid, a_id, None, None, now - 1000);
        graph.record_interaction(v_id, pid, SignalType::Like, now - 500);
    }

    // Candidate posts endorsed by co-users:
    // p_single: only liked by single_overlap_user (shared = 1 with viewer via viewer_posts[0])
    // p_double: only liked by double_overlap_user (shared = 2 with viewer via viewer_posts[0], viewer_posts[1])
    // p_triple: only liked by triple_overlap_user (shared = 3 with viewer via viewer_posts[0], viewer_posts[1], viewer_posts[2])
    let p_single = interner.intern("at://did:plc:author100/app.bsky.feed.post/cand_single");
    let p_double = interner.intern("at://did:plc:author100/app.bsky.feed.post/cand_double");
    let p_triple = interner.intern("at://did:plc:author100/app.bsky.feed.post/cand_triple");

    for &pid in &[p_single, p_double, p_triple] {
        graph.record_post_meta(pid, a_id, None, None, now - 300);
    }

    // Single overlap user likes viewer_posts[0] (shared 1) and p_single
    graph.record_interaction(s_id, viewer_posts[0], SignalType::Like, now - 400);
    graph.record_interaction(s_id, p_single, SignalType::Like, now - 200);

    // Double overlap user likes viewer_posts[0], viewer_posts[1] (shared 2) and p_double
    graph.record_interaction(d_id, viewer_posts[0], SignalType::Like, now - 400);
    graph.record_interaction(d_id, viewer_posts[1], SignalType::Like, now - 400);
    graph.record_interaction(d_id, p_double, SignalType::Like, now - 200);

    // Triple overlap user likes viewer_posts[0], viewer_posts[1], viewer_posts[2] (shared 3) and p_triple
    graph.record_interaction(t_id, viewer_posts[0], SignalType::Like, now - 400);
    graph.record_interaction(t_id, viewer_posts[1], SignalType::Like, now - 400);
    graph.record_interaction(t_id, viewer_posts[2], SignalType::Like, now - 400);
    graph.record_interaction(t_id, p_triple, SignalType::Like, now - 200);

    // 1. Test explain_recommendation:
    // p_single should NOT have a Tier 1 Taste Twin explanation because single_overlap_user is filtered out
    let single_uri = interner.lookup_str(p_single).unwrap();
    let explain_single = rec.explain_recommendation(viewer, &single_uri).unwrap();
    assert!(
        !explain_single
            .steps
            .iter()
            .any(|s| s.node_id == single_overlap_user),
        "Single-overlap user must not qualify as a taste twin in explanation"
    );

    // p_double should have a Tier 1 Taste Twin explanation with double_overlap_user
    let double_uri = interner.lookup_str(p_double).unwrap();
    let explain_double = rec.explain_recommendation(viewer, &double_uri).unwrap();
    assert!(
        explain_double
            .steps
            .iter()
            .any(|s| s.node_id == double_overlap_user),
        "Double-overlap user MUST qualify as a taste twin in explanation"
    );

    // 2. Test recommend_preview:
    let dials = RecommendationDials {
        explain: true,
        min_likes: 1,
        ..Default::default()
    };

    let preview = rec.recommend_preview(Some(viewer), &dials).unwrap();
    let feed_uris: Vec<String> = preview
        .items
        .iter()
        .map(|item| item.uri.to_string())
        .collect();

    // Verify p_single is NOT present in Tier 1 candidate generation
    assert!(
        !feed_uris.contains(&single_uri.to_string()),
        "p_single must be excluded from Tier 1 recommendations due to S=1 filter!"
    );

    // Verify p_triple and p_double ARE present
    assert!(
        feed_uris.contains(&double_uri.to_string()),
        "p_double must be present in Tier 1 recommendations!"
    );
    let triple_uri = interner.lookup_str(p_triple).unwrap();
    assert!(
        feed_uris.contains(&triple_uri.to_string()),
        "p_triple must be present in Tier 1 recommendations!"
    );

    // Triple overlap post must have higher taste_similarity score than double overlap post
    let item_triple = preview
        .items
        .iter()
        .find(|i| i.uri == triple_uri.as_str())
        .unwrap();
    let item_double = preview
        .items
        .iter()
        .find(|i| i.uri == double_uri.as_str())
        .unwrap();
    assert!(
        item_triple.score_breakdown.taste_similarity > item_double.score_breakdown.taste_similarity,
        "p_triple taste_similarity ({}) must exceed p_double taste_similarity ({})",
        item_triple.score_breakdown.taste_similarity,
        item_double.score_breakdown.taste_similarity
    );
}

// ===========================================================================
// Section 4: Taste Twins API Strict S=1 Exclusion & S>=2 Confidence Ranking
// ===========================================================================

#[test]
fn test_taste_twins_api_strict_single_overlap_exclusion_and_confidence_ranking() {
    let interner = Arc::new(StringInterner::new());
    let graph = Arc::new(GraphStore::new());
    let rec = Recommender::new(Arc::clone(&interner), Arc::clone(&graph));

    let viewer = "did:plc:twin_viewer";
    let v_id = interner.intern(viewer);
    let author_id = interner.intern("did:plc:author200");

    // Viewer likes 10 posts: 1..=10
    let now = BLUESKY_EPOCH_SECS + 5000;
    for i in 1..=10 {
        let uri = format!("at://did:plc:author200/app.bsky.feed.post/{i}");
        let pid = interner.intern(&uri);
        graph.record_post_meta(pid, author_id, None, None, now);
        graph.record_interaction(v_id, pid, SignalType::Like, now);
    }

    // User A: 1 shared like (Post 1) -> MUST BE EXCLUDED
    let user_a = "did:plc:user_a_single";
    let a_id = interner.intern(user_a);
    let p1 = interner.intern("at://did:plc:author200/app.bsky.feed.post/1");
    graph.record_interaction(a_id, p1, SignalType::Like, now);

    // User B: 2 shared likes (Posts 1, 2) + 0 extra likes (total 2 likes)
    // raw cosine = 2 / sqrt(10 * 2) = 2 / sqrt(20) ≈ 0.4472136
    // Shrinkage(2, 3.0) = 2 / 5 = 0.40
    // Confidence = 0.4472136 * 0.40 ≈ 0.1788854
    let user_b = "did:plc:user_b_double";
    let b_id = interner.intern(user_b);
    let p2 = interner.intern("at://did:plc:author200/app.bsky.feed.post/2");
    graph.record_interaction(b_id, p1, SignalType::Like, now);
    graph.record_interaction(b_id, p2, SignalType::Like, now);

    // User C: 3 shared likes (Posts 1, 2, 3) + 3 extra likes (total 6 likes)
    // raw cosine = 3 / sqrt(10 * 6) = 3 / sqrt(60) ≈ 0.3872983
    // Shrinkage(3, 3.0) = 3 / 6 = 0.50
    // Confidence = 0.3872983 * 0.50 ≈ 0.1936491
    let user_c = "did:plc:user_c_triple";
    let c_id = interner.intern(user_c);
    let p3 = interner.intern("at://did:plc:author200/app.bsky.feed.post/3");
    graph.record_interaction(c_id, p1, SignalType::Like, now);
    graph.record_interaction(c_id, p2, SignalType::Like, now);
    graph.record_interaction(c_id, p3, SignalType::Like, now);
    for extra in 101..=103 {
        let p_extra = interner.intern(&format!(
            "at://did:plc:author200/app.bsky.feed.post/{extra}"
        ));
        graph.record_post_meta(p_extra, author_id, None, None, now);
        graph.record_interaction(c_id, p_extra, SignalType::Like, now);
    }

    // User D: 0 shared likes -> MUST BE EXCLUDED
    let user_d = "did:plc:user_d_zero";
    let d_id = interner.intern(user_d);
    let p_unrelated = interner.intern("at://did:plc:author200/app.bsky.feed.post/999");
    graph.record_post_meta(p_unrelated, author_id, None, None, now);
    graph.record_interaction(d_id, p_unrelated, SignalType::Like, now);

    // Execute find_taste_twins
    let resp = rec.find_taste_twins(viewer, 10).unwrap();

    // Verification 1: Exactly 2 twins returned (Users C and B). Users A (S=1) and D (S=0) are excluded.
    assert_eq!(
        resp.twins.len(),
        2,
        "Expected exactly 2 twins, but found {}: {:?}",
        resp.twins.len(),
        resp.twins.iter().map(|t| &t.user_did).collect::<Vec<_>>()
    );

    // Verification 2: User A is NOT in twins
    assert!(
        !resp.twins.iter().any(|t| t.user_did == user_a),
        "User A (single overlap S=1) must be strictly excluded from taste twins!"
    );

    // Verification 3: User D is NOT in twins
    assert!(
        !resp.twins.iter().any(|t| t.user_did == user_d),
        "User D (zero overlap S=0) must be strictly excluded from taste twins!"
    );

    // Verification 4: User C ranks #1 (confidence ≈ 0.1936), User B ranks #2 (confidence ≈ 0.1789)
    assert_eq!(resp.twins[0].user_did, user_c);
    assert_eq!(resp.twins[0].shared_posts_count, 3);
    let expected_c_conf = (3.0f32 / (60.0f32).sqrt()) * 0.50f32;
    assert!(
        (resp.twins[0].similarity_score - expected_c_conf).abs() < 1e-4,
        "User C confidence mismatch: expected {expected_c_conf}, got {}",
        resp.twins[0].similarity_score
    );

    assert_eq!(resp.twins[1].user_did, user_b);
    assert_eq!(resp.twins[1].shared_posts_count, 2);
    let expected_b_conf = (2.0f32 / (20.0f32).sqrt()) * 0.40f32;
    assert!(
        (resp.twins[1].similarity_score - expected_b_conf).abs() < 1e-4,
        "User B confidence mismatch: expected {expected_b_conf}, got {}",
        resp.twins[1].similarity_score
    );
}

// ===========================================================================
// Section 5: Proptest Property-Based Invariant Fuzzing
// ===========================================================================

proptest! {
    #![proptest_config(ProptestConfig::with_cases(500))]

    #[test]
    fn prop_bayesian_shrinkage_bounded(
        s in 0usize..1_000_000,
        beta in 0.01f32..100.0f32,
    ) {
        let shrinkage = calculate_bayesian_shrinkage(s, beta);
        prop_assert!(shrinkage >= 0.0, "Shrinkage must be >= 0.0, got {shrinkage}");
        prop_assert!(shrinkage < 1.0 || s == 0, "Shrinkage must be < 1.0, got {shrinkage}");

        if s == 0 {
            prop_assert_eq!(shrinkage, 0.0);
        } else {
            prop_assert!(shrinkage > 0.0);
        }
    }

    #[test]
    fn prop_bayesian_shrinkage_monotonicity(
        s in 0usize..500_000,
        beta in 0.1f32..20.0f32,
    ) {
        let s1 = calculate_bayesian_shrinkage(s, beta);
        let s2 = calculate_bayesian_shrinkage(s + 1, beta);
        // Floating point f32 has finite precision, so for very large S / small beta, s2 >= s1
        prop_assert!(s2 >= s1, "Monotonicity violated: s1={s1} for S={s}, s2={s2} for S={}", s + 1);
        if s < 1000 {
            prop_assert!(s2 > s1, "Strict monotonicity violated for S < 1000: s1={s1}, s2={s2}");
        }
    }

    #[test]
    fn prop_bayesian_confidence_scale_invariant(
        raw_cosine in 0.0f32..1.0f32,
        s in 0usize..100_000,
        beta in 0.1f32..10.0f32,
    ) {
        let conf = calculate_bayesian_confidence(raw_cosine, s, beta);
        prop_assert!(conf >= 0.0, "Confidence must be non-negative: {conf}");
        prop_assert!(conf <= raw_cosine + 1e-6, "Confidence {conf} cannot exceed raw cosine {raw_cosine}");
        if s == 0 || raw_cosine == 0.0 {
            prop_assert_eq!(conf, 0.0);
        }
    }
}
