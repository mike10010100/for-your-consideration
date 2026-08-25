#![forbid(unsafe_code)]

//! Comprehensive test suite for Recommender APIs:
//! - Taste Twins Discovery (`find_taste_twins`)
//! - Live Algorithmic Dials & Read-Only Feed Preview (`recommend_preview`)
//! - 3-Step Graph Proof Chain Explainer (`explain_recommendation`)
//! - Score Breakdown Mathematical Verification
//! - Topic Weights Modulation
//! - Impression LRU Read-Only Safety

use std::sync::Arc;

use for_your_consideration::prelude::*;

fn setup_graph_and_interner() -> (Arc<StringInterner>, Arc<GraphStore>, Recommender) {
    let interner = Arc::new(StringInterner::new());
    let graph = Arc::new(GraphStore::new());
    let rec = Recommender::new(Arc::clone(&interner), Arc::clone(&graph));
    (interner, graph, rec)
}

#[test]
fn test_find_taste_twins_identical_user_exclusion() {
    let (interner, graph, rec) = setup_graph_and_interner();
    let now = BLUESKY_EPOCH_SECS + 100_000;

    let viewer = interner.intern("did:plc:viewer");
    let twin1 = interner.intern("did:plc:twin1");
    let author = interner.intern("did:plc:author");

    let p1 = interner.intern("at://did:plc:author/app.bsky.feed.post/1");
    let p2 = interner.intern("at://did:plc:author/app.bsky.feed.post/2");

    graph.record_post_meta(p1, author, None, None, now - 500);
    graph.record_post_meta(p2, author, None, None, now - 500);

    graph.record_interaction(viewer, p1, SignalType::Like, now - 100);
    graph.record_interaction(viewer, p2, SignalType::Like, now - 100);

    graph.record_interaction(twin1, p1, SignalType::Like, now - 90);
    graph.record_interaction(twin1, p2, SignalType::Like, now - 90);

    let res = rec.find_taste_twins("did:plc:viewer", 10).unwrap();
    assert_eq!(res.viewer_did, "did:plc:viewer");
    assert_eq!(res.total_liked_posts, 2);
    assert_eq!(res.twins.len(), 1);
    assert_eq!(res.twins[0].user_did, "did:plc:twin1");

    // Ensure viewer is never returned as their own twin
    assert!(res.twins.iter().all(|t| t.user_did != "did:plc:viewer"));
}

#[test]
fn test_find_taste_twins_cosine_accuracy() {
    let (interner, graph, rec) = setup_graph_and_interner();
    let now = BLUESKY_EPOCH_SECS + 100_000;

    let viewer = interner.intern("did:plc:viewer");
    let user_b = interner.intern("did:plc:user_b");
    let user_c = interner.intern("did:plc:user_c");
    let author = interner.intern("did:plc:author");

    // Viewer likes posts 1, 2, 3, 4 (|Viewer| = 4)
    let p1 = interner.intern("at://did:plc:author/app.bsky.feed.post/1");
    let p2 = interner.intern("at://did:plc:author/app.bsky.feed.post/2");
    let p3 = interner.intern("at://did:plc:author/app.bsky.feed.post/3");
    let p4 = interner.intern("at://did:plc:author/app.bsky.feed.post/4");
    let p5 = interner.intern("at://did:plc:author/app.bsky.feed.post/5");
    let p6 = interner.intern("at://did:plc:author/app.bsky.feed.post/6");

    for &p in &[p1, p2, p3, p4, p5, p6] {
        graph.record_post_meta(p, author, None, None, now - 1000);
    }

    graph.record_interaction(viewer, p1, SignalType::Like, now - 500);
    graph.record_interaction(viewer, p2, SignalType::Like, now - 500);
    graph.record_interaction(viewer, p3, SignalType::Like, now - 500);
    graph.record_interaction(viewer, p4, SignalType::Like, now - 500);

    // User B likes posts 2, 3, 4, 5, 6 (|B| = 5, overlap = {2, 3, 4} -> |overlap| = 3)
    // Expected Cosine = 3.0 / sqrt(4 * 5) = 3.0 / sqrt(20) ≈ 0.6708204
    graph.record_interaction(user_b, p2, SignalType::Like, now - 400);
    graph.record_interaction(user_b, p3, SignalType::Like, now - 400);
    graph.record_interaction(user_b, p4, SignalType::Like, now - 400);
    graph.record_interaction(user_b, p5, SignalType::Like, now - 400);
    graph.record_interaction(user_b, p6, SignalType::Like, now - 400);

    // User C likes posts 1, 2 (|C| = 2, overlap = {1, 2} -> |overlap| = 2)
    // Expected Cosine = 2.0 / sqrt(4 * 2) = 2.0 / sqrt(8) ≈ 0.7071068
    graph.record_interaction(user_c, p1, SignalType::Like, now - 300);
    graph.record_interaction(user_c, p2, SignalType::Like, now - 300);

    let res = rec.find_taste_twins("did:plc:viewer", 10).unwrap();
    assert_eq!(res.twins.len(), 2);

    // User C has higher cosine similarity (0.7071) than User B (0.6708)
    assert_eq!(res.twins[0].user_did, "did:plc:user_c");
    let expected_c = 2.0 / (4.0 * 2.0f32).sqrt();
    assert!((res.twins[0].similarity_score - expected_c).abs() < 1e-4);
    assert_eq!(res.twins[0].shared_posts_count, 2);

    assert_eq!(res.twins[1].user_did, "did:plc:user_b");
    let expected_b = 3.0 / (4.0 * 5.0f32).sqrt();
    assert!((res.twins[1].similarity_score - expected_b).abs() < 1e-4);
    assert_eq!(res.twins[1].shared_posts_count, 3);
}

#[test]
fn test_find_taste_twins_unknown_did_graceful_empty() {
    let (_interner, _graph, rec) = setup_graph_and_interner();

    let res = rec
        .find_taste_twins("did:plc:nonexistent_user", 10)
        .unwrap();
    assert_eq!(res.viewer_did, "did:plc:nonexistent_user");
    assert_eq!(res.total_liked_posts, 0);
    assert!(res.twins.is_empty());
}

#[test]
fn test_find_taste_twins_at_prefix_stripping() {
    let (interner, graph, rec) = setup_graph_and_interner();
    let now = BLUESKY_EPOCH_SECS + 100_000;

    let viewer = interner.intern("alice.bsky.social");
    let twin = interner.intern("bob.bsky.social");
    let author = interner.intern("charlie.bsky.social");

    let p1 = interner.intern("at://charlie.bsky.social/app.bsky.feed.post/1");
    graph.record_post_meta(p1, author, None, None, now - 100);
    graph.record_interaction(viewer, p1, SignalType::Like, now - 50);
    graph.record_interaction(twin, p1, SignalType::Like, now - 40);

    let res_without_at = rec.find_taste_twins("alice.bsky.social", 10).unwrap();
    let res_with_at = rec.find_taste_twins("@alice.bsky.social", 10).unwrap();

    assert_eq!(res_without_at.twins.len(), 1);
    assert_eq!(res_with_at.twins.len(), 1);
    assert_eq!(
        res_without_at.twins[0].user_did,
        res_with_at.twins[0].user_did
    );
}

#[test]
fn test_find_taste_twins_shared_posts_and_top_interests() {
    let (interner, graph, rec) = setup_graph_and_interner();
    let now = BLUESKY_EPOCH_SECS + 100_000;

    let viewer = interner.intern("did:plc:viewer");
    let twin = interner.intern("did:plc:twin");
    let tech_author = interner.intern("did:plc:tech_seed");

    let tech_p1 = interner.intern("at://did:plc:tech_seed/app.bsky.feed.post/rust_systems");
    let tech_p2 = interner.intern("at://did:plc:tech_seed/app.bsky.feed.post/ai_compilers");

    graph.record_post_meta(tech_p1, tech_author, None, None, now - 500);
    graph.record_post_meta(tech_p2, tech_author, None, None, now - 500);

    graph.record_interaction(viewer, tech_p1, SignalType::Like, now - 200);
    graph.record_interaction(viewer, tech_p2, SignalType::Like, now - 200);

    graph.record_interaction(twin, tech_p1, SignalType::Like, now - 100);
    graph.record_interaction(twin, tech_p2, SignalType::Like, now - 100);

    let res = rec.find_taste_twins("did:plc:viewer", 10).unwrap();
    assert_eq!(res.twins.len(), 1);
    let item = &res.twins[0];

    assert_eq!(item.shared_posts.len(), 2);
    assert_eq!(item.shared_posts[0].category, TopicCategory::Tech);
    assert_eq!(item.shared_posts[0].author_did, "did:plc:tech_seed");
    assert!(item.top_interests.contains(&TopicCategory::Tech));
}

#[test]
fn test_recommend_preview_score_breakdown() {
    let (interner, graph, rec) = setup_graph_and_interner();
    let now = BLUESKY_EPOCH_SECS + 100_000;

    let viewer = interner.intern("did:plc:viewer");
    let co_user = interner.intern("did:plc:co_user");
    let author = interner.intern("did:plc:tech_seed");

    let seed_post = interner.intern("at://did:plc:tech_seed/app.bsky.feed.post/seed");
    let cand_post = interner.intern("at://did:plc:tech_seed/app.bsky.feed.post/cand");

    graph.record_post_meta(seed_post, author, None, None, now - 500);
    graph.record_post_meta(cand_post, author, None, None, now - 300);

    // Populate active likes to enable Tier 1
    for i in 1..=10 {
        let p = interner.intern(&format!(
            "at://did:plc:tech_seed/app.bsky.feed.post/dummy_{i}"
        ));
        graph.record_post_meta(p, author, None, None, now - 600);
        graph.record_interaction(viewer, p, SignalType::Like, now - 400);
    }

    graph.record_interaction(viewer, seed_post, SignalType::Like, now - 200);
    graph.record_interaction(co_user, seed_post, SignalType::Like, now - 180);
    graph.record_interaction(co_user, cand_post, SignalType::Repost, now - 150);

    let dials = RecommendationDials {
        explain: true,
        ..Default::default()
    };

    let preview = rec
        .recommend_preview(Some("did:plc:viewer"), &dials)
        .unwrap();
    assert_eq!(preview.viewer_did, "did:plc:viewer");
    assert!(!preview.items.is_empty());

    let item = preview
        .items
        .iter()
        .find(|i| i.uri == "at://did:plc:tech_seed/app.bsky.feed.post/cand")
        .expect("Candidate post must be present in preview");

    assert_eq!(item.topic, TopicCategory::Tech);
    assert!(item.tier.contains("Tier 1"));
    assert!(item.score_breakdown.time_decay > 0.0);
    assert!(item.score_breakdown.time_decay <= 3.0);
    assert!(item.score_breakdown.taste_similarity > 0.0);
    assert_eq!(item.score_breakdown.topic_boost, 1.0);
    assert_eq!(item.score_breakdown.fatigue_penalty, 1.0);
    assert!(item.score_breakdown.final_score > 0.0);

    // Verify proof chain is populated when explain = true
    assert!(item.proof_chain.is_some());
    let chain = item.proof_chain.as_ref().unwrap();
    assert_eq!(chain.steps.len(), 3);
    assert!(chain.summary.contains("Recommended because"));
}

#[test]
fn test_recommend_preview_read_only_impression_safety() {
    let (interner, graph, rec) = setup_graph_and_interner();
    let now = BLUESKY_EPOCH_SECS + 100_000;

    let viewer = interner.intern("did:plc:viewer");
    let followed = interner.intern("did:plc:followed");
    let author = interner.intern("did:plc:author");

    let p1 = interner.intern("at://did:plc:author/app.bsky.feed.post/p1");
    let p2 = interner.intern("at://did:plc:author/app.bsky.feed.post/p2");

    graph.record_post_meta(p1, author, None, None, now - 200);
    graph.record_post_meta(p2, author, None, None, now - 200);
    graph.record_follow(viewer, followed);
    graph.record_interaction(followed, p1, SignalType::Like, now - 100);
    graph.record_interaction(followed, p2, SignalType::Like, now - 100);

    let dials = RecommendationDials::default();

    // Call recommend_preview 10 times in rapid succession
    for _ in 0..10 {
        let prev = rec
            .recommend_preview(Some("did:plc:viewer"), &dials)
            .unwrap();
        assert_eq!(prev.items.len(), 2);
    }

    // Verify ImpressionStore recorded 0 impressions for viewer
    assert_eq!(
        rec.impression_store().get_viewer_impression_count(viewer),
        0
    );

    // Calling regular recommend() should still serve both posts without any hard suppression
    let regular = rec.recommend(Some("did:plc:viewer"), &dials, now).unwrap();
    assert_eq!(regular.posts.len(), 2);
}

#[test]
fn test_recommend_preview_topic_weights_modulation() {
    let (interner, graph, rec) = setup_graph_and_interner();
    let now = BLUESKY_EPOCH_SECS + 100_000;

    let art_author = interner.intern("did:plc:art_seed");
    let tech_author = interner.intern("did:plc:tech_seed");

    let art_post = interner.intern("at://did:plc:art_seed/app.bsky.feed.post/painting");
    let tech_post = interner.intern("at://did:plc:tech_seed/app.bsky.feed.post/coding");

    graph.record_post_meta(art_post, art_author, None, None, now - 100);
    graph.record_post_meta(tech_post, tech_author, None, None, now - 100);

    let liker = interner.intern("did:plc:liker");
    graph.record_interaction(liker, art_post, SignalType::Like, now - 50);
    graph.record_interaction(liker, tech_post, SignalType::Like, now - 50);

    // Boost art to 4.0 and reduce tech to 0.1
    let dials = RecommendationDials {
        topic_weights: TopicWeights {
            art: 4.0,
            tech: 0.1,
            science: 1.0,
            news: 1.0,
            culture: 1.0,
        },
        ..Default::default()
    };

    let preview = rec.recommend_preview(None, &dials).unwrap();
    assert_eq!(preview.items.len(), 2);

    // Art post should rank 1st with boosted score
    assert_eq!(preview.items[0].topic, TopicCategory::Art);
    assert_eq!(preview.items[0].score_breakdown.topic_boost, 4.0);

    assert_eq!(preview.items[1].topic, TopicCategory::Tech);
    assert_eq!(preview.items[1].score_breakdown.topic_boost, 0.1);
}

#[test]
fn test_explain_recommendation_tier1_3step_proof_chain() {
    let (interner, graph, rec) = setup_graph_and_interner();
    let now = BLUESKY_EPOCH_SECS + 100_000;

    let viewer = interner.intern("did:plc:viewer");
    let twin = interner.intern("did:plc:twin");
    let author = interner.intern("did:plc:tech_seed");

    let seed_post = interner.intern("at://did:plc:tech_seed/app.bsky.feed.post/seed_post");
    let rec_post = interner.intern("at://did:plc:tech_seed/app.bsky.feed.post/recommended_post");

    graph.record_post_meta(seed_post, author, None, None, now - 500);
    graph.record_post_meta(rec_post, author, None, None, now - 300);

    graph.record_interaction(viewer, seed_post, SignalType::Like, now - 200);
    graph.record_interaction(twin, seed_post, SignalType::Like, now - 150);
    graph.record_interaction(twin, rec_post, SignalType::Repost, now - 100);

    let chain = rec
        .explain_recommendation(
            "did:plc:viewer",
            "at://did:plc:tech_seed/app.bsky.feed.post/recommended_post",
        )
        .unwrap();

    assert_eq!(chain.steps.len(), 3);

    // Step 1: Viewer -> Seed Post
    assert_eq!(chain.steps[0].step_type, "viewer_interaction");
    assert_eq!(
        chain.steps[0].node_id,
        "at://did:plc:tech_seed/app.bsky.feed.post/seed_post"
    );

    // Step 2: Seed Post -> Taste Twin
    assert_eq!(chain.steps[1].step_type, "taste_similarity");
    assert_eq!(chain.steps[1].node_id, "did:plc:twin");

    // Step 3: Taste Twin -> Recommended Post
    assert_eq!(chain.steps[2].step_type, "recommendation_signal");
    assert_eq!(
        chain.steps[2].node_id,
        "at://did:plc:tech_seed/app.bsky.feed.post/recommended_post"
    );

    assert!(chain
        .summary
        .contains("Recommended because you liked an earlier post"));
    assert!(chain.summary.contains("did:plc:twin"));
}

#[test]
fn test_explain_recommendation_tier2_follow_proof_chain() {
    let (interner, graph, rec) = setup_graph_and_interner();
    let now = BLUESKY_EPOCH_SECS + 100_000;

    let viewer = interner.intern("did:plc:viewer");
    let followed = interner.intern("did:plc:followed_friend");
    let author = interner.intern("did:plc:science_seed");

    let post = interner.intern("at://did:plc:science_seed/app.bsky.feed.post/quantum_paper");
    graph.record_post_meta(post, author, None, None, now - 200);
    graph.record_follow(viewer, followed);
    graph.record_interaction(followed, post, SignalType::Like, now - 50);

    let chain = rec
        .explain_recommendation(
            "did:plc:viewer",
            "at://did:plc:science_seed/app.bsky.feed.post/quantum_paper",
        )
        .unwrap();

    assert_eq!(chain.steps.len(), 3);
    assert_eq!(chain.steps[0].step_type, "follow_graph");
    assert_eq!(chain.steps[0].node_id, "did:plc:followed_friend");
    assert_eq!(chain.steps[1].step_type, "followed_interaction");
    assert_eq!(chain.steps[2].step_type, "follow_affinity_boost");
    assert!(chain.summary.contains("did:plc:followed_friend"));
}

#[test]
fn test_explain_recommendation_tier3_velocity_proof_chain() {
    let (interner, graph, rec) = setup_graph_and_interner();
    let now = BLUESKY_EPOCH_SECS + 100_000;

    let author = interner.intern("did:plc:news_seed");
    let post = interner.intern("at://did:plc:news_seed/app.bsky.feed.post/breaking_event");
    graph.record_post_meta(post, author, None, None, now - 100);

    let chain = rec
        .explain_recommendation(
            "did:plc:unauthenticated_viewer",
            "at://did:plc:news_seed/app.bsky.feed.post/breaking_event",
        )
        .unwrap();

    assert_eq!(chain.steps.len(), 3);
    assert_eq!(chain.steps[0].step_type, "cold_start_onboarding");
    assert_eq!(chain.steps[1].step_type, "velocity_trending");
    assert_eq!(chain.steps[2].step_type, "topic_diversity_interleaving");
    assert!(chain.summary.contains("trending high-velocity post"));
}

#[test]
fn test_sub_2ms_latency_taste_twins_and_preview() {
    let (interner, graph, rec) = setup_graph_and_interner();
    let now = BLUESKY_EPOCH_SECS + 100_000;

    let viewer = interner.intern("did:plc:active_viewer");
    let author = interner.intern("did:plc:author");

    // Populate 50 liked posts for viewer
    for i in 0..50 {
        let p = interner.intern(&format!(
            "at://did:plc:author/app.bsky.feed.post/viewer_post_{i}"
        ));
        graph.record_post_meta(p, author, None, None, now - 1000);
        graph.record_interaction(viewer, p, SignalType::Like, now - 500);
    }

    // Populate 500 co-interactor users with overlapping interactions
    for u in 0..500 {
        let user = interner.intern(&format!("did:plc:user_{u}"));
        let cand_author = interner.intern(&format!("did:plc:author_{u}"));
        let shared_p = interner.intern(&format!(
            "at://did:plc:author/app.bsky.feed.post/viewer_post_{}",
            u % 50
        ));
        graph.record_interaction(user, shared_p, SignalType::Like, now - 400);

        let cand_p = interner.intern(&format!(
            "at://did:plc:author_{u}/app.bsky.feed.post/cand_{u}"
        ));
        graph.record_post_meta(cand_p, cand_author, None, None, now - 300);
        graph.record_interaction(user, cand_p, SignalType::Like, now - 200);
    }

    // Measure find_taste_twins latency
    let twins_resp = rec.find_taste_twins("did:plc:active_viewer", 20).unwrap();
    assert!(!twins_resp.twins.is_empty());
    #[cfg(not(debug_assertions))]
    assert!(
        twins_resp.query_latency_us < 2_000,
        "Query latency SLA violation in release: {}us",
        twins_resp.query_latency_us
    );
    #[cfg(debug_assertions)]
    assert!(
        twins_resp.query_latency_us < 100_000,
        "Query latency abnormal debug spike: {}us",
        twins_resp.query_latency_us
    );

    // Measure recommend_preview latency
    let dials = RecommendationDials {
        explain: true,
        limit: 30,
        ..Default::default()
    };
    let preview_resp = rec
        .recommend_preview(Some("did:plc:active_viewer"), &dials)
        .unwrap();
    assert_eq!(preview_resp.items.len(), 30);
    #[cfg(not(debug_assertions))]
    assert!(
        preview_resp.query_latency_us < 2_000,
        "Preview query latency SLA violation in release: {}us",
        preview_resp.query_latency_us
    );
    #[cfg(debug_assertions)]
    assert!(
        preview_resp.query_latency_us < 100_000,
        "Preview query latency abnormal debug spike: {}us",
        preview_resp.query_latency_us
    );
}
