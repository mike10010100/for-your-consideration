#![forbid(unsafe_code)]

//! Integration test suite for Milestone 3: Cold-Start Topic Diversity, Creator Seeds,
//! and Balanced Round-Robin Candidate Interleaving.

use ahash::AHashSet;
use for_your_consideration::prelude::*;
use std::sync::Arc;

const BASE_NOW: u64 = BLUESKY_EPOCH_SECS + 100_000;

#[test]
fn test_creator_seed_domain_categorization_all_categories() {
    // 1. Art seeds
    assert_eq!(
        match_creator_seed("did:plc:art_seed"),
        Some(TopicCategory::Art)
    );
    assert_eq!(
        match_creator_seed("did:plc:artist_carol"),
        Some(TopicCategory::Art)
    );
    assert_eq!(
        match_creator_seed("art.bsky.social"),
        Some(TopicCategory::Art)
    );
    assert_eq!(
        match_creator_seed("photography.bsky.social"),
        Some(TopicCategory::Art)
    );
    assert_eq!(
        match_creator_seed("illustration.bsky.social"),
        Some(TopicCategory::Art)
    );

    // 2. Tech seeds
    assert_eq!(
        match_creator_seed("did:plc:tech_seed"),
        Some(TopicCategory::Tech)
    );
    assert_eq!(
        match_creator_seed("did:plc:developer_dan"),
        Some(TopicCategory::Tech)
    );
    assert_eq!(
        match_creator_seed("rustlang.bsky.social"),
        Some(TopicCategory::Tech)
    );
    assert_eq!(
        match_creator_seed("linux.bsky.social"),
        Some(TopicCategory::Tech)
    );
    assert_eq!(
        match_creator_seed("dev.bsky.social"),
        Some(TopicCategory::Tech)
    );

    // 3. Science seeds
    assert_eq!(
        match_creator_seed("did:plc:science_seed"),
        Some(TopicCategory::Science)
    );
    assert_eq!(
        match_creator_seed("did:plc:scientist_eve"),
        Some(TopicCategory::Science)
    );
    assert_eq!(
        match_creator_seed("nasa.bsky.social"),
        Some(TopicCategory::Science)
    );
    assert_eq!(
        match_creator_seed("physics.bsky.social"),
        Some(TopicCategory::Science)
    );
    assert_eq!(
        match_creator_seed("biology.bsky.social"),
        Some(TopicCategory::Science)
    );

    // 4. News seeds
    assert_eq!(
        match_creator_seed("did:plc:news_seed"),
        Some(TopicCategory::News)
    );
    assert_eq!(
        match_creator_seed("did:plc:journalist_bob"),
        Some(TopicCategory::News)
    );
    assert_eq!(
        match_creator_seed("news.bsky.social"),
        Some(TopicCategory::News)
    );
    assert_eq!(
        match_creator_seed("reuters.bsky.social"),
        Some(TopicCategory::News)
    );
    assert_eq!(
        match_creator_seed("press.bsky.social"),
        Some(TopicCategory::News)
    );

    // 5. Culture seeds
    assert_eq!(
        match_creator_seed("did:plc:culture_seed"),
        Some(TopicCategory::Culture)
    );
    assert_eq!(
        match_creator_seed("did:plc:writer_alice"),
        Some(TopicCategory::Culture)
    );
    assert_eq!(
        match_creator_seed("books.bsky.social"),
        Some(TopicCategory::Culture)
    );
    assert_eq!(
        match_creator_seed("cinema.bsky.social"),
        Some(TopicCategory::Culture)
    );
    assert_eq!(
        match_creator_seed("music.bsky.social"),
        Some(TopicCategory::Culture)
    );

    // 6. Unknown / Unclassified creator
    assert_eq!(match_creator_seed("did:plc:regular_user_999"), None);
}

#[test]
fn test_uri_hashtag_and_keyword_categorization() {
    // Art keywords & hashtags
    assert_eq!(
        match_uri_keywords("at://did:plc:user/app.bsky.feed.post/my_new_illustration_art"),
        Some(TopicCategory::Art)
    );
    assert_eq!(
        match_uri_keywords("at://did:plc:user/app.bsky.feed.post/photo_landscape_3k12"),
        Some(TopicCategory::Art)
    );

    // Tech keywords
    assert_eq!(
        match_uri_keywords("at://did:plc:user/app.bsky.feed.post/rust_async_tokio_tutorial"),
        Some(TopicCategory::Tech)
    );
    assert_eq!(
        match_uri_keywords("at://did:plc:user/app.bsky.feed.post/ai_machinelearning_breakthrough"),
        Some(TopicCategory::Tech)
    );

    // Science keywords
    assert_eq!(
        match_uri_keywords("at://did:plc:user/app.bsky.feed.post/astronomy_telescope_image"),
        Some(TopicCategory::Science)
    );
    assert_eq!(
        match_uri_keywords("at://did:plc:user/app.bsky.feed.post/quantum_physics_discovery"),
        Some(TopicCategory::Science)
    );

    // News keywords
    assert_eq!(
        match_uri_keywords("at://did:plc:user/app.bsky.feed.post/breakingnews_election_results"),
        Some(TopicCategory::News)
    );
    assert_eq!(
        match_uri_keywords("at://did:plc:user/app.bsky.feed.post/headline_global_economy"),
        Some(TopicCategory::News)
    );

    // Culture keywords
    assert_eq!(
        match_uri_keywords("at://did:plc:user/app.bsky.feed.post/booksky_history_literature"),
        Some(TopicCategory::Culture)
    );
    assert_eq!(
        match_uri_keywords("at://did:plc:user/app.bsky.feed.post/cinema_film_review_2026"),
        Some(TopicCategory::Culture)
    );

    // Unclassified slug
    assert_eq!(
        match_uri_keywords("at://did:plc:user/app.bsky.feed.post/3k12345abcde"),
        None
    );
}

#[test]
fn test_deterministic_topic_hash_fallback_invariants() {
    // 1. Strict Determinism
    let uri = "at://did:plc:unclassified_author/app.bsky.feed.post/3k98765";
    let expected = deterministic_topic_fallback(100, uri);
    for _ in 0..100 {
        assert_eq!(deterministic_topic_fallback(100, uri), expected);
    }

    // 2. Uniform Distribution over 5 categories
    let mut category_counts = [0usize; 5];
    let total_samples = 5_000;

    for pid in 1..=total_samples {
        let test_uri = format!("at://did:plc:generic_user_{pid}/app.bsky.feed.post/post_{pid}");
        let cat = deterministic_topic_fallback(pid, &test_uri);
        category_counts[cat.to_index()] += 1;
    }

    // Expected per category is 1,000 (20%). Ensure each is within [800, 1200] (16% to 24%).
    for (idx, &count) in category_counts.iter().enumerate() {
        let cat = TOPIC_CATEGORIES[idx];
        assert!(
            (800..=1200).contains(&count),
            "Category {} count {} out of expected range [800, 1200]",
            cat.as_str(),
            count
        );
    }
}

#[test]
fn test_cold_start_unauthenticated_feed_topic_diversity() {
    let interner = Arc::new(StringInterner::new());
    let graph = Arc::new(GraphStore::new());

    // Populate 5 creators from 5 different categories, each with 4 posts
    let categories_creators = [
        ("did:plc:art_creator", TopicCategory::Art, "art_drawing"),
        ("did:plc:tech_creator", TopicCategory::Tech, "rust_software"),
        (
            "did:plc:science_creator",
            TopicCategory::Science,
            "quantum_physics",
        ),
        ("did:plc:news_creator", TopicCategory::News, "breaking_news"),
        (
            "did:plc:culture_creator",
            TopicCategory::Culture,
            "books_cinema",
        ),
    ];

    let liker = interner.intern("did:plc:velocity_liker");

    for (author_did, _cat, slug) in &categories_creators {
        let aid = interner.intern(author_did);
        for post_idx in 1..=4 {
            let uri_str = format!("at://{author_did}/app.bsky.feed.post/{slug}_{post_idx}");
            let pid = interner.intern(&uri_str);

            graph.record_post_meta(pid, aid, None, None, BASE_NOW - 500);
            graph.record_interaction(liker, pid, SignalType::Like, BASE_NOW - 100);
        }
    }

    let rec = Recommender::new(interner, graph);
    let dials = RecommendationDials {
        limit: 30,
        explain: true,
        min_likes: 1,
        ..Default::default()
    };

    // Anonymous viewer request (viewer_did: None) -> Tier 3
    let rec_feed = rec.recommend(None, &dials, BASE_NOW).unwrap();
    assert_eq!(rec_feed.posts.len(), 10); // max 2 posts per author diversity filter (5 authors * 2 = 10)

    // Check that all 5 topic categories are represented in the feed
    let mut observed_topics = AHashSet::new();
    for post in &rec_feed.posts {
        if let Some(ref exp) = post.explain {
            for cat in &TOPIC_CATEGORIES {
                if exp.contains(&format!("topic={}", cat.as_str())) {
                    observed_topics.insert(*cat);
                }
            }
        }
    }

    assert_eq!(
        observed_topics.len(),
        5,
        "Cold-start feed must contain all 5 topic categories"
    );
}

#[test]
fn test_single_topic_viral_spike_injection_defense() {
    let interner = Arc::new(StringInterner::new());
    let graph = Arc::new(GraphStore::new());

    let liker = interner.intern("did:plc:viral_booster");

    // 1. Inject 100 hyper-viral Tech posts from different authors
    for i in 1..=100 {
        let author_did = format!("did:plc:tech_author_{i}");
        let aid = interner.intern(&author_did);
        let uri = format!("at://did:plc:tech_author_{i}/post/tech_viral_{i}");
        let pid = interner.intern(&uri);

        graph.record_post_meta(pid, aid, None, None, BASE_NOW - 200);
        // Boost interaction count
        for _ in 0..10 {
            graph.record_interaction(liker, pid, SignalType::Like, BASE_NOW - 50);
        }
    }

    // 2. Inject minority categories: 2 Art, 2 Science, 2 Culture posts
    let minority_topics = [
        ("did:plc:art_seed_minority", "drawing_art"),
        ("did:plc:science_seed_minority", "space_physics"),
        ("did:plc:culture_seed_minority", "books_novel"),
    ];

    for (author_did, keyword) in &minority_topics {
        let aid = interner.intern(author_did);
        for post_idx in 1..=2 {
            let uri = format!("at://{author_did}/post/{keyword}_{post_idx}");
            let pid = interner.intern(&uri);

            graph.record_post_meta(pid, aid, None, None, BASE_NOW - 200);
            graph.record_interaction(liker, pid, SignalType::Like, BASE_NOW - 50);
        }
    }

    let rec = Recommender::new(interner, graph);
    let dials = RecommendationDials {
        limit: 30,
        explain: true,
        min_likes: 1,
        ..Default::default()
    };

    let result = rec.recommend(None, &dials, BASE_NOW).unwrap();

    // Verify top 10 posts contain minority topics due to round-robin diversity interleaving
    let top_10 = &result.posts[0..10.min(result.posts.len())];

    let mut top_topics = AHashSet::new();
    for post in top_10 {
        if let Some(ref exp) = post.explain {
            for cat in &TOPIC_CATEGORIES {
                if exp.contains(&format!("topic={}", cat.as_str())) {
                    top_topics.insert(*cat);
                }
            }
        }
    }

    // Minority topics (Art, Science, Culture) MUST be present in the top 10 despite the 100 viral tech posts
    assert!(
        top_topics.contains(&TopicCategory::Art),
        "Art should be guaranteed early representation"
    );
    assert!(
        top_topics.contains(&TopicCategory::Science),
        "Science should be guaranteed early representation"
    );
    assert!(
        top_topics.contains(&TopicCategory::Culture),
        "Culture should be guaranteed early representation"
    );

    // In the top 10 posts, tech must not have more than ~40% (since 4 categories are active in round 0 and 1)
    let tech_count_in_top_10 = top_10
        .iter()
        .filter(|p| p.explain.as_ref().is_some_and(|e| e.contains("topic=tech")))
        .count();

    assert!(
        tech_count_in_top_10 <= 4,
        "Viral tech posts must not monopolize top slots (found {tech_count_in_top_10}/10)"
    );
}

#[test]
fn test_empty_and_sparse_topic_pools_graceful_backfill() {
    let interner = Arc::new(StringInterner::new());
    let graph = Arc::new(GraphStore::new());

    let rec = Recommender::new(Arc::clone(&interner), Arc::clone(&graph));
    let dials = RecommendationDials {
        min_likes: 1,
        ..Default::default()
    };

    // 1. Completely empty graph
    let empty_rec = rec.recommend(None, &dials, BASE_NOW).unwrap();
    assert!(empty_rec.posts.is_empty());
    assert_eq!(empty_rec.cursor, None);

    // 2. Only 1 category has posts (e.g. Science only)
    let sci_author = interner.intern("did:plc:science_seed");
    let liker = interner.intern("did:plc:liker");

    for i in 1..=3 {
        let uri = format!("at://did:plc:science_seed/post/physics_{i}");
        let pid = interner.intern(&uri);
        graph.record_post_meta(pid, sci_author, None, None, BASE_NOW - 100);
        graph.record_interaction(liker, pid, SignalType::Like, BASE_NOW - 50);
    }

    let single_cat_rec = rec.recommend(None, &dials, BASE_NOW).unwrap();
    // Author diversity restricts to 2 posts per author
    assert_eq!(single_cat_rec.posts.len(), 2);
    assert_eq!(
        single_cat_rec.posts[0].source,
        RecommendationSource::Tier3VelocityPool
    );
}

#[test]
fn test_topic_diversity_multi_page_pagination() {
    let interner = Arc::new(StringInterner::new());
    let graph = Arc::new(GraphStore::new());

    let liker = interner.intern("did:plc:liker");

    // Create 15 distinct authors across categories (3 per category)
    for cat in &TOPIC_CATEGORIES {
        for author_idx in 1..=3 {
            let author_did = format!("did:plc:{}_author_{author_idx}", cat.as_str());
            let aid = interner.intern(&author_did);
            let uri = format!("at://{author_did}/post/{}_{author_idx}", cat.as_str());
            let pid = interner.intern(&uri);

            graph.record_post_meta(pid, aid, None, None, BASE_NOW - 100);
            graph.record_interaction(liker, pid, SignalType::Like, BASE_NOW - 50);
        }
    }

    let rec = Recommender::new(interner, graph);

    // Page 1: limit = 5
    let dials1 = RecommendationDials {
        limit: 5,
        min_likes: 1,
        ..Default::default()
    };
    let page1 = rec.recommend(None, &dials1, BASE_NOW).unwrap();
    assert_eq!(page1.posts.len(), 5);
    assert_eq!(page1.cursor.as_deref(), Some("5"));

    // Page 2: limit = 5 with cursor from Page 1
    let dials2 = RecommendationDials {
        limit: 5,
        cursor: page1.cursor,
        min_likes: 1,
        ..Default::default()
    };
    let page2 = rec.recommend(None, &dials2, BASE_NOW).unwrap();
    assert_eq!(page2.posts.len(), 5);
    assert_eq!(page2.cursor.as_deref(), Some("10"));

    // Verify non-overlapping post IDs across pages
    let page1_ids: AHashSet<u32> = page1.posts.iter().map(|p| p.post_id).collect();
    for p in &page2.posts {
        assert!(
            !page1_ids.contains(&p.post_id),
            "Page 2 post {} must not overlap with Page 1",
            p.post_id
        );
    }
}

#[tokio::test]
async fn test_high_concurrency_tier3_diversity_queries() {
    let interner = Arc::new(StringInterner::new());
    let graph = Arc::new(GraphStore::new());

    let liker = interner.intern("did:plc:concurrency_liker");

    for cat in &TOPIC_CATEGORIES {
        for author_idx in 1..=5 {
            let author_did = format!("did:plc:{}_concur_{author_idx}", cat.as_str());
            let aid = interner.intern(&author_did);
            let uri = format!("at://{author_did}/post/{}_{author_idx}", cat.as_str());
            let pid = interner.intern(&uri);

            graph.record_post_meta(pid, aid, None, None, BASE_NOW - 200);
            graph.record_interaction(liker, pid, SignalType::Like, BASE_NOW - 100);
        }
    }

    let rec = Arc::new(Recommender::new(interner, graph));

    let mut handles = Vec::new();
    for _ in 0..50 {
        let rec_clone = Arc::clone(&rec);
        let handle = tokio::spawn(async move {
            let dials = RecommendationDials {
                limit: 15,
                explain: true,
                min_likes: 1,
                ..Default::default()
            };
            let feed = rec_clone.recommend(None, &dials, BASE_NOW).unwrap();
            assert!(!feed.posts.is_empty());
            feed.posts.len()
        });
        handles.push(handle);
    }

    for h in handles {
        let post_count = h.await.unwrap();
        assert!(post_count > 0);
    }
}
