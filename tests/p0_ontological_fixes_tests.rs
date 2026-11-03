#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::panic,
    clippy::cast_precision_loss,
    missing_docs
)]

//! End-to-end behavioral and adversarial verification for Track 1 P0 Ontological Fixes:
//! - P0-1: Real-world 2-byte varint `[0xe7, 0x01]` (`zQ3sh...`) secp256k1 & `[0x80, 0x24]` (`zDna...`)
//!   P-256 `Multikey` decoding, curve-to-JOSE-alg binding (`ES256K` vs `ES256`), and bounded `DidKeyCache`
//!   over a live local HTTP PLC directory server in `ServiceAuthMode::Enforce`.
//! - P0-2: `compact_memory_stores_with_impressions` remapping `ImpressionStore` user/post IDs without
//!   cross-user or cross-post corruption, preserving `SignalType::Quote` and `SignalType::Repost` edges in
//!   `user_likes_bitmaps`, and remapping `RecentActiveRingBuffer` without 1-item velocity pool blindness.
//! - P0-3: Ingest-time content topic classification (`text`, `tags`, `facets`, `embed.external`) stored in
//!   `GraphStore::post_topics`, persisted in Section 6 of `snapshot.bin`, remapped on compaction, and used
//!   by `Recommender` when scoring and explaining opaque TID post URIs.
//! - P0-4: Synthesizing `PostMeta` from AT-URIs (`at://<author_did>/app.bsky.feed.post/<tid>`) on
//!   interaction-only posts (preventing silent drops in Tier 1/Tier 2 and eliminating `author_id = 0`
//!   sentinel collision with interned ID 0), and removing deleted posts on `app.bsky.feed.post` delete events.

use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use axum::{extract::Path, routing::get, Json, Router};
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine;
use compact_str::CompactString;
use for_your_consideration::prelude::*;
use k256::ecdsa::signature::Signer as _;

fn now_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap()
        .as_secs()
}

fn bs58_encode(input: &[u8]) -> String {
    const ALPHABET: &[u8; 58] = b"123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz";
    let zeros = input.iter().take_while(|&&b| b == 0).count();
    let mut digits: Vec<u8> = Vec::new();
    for &byte in &input[zeros..] {
        let mut carry = u32::from(byte);
        for d in &mut digits {
            let val = u32::from(*d) * 256 + carry;
            *d = (val % 58) as u8;
            carry = val / 58;
        }
        while carry > 0 {
            digits.push((carry % 58) as u8);
            carry /= 58;
        }
    }
    let mut out = "1".repeat(zeros);
    for &d in digits.iter().rev() {
        out.push(ALPHABET[usize::from(d)] as char);
    }
    out
}

fn encode_jwt_segment(json: &serde_json::Value) -> String {
    URL_SAFE_NO_PAD.encode(json.to_string().as_bytes())
}

// ============================================================================
// Scenario 1 (P0-1): Live HTTP PLC Directory + Real XRPC Server in Enforce Mode
// ============================================================================

#[tokio::test]
async fn test_p0_1_enforce_mode_real_plc_2byte_varint_secp256k1_and_p256_over_http() {
    let k256_signing = k256::ecdsa::SigningKey::from_bytes(&[0x21u8; 32].into()).unwrap();
    let k256_compressed = k256_signing.verifying_key().to_sec1_point(true);
    let k256_multikey_bytes = [
        MULTIKEY_SECP256K1_VARINT.to_vec(),
        k256_compressed.as_bytes().to_vec(),
    ]
    .concat();
    assert_eq!(k256_multikey_bytes.len(), 35);
    let k256_multibase = format!("z{}", bs58_encode(&k256_multikey_bytes));
    assert!(k256_multibase.starts_with("zQ3sh"));

    let p256_signing = p256::ecdsa::SigningKey::from_bytes(&[0x33u8; 32].into()).unwrap();
    let p256_compressed = p256_signing.verifying_key().to_sec1_point(true);
    let p256_multikey_bytes = [
        MULTIKEY_P256_VARINT.to_vec(),
        p256_compressed.as_bytes().to_vec(),
    ]
    .concat();
    assert_eq!(p256_multikey_bytes.len(), 35);
    let p256_multibase = format!("z{}", bs58_encode(&p256_multikey_bytes));
    assert!(p256_multibase.starts_with("zDna"));

    let k256_did = "did:plc:real2bytesecp256k1user00";
    let p256_did = "did:plc:real2bytep256user0000000";

    let k256_mb_clone = k256_multibase.clone();
    let p256_mb_clone = p256_multibase.clone();

    // Spin up a real local HTTP server acting as the PLC directory
    let plc_router = Router::new().route(
        "/{did}",
        get(move |Path(did): Path<String>| {
            let k_mb = k256_mb_clone.clone();
            let p_mb = p256_mb_clone.clone();
            async move {
                let mb = if did == k256_did {
                    k_mb
                } else if did == p256_did {
                    p_mb
                } else {
                    return (
                        axum::http::StatusCode::NOT_FOUND,
                        Json(serde_json::json!({"message": "DID not found"})),
                    );
                };
                (
                    axum::http::StatusCode::OK,
                    Json(serde_json::json!({
                        "@context": [
                            "https://www.w3.org/ns/did/v1",
                            "https://w3id.org/security/multikey/v1"
                        ],
                        "id": did,
                        "verificationMethod": [{
                            "id": format!("{did}#atproto"),
                            "type": "Multikey",
                            "controller": did,
                            "publicKeyMultibase": mb
                        }],
                        "service": []
                    })),
                )
            }
        }),
    );

    let plc_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let plc_addr = plc_listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(plc_listener, plc_router).await.unwrap();
    });

    // Build a ServiceAuthVerifier pointed at the local mock PLC directory HTTP server
    let verifier = ServiceAuthVerifier::with_plc_url(format!("http://{plc_addr}"), true);

    let interner = Arc::new(StringInterner::new());
    let graph = Arc::new(GraphStore::new());
    let now = now_secs();

    // Seed a high-velocity post so getFeedSkeleton returns a non-empty feed
    let post_uri = "at://did:plc:author1/app.bsky.feed.post/3k2yihcrp6f2c";
    let pid = interner.intern(post_uri);
    let aid = interner.intern("did:plc:author1");
    graph.record_post_meta(pid, aid, None, None, now - 60);
    for i in 0..5 {
        let uid = interner.intern(&format!("did:plc:liker_{i}"));
        graph.record_interaction(uid, pid, SignalType::Like, now - 30);
    }

    let recommender = Arc::new(Recommender::new(Arc::clone(&interner), Arc::clone(&graph)));
    let mut state = AppState::new(recommender, "did:web:feed.example.com", "feed.example.com")
        .with_feed_uri("at://did:plc:publisher/app.bsky.feed.generator/for-your-consideration")
        .with_service_auth_mode(ServiceAuthMode::Enforce);
    state.service_auth = verifier;

    let app = create_xrpc_router(state.clone());
    let app_listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let app_addr = app_listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(app_listener, app).await.unwrap();
    });

    let client = reqwest::Client::new();
    let feed_url = format!(
        "http://{app_addr}/xrpc/app.bsky.feed.getFeedSkeleton?feed=at://did:plc:publisher/app.bsky.feed.generator/for-your-consideration"
    );

    // 1. Verify ES256K JWT signed by 35-byte [0xe7, 0x01] zQ3sh... DID document succeeds (200 OK)
    //    and authenticates the viewer (recording viewer impressions).
    let k256_header = encode_jwt_segment(&serde_json::json!({"alg": "ES256K", "typ": "JWT"}));
    let k256_payload = encode_jwt_segment(&serde_json::json!({
        "iss": k256_did,
        "sub": k256_did,
        "aud": "did:web:feed.example.com",
        "exp": now + 3600,
        "iat": now
    }));
    let k256_input = format!("{k256_header}.{k256_payload}");
    let k256_sig: k256::ecdsa::Signature = k256_signing.sign(k256_input.as_bytes());
    let k256_jwt = format!(
        "{k256_input}.{}",
        URL_SAFE_NO_PAD.encode(k256_sig.to_bytes().as_slice())
    );

    let resp_k256 = client
        .get(&feed_url)
        .header("Authorization", format!("Bearer {k256_jwt}"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp_k256.status(), reqwest::StatusCode::OK);
    let body_k256: FeedSkeletonResponse = resp_k256.json().await.unwrap();
    assert_eq!(body_k256.feed.len(), 1);
    assert_eq!(body_k256.feed[0].post, post_uri);
    let k256_uid = state
        .recommender
        .interner
        .lookup_id(k256_did)
        .expect("Verified ES256K 2-byte varint DID must be interned as authenticated viewer");
    assert!(
        state
            .recommender
            .impression_store()
            .get_viewer_impression_count(k256_uid)
            > 0,
        "Verified ES256K viewer must have recorded impressions"
    );

    // 2. Verify ES256 JWT signed by 35-byte [0x80, 0x24] zDna... P-256 DID document succeeds (200 OK)
    //    and authenticates the P-256 viewer!
    let p256_header = encode_jwt_segment(&serde_json::json!({"alg": "ES256", "typ": "JWT"}));
    let p256_payload = encode_jwt_segment(&serde_json::json!({
        "iss": p256_did,
        "sub": p256_did,
        "aud": "did:web:feed.example.com",
        "exp": now + 3600,
        "iat": now
    }));
    let p256_input = format!("{p256_header}.{p256_payload}");
    let p256_sig: p256::ecdsa::Signature = p256_signing.sign(p256_input.as_bytes());
    let p256_jwt = format!(
        "{p256_input}.{}",
        URL_SAFE_NO_PAD.encode(p256_sig.to_bytes().as_slice())
    );

    let resp_p256 = client
        .get(&feed_url)
        .header("Authorization", format!("Bearer {p256_jwt}"))
        .send()
        .await
        .unwrap();
    assert_eq!(resp_p256.status(), reqwest::StatusCode::OK);
    let p256_uid = state
        .recommender
        .interner
        .lookup_id(p256_did)
        .expect("Verified ES256 P-256 DID must be interned as authenticated viewer");
    assert!(
        state
            .recommender
            .impression_store()
            .get_viewer_impression_count(p256_uid)
            > 0,
        "Verified ES256 P-256 viewer must have recorded impressions"
    );

    // 3. Cross-curve confusion: ES256 header claiming k256_did must be rejected by verifier
    let confused_input = format!("{p256_header}.{k256_payload}");
    let confused_sig: k256::ecdsa::Signature = k256_signing.sign(confused_input.as_bytes());
    let confused_jwt = format!(
        "{confused_input}.{}",
        URL_SAFE_NO_PAD.encode(confused_sig.to_bytes().as_slice())
    );
    assert!(
        state
            .service_auth
            .verify_service_jwt(
                &format!("Bearer {confused_jwt}"),
                Some("did:web:feed.example.com"),
                now
            )
            .await
            .is_err(),
        "Cross-curve ES256 header with secp256k1 DID key must be rejected"
    );
}

// ============================================================================
// Scenario 2 (P0-2): Compaction Preserves Impressions, Quote/Repost Bitmaps, and Velocity Pool
// ============================================================================

#[test]
fn test_p0_2_compaction_remaps_impressions_preserves_quote_repost_bitmaps_and_velocity_pool() {
    let interner = Arc::new(StringInterner::new());
    let graph = Arc::new(GraphStore::new());
    let preferences = Arc::new(UserPreferencesStore::new());
    let impression_store = Arc::new(ImpressionStore::new(1000));
    let now = BLUESKY_EPOCH_SECS + 100_000;

    // Create dead strings in every shard first so that compaction shifts local indices downward!
    for i in 0..256 {
        let dead_str = format!("did:plc:dead_entity_before_active_{i}");
        let dead_id = interner.intern(&dead_str);
        // Record an old interaction that will be pruned before compaction
        let dead_post = interner.intern(&format!(
            "at://did:plc:dead/app.bsky.feed.post/dead_post_{i}"
        ));
        graph.record_interaction(
            dead_id,
            dead_post,
            SignalType::Like,
            BLUESKY_EPOCH_SECS + 10,
        );
    }

    // Now intern active viewer A, viewer B, twin T, and active posts P1..P12
    let viewer_a_did = "did:plc:active_viewer_alpha";
    let viewer_b_did = "did:plc:active_viewer_beta";
    let twin_did = "did:plc:active_taste_twin";
    let uid_a_before = interner.intern(viewer_a_did);
    let uid_b_before = interner.intern(viewer_b_did);
    let uid_twin_before = interner.intern(twin_did);

    let mut post_uris = Vec::new();
    let mut pids_before = Vec::new();
    for idx in 0..12 {
        let uri = format!("at://did:plc:creator_{idx}/app.bsky.feed.post/3k2yihcrp6f2c");
        let pid = interner.intern(&uri);
        let aid = interner.intern(&format!("did:plc:creator_{idx}"));
        graph.record_post_meta(pid, aid, None, None, now - 300);
        post_uris.push(uri);
        pids_before.push(pid);
    }

    // Viewer A and Twin share 10 Repost/Quote interactions (NO Likes!) on P0..P9
    for &pid in &pids_before[0..10] {
        graph.record_interaction(uid_a_before, pid, SignalType::Repost, now - 200);
        graph.record_interaction(uid_twin_before, pid, SignalType::Quote, now - 190);
    }
    // Twin also quotes P10 and reposts P11 (candidate recommendations for Viewer A)
    graph.record_interaction(
        uid_twin_before,
        pids_before[10],
        SignalType::Quote,
        now - 100,
    );
    graph.record_interaction(
        uid_twin_before,
        pids_before[11],
        SignalType::Repost,
        now - 90,
    );

    // Viewer B has a recent interaction so Viewer B stays active
    graph.record_interaction(uid_b_before, pids_before[0], SignalType::Like, now - 80);

    // Record impressions for Viewer A on P10 (so P10 is fatigued for Viewer A, while P11 is unseen)
    impression_store.record_impressions(uid_a_before, &[pids_before[10]], now - 30);
    assert!(impression_store.contains_impression(uid_a_before, pids_before[10]));
    assert!(!impression_store.contains_impression(uid_b_before, pids_before[10]));

    // Prune dead old interactions and run full compaction with impressions
    graph.prune_older_than(now - 3600);
    let stats =
        compact_memory_stores_with_impressions(&interner, &graph, &preferences, &impression_store);
    assert!(
        stats.strings_after < stats.strings_before,
        "dead strings must be garbage-collected"
    );

    let uid_a_after = interner.lookup_id(viewer_a_did).unwrap();
    let uid_b_after = interner.lookup_id(viewer_b_did).unwrap();
    let uid_twin_after = interner.lookup_id(twin_did).unwrap();
    let p10_after = interner.lookup_id(&post_uris[10]).unwrap();
    let p11_after = interner.lookup_id(&post_uris[11]).unwrap();

    // 1. ImpressionStore must be remapped to the new IDs without cross-viewer or cross-post corruption
    assert!(
        impression_store.contains_impression(uid_a_after, p10_after),
        "Viewer A's impression on P10 must survive compaction under remapped IDs"
    );
    assert!(
        !impression_store.contains_impression(uid_a_after, p11_after),
        "Viewer A must not have false impression on P11"
    );
    assert!(
        !impression_store.contains_impression(uid_b_after, p10_after),
        "Viewer B must not inherit Viewer A's impression"
    );

    // 2. Quote and Repost edges must still be present in user_likes_bitmaps after compaction
    let bm_a = graph.get_user_likes_bitmap(uid_a_after).unwrap();
    let bm_twin = graph.get_user_likes_bitmap(uid_twin_after).unwrap();
    assert_eq!(bm_a.len(), 10, "Repost edges must survive bitmap rebuild");
    assert_eq!(
        bm_twin.len(),
        12,
        "Quote/Repost edges must survive bitmap rebuild"
    );
    assert!(
        graph.compute_cosine_similarity(uid_a_after, uid_twin_after) > 0.5,
        "Cosine similarity over Quote/Repost bitmaps must remain non-zero after compaction"
    );

    // 3. Adding 1 new interaction after compaction must NOT cause velocity pool 1-item blindness
    let new_post_uri = "at://did:plc:new_author/app.bsky.feed.post/3k2yihcrp6f2c";
    let new_pid = interner.intern(new_post_uri);
    graph.record_interaction(uid_b_after, new_pid, SignalType::Like, now - 10);
    let velocity_candidates = graph.get_velocity_pool_candidates_at(now, 50);
    assert!(
        velocity_candidates.len() >= 12,
        "Velocity pool must retain pre-compaction active posts alongside new post, got {} posts",
        velocity_candidates.len()
    );
}

// ============================================================================
// Scenario 3 (P0-3): Content-Based Topic Classification & Snapshot Persistence
// ============================================================================

#[test]
fn test_p0_3_content_topic_classification_persists_across_snapshot_and_boosts_ranking() {
    let interner = Arc::new(StringInterner::new());
    let graph = Arc::new(GraphStore::new());
    let preferences = Arc::new(UserPreferencesStore::new());
    let now = BLUESKY_EPOCH_SECS + 200_000;

    // Ingest two posts with opaque DIDs and opaque TID rkeys (no topic keywords in DID or URI!)
    let tech_frame = serde_json::json!({
        "did": "did:plc:opaque111111111111111111",
        "time_us": now * 1_000_000,
        "kind": "commit",
        "commit": {
            "operation": "create",
            "collection": "app.bsky.feed.post",
            "rkey": "3k2yihcrp6f2c",
            "record": {
                "$type": "app.bsky.feed.post",
                "text": "Building a distributed database in Rust with Linux io_uring",
                "createdAt": "2024-03-09T16:00:00Z"
            }
        }
    })
    .to_string();

    let art_frame = serde_json::json!({
        "did": "did:plc:opaque222222222222222222",
        "time_us": now * 1_000_000,
        "kind": "commit",
        "commit": {
            "operation": "create",
            "collection": "app.bsky.feed.post",
            "rkey": "3k2yihcrp6f2d",
            "record": {
                "$type": "app.bsky.feed.post",
                "text": "Finished a new watercolor portrait illustration in my sketchbook",
                "createdAt": "2024-03-09T16:00:00Z"
            }
        }
    })
    .to_string();

    for frame in [&tech_frame, &art_frame] {
        let (events, _) = parse_jetstream_frame(frame).unwrap();
        for ev in &events {
            apply_event_to_graph(ev, &interner, &graph);
        }
    }

    let tech_uri = "at://did:plc:opaque111111111111111111/app.bsky.feed.post/3k2yihcrp6f2c";
    let art_uri = "at://did:plc:opaque222222222222222222/app.bsky.feed.post/3k2yihcrp6f2d";
    let tech_pid = interner.lookup_id(tech_uri).unwrap();
    let art_pid = interner.lookup_id(art_uri).unwrap();

    assert_eq!(graph.get_post_topic(tech_pid), Some(TopicCategory::Tech));
    assert_eq!(graph.get_post_topic(art_pid), Some(TopicCategory::Art));

    // Give both posts identical interaction velocity
    for i in 0..4 {
        let liker = interner.intern(&format!("did:plc:topic_liker_{i}"));
        graph.record_interaction(liker, tech_pid, SignalType::Like, now - 60);
        graph.record_interaction(liker, art_pid, SignalType::Like, now - 60);
    }

    // Save snapshot to disk and restore into fresh stores
    let snap_path = std::env::temp_dir().join(format!(
        "fyc_p0_3_topic_snapshot_{}_{}.bin",
        std::process::id(),
        now
    ));
    save_snapshot_with_preferences(&snap_path, &interner, &graph, &preferences, 123_456).unwrap();

    let rest_interner = Arc::new(StringInterner::new());
    let rest_graph = Arc::new(GraphStore::new());
    let rest_prefs = Arc::new(UserPreferencesStore::new());
    load_snapshot_with_preferences(&snap_path, &rest_interner, &rest_graph, &rest_prefs)
        .unwrap()
        .unwrap();
    let _ = std::fs::remove_file(&snap_path);

    let rest_tech_pid = rest_interner.lookup_id(tech_uri).unwrap();
    let rest_art_pid = rest_interner.lookup_id(art_uri).unwrap();
    assert_eq!(
        rest_graph.get_post_topic(rest_tech_pid),
        Some(TopicCategory::Tech)
    );
    assert_eq!(
        rest_graph.get_post_topic(rest_art_pid),
        Some(TopicCategory::Art)
    );

    // Verify Recommender preview uses the persisted content topics when applying topic weights
    let rec = Recommender::new(rest_interner, rest_graph);
    let mut dials = RecommendationDials::default();
    dials.topic_weights.tech = 2.0;
    dials.topic_weights.art = 0.2;
    let preview = rec.recommend_preview_at(None, &dials, now).unwrap();
    assert_eq!(preview.items.len(), 2);
    assert_eq!(preview.items[0].uri.as_str(), tech_uri);
    assert_eq!(preview.items[0].topic, TopicCategory::Tech);
    assert_eq!(preview.items[1].uri.as_str(), art_uri);
    assert_eq!(preview.items[1].topic, TopicCategory::Art);
    assert!(
        preview.items[0].score_breakdown.final_score > preview.items[1].score_breakdown.final_score
    );
}

// ============================================================================
// Scenario 4 (P0-4): PostMeta Synthesis (Zero Sentinel Collision) & Post Deletion
// ============================================================================

#[test]
fn test_p0_4_post_meta_synthesis_no_author_zero_collision_and_post_deletion() {
    let interner = Arc::new(StringInterner::new());
    let graph = Arc::new(GraphStore::new());
    let now = 1_710_000_500u64;

    // Find a viewer DID that hashes to shard 0 so it receives interned ID == 0 (shard=0, local=0)!
    let mut viewer_zero_did = String::new();
    for i in 0..10_000 {
        let candidate = format!("did:plc:shard_zero_probe_{i}");
        let probe_interner = StringInterner::new();
        if probe_interner.intern(&candidate) == 0 {
            viewer_zero_did = candidate;
            break;
        }
    }
    assert_ne!(viewer_zero_did, "");
    let viewer_id = interner.intern(&viewer_zero_did);
    assert_eq!(
        viewer_id, 0,
        "First entity in shard 0 has interned u32 ID == 0"
    );

    // Viewer 0 follows `did:plc:followed_curator`
    let curator_did = "did:plc:followed_curator";
    let curator_id = interner.intern(curator_did);
    graph.record_follow(viewer_id, curator_id);

    // Curator likes 4 posts by DIFFERENT authors where NO PostMeta event was ever ingested!
    let orphan_posts = [
        "at://did:plc:author_one/app.bsky.feed.post/3k2yihcrp6f2c",
        "at://did:plc:author_two/app.bsky.feed.post/3k2yihcrp6f2d",
        "at://did:plc:author_three/app.bsky.feed.post/3k2yihcrp6f2e",
        "at://did:plc:author_four/app.bsky.feed.post/3k2yihcrp6f2f",
    ];
    for uri in &orphan_posts {
        let ev = JetstreamEvent::Interaction {
            user_did: CompactString::new(curator_did),
            post_uri: CompactString::new(*uri),
            signal: SignalType::Like,
            timestamp_secs: now - 30,
        };
        apply_event_to_graph(&ev, &interner, &graph);
    }

    let rec = Recommender::new(Arc::clone(&interner), Arc::clone(&graph));
    let dials = RecommendationDials {
        min_likes: 1,
        ..Default::default()
    };

    // 1. Tier 2 follow walk must recommend all 4 posts (previously all 4 were dropped because
    //    get_post_meta returned None, OR in Tier 3 they had author_id = 0 which collided with viewer_id == 0
    //    and triggered self-post exclusion + 2-per-author cap!).
    let feed = rec
        .recommend_at(Some(&viewer_zero_did), &dials, now)
        .unwrap();
    assert_eq!(
        feed.posts.len(),
        4,
        "All 4 interaction-only posts by distinct authors must be recommended to viewer_id == 0"
    );

    // 2. Delete the first post via Jetstream delete commit and verify it is immediately removed from recommendations
    let delete_ev = JetstreamEvent::Delete {
        did: CompactString::new("did:plc:author_one"),
        collection: CompactString::new("app.bsky.feed.post"),
        rkey: CompactString::new("3k2yihcrp6f2c"),
    };
    apply_event_to_graph(&delete_ev, &interner, &graph);

    let feed_after_delete = rec
        .recommend_at(Some(&viewer_zero_did), &dials, now)
        .unwrap();
    assert_eq!(feed_after_delete.posts.len(), 3);
    assert!(
        !feed_after_delete
            .posts
            .iter()
            .any(|p| p.uri.as_str() == orphan_posts[0]),
        "Deleted post must not appear in recommendations"
    );
}

// ============================================================================
// Scenario 5: Real Compiled Binary Process Hydration & Live HTTP/XRPC Verification
// ============================================================================

#[tokio::test]
async fn test_p0_5_live_binary_process_snapshot_hydration_and_xrpc_endpoints() {
    let now = now_secs();
    let interner = StringInterner::new();
    let graph = GraphStore::new();
    let preferences = UserPreferencesStore::new();

    // Ingest two content-classified posts with opaque DIDs and TID rkeys
    let rust_uri = "at://did:plc:opaque_rust_dev/app.bsky.feed.post/3k2yihcrp6f2c";
    let art_uri = "at://did:plc:opaque_painter/app.bsky.feed.post/3k2yihcrp6f2d";
    let orphan_uri = "at://did:plc:opaque_orphan/app.bsky.feed.post/3k2yihcrp6f2e";

    let rust_frame = serde_json::json!({
        "did": "did:plc:opaque_rust_dev",
        "time_us": (now - 120) * 1_000_000,
        "kind": "commit",
        "commit": {
            "operation": "create",
            "collection": "app.bsky.feed.post",
            "rkey": "3k2yihcrp6f2c",
            "record": {
                "$type": "app.bsky.feed.post",
                "text": "Optimizing Rust compiler and Linux kernel performance",
                "tags": ["rustlang"],
                "createdAt": "2026-11-02T23:00:00Z"
            }
        }
    })
    .to_string();

    let art_frame = serde_json::json!({
        "did": "did:plc:opaque_painter",
        "time_us": (now - 110) * 1_000_000,
        "kind": "commit",
        "commit": {
            "operation": "create",
            "collection": "app.bsky.feed.post",
            "rkey": "3k2yihcrp6f2d",
            "record": {
                "$type": "app.bsky.feed.post",
                "text": "Watercolor illustration and portrait sketch in the gallery",
                "tags": ["watercolor"],
                "createdAt": "2026-11-02T23:00:00Z"
            }
        }
    })
    .to_string();

    for frame in [&rust_frame, &art_frame] {
        let (events, _) = parse_jetstream_frame(frame).unwrap();
        for ev in &events {
            apply_event_to_graph(ev, &interner, &graph);
        }
    }

    // Record interactions (>= 5 likes/quotes/reposts per post so default min_likes=5 passes)
    // including an interaction-only orphan post where PostMeta is synthesized from AT-URI!
    for idx in 0..6 {
        let user_did = format!("did:plc:live_curator_{idx}");
        for (uri, sig) in [
            (rust_uri, SignalType::Quote),
            (art_uri, SignalType::Repost),
            (orphan_uri, SignalType::Like),
        ] {
            let ev = JetstreamEvent::Interaction {
                user_did: CompactString::new(&user_did),
                post_uri: CompactString::new(uri),
                signal: sig,
                timestamp_secs: now - 60,
            };
            apply_event_to_graph(&ev, &interner, &graph);
        }
    }

    let snap_path = std::env::temp_dir().join(format!(
        "fyc_live_binary_test_{}_{}.bin",
        std::process::id(),
        now
    ));
    save_snapshot_with_preferences(&snap_path, &interner, &graph, &preferences, now * 1_000_000)
        .unwrap();

    // Reserve an ephemeral port
    let probe = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    let port = probe.local_addr().unwrap().port();
    drop(probe);

    let bin_path = env!("CARGO_BIN_EXE_for-your-consideration");
    let child = std::process::Command::new(bin_path)
        .env("ENABLE_INGESTION", "false")
        .env("SNAPSHOT_PATH", &snap_path)
        .env("HOST", "127.0.0.1")
        .env("PORT", port.to_string())
        .env("HOSTNAME", "feed.example.com")
        .env("SERVICE_DID", "did:web:feed.example.com")
        .env("SERVICE_AUTH_MODE", "enforce")
        .env(
            "SESSION_SECRET",
            "live-binary-integration-test-secret-0123456789abcdef",
        )
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
        .expect("Failed to spawn for-your-consideration binary");

    let client = reqwest::Client::new();
    let base_url = format!("http://127.0.0.1:{port}");

    // Wait for server readiness and background snapshot hydration
    let mut ready = false;
    for _ in 0..50 {
        if let Ok(resp) = client.get(format!("{base_url}/healthz")).send().await {
            if let Ok(json) = resp.json::<serde_json::Value>().await {
                if json["edges"].as_u64().unwrap_or(0) >= 18 {
                    ready = true;
                    break;
                }
            }
        }
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
    }

    // Ensure child process is always cleaned up
    let cleanup = |mut c: std::process::Child, path: &std::path::Path| {
        let _ = c.kill();
        let _ = c.wait();
        let _ = std::fs::remove_file(path);
    };

    if !ready {
        cleanup(child, &snap_path);
        panic!("Live binary did not hydrate snapshot within timeout");
    }

    // 1. Verify /xrpc/app.bsky.feed.getFeedSkeleton serves all 3 posts (including the synthesized PostMeta post!)
    let skel_resp = client
        .get(format!(
            "{base_url}/xrpc/app.bsky.feed.getFeedSkeleton?feed=at://did:web:feed.example.com/app.bsky.feed.generator/for-your-consideration"
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(skel_resp.status(), reqwest::StatusCode::OK);
    let skel: FeedSkeletonResponse = skel_resp.json().await.unwrap();
    assert_eq!(skel.feed.len(), 3);

    // 2. Verify /api/feed-preview preserves content-classified topics from the hydrated snapshot
    let preview_resp = client
        .get(format!(
            "{base_url}/api/feed-preview?tech=2.0&art=0.2&explain=true"
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(preview_resp.status(), reqwest::StatusCode::OK);
    let preview: FeedPreviewResponse = preview_resp.json().await.unwrap();
    let rust_item = preview
        .items
        .iter()
        .find(|i| i.uri.as_str() == rust_uri)
        .expect("Rust post must be in preview");
    let art_item = preview
        .items
        .iter()
        .find(|i| i.uri.as_str() == art_uri)
        .expect("Art post must be in preview");
    assert_eq!(rust_item.topic, TopicCategory::Tech);
    assert_eq!(art_item.topic, TopicCategory::Art);
    assert!(rust_item.score_breakdown.final_score > art_item.score_breakdown.final_score);

    // 3. Verify /api/taste-twins finds taste twins connected solely via Quote/Repost bitmaps
    let twins_resp = client
        .get(format!(
            "{base_url}/api/taste-twins?did=did:plc:live_curator_0"
        ))
        .send()
        .await
        .unwrap();
    assert_eq!(twins_resp.status(), reqwest::StatusCode::OK);
    let twins: TasteTwinsResponse = twins_resp.json().await.unwrap();
    assert_eq!(twins.twins.len(), 5);

    cleanup(child, &snap_path);
}
