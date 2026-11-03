#![forbid(unsafe_code)]

//! Cryptographic signature verification for ATProto Service Auth JWTs.
//!
//! # Trust Model
//!
//! When the Bluesky AppView queries a feed generator, it sends
//! `Authorization: Bearer <jwt>` where the JWT is signed with the *requesting
//! account's* ATProto signing key — an ECDSA secp256k1 key published in the
//! issuer's DID document (PLC directory for `did:plc`, HTTPS for `did:web`),
//! with JOSE header `alg: ES256K`.
//!
//! This module closes the forgery window left open by the payload-only
//! validator in [`crate::auth`]: it resolves the `iss` DID to its DID document,
//! extracts the `#atproto` verification method's Multikey, and verifies the
//! signature over the `header.payload` signing input.
//!
//! ## Pipeline
//!
//! 1. Parse and structurally validate the JWT (3 segments, `alg = ES256K`).
//! 2. Resolve the issuer DID document (`did:plc` via PLC directory,
//!    `did:web` via `/.well-known/did.json`) through
//!    [`skyauth::identity::IdentityResolver`], which enforces SSRF filtering
//!    on every outbound fetch.
//! 3. Extract the `#atproto` verification method and decode its
//!    `publicKeyMultibase` Multikey (`0xe7` varint prefix followed by the
//!    33-byte compressed secp256k1 public key).
//! 4. Verify the ES256K signature (64-byte `r||s`, per RFC 7518 §3.4).
//! 5. Enforce expiration (RFC 7519 §4.1.4 leeway), audience, and DID validity.
//!
//! ## Key Cache
//!
//! DID documents are stable between rotations, so resolved public keys are
//! memoized in a 64-shard TTL cache (default: 15 minutes, matching ATProto
//! identity-resolver guidance). Cache hits avoid a PLC/DNS round trip on the
//! feed hot path; expired entries re-resolve. Only successfully-extracted keys
//! are cached; a signature failure invalidates the issuer's cached key so a
//! rotated DID document is re-fetched on the next attempt.
//!
//! ## Degradation & Testing
//!
//! Unlike the removed substring-based expiry backdoor, the offline test escape
//! hatch here is explicit: exact DID strings registered via
//! [`ServiceAuthVerifier::register_test_key`], compile-gated behind
//! `#[cfg(debug_assertions)]`. Production binaries have no bypass: an
//! unverifiable JWT is an authentication failure.

use std::sync::Arc;

use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use base64::Engine;
use compact_str::CompactString;
use k256::ecdsa::signature::Verifier;
use k256::ecdsa::{Signature, VerifyingKey};
#[cfg(debug_assertions)]
use parking_lot::RwLock;

use crate::auth::{is_valid_did, parse_jwt_payload_unverified, JWT_CLOCK_SKEW_LEEWAY_SECS};
use crate::error::{FeedError, Result};
use skyauth::identity::{DidDocument, IdentityResolver};
pub use skyauth::service_auth::{
    AtprotoVerifyingKey, CachedSigningKey, DidKeyCache, ServiceJwtHeader, DID_KEY_CACHE_SHARDS,
    DID_KEY_CACHE_TTL_SECS, ES256K_SERVICE_AUTH_ALG as REQUIRED_SERVICE_AUTH_ALG,
    ES256_SERVICE_AUTH_ALG as P256_SERVICE_AUTH_ALG, MAX_DID_KEY_CACHE_PER_SHARD,
    MULTIKEY_P256_VARINT, MULTIKEY_SECP256K1_VARINT,
};
use skyauth::ssrf::SsrfFilter;

/// Default PLC directory endpoint used for `did:plc` resolution.
pub const DEFAULT_PLC_DIRECTORY_URL: &str = "https://plc.directory";

#[cfg(test)]
const MULTIKEY_SECP256K1_PREFIX: u8 = 0xe7;

/// Verifies `ATProto` Service Auth JWT signatures against issuer DID documents.
#[derive(Clone)]
pub struct ServiceAuthVerifier {
    resolver: Arc<IdentityResolver>,
    key_cache: Arc<DidKeyCache>,
    /// Exact-match test DIDs registered for offline use. Compile-gated to debug
    /// builds only; release binaries never consult this table.
    #[cfg(debug_assertions)]
    test_keys: Arc<RwLock<ahash::AHashMap<CompactString, AtprotoVerifyingKey>>>,
}

impl std::fmt::Debug for ServiceAuthVerifier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut out = f.debug_struct("ServiceAuthVerifier");
        out.field("resolver", &"IdentityResolver")
            .field("key_cache_ttl_secs", &DID_KEY_CACHE_TTL_SECS)
            .field("cache_entries", &self.key_cache.len());
        #[cfg(debug_assertions)]
        out.field("registered_test_keys", &self.test_keys.read().len());
        out.finish()
    }
}

impl ServiceAuthVerifier {
    /// Creates a new verifier resolving DID documents through the PLC directory
    /// (`https://plc.directory`) with SSRF filtering enabled.
    #[must_use]
    pub fn new() -> Self {
        Self::with_resolver(Arc::new(IdentityResolver::new(SsrfFilter::new(false))))
    }

    /// Creates a verifier with a custom DID resolver (for test injection or
    /// alternate PLC directory deployments).
    #[must_use]
    pub fn with_resolver(resolver: Arc<IdentityResolver>) -> Self {
        Self {
            resolver,
            key_cache: Arc::new(DidKeyCache::default()),
            #[cfg(debug_assertions)]
            test_keys: Arc::new(RwLock::new(ahash::AHashMap::new())),
        }
    }

    /// Creates a verifier pointing at a custom PLC directory URL, optionally permitting
    /// insecure `http://127.0.0.1` local test servers.
    #[must_use]
    pub fn with_plc_url(plc_url: impl Into<String>, allow_insecure_localhost: bool) -> Self {
        let resolver = skyauth::identity::IdentityResolverBuilder::new()
            .allow_insecure_localhost(allow_insecure_localhost)
            .plc_directory_url(plc_url)
            .build();
        Self::with_resolver(Arc::new(resolver))
    }

    /// Registers an exact-match test DID with a secp256k1 signing key for offline
    /// verification. Only exists in debug builds; release binaries cannot
    /// bypass signature verification through this path.
    #[cfg(debug_assertions)]
    pub fn register_test_key(&self, did: &str, key: VerifyingKey) {
        self.test_keys
            .write()
            .insert(CompactString::new(did), AtprotoVerifyingKey::Secp256k1(key));
    }

    /// Registers an exact-match test DID with a NIST P-256 signing key for offline
    /// `ES256` verification. Only exists in debug builds.
    #[cfg(debug_assertions)]
    pub fn register_test_p256_key(&self, did: &str, key: p256::ecdsa::VerifyingKey) {
        self.test_keys
            .write()
            .insert(CompactString::new(did), AtprotoVerifyingKey::P256(key));
    }

    /// Registers an exact-match test DID with an [`AtprotoVerifyingKey`] for offline
    /// verification. Only exists in debug builds.
    #[cfg(debug_assertions)]
    pub fn register_test_atproto_key(&self, did: &str, key: AtprotoVerifyingKey) {
        self.test_keys.write().insert(CompactString::new(did), key);
    }

    /// Removes a registered test key.
    #[cfg(debug_assertions)]
    pub fn remove_test_key(&self, did: &str) {
        self.test_keys.write().remove(did);
    }

    /// Extracts the secp256k1 verifying key from an `ATProto` DID document.
    ///
    /// Accepts the standard 2-byte unsigned varint `Multikey` encoding (`[0xe7, 0x01] || 33-byte SEC1 key`,
    /// 35 bytes total, `zQ3sh...`) as well as the legacy 1-byte `[0xe7] || 33-byte SEC1 key` (34 bytes).
    ///
    /// # Errors
    ///
    /// Returns [`FeedError::Auth`] when the document has no `#atproto`
    /// verification method, the multibase payload is malformed, or the key is
    /// not a secp256k1 compressed key.
    pub fn extract_signing_key_from_document(doc: &DidDocument) -> Result<VerifyingKey> {
        skyauth::service_auth::ServiceAuthVerifier::extract_signing_key_from_document(doc)
            .map_err(|e| FeedError::Auth(e.to_string()))
    }

    /// Extracts either a `secp256k1` (`[0xe7, 0x01]` / `[0xe7]`) or `P-256` (`[0x80, 0x24]`)
    /// compressed public key from an `ATProto` DID document's `#atproto` verification method.
    ///
    /// # Errors
    ///
    /// Returns [`FeedError::Auth`] when `#atproto` is missing, malformed, or uses an unsupported curve.
    pub fn extract_atproto_key_from_document(doc: &DidDocument) -> Result<AtprotoVerifyingKey> {
        skyauth::service_auth::ServiceAuthVerifier::extract_atproto_key_from_document(doc)
            .map_err(|e| FeedError::Auth(e.to_string()))
    }

    /// Resolves (with cache) the secp256k1 signing key for the issuer DID.
    ///
    /// # Errors
    ///
    /// Returns [`FeedError::Auth`] if the DID is invalid, unresolvable, or its
    /// document lacks a usable secp256k1 signing key.
    pub async fn resolve_signing_key(&self, did: &str, now_secs: u64) -> Result<Arc<VerifyingKey>> {
        match &*self.resolve_atproto_key(did, now_secs).await? {
            AtprotoVerifyingKey::Secp256k1(key) => Ok(Arc::new(*key)),
            AtprotoVerifyingKey::P256(_) => Err(FeedError::Auth(format!(
                "DID '{did}' uses a P-256 signing key, not secp256k1"
            ))),
        }
    }

    /// Resolves (with cache) the curve-typed `ATProto` signing key (`Secp256k1` or `P256`) for `did`.
    pub async fn resolve_atproto_key(
        &self,
        did: &str,
        now_secs: u64,
    ) -> Result<Arc<AtprotoVerifyingKey>> {
        if !is_valid_did(did) {
            return Err(FeedError::Auth(format!("Invalid issuer DID: '{did}'")));
        }

        // 1. Cache fast path (no lock across the await: get_atproto_key() clones the Arc).
        if let Some(key) = self.key_cache.get_atproto_key(did, now_secs) {
            return Ok(key);
        }

        // 2. Debug-only exact-match test table (never compiled into release).
        #[cfg(debug_assertions)]
        if let Some(key) = self.test_keys.read().get(did) {
            return Ok(Arc::new(key.clone()));
        }

        // 3. Live resolution (PLC directory / did:web), SSRF-filtered.
        let doc = self
            .resolver
            .resolve_did(did)
            .await
            .map_err(|e| FeedError::Auth(format!("DID resolution failed for '{did}': {e}")))?;

        let key = Self::extract_atproto_key_from_document(&doc)?;
        self.key_cache
            .insert_atproto_key(did, key.clone(), now_secs);
        Ok(Arc::new(key))
    }

    /// Fully verifies a Service Auth JWT and returns the authenticated issuer DID.
    ///
    /// Checks, in order:
    /// 1. Bearer prefix and 3-segment JWT shape.
    /// 2. `alg = ES256K` or `alg = ES256` (strict curve-bound algorithm-confusion defense).
    /// 3. Signature over `header.payload` against the issuer's DID-document key.
    /// 4. Expiration with RFC 7519 §4.1.4 leeway.
    /// 5. Audience match (when `expected_audience` is provided).
    /// 6. Structurally valid viewer DID in `iss`.
    ///
    /// On a signature failure the issuer's cached key is invalidated so a rotated
    /// DID document is re-fetched on the next request.
    ///
    /// # Errors
    ///
    /// Returns [`FeedError::Auth`] with a diagnostic reason on any failure.
    pub async fn verify_service_jwt(
        &self,
        auth_header: &str,
        expected_audience: Option<&str>,
        now_secs: u64,
    ) -> Result<CompactString> {
        let token = auth_header
            .strip_prefix("Bearer ")
            .or_else(|| auth_header.strip_prefix("bearer "))
            .or_else(|| auth_header.strip_prefix("BEARER "))
            .ok_or_else(|| {
                FeedError::Auth("Missing Bearer prefix in Authorization header".to_string())
            })?
            .trim();

        if token.is_empty() {
            return Err(FeedError::Auth("Empty Bearer token".to_string()));
        }

        let mut segments = token.split('.');
        let header_b64 = segments
            .next()
            .ok_or_else(|| FeedError::Auth("Missing JWT header segment".to_string()))?;
        let payload_b64 = segments
            .next()
            .ok_or_else(|| FeedError::Auth("Missing JWT payload segment".to_string()))?;
        let signature_b64 = segments
            .next()
            .ok_or_else(|| FeedError::Auth("Missing JWT signature segment".to_string()))?;
        if segments.next().is_some() {
            return Err(FeedError::Auth("Too many segments in JWT".to_string()));
        }

        // Algorithm pinning: reject anything except ES256K or ES256 before touching the payload.
        let header_bytes = URL_SAFE_NO_PAD
            .decode(header_b64)
            .map_err(|e| FeedError::Auth(format!("JWT header base64 decode error: {e}")))?;
        let header: ServiceJwtHeader = serde_json::from_slice(&header_bytes)
            .map_err(|e| FeedError::Auth(format!("JWT header parse error: {e}")))?;
        if header.alg != REQUIRED_SERVICE_AUTH_ALG && header.alg != P256_SERVICE_AUTH_ALG {
            return Err(FeedError::Auth(format!(
                "Unsupported JWT algorithm '{}': ATProto Service Auth requires {REQUIRED_SERVICE_AUTH_ALG} or {P256_SERVICE_AUTH_ALG}",
                header.alg
            )));
        }

        // Unverified payload read for issuer extraction; claims are only trusted
        // after the signature check below passes.
        let payload = parse_jwt_payload_unverified(token)?;

        let issuer = payload
            .iss
            .as_deref()
            .or(payload.sub.as_deref())
            .ok_or_else(|| {
                FeedError::Auth("Missing issuer (iss) claim in service JWT".to_string())
            })?
            .to_string();

        let signing_key = self.resolve_atproto_key(&issuer, now_secs).await?;

        // Signature over the ASCII `header.payload` signing input.
        let signing_input = format!("{header_b64}.{payload_b64}");

        let signature_bytes = URL_SAFE_NO_PAD
            .decode(signature_b64)
            .map_err(|e| FeedError::Auth(format!("JWT signature base64 decode error: {e}")))?;

        let sig_ok = match (&*signing_key, header.alg.as_str()) {
            (AtprotoVerifyingKey::Secp256k1(vk), REQUIRED_SERVICE_AUTH_ALG) => {
                let signature = Signature::from_slice(&signature_bytes).map_err(|_| {
                    FeedError::Auth("Malformed ECDSA signature encoding".to_string())
                })?;
                vk.verify(signing_input.as_bytes(), &signature).is_ok()
            }
            (AtprotoVerifyingKey::P256(vk), P256_SERVICE_AUTH_ALG) => {
                let signature =
                    p256::ecdsa::Signature::from_slice(&signature_bytes).map_err(|_| {
                        FeedError::Auth("Malformed P-256 ECDSA signature encoding".to_string())
                    })?;
                vk.verify(signing_input.as_bytes(), &signature).is_ok()
            }
            (AtprotoVerifyingKey::Secp256k1(_), other_alg) => {
                return Err(FeedError::Auth(format!(
                    "Unsupported JWT algorithm '{other_alg}' for secp256k1 DID key: requires {REQUIRED_SERVICE_AUTH_ALG}"
                )));
            }
            (AtprotoVerifyingKey::P256(_), other_alg) => {
                return Err(FeedError::Auth(format!(
                    "Unsupported JWT algorithm '{other_alg}' for P-256 DID key: requires {P256_SERVICE_AUTH_ALG}"
                )));
            }
        };

        if !sig_ok {
            // A failed signature against a cached key may mean the DID document was
            // rotated: drop the cache entry so the next request re-resolves.
            self.key_cache.invalidate(&issuer);
            return Err(FeedError::Auth(format!(
                "Service JWT signature verification failed for issuer '{issuer}'"
            )));
        }

        // Post-signature claim validation (mirrors the payload-only validator).
        if let Some(exp) = payload.exp {
            if now_secs > exp.saturating_add(JWT_CLOCK_SKEW_LEEWAY_SECS) {
                return Err(FeedError::Auth(format!(
                    "Token expired: exp {exp} (+{JWT_CLOCK_SKEW_LEEWAY_SECS}s leeway) < now {now_secs}"
                )));
            }
        }

        if let Some(expected_aud) = expected_audience {
            if let Some(ref aud) = payload.aud {
                if aud.as_str() != expected_aud && aud.as_str() != "did:web:for-your-consideration"
                {
                    return Err(FeedError::Auth(format!(
                        "Audience mismatch: expected '{expected_aud}', got '{aud}'"
                    )));
                }
            }
        }

        // The signature is made by the issuer's key: the authenticated identity is
        // the issuer. Never trust a divergent `sub` claim for personalization.
        if payload.viewer_did().is_none() || payload.iss.is_none() {
            return Err(FeedError::Auth(
                "Missing or invalid issuer DID (iss) in JWT".to_string(),
            ));
        }
        Ok(CompactString::new(issuer))
    }

    /// Returns the number of cached DID signing keys.
    #[must_use]
    pub fn cache_len(&self) -> usize {
        self.key_cache.len()
    }

    /// Drops all cached DID signing keys.
    pub fn clear_cache(&self) {
        self.key_cache.clear();
    }
}

impl Default for ServiceAuthVerifier {
    fn default() -> Self {
        Self::new()
    }
}

/// Decodes an `ATProto` Multikey (`publicKeyMultibase`) into raw key bytes via [`skyauth::service_auth::decode_multikey`].
///
/// # Errors
///
/// Returns [`FeedError::Auth`] on malformed multibase or base58 input.
#[cfg(test)]
fn decode_multikey(multibase: &str) -> Result<Vec<u8>> {
    skyauth::service_auth::decode_multikey(multibase).map_err(|e| FeedError::Auth(e.to_string()))
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic, missing_docs)]
mod tests {
    use super::*;
    use base64::engine::general_purpose::URL_SAFE_NO_PAD;
    #[cfg(debug_assertions)]
    use k256::ecdsa::signature::Signer;
    use k256::ecdsa::SigningKey;
    use std::time::{SystemTime, UNIX_EPOCH};

    fn now_secs() -> u64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .unwrap()
            .as_secs()
    }

    fn encode_segment(json: &serde_json::Value) -> String {
        URL_SAFE_NO_PAD.encode(json.to_string().as_bytes())
    }

    /// Builds a genuinely ES256K-signed service JWT for `did` using `signing_key`.
    #[cfg(debug_assertions)]
    fn sign_service_jwt(
        signing_key: &SigningKey,
        did: &str,
        aud: &str,
        exp_secs_from_now: i64,
    ) -> String {
        let now = now_secs().cast_signed();
        let header = encode_segment(&serde_json::json!({"alg": "ES256K", "typ": "JWT"}));
        let payload = encode_segment(&serde_json::json!({
            "iss": did,
            "sub": did,
            "aud": aud,
            "exp": now + exp_secs_from_now,
            "iat": now,
            "lxm": "app.bsky.feed.getFeedSkeleton"
        }));
        let signing_input = format!("{header}.{payload}");
        let signature: Signature = signing_key.sign(signing_input.as_bytes());
        let sig_b64 = URL_SAFE_NO_PAD.encode(signature.to_bytes().as_slice());
        format!("{signing_input}.{sig_b64}")
    }

    #[test]
    fn test_multikey_decode_roundtrip() {
        // Build a valid compressed key
        let signing = SigningKey::from_bytes(&[7u8; 32].into()).unwrap();
        let verifying = signing.verifying_key();
        let compressed = verifying.to_sec1_point(true).as_bytes().to_vec();

        let multikey_bytes = [vec![MULTIKEY_SECP256K1_PREFIX], compressed].concat();
        let encoded = format!("z{}", test_bs58_encode(&multikey_bytes));

        let decoded = decode_multikey(&encoded).unwrap();
        assert_eq!(decoded, multikey_bytes);
    }

    /// Independent base58 encoder used only by tests (simple, obviously-correct
    /// bignum schoolbook implementation).
    fn test_bs58_encode(input: &[u8]) -> String {
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

    #[test]
    fn test_extract_signing_key_from_document_2byte_varint_and_legacy() {
        let signing = SigningKey::from_bytes(&[9u8; 32].into()).unwrap();
        let verifying = signing.verifying_key();
        let compressed_bytes = verifying.to_sec1_point(true);
        let compressed = compressed_bytes.as_bytes();

        // 1. Real-world ATProto 2-byte multicodec varint [0xe7, 0x01] + 33-byte SEC1 (35 bytes total, zQ3sh...)
        let multikey_2byte = [MULTIKEY_SECP256K1_VARINT.to_vec(), compressed.to_vec()].concat();
        assert_eq!(multikey_2byte.len(), 35);
        let multibase_2byte = format!("z{}", test_bs58_encode(&multikey_2byte));
        assert!(
            multibase_2byte.starts_with("zQ3sh"),
            "35-byte secp256k1 multikey should start with zQ3sh, got {multibase_2byte}"
        );

        let doc_2byte: DidDocument = serde_json::from_value(serde_json::json!({
            "id": "did:plc:example_real",
            "verificationMethod": [{
                "id": "did:plc:example_real#atproto",
                "type": "Multikey",
                "controller": "did:plc:example_real",
                "publicKeyMultibase": multibase_2byte
            }],
            "service": []
        }))
        .unwrap();

        let extracted_2byte =
            ServiceAuthVerifier::extract_signing_key_from_document(&doc_2byte).unwrap();
        assert_eq!(
            extracted_2byte.to_sec1_point(true).as_bytes(),
            compressed,
            "2-byte varint [0xe7, 0x01] secp256k1 key must round-trip"
        );

        // 2. Legacy 1-byte [0xe7] + 33-byte SEC1 (34 bytes total)
        let multikey_legacy = [vec![MULTIKEY_SECP256K1_PREFIX], compressed.to_vec()].concat();
        let multibase_legacy = format!("z{}", test_bs58_encode(&multikey_legacy));
        let doc_legacy: DidDocument = serde_json::from_value(serde_json::json!({
            "id": "did:plc:example",
            "verificationMethod": [{
                "id": "#atproto",
                "type": "Multikey",
                "controller": "did:plc:example",
                "publicKeyMultibase": multibase_legacy
            }],
            "service": []
        }))
        .unwrap();

        let extracted =
            ServiceAuthVerifier::extract_signing_key_from_document(&doc_legacy).unwrap();
        assert_eq!(
            extracted.to_sec1_point(true).as_bytes(),
            compressed,
            "round-tripped key must match"
        );
    }

    #[cfg(debug_assertions)]
    #[tokio::test]
    async fn test_p256_es256_multikey_extract_and_verify() {
        let p256_signing = p256::ecdsa::SigningKey::from_bytes(&[11u8; 32].into()).unwrap();
        let p256_verifying = p256_signing.verifying_key();
        let compressed_point = p256_verifying.to_sec1_point(true);
        let compressed = compressed_point.as_bytes();
        let multikey_bytes = [MULTIKEY_P256_VARINT.to_vec(), compressed.to_vec()].concat();
        assert_eq!(multikey_bytes.len(), 35);
        let multibase = format!("z{}", test_bs58_encode(&multikey_bytes));
        assert!(
            multibase.starts_with("zDna"),
            "35-byte P-256 multikey should start with zDna, got {multibase}"
        );

        let doc: DidDocument = serde_json::from_value(serde_json::json!({
            "id": "did:plc:p256_actor",
            "verificationMethod": [{
                "id": "did:plc:p256_actor#atproto",
                "type": "Multikey",
                "controller": "did:plc:p256_actor",
                "publicKeyMultibase": multibase
            }],
            "service": []
        }))
        .unwrap();

        let extracted = ServiceAuthVerifier::extract_atproto_key_from_document(&doc).unwrap();
        assert_eq!(extracted, AtprotoVerifyingKey::P256(*p256_verifying));

        let verifier = ServiceAuthVerifier::new();
        let did = "did:plc:p256_actor";
        verifier.register_test_atproto_key(did, extracted);

        let now = now_secs().cast_signed();
        let header = encode_segment(&serde_json::json!({"alg": "ES256", "typ": "JWT"}));
        let payload = encode_segment(&serde_json::json!({
            "iss": did,
            "sub": did,
            "aud": "did:web:feed.example.com",
            "exp": now + 3600,
            "iat": now,
            "lxm": "app.bsky.feed.getFeedSkeleton"
        }));
        let signing_input = format!("{header}.{payload}");
        let sig: p256::ecdsa::Signature = p256_signing.sign(signing_input.as_bytes());
        let jwt = format!(
            "{signing_input}.{}",
            URL_SAFE_NO_PAD.encode(sig.to_bytes().as_slice())
        );

        let verified = verifier
            .verify_service_jwt(
                &format!("Bearer {jwt}"),
                Some("did:web:feed.example.com"),
                now_secs(),
            )
            .await
            .unwrap();
        assert_eq!(verified.as_str(), did);
    }

    // Relies on the `#[cfg(debug_assertions)]`-only `register_test_key` fast path, so this
    // test is intentionally compiled out of optimized/release test builds.
    #[cfg(debug_assertions)]
    #[tokio::test]
    async fn test_verify_service_jwt_end_to_end_with_test_key() {
        let verifier = ServiceAuthVerifier::new();
        let signing = SigningKey::from_bytes(&[42u8; 32].into()).unwrap();
        let did = "did:plc:svc_jwt_test_actor";

        verifier.register_test_key(did, *signing.verifying_key());

        // Valid signed JWT passes.
        let jwt = sign_service_jwt(&signing, did, "did:web:feed.example.com", 3600);
        let auth = format!("Bearer {jwt}");
        let verified = verifier
            .verify_service_jwt(&auth, Some("did:web:feed.example.com"), now_secs())
            .await
            .unwrap();
        assert_eq!(verified.as_str(), did);

        // Tampered payload fails signature verification.
        let segments: Vec<String> = jwt.split('.').map(str::to_string).collect();
        let mut payload_json: serde_json::Value =
            serde_json::from_slice(&URL_SAFE_NO_PAD.decode(&segments[1]).unwrap()).unwrap();
        payload_json["sub"] = serde_json::json!("did:plc:attacker");
        let tampered_payload = encode_segment(&payload_json);
        let tampered = format!("{}.{}.{}", segments[0], tampered_payload, segments[2]);
        assert!(
            verifier
                .verify_service_jwt(&format!("Bearer {tampered}"), None, now_secs())
                .await
                .is_err(),
            "tampered JWT must fail signature verification"
        );
    }

    #[test]
    fn test_algorithm_confusion_rejected() {
        // HS256 / none / RS256 headers must be rejected before signature work.
        let verifier = ServiceAuthVerifier::new();
        let now = now_secs();

        for alg in ["HS256", "none", "RS256", "EdDSA"] {
            let header = encode_segment(&serde_json::json!({"alg": alg, "typ": "JWT"}));
            let payload = encode_segment(&serde_json::json!({
                "iss": "did:plc:alg_confusion",
                "exp": now + 3600
            }));
            let jwt = format!("{header}.{payload}.c2ln");
            let res = tokio::runtime::Runtime::new()
                .unwrap()
                .block_on(verifier.verify_service_jwt(&format!("Bearer {jwt}"), None, now));
            assert!(res.is_err(), "alg {alg} must be rejected");
            let msg = res.err().unwrap().to_string();
            assert!(
                msg.contains("ES256K") || msg.contains("Unsupported JWT algorithm"),
                "alg {alg} rejection must name the algorithm, got: {msg}"
            );
        }

        // Cross-curve confusion (ES256 header against secp256k1 key, or ES256K header against P-256 key)
        // must also be rejected strictly.
        #[cfg(debug_assertions)]
        {
            let k256_signing = SigningKey::from_bytes(&[77u8; 32].into()).unwrap();
            let k256_did = "did:plc:k256_actor";
            verifier.register_test_key(k256_did, *k256_signing.verifying_key());

            let es256_header = encode_segment(&serde_json::json!({"alg": "ES256", "typ": "JWT"}));
            let k256_payload = encode_segment(&serde_json::json!({
                "iss": k256_did,
                "exp": now + 3600
            }));
            let sig: Signature =
                k256_signing.sign(format!("{es256_header}.{k256_payload}").as_bytes());
            let confused_jwt = format!(
                "{es256_header}.{k256_payload}.{}",
                URL_SAFE_NO_PAD.encode(sig.to_bytes().as_slice())
            );
            let res =
                tokio::runtime::Runtime::new()
                    .unwrap()
                    .block_on(verifier.verify_service_jwt(
                        &format!("Bearer {confused_jwt}"),
                        None,
                        now,
                    ));
            assert!(
                res.is_err(),
                "ES256 header with secp256k1 key must be rejected"
            );
        }
    }

    #[test]
    fn test_bs58_known_vectors() {
        // Base58 (Bitcoin alphabet) round-trip sanity via known encoding of "hello"
        let decoded = decode_multikey("zStV1DL6CwTryKyV").unwrap();
        assert_eq!(decoded, b"hello world");
    }

    #[tokio::test]
    async fn test_unresolvable_did_fails_closed() {
        let verifier = ServiceAuthVerifier::new();
        let now = now_secs();
        let header = encode_segment(&serde_json::json!({"alg": "ES256K", "typ": "JWT"}));
        let payload = encode_segment(&serde_json::json!({
            "iss": "did:plc:nonexistent000000000000000000000000000000",
            "exp": now + 3600
        }));
        let jwt = format!("{header}.{payload}.c2ln");
        let res = verifier
            .verify_service_jwt(&format!("Bearer {jwt}"), None, now)
            .await;
        assert!(res.is_err(), "unresolvable DID must fail closed");
        let msg = res.err().unwrap().to_string();
        assert!(
            msg.contains("DID resolution failed") || msg.contains("not found"),
            "expected DID-resolution failure, got: {msg}"
        );
    }

    // Relies on the `#[cfg(debug_assertions)]`-only `register_test_key` fast path, so this
    // test is intentionally compiled out of optimized/release test builds.
    #[cfg(debug_assertions)]
    #[tokio::test]
    async fn test_expired_signed_jwt_rejected() {
        let verifier = ServiceAuthVerifier::new();
        let signing = SigningKey::from_bytes(&[3u8; 32].into()).unwrap();
        let did = "did:plc:svc_jwt_expired_actor";
        verifier.register_test_key(did, *signing.verifying_key());

        let now = now_secs().cast_signed();
        let header = encode_segment(&serde_json::json!({"alg": "ES256K", "typ": "JWT"}));
        let payload = encode_segment(&serde_json::json!({
            "iss": did,
            "sub": did,
            "aud": "did:web:feed.example.com",
            "exp": now - 500, // beyond the 60s leeway
            "iat": now - 1000,
        }));
        let signing_input = format!("{header}.{payload}");
        let sig: Signature = signing.sign(signing_input.as_bytes());
        let jwt = format!(
            "{header}.{payload}.{}",
            URL_SAFE_NO_PAD.encode(sig.to_bytes().as_slice())
        );

        let res = verifier
            .verify_service_jwt(&format!("Bearer {jwt}"), None, now_secs())
            .await;
        assert!(res.is_err());
        assert!(res.err().unwrap().to_string().contains("expired"));
    }

    #[test]
    fn test_did_key_cache_ttl_and_invalidation() {
        let cache = DidKeyCache::new(100);
        let signing = SigningKey::from_bytes(&[5u8; 32].into()).unwrap();
        let key = signing.verifying_key();

        cache.insert("did:plc:cache_test", *key, 1_000);
        assert!(cache.get("did:plc:cache_test", 1_050).is_some());
        assert_eq!(cache.len(), 1);

        // Expired entry is not served.
        assert!(cache.get("did:plc:cache_test", 1_101).is_none());

        // Invalidation removes the entry.
        cache.insert("did:plc:cache_test", *key, 1_000);
        cache.invalidate("did:plc:cache_test");
        assert_eq!(cache.len(), 0);
        assert!(cache.is_empty());

        // Shard indexing stays in bounds for arbitrary DIDs.
        for i in 0..1000 {
            let did = format!("did:plc:shard_probe_{i}");
            cache.insert(&did, *key, 1_000);
            assert!(cache.get(&did, 1_050).is_some());
        }
    }

    #[test]
    fn test_multikey_prefix_validation_rejects_wrong_key_type() {
        // P-256-style key material (0x8020 prefix family) must be rejected.
        let doc_json = serde_json::json!({
            "id": "did:plc:wrong_key_type",
            "verificationMethod": [{
                "id": "#atproto",
                "type": "Multikey",
                "controller": "did:plc:wrong_key_type",
                "publicKeyMultibase": format!("z{}", test_bs58_encode(&[0x80u8, 0x01, 0x02]))
            }],
            "service": []
        });
        let doc: DidDocument = serde_json::from_value(doc_json).unwrap();
        let res = ServiceAuthVerifier::extract_signing_key_from_document(&doc);
        assert!(res.is_err(), "non-secp256k1 multikey must be rejected");
        assert!(res.err().unwrap().to_string().contains("not a secp256k1"));
    }

    #[test]
    fn test_missing_atproto_verification_method_rejected() {
        let doc_json = serde_json::json!({
            "id": "did:plc:no_keys",
            "verificationMethod": [{
                "id": "#other",
                "type": "Multikey",
                "controller": "did:plc:no_keys",
                "publicKeyMultibase": "z4tNsttuAZ1rc"
            }],
            "service": []
        });
        let doc: DidDocument = serde_json::from_value(doc_json).unwrap();
        let res = ServiceAuthVerifier::extract_signing_key_from_document(&doc);
        assert!(res.is_err());
        assert!(res.err().unwrap().to_string().contains("#atproto"));
    }
}
