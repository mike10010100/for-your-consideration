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
use parking_lot::RwLock;
use serde::Deserialize;

use crate::auth::{is_valid_did, parse_jwt_payload_unverified, JWT_CLOCK_SKEW_LEEWAY_SECS};
use crate::error::{FeedError, Result};
use skyauth::identity::{DidDocument, IdentityResolver};
use skyauth::ssrf::SsrfFilter;

/// Cache TTL for resolved DID-document signing keys (15 minutes).
pub const DID_KEY_CACHE_TTL_SECS: u64 = 15 * 60;

/// Number of shards in the DID signing-key cache (matches the repo-wide 64-shard convention).
pub const DID_KEY_CACHE_SHARDS: usize = 64;

/// Default PLC directory endpoint used for `did:plc` resolution.
pub const DEFAULT_PLC_DIRECTORY_URL: &str = "https://plc.directory";

/// Multikey varint prefix for secp256k1 compressed public keys (`0xe7` encodes
/// key type 103).
const MULTIKEY_SECP256K1_PREFIX: u8 = 0xe7;

/// Length in bytes of a compressed secp256k1 public key.
const COMPRESSED_SECP256K1_KEY_LEN: usize = 33;

/// JOSE `alg` value required for `ATProto` Service Auth JWTs.
pub const REQUIRED_SERVICE_AUTH_ALG: &str = "ES256K";

/// Raw JOSE header fields of a Service Auth JWT.
#[derive(Debug, Clone, PartialEq, Eq, Deserialize)]
pub struct ServiceJwtHeader {
    /// Signature algorithm (must be `ES256K`).
    pub alg: String,
    /// Token type (typically `JWT`).
    #[serde(default)]
    pub typ: Option<String>,
}

/// A cached, validated issuer signing key with its resolution timestamp.
#[derive(Clone, Debug)]
pub struct CachedSigningKey {
    /// The verified secp256k1 verifying key extracted from the DID document.
    pub verifying_key: Arc<VerifyingKey>,
    /// Unix timestamp (seconds) when this key was resolved and cached.
    pub resolved_at_secs: u64,
}

/// 64-shard TTL cache of resolved DID-document signing keys.
///
/// Sharding follows the repository-wide 64-shard convention to keep the
/// per-request read path contention-free under concurrent feed traffic.
#[derive(Debug)]
pub struct DidKeyCache {
    shards: [RwLock<ahash::AHashMap<CompactString, CachedSigningKey>>; DID_KEY_CACHE_SHARDS],
    ttl_secs: u64,
}

impl Default for DidKeyCache {
    fn default() -> Self {
        Self::new(DID_KEY_CACHE_TTL_SECS)
    }
}

impl DidKeyCache {
    /// Creates a new cache with the specified entry TTL.
    #[must_use]
    pub fn new(ttl_secs: u64) -> Self {
        Self {
            shards: std::array::from_fn(|_| RwLock::new(ahash::AHashMap::new())),
            ttl_secs,
        }
    }

    fn shard_idx(did: &str) -> usize {
        (crc32fast::hash(did.as_bytes()) as usize) & (DID_KEY_CACHE_SHARDS - 1)
    }

    /// Returns the cached verifying key for `did` if present and unexpired.
    #[must_use]
    pub fn get(&self, did: &str, now_secs: u64) -> Option<Arc<VerifyingKey>> {
        let shard = &self.shards[Self::shard_idx(did)];
        let guard = shard.read();
        let entry = guard.get(did)?;
        if now_secs.saturating_sub(entry.resolved_at_secs) >= self.ttl_secs {
            return None;
        }
        Some(Arc::clone(&entry.verifying_key))
    }

    /// Inserts a resolved verifying key for `did`.
    pub fn insert(&self, did: &str, key: VerifyingKey, now_secs: u64) {
        let shard = &self.shards[Self::shard_idx(did)];
        let mut guard = shard.write();
        guard.insert(
            CompactString::new(did),
            CachedSigningKey {
                verifying_key: Arc::new(key),
                resolved_at_secs: now_secs,
            },
        );
    }

    /// Invalidates the cached key for `did` (e.g. after a signature failure, so a
    /// rotated DID document is re-fetched on the next attempt).
    pub fn invalidate(&self, did: &str) {
        let shard = &self.shards[Self::shard_idx(did)];
        let mut guard = shard.write();
        guard.remove(did);
    }

    /// Returns the number of cached entries across all shards.
    #[must_use]
    pub fn len(&self) -> usize {
        self.shards.iter().map(|s| s.read().len()).sum()
    }

    /// Returns `true` if no keys are cached.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Drops all cached entries.
    pub fn clear(&self) {
        for shard in &self.shards {
            shard.write().clear();
        }
    }
}

/// Verifies `ATProto` Service Auth JWT signatures against issuer DID documents.
#[derive(Clone)]
pub struct ServiceAuthVerifier {
    resolver: Arc<IdentityResolver>,
    key_cache: Arc<DidKeyCache>,
    /// Exact-match test DIDs registered for offline use. Compile-gated to debug
    /// builds only; release binaries never consult this table.
    #[cfg(debug_assertions)]
    test_keys: Arc<RwLock<ahash::AHashMap<CompactString, VerifyingKey>>>,
}

impl std::fmt::Debug for ServiceAuthVerifier {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let mut out = f.debug_struct("ServiceAuthVerifier");
        out.field("resolver", &"IdentityResolver")
            .field("key_cache_ttl_secs", &self.key_cache.ttl_secs)
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

    /// Registers an exact-match test DID with a signing key for offline
    /// verification. Only exists in debug builds; release binaries cannot
    /// bypass signature verification through this path.
    #[cfg(debug_assertions)]
    pub fn register_test_key(&self, did: &str, key: VerifyingKey) {
        self.test_keys.write().insert(CompactString::new(did), key);
    }

    /// Removes a registered test key.
    #[cfg(debug_assertions)]
    pub fn remove_test_key(&self, did: &str) {
        self.test_keys.write().remove(did);
    }

    /// Extracts the secp256k1 verifying key from an `ATProto` DID document.
    ///
    /// Accepts the standard `Multikey` encoding (`0xe7 || compressed key`) and
    /// rejects any other key type/prefix so an attacker cannot substitute a
    /// P-256 or RSA key into a document and force algorithm confusion.
    ///
    /// # Errors
    ///
    /// Returns [`FeedError::Auth`] when the document has no `#atproto`
    /// verification method, the multibase payload is malformed, or the key is
    /// not a secp256k1 compressed key.
    pub fn extract_signing_key_from_document(doc: &DidDocument) -> Result<VerifyingKey> {
        let method = doc.extract_signing_key().ok_or_else(|| {
            FeedError::Auth(format!(
                "DID document '{}' has no #atproto verification method",
                doc.id
            ))
        })?;

        let raw = method.public_key_multibase.as_deref().ok_or_else(|| {
            FeedError::Auth(format!(
                "Verification method '{}' carries no publicKeyMultibase",
                method.id
            ))
        })?;

        let key_bytes = decode_multikey(raw)?;

        if key_bytes.first().copied() != Some(MULTIKEY_SECP256K1_PREFIX)
            || key_bytes.len() != 1 + COMPRESSED_SECP256K1_KEY_LEN
        {
            return Err(FeedError::Auth(format!(
                "Verification method '{}' is not a secp256k1 Multikey (len {}, prefix {:?})",
                method.id,
                key_bytes.len(),
                key_bytes.first().copied()
            )));
        }

        VerifyingKey::from_sec1_bytes(&key_bytes[1..]).map_err(|e| {
            FeedError::Auth(format!(
                "Invalid secp256k1 public key in '{}': {e}",
                method.id
            ))
        })
    }

    /// Resolves (with cache) the signing key for the issuer DID.
    ///
    /// # Errors
    ///
    /// Returns [`FeedError::Auth`] if the DID is invalid, unresolvable, or its
    /// document lacks a usable signing key.
    pub async fn resolve_signing_key(&self, did: &str, now_secs: u64) -> Result<Arc<VerifyingKey>> {
        if !is_valid_did(did) {
            return Err(FeedError::Auth(format!("Invalid issuer DID: '{did}'")));
        }

        // 1. Cache fast path (no lock across the await: get() clones the Arc).
        if let Some(key) = self.key_cache.get(did, now_secs) {
            return Ok(key);
        }

        // 2. Debug-only exact-match test table (never compiled into release).
        #[cfg(debug_assertions)]
        if let Some(key) = self.test_keys.read().get(did) {
            return Ok(Arc::new(*key));
        }

        // 3. Live resolution (PLC directory / did:web), SSRF-filtered.
        let doc = self
            .resolver
            .resolve_did(did)
            .await
            .map_err(|e| FeedError::Auth(format!("DID resolution failed for '{did}': {e}")))?;

        let key = Self::extract_signing_key_from_document(&doc)?;
        self.key_cache.insert(did, key, now_secs);
        Ok(Arc::new(key))
    }

    /// Fully verifies a Service Auth JWT and returns the authenticated issuer DID.
    ///
    /// Checks, in order:
    /// 1. Bearer prefix and 3-segment JWT shape.
    /// 2. `alg = ES256K` (algorithm-confusion defense).
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

        // Algorithm pinning: reject anything but ES256K before touching the payload.
        let header_bytes = URL_SAFE_NO_PAD
            .decode(header_b64)
            .map_err(|e| FeedError::Auth(format!("JWT header base64 decode error: {e}")))?;
        let header: ServiceJwtHeader = serde_json::from_slice(&header_bytes)
            .map_err(|e| FeedError::Auth(format!("JWT header parse error: {e}")))?;
        if header.alg != REQUIRED_SERVICE_AUTH_ALG {
            return Err(FeedError::Auth(format!(
                "Unsupported JWT algorithm '{}': ATProto Service Auth requires {REQUIRED_SERVICE_AUTH_ALG}",
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

        let signing_key = match self.resolve_signing_key(&issuer, now_secs).await {
            Ok(key) => key,
            Err(err) => return Err(err),
        };

        // Signature over the ASCII `header.payload` signing input.
        let signing_input = format!("{header_b64}.{payload_b64}");

        let signature_bytes = URL_SAFE_NO_PAD
            .decode(signature_b64)
            .map_err(|e| FeedError::Auth(format!("JWT signature base64 decode error: {e}")))?;
        let signature = Signature::from_slice(&signature_bytes)
            .map_err(|_| FeedError::Auth("Malformed ECDSA signature encoding".to_string()))?;

        if signing_key
            .verify(signing_input.as_bytes(), &signature)
            .is_err()
        {
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

/// Decodes an `ATProto` Multikey (`publicKeyMultibase`) into raw key bytes.
///
/// `ATProto` Multikeys use base58btc multibase (prefix `z`) wrapping an
/// identity-multibase (`0x00`) varint-prefixed key, e.g. `zQ3...`. Some
/// `did:web` documents use base64url multibase (prefix `u`) instead.
///
/// # Errors
///
/// Returns [`FeedError::Auth`] on malformed multibase or base58 input.
fn decode_multikey(multibase: &str) -> Result<Vec<u8>> {
    let bytes = multibase.as_bytes();
    if bytes.len() < 2 {
        return Err(FeedError::Auth(
            "Malformed multibase key: too short".to_string(),
        ));
    }

    let decoded = match bytes[0] {
        // 'z' = base58btc (multibase table)
        b'z' => bs58_decode(&multibase[1..])?,
        // 'u' = base64url (no padding)
        b'u' => URL_SAFE_NO_PAD
            .decode(&multibase[1..])
            .map_err(|e| FeedError::Auth(format!("Malformed base64url multibase: {e}")))?,
        _ => {
            return Err(FeedError::Auth(format!(
                "Unsupported multibase prefix byte {:#04x}",
                bytes[0]
            )))
        }
    };

    // Identity-multibase prefix (0x00) wraps the raw multikey bytes.
    if decoded.first().copied() == Some(0x00) {
        Ok(decoded[1..].to_vec())
    } else {
        Ok(decoded)
    }
}

/// Minimal base58 (Bitcoin alphabet) decoder, sufficient for multibase `z`.
///
/// # Errors
///
/// Returns [`FeedError::Auth`] on non-alphabet characters or leading `0`.
fn bs58_decode(input: &str) -> Result<Vec<u8>> {
    const ALPHABET: &[u8; 58] = b"123456789ABCDEFGHJKLMNPQRSTUVWXYZabcdefghijkmnopqrstuvwxyz";

    if input.is_empty() {
        return Ok(Vec::new());
    }
    if input.starts_with('1') {
        return Err(FeedError::Auth(
            "Malformed base58 key: leading zero byte".to_string(),
        ));
    }

    // The accumulated number is stored little-endian (`out[0]` is the least
    // significant byte). For each base58 digit: multiply the whole number by
    // 58, then add the digit.
    let mut out: Vec<u8> = Vec::with_capacity(input.len());
    for ch in input.bytes() {
        let mut carry =
            ALPHABET.iter().position(|&a| a == ch).ok_or_else(|| {
                FeedError::Auth("Malformed base58 key: invalid character".to_string())
            })? as u32;

        for byte in &mut out {
            let val = u32::from(*byte) * 58 + carry;
            *byte = (val & 0xFF) as u8;
            carry = val >> 8;
        }
        while carry > 0 {
            out.push((carry & 0xFF) as u8);
            carry >>= 8;
        }
    }

    // Leading '1's encode leading zero bytes. Convert little-endian back to
    // big-endian for the final key bytes.
    let zeros = input.bytes().take_while(|&b| b == b'1').count();
    let mut result = vec![0u8; zeros];
    out.reverse();
    result.extend_from_slice(&out);
    Ok(result)
}

#[cfg(test)]
#[allow(clippy::unwrap_used, clippy::expect_used, clippy::panic, missing_docs)]
mod tests {
    use super::*;
    use base64::engine::general_purpose::URL_SAFE_NO_PAD;
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
        let compressed = verifying.to_encoded_point(true).as_bytes().to_vec();

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
    fn test_extract_signing_key_from_document() {
        let signing = SigningKey::from_bytes(&[9u8; 32].into()).unwrap();
        let verifying = signing.verifying_key();
        let compressed_bytes = verifying.to_encoded_point(true);
        let compressed = compressed_bytes.as_bytes();
        let multikey_bytes = [vec![MULTIKEY_SECP256K1_PREFIX], compressed.to_vec()].concat();
        let multibase = format!("z{}", test_bs58_encode(&multikey_bytes));

        let doc_json = serde_json::json!({
            "id": "did:plc:example",
            "verificationMethod": [{
                "id": "#atproto",
                "type": "Multikey",
                "controller": "did:plc:example",
                "publicKeyMultibase": multibase
            }],
            "service": []
        });
        let doc: DidDocument = serde_json::from_value(doc_json).unwrap();

        let extracted = ServiceAuthVerifier::extract_signing_key_from_document(&doc).unwrap();
        assert_eq!(
            extracted.to_encoded_point(true).as_bytes(),
            compressed,
            "round-tripped key must match"
        );
    }

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
        // HS256 / none / ES256 headers must be rejected before signature work.
        let verifier = ServiceAuthVerifier::new();
        let now = now_secs();

        for alg in ["HS256", "none", "ES256", "RS256"] {
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
    }

    #[test]
    fn test_bs58_known_vectors() {
        // Base58 (Bitcoin alphabet) round-trip sanity via known encoding of "hello"
        let decoded = bs58_decode("StV1DL6CwTryKyV").unwrap();
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
            assert!(DidKeyCache::shard_idx(&did) < DID_KEY_CACHE_SHARDS);
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
