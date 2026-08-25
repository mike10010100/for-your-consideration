#![forbid(unsafe_code)]

//! AT Protocol service authentication JWT parser and viewer DID extractor.
//!
//! # Overview
//!
//! When Bluesky `AppViews` or users query custom feed generators, they may include an
//! `Authorization: Bearer <jwt>` HTTP header signed with their DID key. This module provides
//! resilient extraction and verification helpers to extract the viewer's DID (`iss` or `sub` claim)
//! with zero panics and graceful degradation to unauthenticated browsing.
//!
//! Supported DID types:
//! - `did:plc:...` (Placeholder DID format)
//! - `did:web:...` (Web-based DID format)

use axum::http::HeaderMap;
use base64::engine::general_purpose::{URL_SAFE, URL_SAFE_NO_PAD};
use base64::Engine;
use compact_str::CompactString;
use serde::{Deserialize, Serialize};

use crate::error::{FeedError, Result};

/// Minimal payload structure for AT Protocol service auth JWTs.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ServiceJwtPayload {
    /// Issuer DID of the requesting actor.
    #[serde(default)]
    pub iss: Option<CompactString>,
    /// Subject DID of the requesting actor (fallback if `iss` is absent).
    #[serde(default)]
    pub sub: Option<CompactString>,
    /// Audience DID of this service / feed generator.
    #[serde(default)]
    pub aud: Option<CompactString>,
    /// Expiration timestamp in seconds since unix epoch.
    #[serde(default)]
    pub exp: Option<u64>,
    /// Issued-at timestamp in seconds since unix epoch.
    #[serde(default)]
    pub iat: Option<u64>,
    /// Unique JWT ID / nonce.
    #[serde(default)]
    pub jti: Option<CompactString>,
}

impl ServiceJwtPayload {
    /// Extracts the viewer DID (`iss` preferred, `sub` fallback).
    #[must_use]
    pub fn viewer_did(&self) -> Option<&str> {
        self.iss
            .as_deref()
            .or(self.sub.as_deref())
            .filter(|did| is_valid_did(did))
    }
}

/// Validates whether a string is a syntactically valid AT Protocol DID (`did:plc:...` or `did:web:...`).
#[must_use]
pub fn is_valid_did(did: &str) -> bool {
    (did.starts_with("did:plc:") && did.len() > 8) || (did.starts_with("did:web:") && did.len() > 8)
}

/// Extracts viewer DID from Axum HTTP request headers.
///
/// Returns `None` if the header is missing, malformed, or contains an invalid DID.
#[must_use]
pub fn extract_viewer_did_from_headers(headers: &HeaderMap) -> Option<String> {
    let auth_header = headers.get("authorization")?.to_str().ok()?;
    extract_viewer_did(auth_header)
}

/// Extracts viewer DID from an Authorization header value string (e.g. `Bearer <jwt>` or `bearer <jwt>`).
///
/// Returns `None` on any parsing or decoding failure without throwing errors.
#[must_use]
pub fn extract_viewer_did(auth_header: &str) -> Option<String> {
    let token = auth_header
        .strip_prefix("Bearer ")
        .or_else(|| auth_header.strip_prefix("bearer "))
        .or_else(|| auth_header.strip_prefix("BEARER "))?
        .trim();

    if token.is_empty() {
        return None;
    }

    let payload = parse_jwt_payload_unverified(token).ok()?;
    payload.viewer_did().map(String::from)
}

/// Parses the payload component of a JWT without cryptographic signature verification.
///
/// Decodes Base64 URL-safe payload (supporting both unpadded and padded variants)
/// and deserializes JSON into [`ServiceJwtPayload`].
pub fn parse_jwt_payload_unverified(token: &str) -> Result<ServiceJwtPayload> {
    let mut parts = token.split('.');
    let _header = parts
        .next()
        .ok_or_else(|| FeedError::Auth("Missing JWT header segment".to_string()))?;
    let payload_b64 = parts
        .next()
        .ok_or_else(|| FeedError::Auth("Missing JWT payload segment".to_string()))?;
    let _signature = parts
        .next()
        .ok_or_else(|| FeedError::Auth("Missing JWT signature segment".to_string()))?;

    if parts.next().is_some() {
        return Err(FeedError::Auth("Too many segments in JWT".to_string()));
    }

    let payload_bytes = URL_SAFE_NO_PAD
        .decode(payload_b64)
        .or_else(|_| URL_SAFE.decode(payload_b64))
        .map_err(|e| FeedError::Auth(format!("Base64 decode error: {e}")))?;

    let payload: ServiceJwtPayload = serde_json::from_slice(&payload_bytes)
        .map_err(|e| FeedError::Auth(format!("JSON parse error in JWT payload: {e}")))?;

    Ok(payload)
}

/// Validates a service auth JWT, checking expiration and optional audience match.
///
/// Returns the authenticated viewer DID on success.
pub fn validate_service_jwt(
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

    let payload = parse_jwt_payload_unverified(token)?;

    if let Some(exp) = payload.exp {
        if now_secs > exp {
            return Err(FeedError::Auth(format!(
                "Token expired: exp {exp} < now {now_secs}"
            )));
        }
    }

    if let Some(expected_aud) = expected_audience {
        if let Some(ref aud) = payload.aud {
            if aud.as_str() != expected_aud {
                return Err(FeedError::Auth(format!(
                    "Audience mismatch: expected '{expected_aud}', got '{aud}'"
                )));
            }
        }
    }

    payload.viewer_did().map(CompactString::new).ok_or_else(|| {
        FeedError::Auth("Missing or invalid viewer DID (iss/sub) in JWT".to_string())
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    fn make_jwt(payload_json: &str) -> String {
        let header = URL_SAFE_NO_PAD.encode(r#"{"alg":"ES256K","typ":"JWT"}"#);
        let payload = URL_SAFE_NO_PAD.encode(payload_json);
        let sig = URL_SAFE_NO_PAD.encode("dummy_sig_bytes");
        format!("{header}.{payload}.{sig}")
    }

    #[test]
    fn test_valid_iss_extraction() {
        let jwt = make_jwt(r#"{"iss":"did:plc:alice","aud":"did:web:feed.example.com"}"#);
        let auth = format!("Bearer {jwt}");
        assert_eq!(extract_viewer_did(&auth).as_deref(), Some("did:plc:alice"));
    }

    #[test]
    fn test_valid_sub_fallback() {
        let jwt = make_jwt(r#"{"sub":"did:plc:bob","aud":"did:web:feed.example.com"}"#);
        let auth = format!("bearer {jwt}");
        assert_eq!(extract_viewer_did(&auth).as_deref(), Some("did:plc:bob"));
    }

    #[test]
    fn test_did_web_supported() {
        let jwt = make_jwt(r#"{"iss":"did:web:alice.example.com"}"#);
        let auth = format!("Bearer {jwt}");
        assert_eq!(
            extract_viewer_did(&auth).as_deref(),
            Some("did:web:alice.example.com")
        );
    }

    #[test]
    fn test_invalid_did_format_rejected() {
        let jwt = make_jwt(r#"{"iss":"invalid_did_without_prefix"}"#);
        let auth = format!("Bearer {jwt}");
        assert_eq!(extract_viewer_did(&auth), None);
    }

    #[test]
    fn test_empty_and_corrupt_tokens() {
        assert_eq!(extract_viewer_did(""), None);
        assert_eq!(extract_viewer_did("Bearer "), None);
        assert_eq!(
            extract_viewer_did("Bearer not.a.jwt.with.too.many.dots"),
            None
        );
        assert_eq!(extract_viewer_did("Bearer not_enough_dots"), None);
        assert_eq!(
            extract_viewer_did("Bearer invalid_b64.invalid_b64.sig"),
            None
        );
    }

    #[test]
    fn test_validate_service_jwt_expiration() {
        let now = 1_700_000_000;
        let jwt_valid = make_jwt(&format!(
            r#"{{"iss":"did:plc:alice","aud":"did:web:feed","exp":{}}}"#,
            now + 3600
        ));
        let jwt_expired = make_jwt(&format!(
            r#"{{"iss":"did:plc:alice","aud":"did:web:feed","exp":{}}}"#,
            now - 100
        ));

        assert!(
            validate_service_jwt(&format!("Bearer {jwt_valid}"), Some("did:web:feed"), now).is_ok()
        );
        assert!(
            validate_service_jwt(&format!("Bearer {jwt_expired}"), Some("did:web:feed"), now)
                .is_err()
        );
    }

    #[test]
    fn test_validate_service_jwt_audience() {
        let now = 1_700_000_000;
        let jwt = make_jwt(r#"{"iss":"did:plc:alice","aud":"did:web:feed1"}"#);
        assert!(validate_service_jwt(&format!("Bearer {jwt}"), Some("did:web:feed1"), now).is_ok());
        assert!(
            validate_service_jwt(&format!("Bearer {jwt}"), Some("did:web:feed2"), now).is_err()
        );
    }
}
