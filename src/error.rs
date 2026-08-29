use thiserror::Error;

/// Root domain error type for the `for-your-consideration` engine.
#[derive(Debug, Error)]
pub enum FeedError {
    /// String interner error
    #[error("Interner error: {0}")]
    Interner(String),

    /// Graph store operation failure
    #[error("Graph store error: {0}")]
    Graph(String),

    /// Snapshot persistence or hydration failure
    #[error("Snapshot error: {0}")]
    Snapshot(String),

    /// Invalid input or configuration parameter
    #[error("Invalid input: {0}")]
    InvalidInput(String),

    /// Ingestion stream failure
    #[error("Ingestion error: {0}")]
    Ingest(String),

    /// Authentication / JWT verification failure
    #[error("Authentication error: {0}")]
    Auth(String),

    /// Server configuration or execution error
    #[error("Server error: {0}")]
    Server(String),

    /// Serialization / deserialization failure
    #[error("Serialization error: {0}")]
    Serialization(#[from] serde_json::Error),

    /// Requested user / DID not found
    #[error("User not found: {0}")]
    UserNotFound(String),

    /// Requested post URI not found
    #[error("Post not found: {0}")]
    PostNotFound(String),

    /// Standard I/O failure
    #[error("I/O error: {0}")]
    Io(#[from] std::io::Error),

    /// AT Protocol OAuth error
    #[error("OAuth error: {0}")]
    OAuth(#[from] skyauth::error::AtprotoOAuthError),

    /// RFC 9449 `DPoP` proof error
    #[error("DPoP error: {0}")]
    DPoP(#[from] skyauth::error::DPoPError),

    /// RFC 7636 `PKCE` verification error
    #[error("PKCE error: {0}")]
    Pkce(#[from] skyauth::error::PkceError),

    /// Cryptographic operation error
    #[error("Crypto error: {0}")]
    Crypto(#[from] skyauth::error::CryptoError),

    /// Framework integration error
    #[error("Integration error: {0}")]
    Integration(#[from] skyauth::error::IntegrationError),
}

/// Convenience alias for operations returning `FeedError`.
pub type Result<T> = std::result::Result<T, FeedError>;
