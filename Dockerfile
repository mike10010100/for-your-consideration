# ==============================================================================
# Multi-Stage Production Dockerfile for "For Your Consideration" Feed Engine
# ==============================================================================

# Stage 1: Build Release Binary
FROM rust:1-bookworm AS builder

WORKDIR /app

# Copy dependency manifests
COPY Cargo.toml ./

# Create dummy source to pre-build dependencies for caching
RUN mkdir src benches && \
    echo "pub fn main() {}" > src/main.rs && \
    echo "" > src/lib.rs && \
    echo "fn main() {}" > benches/recommendation_latency.rs && \
    echo "fn main() {}" > benches/memory_footprint.rs && \
    cargo build --release || true && \
    rm -rf src benches

# Copy real source code and assets
COPY src ./src
COPY benches ./benches
COPY assets ./assets

# Build production release binary (clean dummy crate artifacts so real code is compiled)
RUN rm -rf target/release/deps/for_your_consideration* target/release/for-your-consideration* target/release/.fingerprint/for-your-consideration* target/release/.fingerprint/for_your_consideration* && \
    cargo build --release --bin for-your-consideration && \
    strip target/release/for-your-consideration

# ==============================================================================
# Stage 2: Minimal Runtime Image
# ==============================================================================
FROM debian:bookworm-slim AS runtime

# Install CA certificates for secure WSS/HTTPS connections and curl for healthchecks
RUN apt-get update && \
    apt-get install -y --no-install-recommends ca-certificates curl && \
    rm -rf /var/lib/apt/lists/*

# Create dedicated non-root user and group
RUN groupadd -g 10001 appgroup && \
    useradd -u 10001 -g appgroup -s /sbin/nologin -d /app appuser

# Create directories for app and persistent snapshot data
RUN mkdir -p /app /data && \
    chown -R appuser:appgroup /app /data

WORKDIR /app

# Copy stripped binary from builder
COPY --from=builder /app/target/release/for-your-consideration /usr/local/bin/for-your-consideration

# Switch to non-root user
USER appuser:appgroup

# Runtime environment defaults
ENV HOST=0.0.0.0 \
    PORT=3000 \
    SNAPSHOT_PATH=/data/snapshot.bin \
    SNAPSHOT_INTERVAL_SECS=300 \
    RUST_LOG=info,for_your_consideration=info

# Expose HTTP port
EXPOSE 3000

# Container healthcheck
HEALTHCHECK --interval=30s --timeout=5s --start-period=10s --retries=3 \
    CMD curl -f http://localhost:3000/healthz || exit 1

# Start feed engine
ENTRYPOINT ["/usr/local/bin/for-your-consideration"]
