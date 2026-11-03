# ==============================================================================
# Multi-Stage Production Dockerfile for "For Your Consideration" Feed Engine
# ==============================================================================

# Stage 0: Normalize workspace version in manifests so version bumps (e.g. 0.6.9 -> 0.6.10)
# produce identical manifest checksums and preserve the compiled dependency layer cache.
FROM debian:bookworm-slim AS manifest-prep
WORKDIR /manifests
COPY Cargo.toml Cargo.lock ./
RUN sed -i '0,/^version = ".*"/s//version = "0.0.1"/' Cargo.toml && \
    sed -i '/^name = "for-your-consideration"$/{n;s/^version = ".*"/version = "0.0.1"/;}' Cargo.lock

# Stage 1: Build Release Binary
FROM rust:1-bookworm AS builder

WORKDIR /app

# Copy version-normalized dependency manifests
COPY --from=manifest-prep /manifests/Cargo.toml /manifests/Cargo.lock ./

# Create dummy source to pre-build dependencies for caching
RUN mkdir src benches && \
    echo "pub fn main() {}" > src/main.rs && \
    echo "" > src/lib.rs && \
    echo "fn main() {}" > benches/recommendation_latency.rs && \
    echo "fn main() {}" > benches/memory_footprint.rs && \
    cargo build --release --locked || true && \
    rm -rf src benches

# Copy real manifests, source code, and assets
COPY Cargo.toml Cargo.lock ./
COPY src ./src
COPY benches ./benches
COPY assets ./assets

# Build production release binary (clean dummy crate artifacts so real code is compiled)
RUN rm -rf target/release/deps/for_your_consideration* target/release/for-your-consideration* target/release/.fingerprint/for-your-consideration* target/release/.fingerprint/for_your_consideration* && \
    cargo build --release --locked --bin for-your-consideration && \
    strip target/release/for-your-consideration

# ==============================================================================
# Stage 2: Minimal Runtime Image
# ==============================================================================
FROM debian:bookworm-slim AS runtime

# Install CA certificates for secure WSS/HTTPS connections and curl for healthchecks,
# and explicitly distrust the October 2026 compromised eMudhra / Comodo AAA roots in the OS store.
RUN apt-get update && \
    apt-get install -y --no-install-recommends ca-certificates curl && \
    sed -i \
      -e 's|^mozilla/emSign_ECC_Root_CA_-_G3.crt|!&|' \
      -e 's|^mozilla/Comodo_AAA_Services_root.crt|!&|' \
      /etc/ca-certificates.conf && \
    update-ca-certificates && \
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
    SNAPSHOT_INTERVAL_SECS=14400 \
    SHUTDOWN_SAVE_TIMEOUT_SECS=240 \
    RUST_LOG=info,for_your_consideration=info

# Expose HTTP port
EXPOSE 3000

# Container healthcheck
HEALTHCHECK --interval=5s --timeout=10s --start-period=60s --retries=5 \
    CMD curl -f http://localhost:3000/healthz || exit 1

# Start feed engine
ENTRYPOINT ["/usr/local/bin/for-your-consideration"]
