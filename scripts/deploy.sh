#!/usr/bin/env bash
# ==============================================================================
# Script: deploy.sh
# Purpose: Auto-extracts version from Cargo.toml, builds version-tagged & latest
#          Docker images, and launches the production feed-engine stack.
# ==============================================================================

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
cd "${ROOT_DIR}"

# Locate docker compose command
COMPOSE_CMD=""
if docker compose version >/dev/null 2>&1; then
    COMPOSE_CMD="docker compose"
elif command -v docker-compose >/dev/null 2>&1; then
    COMPOSE_CMD="docker-compose"
elif [[ -x "/home/linuxbrew/.linuxbrew/bin/docker-compose" ]]; then
    COMPOSE_CMD="/home/linuxbrew/.linuxbrew/bin/docker-compose"
else
    echo "Error: Neither 'docker compose' nor 'docker-compose' found!" >&2
    exit 1
fi

# Extract SemVer version from Cargo.toml
if [[ ! -f "Cargo.toml" ]]; then
    echo "Error: Cargo.toml not found in ${ROOT_DIR}!" >&2
    exit 1
fi

VERSION=$(grep -m1 '^version\s*=' Cargo.toml | sed -E 's/version\s*=\s*"([^"]+)".*/\1/')
if [[ -z "${VERSION}" ]]; then
    echo "Error: Could not parse version from Cargo.toml!" >&2
    exit 1
fi

IMAGE_TAG="v${VERSION}"
echo "======================================================================"
echo "🚀 Deploying For Your Consideration (${IMAGE_TAG})"
echo "======================================================================"

# Pull updated base or sidecar images if available
${COMPOSE_CMD} pull cloudflared || true

# Build versioned image
export IMAGE_TAG
${COMPOSE_CMD} build feed-engine

# Also maintain 'latest' tag pointing to the new versioned build
if docker image inspect "for-your-consideration:${IMAGE_TAG}" >/dev/null 2>&1; then
    docker tag "for-your-consideration:${IMAGE_TAG}" "for-your-consideration:latest" || true
fi

# Start the stack
${COMPOSE_CMD} up -d

# Wait for feed-engine to hydrate snapshot and become healthy
echo "⏳ Waiting for feed engine to hydrate snapshot and pass healthcheck..."
CONTAINER_NAME="for-your-consideration"
MAX_WAIT_SECS=180
ELAPSED=0
HEALTH_STATUS="unknown"

while [[ ${ELAPSED} -lt ${MAX_WAIT_SECS} ]]; do
    HEALTH_STATUS=$(docker inspect --format='{{if .State.Health}}{{.State.Health.Status}}{{else}}{{.State.Status}}{{end}}' "${CONTAINER_NAME}" 2>/dev/null || echo "starting")
    if [[ "${HEALTH_STATUS}" == "healthy" ]]; then
        echo ""
        echo "✅ Feed engine is healthy and accepting traffic (took ${ELAPSED}s)."
        break
    fi
    sleep 3
    ELAPSED=$((ELAPSED + 3))
    echo -n "."
done

if [[ "${HEALTH_STATUS}" != "healthy" ]]; then
    echo ""
    echo "❌ Error: Container '${CONTAINER_NAME}' did not report healthy within ${MAX_WAIT_SECS}s (status: ${HEALTH_STATUS})." >&2
    echo "Check logs: docker logs ${CONTAINER_NAME}" >&2
    exit 1
fi

echo "----------------------------------------------------------------------"
echo "✅ Deployed for-your-consideration:${IMAGE_TAG} (and latest) successfully!"
echo "======================================================================"
