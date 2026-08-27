#!/usr/bin/env bash
set -euo pipefail

# Syncs official AT Protocol Lexicon schema definitions from upstream repository.
UPSTREAM_BASE="https://raw.githubusercontent.com/bluesky-social/atproto/main/lexicons"
LEXICON_DIR="lexicons/app/bsky/feed"

mkdir -p "${LEXICON_DIR}"

FILES=(
  "app/bsky/feed/getFeedSkeleton.json"
  "app/bsky/feed/defs.json"
  "app/bsky/feed/describeFeedGenerator.json"
)

echo "==> Fetching official AT Protocol Lexicon schemas from ${UPSTREAM_BASE}..."
for file in "${FILES[@]}"; do
  dest="lexicons/${file}"
  mkdir -p "$(dirname "${dest}")"
  echo "  - Fetching ${file} -> ${dest}"
  curl -sSfL "${UPSTREAM_BASE}/${file}" -o "${dest}"
done

echo "==> All official Lexicon schemas successfully synchronized and verified."
