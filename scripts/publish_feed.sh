#!/usr/bin/env bash
set -euo pipefail

# ==============================================================================
# Publish "For Your Consideration" Feed Generator Record to Bluesky / ATProto
# ==============================================================================
# This script authenticates with your Bluesky account using an App Password,
# creates the app.bsky.feed.generator record, and publishes it to the network.
#
# Requirements: curl, jq
# Usage:
#   BSKY_HANDLE="your-handle.bsky.social" \
#   BSKY_PASSWORD="your-app-password" \
#   FEED_HOSTNAME="feed.yourdomain.com" \
#   ./scripts/publish_feed.sh
# ==============================================================================

BSKY_HANDLE="${BSKY_HANDLE:-}"
FEED_RKEY="${FEED_RKEY:-for-your-consideration}"
FEED_DISPLAY_NAME="${FEED_DISPLAY_NAME:-For Your Consideration}"
FEED_HOSTNAME="${FEED_HOSTNAME:-fyc.mike10010100.com}"
FEED_DESCRIPTION="${FEED_DESCRIPTION:-Personalized algorithmic recommendation feed engine powered by multi-signal graph collaborative filtering, anti-fatigue decay, and serendipity exploration (homage to For You). Customize your algorithm dials at https://${FEED_HOSTNAME}/dashboard}"
PDS_URL="${PDS_URL:-https://bsky.social}"

if [[ -z "${BSKY_HANDLE:-}" ]]; then
  echo "Error: BSKY_HANDLE environment variable is required (e.g. BSKY_HANDLE=\"your-handle.bsky.social\")." >&2
  exit 1
fi

if [[ -z "${BSKY_PASSWORD:-}" ]]; then
  echo "Error: BSKY_PASSWORD environment variable is required (use a Bluesky App Password)." >&2
  echo "Example: BSKY_HANDLE=\"your-handle.bsky.social\" BSKY_PASSWORD=\"xxxx-xxxx-xxxx-xxxx\" ./scripts/publish_feed.sh" >&2
  exit 1
fi

echo "=========================================================="
echo "Publishing Feed Generator to Bluesky"
echo "  Handle:       $BSKY_HANDLE"
echo "  Feed Name:    $FEED_DISPLAY_NAME"
echo "  Record Key:   $FEED_RKEY"
echo "  Service Host: https://$FEED_HOSTNAME"
echo "=========================================================="

# 1. Create Session / Authenticate
echo -n "Authenticating with $PDS_URL... "
SESSION_RESP=$(curl -s -X POST "$PDS_URL/xrpc/com.atproto.server.createSession" \
  -H "Content-Type: application/json" \
  -d "{\"identifier\": \"$BSKY_HANDLE\", \"password\": \"$BSKY_PASSWORD\"}")

JWT=$(echo "$SESSION_RESP" | jq -r '.accessJwt // empty')
USER_DID=$(echo "$SESSION_RESP" | jq -r '.did // empty')

if [[ -z "$JWT" || -z "$USER_DID" ]]; then
  echo "FAILED!"
  echo "Error response: $SESSION_RESP" >&2
  exit 1
fi
echo "OK (DID: $USER_DID)"

# 2. Derive Service DID (did:web:<hostname>)
SERVICE_DID="did:web:$FEED_HOSTNAME"
CREATED_AT=$(date -u +"%Y-%m-%dT%H:%M:%SZ")
AVATAR_PATH="${AVATAR_PATH:-}"

AVATAR_BLOB_JSON=""
if [[ -n "$AVATAR_PATH" && -f "$AVATAR_PATH" ]]; then
  echo -n "Uploading avatar blob ($AVATAR_PATH)... "
  MIME_TYPE="image/jpeg"
  if [[ "$AVATAR_PATH" == *.png ]]; then
    MIME_TYPE="image/png"
  fi
  UPLOAD_RESP=$(curl -s -X POST "$PDS_URL/xrpc/com.atproto.repo.uploadBlob" \
    -H "Authorization: Bearer $JWT" \
    -H "Content-Type: $MIME_TYPE" \
    --data-binary "@$AVATAR_PATH")
  BLOB_LINK=$(echo "$UPLOAD_RESP" | jq -r '.blob.ref."$link" // empty')
  if [[ -n "$BLOB_LINK" ]]; then
    echo "OK (Blob: $BLOB_LINK)"
    AVATAR_BLOB_JSON=$(echo "$UPLOAD_RESP" | jq '.blob')
  else
    echo "WARNING: Could not parse blob link, response: $UPLOAD_RESP" >&2
  fi
fi

if [[ -n "$AVATAR_BLOB_JSON" ]]; then
RECORD_PAYLOAD=$(cat <<EOF
{
  "\$type": "app.bsky.feed.generator",
  "did": "$SERVICE_DID",
  "displayName": "$FEED_DISPLAY_NAME",
  "description": "$FEED_DESCRIPTION",
  "avatar": $AVATAR_BLOB_JSON,
  "createdAt": "$CREATED_AT"
}
EOF
)
else
RECORD_PAYLOAD=$(cat <<EOF
{
  "\$type": "app.bsky.feed.generator",
  "did": "$SERVICE_DID",
  "displayName": "$FEED_DISPLAY_NAME",
  "description": "$FEED_DESCRIPTION",
  "createdAt": "$CREATED_AT"
}
EOF
)
fi

# 3. Put Record into AT Protocol Repo
echo -n "Publishing record at://$USER_DID/app.bsky.feed.generator/$FEED_RKEY... "
PUT_RESP=$(curl -s -X POST "$PDS_URL/xrpc/com.atproto.repo.putRecord" \
  -H "Authorization: Bearer $JWT" \
  -H "Content-Type: application/json" \
  -d "{
    \"repo\": \"$USER_DID\",
    \"collection\": \"app.bsky.feed.generator\",
    \"rkey\": \"$FEED_RKEY\",
    \"record\": $RECORD_PAYLOAD
  }")

RECORD_URI=$(echo "$PUT_RESP" | jq -r '.uri // empty')

if [[ -z "$RECORD_URI" ]]; then
  echo "FAILED!"
  echo "Error response: $PUT_RESP" >&2
  exit 1
fi

echo "SUCCESS! 🎉"
echo ""
echo "=========================================================="
echo "Feed Generator Published Successfully!"
echo "  AT-URI:      $RECORD_URI"
echo "  Share Link:  https://bsky.app/profile/$USER_DID/feed/$FEED_RKEY"
echo "=========================================================="
echo "Next step: Open the share link in your browser or mobile app to pin the feed!"
