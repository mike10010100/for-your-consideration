#!/usr/bin/env bash
set -euo pipefail

# ==============================================================================
# Cloudflare Tunnel Setup & Automation for "For Your Consideration" Feed Engine
# ==============================================================================
# This script automates exposing your local feed engine to the internet via
# Cloudflare Tunnel with HTTPS support for Bluesky AppView verification.
#
# Supported Modes:
#   1. Quick Ephemeral Tunnel (Zero-config, instant *.trycloudflare.com URL)
#   2. Named Custom Domain Tunnel (Production setup for permanent server)
# ==============================================================================

LOCAL_PORT="${LOCAL_PORT:-3000}"
DEFAULT_DOMAIN="${DEFAULT_DOMAIN:-feed.mike10010100.com}"
TUNNEL_NAME="${TUNNEL_NAME:-fyc-feed}"

echo "=========================================================="
echo "  🌟 For Your Consideration — Cloudflare Tunnel Helper"
echo "=========================================================="

# 1. Verify cloudflared installation
if ! command -v cloudflared &>/dev/null; then
  echo "⚠️  'cloudflared' is not installed."
  echo ""
  if command -v brew &>/dev/null; then
    echo "Installing cloudflared via Homebrew..."
    brew install cloudflared
  else
    echo "Please install cloudflared from: https://developers.cloudflare.com/cloudflare-one/connections/connect-networks/downloads/"
    exit 1
  fi
fi

echo "✅ cloudflared is installed: $(cloudflared --version)"
echo ""

# 2. Select Mode
echo "Choose setup mode:"
echo "  1) Quick Ephemeral Tunnel (Instant *.trycloudflare.com for immediate testing)"
echo "  2) Production Named Tunnel (Route to your custom domain: e.g. $DEFAULT_DOMAIN)"
echo ""
read -r -p "Enter selection [1 or 2] (default: 1): " MODE_CHOICE
MODE_CHOICE="${MODE_CHOICE:-1}"

if [[ "$MODE_CHOICE" == "1" ]]; then
  echo ""
  echo "----------------------------------------------------------"
  echo "🚀 Launching Quick Ephemeral Tunnel to http://localhost:$LOCAL_PORT..."
  echo "----------------------------------------------------------"
  echo "Starting cloudflared tunnel in background..."
  
  LOG_FILE=$(mktemp /tmp/cloudflared_quick.XXXXXX.log)
  cloudflared tunnel --url "http://localhost:$LOCAL_PORT" > "$LOG_FILE" 2>&1 &
  TUNNEL_PID=$!

  # Trap exit to cleanup background process if script is terminated
  cleanup() {
    echo ""
    echo "Shutting down tunnel (PID: $TUNNEL_PID)..."
    kill "$TUNNEL_PID" 2>/dev/null || true
    rm -f "$LOG_FILE"
  }
  trap cleanup EXIT INT TERM

  # Wait for URL to appear in logs
  echo -n "Waiting for public HTTPS tunnel URL"
  PUBLIC_URL=""
  for i in {1..30}; do
    echo -n "."
    if grep -q "https://.*\.trycloudflare\.com" "$LOG_FILE"; then
      PUBLIC_URL=$(grep -o "https://[a-zA-Z0-9-]*\.trycloudflare\.com" "$LOG_FILE" | head -n 1)
      break
    fi
    sleep 1
  done
  echo ""

  if [[ -z "$PUBLIC_URL" ]]; then
    echo "❌ Failed to retrieve tunnel URL within 30s. Log contents:"
    cat "$LOG_FILE"
    exit 1
  fi

  HOSTNAME_ONLY="${PUBLIC_URL#https://}"

  echo ""
  echo "=========================================================="
  echo "🎉 Public Cloudflare Tunnel is LIVE!"
  echo "=========================================================="
  echo "  Public URL:   $PUBLIC_URL"
  echo "  Hostname:     $HOSTNAME_ONLY"
  echo "  Service DID:  did:web:$HOSTNAME_ONLY"
  echo "  Dashboard:    $PUBLIC_URL/dashboard"
  echo "  XRPC Health:  $PUBLIC_URL/healthz"
  echo "=========================================================="
  echo ""
  echo "To publish your feed to Bluesky right now with this tunnel URL, run:"
  echo "----------------------------------------------------------"
  echo "BSKY_HANDLE=\"mike10010100.com\" \\"
  echo "BSKY_PASSWORD=\"xxxx-xxxx-xxxx-xxxx\" \\"
  echo "FEED_HOSTNAME=\"$HOSTNAME_ONLY\" \\"
  echo "./scripts/publish_feed.sh"
  echo "----------------------------------------------------------"
  echo ""
  echo "Press Ctrl+C at any time to close this tunnel."
  
  # Keep alive until user interrupts
  wait "$TUNNEL_PID"

elif [[ "$MODE_CHOICE" == "2" ]]; then
  echo ""
  echo "----------------------------------------------------------"
  echo "🛠️  Production Named Tunnel Setup"
  echo "----------------------------------------------------------"
  read -r -p "Enter custom domain for feed [default: $DEFAULT_DOMAIN]: " USER_DOMAIN
  USER_DOMAIN="${USER_DOMAIN:-$DEFAULT_DOMAIN}"

  read -r -p "Enter tunnel name [default: $TUNNEL_NAME]: " USER_TUNNEL_NAME
  USER_TUNNEL_NAME="${USER_TUNNEL_NAME:-$TUNNEL_NAME}"

  echo ""
  echo "Step 1: Authenticate with Cloudflare (if not already logged in)..."
  cloudflared tunnel login

  echo ""
  echo "Step 2: Creating named tunnel '$USER_TUNNEL_NAME'..."
  cloudflared tunnel create "$USER_TUNNEL_NAME" || true

  echo ""
  echo "Step 3: Routing DNS record for $USER_DOMAIN..."
  cloudflared tunnel route dns "$USER_TUNNEL_NAME" "$USER_DOMAIN" || true

  CONFIG_DIR="$HOME/.cloudflared"
  CONFIG_PATH="$CONFIG_DIR/config.yml"
  mkdir -p "$CONFIG_DIR"

  # Find tunnel credentials JSON file
  CRED_FILE=$(find "$CONFIG_DIR" -name "*.json" | head -n 1)

  if [[ -n "$CRED_FILE" ]]; then
    TUNNEL_ID=$(basename "$CRED_FILE" .json)
    cat <<EOF > "$CONFIG_PATH"
tunnel: $TUNNEL_ID
credentials-file: $CRED_FILE

ingress:
  - hostname: $USER_DOMAIN
    service: http://localhost:$LOCAL_PORT
  - service: http_status:404
EOF
    echo "✅ Created Cloudflare Tunnel configuration at: $CONFIG_PATH"
  fi

  echo ""
  echo "=========================================================="
  echo "Production Tunnel Configured Successfully!"
  echo "=========================================================="
  echo "To run the tunnel on your server:"
  echo "  cloudflared tunnel run $USER_TUNNEL_NAME"
  echo ""
  echo "To install as a permanent system service (systemd / launchd):"
  echo "  sudo cloudflared service install"
  echo "=========================================================="
fi
