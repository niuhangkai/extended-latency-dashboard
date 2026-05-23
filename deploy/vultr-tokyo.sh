#!/usr/bin/env bash
set -euo pipefail

# Run this on the Vultr Tokyo node. The dashboard will mark it as cross-cloud.
REPO_URL="${REPO_URL:-git@github.com:niuhangkai/extended-latency-dashboard.git}"
APP_DIR="${APP_DIR:-/opt/exchange-latency-dashboard}"
APP_PORT="${APP_PORT:-8080}"
NODE_LABEL="${NODE_LABEL:-vultr-tokyo}"

EXTENDED_ENV="${EXTENDED_ENV:-testnet}"
EXTENDED_MARKET="${EXTENDED_MARKET:-BTC-USD}"
EXTENDED_API_KEY="${EXTENDED_API_KEY:-替换_EXTENDED_API_KEY}"
EXTENDED_STARK_PUBLIC_KEY="${EXTENDED_STARK_PUBLIC_KEY:-替换_EXTENDED_STARK_PUBLIC_KEY}"
EXTENDED_STARK_PRIVATE_KEY="${EXTENDED_STARK_PRIVATE_KEY:-替换_EXTENDED_STARK_PRIVATE_KEY}"
EXTENDED_VAULT="${EXTENDED_VAULT:-替换_EXTENDED_VAULT}"
EXTENDED_CLIENT_ID="${EXTENDED_CLIENT_ID:-}"

cd /tmp
export DEBIAN_FRONTEND=noninteractive
sudo apt update
sudo apt install -y ca-certificates curl git
if ! command -v docker >/dev/null 2>&1; then
  curl -fsSL https://get.docker.com | sudo sh
fi

PUBLIC_IP="$(curl -fsS https://api.ipify.org 2>/dev/null || true)"
PRIVATE_IP="$(hostname -I 2>/dev/null | awk '{print $1}' || true)"

sudo rm -rf "$APP_DIR"
sudo git clone "$REPO_URL" "$APP_DIR"
sudo chown -R "$(id -u):$(id -g)" "$APP_DIR"
cd "$APP_DIR"

cat > .env <<EOF
APP_PORT=$APP_PORT
EXCHANGE_REGION=$NODE_LABEL
EXCHANGE_CLOUD_PROVIDER=vultr
EXCHANGE_PRIVATE_IP=$PRIVATE_IP
EXCHANGE_PUBLIC_IP=$PUBLIC_IP
EXCHANGE_STREAMS=extended_rest,extended_bbo,extended_l2,extended_trades,extended_mark,extended_index,extended_order_test
EXCHANGE_REPORT_SECONDS=5
EXTENDED_ENV=$EXTENDED_ENV
EXTENDED_MARKET=$EXTENDED_MARKET
EXTENDED_API_KEY=$EXTENDED_API_KEY
EXTENDED_STARK_PUBLIC_KEY=$EXTENDED_STARK_PUBLIC_KEY
EXTENDED_STARK_PRIVATE_KEY=$EXTENDED_STARK_PRIVATE_KEY
EXTENDED_VAULT=$EXTENDED_VAULT
EXTENDED_CLIENT_ID=$EXTENDED_CLIENT_ID
EXTENDED_ORDER_TEST_SIDE=BUY
EXTENDED_ORDER_TEST_QUANTITY=
EXTENDED_ORDER_TEST_PRICE_OFFSET_PCT=10
EXTENDED_ORDER_TEST_INTERVAL_SECONDS=15
EXTENDED_ORDER_TEST_TIMEOUT_SECONDS=10
EXTENDED_ORDER_TEST_TAKER_FEE=0.00025
EOF

sudo mkdir -p data
sudo rm -f data/latency.sqlite data/latency.sqlite-shm data/latency.sqlite-wal
sudo docker rm -f exchange-latency-dashboard 2>/dev/null || true
sudo docker compose up -d --build
sudo docker compose ps
curl -fsS "http://127.0.0.1:$APP_PORT/api/status" || true
