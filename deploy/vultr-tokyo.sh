#!/usr/bin/env bash
set -euo pipefail

# Run this on the Vultr Tokyo node. The dashboard will mark it as cross-cloud.
REPO_URL="${REPO_URL:-https://github.com/niuhangkai/extended-latency-dashboard.git}"
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
EXTENDED_FILL_TEST_SIDE="${EXTENDED_FILL_TEST_SIDE:-BUY}"
EXTENDED_FILL_TEST_QUANTITY="${EXTENDED_FILL_TEST_QUANTITY:-}"
EXTENDED_FILL_TEST_PRICE_OFFSET_PCT="${EXTENDED_FILL_TEST_PRICE_OFFSET_PCT:-1}"
EXTENDED_FILL_TEST_INTERVAL_SECONDS="${EXTENDED_FILL_TEST_INTERVAL_SECONDS:-60}"
EXTENDED_FILL_TEST_TIMEOUT_SECONDS="${EXTENDED_FILL_TEST_TIMEOUT_SECONDS:-10}"
EXTENDED_FILL_TEST_TAKER_FEE="${EXTENDED_FILL_TEST_TAKER_FEE:-}"
EXTENDED_FILL_ALLOW_MAINNET="${EXTENDED_FILL_ALLOW_MAINNET:-false}"

install_runtime() {
  if command -v apt-get >/dev/null 2>&1; then
    export DEBIAN_FRONTEND=noninteractive
    sudo apt-get update
    sudo apt-get install -y ca-certificates curl git
    if ! command -v docker >/dev/null 2>&1; then
      curl -fsSL https://get.docker.com | sudo sh
    fi
  elif command -v dnf >/dev/null 2>&1; then
    sudo dnf install -y ca-certificates curl git docker
  elif command -v yum >/dev/null 2>&1; then
    sudo yum install -y ca-certificates curl git docker
  else
    echo "Unsupported Linux: apt-get/dnf/yum not found" >&2
    exit 1
  fi

  if command -v systemctl >/dev/null 2>&1; then
    sudo systemctl enable --now docker
  else
    sudo service docker start 2>/dev/null || true
  fi

  if ! sudo docker compose version >/dev/null 2>&1; then
    compose_arch="$(uname -m)"
    case "$compose_arch" in
      x86_64) compose_arch="x86_64" ;;
      aarch64|arm64) compose_arch="aarch64" ;;
      *) echo "Unsupported Docker Compose arch: $compose_arch" >&2; exit 1 ;;
    esac
    sudo mkdir -p /usr/local/lib/docker/cli-plugins
    sudo curl -fsSL "https://github.com/docker/compose/releases/download/v2.36.2/docker-compose-linux-$compose_arch" \
      -o /usr/local/lib/docker/cli-plugins/docker-compose
    sudo chmod +x /usr/local/lib/docker/cli-plugins/docker-compose
  fi
}

cd /tmp
install_runtime

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
EXCHANGE_STREAMS=extended_rest,extended_bbo,extended_l2,extended_trades,extended_mark,extended_index,extended_order_test,extended_fill_test
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
EXTENDED_ORDER_TEST_TAKER_FEE=
EXTENDED_FILL_TEST_SIDE=$EXTENDED_FILL_TEST_SIDE
EXTENDED_FILL_TEST_QUANTITY=$EXTENDED_FILL_TEST_QUANTITY
EXTENDED_FILL_TEST_PRICE_OFFSET_PCT=$EXTENDED_FILL_TEST_PRICE_OFFSET_PCT
EXTENDED_FILL_TEST_INTERVAL_SECONDS=$EXTENDED_FILL_TEST_INTERVAL_SECONDS
EXTENDED_FILL_TEST_TIMEOUT_SECONDS=$EXTENDED_FILL_TEST_TIMEOUT_SECONDS
EXTENDED_FILL_TEST_TAKER_FEE=$EXTENDED_FILL_TEST_TAKER_FEE
EXTENDED_FILL_ALLOW_MAINNET=$EXTENDED_FILL_ALLOW_MAINNET
EOF

sudo mkdir -p data
sudo rm -f data/latency.sqlite data/latency.sqlite-shm data/latency.sqlite-wal
sudo docker rm -f exchange-latency-dashboard 2>/dev/null || true
sudo docker compose up -d --build
sudo docker compose ps
curl -fsS "http://127.0.0.1:$APP_PORT/api/status" || true
