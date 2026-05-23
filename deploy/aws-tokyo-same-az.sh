#!/usr/bin/env bash
set -euo pipefail

# Run this on the AWS Tokyo instance that should be in the same AZ as Extended:
# ap-northeast-1 / AZ ID apne1-az4.
REPO_URL="${REPO_URL:-git@github.com:niuhangkai/extended-latency-dashboard.git}"
APP_DIR="${APP_DIR:-/opt/exchange-latency-dashboard}"
APP_PORT="${APP_PORT:-8080}"
NODE_LABEL="${NODE_LABEL:-aws-tokyo-same-az}"
EXPECTED_AZ_ID="${EXPECTED_AZ_ID:-apne1-az4}"

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

TOKEN="$(curl -sX PUT http://169.254.169.254/latest/api/token \
  -H 'X-aws-ec2-metadata-token-ttl-seconds: 21600')"
meta() {
  curl -fsS -H "X-aws-ec2-metadata-token: $TOKEN" "http://169.254.169.254/latest/$1"
}

AZ="$(meta meta-data/placement/availability-zone)"
AZ_ID="$(meta meta-data/placement/availability-zone-id)"
INSTANCE_ID="$(meta meta-data/instance-id)"
PRIVATE_IP="$(meta meta-data/local-ipv4)"
PUBLIC_IP="$(meta meta-data/public-ipv4 2>/dev/null || true)"
MAC="$(meta meta-data/network/interfaces/macs/ | head -n1 | tr -d /)"
SUBNET_ID="$(meta "meta-data/network/interfaces/macs/$MAC/subnet-id")"
VPC_ID="$(meta "meta-data/network/interfaces/macs/$MAC/vpc-id")"

if [ "$AZ_ID" != "$EXPECTED_AZ_ID" ]; then
  echo "WARNING: this instance AZ ID is $AZ_ID, expected $EXPECTED_AZ_ID for same-AZ Extended placement."
fi

sudo rm -rf "$APP_DIR"
sudo git clone "$REPO_URL" "$APP_DIR"
sudo chown -R "$(id -u):$(id -g)" "$APP_DIR"
cd "$APP_DIR"

cat > .env <<EOF
APP_PORT=$APP_PORT
EXCHANGE_REGION=$NODE_LABEL-$AZ_ID
EXCHANGE_CLOUD_PROVIDER=aws
EXCHANGE_AWS_REGION=ap-northeast-1
EXCHANGE_AWS_AZ=$AZ
EXCHANGE_AWS_AZ_ID=$AZ_ID
EXCHANGE_AWS_SUBNET_ID=$SUBNET_ID
EXCHANGE_AWS_VPC_ID=$VPC_ID
EXCHANGE_INSTANCE_ID=$INSTANCE_ID
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
