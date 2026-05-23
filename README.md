# Extended API 连接监控

只监控 Extended 交易所。项目会在每个节点上采集 Extended 公共 REST、公共 WebSocket、私有订单 WebSocket、测试下单/撤单 ACK，并在页面里标出节点是否和 Extended 官方文档中的 AWS Tokyo `ap-northeast-1a / apne1-az4` 同 AZ。

## 当前采集项

| 流 | 页面名称 | 含义 |
| --- | --- | --- |
| `extended_rest` | Extended 公共 REST | `GET /info/markets` RTT |
| `extended_bbo` | Extended BBO | `orderbooks/{market}?depth=1` 消息时间戳 lag |
| `extended_l2` | Extended L2 | `orderbooks/{market}` 消息间隔 |
| `extended_trades` | Extended 成交 | `publicTrades/{market}` 消息间隔 |
| `extended_mark` | Extended 标记价格 | `prices/mark/{market}` 消息时间戳 lag |
| `extended_index` | Extended 指数价格 | `prices/index/{market}` 消息时间戳 lag |
| `extended_order_place` | Extended 下单 ACK | 测试下单 REST ACK |
| `extended_order_cancel` | Extended 撤单 ACK | 测试撤单 REST ACK |
| `extended_order_ws` | Extended 私有回报 | 私有 WebSocket 下单/撤单回报 |

启用 `extended_order_test` 会自动打开 `extended_order_place`、`extended_order_cancel`、`extended_order_ws`。下单测试需要 Extended API key、Stark key、vault；建议先用 `EXTENDED_ENV=testnet`。

## 本地运行

```bash
cd /Users/niuhangkai/Desktop/exchange-latency-dashboard
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
EXCHANGE_REGION=local \
EXCHANGE_STREAMS=extended_rest,extended_bbo,extended_l2,extended_trades,extended_mark,extended_index \
uvicorn app.main:app --host 0.0.0.0 --port 8080
```

## AWS 同 AZ 部署

Extended 文档标注目标位置为：

```text
Region: ap-northeast-1
AZ:     ap-northeast-1a
AZ ID:  apne1-az4
```

在 AWS 账号里 AZ 名称会按账号映射，真正要对齐的是 `AZ ID=apne1-az4`。先用 CloudShell 查表：

```bash
aws ec2 describe-availability-zones \
  --region ap-northeast-1 \
  --query 'AvailabilityZones[*].[ZoneName,ZoneId]' \
  --output table
```

如果你的表里显示 `ap-northeast-1a = apne1-az4`，新建实例时子网选择 `ap-northeast-1a (apne1-az4)`。如果不同，就选择映射到 `apne1-az4` 的那个子网。

在实例上部署：

```bash
cd /tmp
sudo apt update
sudo apt install -y ca-certificates curl git
if ! command -v docker >/dev/null 2>&1; then
  curl -fsSL https://get.docker.com | sudo sh
fi

sudo rm -rf /opt/exchange-latency-dashboard
REPO_URL=替换为你的仓库_SSH_地址
sudo git clone "$REPO_URL" /opt/exchange-latency-dashboard
cd /opt/exchange-latency-dashboard

TOKEN=$(curl -sX PUT http://169.254.169.254/latest/api/token \
  -H "X-aws-ec2-metadata-token-ttl-seconds: 21600")
AZ=$(curl -sH "X-aws-ec2-metadata-token: $TOKEN" \
  http://169.254.169.254/latest/meta-data/placement/availability-zone)
AZ_ID=$(curl -sH "X-aws-ec2-metadata-token: $TOKEN" \
  http://169.254.169.254/latest/meta-data/placement/availability-zone-id)
INSTANCE_ID=$(curl -sH "X-aws-ec2-metadata-token: $TOKEN" \
  http://169.254.169.254/latest/meta-data/instance-id)
PRIVATE_IP=$(curl -sH "X-aws-ec2-metadata-token: $TOKEN" \
  http://169.254.169.254/latest/meta-data/local-ipv4)
PUBLIC_IP=$(curl -sH "X-aws-ec2-metadata-token: $TOKEN" \
  http://169.254.169.254/latest/meta-data/public-ipv4)
MAC=$(curl -sH "X-aws-ec2-metadata-token: $TOKEN" \
  http://169.254.169.254/latest/meta-data/network/interfaces/macs/ | head -n1 | tr -d /)
SUBNET_ID=$(curl -sH "X-aws-ec2-metadata-token: $TOKEN" \
  http://169.254.169.254/latest/meta-data/network/interfaces/macs/$MAC/subnet-id)
VPC_ID=$(curl -sH "X-aws-ec2-metadata-token: $TOKEN" \
  http://169.254.169.254/latest/meta-data/network/interfaces/macs/$MAC/vpc-id)

cat > .env <<EOF
APP_PORT=8080
EXCHANGE_REGION=aws-tokyo-$AZ_ID
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
EXTENDED_ENV=testnet
EXTENDED_MARKET=BTC-USD
EXTENDED_API_KEY=替换
EXTENDED_STARK_PUBLIC_KEY=替换
EXTENDED_STARK_PRIVATE_KEY=替换
EXTENDED_VAULT=替换
EXTENDED_CLIENT_ID=
EXTENDED_ORDER_TEST_SIDE=BUY
EXTENDED_ORDER_TEST_QUANTITY=
EXTENDED_ORDER_TEST_PRICE_OFFSET_PCT=10
EXTENDED_ORDER_TEST_INTERVAL_SECONDS=15
EXTENDED_ORDER_TEST_TIMEOUT_SECONDS=10
EXTENDED_ORDER_TEST_TAKER_FEE=0.00025
EOF

sudo rm -f data/latency.sqlite data/latency.sqlite-shm data/latency.sqlite-wal
sudo docker rm -f exchange-latency-dashboard 2>/dev/null || true
sudo docker compose up -d --build
sudo docker compose ps
```

注意第一行 `cd /tmp`：不要在 `/opt/exchange-latency-dashboard` 目录里执行 `rm -rf /opt/exchange-latency-dashboard`，否则 shell 当前目录被删除，会出现 `getcwd() failed: No such file or directory`。

## Vultr 部署

Vultr 不属于 AWS AZ，页面会标为 `跨云节点`，用于和 AWS 同 AZ 节点对比。

```bash
cd /tmp
sudo apt update
sudo apt install -y ca-certificates curl git
if ! command -v docker >/dev/null 2>&1; then
  curl -fsSL https://get.docker.com | sudo sh
fi

sudo rm -rf /opt/exchange-latency-dashboard
REPO_URL=替换为你的仓库_SSH_地址
sudo git clone "$REPO_URL" /opt/exchange-latency-dashboard
cd /opt/exchange-latency-dashboard

cat > .env <<EOF
APP_PORT=8080
EXCHANGE_REGION=vultr-jp
EXCHANGE_CLOUD_PROVIDER=vultr
EXCHANGE_STREAMS=extended_rest,extended_bbo,extended_l2,extended_trades,extended_mark,extended_index,extended_order_test
EXCHANGE_REPORT_SECONDS=5
EXTENDED_ENV=testnet
EXTENDED_MARKET=BTC-USD
EXTENDED_API_KEY=替换
EXTENDED_STARK_PUBLIC_KEY=替换
EXTENDED_STARK_PRIVATE_KEY=替换
EXTENDED_VAULT=替换
EXTENDED_CLIENT_ID=
EXTENDED_ORDER_TEST_SIDE=BUY
EXTENDED_ORDER_TEST_QUANTITY=
EXTENDED_ORDER_TEST_PRICE_OFFSET_PCT=10
EXTENDED_ORDER_TEST_INTERVAL_SECONDS=15
EXTENDED_ORDER_TEST_TIMEOUT_SECONDS=10
EXTENDED_ORDER_TEST_TAKER_FEE=0.00025
EOF

sudo rm -f data/latency.sqlite data/latency.sqlite-shm data/latency.sqlite-wal
sudo docker rm -f exchange-latency-dashboard 2>/dev/null || true
sudo docker compose up -d --build
sudo docker compose ps
```

## 配置项

| 变量 | 默认值 | 说明 |
| --- | --- | --- |
| `APP_PORT` | `8080` | 宿主机访问端口 |
| `EXCHANGE_REGION` | `local` | 页面显示的节点名 |
| `EXCHANGE_CLOUD_PROVIDER` | 空 | `aws` / `vultr`，用于同 AZ 或跨云标记 |
| `EXCHANGE_STREAMS` | Extended 全部流 | 逗号分隔的采集流 |
| `EXCHANGE_REPORT_SECONDS` | `5` | 统计窗口秒数 |
| `EXTENDED_ENV` | `mainnet` | `mainnet` 或 `testnet` |
| `EXTENDED_MARKET` | `BTC-USD` | Extended 市场名 |
| `EXTENDED_API_KEY` | 空 | 下单测试需要 |
| `EXTENDED_STARK_PUBLIC_KEY` | 空 | 下单测试需要 |
| `EXTENDED_STARK_PRIVATE_KEY` | 空 | 下单测试需要 |
| `EXTENDED_VAULT` | 空 | 下单测试需要 |
| `EXTENDED_ORDER_TEST_INTERVAL_SECONDS` | `15` | 下单/撤单测试间隔 |
| `EXTENDED_ORDER_TEST_TIMEOUT_SECONDS` | `10` | 下单/撤单超时 |

## 验证

```bash
curl -s http://127.0.0.1:8080/api/status | jq
curl -s "http://127.0.0.1:8080/api/series?minutes=60" | jq '.items | length'
sudo docker compose logs -f --tail=100
```

如果页面仍显示旧内容，先强刷浏览器缓存，再确认容器镜像是重新构建过的：

```bash
sudo docker compose down
sudo docker compose build --no-cache
sudo docker compose up -d
```
