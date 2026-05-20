# MEXC Latency Dashboard

用于在 VPS 节点上 7x24 监控 MEXC WebSocket 延迟和稳定性，并通过公网 IP 打开网页实时查看。

## 功能

- 实时监控 MEXC 现货 `BBO / trades / L2` 消息间隔。
- 实时监控 MEXC 合约 WebSocket `ping/pong RTT`。
- 可选监控 MEXC 现货 `POST /api/v3/order/test` 测试下单 ACK 耗时。
- 可选监控 Extended REST RTT、BBO 消息 lag、L2/trades 消息间隔。
- SQLite 本地保存每个窗口的统计指标。
- 记录异常：断连、重连、超时、消息间隔尖峰、RTT 尖峰。
- 前端页面展示最新统计窗口、15m/1h/6h/24h/48h/72h 历史曲线、时间维度统计、异常日志。

注意：本项目是节点质量监控，不是完整行情录制器。完整行情录制仍放在 `crypto-history-data`。

顶部指标卡展示的是最新一个统计窗口，窗口长度由 `MEXC_REPORT_SECONDS` 控制，默认约 5 秒；右侧“时间维度”和主图才会跟随 15m/1h/6h/24h/48h/72h 按钮切换。

`spot_order_test` 使用 MEXC 现货测试下单接口，只校验订单参数，不进入撮合引擎。MEXC 官方文档说明当前 API 没有 sandbox/test 环境，所以这个指标代表“现货测试下单 ACK 耗时”，不是合约模拟盘真实撮合延迟。

Extended 官方 API 服务器位于 AWS Tokyo `ap-northeast-1a`。Extended 的 BBO 使用消息时间戳计算本机收到时的 lag；Extended full L2 和 trades 的初始快照/成交流可能包含较旧时间戳，因此本项目按消息间隔展示，避免误判为网络延迟。

## 本地运行

```bash
cd /Users/niuhangkai/Desktop/mexc-latency-dashboard
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
MEXC_REGION=local MEXC_SYMBOL=BTCUSDT MEXC_REPORT_SECONDS=5 uvicorn app.main:app --host 0.0.0.0 --port 8080
```

打开：

```text
http://127.0.0.1:8080
```

## Docker 运行

```bash
cd /Users/niuhangkai/Desktop/mexc-latency-dashboard
docker compose up -d --build
```

打开：

```text
http://127.0.0.1:8080
```

停止：

```bash
docker compose down
```

## 测试下单延迟

先在 MEXC 创建 API Key，开启现货交易权限，建议绑定 VPS 出口 IP。然后在 `.env` 中开启 `spot_order_test`：

```bash
cat > .env <<'EOF'
MEXC_REGION=vultr-jp
MEXC_SYMBOL=BTCUSDT
MEXC_STREAMS=spot_bbo,spot_trades,spot_l2,contract_ping,spot_order_test
MEXC_REPORT_SECONDS=5
APP_PORT=8080

MEXC_API_KEY=你的_API_KEY
MEXC_API_SECRET=你的_API_SECRET
MEXC_ORDER_TEST_SYMBOL=BTCUSDT
MEXC_ORDER_TEST_SIDE=BUY
MEXC_ORDER_TEST_QUANTITY=0.001
MEXC_ORDER_TEST_PRICE=100000
MEXC_ORDER_TEST_INTERVAL_SECONDS=10
EOF

docker compose up -d --build
```

默认每 10 秒发送一次 `POST /api/v3/order/test`。这个接口不会产生真实订单，但 API Key 仍然要当敏感信息保护，`.env` 不要提交到 Git。

## Extended 延迟监控

Extended 公共行情不需要 API Key。开启 Extended 监控：

```bash
cat > .env <<'EOF'
MEXC_REGION=vultr-jp
MEXC_SYMBOL=BTCUSDT
MEXC_STREAMS=extended_rest,extended_bbo,extended_l2,extended_trades
MEXC_REPORT_SECONDS=5
APP_PORT=8080

EXTENDED_MARKET=BTC-USD
EXTENDED_REST_INTERVAL_SECONDS=1
EXTENDED_TIMEOUT_SECONDS=5
EOF

docker compose up -d --build
```

Extended 主网 API：

```text
REST https://api.starknet.extended.exchange/api/v1
WS   wss://api.starknet.extended.exchange/stream.extended.exchange/v1
```

## VPS 部署

服务器安装 Docker：

```bash
apt update
apt install -y ca-certificates curl git
install -m 0755 -d /etc/apt/keyrings
curl -fsSL https://download.docker.com/linux/ubuntu/gpg -o /etc/apt/keyrings/docker.asc
chmod a+r /etc/apt/keyrings/docker.asc
echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.asc] https://download.docker.com/linux/ubuntu $(. /etc/os-release && echo ${UBUNTU_CODENAME:-$VERSION_CODENAME}) stable" > /etc/apt/sources.list.d/docker.list
apt update
apt install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
```

拉 GitHub 项目：

```bash
git clone YOUR_GITHUB_REPO_URL mexc-latency-dashboard
cd mexc-latency-dashboard
```

日本节点示例：

```bash
MEXC_REGION=vultr-jp \
MEXC_SYMBOL=BTCUSDT \
MEXC_REPORT_SECONDS=5 \
APP_PORT=8080 \
docker compose up -d --build
```

韩国节点示例：

```bash
MEXC_REGION=vultr-kr \
MEXC_SYMBOL=BTCUSDT \
MEXC_REPORT_SECONDS=5 \
APP_PORT=8080 \
docker compose up -d --build
```

新加坡节点示例：

```bash
MEXC_REGION=vultr-sg \
MEXC_SYMBOL=BTCUSDT \
MEXC_REPORT_SECONDS=5 \
APP_PORT=8080 \
docker compose up -d --build
```

`docker compose up -d` 会后台运行，SSH 断开后不会停止。`restart: unless-stopped` 会在容器崩溃或服务器重启后自动拉起。

## 公网访问

Vultr 默认有公网 IPv4。应用监听：

```text
0.0.0.0:8080
```

打开：

```text
http://服务器公网IP:8080
```

如果访问不了，检查 VPS 防火墙和系统防火墙。

Ubuntu 本机防火墙：

```bash
ufw allow 8080/tcp
ufw status
```

Vultr 控制台如果配置了 Firewall Group，也需要放行：

```text
TCP 8080
Source 0.0.0.0/0
```

为了安全，建议后续改成：

```text
只允许你的本地公网 IP 访问 8080
```

## 日志和数据

SQLite 数据文件：

```text
./data/latency.sqlite
```

查看容器日志：

```bash
docker compose logs -f --tail=100
```

查看运行状态：

```bash
docker compose ps
```

## 环境变量

| 变量 | 默认值 | 说明 |
|---|---|---|
| `MEXC_REGION` | `local` | 节点标签，比如 `vultr-jp` |
| `MEXC_SYMBOL` | `BTCUSDT` | 监控交易对 |
| `MEXC_STREAMS` | `spot_bbo,spot_trades,spot_l2,contract_ping` | 监控流 |
| `MEXC_REPORT_SECONDS` | `5` | 统计窗口秒数 |
| `APP_PORT` | `8080` | 对外访问端口 |
| `MEXC_API_KEY` | 空 | 开启 `spot_order_test` 时需要 |
| `MEXC_API_SECRET` | 空 | 开启 `spot_order_test` 时需要 |
| `MEXC_ORDER_TEST_SYMBOL` | `MEXC_SYMBOL` | 测试下单交易对 |
| `MEXC_ORDER_TEST_SIDE` | `BUY` | 测试方向 |
| `MEXC_ORDER_TEST_QUANTITY` | `0.001` | 测试数量 |
| `MEXC_ORDER_TEST_PRICE` | `100000` | 测试限价 |
| `MEXC_ORDER_TEST_INTERVAL_SECONDS` | `10` | 测试下单请求间隔 |
| `MEXC_ORDER_TEST_RECV_WINDOW_MS` | `5000` | 签名请求 recvWindow |
| `MEXC_ORDER_TEST_TIMEOUT_SECONDS` | `5` | HTTP 请求超时秒数 |
| `EXTENDED_MARKET` | `BTC-USD` | Extended 监控市场 |
| `EXTENDED_REST_INTERVAL_SECONDS` | `1` | Extended REST RTT 请求间隔 |
| `EXTENDED_TIMEOUT_SECONDS` | `5` | Extended HTTP/WS 超时基准秒数 |

## 和行情录制的关系

本项目只保存统计指标，流量和磁盘占用很小。正式行情录制使用：

```text
/Users/niuhangkai/Desktop/crypto-history-data/scripts/record_mexc_spot_l2.py
```

建议：

1. 三台节点都跑本项目，比较 7x24 延迟稳定性。
2. 选最稳节点跑正式行情录制。
3. 正式录制不要一开始全市场 L2，先用少量候选币，观察延迟是否被录制压力拉高。
