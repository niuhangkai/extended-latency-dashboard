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
| `extended_fill_place` | Extended 实际成交下单 | IOC 可成交订单 REST ACK |
| `extended_fill_ws` | Extended 实际成交回报 | 私有 WebSocket FILLED/PARTIALLY_FILLED 回报 |

启用 `extended_order_test` 会自动打开 `extended_order_place`、`extended_order_cancel`、`extended_order_ws`。下单测试需要 Extended API key、Stark key、vault；建议先用 `EXTENDED_ENV=testnet`。

启用 `extended_fill_test` 会发送 IOC 可成交订单，自动打开 `extended_fill_place`、`extended_fill_ws`。该测试默认只允许 `EXTENDED_ENV=testnet`；如果要在主网运行，必须显式设置 `EXTENDED_FILL_ALLOW_MAINNET=true`。

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

## 仓库改名

目标仓库名：`extended-latency-dashboard`。

GitHub 仓库需要在 GitHub 网页 Settings 里 Rename，或者使用已登录的 GitHub CLI/API token 修改。改名完成后，本地执行：

```bash
git remote set-url origin git@github.com:niuhangkai/extended-latency-dashboard.git
git remote -v
```

本项目部署脚本默认已经使用新仓库地址：

```text
git@github.com:niuhangkai/extended-latency-dashboard.git
```

如果 GitHub 仓库还没改名，先完成 Rename 再部署，避免服务器拉取不到新地址。

## 三节点部署脚本

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

每台机器先设置 Extended 下单测试密钥，再执行对应脚本。密钥不写入仓库，只写入服务器 `.env`：

```bash
EXTENDED_ENV=testnet
EXTENDED_MARKET=BTC-USD
EXTENDED_API_KEY=替换_EXTENDED_API_KEY
EXTENDED_STARK_PUBLIC_KEY=替换_EXTENDED_STARK_PUBLIC_KEY
EXTENDED_STARK_PRIVATE_KEY=替换_EXTENDED_STARK_PRIVATE_KEY
EXTENDED_VAULT=替换_EXTENDED_VAULT
EXTENDED_CLIENT_ID=
```

AWS 同 AZ 节点：

```bash
bash deploy/aws-tokyo-same-az.sh
```

AWS 非同 AZ 对照节点：

```bash
bash deploy/aws-tokyo-other-az.sh
```

Vultr Tokyo 跨云节点：

```bash
bash deploy/vultr-tokyo.sh
```

注意：脚本都会先 `cd /tmp` 再删除 `/opt/exchange-latency-dashboard`，避免在当前目录被删除后出现 `getcwd() failed: No such file or directory`。

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
| `EXTENDED_FILL_TEST_SIDE` | `BUY` | 实际成交测试方向 |
| `EXTENDED_FILL_TEST_QUANTITY` | 空 | 实际成交测试数量；空值使用最小下单量 |
| `EXTENDED_FILL_TEST_PRICE_OFFSET_PCT` | `1` | IOC 穿盘口价格偏移百分比 |
| `EXTENDED_FILL_TEST_INTERVAL_SECONDS` | `60` | 实际成交测试间隔 |
| `EXTENDED_FILL_TEST_TIMEOUT_SECONDS` | `10` | 实际成交下单超时 |
| `EXTENDED_FILL_ALLOW_MAINNET` | `false` | 是否允许主网实际成交测试 |

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
