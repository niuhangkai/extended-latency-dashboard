# MEXC Latency Dashboard

用于在 VPS 节点上 7x24 监控 MEXC WebSocket 延迟和稳定性，并通过公网 IP 打开网页实时查看。

## 功能

- 实时监控 MEXC 现货 `BBO / trades / L2` 消息间隔。
- 实时监控 MEXC 合约 WebSocket `ping/pong RTT`。
- SQLite 本地保存每个窗口的统计指标。
- 记录异常：断连、重连、超时、消息间隔尖峰、RTT 尖峰。
- 前端页面展示最新统计窗口、15m/1h/6h/24h/48h/72h 历史曲线、时间维度统计、异常日志。

注意：本项目是节点质量监控，不是完整行情录制器。完整行情录制仍放在 `crypto-history-data`。

顶部四个指标卡展示的是最新一个统计窗口，窗口长度由 `MEXC_REPORT_SECONDS` 控制，默认约 5 秒；右侧“时间维度”和主图才会跟随 15m/1h/6h/24h/48h/72h 按钮切换。

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

## 和行情录制的关系

本项目只保存统计指标，流量和磁盘占用很小。正式行情录制使用：

```text
/Users/niuhangkai/Desktop/crypto-history-data/scripts/record_mexc_spot_l2.py
```

建议：

1. 三台节点都跑本项目，比较 7x24 延迟稳定性。
2. 选最稳节点跑正式行情录制。
3. 正式录制不要一开始全市场 L2，先用少量候选币，观察延迟是否被录制压力拉高。
