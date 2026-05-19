from __future__ import annotations

import asyncio
import hashlib
import hmac
import json
import ssl
import statistics
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import Any
from urllib.parse import urlencode

import httpx
import websockets

from app.config import Settings
from app.storage import Storage, now_ms


SPOT_WS_URL = "wss://wbs-api.mexc.com/ws"
SPOT_REST_URL = "https://api.mexc.com"
CONTRACT_WS_URL = "wss://contract.mexc.com/edge"

Broadcast = Callable[[dict[str, Any]], Awaitable[None]]


def percentile(values: list[float], p: float) -> float | None:
    if not values:
        return None
    ordered = sorted(values)
    index = int(len(ordered) * p / 100)
    index = min(max(index, 0), len(ordered) - 1)
    return ordered[index]


def rounded(value: float | None) -> float | None:
    if value is None:
        return None
    return round(value, 3)


@dataclass
class Window:
    values: list[float] = field(default_factory=list)
    messages: int = 0
    bytes: int = 0
    timeouts: int = 0

    def clear(self) -> None:
        self.values.clear()
        self.messages = 0
        self.bytes = 0
        self.timeouts = 0

    def add_value(self, value: float) -> None:
        self.values.append(value)

    def summary(self) -> dict[str, Any]:
        if not self.values:
            return {
                "count": 0,
                "avg_ms": None,
                "p50_ms": None,
                "p95_ms": None,
                "p99_ms": None,
                "max_ms": None,
            }
        return {
            "count": len(self.values),
            "avg_ms": rounded(statistics.mean(self.values)),
            "p50_ms": rounded(percentile(self.values, 50)),
            "p95_ms": rounded(percentile(self.values, 95)),
            "p99_ms": rounded(percentile(self.values, 99)),
            "max_ms": rounded(max(self.values)),
        }


class MexcLatencyCollector:
    def __init__(self, settings: Settings, storage: Storage, broadcast: Broadcast) -> None:
        self.settings = settings
        self.storage = storage
        self.broadcast = broadcast
        self.stop_event = asyncio.Event()
        self.tasks: list[asyncio.Task[Any]] = []

    async def start(self) -> None:
        if "spot_bbo" in self.settings.streams:
            self.tasks.append(
                asyncio.create_task(
                    self._spot_stream(
                        stream="spot_bbo",
                        metric_type="message_gap",
                        channel=f"spot@public.aggre.bookTicker.v3.api.pb@100ms@{self.settings.symbol}",
                    )
                )
            )
        if "spot_trades" in self.settings.streams:
            self.tasks.append(
                asyncio.create_task(
                    self._spot_stream(
                        stream="spot_trades",
                        metric_type="message_gap",
                        channel=f"spot@public.aggre.deals.v3.api.pb@100ms@{self.settings.symbol}",
                    )
                )
            )
        if "spot_l2" in self.settings.streams:
            self.tasks.append(
                asyncio.create_task(
                    self._spot_stream(
                        stream="spot_l2",
                        metric_type="message_gap",
                        channel=f"spot@public.aggre.depth.v3.api.pb@100ms@{self.settings.symbol}",
                    )
                )
            )
        if "contract_ping" in self.settings.streams:
            self.tasks.append(asyncio.create_task(self._contract_ping()))
        if "spot_order_test" in self.settings.streams:
            self.tasks.append(asyncio.create_task(self._spot_order_test()))

    async def stop(self) -> None:
        self.stop_event.set()
        for task in self.tasks:
            task.cancel()
        await asyncio.gather(*self.tasks, return_exceptions=True)

    async def _save_sample(self, sample: dict[str, Any]) -> None:
        row = self.storage.insert_sample(sample)
        await self.broadcast({"type": "sample", "data": row})

    async def _save_incident(self, incident: dict[str, Any]) -> None:
        row = self.storage.insert_incident(incident)
        await self.broadcast({"type": "incident", "data": row})

    async def _spot_stream(self, *, stream: str, metric_type: str, channel: str) -> None:
        ssl_ctx = ssl.create_default_context()
        reconnects = 0
        window = Window()
        last_msg_at: float | None = None
        report_started_at = time.time()

        while not self.stop_event.is_set():
            try:
                reconnects += 1
                connect_t0 = time.perf_counter()
                async with websockets.connect(SPOT_WS_URL, ssl=ssl_ctx, ping_interval=None) as ws:
                    connect_ms = (time.perf_counter() - connect_t0) * 1000
                    await self._save_incident(
                        {
                            "ts_ms": now_ms(),
                            "region": self.settings.region,
                            "stream": stream,
                            "symbol": self.settings.symbol,
                            "severity": "info",
                            "type": "connect",
                            "message": f"{stream} 连接成功，耗时 {connect_ms:.2f} ms",
                            "extra": {"channel": channel, "connect_ms": rounded(connect_ms)},
                        }
                    )
                    await ws.send(
                        json.dumps(
                            {"method": "SUBSCRIPTION", "params": [channel], "id": reconnects},
                            separators=(",", ":"),
                        )
                    )

                    while not self.stop_event.is_set():
                        try:
                            msg = await asyncio.wait_for(ws.recv(), timeout=10)
                        except asyncio.TimeoutError:
                            window.timeouts += 1
                            await self._save_incident(
                                {
                                    "ts_ms": now_ms(),
                                    "region": self.settings.region,
                                    "stream": stream,
                                    "symbol": self.settings.symbol,
                                    "severity": "warning",
                                    "type": "timeout",
                                    "message": f"{stream} 10 秒内没有收到消息",
                                    "extra": {"channel": channel},
                                }
                            )
                            break

                        current = time.time()
                        if isinstance(msg, str):
                            await self._save_incident(
                                {
                                    "ts_ms": now_ms(),
                                    "region": self.settings.region,
                                    "stream": stream,
                                    "symbol": self.settings.symbol,
                                    "severity": "info",
                                    "type": "text",
                                    "message": msg[:300],
                                    "extra": {"channel": channel},
                                }
                            )
                        else:
                            window.messages += 1
                            window.bytes += len(msg)
                            if last_msg_at is not None:
                                window.add_value((current - last_msg_at) * 1000)
                            last_msg_at = current

                        if current - report_started_at >= self.settings.report_seconds:
                            sample = {
                                "ts_ms": now_ms(),
                                "region": self.settings.region,
                                "stream": stream,
                                "symbol": self.settings.symbol,
                                "metric_type": metric_type,
                                "window_s": current - report_started_at,
                                "messages": window.messages,
                                "bytes": window.bytes,
                                "reconnects": max(0, reconnects - 1),
                                "timeouts": window.timeouts,
                                **window.summary(),
                            }
                            await self._save_sample(sample)
                            if sample.get("max_ms") and sample["max_ms"] > 1000:
                                await self._save_incident(
                                    {
                                        "ts_ms": now_ms(),
                                        "region": self.settings.region,
                                        "stream": stream,
                                        "symbol": self.settings.symbol,
                                        "severity": "warning",
                                        "type": "gap_spike",
                                        "message": f"{stream} 最大消息间隔 {sample['max_ms']:.2f} ms",
                                        "extra": sample,
                                    }
                                )
                            window.clear()
                            report_started_at = current

            except asyncio.CancelledError:
                raise
            except Exception as exc:
                await self._save_incident(
                    {
                        "ts_ms": now_ms(),
                        "region": self.settings.region,
                        "stream": stream,
                        "symbol": self.settings.symbol,
                        "severity": "error",
                        "type": "error",
                        "message": repr(exc),
                        "extra": {"channel": channel},
                    }
                )
                await asyncio.sleep(2)

    def _signed_order_test_body(self) -> str:
        params = [
            ("symbol", self.settings.order_test_symbol),
            ("side", self.settings.order_test_side),
            ("type", "LIMIT"),
            ("quantity", self.settings.order_test_quantity),
            ("price", self.settings.order_test_price),
            ("recvWindow", str(self.settings.order_test_recv_window_ms)),
            ("timestamp", str(now_ms())),
        ]
        body = urlencode(params)
        signature = hmac.new(
            self.settings.api_secret.encode(),
            body.encode(),
            hashlib.sha256,
        ).hexdigest()
        return f"{body}&signature={signature}"

    async def _post_order_test(self, client: httpx.AsyncClient) -> tuple[bool, int | None, str]:
        body = self._signed_order_test_body()
        response = await client.post(
            "/api/v3/order/test",
            content=body,
            headers={
                "X-MEXC-APIKEY": self.settings.api_key,
                "Content-Type": "application/x-www-form-urlencoded",
            },
        )
        return response.status_code < 400, response.status_code, response.text[:300]

    async def _spot_order_test(self) -> None:
        stream = "spot_order_test"
        if not self.settings.api_key or not self.settings.api_secret:
            await self._save_incident(
                {
                    "ts_ms": now_ms(),
                    "region": self.settings.region,
                    "stream": stream,
                    "symbol": self.settings.order_test_symbol,
                    "severity": "error",
                    "type": "config_error",
                    "message": "未配置 MEXC_API_KEY 或 MEXC_API_SECRET，模拟下单延迟监控未启动",
                    "extra": {},
                }
            )
            return

        window = Window()
        report_started_at = time.time()
        timeout = httpx.Timeout(self.settings.order_test_timeout_seconds)
        async with httpx.AsyncClient(base_url=SPOT_REST_URL, timeout=timeout) as client:
            while not self.stop_event.is_set():
                try:
                    send_t = time.perf_counter()
                    ok, status_code, response_text = await self._post_order_test(client)
                    latency_ms = (time.perf_counter() - send_t) * 1000
                    window.messages += 1

                    if ok:
                        window.add_value(latency_ms)
                    else:
                        window.timeouts += 1
                        await self._save_incident(
                            {
                                "ts_ms": now_ms(),
                                "region": self.settings.region,
                                "stream": stream,
                                "symbol": self.settings.order_test_symbol,
                                "severity": "warning",
                                "type": "order_test_error",
                                "message": f"模拟下单测试失败，HTTP {status_code}",
                                "extra": {"status_code": status_code, "response": response_text},
                            }
                        )

                    current = time.time()
                    if current - report_started_at >= self.settings.report_seconds:
                        sample = {
                            "ts_ms": now_ms(),
                            "region": self.settings.region,
                            "stream": stream,
                            "symbol": self.settings.order_test_symbol,
                            "metric_type": "order_test_ack",
                            "window_s": current - report_started_at,
                            "messages": window.messages,
                            "bytes": 0,
                            "reconnects": 0,
                            "timeouts": window.timeouts,
                            **window.summary(),
                        }
                        await self._save_sample(sample)
                        if sample.get("max_ms") and sample["max_ms"] > 500:
                            await self._save_incident(
                                {
                                    "ts_ms": now_ms(),
                                    "region": self.settings.region,
                                    "stream": stream,
                                    "symbol": self.settings.order_test_symbol,
                                    "severity": "warning",
                                    "type": "order_test_spike",
                                    "message": f"模拟下单 ACK 最大耗时 {sample['max_ms']:.2f} ms",
                                    "extra": sample,
                                }
                            )
                        window.clear()
                        report_started_at = current

                except asyncio.CancelledError:
                    raise
                except httpx.TimeoutException:
                    window.timeouts += 1
                    await self._save_incident(
                        {
                            "ts_ms": now_ms(),
                            "region": self.settings.region,
                            "stream": stream,
                            "symbol": self.settings.order_test_symbol,
                            "severity": "warning",
                            "type": "timeout",
                            "message": "模拟下单测试请求超时",
                            "extra": {},
                        }
                    )
                except Exception as exc:
                    window.timeouts += 1
                    await self._save_incident(
                        {
                            "ts_ms": now_ms(),
                            "region": self.settings.region,
                            "stream": stream,
                            "symbol": self.settings.order_test_symbol,
                            "severity": "error",
                            "type": "error",
                            "message": repr(exc),
                            "extra": {},
                        }
                    )

                try:
                    await asyncio.wait_for(
                        self.stop_event.wait(),
                        timeout=self.settings.order_test_interval_seconds,
                    )
                except asyncio.TimeoutError:
                    pass

    async def _contract_ping(self) -> None:
        ssl_ctx = ssl.create_default_context()
        reconnects = 0
        window = Window()
        report_started_at = time.time()

        while not self.stop_event.is_set():
            try:
                reconnects += 1
                connect_t0 = time.perf_counter()
                async with websockets.connect(CONTRACT_WS_URL, ssl=ssl_ctx, ping_interval=None) as ws:
                    connect_ms = (time.perf_counter() - connect_t0) * 1000
                    await self._save_incident(
                        {
                            "ts_ms": now_ms(),
                            "region": self.settings.region,
                            "stream": "contract_ping",
                            "symbol": None,
                            "severity": "info",
                            "type": "connect",
                            "message": f"合约 WebSocket 连接成功，耗时 {connect_ms:.2f} ms",
                            "extra": {"connect_ms": rounded(connect_ms)},
                        }
                    )

                    while not self.stop_event.is_set():
                        send_t = time.perf_counter()
                        await ws.send(json.dumps({"method": "ping"}, separators=(",", ":")))
                        try:
                            await asyncio.wait_for(ws.recv(), timeout=5)
                        except asyncio.TimeoutError:
                            window.timeouts += 1
                            await self._save_incident(
                                {
                                    "ts_ms": now_ms(),
                                    "region": self.settings.region,
                                    "stream": "contract_ping",
                                    "symbol": None,
                                    "severity": "warning",
                                    "type": "timeout",
                                    "message": "合约 ping 5 秒内没有收到 pong",
                                    "extra": {},
                                }
                            )
                            break

                        rtt_ms = (time.perf_counter() - send_t) * 1000
                        window.add_value(rtt_ms)
                        current = time.time()
                        if current - report_started_at >= self.settings.report_seconds:
                            sample = {
                                "ts_ms": now_ms(),
                                "region": self.settings.region,
                                "stream": "contract_ping",
                                "symbol": None,
                                "metric_type": "rtt",
                                "window_s": current - report_started_at,
                                "messages": 0,
                                "bytes": 0,
                                "reconnects": max(0, reconnects - 1),
                                "timeouts": window.timeouts,
                                **window.summary(),
                            }
                            await self._save_sample(sample)
                            if sample.get("max_ms") and sample["max_ms"] > 250:
                                await self._save_incident(
                                    {
                                        "ts_ms": now_ms(),
                                        "region": self.settings.region,
                                        "stream": "contract_ping",
                                        "symbol": None,
                                        "severity": "warning",
                                        "type": "rtt_spike",
                                        "message": f"合约 ping 最大 RTT {sample['max_ms']:.2f} ms",
                                        "extra": sample,
                                    }
                                )
                            window.clear()
                            report_started_at = current

                        await asyncio.sleep(1)

            except asyncio.CancelledError:
                raise
            except Exception as exc:
                await self._save_incident(
                    {
                        "ts_ms": now_ms(),
                        "region": self.settings.region,
                        "stream": "contract_ping",
                        "symbol": None,
                        "severity": "error",
                        "type": "error",
                        "message": repr(exc),
                        "extra": {},
                    }
                )
                await asyncio.sleep(2)
