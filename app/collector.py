from __future__ import annotations

import asyncio
import json
import ssl
import statistics
import time
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from typing import Any

import websockets

from app.config import Settings
from app.storage import Storage, now_ms


SPOT_WS_URL = "wss://wbs-api.mexc.com/ws"
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
