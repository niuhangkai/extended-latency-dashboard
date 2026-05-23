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
from decimal import Decimal
from typing import Any
from urllib.parse import quote
from urllib.parse import urlencode
from uuid import uuid4

import httpx
import websockets

from app.config import Settings
from app.storage import Storage, now_ms


SPOT_WS_URL = "wss://wbs-api.mexc.com/ws"
SPOT_REST_URL = "https://api.mexc.com"
CONTRACT_WS_URL = "wss://contract.mexc.com/edge"
EXTENDED_REST_URL = "https://api.starknet.extended.exchange/api/v1"
EXTENDED_WS_URL = "wss://api.starknet.extended.exchange/stream.extended.exchange/v1"
EXTENDED_TESTNET_REST_URL = "https://api.starknet.sepolia.extended.exchange/api/v1"
EXTENDED_TESTNET_WS_URL = "wss://api.starknet.sepolia.extended.exchange/stream.extended.exchange/v1"

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


class ExchangeLatencyCollector:
    def __init__(self, settings: Settings, storage: Storage, broadcast: Broadcast) -> None:
        self.settings = settings
        self.storage = storage
        self.broadcast = broadcast
        self.stop_event = asyncio.Event()
        self.tasks: list[asyncio.Task[Any]] = []
        self.extended_order_submitted_at: dict[str, float] = {}
        self.extended_cancel_submitted_at: dict[str, float] = {}

    @property
    def extended_rest_url(self) -> str:
        return EXTENDED_TESTNET_REST_URL if self.settings.extended_env == "testnet" else EXTENDED_REST_URL

    @property
    def extended_ws_url(self) -> str:
        return EXTENDED_TESTNET_WS_URL if self.settings.extended_env == "testnet" else EXTENDED_WS_URL

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
        if "extended_rest" in self.settings.streams:
            self.tasks.append(asyncio.create_task(self._extended_rest_ping()))
        if "extended_bbo" in self.settings.streams:
            self.tasks.append(
                asyncio.create_task(
                    self._extended_ws_stream(
                        stream="extended_bbo",
                        path=f"orderbooks/{quote(self.settings.extended_market)}?depth=1",
                        metric_type="event_lag",
                    )
                )
            )
        if "extended_l2" in self.settings.streams:
            self.tasks.append(
                asyncio.create_task(
                    self._extended_ws_stream(
                        stream="extended_l2",
                        path=f"orderbooks/{quote(self.settings.extended_market)}",
                        metric_type="message_gap",
                    )
                )
            )
        if "extended_trades" in self.settings.streams:
            self.tasks.append(
                asyncio.create_task(
                    self._extended_ws_stream(
                        stream="extended_trades",
                        path=f"publicTrades/{quote(self.settings.extended_market)}",
                        metric_type="message_gap",
                    )
                )
            )
        if "extended_order_test" in self.settings.streams:
            self.tasks.append(asyncio.create_task(self._extended_order_test()))
            self.tasks.append(asyncio.create_task(self._extended_account_stream()))

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

    def _extended_event_lag_ms(self, raw: str) -> float | None:
        try:
            payload = json.loads(raw)
        except json.JSONDecodeError:
            return None
        ts = payload.get("ts")
        if not isinstance(ts, (int, float)):
            return None
        lag = now_ms() - int(ts)
        if lag < 0 or lag > 600_000:
            return None
        return float(lag)

    async def _extended_rest_ping(self) -> None:
        stream = "extended_rest"
        window = Window()
        report_started_at = time.time()
        timeout = httpx.Timeout(self.settings.extended_timeout_seconds)
        async with httpx.AsyncClient(base_url=self.extended_rest_url, timeout=timeout) as client:
            while not self.stop_event.is_set():
                try:
                    send_t = time.perf_counter()
                    response = await client.get("/info/markets", params={"market": self.settings.extended_market})
                    latency_ms = (time.perf_counter() - send_t) * 1000
                    window.messages += 1
                    if response.status_code < 400:
                        window.add_value(latency_ms)
                    else:
                        window.timeouts += 1
                        await self._save_incident(
                            {
                                "ts_ms": now_ms(),
                                "region": self.settings.region,
                                "stream": stream,
                                "symbol": self.settings.extended_market,
                                "severity": "warning",
                                "type": "extended_rest_error",
                                "message": f"Extended REST 请求失败，HTTP {response.status_code}",
                                "extra": {"response": response.text[:300]},
                            }
                        )

                    current = time.time()
                    if current - report_started_at >= self.settings.report_seconds:
                        sample = {
                            "ts_ms": now_ms(),
                            "region": self.settings.region,
                            "stream": stream,
                            "symbol": self.settings.extended_market,
                            "metric_type": "rest_rtt",
                            "window_s": current - report_started_at,
                            "messages": window.messages,
                            "bytes": 0,
                            "reconnects": 0,
                            "timeouts": window.timeouts,
                            **window.summary(),
                        }
                        await self._save_sample(sample)
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
                            "symbol": self.settings.extended_market,
                            "severity": "warning",
                            "type": "timeout",
                            "message": "Extended REST 请求超时",
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
                            "symbol": self.settings.extended_market,
                            "severity": "error",
                            "type": "error",
                            "message": repr(exc),
                            "extra": {},
                        }
                    )

                try:
                    await asyncio.wait_for(
                        self.stop_event.wait(),
                        timeout=self.settings.extended_rest_interval_seconds,
                    )
                except asyncio.TimeoutError:
                    pass

    async def _extended_ws_stream(self, *, stream: str, path: str, metric_type: str) -> None:
        reconnects = 0
        window = Window()
        last_msg_at: float | None = None
        report_started_at = time.time()
        url = f"{self.extended_ws_url}/{path}"

        while not self.stop_event.is_set():
            try:
                reconnects += 1
                connect_t0 = time.perf_counter()
                async with websockets.connect(url, ping_interval=None) as ws:
                    connect_ms = (time.perf_counter() - connect_t0) * 1000
                    await self._save_incident(
                        {
                            "ts_ms": now_ms(),
                            "region": self.settings.region,
                            "stream": stream,
                            "symbol": self.settings.extended_market,
                            "severity": "info",
                            "type": "connect",
                            "message": f"{stream} 连接成功，耗时 {connect_ms:.2f} ms",
                            "extra": {"url": url, "connect_ms": rounded(connect_ms)},
                        }
                    )

                    while not self.stop_event.is_set():
                        try:
                            msg = await asyncio.wait_for(
                                ws.recv(),
                                timeout=self.settings.extended_timeout_seconds + 15,
                            )
                        except asyncio.TimeoutError:
                            window.timeouts += 1
                            await self._save_incident(
                                {
                                    "ts_ms": now_ms(),
                                    "region": self.settings.region,
                                    "stream": stream,
                                    "symbol": self.settings.extended_market,
                                    "severity": "warning",
                                    "type": "timeout",
                                    "message": f"{stream} 长时间没有收到消息",
                                    "extra": {"url": url},
                                }
                            )
                            break

                        current = time.time()
                        raw = msg if isinstance(msg, str) else msg.decode("utf-8", errors="ignore")
                        window.messages += 1
                        window.bytes += len(raw)

                        lag_ms = self._extended_event_lag_ms(raw) if metric_type == "event_lag" else None
                        if metric_type == "event_lag" and lag_ms is not None:
                            window.add_value(lag_ms)
                        elif last_msg_at is not None:
                            window.add_value((current - last_msg_at) * 1000)
                        last_msg_at = current

                        if current - report_started_at >= self.settings.report_seconds:
                            sample = {
                                "ts_ms": now_ms(),
                                "region": self.settings.region,
                                "stream": stream,
                                "symbol": self.settings.extended_market,
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
                                        "symbol": self.settings.extended_market,
                                        "severity": "warning",
                                        "type": "extended_lag_spike",
                                        "message": f"{stream} 最大消息 lag {sample['max_ms']:.2f} ms",
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
                        "symbol": self.settings.extended_market,
                        "severity": "error",
                        "type": "error",
                        "message": repr(exc),
                        "extra": {"url": url},
                    }
                )
                await asyncio.sleep(2)

    def _extended_order_config_errors(self) -> list[str]:
        missing = []
        if not self.settings.extended_api_key:
            missing.append("EXTENDED_API_KEY")
        if not self.settings.extended_stark_public_key:
            missing.append("EXTENDED_STARK_PUBLIC_KEY")
        if not self.settings.extended_stark_private_key:
            missing.append("EXTENDED_STARK_PRIVATE_KEY")
        if not self.settings.extended_vault:
            missing.append("EXTENDED_VAULT")
        return missing

    async def _save_extended_order_sample(
        self,
        *,
        stream: str,
        metric_type: str,
        latency_ms: float,
        messages: int = 1,
        timeouts: int = 0,
        extra: dict[str, Any] | None = None,
    ) -> None:
        sample = {
            "ts_ms": now_ms(),
            "region": self.settings.region,
            "stream": stream,
            "symbol": self.settings.extended_market,
            "metric_type": metric_type,
            "window_s": self.settings.extended_order_test_interval_seconds,
            "messages": messages,
            "bytes": 0,
            "reconnects": 0,
            "timeouts": timeouts,
            "count": 1 if timeouts == 0 else 0,
            "avg_ms": rounded(latency_ms) if timeouts == 0 else None,
            "p50_ms": rounded(latency_ms) if timeouts == 0 else None,
            "p95_ms": rounded(latency_ms) if timeouts == 0 else None,
            "p99_ms": rounded(latency_ms) if timeouts == 0 else None,
            "max_ms": rounded(latency_ms) if timeouts == 0 else None,
            "extra": extra or {},
        }
        await self._save_sample(sample)

    def _extended_order_side(self) -> Any:
        from x10.models.order import OrderSide

        return OrderSide.SELL if self.settings.extended_order_test_side == "SELL" else OrderSide.BUY

    async def _create_extended_client(self) -> Any:
        from x10.config import MAINNET_CONFIG, TESTNET_CONFIG
        from x10.core.stark_account import StarkPerpetualAccount
        from x10.perpetual.trading_client import PerpetualTradingClient

        config = TESTNET_CONFIG if self.settings.extended_env == "testnet" else MAINNET_CONFIG
        account = StarkPerpetualAccount(
            api_key=self.settings.extended_api_key,
            public_key=self.settings.extended_stark_public_key.lower(),
            private_key=self.settings.extended_stark_private_key.lower(),
            vault=self.settings.extended_vault,
        )
        return PerpetualTradingClient(config, account)

    def _extended_order_price(self, market: Any, side: Any) -> Decimal:
        from x10.models.order import OrderSide

        offset = Decimal(self.settings.extended_order_test_price_offset_pct) / Decimal("100")
        if side == OrderSide.BUY:
            ref = Decimal(market.market_stats.bid_price)
            raw_price = ref * (Decimal("1") - offset)
        else:
            ref = Decimal(market.market_stats.ask_price)
            raw_price = ref * (Decimal("1") + offset)
        return market.trading_config.round_price(raw_price)

    def _extended_order_quantity(self, market: Any) -> Decimal:
        if self.settings.extended_order_test_quantity:
            return market.trading_config.round_order_size(Decimal(self.settings.extended_order_test_quantity))
        return Decimal(market.trading_config.min_order_size)

    async def _extended_account_stream(self) -> None:
        stream = "extended_order_ws"
        if self._extended_order_config_errors():
            return

        reconnects = 0
        while not self.stop_event.is_set():
            try:
                from x10.config import MAINNET_CONFIG, TESTNET_CONFIG
                from x10.models.order import OrderStatus
                from x10.perpetual.stream_client import PerpetualStreamClient

                config = TESTNET_CONFIG if self.settings.extended_env == "testnet" else MAINNET_CONFIG
                stream_client = PerpetualStreamClient(api_url=config.endpoints.stream_url)
                reconnects += 1
                connect_t0 = time.perf_counter()
                async with stream_client.subscribe_to_account_updates(self.settings.extended_api_key) as account_stream:
                    connect_ms = (time.perf_counter() - connect_t0) * 1000
                    await self._save_incident(
                        {
                            "ts_ms": now_ms(),
                            "region": self.settings.region,
                            "stream": stream,
                            "symbol": self.settings.extended_market,
                            "severity": "info",
                            "type": "connect",
                            "message": f"{stream} 连接成功，耗时 {connect_ms:.2f} ms",
                            "extra": {"connect_ms": rounded(connect_ms), "reconnects": max(0, reconnects - 1)},
                        }
                    )

                    while not self.stop_event.is_set():
                        msg = await asyncio.wait_for(
                            account_stream.recv(),
                            timeout=self.settings.extended_order_test_timeout_seconds + 30,
                        )
                        orders = getattr(getattr(msg, "data", None), "orders", None) or []
                        for order in orders:
                            external_id = str(getattr(order, "external_id", "") or "")
                            status = getattr(order, "status", None)
                            now_t = time.perf_counter()
                            submitted = self.extended_order_submitted_at.pop(external_id, None)
                            if submitted is not None:
                                await self._save_extended_order_sample(
                                    stream=stream,
                                    metric_type="order_ws_ack",
                                    latency_ms=(now_t - submitted) * 1000,
                                    extra={"external_id": external_id, "status": str(status)},
                                )
                            cancel_submitted = self.extended_cancel_submitted_at.pop(external_id, None)
                            if cancel_submitted is not None and status == OrderStatus.CANCELLED:
                                await self._save_extended_order_sample(
                                    stream=stream,
                                    metric_type="cancel_ws_ack",
                                    latency_ms=(now_t - cancel_submitted) * 1000,
                                    extra={"external_id": external_id, "status": str(status)},
                                )
            except asyncio.CancelledError:
                raise
            except asyncio.TimeoutError:
                await self._save_incident(
                    {
                        "ts_ms": now_ms(),
                        "region": self.settings.region,
                        "stream": stream,
                        "symbol": self.settings.extended_market,
                        "severity": "warning",
                        "type": "timeout",
                        "message": "Extended 私有订单 WS 长时间没有收到消息",
                        "extra": {},
                    }
                )
            except Exception as exc:
                await self._save_incident(
                    {
                        "ts_ms": now_ms(),
                        "region": self.settings.region,
                        "stream": stream,
                        "symbol": self.settings.extended_market,
                        "severity": "error",
                        "type": "error",
                        "message": repr(exc),
                        "extra": {},
                    }
                )
                await asyncio.sleep(2)

    async def _extended_order_test(self) -> None:
        stream = "extended_order_place"
        missing = self._extended_order_config_errors()
        if missing:
            await self._save_incident(
                {
                    "ts_ms": now_ms(),
                    "region": self.settings.region,
                    "stream": stream,
                    "symbol": self.settings.extended_market,
                    "severity": "error",
                    "type": "config_error",
                    "message": f"Extended 下单测试缺少配置：{', '.join(missing)}",
                    "extra": {"missing": missing},
                }
            )
            return

        client = None
        try:
            from x10.models.order import TimeInForce

            client = await self._create_extended_client()
            markets = await client.markets_info.get_markets_dict()
            market = markets[self.settings.extended_market]

            await self._save_incident(
                {
                    "ts_ms": now_ms(),
                    "region": self.settings.region,
                    "stream": stream,
                    "symbol": self.settings.extended_market,
                    "severity": "info",
                    "type": "connect",
                    "message": "Extended 下单测试启动成功",
                    "extra": {"env": self.settings.extended_env, "market": self.settings.extended_market},
                }
            )

            while not self.stop_event.is_set():
                placed_order_id: int | None = None
                external_id = f"lat-{self.settings.region}-{uuid4().hex[:24]}"
                try:
                    side = self._extended_order_side()
                    quantity = self._extended_order_quantity(market)
                    price = self._extended_order_price(market, side)

                    submit_t = time.perf_counter()
                    self.extended_order_submitted_at[external_id] = submit_t
                    placed = await asyncio.wait_for(
                        client.place_order(
                            market_name=self.settings.extended_market,
                            amount_of_synthetic=quantity,
                            price=price,
                            side=side,
                            taker_fee=Decimal(self.settings.extended_order_test_taker_fee),
                            post_only=True,
                            time_in_force=TimeInForce.GTT,
                            external_id=external_id,
                        ),
                        timeout=self.settings.extended_order_test_timeout_seconds,
                    )
                    place_ms = (time.perf_counter() - submit_t) * 1000
                    placed_order_id = int(placed.data.id)
                    await self._save_extended_order_sample(
                        stream="extended_order_place",
                        metric_type="order_ack",
                        latency_ms=place_ms,
                        extra={
                            "external_id": external_id,
                            "order_id": placed_order_id,
                            "side": str(side),
                            "quantity": str(quantity),
                            "price": str(price),
                        },
                    )

                    cancel_t = time.perf_counter()
                    self.extended_cancel_submitted_at[external_id] = cancel_t
                    await asyncio.wait_for(
                        client.orders.cancel_order(placed_order_id),
                        timeout=self.settings.extended_order_test_timeout_seconds,
                    )
                    cancel_ms = (time.perf_counter() - cancel_t) * 1000
                    await self._save_extended_order_sample(
                        stream="extended_order_cancel",
                        metric_type="cancel_ack",
                        latency_ms=cancel_ms,
                        extra={"external_id": external_id, "order_id": placed_order_id},
                    )
                except asyncio.CancelledError:
                    raise
                except asyncio.TimeoutError:
                    await self._save_extended_order_sample(
                        stream="extended_order_place",
                        metric_type="order_ack",
                        latency_ms=0,
                        timeouts=1,
                        extra={"external_id": external_id, "order_id": placed_order_id},
                    )
                    await self._save_incident(
                        {
                            "ts_ms": now_ms(),
                            "region": self.settings.region,
                            "stream": "extended_order_place",
                            "symbol": self.settings.extended_market,
                            "severity": "warning",
                            "type": "timeout",
                            "message": "Extended 下单/撤单请求超时",
                            "extra": {"external_id": external_id, "order_id": placed_order_id},
                        }
                    )
                except Exception as exc:
                    await self._save_incident(
                        {
                            "ts_ms": now_ms(),
                            "region": self.settings.region,
                            "stream": "extended_order_place",
                            "symbol": self.settings.extended_market,
                            "severity": "error",
                            "type": "extended_order_error",
                            "message": repr(exc),
                            "extra": {"external_id": external_id, "order_id": placed_order_id},
                        }
                    )
                finally:
                    stale_before = time.perf_counter() - 300
                    self.extended_order_submitted_at = {
                        key: value for key, value in self.extended_order_submitted_at.items() if value > stale_before
                    }
                    self.extended_cancel_submitted_at = {
                        key: value for key, value in self.extended_cancel_submitted_at.items() if value > stale_before
                    }

                try:
                    await asyncio.wait_for(
                        self.stop_event.wait(),
                        timeout=self.settings.extended_order_test_interval_seconds,
                    )
                except asyncio.TimeoutError:
                    pass
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            await self._save_incident(
                {
                    "ts_ms": now_ms(),
                    "region": self.settings.region,
                    "stream": stream,
                    "symbol": self.settings.extended_market,
                    "severity": "error",
                    "type": "extended_order_error",
                    "message": repr(exc),
                    "extra": {},
                }
            )
        finally:
            if client is not None:
                await client.close()

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
