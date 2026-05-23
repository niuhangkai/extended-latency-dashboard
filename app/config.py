from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


def env(name: str, default: str = "", legacy: str | None = None) -> str:
    return os.getenv(name) or (os.getenv(legacy) if legacy else None) or default


@dataclass(frozen=True)
class Settings:
    region: str
    symbol: str
    streams: set[str]
    report_seconds: int
    db_path: Path
    api_key: str
    api_secret: str
    order_test_symbol: str
    order_test_side: str
    order_test_quantity: str
    order_test_price: str
    order_test_interval_seconds: int
    order_test_recv_window_ms: int
    order_test_timeout_seconds: int
    extended_market: str
    extended_env: str
    extended_rest_interval_seconds: int
    extended_timeout_seconds: int
    extended_api_key: str
    extended_stark_public_key: str
    extended_stark_private_key: str
    extended_vault: str
    extended_client_id: str
    extended_order_test_side: str
    extended_order_test_quantity: str
    extended_order_test_price_offset_pct: str
    extended_order_test_interval_seconds: int
    extended_order_test_timeout_seconds: int
    extended_order_test_taker_fee: str


def load_settings() -> Settings:
    symbol = env("EXCHANGE_SYMBOL", "BTCUSDT", "MEXC_SYMBOL").upper()
    streams = {
        item.strip()
        for item in env("EXCHANGE_STREAMS", "spot_bbo,spot_trades,spot_l2,contract_ping", "MEXC_STREAMS").split(",")
        if item.strip()
    }
    if "extended_order_test" in streams:
        streams.update({"extended_order_place", "extended_order_cancel", "extended_order_ws"})
    return Settings(
        region=env("EXCHANGE_REGION", "local", "MEXC_REGION"),
        symbol=symbol,
        streams=streams,
        report_seconds=max(1, int(env("EXCHANGE_REPORT_SECONDS", "5", "MEXC_REPORT_SECONDS"))),
        db_path=Path(env("EXCHANGE_DB_PATH", "data/latency.sqlite", "MEXC_DB_PATH")),
        api_key=os.getenv("MEXC_API_KEY", ""),
        api_secret=os.getenv("MEXC_API_SECRET", ""),
        order_test_symbol=(os.getenv("MEXC_ORDER_TEST_SYMBOL") or symbol).upper(),
        order_test_side=os.getenv("MEXC_ORDER_TEST_SIDE", "BUY").upper(),
        order_test_quantity=os.getenv("MEXC_ORDER_TEST_QUANTITY", "0.001"),
        order_test_price=os.getenv("MEXC_ORDER_TEST_PRICE", "100000"),
        order_test_interval_seconds=max(1, int(os.getenv("MEXC_ORDER_TEST_INTERVAL_SECONDS", "10"))),
        order_test_recv_window_ms=max(1, int(os.getenv("MEXC_ORDER_TEST_RECV_WINDOW_MS", "5000"))),
        order_test_timeout_seconds=max(1, int(os.getenv("MEXC_ORDER_TEST_TIMEOUT_SECONDS", "5"))),
        extended_market=os.getenv("EXTENDED_MARKET", "BTC-USD").upper(),
        extended_env=os.getenv("EXTENDED_ENV", "mainnet").lower(),
        extended_rest_interval_seconds=max(1, int(os.getenv("EXTENDED_REST_INTERVAL_SECONDS", "1"))),
        extended_timeout_seconds=max(1, int(os.getenv("EXTENDED_TIMEOUT_SECONDS", "5"))),
        extended_api_key=os.getenv("EXTENDED_API_KEY", ""),
        extended_stark_public_key=os.getenv("EXTENDED_STARK_PUBLIC_KEY", ""),
        extended_stark_private_key=os.getenv("EXTENDED_STARK_PRIVATE_KEY", ""),
        extended_vault=os.getenv("EXTENDED_VAULT", ""),
        extended_client_id=os.getenv("EXTENDED_CLIENT_ID", ""),
        extended_order_test_side=os.getenv("EXTENDED_ORDER_TEST_SIDE", "BUY").upper(),
        extended_order_test_quantity=os.getenv("EXTENDED_ORDER_TEST_QUANTITY", ""),
        extended_order_test_price_offset_pct=os.getenv("EXTENDED_ORDER_TEST_PRICE_OFFSET_PCT", "10"),
        extended_order_test_interval_seconds=max(1, int(os.getenv("EXTENDED_ORDER_TEST_INTERVAL_SECONDS", "15"))),
        extended_order_test_timeout_seconds=max(1, int(os.getenv("EXTENDED_ORDER_TEST_TIMEOUT_SECONDS", "10"))),
        extended_order_test_taker_fee=os.getenv("EXTENDED_ORDER_TEST_TAKER_FEE", "0.00025"),
    )
