from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


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
    extended_rest_interval_seconds: int
    extended_timeout_seconds: int


def load_settings() -> Settings:
    symbol = os.getenv("MEXC_SYMBOL", "BTCUSDT").upper()
    streams = {
        item.strip()
        for item in os.getenv("MEXC_STREAMS", "spot_bbo,spot_trades,spot_l2,contract_ping").split(",")
        if item.strip()
    }
    return Settings(
        region=os.getenv("MEXC_REGION", "local"),
        symbol=symbol,
        streams=streams,
        report_seconds=max(1, int(os.getenv("MEXC_REPORT_SECONDS", "5"))),
        db_path=Path(os.getenv("MEXC_DB_PATH", "data/latency.sqlite")),
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
        extended_rest_interval_seconds=max(1, int(os.getenv("EXTENDED_REST_INTERVAL_SECONDS", "1"))),
        extended_timeout_seconds=max(1, int(os.getenv("EXTENDED_TIMEOUT_SECONDS", "5"))),
    )
