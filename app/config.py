from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path


DEFAULT_STREAMS = ",".join(
    [
        "extended_rest",
        "extended_bbo",
        "extended_l2",
        "extended_trades",
        "extended_mark",
        "extended_index",
        "extended_order_test",
    ]
)


@dataclass(frozen=True)
class Settings:
    region: str
    streams: set[str]
    report_seconds: int
    db_path: Path
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
    streams = {
        item.strip()
        for item in os.getenv("EXCHANGE_STREAMS", DEFAULT_STREAMS).split(",")
        if item.strip()
    }
    if "extended_order_test" in streams:
        streams.update({"extended_order_place", "extended_order_cancel", "extended_order_ws"})
    return Settings(
        region=os.getenv("EXCHANGE_REGION", "local"),
        streams=streams,
        report_seconds=max(1, int(os.getenv("EXCHANGE_REPORT_SECONDS", "5"))),
        db_path=Path(os.getenv("EXCHANGE_DB_PATH", "data/latency.sqlite")),
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
