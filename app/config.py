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


def load_settings() -> Settings:
    streams = {
        item.strip()
        for item in os.getenv("MEXC_STREAMS", "spot_bbo,spot_trades,spot_l2,contract_ping").split(",")
        if item.strip()
    }
    return Settings(
        region=os.getenv("MEXC_REGION", "local"),
        symbol=os.getenv("MEXC_SYMBOL", "BTCUSDT").upper(),
        streams=streams,
        report_seconds=max(1, int(os.getenv("MEXC_REPORT_SECONDS", "5"))),
        db_path=Path(os.getenv("MEXC_DB_PATH", "data/latency.sqlite")),
    )
