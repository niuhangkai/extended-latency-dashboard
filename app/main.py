from __future__ import annotations

import asyncio
import time
from pathlib import Path
from typing import Any, Optional

from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from app.collector import MexcLatencyCollector
from app.config import load_settings
from app.storage import Storage


settings = load_settings()
storage = Storage(settings.db_path)
app = FastAPI(title="MEXC Latency Dashboard")
clients: set[WebSocket] = set()
collector: MexcLatencyCollector | None = None


async def broadcast(payload: dict[str, Any]) -> None:
    disconnected: list[WebSocket] = []
    for client in list(clients):
        try:
            await client.send_json(payload)
        except Exception:
            disconnected.append(client)
    for client in disconnected:
        clients.discard(client)


@app.on_event("startup")
async def startup() -> None:
    global collector
    collector = MexcLatencyCollector(settings, storage, broadcast)
    await collector.start()


@app.on_event("shutdown")
async def shutdown() -> None:
    if collector:
        await collector.stop()


static_dir = Path(__file__).parent / "static"
app.mount("/static", StaticFiles(directory=static_dir), name="static")


@app.get("/")
async def index() -> FileResponse:
    return FileResponse(static_dir / "index.html")


@app.get("/api/status")
async def status() -> dict[str, Any]:
    return {
        "region": settings.region,
        "symbol": settings.symbol,
        "streams": sorted(settings.streams),
        "report_seconds": settings.report_seconds,
        "latest": storage.latest_samples(),
    }


@app.get("/api/series")
async def series(minutes: int = 60, stream: Optional[str] = None) -> dict[str, Any]:
    since_ms = int((time.time() - minutes * 60) * 1000)
    return {"items": storage.series(since_ms, stream)}


@app.get("/api/summary")
async def summary(minutes: int = 60) -> dict[str, Any]:
    since_ms = int((time.time() - minutes * 60) * 1000)
    return {"items": storage.summary(since_ms)}


@app.get("/api/incidents")
async def incidents(minutes: int = 60) -> dict[str, Any]:
    since_ms = int((time.time() - minutes * 60) * 1000)
    return {"items": storage.incidents(since_ms)}


@app.websocket("/ws")
async def websocket_endpoint(websocket: WebSocket) -> None:
    await websocket.accept()
    clients.add(websocket)
    try:
        await websocket.send_json({"type": "hello", "data": await status()})
        while True:
            await websocket.receive_text()
    except WebSocketDisconnect:
        clients.discard(websocket)
    except asyncio.CancelledError:
        clients.discard(websocket)
        raise
    except Exception:
        clients.discard(websocket)
