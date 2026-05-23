from __future__ import annotations

import asyncio
import os
from pathlib import Path
from typing import Any, Optional

import httpx
from fastapi import FastAPI, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse
from fastapi.staticfiles import StaticFiles

from app.collector import ExchangeLatencyCollector
from app.config import load_settings
from app.storage import Storage
from app.storage import now_ms


settings = load_settings()
storage = Storage(settings.db_path)
app = FastAPI(title="Exchange Latency Dashboard")
clients: set[WebSocket] = set()
collector: ExchangeLatencyCollector | None = None
placement: dict[str, Any] | None = None

EXTENDED_TARGET_LOCATION = {
    "provider": "aws",
    "region": "ap-northeast-1",
    "az": "ap-northeast-1a",
    "az_id": "apne1-az4",
}


async def broadcast(payload: dict[str, Any]) -> None:
    disconnected: list[WebSocket] = []
    for client in list(clients):
        try:
            await client.send_json(payload)
        except Exception:
            disconnected.append(client)
    for client in disconnected:
        clients.discard(client)


async def _aws_metadata(client: httpx.AsyncClient, path: str, token: str) -> str | None:
    response = await client.get(
        f"http://169.254.169.254/latest/{path}",
        headers={"X-aws-ec2-metadata-token": token},
    )
    if response.status_code >= 400:
        return None
    return response.text.strip()


async def detect_placement() -> dict[str, Any]:
    configured_provider = os.getenv("EXCHANGE_CLOUD_PROVIDER", "").lower()
    if configured_provider == "aws" or os.getenv("EXCHANGE_AWS_AZ_ID"):
        return {
            "provider": "aws",
            "region": os.getenv("EXCHANGE_AWS_REGION") or "ap-northeast-1",
            "az": os.getenv("EXCHANGE_AWS_AZ"),
            "az_id": os.getenv("EXCHANGE_AWS_AZ_ID"),
            "subnet_id": os.getenv("EXCHANGE_AWS_SUBNET_ID"),
            "vpc_id": os.getenv("EXCHANGE_AWS_VPC_ID"),
            "instance_id": os.getenv("EXCHANGE_INSTANCE_ID"),
            "private_ip": os.getenv("EXCHANGE_PRIVATE_IP"),
            "public_ip": os.getenv("EXCHANGE_PUBLIC_IP"),
        }
    if configured_provider == "vultr":
        return {
            "provider": "vultr",
            "region": settings.region,
            "az_match": "cross_cloud",
            "note": "Vultr 节点不属于 AWS AZ，不能和 Extended 做同 AZ 判定。",
        }

    aws_timeout = httpx.Timeout(0.45, connect=0.2)
    try:
        async with httpx.AsyncClient(timeout=aws_timeout) as client:
            token_response = await client.put(
                "http://169.254.169.254/latest/api/token",
                headers={"X-aws-ec2-metadata-token-ttl-seconds": "21600"},
            )
            token_response.raise_for_status()
            token = token_response.text
            identity_response = await client.get(
                "http://169.254.169.254/latest/dynamic/instance-identity/document",
                headers={"X-aws-ec2-metadata-token": token},
            )
            identity_response.raise_for_status()
            identity = identity_response.json()

            macs = await _aws_metadata(client, "meta-data/network/interfaces/macs/", token)
            mac = (macs or "").splitlines()[0].rstrip("/") if macs else ""
            subnet_id = await _aws_metadata(client, f"meta-data/network/interfaces/macs/{mac}/subnet-id", token) if mac else None
            vpc_id = await _aws_metadata(client, f"meta-data/network/interfaces/macs/{mac}/vpc-id", token) if mac else None
            public_ipv4s = await _aws_metadata(client, f"meta-data/network/interfaces/macs/{mac}/public-ipv4s", token) if mac else None
            az_id = await _aws_metadata(client, "meta-data/placement/availability-zone-id", token)

            return {
                "provider": "aws",
                "region": identity.get("region"),
                "az": identity.get("availabilityZone"),
                "az_id": az_id,
                "subnet_id": subnet_id,
                "vpc_id": vpc_id,
                "instance_id": identity.get("instanceId"),
                "private_ip": identity.get("privateIp"),
                "public_ip": (public_ipv4s or "").splitlines()[0] if public_ipv4s else None,
            }
    except Exception:
        pass

    if settings.region.lower().startswith("vultr"):
        return {
            "provider": "vultr",
            "region": settings.region,
            "az_match": "cross_cloud",
            "note": "Vultr 节点不属于 AWS AZ，不能和 Extended 做同 AZ 判定。",
        }

    return {
        "provider": "unknown",
        "region": settings.region,
        "az_match": "unknown",
        "note": "未检测到 AWS EC2 metadata；如果这是本地或非 AWS 节点，将不做同 AZ 判定。",
    }


def placement_status() -> dict[str, Any]:
    current = placement or {
        "provider": "unknown",
        "region": settings.region,
        "az_match": "unknown",
    }
    status = dict(current)
    target = dict(EXTENDED_TARGET_LOCATION)
    status["extended_target"] = target

    if status.get("provider") == "aws":
        status["az_match"] = "same" if status.get("az_id") == target["az_id"] else "different"
    elif status.get("provider") == "vultr":
        status["az_match"] = "cross_cloud"
    else:
        status["az_match"] = status.get("az_match") or "unknown"
    return status


def resolve_range(
    minutes: int | None = 60,
    since_ms: int | None = None,
    until_ms: int | None = None,
) -> tuple[int, int]:
    end_ms = until_ms or now_ms()
    if since_ms is not None:
        start_ms = since_ms
    else:
        window_minutes = max(1, min(minutes or 60, 60 * 24 * 30))
        start_ms = end_ms - window_minutes * 60 * 1000
    if start_ms > end_ms:
        start_ms, end_ms = end_ms, start_ms
    return start_ms, end_ms


def series_bucket_ms(since_ms: int, until_ms: int) -> int | None:
    range_ms = max(1, until_ms - since_ms)
    target_points_per_stream = 900
    raw_step_ms = range_ms // target_points_per_stream
    if raw_step_ms <= 10_000:
        return None

    minute = 60_000
    buckets = [
        15_000,
        30_000,
        minute,
        2 * minute,
        5 * minute,
        10 * minute,
        15 * minute,
        30 * minute,
        60 * minute,
        2 * 60 * minute,
        4 * 60 * minute,
    ]
    for bucket in buckets:
        if raw_step_ms <= bucket:
            return bucket
    return buckets[-1]


@app.on_event("startup")
async def startup() -> None:
    global collector, placement
    placement = await detect_placement()
    collector = ExchangeLatencyCollector(settings, storage, broadcast)
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
        "extended_market": settings.extended_market,
        "extended_env": settings.extended_env,
        "streams": sorted(settings.streams),
        "report_seconds": settings.report_seconds,
        "placement": placement_status(),
        "latest": storage.latest_samples(settings.streams),
    }


@app.get("/api/series")
async def series(
    minutes: int = 60,
    stream: Optional[str] = None,
    since_ms: Optional[int] = None,
    until_ms: Optional[int] = None,
) -> dict[str, Any]:
    start_ms, end_ms = resolve_range(minutes, since_ms, until_ms)
    bucket_ms = series_bucket_ms(start_ms, end_ms)
    return {
        "items": storage.series(start_ms, end_ms, stream, settings.streams, bucket_ms),
        "range": {"since_ms": start_ms, "until_ms": end_ms},
        "bucket_ms": bucket_ms,
    }


@app.get("/api/summary")
async def summary(
    minutes: int = 60,
    since_ms: Optional[int] = None,
    until_ms: Optional[int] = None,
) -> dict[str, Any]:
    start_ms, end_ms = resolve_range(minutes, since_ms, until_ms)
    return {"items": storage.summary(start_ms, end_ms, settings.streams), "range": {"since_ms": start_ms, "until_ms": end_ms}}


@app.get("/api/incidents")
async def incidents(
    minutes: int = 60,
    since_ms: Optional[int] = None,
    until_ms: Optional[int] = None,
) -> dict[str, Any]:
    start_ms, end_ms = resolve_range(minutes, since_ms, until_ms)
    return {"items": storage.incidents(start_ms, end_ms, settings.streams), "range": {"since_ms": start_ms, "until_ms": end_ms}}


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
