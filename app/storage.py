from __future__ import annotations

import json
import sqlite3
import threading
import time
from pathlib import Path
from typing import Any


def now_ms() -> int:
    return int(time.time() * 1000)


class Storage:
    def __init__(self, db_path: Path) -> None:
        self.db_path = db_path
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = threading.RLock()
        self._conn = sqlite3.connect(self.db_path, check_same_thread=False)
        self._conn.row_factory = sqlite3.Row
        self.init()

    def init(self) -> None:
        with self._lock:
            self._conn.executescript(
                """
                PRAGMA journal_mode=WAL;
                PRAGMA synchronous=NORMAL;

                CREATE TABLE IF NOT EXISTS samples (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ts_ms INTEGER NOT NULL,
                    region TEXT NOT NULL,
                    stream TEXT NOT NULL,
                    symbol TEXT,
                    metric_type TEXT NOT NULL,
                    window_s REAL NOT NULL,
                    count INTEGER NOT NULL,
                    avg_ms REAL,
                    p50_ms REAL,
                    p95_ms REAL,
                    p99_ms REAL,
                    max_ms REAL,
                    messages INTEGER NOT NULL DEFAULT 0,
                    bytes INTEGER NOT NULL DEFAULT 0,
                    reconnects INTEGER NOT NULL DEFAULT 0,
                    timeouts INTEGER NOT NULL DEFAULT 0
                );

                CREATE INDEX IF NOT EXISTS idx_samples_ts ON samples(ts_ms);
                CREATE INDEX IF NOT EXISTS idx_samples_stream_ts ON samples(stream, ts_ms);

                CREATE TABLE IF NOT EXISTS incidents (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ts_ms INTEGER NOT NULL,
                    region TEXT NOT NULL,
                    stream TEXT NOT NULL,
                    symbol TEXT,
                    severity TEXT NOT NULL,
                    type TEXT NOT NULL,
                    message TEXT NOT NULL,
                    extra_json TEXT
                );

                CREATE INDEX IF NOT EXISTS idx_incidents_ts ON incidents(ts_ms);
                CREATE INDEX IF NOT EXISTS idx_incidents_stream_ts ON incidents(stream, ts_ms);
                """
            )
            self._conn.commit()

    def insert_sample(self, sample: dict[str, Any]) -> dict[str, Any]:
        row = {
            "ts_ms": int(sample["ts_ms"]),
            "region": sample["region"],
            "stream": sample["stream"],
            "symbol": sample.get("symbol"),
            "metric_type": sample["metric_type"],
            "window_s": float(sample["window_s"]),
            "count": int(sample.get("count") or 0),
            "avg_ms": sample.get("avg_ms"),
            "p50_ms": sample.get("p50_ms"),
            "p95_ms": sample.get("p95_ms"),
            "p99_ms": sample.get("p99_ms"),
            "max_ms": sample.get("max_ms"),
            "messages": int(sample.get("messages") or 0),
            "bytes": int(sample.get("bytes") or 0),
            "reconnects": int(sample.get("reconnects") or 0),
            "timeouts": int(sample.get("timeouts") or 0),
        }
        with self._lock:
            cur = self._conn.execute(
                """
                INSERT INTO samples (
                    ts_ms, region, stream, symbol, metric_type, window_s, count,
                    avg_ms, p50_ms, p95_ms, p99_ms, max_ms,
                    messages, bytes, reconnects, timeouts
                )
                VALUES (
                    :ts_ms, :region, :stream, :symbol, :metric_type, :window_s, :count,
                    :avg_ms, :p50_ms, :p95_ms, :p99_ms, :max_ms,
                    :messages, :bytes, :reconnects, :timeouts
                )
                """,
                row,
            )
            self._conn.commit()
            row["id"] = cur.lastrowid
        return row

    def insert_incident(self, incident: dict[str, Any]) -> dict[str, Any]:
        row = {
            "ts_ms": int(incident.get("ts_ms") or now_ms()),
            "region": incident["region"],
            "stream": incident["stream"],
            "symbol": incident.get("symbol"),
            "severity": incident["severity"],
            "type": incident["type"],
            "message": incident["message"],
            "extra_json": json.dumps(incident.get("extra") or {}, ensure_ascii=False),
        }
        with self._lock:
            cur = self._conn.execute(
                """
                INSERT INTO incidents (
                    ts_ms, region, stream, symbol, severity, type, message, extra_json
                )
                VALUES (
                    :ts_ms, :region, :stream, :symbol, :severity, :type, :message, :extra_json
                )
                """,
                row,
            )
            self._conn.commit()
            row["id"] = cur.lastrowid
        return row

    def latest_samples(self) -> list[dict[str, Any]]:
        with self._lock:
            rows = self._conn.execute(
                """
                SELECT s.*
                FROM samples s
                JOIN (
                    SELECT stream, MAX(ts_ms) AS ts_ms
                    FROM samples
                    GROUP BY stream
                ) latest
                ON latest.stream = s.stream AND latest.ts_ms = s.ts_ms
                ORDER BY s.stream
                """
            ).fetchall()
        return [dict(row) for row in rows]

    def series(self, since_ms: int, stream: str | None = None) -> list[dict[str, Any]]:
        params: list[Any] = [since_ms]
        where = "ts_ms >= ?"
        if stream:
            where += " AND stream = ?"
            params.append(stream)
        with self._lock:
            rows = self._conn.execute(
                f"""
                SELECT *
                FROM samples
                WHERE {where}
                ORDER BY ts_ms ASC
                """,
                params,
            ).fetchall()
        return [dict(row) for row in rows]

    def summary(self, since_ms: int) -> list[dict[str, Any]]:
        with self._lock:
            rows = self._conn.execute(
                """
                SELECT
                    stream,
                    metric_type,
                    COUNT(*) AS windows,
                    SUM(count) AS count,
                    ROUND(AVG(avg_ms), 3) AS avg_ms,
                    ROUND(AVG(p50_ms), 3) AS p50_ms,
                    ROUND(AVG(p95_ms), 3) AS p95_ms,
                    ROUND(AVG(p99_ms), 3) AS p99_ms,
                    ROUND(MAX(max_ms), 3) AS max_ms,
                    SUM(messages) AS messages,
                    SUM(bytes) AS bytes,
                    MAX(reconnects) AS reconnects,
                    SUM(timeouts) AS timeouts
                FROM samples
                WHERE ts_ms >= ?
                GROUP BY stream, metric_type
                ORDER BY stream
                """,
                [since_ms],
            ).fetchall()
        return [dict(row) for row in rows]

    def incidents(self, since_ms: int, limit: int = 200) -> list[dict[str, Any]]:
        with self._lock:
            rows = self._conn.execute(
                """
                SELECT *
                FROM incidents
                WHERE ts_ms >= ?
                ORDER BY ts_ms DESC
                LIMIT ?
                """,
                [since_ms, limit],
            ).fetchall()
        return [dict(row) for row in rows]
