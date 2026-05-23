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

    def _stream_filter(self, streams: set[str] | None, params: list[Any]) -> str:
        if not streams:
            return ""
        placeholders = ",".join("?" for _ in streams)
        params.extend(sorted(streams))
        return f" AND stream IN ({placeholders})"

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

    def latest_samples(self, streams: set[str] | None = None) -> list[dict[str, Any]]:
        params: list[Any] = []
        stream_filter = self._stream_filter(streams, params)
        with self._lock:
            rows = self._conn.execute(
                f"""
                SELECT s.*
                FROM samples s
                JOIN (
                    SELECT stream, MAX(ts_ms) AS ts_ms
                    FROM samples
                    WHERE 1 = 1 {stream_filter}
                    GROUP BY stream
                ) latest
                ON latest.stream = s.stream AND latest.ts_ms = s.ts_ms
                ORDER BY s.stream
                """,
                params,
            ).fetchall()
        return [dict(row) for row in rows]

    def series(
        self,
        since_ms: int,
        until_ms: int,
        stream: str | None = None,
        streams: set[str] | None = None,
        bucket_ms: int | None = None,
    ) -> list[dict[str, Any]]:
        params: list[Any] = [since_ms, until_ms]
        where = "ts_ms >= ? AND ts_ms <= ?"
        if stream:
            where += " AND stream = ?"
            params.append(stream)
        else:
            where += self._stream_filter(streams, params)

        if bucket_ms and bucket_ms > 0:
            bucket_params: list[Any] = [since_ms, bucket_ms, since_ms, bucket_ms, *params]
            with self._lock:
                rows = self._conn.execute(
                    f"""
                    SELECT
                        (? + bucket * ?) AS ts_ms,
                        region,
                        stream,
                        symbol,
                        metric_type,
                        ROUND(AVG(window_s), 3) AS window_s,
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
                    FROM (
                        SELECT
                            CAST((ts_ms - ?) / ? AS INTEGER) AS bucket,
                            *
                        FROM samples
                        WHERE {where}
                    )
                    GROUP BY stream, metric_type, bucket
                    ORDER BY ts_ms ASC
                    """,
                    bucket_params,
                ).fetchall()
            return [dict(row) for row in rows]

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

    def summary(self, since_ms: int, until_ms: int, streams: set[str] | None = None) -> list[dict[str, Any]]:
        params: list[Any] = [since_ms, until_ms]
        stream_filter = self._stream_filter(streams, params)
        with self._lock:
            rows = self._conn.execute(
                f"""
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
                WHERE ts_ms >= ? AND ts_ms <= ? {stream_filter}
                GROUP BY stream, metric_type
                ORDER BY stream
                """,
                params,
            ).fetchall()
        return [dict(row) for row in rows]

    def incidents(
        self,
        since_ms: int,
        until_ms: int,
        streams: set[str] | None = None,
        limit: int = 200,
    ) -> list[dict[str, Any]]:
        params: list[Any] = [since_ms, until_ms]
        stream_filter = self._stream_filter(streams, params)
        params.append(limit)
        with self._lock:
            rows = self._conn.execute(
                f"""
                SELECT *
                FROM incidents
                WHERE ts_ms >= ? AND ts_ms <= ? {stream_filter}
                ORDER BY ts_ms DESC
                LIMIT ?
                """,
                params,
            ).fetchall()
        return [dict(row) for row in rows]

    def stability(self, since_ms: int, until_ms: int, streams: set[str] | None = None) -> list[dict[str, Any]]:
        params: list[Any] = [since_ms, until_ms]
        stream_filter = self._stream_filter(streams, params)
        with self._lock:
            sample_rows = self._conn.execute(
                f"""
                SELECT
                    stream,
                    metric_type,
                    COUNT(*) AS windows,
                    SUM(count) AS count,
                    SUM(messages) AS messages,
                    SUM(reconnects) AS reconnects,
                    SUM(timeouts) AS timeouts,
                    ROUND(AVG(p50_ms), 3) AS p50_ms,
                    ROUND(AVG(p95_ms), 3) AS p95_ms,
                    ROUND(MAX(max_ms), 3) AS max_ms,
                    ROUND(MAX(max_ms) - AVG(p50_ms), 3) AS max_minus_p50_ms,
                    ROUND(AVG(p95_ms) - AVG(p50_ms), 3) AS jitter_ms
                FROM samples
                WHERE ts_ms >= ? AND ts_ms <= ? {stream_filter}
                GROUP BY stream, metric_type
                """,
                params,
            ).fetchall()

            incident_params: list[Any] = [since_ms, until_ms]
            incident_filter = self._stream_filter(streams, incident_params)
            incident_rows = self._conn.execute(
                f"""
                SELECT
                    stream,
                    SUM(CASE WHEN severity = 'error' THEN 1 ELSE 0 END) AS errors,
                    SUM(CASE WHEN severity = 'warning' THEN 1 ELSE 0 END) AS warnings,
                    SUM(CASE WHEN type = 'timeout' THEN 1 ELSE 0 END) AS timeout_events,
                    SUM(CASE WHEN type = 'connect' THEN 1 ELSE 0 END) AS connect_events,
                    SUM(CASE WHEN type IN (
                        'error',
                        'extended_rest_error',
                        'extended_order_error',
                        'extended_fill_error',
                        'config_error'
                    ) THEN 1 ELSE 0 END)
                        AS failure_events
                FROM incidents
                WHERE ts_ms >= ? AND ts_ms <= ? {incident_filter}
                GROUP BY stream
                """,
                incident_params,
            ).fetchall()

        incident_by_stream = {row["stream"]: dict(row) for row in incident_rows}
        hours = max((until_ms - since_ms) / 3_600_000, 1 / 60)
        result: list[dict[str, Any]] = []
        for row in sample_rows:
            item = dict(row)
            incident = incident_by_stream.get(item["stream"], {})
            errors = int(incident.get("errors") or 0)
            warnings = int(incident.get("warnings") or 0)
            failures = int(incident.get("failure_events") or 0)
            messages = int(item.get("messages") or 0)
            timeouts = int(item.get("timeouts") or 0)
            reconnects = int(item.get("reconnects") or 0)
            item.update(
                {
                    "errors": errors,
                    "warnings": warnings,
                    "timeout_events": int(incident.get("timeout_events") or 0),
                    "connect_events": int(incident.get("connect_events") or 0),
                    "failure_events": failures,
                    "reconnects_per_hour": round(reconnects / hours, 3),
                    "timeouts_per_hour": round(timeouts / hours, 3),
                    "failures_per_hour": round(failures / hours, 3),
                    "error_rate_pct": round(failures / max(messages, 1) * 100, 3),
                }
            )
            result.append(item)
        return sorted(result, key=lambda item: item["stream"])
