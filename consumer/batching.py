"""Pure (stdlib-only) helpers for the Kafka -> object-store consumer.

Kept free of third-party imports so they can be unit tested anywhere.
"""
from __future__ import annotations

import json
import uuid
from datetime import datetime, timezone
from typing import Any


def table_from_topic(topic: str) -> str:
    """'banking_server.public.customers' -> 'customers'."""
    return topic.rsplit(".", 1)[-1]


def parse_event(raw: bytes | str | None) -> dict[str, Any] | None:
    """Turn a Debezium change event into a flat row, or None to skip it.

    Works with and without the Kafka Connect schema envelope
    (``{"schema": ..., "payload": {...}}`` vs. the bare ``{...}`` event).
    Deletes / tombstones carry no ``after`` image and are skipped.
    Adds ``_op`` and ``_ts_ms`` so downstream models can order versions of a row.
    """
    if raw is None:
        return None
    if isinstance(raw, bytes):
        raw = raw.decode("utf-8")
    event = json.loads(raw)
    if not isinstance(event, dict):
        return None
    payload = event.get("payload", event)
    if not isinstance(payload, dict):
        return None
    after = payload.get("after")
    if not after:
        return None
    row = dict(after)
    row["_op"] = payload.get("op")
    row["_ts_ms"] = payload.get("ts_ms")
    return row


def object_key(table: str, now: datetime | None = None, token: str | None = None) -> str:
    """Unique, date-partitioned key: customers/date=2026-11-06/customers_101530123456_ab12cd34.parquet"""
    now = now or datetime.now(timezone.utc)
    token = token or uuid.uuid4().hex[:8]
    return f"{table}/date={now:%Y-%m-%d}/{table}_{now:%H%M%S%f}_{token}.parquet"


class BatchBuffer:
    """Per-table row buffer with size- and age-based flush triggers."""

    def __init__(self, batch_size: int = 50, flush_interval: float = 10.0):
        if batch_size < 1:
            raise ValueError("batch_size must be >= 1")
        self.batch_size = batch_size
        self.flush_interval = flush_interval
        self._rows: dict[str, list[dict[str, Any]]] = {}
        self._first_added: float | None = None

    def add(self, table: str, row: dict[str, Any], now: float) -> None:
        if self._first_added is None:
            self._first_added = now
        self._rows.setdefault(table, []).append(row)

    def __len__(self) -> int:
        return sum(len(v) for v in self._rows.values())

    def should_flush(self, now: float) -> bool:
        if not self._rows or len(self) == 0:
            return False
        if any(len(v) >= self.batch_size for v in self._rows.values()):
            return True
        return self._first_added is not None and (now - self._first_added) >= self.flush_interval

    def drain(self) -> dict[str, list[dict[str, Any]]]:
        out = {t: r for t, r in self._rows.items() if r}
        self._rows = {}
        self._first_added = None
        return out
