"""
Reusable helpers to collect market events for run_cycle.
"""

from __future__ import annotations

import logging
import time
from datetime import datetime, timezone
from typing import Callable, Iterable, List, Optional

import httpx
from websockets.sync.client import connect

from app.common.dto import MarketEvent, normalize_symbol
from app.config import AppConfig
from app.features.pipeline import run_feature_pipeline
from app.ingestion.client import _key, build_ws_url, normalize_kline, parse_message
from app.ingestion.resilience import ResilientRunner
from app.ingestion.storage import ParquetWriter


def _synthetic_events(max_events: int) -> List[MarketEvent]:
    now = time.time()
    events: List[MarketEvent] = []
    price = 100.0
    for index in range(max_events):
        ts = now + index
        price += 0.1
        events.append(
            MarketEvent(
                symbol=normalize_symbol("BTCUSDT"),
                event_ts=datetime.fromtimestamp(ts, tz=timezone.utc),
                price=price,
                size=0.01 + index * 0.001,
                source="trade",
                metadata={"mode": "dry"},
            )
        )
    return events


def _ws_stream(url: str, end_time: float | None = None) -> Iterable[MarketEvent]:
    with connect(url) as ws:
        while True:
            if end_time and time.time() >= end_time:
                break
            try:
                raw = ws.recv(timeout=1)
            except TimeoutError:
                continue
            yield parse_message(raw)


def _build_snapshot_fn(cfg: AppConfig):
    def snapshot():
        events: List[MarketEvent] = []
        for symbol in cfg.symbols:
            url = f"{cfg.rest_base.rstrip('/')}/api/v3/klines"
            resp = httpx.get(url, params={"symbol": symbol, "interval": "1m", "limit": 5}, timeout=5.0)
            resp.raise_for_status()
            for row in resp.json():
                payload = {"s": symbol, "E": int(row[6]), "k": {"c": row[4], "q": row[5]}}
                events.append(normalize_kline(payload))
        return events

    return snapshot


class _LiveBatchHandler:
    def __init__(
        self,
        writer: ParquetWriter,
        stats: dict[str, int],
        *,
        max_events: int,
        dedup_enabled: bool,
        batch_size: int,
    ) -> None:
        self.writer = writer
        self.stats = stats
        self.max_events = max_events
        self.dedup_enabled = dedup_enabled
        self.batch_size = max(1, batch_size)
        self.seen = set()
        self.pending: List[MarketEvent] = []

    def __call__(self, event: MarketEvent) -> None:
        if self.dedup_enabled:
            event_key = _key(event)
            if event_key in self.seen:
                self.stats["duplicates_dropped"] += 1
                return
            self.seen.add(event_key)
        self.pending.append(event)
        if self.stats["written"] + len(self.pending) >= self.max_events:
            self._flush_pending()
            if self.stats["written"] >= self.max_events:
                raise StopIteration
            return
        if len(self.pending) >= self.batch_size:
            self._flush_pending()

    def close(self) -> None:
        self._flush_pending()

    def _flush_pending(self) -> None:
        if not self.pending:
            return
        batch = list(self.pending)
        self.pending.clear()
        self.writer.add(batch)
        self.stats["written"] += len(batch)


def _build_live_handler(
    writer: ParquetWriter,
    stats: dict[str, int],
    *,
    max_events: int,
    dedup_enabled: bool,
    batch_size: int = 1,
) -> _LiveBatchHandler:
    return _LiveBatchHandler(
        writer,
        stats,
        max_events=max_events,
        dedup_enabled=dedup_enabled,
        batch_size=batch_size,
    )


def collect_events(
    mode: str,
    cfg: AppConfig,
    max_events: int = 50,
    duration_s: Optional[float] = None,
    logger: Optional[logging.Logger] = None,
    compute_features_after: bool = False,
    max_buffer: int = 10_000,
    dedup_enabled: bool = True,
    batch_size: int = 1,
    snapshot_enabled: bool = True,
    summary_logging: bool = True,
) -> List[MarketEvent]:
    logger = logger or logging.getLogger("ingest")
    if mode == "dry":
        events_out = _synthetic_events(max_events)
        if compute_features_after:
            run_feature_pipeline(events_out)
        return events_out

    try:
        url = build_ws_url(cfg.ws_base, cfg.symbols)
        end_time = time.time() + duration_s if duration_s else None

        def stream():
            yield from _ws_stream(url, end_time=end_time)

        snapshot_fn = _build_snapshot_fn(cfg) if snapshot_enabled else None
        writer = ParquetWriter(base_dir=cfg.data_dir, env=cfg.env, flush_size=max_events, dedup=dedup_enabled)
        stats = {"written": 0, "duplicates_dropped": 0}
        handler = _build_live_handler(
            writer,
            stats,
            max_events=max_events,
            dedup_enabled=dedup_enabled,
            batch_size=batch_size,
        )

        runner = ResilientRunner(
            stream_fn=stream,
            snapshot_fn=snapshot_fn,
            lag_threshold_seconds=5.0,
            max_buffer=max_buffer,
            dedup_enabled=dedup_enabled,
        )
        stop_on_complete = duration_s is not None
        try:
            runner.run(handler, stop_on_complete=stop_on_complete, max_retries=1)
        finally:
            handler.close()
            writer.flush()
        if summary_logging:
            logger.info(
                "ingestion live complete",
                extra={
                    "events_written": stats["written"],
                    "duplicates_dropped": stats["duplicates_dropped"],
                    "batch_size": max(1, batch_size),
                    "env": cfg.env,
                    "reconnects": runner.metrics.reconnects,
                    "buffer_skipped": runner.metrics.buffer_skipped,
                    "max_latency_seconds": runner.metrics.max_latency_seconds,
                },
            )
        events_out = _read_from_writer_buffer(writer, stats["written"])
        if compute_features_after:
            run_feature_pipeline(events_out)
        return events_out
    except Exception as exc:  # pragma: no cover - live path is not fully unit tested
        logger.warning("live ingestion failed; falling back to dry", extra={"error": str(exc)})
        events_out = _synthetic_events(max_events)
        if compute_features_after:
            run_feature_pipeline(events_out)
        return events_out


def _read_from_writer_buffer(writer: ParquetWriter, limit: int) -> List[MarketEvent]:
    events: List[MarketEvent] = []
    buf = getattr(writer, "buffer", None)
    if isinstance(buf, list):
        events.extend(buf[:limit])
    return events
