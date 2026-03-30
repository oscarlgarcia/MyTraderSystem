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


def _build_live_handler(
    writer: ParquetWriter,
    stats: dict[str, int],
    *,
    max_events: int,
    dedup_enabled: bool,
) -> Callable[[MarketEvent], None]:
    seen = set()

    def handler(event: MarketEvent) -> None:
        if dedup_enabled:
            event_key = _key(event)
            if event_key in seen:
                stats["duplicates_dropped"] += 1
                return
            seen.add(event_key)
        writer.add(event)
        stats["written"] += 1
        if stats["written"] >= max_events:
            raise StopIteration

    return handler


def collect_events(
    mode: str,
    cfg: AppConfig,
    max_events: int = 50,
    duration_s: Optional[float] = None,
    logger: Optional[logging.Logger] = None,
    compute_features_after: bool = False,
    max_buffer: int = 10_000,
    dedup_enabled: bool = True,
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

        snapshot_fn = _build_snapshot_fn(cfg)
        writer = ParquetWriter(base_dir=cfg.data_dir, env=cfg.env, flush_size=max_events, dedup=dedup_enabled)
        stats = {"written": 0, "duplicates_dropped": 0}
        handler = _build_live_handler(writer, stats, max_events=max_events, dedup_enabled=dedup_enabled)

        runner = ResilientRunner(
            stream_fn=stream,
            snapshot_fn=snapshot_fn,
            lag_threshold_seconds=5.0,
            max_buffer=max_buffer,
            dedup_enabled=dedup_enabled,
        )
        stop_on_complete = duration_s is not None
        runner.run(handler, stop_on_complete=stop_on_complete, max_retries=1)
        writer.flush()
        logger.info(
            "ingestion live complete",
            extra={
                "events_written": stats["written"],
                "duplicates_dropped": stats["duplicates_dropped"],
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
