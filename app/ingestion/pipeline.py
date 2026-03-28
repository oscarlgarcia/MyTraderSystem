"""
Funciones reutilizables para recolectar eventos de mercado para run_cycle.
"""

from __future__ import annotations

import time
import logging
from typing import Iterable, List, Optional
from datetime import datetime, timezone

from websockets.sync.client import connect
import httpx

from app.common.dto import MarketEvent, normalize_symbol
from app.config import AppConfig
from app.ingestion.client import build_ws_url, normalize_kline, normalize_trade, parse_message
from app.ingestion.resilience import ResilientRunner
from app.ingestion.storage import ParquetWriter
from app.features.pipeline import run_feature_pipeline


def _synthetic_events(max_events: int) -> List[MarketEvent]:
    now = time.time()
    events: List[MarketEvent] = []
    price = 100.0
    for i in range(max_events):
        ts = now + i
        price += 0.1  # ligera tendencia positiva
        events.append(
            MarketEvent(
                symbol=normalize_symbol("BTCUSDT"),
                event_ts=datetime.fromtimestamp(ts, tz=timezone.utc),
                price=price,
                size=0.01 + i * 0.001,
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
        for sym in cfg.symbols:
            url = f"{cfg.rest_base.rstrip('/')}/api/v3/klines"
            resp = httpx.get(url, params={"symbol": sym, "interval": "1m", "limit": 5}, timeout=5.0)
            resp.raise_for_status()
            for row in resp.json():
                payload = {"s": sym, "E": int(row[6]), "k": {"c": row[4], "q": row[5]}}
                events.append(normalize_kline(payload))
        return events

    return snapshot


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

    # modo live: intentar WS + snapshot, con fallback a datos sintéticos
    try:
        url = build_ws_url(cfg.ws_base, cfg.symbols)
        end_time = time.time() + duration_s if duration_s else None

        def stream():
            yield from _ws_stream(url, end_time=end_time)

        snapshot_fn = _build_snapshot_fn(cfg)
        writer = ParquetWriter(base_dir=cfg.data_dir, env=cfg.env, flush_size=max_events)
        stats = {"written": 0}

        def handler(ev: MarketEvent):
            writer.add(ev)
            stats["written"] += 1
            if stats["written"] >= max_events:
                raise StopIteration

        runner = ResilientRunner(
            stream_fn=stream,
            snapshot_fn=snapshot_fn,
            lag_threshold_seconds=5.0,
            max_buffer=max_buffer,
            dedup_enabled=dedup_enabled,
        )
        runner.run(handler, stop_on_complete=False, max_retries=1)
        writer.flush()
        logger.info(
            "ingestion live complete",
            extra={
                "events_written": stats["written"],
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
    except Exception as exc:  # pragma: no cover - ruta live no se prueba en unit tests
        logger.warning("live ingestion failed; falling back to dry", extra={"error": str(exc)})
        events_out = _synthetic_events(max_events)
        if compute_features_after:
            run_feature_pipeline(events_out)
        return events_out


def _read_from_writer_buffer(writer: ParquetWriter, limit: int) -> List[MarketEvent]:
    # ParquetWriter no expone buffer público; reutilizamos su buffer interno si existe,
    # en caso contrario devolvemos lista vacía para mantener typing.
    events: List[MarketEvent] = []
    buf = getattr(writer, "_buffer", None)
    if isinstance(buf, list):
        events.extend(buf[:limit])
    return events
