"""
Punto de entrada puntual para ingesta en vivo (WS/REST) y escritura Parquet.

Uso:
    python -m app.ingestion.runner --env dev --duration 600
"""

from __future__ import annotations

import argparse
import time
from dataclasses import dataclass
from typing import Callable, Iterable, List, Optional

import httpx
from websockets.sync.client import connect

from app.common.dto import MarketEvent, normalize_symbol
from app.config import load_config
from app.ingestion.client import build_ws_url, normalize_kline, normalize_trade, parse_message
from app.ingestion.resilience import ResilientRunner
from app.ingestion.storage import ParquetWriter
from app.observability.logger import get_logger, set_trace_id
from app.observability.logger import clear_trace_id  # noqa: F401  # exported for tests


@dataclass
class IngestStats:
    events_written: int = 0
    files_touched: set[str] = None
    start_time: float = 0.0
    reconnects: int = 0
    last_lag_seconds: float = 0.0

    def __post_init__(self) -> None:
        if self.files_touched is None:
            self.files_touched = set()


def _parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Ingest runner (puntual)")
    parser.add_argument("--env", choices=["dev", "test"], default=None, help="Config environment")
    parser.add_argument("--duration", type=float, default=600.0, help="Duración en segundos antes de salir")
    parser.add_argument("--flush-size", type=int, default=500, help="Número de eventos antes de flush")
    parser.add_argument("--dry-run", action="store_true", help="Usar fixtures locales en lugar de red")
    return parser.parse_args(argv)


def _build_snapshot_fn(cfg) -> Callable[[], Iterable[MarketEvent]]:
    def snapshot() -> List[MarketEvent]:
        events: List[MarketEvent] = []
        for sym in cfg.symbols:
            # Binance klines: [open time, open, high, low, close, volume, close time, ...]
            url = f"{cfg.rest_base.rstrip('/')}/api/v3/klines"
            resp = httpx.get(url, params={"symbol": sym, "interval": "1m", "limit": 5}, timeout=5.0)
            resp.raise_for_status()
            for row in resp.json():
                payload = {
                    "s": sym,
                    "E": int(row[6]),  # close time
                    "k": {"c": row[4], "q": row[5]},
                }
                events.append(normalize_kline(payload))
        return events

    return snapshot


def _ws_stream(url: str, end_time: float | None = None) -> Iterable[MarketEvent]:
    """Generador que abre WS y emite MarketEvent hasta end_time (si se provee)."""
    with connect(url) as ws:
        while True:
            if end_time and time.time() >= end_time:
                break
            try:
                raw = ws.recv(timeout=1)
            except TimeoutError:
                continue
            yield parse_message(raw)


def _dry_stream() -> Iterable[MarketEvent]:
    """Fixture mínima para pruebas locales sin red."""
    now_ms = int(time.time() * 1000)
    samples = [
        {"s": "BTCUSDT", "E": now_ms, "p": "100.0", "q": "0.01"},
        {"s": "BTCUSDT", "E": now_ms + 60000, "k": {"c": "101.0", "q": "2.0"}},
    ]
    for payload in samples:
        if "k" in payload:
            yield normalize_kline(payload)
        else:
            yield normalize_trade(payload)


def run(argv: Optional[list[str]] = None) -> int:
    args = _parse_args(argv)
    cfg = load_config(args.env)
    logger = get_logger(name="ingest", level=cfg.log_level)
    trace_id = f"ingest-{int(time.time())}"
    set_trace_id(trace_id)
    logger.info(
        "ingest starting",
        extra={
            "trace_id": trace_id,
            "env": cfg.env,
            "symbols": cfg.symbols,
            "duration_secs": args.duration,
            "flush_size": args.flush_size,
            "dry_run": args.dry_run,
        },
    )

    writer = ParquetWriter(base_dir=cfg.data_dir, env=cfg.env, flush_size=args.flush_size)
    stats = IngestStats(start_time=time.time())

    if args.dry_run:
        base_stream = _dry_stream
        snapshot_fn = None
    else:
        url = build_ws_url(cfg.ws_base, cfg.symbols)
        snapshot_fn = _build_snapshot_fn(cfg)

        def base_stream():
            end_time = stats.start_time + (args.duration or 0)
            yield from _ws_stream(url, end_time=end_time)

    def timed_stream():
        end_time = stats.start_time + (args.duration or 0)
        for ev in base_stream():
            if args.duration and time.time() >= end_time:
                break
            yield ev

    def handler(ev: MarketEvent) -> None:
        writer.add(ev)
        stats.events_written += 1
        # Progreso visual: un punto por evento procesado.
        print(".", end="", flush=True)

    runner = ResilientRunner(
        stream_fn=timed_stream,
        snapshot_fn=snapshot_fn,
        lag_threshold_seconds=5.0,
    )

    try:
        runner.run(handler, stop_on_complete=True, max_retries=5)
    except StopIteration:
        pass
    finally:
        writer.flush()
        stats.reconnects = runner.metrics.reconnects
        stats.last_lag_seconds = runner.metrics.last_lag_seconds
        elapsed = time.time() - stats.start_time
        logger.info(
            "ingest finished",
            extra={
                "trace_id": trace_id,
                "env": cfg.env,
                "events_written": stats.events_written,
                "reconnects": stats.reconnects,
                "last_lag_seconds": stats.last_lag_seconds,
                "elapsed_secs": round(elapsed, 2),
                "data_dir": str(cfg.data_dir),
            },
        )
    return 0


if __name__ == "__main__":
    raise SystemExit(run())
