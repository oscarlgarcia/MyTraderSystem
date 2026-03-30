"""
Punto de entrada puntual para ingesta en vivo (WS/REST) y escritura Parquet.

Uso:
    python -m app.ingestion.runner --env dev --duration 600
"""

from __future__ import annotations

import argparse
import time
from dataclasses import dataclass
from typing import Optional

from app.common.dto import MarketEvent
from app.config import load_config
from app.ingestion.client import normalize_kline, normalize_trade
from app.ingestion.resilience import ResilientRunner
from app.ingestion.sinks import ParquetEventSink
from app.ingestion.sources import BinanceSource, StaticSource, source_snapshot_fn
from app.ingestion.storage import ParquetWriter
from app.observability.logger import clear_trace_id  # noqa: F401  # exported for tests
from app.observability.logger import get_logger, set_trace_id


@dataclass
class IngestStats:
    events_written: int = 0
    start_time: float = 0.0
    reconnects: int = 0
    last_lag_seconds: float = 0.0


def _parse_args(argv: Optional[list[str]] = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Ingest runner (puntual)")
    parser.add_argument("--env", choices=["dev", "test"], default=None, help="Config environment")
    parser.add_argument("--duration", type=float, default=600.0, help="Duracion en segundos antes de salir")
    parser.add_argument("--flush-size", type=int, default=500, help="Numero de eventos antes de flush")
    parser.add_argument("--dry-run", action="store_true", help="Usar fixtures locales en lugar de red")
    return parser.parse_args(argv)


def _dry_source() -> StaticSource:
    now_ms = int(time.time() * 1000)
    payloads = [
        {"s": "BTCUSDT", "E": now_ms, "p": "100.0", "q": "0.01"},
        {"s": "BTCUSDT", "E": now_ms + 60000, "k": {"c": "101.0", "q": "2.0"}},
    ]
    events = []
    for payload in payloads:
        if "k" in payload:
            events.append(normalize_kline(payload))
        else:
            events.append(normalize_trade(payload))
    return StaticSource(events=events)


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

    stats = IngestStats(start_time=time.time())
    source = _dry_source() if args.dry_run else BinanceSource(cfg)
    sink = ParquetEventSink(ParquetWriter(base_dir=cfg.data_dir, env=cfg.env, flush_size=args.flush_size))

    def stream():
        end_time = stats.start_time + (args.duration or 0)
        for event in source.stream(end_time=end_time):
            if args.duration and time.time() >= end_time:
                break
            yield event

    def handler(ev: MarketEvent) -> None:
        sink.add(ev)
        stats.events_written += 1
        print(".", end="", flush=True)

    runner = ResilientRunner(
        stream_fn=stream,
        snapshot_fn=None if args.dry_run else source_snapshot_fn(source),
        lag_threshold_seconds=5.0,
    )

    try:
        runner.run(handler, stop_on_complete=True, max_retries=5)
    except StopIteration:
        pass
    finally:
        sink.close()
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
