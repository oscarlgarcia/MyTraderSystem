"""
Punto de entrada puntual para ingesta en vivo (WS/REST) y escritura Parquet.

Uso:
    python -m app.ingestion.runner --env dev --duration 600
"""

from __future__ import annotations

import argparse
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Optional

from app.common.dto import MarketEvent
from app.config import load_config
from app.ingestion.client import normalize_kline, normalize_trade
from app.ingestion.checkpoints import CheckpointStore, default_checkpoint_path
from app.ingestion.resilience import ResilientRunner
from app.ingestion.sinks import ParquetEventSink
from app.ingestion.sources import BinanceSource, StaticSource, source_snapshot_fn
from app.ingestion.storage import ParquetWriter
from app.marketdata.raw_sink import JsonlRawSink, NullRawSink
from app.observability.logger import clear_trace_id  # noqa: F401  # exported for tests
from app.observability.logger import get_logger, get_trace_id, set_trace_id


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
    if not args.dry_run and isinstance(getattr(source, "raw_sink", None), NullRawSink):
        source.raw_sink = JsonlRawSink(cfg.data_dir / "raw", env=cfg.env)
    sink = ParquetEventSink(ParquetWriter(base_dir=cfg.data_dir, env=cfg.env, flush_size=args.flush_size))
    checkpoint_store = None if args.dry_run else CheckpointStore(default_checkpoint_path(cfg))

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
    if checkpoint_store is not None:
        try:
            checkpoint_state = checkpoint_store.load()
            logger.info(
                "checkpoint applied",
                extra={
                    "checkpoint_path": str(checkpoint_store.path),
                    "stream_keys": sorted(checkpoint_state.stream_cursors) if checkpoint_state is not None else [],
                    "checkpoint_last_event_ts": checkpoint_state.last_event_ts.isoformat() if checkpoint_state and checkpoint_state.last_event_ts else None,
                },
            )
            checkpoint_store.record_checkpoint_event(
                event_type="checkpoint_applied",
                trace_id=get_trace_id(),
                state=checkpoint_state,
                extra={"mode": "runner"},
            )
            runner.restore_checkpoint(checkpoint_state)
        except ValueError as exc:
            checkpoint_store.record_checkpoint_event(
                event_type="checkpoint_load_failed",
                trace_id=get_trace_id(),
                state=None,
                extra={"mode": "runner", "error": str(exc)},
            )
            logger.warning(
                "checkpoint recovery using empty state",
                extra={"checkpoint_path": str(checkpoint_store.path), "error": str(exc)},
            )

    completed_successfully = False
    try:
        runner.run(handler, stop_on_complete=True, max_retries=5)
        completed_successfully = True
    except StopIteration:
        completed_successfully = True
    finally:
        sink.close()
        if checkpoint_store is not None and completed_successfully:
            checkpoint_to_save = runner.export_checkpoint(
                metadata={
                    "env": cfg.env,
                    "mode": "runner",
                    "saved_at": datetime.now(timezone.utc).isoformat(),
                    "events_written": stats.events_written,
                    "reconnects": runner.metrics.reconnects,
                }
            )
            checkpoint_store.save(checkpoint_to_save)
            logger.info(
                "checkpoint saved",
                extra={
                    "checkpoint_path": str(checkpoint_store.path),
                    "stream_keys": sorted(checkpoint_to_save.stream_cursors),
                    "checkpoint_last_event_ts": checkpoint_to_save.last_event_ts.isoformat() if checkpoint_to_save.last_event_ts else None,
                    "recovery_audit_events_total": len(runner.recovery_audit_events),
                },
            )
            checkpoint_store.record_checkpoint_event(
                event_type="checkpoint_saved",
                trace_id=get_trace_id(),
                state=checkpoint_to_save,
                extra={"mode": "runner"},
            )
            for recovery_event in runner.recovery_audit_events:
                checkpoint_store.append_audit_event(
                    {
                        **recovery_event,
                        "event_type": "recovery_cursor_audit",
                        "checkpoint_path": str(checkpoint_store.path),
                    }
                )
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
