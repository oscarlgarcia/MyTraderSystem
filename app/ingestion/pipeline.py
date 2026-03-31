"""
Reusable helpers to collect market events for run_cycle.
"""

from __future__ import annotations

import logging
import time
from datetime import datetime, timezone
from typing import List, Optional

from app.common.dto import MarketEvent, normalize_symbol
from app.config import AppConfig
from app.features.pipeline import run_feature_pipeline
from app.ingestion.client import _key
from app.ingestion.errors import ErrorPolicy, IngestionError, resolve_error_policy
from app.ingestion.resilience import ResilientRunner
from app.ingestion.sinks import EventSink, ParquetEventSink
from app.ingestion.sources import BinanceSource, Source, source_snapshot_fn
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


def _emit_runtime_warnings(
    logger: logging.Logger,
    runner: ResilientRunner,
    *,
    lag_warn_threshold: float | None,
    buffer_warn_threshold: int | None,
) -> None:
    if buffer_warn_threshold is not None and runner.metrics.buffer_skipped > buffer_warn_threshold:
        logger.warning(
            "ingestion buffer pressure warning",
            extra={
                "buffer_skipped": runner.metrics.buffer_skipped,
                "buffer_warn_threshold": buffer_warn_threshold,
                "buffer_size": runner.metrics.buffer_size,
            },
        )
    if lag_warn_threshold is not None and runner.metrics.max_latency_seconds > lag_warn_threshold:
        logger.warning(
            "ingestion latency warning",
            extra={
                "max_latency_seconds": runner.metrics.max_latency_seconds,
                "lag_warn_threshold": lag_warn_threshold,
                "last_latency_seconds": runner.metrics.last_latency_seconds,
            },
        )


def _emit_ingestion_summary(
    logger: logging.Logger,
    *,
    mode: str,
    cfg: AppConfig,
    events_in: int,
    events_out: int,
    reconnects: int,
    buffer_skipped: int,
    max_latency_seconds: float,
    dedup_on: bool,
    batch_size: int,
    duplicates_dropped: int = 0,
    result: str = "ok",
    error_policy: str = "fail_fast",
    error_category: str | None = None,
    error_severity: str | None = None,
    rejected_payloads: int = 0,
    error_sink_failures: int = 0,
) -> None:
    payload = {
        "mode": mode,
        "env": cfg.env,
        "events_in": int(events_in),
        "events_out": int(events_out),
        "reconnects": int(reconnects),
        "buffer_skipped": int(buffer_skipped),
        "max_latency_seconds": float(max_latency_seconds),
        "dedup_on": bool(dedup_on),
        "batch_size": int(max(1, batch_size)),
        "duplicates_dropped": int(duplicates_dropped),
        "rejected_payloads": int(rejected_payloads),
        "error_sink_failures": int(error_sink_failures),
        "result": result,
        "error_policy": error_policy,
    }
    if error_category is not None:
        payload["error_category"] = error_category
    if error_severity is not None:
        payload["error_severity"] = error_severity
    logger.info(
        "ingestion summary",
        extra=payload,
    )


class _LiveBatchHandler:
    def __init__(
        self,
        sink: EventSink,
        stats: dict[str, int],
        *,
        max_events: int,
        dedup_enabled: bool,
        batch_size: int,
    ) -> None:
        self.sink = sink
        self.stats = stats
        self.max_events = max_events
        self.dedup_enabled = dedup_enabled
        self.batch_size = max(1, batch_size)
        self.seen = set()
        self.pending: List[MarketEvent] = []
        self.events: List[MarketEvent] = []

    def __call__(self, event: MarketEvent) -> None:
        if self.dedup_enabled:
            event_key = _key(event)
            if event_key in self.seen:
                self.stats["duplicates_dropped"] += 1
                return
            self.seen.add(event_key)
        self.events.append(event)
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
        self.sink.add(batch)
        self.stats["written"] += len(batch)


def _build_live_handler(
    sink: EventSink,
    stats: dict[str, int],
    *,
    max_events: int,
    dedup_enabled: bool,
    batch_size: int = 1,
) -> _LiveBatchHandler:
    return _LiveBatchHandler(
        sink,
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
    lag_warn_threshold: float | None = None,
    buffer_warn_threshold: int | None = None,
    allow_live_fallback: bool = False,
    source: Source | None = None,
    sink: EventSink | None = None,
    error_policy: ErrorPolicy | None = None,
) -> List[MarketEvent]:
    logger = logger or logging.getLogger("ingest")
    effective_error_policy = resolve_error_policy(error_policy, allow_live_fallback=allow_live_fallback)
    if mode == "dry":
        events_out = _synthetic_events(max_events)
        source_rejected = getattr(source, "stats", None)
        if summary_logging:
            _emit_ingestion_summary(
                logger,
                mode="dry",
                cfg=cfg,
                events_in=len(events_out),
                events_out=len(events_out),
                reconnects=0,
                buffer_skipped=0,
                max_latency_seconds=0.0,
                dedup_on=dedup_enabled,
                batch_size=batch_size,
                duplicates_dropped=0,
                error_policy=effective_error_policy,
                rejected_payloads=getattr(source_rejected, "rejected_payloads", 0),
                error_sink_failures=getattr(source_rejected, "error_sink_failures", 0),
            )
        if compute_features_after:
            run_feature_pipeline(events_out)
        return events_out

    try:
        source_impl = source or BinanceSource(cfg)
        source_stats = getattr(source_impl, "stats", None)
        end_time = time.time() + duration_s if duration_s else None

        def stream():
            yield from source_impl.stream(end_time=end_time)

        snapshot_fn = source_snapshot_fn(source_impl) if snapshot_enabled else None
        sink_impl = sink or ParquetEventSink(
            ParquetWriter(base_dir=cfg.data_dir, env=cfg.env, flush_size=max_events, dedup=dedup_enabled)
        )
        stats = {"written": 0, "duplicates_dropped": 0}
        handler = _build_live_handler(
            sink_impl,
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
            sink_impl.close()
        _emit_runtime_warnings(
            logger,
            runner,
            lag_warn_threshold=lag_warn_threshold,
            buffer_warn_threshold=buffer_warn_threshold,
        )
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
            _emit_ingestion_summary(
                logger,
                mode="live",
                cfg=cfg,
                events_in=runner.metrics.events_in,
                events_out=len(handler.events),
                reconnects=runner.metrics.reconnects,
                buffer_skipped=runner.metrics.buffer_skipped,
                max_latency_seconds=runner.metrics.max_latency_seconds,
                dedup_on=dedup_enabled,
                batch_size=batch_size,
                duplicates_dropped=runner.metrics.dedup_skipped + stats["duplicates_dropped"],
                error_policy=effective_error_policy,
                rejected_payloads=getattr(source_stats, "rejected_payloads", 0),
                error_sink_failures=getattr(source_stats, "error_sink_failures", 0),
            )
        events_out = list(handler.events)
        if compute_features_after:
            run_feature_pipeline(events_out)
        return events_out
    except Exception as exc:  # pragma: no cover - explicit policy validated by unit tests
        err = exc if isinstance(exc, IngestionError) else IngestionError("source", "permanent", str(exc))
        if err.category == "sink" or effective_error_policy == "fail_fast":
            logger.error(
                "ingestion failed",
                extra={
                    "error": str(err),
                    "error_category": err.category,
                    "error_severity": err.severity,
                    "error_policy": effective_error_policy,
                },
            )
            raise err
        if effective_error_policy == "degraded":
            logger.warning(
                "ingestion degraded",
                extra={
                    "error": str(err),
                    "error_category": err.category,
                    "error_severity": err.severity,
                    "error_policy": effective_error_policy,
                },
            )
            if summary_logging:
                _emit_ingestion_summary(
                    logger,
                    mode="live",
                    cfg=cfg,
                    events_in=0,
                    events_out=0,
                    reconnects=0,
                    buffer_skipped=0,
                    max_latency_seconds=0.0,
                    dedup_on=dedup_enabled,
                    batch_size=batch_size,
                    duplicates_dropped=0,
                    result="degraded",
                    error_policy=effective_error_policy,
                    error_category=err.category,
                    error_severity=err.severity,
                    rejected_payloads=getattr(source_stats, "rejected_payloads", 0),
                    error_sink_failures=getattr(source_stats, "error_sink_failures", 0),
                )
            return []
        logger.warning(
            "live ingestion failed; falling back to dry",
            extra={
                "error": str(err),
                "error_category": err.category,
                "error_severity": err.severity,
                "error_policy": effective_error_policy,
            },
        )
        events_out = _synthetic_events(max_events)
        if summary_logging:
            _emit_ingestion_summary(
                logger,
                mode="dry",
                cfg=cfg,
                events_in=len(events_out),
                events_out=len(events_out),
                reconnects=0,
                buffer_skipped=0,
                max_latency_seconds=0.0,
                dedup_on=dedup_enabled,
                batch_size=batch_size,
                duplicates_dropped=0,
                result="fallback",
                error_policy=effective_error_policy,
                error_category=err.category,
                error_severity=err.severity,
                rejected_payloads=getattr(source_stats, "rejected_payloads", 0),
                error_sink_failures=getattr(source_stats, "error_sink_failures", 0),
            )
        if compute_features_after:
            run_feature_pipeline(events_out)
        return events_out
