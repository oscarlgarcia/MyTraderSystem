"""
Reusable helpers to collect market events for run_cycle.
"""

from __future__ import annotations

import logging
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Optional

from app.common.dto import normalize_symbol
from app.config import AppConfig, DEFAULT_INGEST_STREAM_TYPES
from app.features.pipeline import run_feature_pipeline
from app.ingestion.client import _key
from app.ingestion.checkpoints import CheckpointStore, default_checkpoint_path
from app.ingestion.dedup import Deduplicator
from app.ingestion.errors import ErrorPolicy, IngestionError, resolve_error_policy
from app.ingestion.resilience import BackpressurePolicy, ResilientRunner, TemporalPolicy
from app.ingestion.shadow import (
    ShadowPromotionError,
    assert_shadow_promotable,
    build_shadow_snapshot,
    compare_shadow_snapshots,
    persist_shadow_comparison,
)
from app.ingestion.sinks import EventSink, MirroredEventSink, ParquetEventSink
from app.ingestion.sources import BinanceSource, Source, source_snapshot_fn
from app.ingestion.storage import ParquetWriter
from app.marketdata.models import IngestionEvent, TradeEvent
from app.marketdata.raw_sink import JsonlRawSink, NullRawSink
from app.marketdata.support_matrix import validate_live_feed_support
from app.observability.alerts import emit_operational_alert


STREAM_COUNTER_FIELDS = {
    "messages_invalid_total",
    "duplicates_total",
    "gaps_total",
    "gap_irreparable_total",
    "reconnects_total",
    "heartbeat_missed_total",
    "buffer_dropped_total",
}
STREAM_LATENCY_FIELDS = {"raw_write_latency", "normalized_write_latency"}
STREAM_MAX_FIELDS = {"messages_in_total"}


def _safe_int(value: object, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _safe_float(value: object, default: float = 0.0) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def _merge_stream_metrics(*metric_maps: object) -> list[dict[str, object]]:
    merged: dict[str, dict[str, object]] = {}
    for metric_map in metric_maps:
        if not isinstance(metric_map, dict):
            continue
        for label, raw_metric in metric_map.items():
            if not isinstance(raw_metric, dict):
                continue
            venue = str(raw_metric.get("venue", "BINANCE")).upper()
            symbol = str(raw_metric.get("symbol", "UNKNOWN"))
            stream_type = str(raw_metric.get("stream_type", "unknown"))
            metric = merged.setdefault(
                str(label),
                {
                    "venue": venue,
                    "symbol": symbol,
                    "stream_type": stream_type,
                },
            )
            metric["venue"] = venue
            metric["symbol"] = symbol
            metric["stream_type"] = stream_type
            for key, value in raw_metric.items():
                if key in {"venue", "symbol", "stream_type"}:
                    continue
                if key in STREAM_COUNTER_FIELDS:
                    metric[key] = _safe_int(metric.get(key, 0)) + _safe_int(value)
                    continue
                if key in STREAM_MAX_FIELDS:
                    metric[key] = max(_safe_int(metric.get(key, 0)), _safe_int(value))
                    continue
                if key in STREAM_LATENCY_FIELDS:
                    metric[key] = max(_safe_float(metric.get(key, 0.0)), _safe_float(value))
                    continue
                metric[key] = value
    return [merged[label] for label in sorted(merged)]


def _degraded_streams(stream_metrics: list[dict[str, object]]) -> list[str]:
    degraded = []
    for metric in stream_metrics:
        if (
            _safe_int(metric.get("messages_invalid_total")) > 0
            or _safe_int(metric.get("duplicates_total")) > 0
            or _safe_int(metric.get("gaps_total")) > 0
            or _safe_int(metric.get("gap_irreparable_total")) > 0
            or _safe_int(metric.get("reconnects_total")) > 0
            or _safe_int(metric.get("heartbeat_missed_total")) > 0
            or _safe_int(metric.get("buffer_dropped_total")) > 0
        ):
            degraded.append(f"{metric['venue']}:{metric['symbol']}:{metric['stream_type']}")
    return degraded


def _synthetic_events(max_events: int) -> List[IngestionEvent]:
    now = time.time()
    events: List[IngestionEvent] = []
    price = 100.0
    for index in range(max_events):
        ts = now + index
        price += 0.1
        events.append(
            TradeEvent(
                symbol=normalize_symbol("BTCUSDT"),
                exchange_ts=datetime.fromtimestamp(ts, tz=timezone.utc),
                price=price,
                size=0.01 + index * 0.001,
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
    if buffer_warn_threshold is not None and (
        runner.metrics.buffer_skipped > buffer_warn_threshold
        or runner.metrics.buffer_overflows > buffer_warn_threshold
    ):
        logger.warning(
            "ingestion buffer pressure warning",
            extra={
                "buffer_skipped": runner.metrics.buffer_skipped,
                "buffer_overflows": runner.metrics.buffer_overflows,
                "buffer_pauses": runner.metrics.buffer_pauses,
                "buffer_drop_oldest": runner.metrics.buffer_drop_oldest,
                "buffer_drop_newest": runner.metrics.buffer_drop_newest,
                "buffer_failures": runner.metrics.buffer_failures,
                "backpressure_policy": runner.backpressure_policy,
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
    events_persisted: int,
    reconnects: int,
    buffer_skipped: int,
    buffer_overflows: int,
    buffer_pauses: int,
    buffer_drop_oldest: int,
    buffer_drop_newest: int,
    buffer_failures: int,
    backpressure_policy: str,
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
    source_events_in: int | None = None,
    events_valid: int | None = None,
    events_invalid: int | None = None,
    events_dedup_skipped: int | None = None,
    events_buffer_dropped: int | None = None,
    snapshot_runs: int = 0,
    snapshot_rows: int = 0,
    processing_latency_seconds: float | None = None,
    write_latency_seconds: float = 0.0,
    temporal_policy: str = "accept",
    event_gap_seconds: float = 0.0,
    gaps_total: int = 0,
    gap_irreparable_total: int = 0,
    late_events: int = 0,
    out_of_order_events: int = 0,
    late_events_dropped: int = 0,
    late_event_max_delay_seconds: float = 0.0,
    snapshot_duplicates_skipped: int = 0,
    handoff_bootstrap_rows: int = 0,
    handoff_overlap_dropped: int = 0,
    handoff_inconsistent: int = 0,
    stream_metrics: list[dict[str, object]] | None = None,
) -> None:
    events_invalid = rejected_payloads if events_invalid is None else events_invalid
    events_dedup_skipped = duplicates_dropped if events_dedup_skipped is None else events_dedup_skipped
    events_buffer_dropped = buffer_skipped if events_buffer_dropped is None else events_buffer_dropped
    source_events_in = events_in if source_events_in is None else source_events_in
    events_valid = events_in if events_valid is None else events_valid
    processing_latency_seconds = (
        max_latency_seconds if processing_latency_seconds is None else processing_latency_seconds
    )
    payload = {
        "mode": mode,
        "env": cfg.env,
        "events_in": int(events_in),
        "events_out": int(events_out),
        "events_persisted": int(events_persisted),
        "source_events_in": int(source_events_in),
        "events_valid": int(events_valid),
        "events_invalid": int(events_invalid),
        "events_dedup_skipped": int(events_dedup_skipped),
        "events_buffer_dropped": int(events_buffer_dropped),
        "snapshot_runs": int(snapshot_runs),
        "snapshot_rows": int(snapshot_rows),
        "snapshot_duplicates_skipped": int(snapshot_duplicates_skipped),
        "handoff_bootstrap_rows": int(handoff_bootstrap_rows),
        "handoff_overlap_dropped": int(handoff_overlap_dropped),
        "handoff_inconsistent": int(handoff_inconsistent),
        "stream_metrics": stream_metrics or [],
        "reconnects": int(reconnects),
        "buffer_skipped": int(buffer_skipped),
        "buffer_overflows": int(buffer_overflows),
        "buffer_pauses": int(buffer_pauses),
        "buffer_drop_oldest": int(buffer_drop_oldest),
        "buffer_drop_newest": int(buffer_drop_newest),
        "buffer_failures": int(buffer_failures),
        "backpressure_policy": backpressure_policy,
        "max_latency_seconds": float(max_latency_seconds),
        "processing_latency_seconds": float(processing_latency_seconds),
        "write_latency_seconds": float(write_latency_seconds),
        "temporal_policy": temporal_policy,
        "event_gap_seconds": float(event_gap_seconds),
        "gaps_total": int(gaps_total),
        "gap_irreparable_total": int(gap_irreparable_total),
        "late_events": int(late_events),
        "out_of_order_events": int(out_of_order_events),
        "late_events_dropped": int(late_events_dropped),
        "late_event_max_delay_seconds": float(late_event_max_delay_seconds),
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


def _emit_health_summary(
    logger: logging.Logger,
    *,
    mode: str,
    cfg: AppConfig,
    result: str,
    source_events_in: int,
    events_invalid: int,
    events_dedup_skipped: int,
    events_buffer_dropped: int,
    events_persisted: int,
    snapshot_runs: int,
    reconnects: int,
    processing_latency_seconds: float,
    write_latency_seconds: float,
    temporal_policy: str,
    event_gap_seconds: float,
    gaps_total: int,
    gap_irreparable_total: int,
    late_events: int,
    handoff_inconsistent: int,
    stream_metrics: list[dict[str, object]] | None = None,
) -> None:
    stream_metrics = stream_metrics or []
    logger.info(
        "ingestion health",
        extra={
            "mode": mode,
            "env": cfg.env,
            "result": result,
            "source_events_in": int(source_events_in),
            "events_invalid": int(events_invalid),
            "events_dedup_skipped": int(events_dedup_skipped),
            "events_buffer_dropped": int(events_buffer_dropped),
            "events_persisted": int(events_persisted),
            "snapshot_runs": int(snapshot_runs),
            "reconnects": int(reconnects),
            "processing_latency_seconds": float(processing_latency_seconds),
            "write_latency_seconds": float(write_latency_seconds),
            "temporal_policy": temporal_policy,
            "event_gap_seconds": float(event_gap_seconds),
            "gaps_total": int(gaps_total),
            "gap_irreparable_total": int(gap_irreparable_total),
            "late_events": int(late_events),
            "handoff_inconsistent": int(handoff_inconsistent),
            "streams_observed": len(stream_metrics),
            "streams_degraded": _degraded_streams(stream_metrics),
        },
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
        self.deduplicator = Deduplicator()
        self.pending: List[IngestionEvent] = []
        self.events: List[IngestionEvent] = []

    def __call__(self, event: IngestionEvent) -> None:
        if self.dedup_enabled:
            event_key = _key(event)
            if self.deduplicator.contains_key(event_key):
                self.stats["duplicates_dropped"] += 1
                return
            self.deduplicator.remember_key(event_key)
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
    checkpoint_store: CheckpointStore | None = None,
    backpressure_policy: BackpressurePolicy = "pause",
    temporal_policy: TemporalPolicy = "accept",
    pipeline_version: str = "v2",
    shadow_mode: bool = False,
    shadow_block_on_diff: bool = False,
    stream_types: tuple[str, ...] = DEFAULT_INGEST_STREAM_TYPES,
    production_mode: bool = False,
) -> List[IngestionEvent]:
    logger = logger or logging.getLogger("ingest")
    effective_error_policy = resolve_error_policy(error_policy, allow_live_fallback=allow_live_fallback)
    if mode == "live":
        validate_live_feed_support(
            stream_types,
            require_exact_recovery=production_mode,
            require_handoff=production_mode,
        )
    if mode == "dry":
        events_out = _synthetic_events(max_events)
        source_rejected = getattr(source, "stats", None)
        stream_metrics = _merge_stream_metrics(getattr(source_rejected, "stream_metrics", {}))
        if summary_logging:
            _emit_ingestion_summary(
                logger,
                mode="dry",
                cfg=cfg,
                events_in=len(events_out),
                events_out=len(events_out),
                events_persisted=len(events_out),
                reconnects=0,
                buffer_skipped=0,
                buffer_overflows=0,
                buffer_pauses=0,
                buffer_drop_oldest=0,
                buffer_drop_newest=0,
                buffer_failures=0,
                backpressure_policy=backpressure_policy,
                max_latency_seconds=0.0,
                dedup_on=dedup_enabled,
                batch_size=batch_size,
                duplicates_dropped=0,
                error_policy=effective_error_policy,
                rejected_payloads=getattr(source_rejected, "rejected_payloads", 0),
                error_sink_failures=getattr(source_rejected, "error_sink_failures", 0),
                source_events_in=getattr(source_rejected, "source_events_in", len(events_out)),
                events_valid=getattr(source_rejected, "events_valid", len(events_out)),
                events_invalid=getattr(source_rejected, "events_invalid", 0),
                events_dedup_skipped=0,
                events_buffer_dropped=0,
                snapshot_runs=getattr(source_rejected, "snapshot_runs", 0),
                snapshot_rows=getattr(source_rejected, "snapshot_rows", 0),
                processing_latency_seconds=0.0,
                write_latency_seconds=0.0,
                temporal_policy=temporal_policy,
                event_gap_seconds=0.0,
                gaps_total=0,
                gap_irreparable_total=0,
                late_events=0,
                out_of_order_events=0,
                late_events_dropped=0,
                late_event_max_delay_seconds=0.0,
                snapshot_duplicates_skipped=0,
                handoff_bootstrap_rows=getattr(source_rejected, "handoff_bootstrap_rows", 0),
                handoff_overlap_dropped=getattr(source_rejected, "handoff_overlap_dropped", 0),
                handoff_inconsistent=getattr(source_rejected, "handoff_inconsistent", 0),
                stream_metrics=stream_metrics,
            )
            _emit_health_summary(
                logger,
                mode="dry",
                cfg=cfg,
                result="ok",
                source_events_in=getattr(source_rejected, "source_events_in", len(events_out)),
                events_invalid=getattr(source_rejected, "events_invalid", 0),
                events_dedup_skipped=0,
                events_buffer_dropped=0,
                events_persisted=len(events_out),
                snapshot_runs=getattr(source_rejected, "snapshot_runs", 0),
                reconnects=0,
                processing_latency_seconds=0.0,
                write_latency_seconds=0.0,
                temporal_policy=temporal_policy,
                event_gap_seconds=0.0,
                gaps_total=0,
                gap_irreparable_total=0,
                late_events=0,
                handoff_inconsistent=getattr(source_rejected, "handoff_inconsistent", 0),
                stream_metrics=stream_metrics,
            )
        if compute_features_after:
            run_feature_pipeline(events_out)
        return events_out

    source_impl = source or BinanceSource(cfg, stream_types=stream_types)
    if source is None and isinstance(getattr(source_impl, "raw_sink", None), NullRawSink):
        source_impl.raw_sink = JsonlRawSink(Path(cfg.data_dir) / "raw", env=cfg.env)
    source_stats = getattr(source_impl, "stats", None)
    checkpoint_store_impl = checkpoint_store
    if checkpoint_store_impl is None and source is None and sink is None:
        checkpoint_store_impl = CheckpointStore(default_checkpoint_path(cfg))
    checkpoint_state = None
    if checkpoint_store_impl is not None:
        try:
            checkpoint_state = checkpoint_store_impl.load()
        except ValueError as exc:
            logger.warning(
                "checkpoint recovery using empty state",
                extra={
                    "checkpoint_path": str(checkpoint_store_impl.path),
                    "error": str(exc),
                },
            )
    if checkpoint_state is not None and hasattr(source_impl, "attach_checkpoint_state"):
        source_impl.attach_checkpoint_state(checkpoint_state)

    runner: ResilientRunner | None = None
    sink_impl: EventSink | None = None
    shadow_sink_impl: EventSink | None = None
    sink_pipeline_version = pipeline_version
    handler: _LiveBatchHandler | None = None
    try:
        end_time = time.time() + duration_s if duration_s else None

        def stream():
            yield from source_impl.stream(end_time=end_time)

        snapshot_fn = source_snapshot_fn(source_impl) if snapshot_enabled else None
        if sink is None:
            primary_sink = ParquetEventSink(
                ParquetWriter(
                    base_dir=cfg.data_dir,
                    env=cfg.env,
                    flush_size=max_events,
                    dedup=dedup_enabled,
                    schema_version=pipeline_version,
                )
            )
            if shadow_mode:
                shadow_version = "v1" if pipeline_version == "v2" else "v2"
                shadow_sink_impl = ParquetEventSink(
                    ParquetWriter(
                        base_dir=cfg.data_dir,
                        env=cfg.env,
                        flush_size=max_events,
                        dedup=dedup_enabled,
                        schema_version=shadow_version,
                    )
                )
                sink_impl = MirroredEventSink(primary_sink, shadow_sink_impl)
            else:
                sink_impl = primary_sink
        else:
            sink_impl = sink
            sink_pipeline_version = "custom"
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
            backpressure_policy=backpressure_policy,
            temporal_policy=temporal_policy,
            dedup_enabled=dedup_enabled,
        )
        runner.restore_checkpoint(checkpoint_state)
        stop_on_complete = duration_s is not None
        try:
            runner.run(handler, stop_on_complete=stop_on_complete, max_retries=1)
        finally:
            handler.close()
            sink_impl.close()
        if checkpoint_store_impl is not None:
            checkpoint_store_impl.save(
                runner.export_checkpoint(
                    metadata={
                        "env": cfg.env,
                        "mode": "live",
                        "saved_at": datetime.now(timezone.utc).isoformat(),
                        "events_in": runner.metrics.events_in,
                        "events_out": len(handler.events),
                        "duplicates_dropped": runner.metrics.dedup_skipped + stats["duplicates_dropped"],
                        "reconnects": runner.metrics.reconnects,
                    }
                )
            )
        _emit_runtime_warnings(
            logger,
            runner,
            lag_warn_threshold=lag_warn_threshold,
            buffer_warn_threshold=buffer_warn_threshold,
        )
        events_persisted = _safe_int(getattr(sink_impl, "persisted_count", len(handler.events)), len(handler.events))
        write_latency_seconds = _safe_float(getattr(sink_impl, "write_latency_seconds", 0.0), 0.0)
        events_dedup_skipped = _safe_int(runner.metrics.dedup_skipped + stats["duplicates_dropped"])
        shadow_comparison_payload = None
        if shadow_mode and shadow_sink_impl is not None:
            primary_snapshot = build_shadow_snapshot(
                Path(cfg.data_dir),
                env=cfg.env,
                pipeline_version=sink_pipeline_version,
                gaps_total=runner.metrics.gaps_total,
                processing_latency_seconds=runner.metrics.max_latency_seconds,
                write_latency_seconds=write_latency_seconds,
            )
            shadow_snapshot = build_shadow_snapshot(
                Path(cfg.data_dir),
                env=cfg.env,
                pipeline_version="v1" if sink_pipeline_version == "v2" else "v2",
                gaps_total=runner.metrics.gaps_total,
                processing_latency_seconds=runner.metrics.max_latency_seconds,
                write_latency_seconds=_safe_float(getattr(shadow_sink_impl, "write_latency_seconds", 0.0)),
            )
            shadow_comparison = compare_shadow_snapshots(
                primary_snapshot,
                shadow_snapshot,
            )
            persist_shadow_comparison(Path(cfg.data_dir), env=cfg.env, comparison=shadow_comparison)
            assert_shadow_promotable(shadow_comparison, block_on_diff=shadow_block_on_diff)
            shadow_comparison_payload = {
                "primary_version": shadow_comparison.primary.pipeline_version,
                "shadow_version": shadow_comparison.shadow.pipeline_version,
                "significant": shadow_comparison.significant,
                "diffs": shadow_comparison.diffs,
            }
        if summary_logging:
            source_events_in = _safe_int(getattr(source_stats, "source_events_in", runner.metrics.events_in), runner.metrics.events_in)
            events_valid = _safe_int(getattr(source_stats, "events_valid", runner.metrics.events_in), runner.metrics.events_in)
            events_invalid = _safe_int(getattr(source_stats, "events_invalid", getattr(source_stats, "rejected_payloads", 0)), getattr(source_stats, "rejected_payloads", 0))
            snapshot_runs = _safe_int(getattr(source_stats, "snapshot_runs", runner.metrics.snapshot_runs), runner.metrics.snapshot_runs)
            snapshot_rows = _safe_int(runner.metrics.snapshot_rows, runner.metrics.snapshot_rows)
            handoff_bootstrap_rows = _safe_int(getattr(source_stats, "handoff_bootstrap_rows", 0))
            handoff_overlap_dropped = _safe_int(getattr(source_stats, "handoff_overlap_dropped", 0))
            handoff_inconsistent = _safe_int(getattr(source_stats, "handoff_inconsistent", 0))
            stream_metrics = _merge_stream_metrics(
                getattr(source_stats, "stream_metrics", {}),
                runner.metrics.temporal_streams,
                getattr(sink_impl, "stream_write_metrics", {}),
            )
            logger.info(
                "ingestion live complete",
                extra={
                    "events_written": stats["written"],
                    "events_persisted": events_persisted,
                    "duplicates_dropped": stats["duplicates_dropped"],
                    "batch_size": max(1, batch_size),
                    "env": cfg.env,
                    "reconnects": runner.metrics.reconnects,
                    "buffer_skipped": runner.metrics.buffer_skipped,
                    "buffer_overflows": runner.metrics.buffer_overflows,
                    "buffer_pauses": runner.metrics.buffer_pauses,
                    "buffer_drop_oldest": runner.metrics.buffer_drop_oldest,
                    "buffer_drop_newest": runner.metrics.buffer_drop_newest,
                    "buffer_failures": runner.metrics.buffer_failures,
                    "backpressure_policy": backpressure_policy,
                    "max_latency_seconds": runner.metrics.max_latency_seconds,
                    "write_latency_seconds": write_latency_seconds,
                    "event_gap_seconds": runner.metrics.max_event_gap_seconds,
                    "gaps_total": runner.metrics.gaps_total,
                    "gap_irreparable_total": runner.metrics.gap_irreparable_total,
                    "late_events": runner.metrics.late_events,
                    "handoff_inconsistent": handoff_inconsistent,
                    "stream_metrics": stream_metrics,
                    "temporal_policy": temporal_policy,
                    "shadow_comparison": shadow_comparison_payload,
                },
            )
            _emit_ingestion_summary(
                logger,
                mode="live",
                cfg=cfg,
                events_in=runner.metrics.events_in,
                events_out=len(handler.events),
                events_persisted=events_persisted,
                reconnects=runner.metrics.reconnects,
                buffer_skipped=runner.metrics.buffer_skipped,
                buffer_overflows=runner.metrics.buffer_overflows,
                buffer_pauses=runner.metrics.buffer_pauses,
                buffer_drop_oldest=runner.metrics.buffer_drop_oldest,
                buffer_drop_newest=runner.metrics.buffer_drop_newest,
                buffer_failures=runner.metrics.buffer_failures,
                backpressure_policy=backpressure_policy,
                max_latency_seconds=runner.metrics.max_latency_seconds,
                dedup_on=dedup_enabled,
                batch_size=batch_size,
                duplicates_dropped=runner.metrics.dedup_skipped + stats["duplicates_dropped"],
                error_policy=effective_error_policy,
                rejected_payloads=getattr(source_stats, "rejected_payloads", 0),
                error_sink_failures=getattr(source_stats, "error_sink_failures", 0),
                source_events_in=source_events_in,
                events_valid=events_valid,
                events_invalid=events_invalid,
                events_dedup_skipped=events_dedup_skipped,
                events_buffer_dropped=runner.metrics.buffer_skipped,
                snapshot_runs=snapshot_runs,
                snapshot_rows=snapshot_rows,
                snapshot_duplicates_skipped=runner.metrics.snapshot_duplicates_skipped,
                processing_latency_seconds=runner.metrics.max_latency_seconds,
                write_latency_seconds=write_latency_seconds,
                temporal_policy=temporal_policy,
                event_gap_seconds=runner.metrics.max_event_gap_seconds,
                gaps_total=runner.metrics.gaps_total,
                gap_irreparable_total=runner.metrics.gap_irreparable_total,
                late_events=runner.metrics.late_events,
                out_of_order_events=runner.metrics.out_of_order_events,
                late_events_dropped=runner.metrics.late_events_dropped,
                late_event_max_delay_seconds=runner.metrics.max_late_seconds,
                handoff_bootstrap_rows=handoff_bootstrap_rows,
                handoff_overlap_dropped=handoff_overlap_dropped,
                handoff_inconsistent=handoff_inconsistent,
                stream_metrics=stream_metrics,
            )
            _emit_health_summary(
                logger,
                mode="live",
                cfg=cfg,
                result="ok",
                source_events_in=source_events_in,
                events_invalid=events_invalid,
                events_dedup_skipped=events_dedup_skipped,
                events_buffer_dropped=runner.metrics.buffer_skipped,
                events_persisted=events_persisted,
                snapshot_runs=snapshot_runs,
                reconnects=runner.metrics.reconnects,
                processing_latency_seconds=runner.metrics.max_latency_seconds,
                write_latency_seconds=write_latency_seconds,
                temporal_policy=temporal_policy,
                event_gap_seconds=runner.metrics.max_event_gap_seconds,
                gaps_total=runner.metrics.gaps_total,
                gap_irreparable_total=runner.metrics.gap_irreparable_total,
                late_events=runner.metrics.late_events,
                handoff_inconsistent=handoff_inconsistent,
                stream_metrics=stream_metrics,
            )
        events_out = list(handler.events)
        if compute_features_after:
            run_feature_pipeline(events_out)
        return events_out
    except ShadowPromotionError:
        raise
    except Exception as exc:  # pragma: no cover - explicit policy validated by unit tests
        err = exc if isinstance(exc, IngestionError) else IngestionError("source", "permanent", str(exc))
        source_events_in = _safe_int(getattr(source_stats, "source_events_in", getattr(runner, "metrics", None).events_in if runner else 0))
        events_valid = _safe_int(getattr(source_stats, "events_valid", getattr(runner, "metrics", None).events_in if runner else 0))
        events_invalid = _safe_int(getattr(source_stats, "events_invalid", getattr(source_stats, "rejected_payloads", 0)))
        events_dedup_skipped = _safe_int((getattr(runner, "metrics", None).dedup_skipped if runner else 0) + (0 if handler is None else handler.stats["duplicates_dropped"]))
        events_buffer_dropped = _safe_int(getattr(runner, "metrics", None).buffer_skipped if runner else 0)
        snapshot_runs = _safe_int(getattr(source_stats, "snapshot_runs", getattr(runner, "metrics", None).snapshot_runs if runner else 0))
        snapshot_rows = _safe_int(getattr(source_stats, "snapshot_rows", getattr(runner, "metrics", None).snapshot_rows if runner else 0))
        reconnects = _safe_int(getattr(runner, "metrics", None).reconnects if runner else 0)
        processing_latency_seconds = _safe_float(getattr(runner, "metrics", None).max_latency_seconds if runner else 0.0)
        write_latency_seconds = _safe_float(getattr(sink_impl, "write_latency_seconds", 0.0), 0.0) if sink_impl is not None else 0.0
        events_persisted = _safe_int(getattr(sink_impl, "persisted_count", 0), 0) if sink_impl is not None else 0
        event_gap_seconds = _safe_float(getattr(runner, "metrics", None).max_event_gap_seconds if runner else 0.0)
        gaps_total = _safe_int(getattr(runner, "metrics", None).gaps_total if runner else 0)
        gap_irreparable_total = _safe_int(getattr(runner, "metrics", None).gap_irreparable_total if runner else 0)
        late_events = _safe_int(getattr(runner, "metrics", None).late_events if runner else 0)
        out_of_order_events = _safe_int(getattr(runner, "metrics", None).out_of_order_events if runner else 0)
        late_events_dropped = _safe_int(getattr(runner, "metrics", None).late_events_dropped if runner else 0)
        late_event_max_delay_seconds = _safe_float(getattr(runner, "metrics", None).max_late_seconds if runner else 0.0)
        snapshot_duplicates_skipped = _safe_int(getattr(runner, "metrics", None).snapshot_duplicates_skipped if runner else 0)
        handoff_bootstrap_rows = _safe_int(getattr(source_stats, "handoff_bootstrap_rows", 0))
        handoff_overlap_dropped = _safe_int(getattr(source_stats, "handoff_overlap_dropped", 0))
        handoff_inconsistent = _safe_int(getattr(source_stats, "handoff_inconsistent", 0))
        stream_metrics = _merge_stream_metrics(
            getattr(source_stats, "stream_metrics", {}),
            getattr(runner, "metrics", None).temporal_streams if runner else {},
            getattr(sink_impl, "stream_write_metrics", {}) if sink_impl is not None else {},
        )
        if err.category == "sink":
            emit_operational_alert(
                logger,
                alert_type="sink_failure",
                observed=1,
                extra={
                    "sink_component": type(sink_impl).__name__ if sink_impl is not None else "unknown",
                    "error": str(err),
                    "error_category": err.category,
                    "error_severity": err.severity,
                },
            )
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
            if summary_logging:
                _emit_ingestion_summary(
                    logger,
                    mode="live",
                    cfg=cfg,
                    events_in=int(getattr(runner, "metrics", None).events_in if runner else 0),
                    events_out=len(handler.events) if handler is not None else 0,
                    events_persisted=events_persisted,
                    reconnects=reconnects,
                    buffer_skipped=events_buffer_dropped,
                    buffer_overflows=int(getattr(runner, "metrics", None).buffer_overflows if runner else 0),
                    buffer_pauses=int(getattr(runner, "metrics", None).buffer_pauses if runner else 0),
                    buffer_drop_oldest=int(getattr(runner, "metrics", None).buffer_drop_oldest if runner else 0),
                    buffer_drop_newest=int(getattr(runner, "metrics", None).buffer_drop_newest if runner else 0),
                    buffer_failures=int(getattr(runner, "metrics", None).buffer_failures if runner else 0),
                    backpressure_policy=backpressure_policy,
                    max_latency_seconds=processing_latency_seconds,
                    dedup_on=dedup_enabled,
                    batch_size=batch_size,
                    duplicates_dropped=events_dedup_skipped,
                    result="failed",
                    error_policy=effective_error_policy,
                    error_category=err.category,
                    error_severity=err.severity,
                    rejected_payloads=getattr(source_stats, "rejected_payloads", 0),
                    error_sink_failures=getattr(source_stats, "error_sink_failures", 0),
                    source_events_in=source_events_in,
                    events_valid=events_valid,
                    events_invalid=events_invalid,
                    events_dedup_skipped=events_dedup_skipped,
                    events_buffer_dropped=events_buffer_dropped,
                    snapshot_runs=snapshot_runs,
                    snapshot_rows=snapshot_rows,
                    snapshot_duplicates_skipped=snapshot_duplicates_skipped,
                    processing_latency_seconds=processing_latency_seconds,
                    write_latency_seconds=write_latency_seconds,
                    temporal_policy=temporal_policy,
                    event_gap_seconds=event_gap_seconds,
                    gaps_total=gaps_total,
                    gap_irreparable_total=gap_irreparable_total,
                    late_events=late_events,
                    out_of_order_events=out_of_order_events,
                    late_events_dropped=late_events_dropped,
                    late_event_max_delay_seconds=late_event_max_delay_seconds,
                    handoff_bootstrap_rows=handoff_bootstrap_rows,
                    handoff_overlap_dropped=handoff_overlap_dropped,
                    handoff_inconsistent=handoff_inconsistent,
                    stream_metrics=stream_metrics,
                )
                _emit_health_summary(
                    logger,
                    mode="live",
                    cfg=cfg,
                    result="failed",
                    source_events_in=source_events_in,
                    events_invalid=events_invalid,
                    events_dedup_skipped=events_dedup_skipped,
                    events_buffer_dropped=events_buffer_dropped,
                    events_persisted=events_persisted,
                    snapshot_runs=snapshot_runs,
                    reconnects=reconnects,
                    processing_latency_seconds=processing_latency_seconds,
                    write_latency_seconds=write_latency_seconds,
                    temporal_policy=temporal_policy,
                    event_gap_seconds=event_gap_seconds,
                    gaps_total=gaps_total,
                    gap_irreparable_total=gap_irreparable_total,
                    late_events=late_events,
                    handoff_inconsistent=handoff_inconsistent,
                    stream_metrics=stream_metrics,
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
                    events_persisted=0,
                    reconnects=0,
                    buffer_skipped=0,
                    buffer_overflows=0,
                    buffer_pauses=0,
                    buffer_drop_oldest=0,
                    buffer_drop_newest=0,
                    buffer_failures=0,
                    backpressure_policy=backpressure_policy,
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
                    source_events_in=source_events_in,
                    events_valid=events_valid,
                    events_invalid=events_invalid,
                    events_dedup_skipped=events_dedup_skipped,
                    events_buffer_dropped=events_buffer_dropped,
                    snapshot_runs=snapshot_runs,
                    snapshot_rows=snapshot_rows,
                    snapshot_duplicates_skipped=snapshot_duplicates_skipped,
                    processing_latency_seconds=processing_latency_seconds,
                    write_latency_seconds=write_latency_seconds,
                    temporal_policy=temporal_policy,
                    event_gap_seconds=event_gap_seconds,
                    gaps_total=gaps_total,
                    gap_irreparable_total=gap_irreparable_total,
                    late_events=late_events,
                    out_of_order_events=out_of_order_events,
                    late_events_dropped=late_events_dropped,
                    late_event_max_delay_seconds=late_event_max_delay_seconds,
                    handoff_bootstrap_rows=handoff_bootstrap_rows,
                    handoff_overlap_dropped=handoff_overlap_dropped,
                    handoff_inconsistent=handoff_inconsistent,
                    stream_metrics=stream_metrics,
                )
                _emit_health_summary(
                    logger,
                    mode="live",
                    cfg=cfg,
                    result="degraded",
                    source_events_in=source_events_in,
                    events_invalid=events_invalid,
                    events_dedup_skipped=events_dedup_skipped,
                    events_buffer_dropped=events_buffer_dropped,
                    events_persisted=events_persisted,
                    snapshot_runs=snapshot_runs,
                    reconnects=reconnects,
                    processing_latency_seconds=processing_latency_seconds,
                    write_latency_seconds=write_latency_seconds,
                    temporal_policy=temporal_policy,
                    event_gap_seconds=event_gap_seconds,
                    gaps_total=gaps_total,
                    gap_irreparable_total=gap_irreparable_total,
                    late_events=late_events,
                    handoff_inconsistent=handoff_inconsistent,
                    stream_metrics=stream_metrics,
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
                events_persisted=len(events_out),
                reconnects=0,
                buffer_skipped=0,
                buffer_overflows=0,
                buffer_pauses=0,
                buffer_drop_oldest=0,
                buffer_drop_newest=0,
                buffer_failures=0,
                backpressure_policy=backpressure_policy,
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
                source_events_in=max(source_events_in, len(events_out)),
                events_valid=max(events_valid, len(events_out)),
                events_invalid=events_invalid,
                events_dedup_skipped=events_dedup_skipped,
                events_buffer_dropped=events_buffer_dropped,
                snapshot_runs=snapshot_runs,
                snapshot_rows=snapshot_rows,
                snapshot_duplicates_skipped=snapshot_duplicates_skipped,
                processing_latency_seconds=0.0,
                write_latency_seconds=write_latency_seconds,
                temporal_policy=temporal_policy,
                    event_gap_seconds=event_gap_seconds,
                    gaps_total=gaps_total,
                    gap_irreparable_total=gap_irreparable_total,
                late_events=late_events,
                out_of_order_events=out_of_order_events,
                late_events_dropped=late_events_dropped,
                late_event_max_delay_seconds=late_event_max_delay_seconds,
                handoff_bootstrap_rows=handoff_bootstrap_rows,
                handoff_overlap_dropped=handoff_overlap_dropped,
                handoff_inconsistent=handoff_inconsistent,
                stream_metrics=stream_metrics,
            )
            _emit_health_summary(
                logger,
                mode="dry",
                cfg=cfg,
                result="fallback",
                source_events_in=max(source_events_in, len(events_out)),
                events_invalid=events_invalid,
                events_dedup_skipped=events_dedup_skipped,
                events_buffer_dropped=events_buffer_dropped,
                events_persisted=len(events_out),
                snapshot_runs=snapshot_runs,
                reconnects=reconnects,
                processing_latency_seconds=0.0,
                write_latency_seconds=write_latency_seconds,
                temporal_policy=temporal_policy,
                event_gap_seconds=event_gap_seconds,
                gaps_total=gaps_total,
                gap_irreparable_total=gap_irreparable_total,
                late_events=late_events,
                handoff_inconsistent=handoff_inconsistent,
                stream_metrics=stream_metrics,
            )
        if compute_features_after:
            run_feature_pipeline(events_out)
        return events_out
