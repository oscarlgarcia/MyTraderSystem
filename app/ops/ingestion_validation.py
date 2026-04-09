from __future__ import annotations

import io
import json
import shutil
import subprocess
import sys
import tempfile
import time
import gc
from concurrent.futures import ThreadPoolExecutor
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from threading import Thread
from types import SimpleNamespace
from typing import Callable, Literal, Sequence
import hashlib

import httpx

from app.common.dto import MarketEvent
from app.ingestion.backfill import _interval_to_ms, fetch_klines, normalize_kline_row
from app.ingestion.compaction import CompactionJobPolicy, run_compaction_job
from app.ingestion.checkpoints import CheckpointStore
from app.ingestion.pipeline import collect_events
from app.ingestion.shadow import (
    affected_shadow_partitions,
    build_shadow_snapshot,
    compare_shadow_snapshots,
    persist_shadow_comparison,
)
from app.ingestion.sinks import MirroredEventSink, ParquetEventSink
from app.ingestion.sources import BinanceSource, Source, heartbeat_policy_for_streams, StaticSource, _default_ws_connect
from app.ingestion.storage import ParquetWriter, normalized_partition_path, read_parquet
from app.ingestion.storage_health import collect_storage_health
from app.marketdata.instruments import ensure_default_instruments, get_default_instrument_catalog
from app.marketdata.models import BarEvent, TradeEvent
from app.marketdata.raw_sink import JsonlRawSink, RawRecord
from app.marketdata.replay import ReplaySource
from app.ops.observability_contract import build_observability_contract_report
from app.observability.logger import get_logger


@dataclass(frozen=True, slots=True)
class ValidationRun:
    mode: str
    pipeline_version: str
    shadow_mode: bool
    events_in: int
    events_persisted: int
    duplicates: int
    gaps: int
    gap_irreparable: int
    reconnects: int
    heartbeat_missed_total: int
    exchange_receive_skew_seconds: float
    receive_process_skew_seconds: float
    processing_latency_seconds: float
    write_latency_seconds: float
    streams_degraded: list[str]
    result: str


@dataclass(frozen=True, slots=True)
class SoakEvidence:
    generated_at: str
    target_profile: str
    mode: str
    vendor: str
    symbol: str
    stream_type: str
    interval: str | None
    iterations: int
    max_events_per_iteration: int
    duration_seconds: float
    reconnects_target: int
    reconnects_observed: int
    elapsed_seconds: float
    max_processing_latency_seconds: float
    max_write_latency_seconds: float
    total_events_persisted: int
    max_allowed_gaps: int
    max_gaps: int
    max_allowed_duplicates: int
    max_duplicates: int
    max_allowed_gap_irreparable: int
    max_gap_irreparable: int
    max_allowed_heartbeat_missed_total: int
    max_heartbeat_missed_total: int
    max_allowed_exchange_receive_skew_seconds: float
    max_exchange_receive_skew_seconds: float
    max_allowed_receive_process_skew_seconds: float
    max_receive_process_skew_seconds: float
    max_allowed_processing_latency_seconds: float
    max_allowed_compaction_failures: int
    compaction_failures_total: int
    max_streams_degraded: int
    slo: dict[str, object]
    pass_ok: bool
    runs: list[ValidationRun]


@dataclass(frozen=True, slots=True)
class CanaryBaseline:
    vendor: str
    rest_base: str
    symbol: str
    interval: str
    bars: int
    fetched_at: str
    start_ts: str | None
    end_ts: str | None
    payload_sha256: str
    rows: list[list[object]]


@dataclass(frozen=True, slots=True)
class CanaryEvidence:
    baseline_path: str
    baseline_source: str
    vendor: str
    symbol: str
    interval: str
    bars: int
    baseline_hash: str
    baseline: ValidationRun
    candidate: ValidationRun
    diffs: dict[str, object]
    pass_ok: bool
    comparison_reason: str


@dataclass(frozen=True, slots=True)
class StorageBenchmarkCase:
    name: str
    dataset_kind: str
    target_profile: str
    requested_symbol_count: int
    rows_in: int
    partitions: int
    bursts: int
    batch_size: int
    elapsed_seconds: float
    rows_per_second: float
    max_write_latency_seconds: float
    compaction_elapsed_seconds: float
    shadow_elapsed_seconds: float
    segments_pending_total: int
    segments_per_partition_max: int
    normalized_partition_row_count: int
    pass_ok: bool


@dataclass(frozen=True, slots=True)
class StorageBenchmarkEvidence:
    generated_at: str
    target_profile: str
    synthetic_case: StorageBenchmarkCase
    replay_case: StorageBenchmarkCase
    concurrent_compaction_case: StorageBenchmarkCase
    shadow_scoped_case: StorageBenchmarkCase
    slo: dict[str, float]
    pass_ok: bool
    required_high_cardinality_symbol_counts: tuple[int, ...] = ()
    high_cardinality_cases: tuple[StorageBenchmarkCase, ...] = ()


BenchmarkTargetProfile = Literal["paper", "live", "robustness"]


@dataclass(frozen=True, slots=True)
class WSCanaryEvidence:
    mode: str
    target_profile: str
    vendor: str
    ws_base: str
    symbol: str
    stream_type: str
    interval: str | None
    max_events: int
    duration_seconds: float
    reconnects_target: int
    reconnect_after_events: int
    reconnects_observed: int
    continuity: dict[str, object]
    stream_metrics: list[dict[str, object]]
    alert_types: list[str]
    checkpoint_audit_events: int
    recovery_audit_events: int
    report_generated_at: str
    slo: dict[str, object]
    pass_ok: bool
    comparison_reason: str


@dataclass(frozen=True, slots=True)
class VendorContractsEvidence:
    generated_at: str
    pytest_target: str
    command: list[str]
    cwd: str
    python_executable: str
    duration_seconds: float
    returncode: int
    pass_ok: bool
    stdout: str
    stderr: str


CRITICAL_FAILURE_INJECTION_TEST_IDS: tuple[str, ...] = (
    "tests/ops/test_failure_injection.py::test_failure_injection_release_gate_fails_with_stale_ws_artifact",
    "tests/ops/test_failure_injection.py::test_failure_injection_prod_rejects_fallback_metadata_snapshot",
    "tests/ops/test_failure_injection.py::test_failure_injection_release_gate_fails_with_manifest_mismatch",
)


@dataclass(frozen=True, slots=True)
class FailureInjectionEvidence:
    generated_at: str
    pytest_target: str
    critical_test_ids: tuple[str, ...]
    command: list[str]
    cwd: str
    python_executable: str
    duration_seconds: float
    returncode: int
    pass_ok: bool
    stdout: str
    stderr: str


CanaryFetchRows = Callable[..., list[list[object]]]
WSCanarySourceBuilder = Callable[[SimpleNamespace], Source]


def _cfg(base_dir: Path, *, rest_base: str = "https://api.binance.com", symbols: list[str] | None = None) -> SimpleNamespace:
    return SimpleNamespace(
        env="test",
        data_dir=base_dir.resolve(),
        log_level="INFO",
        ws_base="wss://stream.binance.com:9443",
        rest_base=rest_base,
        symbols=symbols or ["BTCUSDT"],
    )


def _ensure_benchmark_symbols_registered(symbols: Sequence[str], *, venue: str = "BINANCE") -> None:
    catalog = get_default_instrument_catalog()
    missing = [str(symbol).upper() for symbol in symbols if not catalog.has(venue, symbol)]
    if not missing:
        return
    try:
        ensure_default_instruments(missing, venue=venue)
    except KeyError:
        for symbol in missing:
            if not catalog.has(venue, symbol):
                catalog.register_static_spot_symbol(symbol, venue=venue)


def _events(count: int, *, duplicate_edge: bool = False) -> list[MarketEvent]:
    base = datetime(2024, 1, 1, tzinfo=timezone.utc)
    events = [
        MarketEvent(
            symbol="BTCUSDT",
            event_ts=base + timedelta(seconds=index),
            price=100.0 + index,
            size=1.0,
            source="trade",
            metadata={"trade_id": str(index + 1), "source_id": str(index + 1), "venue": "BINANCE"},
        )
        for index in range(count)
    ]
    if duplicate_edge and events:
        events.insert(1, events[0])
    return events


def _json_lines(buffer: io.StringIO) -> list[dict[str, object]]:
    return [json.loads(line) for line in buffer.getvalue().splitlines() if line.strip()]


def _extract_run(logs: list[dict[str, object]], *, pipeline_version: str, shadow_mode: bool) -> ValidationRun:
    summary = next(record for record in logs if record["message"] == "ingestion summary")
    health = next(record for record in logs if record["message"] == "ingestion health")
    stream_metrics = list(summary.get("stream_metrics") or [])
    return ValidationRun(
        mode=str(summary["mode"]),
        pipeline_version=pipeline_version,
        shadow_mode=shadow_mode,
        events_in=int(summary["events_in"]),
        events_persisted=int(summary["events_persisted"]),
        duplicates=int(summary["events_dedup_skipped"]),
        gaps=int(summary["gaps_total"]),
        gap_irreparable=int(summary["gap_irreparable_total"]),
        reconnects=int(summary["reconnects"]),
        heartbeat_missed_total=_sum_stream_metric(stream_metrics, "heartbeat_missed_total"),
        exchange_receive_skew_seconds=float(summary.get("exchange_receive_skew_seconds", 0.0)),
        receive_process_skew_seconds=float(summary.get("receive_process_skew_seconds", 0.0)),
        processing_latency_seconds=float(summary["processing_latency_seconds"]),
        write_latency_seconds=float(summary["write_latency_seconds"]),
        streams_degraded=list(health.get("streams_degraded", [])),
        result=str(health["result"]),
    )


def _extract_alert_types(logs: list[dict[str, object]]) -> list[str]:
    return [
        str(record["alert_type"])
        for record in logs
        if record.get("message") == "operational alert" and record.get("alert_type")
    ]


def _sum_stream_metric(stream_metrics: Sequence[dict[str, object]], key: str) -> int:
    total = 0
    for metric in stream_metrics:
        try:
            total += int(metric.get(key, 0) or 0)
        except (TypeError, ValueError):
            continue
    return total


def _max_stream_metric(stream_metrics: Sequence[dict[str, object]], key: str) -> float:
    observed = 0.0
    for metric in stream_metrics:
        try:
            observed = max(observed, float(metric.get(key, 0.0) or 0.0))
        except (TypeError, ValueError):
            continue
    return observed


def _validation_slo(target_profile: Literal["paper", "live"]) -> dict[str, object]:
    thresholds = build_observability_contract_report(target=target_profile).required_metric_thresholds
    return {
        "target_profile": target_profile,
        "max_allowed_duplicates": int(thresholds["duplicates_total"]["critical"]),
        "max_allowed_gaps": int(thresholds["gaps_total"]["critical"]),
        "max_allowed_gap_irreparable": int(thresholds["gap_irreparable_total"]["critical"]),
        "max_allowed_heartbeat_missed_total": int(thresholds["heartbeat_missed_total"]["critical"]),
        "max_allowed_exchange_receive_skew_seconds": float(thresholds["exchange_receive_skew_seconds"]["critical"]),
        "max_allowed_receive_process_skew_seconds": float(thresholds["receive_process_skew_seconds"]["critical"]),
        "max_allowed_processing_latency_seconds": float(thresholds["processing_latency_seconds"]["critical"]),
        "max_allowed_compaction_failures": int(thresholds["compaction_failures_total"]["critical"]),
    }


def _count_jsonl_records(path: Path) -> int:
    if not path.exists():
        return 0
    return sum(1 for line in path.read_text(encoding="utf-8").splitlines() if line.strip())


def write_json_report(path: Path, payload: object) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=False, indent=2, default=str)
        handle.write("\n")
    return path


def _payload_hash(rows: Sequence[Sequence[object]]) -> str:
    return hashlib.sha256(json.dumps(list(rows), ensure_ascii=False, default=str).encode("utf-8")).hexdigest()


def _baseline_from_payload(payload: dict[str, object]) -> CanaryBaseline:
    rows = payload.get("rows", [])
    if not isinstance(rows, list):
        raise ValueError("canary baseline rows must be a list")
    return CanaryBaseline(
        vendor=str(payload.get("vendor", "BINANCE")),
        rest_base=str(payload.get("rest_base", "https://api.binance.com")),
        symbol=str(payload["symbol"]),
        interval=str(payload["interval"]),
        bars=int(payload["bars"]),
        fetched_at=str(payload["fetched_at"]),
        start_ts=str(payload.get("start_ts")) if payload.get("start_ts") is not None else None,
        end_ts=str(payload.get("end_ts")) if payload.get("end_ts") is not None else None,
        payload_sha256=str(payload["payload_sha256"]),
        rows=[[item for item in row] for row in rows],
    )


def _load_canary_baseline(path: Path) -> CanaryBaseline:
    payload = json.loads(path.read_text(encoding="utf-8"))
    baseline = _baseline_from_payload(payload)
    if baseline.payload_sha256 != _payload_hash(baseline.rows):
        raise ValueError(f"canary baseline hash mismatch: {path}")
    return baseline


def _persist_canary_baseline(path: Path, baseline: CanaryBaseline) -> Path:
    return write_json_report(path, asdict(baseline))


def _resolve_canary_end_time(end_time: datetime | None, interval: str) -> datetime:
    interval_ms = _interval_to_ms(interval)
    now = datetime.now(timezone.utc) if end_time is None else end_time.astimezone(timezone.utc)
    floored_ms = int(now.timestamp() * 1000) // interval_ms * interval_ms
    if floored_ms <= 0:
        raise ValueError("invalid canary end time")
    return datetime.fromtimestamp(floored_ms / 1000, tz=timezone.utc)


def _default_fetch_rows(
    *,
    rest_base: str,
    symbol: str,
    interval: str,
    bars: int,
    end_time: datetime | None,
) -> list[list[object]]:
    resolved_end = _resolve_canary_end_time(end_time, interval)
    end_ms = int(resolved_end.timestamp() * 1000)
    start_ms = end_ms - bars * _interval_to_ms(interval)
    with httpx.Client() as client:
        rows = fetch_klines(
            client=client,
            base_url=rest_base,
            symbol=symbol,
            start_ms=start_ms,
            end_ms=end_ms,
            interval=interval,
            limit=min(max(bars + 2, 10), 1000),
        )
    return [list(row) for row in rows[-bars:]]


def _fetch_canary_baseline(
    *,
    rest_base: str,
    symbol: str,
    interval: str,
    bars: int,
    end_time: datetime | None,
    fetch_rows: CanaryFetchRows | None,
) -> CanaryBaseline:
    rows = (
        fetch_rows(rest_base=rest_base, symbol=symbol, interval=interval, bars=bars, end_time=end_time)
        if fetch_rows is not None
        else _default_fetch_rows(rest_base=rest_base, symbol=symbol, interval=interval, bars=bars, end_time=end_time)
    )
    if not rows:
        raise ValueError("real-feed canary returned no rows")
    sliced_rows = [list(row) for row in rows[-bars:]]
    payload_hash = _payload_hash(sliced_rows)
    start_ts = datetime.fromtimestamp(int(sliced_rows[0][0]) / 1000, tz=timezone.utc).isoformat()
    end_ts = datetime.fromtimestamp(int(sliced_rows[-1][6]) / 1000, tz=timezone.utc).isoformat()
    return CanaryBaseline(
        vendor="BINANCE",
        rest_base=rest_base,
        symbol=symbol,
        interval=interval,
        bars=len(sliced_rows),
        fetched_at=datetime.now(timezone.utc).isoformat(),
        start_ts=start_ts,
        end_ts=end_ts,
        payload_sha256=payload_hash,
        rows=sliced_rows,
    )


def _load_or_refresh_canary_baseline(
    baseline_path: Path,
    *,
    rest_base: str,
    symbol: str,
    interval: str,
    bars: int,
    refresh_baseline: bool,
    end_time: datetime | None,
    fetch_rows: CanaryFetchRows | None,
) -> tuple[CanaryBaseline, str]:
    if baseline_path.exists() and not refresh_baseline:
        baseline = _load_canary_baseline(baseline_path)
        return baseline, "persisted"
    baseline = _fetch_canary_baseline(
        rest_base=rest_base,
        symbol=symbol,
        interval=interval,
        bars=bars,
        end_time=end_time,
        fetch_rows=fetch_rows,
    )
    _persist_canary_baseline(baseline_path, baseline)
    return baseline, "vendor_refresh"


def _events_from_canary_baseline(baseline: CanaryBaseline) -> list[BarEvent]:
    ensure_default_instruments([baseline.symbol], venue=baseline.vendor)
    receive_ts = datetime.now(timezone.utc)
    return [
        normalize_kline_row(
            baseline.symbol,
            row,
            interval=baseline.interval,
            receive_ts=receive_ts,
            process_ts=receive_ts,
            venue=baseline.vendor,
        )
        for row in baseline.rows
    ]


def _projection_rows_from_events(events: list[BarEvent]) -> list[dict[str, object]]:
    rows = [
        {
            "symbol": event.symbol,
            "venue": event.venue,
            "event_ts": event.exchange_ts.isoformat(),
            "open": float(event.open),
            "high": float(event.high),
            "low": float(event.low),
            "close": float(event.close),
            "volume": float(event.volume),
            "volume_kind": event.volume_kind,
            "interval": event.interval,
            "open_ts": event.open_ts.isoformat() if event.open_ts is not None else None,
            "close_ts": event.close_ts.isoformat() if event.close_ts is not None else None,
        }
        for event in events
    ]
    rows.sort(key=lambda row: (str(row["symbol"]), str(row["event_ts"])))
    return rows


def _normalized_ts_string(value: object | None) -> str | None:
    if value in (None, ""):
        return None
    if isinstance(value, datetime):
        return value.isoformat()
    return datetime.fromisoformat(str(value)).isoformat()


def _projection_rows_from_storage(base_dir: Path, *, pipeline_version: str) -> list[dict[str, object]]:
    if pipeline_version == "v1":
        paths = sorted(base_dir.glob("test/symbol=*/date=*/data.parquet"))
    else:
        paths = sorted(base_dir.glob("normalized/*/env=test/venue=*/symbol=*/date=*"))
    rows: list[dict[str, object]] = []
    for path in paths:
        table = read_parquet(path)
        for row in table.to_pylist():
            metadata = row.get("metadata") or {}
            if isinstance(metadata, list):
                mapped: dict[str, str] = {}
                for item in metadata:
                    if isinstance(item, tuple) and len(item) == 2:
                        mapped[str(item[0])] = str(item[1])
                metadata = mapped
            elif not isinstance(metadata, dict):
                metadata = {}
            else:
                metadata = {str(key): str(value) for key, value in metadata.items()}
            rows.append(
                {
                    "symbol": str(row["symbol"]),
                    "venue": str(row.get("venue") or metadata.get("venue", "BINANCE")).upper(),
                    "event_ts": _normalized_ts_string(row.get("event_ts") or row.get("exchange_ts") or metadata.get("close_ts")),
                    "open": float(row.get("open", metadata.get("open", row.get("price")))),
                    "high": float(row.get("high", metadata.get("high", row.get("price")))),
                    "low": float(row.get("low", metadata.get("low", row.get("price")))),
                    "close": float(row.get("close", metadata.get("close", row.get("price")))),
                    "volume": float(row.get("volume", metadata.get("volume", row.get("size")))),
                    "volume_kind": str(row.get("volume_kind") or metadata.get("volume_kind", "quote")),
                    "interval": str(row.get("interval") or metadata.get("interval", "1m")),
                    "open_ts": _normalized_ts_string(row.get("open_ts") or metadata.get("open_ts")),
                    "close_ts": _normalized_ts_string(row.get("close_ts") or metadata.get("close_ts")),
                }
            )
    rows.sort(key=lambda row: (str(row["symbol"]), str(row["event_ts"])))
    return rows


def _canary_diffs(
    *,
    vendor_rows: list[dict[str, object]],
    baseline_rows: list[dict[str, object]],
    candidate_rows: list[dict[str, object]],
) -> dict[str, object]:
    vendor_checksum = _payload_hash(vendor_rows)
    baseline_checksum = _payload_hash(baseline_rows)
    candidate_checksum = _payload_hash(candidate_rows)
    return {
        "vendor_row_count": len(vendor_rows),
        "baseline_row_count": len(baseline_rows),
        "candidate_row_count": len(candidate_rows),
        "row_count": len(candidate_rows) - len(baseline_rows),
        "baseline_matches_vendor": baseline_rows == vendor_rows,
        "candidate_matches_vendor": candidate_rows == vendor_rows,
        "vendor_projection_checksum": vendor_checksum,
        "baseline_projection_checksum": baseline_checksum,
        "candidate_projection_checksum": candidate_checksum,
        "projection_checksum_match": vendor_checksum == baseline_checksum == candidate_checksum,
    }


def _execute_canary_version(
    *,
    base_dir: Path,
    version: str,
    events: list[BarEvent],
) -> ValidationRun:
    writer = ParquetWriter(
        base_dir=base_dir,
        env="test",
        flush_size=max(1, min(len(events), 32)),
        dedup=True,
        schema_version=version,
    )
    sink = ParquetEventSink(writer)
    sink.add(list(events))
    sink.close()
    return ValidationRun(
        mode="canary",
        pipeline_version=version,
        shadow_mode=False,
        events_in=len(events),
        events_persisted=int(getattr(sink, "persisted_count", len(events))),
        duplicates=0,
        gaps=0,
        gap_irreparable=0,
        reconnects=0,
        heartbeat_missed_total=0,
        exchange_receive_skew_seconds=0.0,
        receive_process_skew_seconds=0.0,
        processing_latency_seconds=0.0,
        write_latency_seconds=float(getattr(sink, "write_latency_seconds", 0.0)),
        streams_degraded=[],
        result="ok",
    )


def run_soak_validation(
    output_path: Path,
    *,
    target_profile: Literal["paper", "live"] = "paper",
    mode: str = "deterministic",
    iterations: int = 5,
    events_per_iteration: int = 500,
    duration_seconds: float = 150.0,
    pipeline_version: str = "v2",
    symbol: str = "BTCUSDT",
    stream_type: str = "kline",
    interval: str = "1m",
    ws_base: str = "wss://stream.binance.com:9443",
    rest_base: str = "https://api.binance.com",
    reconnect_after_events: int = 1,
    induced_reconnects: int = 1,
    max_allowed_duplicates: int | None = None,
    max_allowed_gaps: int | None = None,
    max_allowed_heartbeat_missed_total: int | None = None,
    max_allowed_exchange_receive_skew_seconds: float | None = None,
    max_allowed_receive_process_skew_seconds: float | None = None,
    max_allowed_processing_latency_seconds: float | None = None,
    max_allowed_gap_irreparable: int = 0,
    max_allowed_compaction_failures: int = 0,
    source_builder: WSCanarySourceBuilder | None = None,
) -> SoakEvidence:
    if mode not in {"deterministic", "ws-live"}:
        raise ValueError(f"unsupported soak mode: {mode}")
    if mode == "ws-live" and stream_type not in {"trade", "kline"}:
        raise ValueError("ws-live soak supports only trade and kline feeds")
    runs: list[ValidationRun] = []
    reconnects_observed = 0
    compaction_failures_total = 0
    started = time.perf_counter()
    slo = _validation_slo(target_profile)
    allowed_duplicates = slo["max_allowed_duplicates"] if max_allowed_duplicates is None else int(max_allowed_duplicates)
    allowed_gaps = slo["max_allowed_gaps"] if max_allowed_gaps is None else int(max_allowed_gaps)
    allowed_heartbeat_missed_total = (
        slo["max_allowed_heartbeat_missed_total"]
        if max_allowed_heartbeat_missed_total is None
        else int(max_allowed_heartbeat_missed_total)
    )
    allowed_exchange_receive_skew_seconds = (
        slo["max_allowed_exchange_receive_skew_seconds"]
        if max_allowed_exchange_receive_skew_seconds is None
        else float(max_allowed_exchange_receive_skew_seconds)
    )
    allowed_receive_process_skew_seconds = (
        slo["max_allowed_receive_process_skew_seconds"]
        if max_allowed_receive_process_skew_seconds is None
        else float(max_allowed_receive_process_skew_seconds)
    )
    allowed_processing_latency_seconds = (
        slo["max_allowed_processing_latency_seconds"]
        if max_allowed_processing_latency_seconds is None
        else float(max_allowed_processing_latency_seconds)
    )
    allowed_gap_irreparable = (
        slo["max_allowed_gap_irreparable"]
        if max_allowed_gap_irreparable == 0 and "max_allowed_gap_irreparable" in slo
        else int(max_allowed_gap_irreparable)
    )
    allowed_compaction_failures = (
        slo["max_allowed_compaction_failures"]
        if max_allowed_compaction_failures == 0 and "max_allowed_compaction_failures" in slo
        else int(max_allowed_compaction_failures)
    )
    for index in range(iterations):
        with tempfile.TemporaryDirectory() as tmp_dir:
            base_dir = Path(tmp_dir)
            buffer = io.StringIO()
            cfg = _cfg(base_dir, rest_base=rest_base, symbols=[symbol] if mode == "ws-live" else None)
            cfg.ws_base = ws_base
            writer = ParquetWriter(base_dir=base_dir, env="test", flush_size=256, dedup=True, schema_version=pipeline_version)
            sink = ParquetEventSink(writer)
            collect_kwargs = {
                "mode": "live",
                "cfg": cfg,
                "max_events": events_per_iteration,
                "duration_s": duration_seconds if mode == "ws-live" else 0,
                "logger": get_logger(name=f"ops.soak.{index}", level="INFO", stream=buffer),
                "sink": sink,
                "snapshot_enabled": mode == "ws-live",
                "summary_logging": True,
                "dedup_enabled": True,
                "batch_size": 1 if mode == "ws-live" else 32,
                "pipeline_version": pipeline_version,
            }
            if mode == "ws-live":
                checkpoint_dir = base_dir / "checkpoints"
                checkpoint_dir.mkdir(parents=True, exist_ok=True)
                collect_kwargs["source"] = (
                    source_builder(cfg)
                    if source_builder is not None
                    else _build_ws_live_canary_source(
                        cfg,
                        stream_type=stream_type,
                        reconnect_after_events=reconnect_after_events,
                        induced_reconnects=induced_reconnects,
                    )
                )
                collect_kwargs["stream_types"] = (stream_type,)
                collect_kwargs["checkpoint_store"] = CheckpointStore(checkpoint_dir / "soak-checkpoint.json")
            else:
                collect_kwargs["source"] = StaticSource(events=_events(events_per_iteration))
            collect_events(**collect_kwargs)
            sink.close()
            run_compaction_job(
                base_dir,
                "test",
                policy=CompactionJobPolicy(
                    batch_limit=10,
                    retry_attempts=1,
                    min_segments_pending=1,
                    min_compaction_lag_seconds=0.0,
                    retain_compacted_segments=1,
                ),
            )
            run = _extract_run(_json_lines(buffer), pipeline_version=pipeline_version, shadow_mode=False)
            runs.append(run)
            reconnects_observed += run.reconnects
            compaction_failures_total += collect_storage_health(base_dir, "test").compaction_failures_total
    elapsed = max(0.0, time.perf_counter() - started)
    reconnects_target = max(0, induced_reconnects * iterations) if mode == "ws-live" else 0
    observed_max_gaps = max(run.gaps for run in runs)
    observed_max_duplicates = max(run.duplicates for run in runs)
    observed_max_gap_irreparable = max(run.gap_irreparable for run in runs)
    observed_max_heartbeat_missed_total = max(run.heartbeat_missed_total for run in runs)
    observed_max_exchange_receive_skew_seconds = max(run.exchange_receive_skew_seconds for run in runs)
    observed_max_receive_process_skew_seconds = max(run.receive_process_skew_seconds for run in runs)
    observed_max_processing_latency_seconds = max(run.processing_latency_seconds for run in runs)
    observed_max_streams_degraded = max(len(run.streams_degraded) for run in runs)
    evidence = SoakEvidence(
        generated_at=datetime.now(timezone.utc).isoformat(),
        target_profile=target_profile,
        mode=mode,
        vendor="BINANCE" if mode == "ws-live" else "STATIC",
        symbol=symbol,
        stream_type=stream_type if mode == "ws-live" else "trade",
        interval=interval if mode == "ws-live" and stream_type == "kline" else None,
        iterations=iterations,
        max_events_per_iteration=events_per_iteration,
        duration_seconds=float(duration_seconds if mode == "ws-live" else 0.0),
        reconnects_target=reconnects_target,
        reconnects_observed=reconnects_observed,
        elapsed_seconds=elapsed,
        max_processing_latency_seconds=observed_max_processing_latency_seconds,
        max_write_latency_seconds=max(run.write_latency_seconds for run in runs),
        total_events_persisted=sum(run.events_persisted for run in runs),
        max_allowed_gaps=allowed_gaps,
        max_gaps=observed_max_gaps,
        max_allowed_duplicates=allowed_duplicates,
        max_duplicates=observed_max_duplicates,
        max_allowed_gap_irreparable=allowed_gap_irreparable,
        max_gap_irreparable=observed_max_gap_irreparable,
        max_allowed_heartbeat_missed_total=allowed_heartbeat_missed_total,
        max_heartbeat_missed_total=observed_max_heartbeat_missed_total,
        max_allowed_exchange_receive_skew_seconds=allowed_exchange_receive_skew_seconds,
        max_exchange_receive_skew_seconds=observed_max_exchange_receive_skew_seconds,
        max_allowed_receive_process_skew_seconds=allowed_receive_process_skew_seconds,
        max_receive_process_skew_seconds=observed_max_receive_process_skew_seconds,
        max_allowed_processing_latency_seconds=allowed_processing_latency_seconds,
        max_allowed_compaction_failures=allowed_compaction_failures,
        compaction_failures_total=compaction_failures_total,
        max_streams_degraded=observed_max_streams_degraded,
        slo=slo,
        pass_ok=(
            all(run.result == "ok" for run in runs)
            and (mode != "ws-live" or observed_max_duplicates <= allowed_duplicates)
            and (mode != "ws-live" or observed_max_gaps <= allowed_gaps)
            and (mode != "ws-live" or observed_max_gap_irreparable <= allowed_gap_irreparable)
            and (mode != "ws-live" or observed_max_heartbeat_missed_total <= allowed_heartbeat_missed_total)
            and (mode != "ws-live" or observed_max_exchange_receive_skew_seconds <= allowed_exchange_receive_skew_seconds)
            and (mode != "ws-live" or observed_max_receive_process_skew_seconds <= allowed_receive_process_skew_seconds)
            and (mode != "ws-live" or observed_max_processing_latency_seconds <= allowed_processing_latency_seconds)
            and compaction_failures_total <= allowed_compaction_failures
            and (mode != "ws-live" or observed_max_streams_degraded == 0)
            and (mode != "ws-live" or reconnects_observed >= reconnects_target)
        ),
        runs=runs,
    )
    write_json_report(output_path, asdict(evidence))
    return evidence


def run_canary_validation(
    output_path: Path,
    *,
    baseline_version: str = "v1",
    candidate_version: str = "v2",
    event_count: int = 200,
    bars: int | None = None,
    rest_base: str = "https://api.binance.com",
    symbol: str = "BTCUSDT",
    interval: str = "1m",
    baseline_path: Path | None = None,
    refresh_baseline: bool = False,
    end_time: datetime | None = None,
    fetch_rows: CanaryFetchRows | None = None,
) -> CanaryEvidence:
    baseline_path = Path(baseline_path or (output_path.parent / "ingestion_canary_baseline.json"))
    baseline_bars = max(1, int(bars if bars is not None else event_count))
    baseline, baseline_source = _load_or_refresh_canary_baseline(
        baseline_path,
        rest_base=rest_base,
        symbol=symbol,
        interval=interval,
        bars=baseline_bars,
        refresh_baseline=refresh_baseline,
        end_time=end_time,
        fetch_rows=fetch_rows,
    )
    events = _events_from_canary_baseline(baseline)

    with tempfile.TemporaryDirectory() as tmp_dir:
        base_dir = Path(tmp_dir)
        baseline_run = _execute_canary_version(
            base_dir=base_dir,
            version=baseline_version,
            events=events,
        )
        candidate_run = _execute_canary_version(
            base_dir=base_dir,
            version=candidate_version,
            events=events,
        )
        diffs = _canary_diffs(
            vendor_rows=_projection_rows_from_events(events),
            baseline_rows=_projection_rows_from_storage(base_dir, pipeline_version=baseline_version),
            candidate_rows=_projection_rows_from_storage(base_dir, pipeline_version=candidate_version),
        )

    pass_ok = (
        baseline_run.result == "ok"
        and candidate_run.result == "ok"
        and diffs["row_count"] == 0
        and bool(diffs["baseline_matches_vendor"])
        and bool(diffs["candidate_matches_vendor"])
        and bool(diffs["projection_checksum_match"])
    )
    reason = "semantic_match" if pass_ok else "semantic_diff_detected"
    evidence = CanaryEvidence(
        baseline_path=str(baseline_path),
        baseline_source=baseline_source,
        vendor=baseline.vendor,
        symbol=baseline.symbol,
        interval=baseline.interval,
        bars=baseline.bars,
        baseline_hash=baseline.payload_sha256,
        baseline=baseline_run,
        candidate=candidate_run,
        diffs=diffs,
        pass_ok=pass_ok,
        comparison_reason=reason,
    )
    write_json_report(output_path, asdict(evidence))
    return evidence


def _build_ws_live_canary_source(
    cfg: SimpleNamespace,
    *,
    stream_type: str,
    reconnect_after_events: int,
    induced_reconnects: int,
) -> Source:
    reconnect_state = {"remaining": max(0, induced_reconnects)}
    heartbeat = heartbeat_policy_for_streams((stream_type,))

    def controlled_ws_stream(url: str, end_time: float | None = None):
        ws = _default_ws_connect(url)
        last_activity = time.monotonic()
        last_ping = last_activity
        session_events = 0
        try:
            while True:
                if end_time and time.time() >= end_time:
                    break
                try:
                    raw = ws.recv(timeout=heartbeat.recv_timeout_seconds)
                except TimeoutError:
                    now = time.monotonic()
                    idle_seconds = now - last_activity
                    if idle_seconds >= heartbeat.ping_interval_seconds and now - last_ping >= heartbeat.ping_interval_seconds:
                        pong = ws.ping()
                        if not pong.wait(timeout=heartbeat.ping_timeout_seconds):
                            raise TimeoutError(
                                f"websocket heartbeat timeout after {idle_seconds:.1f}s idle"
                            )
                        last_activity = time.monotonic()
                        last_ping = last_activity
                        continue
                    if idle_seconds >= heartbeat.inactivity_timeout_seconds:
                        raise TimeoutError(
                            f"websocket inactivity watchdog exceeded {heartbeat.inactivity_timeout_seconds:.1f}s"
                        )
                    continue
                last_activity = time.monotonic()
                yield raw
                session_events += 1
                if reconnect_state["remaining"] > 0 and session_events >= max(1, reconnect_after_events):
                    reconnect_state["remaining"] -= 1
                    ws.close_socket()
                    raise ConnectionAbortedError("ws canary induced reconnect")
        finally:
            try:
                ws.close_socket()
            except Exception:
                pass

    return BinanceSource(
        cfg=cfg,
        stream_types=(stream_type,),
        ws_stream=controlled_ws_stream,
        raw_sink=JsonlRawSink(Path(cfg.data_dir) / "raw", env=cfg.env),
        heartbeat_policy=heartbeat,
    )


def run_ws_live_canary(
    output_path: Path,
    *,
    target_profile: Literal["paper", "live"] = "live",
    symbol: str = "BTCUSDT",
    stream_type: str = "kline",
    interval: str = "1m",
    ws_base: str = "wss://stream.binance.com:9443",
    rest_base: str = "https://api.binance.com",
    max_events: int = 2,
    duration_seconds: float = 130.0,
    reconnect_after_events: int = 1,
    induced_reconnects: int = 1,
    pipeline_version: str = "v2",
    max_allowed_duplicates: int | None = None,
    max_allowed_gaps: int | None = None,
    max_allowed_heartbeat_missed_total: int | None = None,
    max_allowed_exchange_receive_skew_seconds: float | None = None,
    max_allowed_receive_process_skew_seconds: float | None = None,
    max_allowed_processing_latency_seconds: float | None = None,
    source_builder: WSCanarySourceBuilder | None = None,
) -> WSCanaryEvidence:
    if stream_type not in {"trade", "kline"}:
        raise ValueError("ws-live canary supports only trade and kline feeds")
    slo = _validation_slo(target_profile)
    allowed_duplicates = slo["max_allowed_duplicates"] if max_allowed_duplicates is None else int(max_allowed_duplicates)
    allowed_gaps = slo["max_allowed_gaps"] if max_allowed_gaps is None else int(max_allowed_gaps)
    allowed_heartbeat_missed_total = (
        slo["max_allowed_heartbeat_missed_total"]
        if max_allowed_heartbeat_missed_total is None
        else int(max_allowed_heartbeat_missed_total)
    )
    allowed_exchange_receive_skew_seconds = (
        slo["max_allowed_exchange_receive_skew_seconds"]
        if max_allowed_exchange_receive_skew_seconds is None
        else float(max_allowed_exchange_receive_skew_seconds)
    )
    allowed_receive_process_skew_seconds = (
        slo["max_allowed_receive_process_skew_seconds"]
        if max_allowed_receive_process_skew_seconds is None
        else float(max_allowed_receive_process_skew_seconds)
    )
    allowed_processing_latency_seconds = (
        slo["max_allowed_processing_latency_seconds"]
        if max_allowed_processing_latency_seconds is None
        else float(max_allowed_processing_latency_seconds)
    )

    with tempfile.TemporaryDirectory() as tmp_dir:
        base_dir = Path(tmp_dir)
        cfg = _cfg(base_dir, rest_base=rest_base, symbols=[symbol])
        cfg.ws_base = ws_base
        log_buffer = io.StringIO()
        checkpoint_dir = base_dir / "checkpoints"
        checkpoint_dir.mkdir(parents=True, exist_ok=True)
        checkpoint_store = CheckpointStore(checkpoint_dir / "canary-checkpoint.json")
        source = source_builder(cfg) if source_builder is not None else _build_ws_live_canary_source(
            cfg,
            stream_type=stream_type,
            reconnect_after_events=reconnect_after_events,
            induced_reconnects=induced_reconnects,
        )
        collect_events(
            mode="live",
            cfg=cfg,
            max_events=max_events,
            duration_s=duration_seconds,
            logger=get_logger(name="ops.ws_canary", level="INFO", stream=log_buffer),
            source=source,
            sink=None,
            snapshot_enabled=True,
            summary_logging=True,
            dedup_enabled=True,
            batch_size=1,
            pipeline_version=pipeline_version,
            stream_types=(stream_type,),
            checkpoint_store=checkpoint_store,
        )
        logs = _json_lines(log_buffer)
        summary = next(record for record in logs if record["message"] == "ingestion summary")
        run = _extract_run(logs, pipeline_version=pipeline_version, shadow_mode=False)
        alert_types = _extract_alert_types(logs)
        stream_metrics = list(summary.get("stream_metrics") or [])
        heartbeat_missed_total = _sum_stream_metric(stream_metrics, "heartbeat_missed_total")
        exchange_receive_skew_seconds = _max_stream_metric(stream_metrics, "exchange_receive_skew_seconds")
        receive_process_skew_seconds = _max_stream_metric(stream_metrics, "receive_process_skew_seconds")
        continuity = {
            "events_persisted": run.events_persisted,
            "duplicates": run.duplicates,
            "gaps": run.gaps,
            "gap_irreparable": run.gap_irreparable,
            "reconnects": run.reconnects,
            "result": run.result,
            "streams_degraded": list(run.streams_degraded),
            "heartbeat_missed_total": heartbeat_missed_total,
            "exchange_receive_skew_seconds": exchange_receive_skew_seconds,
            "receive_process_skew_seconds": receive_process_skew_seconds,
            "processing_latency_seconds": run.processing_latency_seconds,
        }
        checkpoint_audit_events = _count_jsonl_records(checkpoint_dir / "ingestion-checkpoint-audit.jsonl")
        recovery_audit_events = sum(
            1
            for metric in stream_metrics
            if metric.get("last_recovery_request_start_ts") is not None
        )
        pass_ok = (
            run.result == "ok"
            and run.events_persisted > 0
            and run.reconnects >= max(0, induced_reconnects)
            and run.duplicates <= allowed_duplicates
            and run.gaps <= allowed_gaps
            and run.gap_irreparable == 0
            and heartbeat_missed_total <= allowed_heartbeat_missed_total
            and exchange_receive_skew_seconds <= allowed_exchange_receive_skew_seconds
            and receive_process_skew_seconds <= allowed_receive_process_skew_seconds
            and run.processing_latency_seconds <= allowed_processing_latency_seconds
            and not run.streams_degraded
        )
        reasons: list[str] = []
        if run.result != "ok":
            reasons.append(f"result={run.result}")
        if run.events_persisted <= 0:
            reasons.append("no_events_persisted")
        if run.reconnects < max(0, induced_reconnects):
            reasons.append("reconnect_target_not_met")
        if run.duplicates > allowed_duplicates:
            reasons.append("duplicates_detected")
        if run.gaps > allowed_gaps:
            reasons.append("gaps_detected")
        if run.gap_irreparable > 0:
            reasons.append("gap_irreparable_detected")
        if heartbeat_missed_total > allowed_heartbeat_missed_total:
            reasons.append("heartbeat_missed_detected")
        if exchange_receive_skew_seconds > allowed_exchange_receive_skew_seconds:
            reasons.append("exchange_receive_skew_slo_breached")
        if receive_process_skew_seconds > allowed_receive_process_skew_seconds:
            reasons.append("receive_process_skew_slo_breached")
        if run.processing_latency_seconds > allowed_processing_latency_seconds:
            reasons.append("processing_latency_slo_breached")
        if run.streams_degraded:
            reasons.append("streams_degraded_detected")
        evidence = WSCanaryEvidence(
            mode="ws-live",
            target_profile=target_profile,
            vendor="BINANCE",
            ws_base=ws_base,
            symbol=symbol,
            stream_type=stream_type,
            interval=interval if stream_type == "kline" else None,
            max_events=max_events,
            duration_seconds=float(duration_seconds),
            reconnects_target=max(0, induced_reconnects),
            reconnect_after_events=max(1, reconnect_after_events),
            reconnects_observed=run.reconnects,
            continuity=continuity,
            stream_metrics=stream_metrics,
            alert_types=alert_types,
            checkpoint_audit_events=checkpoint_audit_events,
            recovery_audit_events=recovery_audit_events,
            report_generated_at=datetime.now(timezone.utc).isoformat(),
            slo=slo,
            pass_ok=pass_ok,
            comparison_reason="continuity_ok" if pass_ok else ",".join(reasons),
        )
    write_json_report(output_path, asdict(evidence))
    return evidence


def run_vendor_contract_validation(
    output_path: Path,
    *,
    pytest_target: str = "tests/network/test_binance_contracts.py",
    runner: Callable[..., subprocess.CompletedProcess[str]] | None = None,
) -> VendorContractsEvidence:
    command = [sys.executable, "-m", "pytest", pytest_target, "-q", "-m", "network"]
    started = time.perf_counter()
    effective_runner = runner or subprocess.run
    result = effective_runner(
        command,
        capture_output=True,
        text=True,
        check=False,
    )
    elapsed = max(0.0, time.perf_counter() - started)
    evidence = VendorContractsEvidence(
        generated_at=datetime.now(timezone.utc).isoformat(),
        pytest_target=pytest_target,
        command=list(command),
        cwd=str(Path.cwd()),
        python_executable=sys.executable,
        duration_seconds=elapsed,
        returncode=int(result.returncode),
        pass_ok=result.returncode == 0,
        stdout=str(result.stdout),
        stderr=str(result.stderr),
    )
    write_json_report(output_path, asdict(evidence))
    return evidence


def run_failure_injection_validation(
    output_path: Path,
    *,
    pytest_target: str = "tests/ops/test_failure_injection.py",
    critical_test_ids: Sequence[str] = CRITICAL_FAILURE_INJECTION_TEST_IDS,
    runner: Callable[..., subprocess.CompletedProcess[str]] | None = None,
) -> FailureInjectionEvidence:
    selected_ids = tuple(str(test_id) for test_id in critical_test_ids)
    command = [sys.executable, "-m", "pytest", "-q", *selected_ids]
    started = time.perf_counter()
    effective_runner = runner or subprocess.run
    result = effective_runner(
        command,
        capture_output=True,
        text=True,
        check=False,
    )
    elapsed = max(0.0, time.perf_counter() - started)
    evidence = FailureInjectionEvidence(
        generated_at=datetime.now(timezone.utc).isoformat(),
        pytest_target=pytest_target,
        critical_test_ids=selected_ids,
        command=list(command),
        cwd=str(Path.cwd()),
        python_executable=sys.executable,
        duration_seconds=elapsed,
        returncode=int(result.returncode),
        pass_ok=result.returncode == 0,
        stdout=str(result.stdout),
        stderr=str(result.stderr),
    )
    write_json_report(output_path, asdict(evidence))
    return evidence


def _benchmark_trade_events(
    *,
    symbols: Sequence[str],
    bursts: int,
    events_per_symbol_per_burst: int,
) -> list[TradeEvent]:
    base = datetime(2024, 1, 1, tzinfo=timezone.utc)
    burst_window_seconds = max(2, events_per_symbol_per_burst + 1)
    events: list[TradeEvent] = []
    for burst in range(bursts):
        for symbol_index, symbol in enumerate(symbols):
            for offset in range(events_per_symbol_per_burst):
                event_ts = base + timedelta(
                    seconds=burst * burst_window_seconds + offset,
                )
                trade_id = symbol_index * 100_000 + burst * events_per_symbol_per_burst + offset + 1
                events.append(
                    TradeEvent(
                        symbol=symbol,
                        exchange_ts=event_ts,
                        receive_ts=event_ts,
                        process_ts=event_ts,
                        venue="BINANCE",
                        source_id=str(trade_id),
                        price=100.0 + symbol_index + burst,
                        size=1.0 + (offset / 10.0),
                        trade_id=str(trade_id),
                        side="buy" if offset % 2 else "sell",
                        metadata={
                            "instrument_catalog_version": "benchmark",
                            "instrument_snapshot": json.dumps(
                                {
                                    "symbol": symbol,
                                    "venue": "BINANCE",
                                    "price_precision": 8,
                                    "size_precision": 8,
                                },
                                sort_keys=True,
                                separators=(",", ":"),
                            ),
                            "metadata_source": "benchmark",
                        },
                    )
                )
    return events


def _seed_replay_raw_dataset(
    base_dir: Path,
    *,
    env: str,
    symbols: Sequence[str],
    bursts: int,
    events_per_symbol_per_burst: int,
) -> int:
    sink = JsonlRawSink(base_dir / "raw", env=env, run_id="20240101T000000000100Z-bench")
    base = datetime(2024, 1, 1, tzinfo=timezone.utc)
    burst_window_seconds = max(2, events_per_symbol_per_burst + 1)
    count = 0
    for burst in range(bursts):
        for symbol_index, symbol in enumerate(symbols):
            for offset in range(events_per_symbol_per_burst):
                trade_id = symbol_index * 100_000 + burst * events_per_symbol_per_burst + offset + 1
                event_ts = base + timedelta(
                    seconds=burst * burst_window_seconds + offset,
                )
                payload = {
                    "stream": f"{symbol.lower()}@trade",
                    "data": {
                        "s": symbol,
                        "E": int(event_ts.timestamp() * 1000),
                        "p": f"{100.0 + symbol_index + burst:.2f}",
                        "q": f"{1.0 + (offset / 10.0):.4f}",
                        "t": trade_id,
                        "m": bool(offset % 2),
                    },
                }
                sink.write(
                    RawRecord(
                        payload=payload,
                        venue="BINANCE",
                        stream_type="trade",
                        symbol=symbol,
                        exchange_ts=event_ts,
                        receive_ts=event_ts,
                        process_ts=event_ts,
                        trace_id="storage-benchmark",
                        source_id=str(trade_id),
                    )
                )
                count += 1
    return count


def _make_benchmark_case(
    *,
    name: str,
    dataset_kind: str,
    target_profile: BenchmarkTargetProfile,
    requested_symbol_count: int,
    rows_in: int,
    partitions: int,
    bursts: int,
    batch_size: int,
    elapsed_seconds: float,
    max_write_latency_seconds: float,
    compaction_elapsed_seconds: float,
    shadow_elapsed_seconds: float,
    segments_pending_total: int,
    segments_per_partition_max: int,
    normalized_partition_row_count: int,
    min_rows_per_second: float,
    max_write_latency_slo: float,
    max_compaction_elapsed_slo: float,
    max_shadow_elapsed_slo: float,
) -> StorageBenchmarkCase:
    rows_per_second = rows_in / max(elapsed_seconds, 1e-9)
    pass_ok = (
        rows_per_second >= min_rows_per_second
        and max_write_latency_seconds <= max_write_latency_slo
        and compaction_elapsed_seconds <= max_compaction_elapsed_slo
        and shadow_elapsed_seconds <= max_shadow_elapsed_slo
    )
    return StorageBenchmarkCase(
        name=name,
        dataset_kind=dataset_kind,
        target_profile=target_profile,
        requested_symbol_count=requested_symbol_count,
        rows_in=rows_in,
        partitions=partitions,
        bursts=bursts,
        batch_size=batch_size,
        elapsed_seconds=elapsed_seconds,
        rows_per_second=rows_per_second,
        max_write_latency_seconds=max_write_latency_seconds,
        compaction_elapsed_seconds=compaction_elapsed_seconds,
        shadow_elapsed_seconds=shadow_elapsed_seconds,
        segments_pending_total=segments_pending_total,
        segments_per_partition_max=segments_per_partition_max,
        normalized_partition_row_count=normalized_partition_row_count,
        pass_ok=pass_ok,
    )


def storage_benchmark_slo_for_target(
    target_profile: BenchmarkTargetProfile,
    *,
    min_rows_per_second: float | None = None,
    max_write_latency_slo: float | None = None,
    max_compaction_elapsed_slo: float | None = None,
    max_shadow_elapsed_slo: float | None = None,
    high_cardinality_symbol_counts: Sequence[int] | None = None,
) -> dict[str, object]:
    defaults: dict[BenchmarkTargetProfile, dict[str, object]] = {
        "paper": {
            "min_rows_per_second": 50.0,
            "max_write_latency_seconds": 2.0,
            "max_compaction_elapsed_seconds": 5.0,
            "max_shadow_elapsed_seconds": 5.0,
            "required_high_cardinality_symbol_counts": (100,),
        },
        "live": {
            "min_rows_per_second": 100.0,
            "max_write_latency_seconds": 2.0,
            "max_compaction_elapsed_seconds": 5.0,
            "max_shadow_elapsed_seconds": 5.0,
            "required_high_cardinality_symbol_counts": (100, 500),
        },
        "robustness": {
            "min_rows_per_second": 125.0,
            "max_write_latency_seconds": 2.0,
            "max_compaction_elapsed_seconds": 5.0,
            "max_shadow_elapsed_seconds": 5.0,
            "required_high_cardinality_symbol_counts": (100, 500, 1000),
        },
    }
    profile = defaults[target_profile]
    requested_counts = tuple(int(value) for value in (high_cardinality_symbol_counts or profile["required_high_cardinality_symbol_counts"]))
    return {
        "target_profile": target_profile,
        "min_rows_per_second": float(profile["min_rows_per_second"] if min_rows_per_second is None else min_rows_per_second),
        "max_write_latency_seconds": float(
            profile["max_write_latency_seconds"] if max_write_latency_slo is None else max_write_latency_slo
        ),
        "max_compaction_elapsed_seconds": float(
            profile["max_compaction_elapsed_seconds"]
            if max_compaction_elapsed_slo is None
            else max_compaction_elapsed_slo
        ),
        "max_shadow_elapsed_seconds": float(
            profile["max_shadow_elapsed_seconds"] if max_shadow_elapsed_slo is None else max_shadow_elapsed_slo
        ),
        "required_high_cardinality_symbol_counts": tuple(requested_counts),
    }


def run_storage_benchmark(
    output_path: Path,
    *,
    target_profile: BenchmarkTargetProfile = "paper",
    symbol_count: int = 12,
    high_cardinality_symbol_counts: Sequence[int] = (),
    bursts: int = 4,
    events_per_symbol_per_burst: int = 12,
    min_rows_per_second: float | None = None,
    max_write_latency_slo: float | None = None,
    max_compaction_elapsed_slo: float | None = None,
    max_shadow_elapsed_slo: float | None = None,
    cleanup: bool = True,
    workspace_dir: Path | None = None,
) -> StorageBenchmarkEvidence:
    slo = storage_benchmark_slo_for_target(
        target_profile,
        min_rows_per_second=min_rows_per_second,
        max_write_latency_slo=max_write_latency_slo,
        max_compaction_elapsed_slo=max_compaction_elapsed_slo,
        max_shadow_elapsed_slo=max_shadow_elapsed_slo,
        high_cardinality_symbol_counts=high_cardinality_symbol_counts,
    )
    required_high_cardinality_symbol_counts = tuple(
        int(value) for value in slo["required_high_cardinality_symbol_counts"]  # type: ignore[index]
    )
    benchmark_symbols = ["BTCUSDT", "ETHUSDT", "SOLUSDT"]
    if symbol_count <= len(benchmark_symbols):
        symbols = benchmark_symbols[: max(1, symbol_count)]
    else:
        symbols = [f"BENCH{index:04d}USDT" for index in range(1, symbol_count + 1)]
    _ensure_benchmark_symbols_registered(symbols)
    rows_in = len(symbols) * max(1, bursts) * max(1, events_per_symbol_per_burst)
    partitions = len(symbols)
    min_rows_per_second_value = float(slo["min_rows_per_second"])
    max_write_latency_slo_value = float(slo["max_write_latency_seconds"])
    max_compaction_elapsed_slo_value = float(slo["max_compaction_elapsed_seconds"])
    max_shadow_elapsed_slo_value = float(slo["max_shadow_elapsed_seconds"])

    base_dir = Path(workspace_dir) if workspace_dir is not None else Path(tempfile.mkdtemp(prefix="ingestion-storage-benchmark-"))
    extra_cleanup_dirs: list[Path] = []
    try:
        def _run_shadow_case() -> StorageBenchmarkCase:
            shadow_base_dir = (
                Path(tempfile.mkdtemp(prefix="ingestion-shadow-benchmark-"))
                if workspace_dir is None
                else base_dir / "shadow-benchmark"
            )
            if workspace_dir is None:
                extra_cleanup_dirs.append(shadow_base_dir)
            shadow_bursts = max(2, min(3, bursts))
            shadow_events_per_symbol = max(8, min(12, events_per_symbol_per_burst))
            shadow_symbol_count = max(2, min(2, len(symbols)))
            if target_profile == "live":
                shadow_bursts = max(4, shadow_bursts)
                shadow_events_per_symbol = max(64, events_per_symbol_per_burst * 4)
                shadow_symbol_count = 1
            shadow_symbols = [f"SHADOW{index:04d}USDT" for index in range(1, shadow_symbol_count + 1)]
            shadow_events = _benchmark_trade_events(
                symbols=shadow_symbols,
                bursts=shadow_bursts,
                events_per_symbol_per_burst=shadow_events_per_symbol,
            )
            gc.collect()
            shadow_batch_size = max(24, len(shadow_events))
            shadow_partition_flush_size = shadow_bursts * shadow_events_per_symbol if target_profile == "live" else shadow_events_per_symbol
            shadow_started = time.perf_counter()
            primary_sink = ParquetEventSink(
                ParquetWriter(
                    base_dir=shadow_base_dir,
                    env="test",
                    flush_size=shadow_batch_size,
                    partition_flush_size=shadow_partition_flush_size,
                    dedup=True,
                    schema_version="v2",
                    max_parallel_partition_writes=8,
                )
            )
            shadow_sink = ParquetEventSink(
                ParquetWriter(
                    base_dir=shadow_base_dir,
                    env="test",
                    flush_size=shadow_batch_size,
                    partition_flush_size=shadow_partition_flush_size,
                    dedup=True,
                    schema_version="v1",
                    max_parallel_partition_writes=8,
                )
            )
            mirrored_sink = MirroredEventSink(primary_sink, shadow_sink)
            mirrored_sink.add(shadow_events)
            mirrored_sink.close()
            shadow_partitions = affected_shadow_partitions(shadow_events)
            primary_snapshot = build_shadow_snapshot(
                shadow_base_dir,
                env="test",
                pipeline_version="v2",
                gaps_total=0,
                processing_latency_seconds=0.0,
                write_latency_seconds=primary_sink.write_latency_seconds,
                partition_keys=shadow_partitions or None,
            )
            shadow_snapshot = build_shadow_snapshot(
                shadow_base_dir,
                env="test",
                pipeline_version="v1",
                gaps_total=0,
                processing_latency_seconds=0.0,
                write_latency_seconds=shadow_sink.write_latency_seconds,
                partition_keys=shadow_partitions or None,
            )
            shadow_comparison = compare_shadow_snapshots(primary_snapshot, shadow_snapshot)
            persist_shadow_comparison(shadow_base_dir, env="test", comparison=shadow_comparison)
            if shadow_comparison.significant:
                raise AssertionError(f"shadow benchmark detected semantic diff: {shadow_comparison.diffs}")
            shadow_elapsed = max(0.0, time.perf_counter() - shadow_started)
            return _make_benchmark_case(
                name="shadow_scoped_runtime",
                dataset_kind="synthetic",
                target_profile=target_profile,
                requested_symbol_count=len({event.symbol for event in shadow_events}),
                rows_in=len(shadow_events) * 2,
                partitions=len({event.symbol for event in shadow_events}),
                bursts=shadow_bursts,
                batch_size=shadow_batch_size,
                elapsed_seconds=shadow_elapsed,
                max_write_latency_seconds=0.0,
                compaction_elapsed_seconds=0.0,
                shadow_elapsed_seconds=shadow_elapsed,
                segments_pending_total=0,
                segments_per_partition_max=0,
                normalized_partition_row_count=0,
                min_rows_per_second=min_rows_per_second_value,
                max_write_latency_slo=max_write_latency_slo_value,
                max_compaction_elapsed_slo=max_compaction_elapsed_slo_value,
                max_shadow_elapsed_slo=max_shadow_elapsed_slo_value,
            )

        shadow_case = _run_shadow_case()

        synthetic_events = _benchmark_trade_events(
            symbols=symbols,
            bursts=bursts,
            events_per_symbol_per_burst=events_per_symbol_per_burst,
        )
        writer = ParquetWriter(base_dir=base_dir, env="test", flush_size=max(16, events_per_symbol_per_burst), dedup=True)
        started = time.perf_counter()
        sink = ParquetEventSink(writer)
        collect_events(
            mode="live",
            cfg=_cfg(base_dir, symbols=list(symbols)),
            max_events=rows_in,
            duration_s=0,
            logger=get_logger(name="ops.storage.synthetic", level="INFO", stream=io.StringIO()),
            source=StaticSource(events=synthetic_events),
            sink=sink,
            snapshot_enabled=False,
            summary_logging=True,
            dedup_enabled=True,
            batch_size=max(8, events_per_symbol_per_burst),
            pipeline_version="v2",
        )
        sink.close()
        synthetic_elapsed = max(0.0, time.perf_counter() - started)
        synthetic_health = collect_storage_health(base_dir, "test")
        synthetic_case = _make_benchmark_case(
            name="synthetic_segmented_write",
            dataset_kind="synthetic",
            target_profile=target_profile,
            requested_symbol_count=len(symbols),
            rows_in=rows_in,
            partitions=partitions,
            bursts=bursts,
            batch_size=max(8, events_per_symbol_per_burst),
            elapsed_seconds=synthetic_elapsed,
            max_write_latency_seconds=writer.max_write_latency_seconds,
            compaction_elapsed_seconds=0.0,
            shadow_elapsed_seconds=0.0,
            segments_pending_total=synthetic_health.segments_pending_total,
            segments_per_partition_max=synthetic_health.segments_per_partition_max,
            normalized_partition_row_count=synthetic_health.normalized_partition_row_count,
            min_rows_per_second=min_rows_per_second_value,
            max_write_latency_slo=max_write_latency_slo_value,
            max_compaction_elapsed_slo=max_compaction_elapsed_slo_value,
            max_shadow_elapsed_slo=max_shadow_elapsed_slo_value,
        )
        del synthetic_events, writer, sink, synthetic_health

        replay_rows = _seed_replay_raw_dataset(
            base_dir,
            env="test",
            symbols=symbols,
            bursts=bursts,
            events_per_symbol_per_burst=events_per_symbol_per_burst,
        )
        replay_writer = ParquetWriter(base_dir=base_dir / "replay-benchmark", env="test", flush_size=max(16, events_per_symbol_per_burst), dedup=True)
        replay_started = time.perf_counter()
        replay_sink = ParquetEventSink(replay_writer)
        collect_events(
            mode="live",
            cfg=_cfg(base_dir / "replay-benchmark", symbols=list(symbols)),
            max_events=replay_rows,
            duration_s=0,
            logger=get_logger(name="ops.storage.replay", level="INFO", stream=io.StringIO()),
            source=ReplaySource(base_dir=base_dir / "raw", env="test", stream_types=("trade",)),
            sink=replay_sink,
            snapshot_enabled=False,
            summary_logging=True,
            dedup_enabled=True,
            batch_size=max(8, events_per_symbol_per_burst),
            pipeline_version="v2",
        )
        replay_sink.close()
        replay_elapsed = max(0.0, time.perf_counter() - replay_started)
        replay_health = collect_storage_health(base_dir / "replay-benchmark", "test")
        replay_case = _make_benchmark_case(
            name="replay_backed_segmented_write",
            dataset_kind="replay_raw",
            target_profile=target_profile,
            requested_symbol_count=len(symbols),
            rows_in=replay_rows,
            partitions=partitions,
            bursts=bursts,
            batch_size=max(8, events_per_symbol_per_burst),
            elapsed_seconds=replay_elapsed,
            max_write_latency_seconds=replay_writer.max_write_latency_seconds,
            compaction_elapsed_seconds=0.0,
            shadow_elapsed_seconds=0.0,
            segments_pending_total=replay_health.segments_pending_total,
            segments_per_partition_max=replay_health.segments_per_partition_max,
            normalized_partition_row_count=replay_health.normalized_partition_row_count,
            min_rows_per_second=min_rows_per_second_value,
            max_write_latency_slo=max_write_latency_slo_value,
            max_compaction_elapsed_slo=max_compaction_elapsed_slo_value,
            max_shadow_elapsed_slo=max_shadow_elapsed_slo_value,
        )

        concurrent_base_dir = base_dir / "concurrent-benchmark"
        concurrent_symbols = symbols[: max(2, min(4, len(symbols)))]
        first_half_symbols = concurrent_symbols[: max(1, len(concurrent_symbols) // 2)]
        second_half_symbols = concurrent_symbols[max(1, len(concurrent_symbols) // 2) :] or concurrent_symbols[:1]
        concurrent_events_per_symbol = max(8, events_per_symbol_per_burst)
        if target_profile == "live":
            concurrent_events_per_symbol = max(24, events_per_symbol_per_burst * 2)
        concurrent_flush_size = max(48, concurrent_events_per_symbol * 4)
        first_writer = ParquetWriter(base_dir=concurrent_base_dir, env="test", flush_size=concurrent_flush_size, dedup=True)
        first_sink = ParquetEventSink(first_writer)
        first_events = _benchmark_trade_events(
            symbols=first_half_symbols,
            bursts=bursts,
            events_per_symbol_per_burst=concurrent_events_per_symbol,
        )
        collect_events(
            mode="live",
            cfg=_cfg(concurrent_base_dir, symbols=list(first_half_symbols)),
            max_events=len(first_events),
            duration_s=0,
            logger=get_logger(name="ops.storage.concurrent.first", level="INFO", stream=io.StringIO()),
            source=StaticSource(events=first_events),
            sink=first_sink,
            snapshot_enabled=False,
            summary_logging=True,
            dedup_enabled=True,
            batch_size=concurrent_flush_size,
            pipeline_version="v2",
        )
        first_sink.close()
        concurrent_days = sorted({event.event_ts.date().isoformat() for event in first_events})
        concurrent_partition_paths = tuple(
            normalized_partition_path(
                concurrent_base_dir,
                "test",
                source="trade",
                symbol=symbol,
                day=day,
            )
            for symbol in sorted(set(first_half_symbols))
            for day in concurrent_days
        )

        compaction_elapsed_holder = {"elapsed": 0.0}

        def _run_compaction() -> None:
            started_local = time.perf_counter()
            run_compaction_job(
                concurrent_base_dir,
                "test",
                partition_paths=concurrent_partition_paths,
                policy=CompactionJobPolicy(
                    batch_limit=1,
                    retry_attempts=1,
                    min_segments_pending=2,
                    min_compaction_lag_seconds=0.0,
                    retain_compacted_segments=0,
                ),
            )
            compaction_elapsed_holder["elapsed"] = max(0.0, time.perf_counter() - started_local)

        started_concurrent = time.perf_counter()
        compaction_thread = Thread(target=_run_compaction)
        compaction_thread.start()
        second_writer = ParquetWriter(base_dir=concurrent_base_dir, env="test", flush_size=concurrent_flush_size, dedup=True)
        second_sink = ParquetEventSink(second_writer)
        second_events = _benchmark_trade_events(
            symbols=second_half_symbols,
            bursts=bursts,
            events_per_symbol_per_burst=concurrent_events_per_symbol,
        )
        collect_events(
            mode="live",
            cfg=_cfg(concurrent_base_dir, symbols=list(second_half_symbols)),
            max_events=len(second_events),
            duration_s=0,
            logger=get_logger(name="ops.storage.concurrent.second", level="INFO", stream=io.StringIO()),
            source=StaticSource(events=second_events),
            sink=second_sink,
            snapshot_enabled=False,
            summary_logging=True,
            dedup_enabled=True,
            batch_size=concurrent_flush_size,
            pipeline_version="v2",
        )
        second_sink.close()
        compaction_thread.join()
        concurrent_elapsed = max(0.0, time.perf_counter() - started_concurrent)
        concurrent_health = collect_storage_health(concurrent_base_dir, "test")
        concurrent_case = _make_benchmark_case(
            name="concurrent_compaction",
            dataset_kind="synthetic",
            target_profile=target_profile,
            requested_symbol_count=len(set(first_half_symbols).union(second_half_symbols)),
            rows_in=len(first_events) + len(second_events),
            partitions=len(set(first_half_symbols).union(second_half_symbols)),
            bursts=bursts,
            batch_size=concurrent_flush_size,
            elapsed_seconds=concurrent_elapsed,
            max_write_latency_seconds=max(first_writer.max_write_latency_seconds, second_writer.max_write_latency_seconds),
            compaction_elapsed_seconds=compaction_elapsed_holder["elapsed"],
            shadow_elapsed_seconds=0.0,
            segments_pending_total=concurrent_health.segments_pending_total,
            segments_per_partition_max=concurrent_health.segments_per_partition_max,
            normalized_partition_row_count=concurrent_health.normalized_partition_row_count,
            min_rows_per_second=min_rows_per_second_value,
            max_write_latency_slo=max_write_latency_slo_value,
            max_compaction_elapsed_slo=max_compaction_elapsed_slo_value,
            max_shadow_elapsed_slo=max_shadow_elapsed_slo_value,
        )

        high_cardinality_cases: list[StorageBenchmarkCase] = []
        gc.collect()
        for high_symbol_count in required_high_cardinality_symbol_counts:
            case_symbols = [f"HICARD{index:04d}USDT" for index in range(1, max(1, int(high_symbol_count)) + 1)]
            _ensure_benchmark_symbols_registered(case_symbols)
            high_bursts = 1
            high_events_per_symbol = max(96, min(128, events_per_symbol_per_burst * 8))
            if target_profile == "live" and high_symbol_count >= 500:
                high_events_per_symbol = max(384, high_events_per_symbol)
            case_events = _benchmark_trade_events(
                symbols=case_symbols,
                bursts=high_bursts,
                events_per_symbol_per_burst=high_events_per_symbol,
            )
            case_base_dir = base_dir / f"high-cardinality-{high_symbol_count}"
            chunk_symbol_count = min(len(case_symbols), max(8, min(32, len(case_symbols) // 8 or 1)))
            if target_profile == "live" and high_symbol_count >= 500:
                chunk_symbol_count = min(len(case_symbols), 16)
            case_batch_size = max(high_events_per_symbol, chunk_symbol_count * high_events_per_symbol)
            case_flush_size = case_batch_size
            case_writer = ParquetWriter(
                base_dir=case_base_dir,
                env="test",
                flush_size=case_flush_size,
                partition_flush_size=high_events_per_symbol,
                dedup=True,
                max_parallel_partition_writes=32 if target_profile == "live" and high_symbol_count >= 500 else None,
            )
            case_started = time.perf_counter()
            for chunk_start in range(0, len(case_events), case_batch_size):
                case_writer.add(case_events[chunk_start : chunk_start + case_batch_size])
            case_writer.flush()
            case_elapsed = max(0.0, time.perf_counter() - case_started)
            case_health = collect_storage_health(case_base_dir, "test")
            high_cardinality_cases.append(
                _make_benchmark_case(
                    name=f"high_cardinality_{high_symbol_count}",
                    dataset_kind="synthetic_high_cardinality",
                    target_profile=target_profile,
                    requested_symbol_count=len(case_symbols),
                    rows_in=len(case_events),
                    partitions=len(case_symbols),
                    bursts=high_bursts,
                    batch_size=case_batch_size,
                    elapsed_seconds=case_elapsed,
                    max_write_latency_seconds=case_writer.max_write_latency_seconds,
                    compaction_elapsed_seconds=0.0,
                    shadow_elapsed_seconds=0.0,
                    segments_pending_total=case_health.segments_pending_total,
                    segments_per_partition_max=case_health.segments_per_partition_max,
                    normalized_partition_row_count=case_health.normalized_partition_row_count,
                    min_rows_per_second=min_rows_per_second_value,
                    max_write_latency_slo=max_write_latency_slo_value,
                    max_compaction_elapsed_slo=max_compaction_elapsed_slo_value,
                    max_shadow_elapsed_slo=max_shadow_elapsed_slo_value,
                )
            )

        evidence = StorageBenchmarkEvidence(
            generated_at=datetime.now(timezone.utc).isoformat(),
            target_profile=target_profile,
            synthetic_case=synthetic_case,
            replay_case=replay_case,
            concurrent_compaction_case=concurrent_case,
            shadow_scoped_case=shadow_case,
            required_high_cardinality_symbol_counts=required_high_cardinality_symbol_counts,
            high_cardinality_cases=tuple(high_cardinality_cases),
            slo=slo,
            pass_ok=all(
                case.pass_ok
                for case in (
                    synthetic_case,
                    replay_case,
                    concurrent_case,
                    shadow_case,
                    *high_cardinality_cases,
                )
            ),
        )
        write_json_report(output_path, asdict(evidence))
        return evidence
    finally:
        if cleanup and workspace_dir is None:
            shutil.rmtree(base_dir, ignore_errors=True)
            for extra_dir in extra_cleanup_dirs:
                shutil.rmtree(extra_dir, ignore_errors=True)
