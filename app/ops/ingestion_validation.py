from __future__ import annotations

import io
import json
import tempfile
import time
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from threading import Thread
from types import SimpleNamespace
from typing import Callable, Sequence
import hashlib

import httpx

from app.common.dto import MarketEvent
from app.ingestion.backfill import _interval_to_ms, fetch_klines, normalize_kline_row
from app.ingestion.compaction import CompactionJobPolicy, run_compaction_job
from app.ingestion.checkpoints import CheckpointStore
from app.ingestion.pipeline import collect_events
from app.ingestion.sinks import ParquetEventSink
from app.ingestion.sources import BinanceSource, Source, heartbeat_policy_for_streams, StaticSource, _ws_stream
from app.ingestion.storage import ParquetWriter, read_parquet
from app.ingestion.storage_health import collect_storage_health
from app.marketdata.instruments import ensure_default_instruments
from app.marketdata.models import BarEvent
from app.marketdata.raw_sink import JsonlRawSink, RawRecord
from app.marketdata.replay import ReplaySource
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
    processing_latency_seconds: float
    write_latency_seconds: float
    streams_degraded: list[str]
    result: str


@dataclass(frozen=True, slots=True)
class SoakEvidence:
    iterations: int
    events_per_iteration: int
    elapsed_seconds: float
    max_processing_latency_seconds: float
    max_write_latency_seconds: float
    total_events_persisted: int
    max_gaps: int
    max_gap_irreparable: int
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
    rows_in: int
    partitions: int
    bursts: int
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
    synthetic_case: StorageBenchmarkCase
    replay_case: StorageBenchmarkCase
    concurrent_compaction_case: StorageBenchmarkCase
    shadow_scoped_case: StorageBenchmarkCase
    slo: dict[str, float]
    pass_ok: bool


@dataclass(frozen=True, slots=True)
class WSCanaryEvidence:
    mode: str
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
    pass_ok: bool
    comparison_reason: str


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
        processing_latency_seconds=0.0,
        write_latency_seconds=float(getattr(sink, "write_latency_seconds", 0.0)),
        streams_degraded=[],
        result="ok",
    )


def run_soak_validation(
    output_path: Path,
    *,
    iterations: int = 5,
    events_per_iteration: int = 500,
    pipeline_version: str = "v2",
) -> SoakEvidence:
    runs: list[ValidationRun] = []
    started = time.perf_counter()
    for index in range(iterations):
        with tempfile.TemporaryDirectory() as tmp_dir:
            base_dir = Path(tmp_dir)
            buffer = io.StringIO()
            collect_events(
                mode="live",
                cfg=_cfg(base_dir),
                max_events=events_per_iteration,
                duration_s=0,
                logger=get_logger(name=f"ops.soak.{index}", level="INFO", stream=buffer),
                source=StaticSource(events=_events(events_per_iteration)),
                sink=ParquetEventSink(
                    ParquetWriter(base_dir=base_dir, env="test", flush_size=256, dedup=True, schema_version=pipeline_version)
                ),
                snapshot_enabled=False,
                summary_logging=True,
                dedup_enabled=True,
                batch_size=32,
                pipeline_version=pipeline_version,
            )
            runs.append(_extract_run(_json_lines(buffer), pipeline_version=pipeline_version, shadow_mode=False))
    elapsed = max(0.0, time.perf_counter() - started)
    evidence = SoakEvidence(
        iterations=iterations,
        events_per_iteration=events_per_iteration,
        elapsed_seconds=elapsed,
        max_processing_latency_seconds=max(run.processing_latency_seconds for run in runs),
        max_write_latency_seconds=max(run.write_latency_seconds for run in runs),
        total_events_persisted=sum(run.events_persisted for run in runs),
        max_gaps=max(run.gaps for run in runs),
        max_gap_irreparable=max(run.gap_irreparable for run in runs),
        pass_ok=all(run.result == "ok" for run in runs) and all(run.gaps == 0 for run in runs),
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
        session_events = 0
        for raw in _ws_stream(url, end_time=end_time, heartbeat=heartbeat):
            yield raw
            session_events += 1
            if reconnect_state["remaining"] > 0 and session_events >= max(1, reconnect_after_events):
                reconnect_state["remaining"] -= 1
                raise TimeoutError("ws canary induced reconnect")

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
    source_builder: WSCanarySourceBuilder | None = None,
) -> WSCanaryEvidence:
    if stream_type != "kline":
        raise ValueError("ws-live canary currently supports only kline feeds because live pipeline support is bars-only")

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
        continuity = {
            "events_persisted": run.events_persisted,
            "duplicates": run.duplicates,
            "gaps": run.gaps,
            "gap_irreparable": run.gap_irreparable,
            "reconnects": run.reconnects,
            "result": run.result,
            "streams_degraded": list(run.streams_degraded),
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
            and run.gap_irreparable == 0
        )
        reasons: list[str] = []
        if run.result != "ok":
            reasons.append(f"result={run.result}")
        if run.events_persisted <= 0:
            reasons.append("no_events_persisted")
        if run.reconnects < max(0, induced_reconnects):
            reasons.append("reconnect_target_not_met")
        if run.gap_irreparable > 0:
            reasons.append("gap_irreparable_detected")
        evidence = WSCanaryEvidence(
            mode="ws-live",
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
            pass_ok=pass_ok,
            comparison_reason="continuity_ok" if pass_ok else ",".join(reasons),
        )
    write_json_report(output_path, asdict(evidence))
    return evidence


def _benchmark_trade_events(
    *,
    symbols: Sequence[str],
    bursts: int,
    events_per_symbol_per_burst: int,
) -> list[MarketEvent]:
    base = datetime(2024, 1, 1, tzinfo=timezone.utc)
    burst_window_seconds = max(2, events_per_symbol_per_burst + 1)
    events: list[MarketEvent] = []
    for burst in range(bursts):
        for symbol_index, symbol in enumerate(symbols):
            for offset in range(events_per_symbol_per_burst):
                event_ts = base + timedelta(
                    seconds=burst * burst_window_seconds + offset,
                )
                trade_id = symbol_index * 100_000 + burst * events_per_symbol_per_burst + offset + 1
                events.append(
                    MarketEvent(
                        symbol=symbol,
                        event_ts=event_ts,
                        price=100.0 + symbol_index + burst,
                        size=1.0 + (offset / 10.0),
                        source="trade",
                        metadata={
                            "trade_id": str(trade_id),
                            "source_id": str(trade_id),
                            "venue": "BINANCE",
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
    rows_in: int,
    partitions: int,
    bursts: int,
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
        rows_in=rows_in,
        partitions=partitions,
        bursts=bursts,
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


def run_storage_benchmark(
    output_path: Path,
    *,
    symbol_count: int = 12,
    bursts: int = 4,
    events_per_symbol_per_burst: int = 12,
    min_rows_per_second: float = 100.0,
    max_write_latency_slo: float = 2.0,
    max_compaction_elapsed_slo: float = 5.0,
    max_shadow_elapsed_slo: float = 5.0,
) -> StorageBenchmarkEvidence:
    benchmark_symbols = [
        "BTCUSDT",
        "ETHUSDT",
        "SOLUSDT",
    ]
    symbols = benchmark_symbols[: max(1, min(symbol_count, len(benchmark_symbols)))]
    rows_in = len(symbols) * max(1, bursts) * max(1, events_per_symbol_per_burst)
    partitions = len(symbols)
    slo = {
        "min_rows_per_second": float(min_rows_per_second),
        "max_write_latency_seconds": float(max_write_latency_slo),
        "max_compaction_elapsed_seconds": float(max_compaction_elapsed_slo),
        "max_shadow_elapsed_seconds": float(max_shadow_elapsed_slo),
    }

    with tempfile.TemporaryDirectory() as tmp_dir:
        base_dir = Path(tmp_dir)

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
            rows_in=rows_in,
            partitions=partitions,
            bursts=bursts,
            elapsed_seconds=synthetic_elapsed,
            max_write_latency_seconds=writer.max_write_latency_seconds,
            compaction_elapsed_seconds=0.0,
            shadow_elapsed_seconds=0.0,
            segments_pending_total=synthetic_health.segments_pending_total,
            segments_per_partition_max=synthetic_health.segments_per_partition_max,
            normalized_partition_row_count=synthetic_health.normalized_partition_row_count,
            min_rows_per_second=min_rows_per_second,
            max_write_latency_slo=max_write_latency_slo,
            max_compaction_elapsed_slo=max_compaction_elapsed_slo,
            max_shadow_elapsed_slo=max_shadow_elapsed_slo,
        )

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
            rows_in=replay_rows,
            partitions=partitions,
            bursts=bursts,
            elapsed_seconds=replay_elapsed,
            max_write_latency_seconds=replay_writer.max_write_latency_seconds,
            compaction_elapsed_seconds=0.0,
            shadow_elapsed_seconds=0.0,
            segments_pending_total=replay_health.segments_pending_total,
            segments_per_partition_max=replay_health.segments_per_partition_max,
            normalized_partition_row_count=replay_health.normalized_partition_row_count,
            min_rows_per_second=min_rows_per_second,
            max_write_latency_slo=max_write_latency_slo,
            max_compaction_elapsed_slo=max_compaction_elapsed_slo,
            max_shadow_elapsed_slo=max_shadow_elapsed_slo,
        )

        concurrent_base_dir = base_dir / "concurrent-benchmark"
        first_half_symbols = symbols[: max(1, len(symbols) // 2)]
        second_half_symbols = symbols[max(1, len(symbols) // 2) :] or symbols[:1]
        first_writer = ParquetWriter(base_dir=concurrent_base_dir, env="test", flush_size=max(8, events_per_symbol_per_burst), dedup=True)
        first_sink = ParquetEventSink(first_writer)
        first_events = _benchmark_trade_events(
            symbols=first_half_symbols,
            bursts=bursts,
            events_per_symbol_per_burst=events_per_symbol_per_burst,
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
            batch_size=max(8, events_per_symbol_per_burst),
            pipeline_version="v2",
        )
        first_sink.close()

        compaction_elapsed_holder = {"elapsed": 0.0}

        def _run_compaction() -> None:
            started_local = time.perf_counter()
            run_compaction_job(
                concurrent_base_dir,
                "test",
                policy=CompactionJobPolicy(
                    batch_limit=10,
                    retry_attempts=1,
                    min_segments_pending=2,
                    min_compaction_lag_seconds=0.0,
                    retain_compacted_segments=1,
                ),
            )
            compaction_elapsed_holder["elapsed"] = max(0.0, time.perf_counter() - started_local)

        started_concurrent = time.perf_counter()
        compaction_thread = Thread(target=_run_compaction)
        compaction_thread.start()
        second_writer = ParquetWriter(base_dir=concurrent_base_dir, env="test", flush_size=max(8, events_per_symbol_per_burst), dedup=True)
        second_sink = ParquetEventSink(second_writer)
        second_events = _benchmark_trade_events(
            symbols=second_half_symbols,
            bursts=bursts,
            events_per_symbol_per_burst=events_per_symbol_per_burst,
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
            batch_size=max(8, events_per_symbol_per_burst),
            pipeline_version="v2",
        )
        second_sink.close()
        compaction_thread.join()
        concurrent_elapsed = max(0.0, time.perf_counter() - started_concurrent)
        concurrent_health = collect_storage_health(concurrent_base_dir, "test")
        concurrent_case = _make_benchmark_case(
            name="concurrent_compaction",
            dataset_kind="synthetic",
            rows_in=len(first_events) + len(second_events),
            partitions=len(set(first_half_symbols).union(second_half_symbols)),
            bursts=bursts,
            elapsed_seconds=concurrent_elapsed,
            max_write_latency_seconds=max(first_writer.max_write_latency_seconds, second_writer.max_write_latency_seconds),
            compaction_elapsed_seconds=compaction_elapsed_holder["elapsed"],
            shadow_elapsed_seconds=0.0,
            segments_pending_total=concurrent_health.segments_pending_total,
            segments_per_partition_max=concurrent_health.segments_per_partition_max,
            normalized_partition_row_count=concurrent_health.normalized_partition_row_count,
            min_rows_per_second=min_rows_per_second,
            max_write_latency_slo=max_write_latency_slo,
            max_compaction_elapsed_slo=max_compaction_elapsed_slo,
            max_shadow_elapsed_slo=max_shadow_elapsed_slo,
        )

        shadow_base_dir = base_dir / "shadow-benchmark"
        shadow_events = _benchmark_trade_events(
            symbols=symbols[: max(2, min(4, len(symbols)))],
            bursts=max(1, min(2, bursts)),
            events_per_symbol_per_burst=max(4, min(8, events_per_symbol_per_burst)),
        )
        shadow_started = time.perf_counter()
        shadow_buffer = io.StringIO()
        collect_events(
            mode="live",
            cfg=_cfg(shadow_base_dir, symbols=list({event.symbol for event in shadow_events})),
            max_events=len(shadow_events),
            duration_s=0,
            logger=get_logger(name="ops.storage.shadow", level="INFO", stream=shadow_buffer),
            source=StaticSource(events=shadow_events),
            sink=None,
            snapshot_enabled=False,
            summary_logging=True,
            dedup_enabled=True,
            batch_size=8,
            pipeline_version="v2",
            shadow_mode=True,
        )
        shadow_elapsed = max(0.0, time.perf_counter() - shadow_started)
        shadow_health = collect_storage_health(shadow_base_dir, "test")
        shadow_case = _make_benchmark_case(
            name="shadow_scoped_runtime",
            dataset_kind="synthetic",
            rows_in=len(shadow_events),
            partitions=len({event.symbol for event in shadow_events}),
            bursts=max(1, min(2, bursts)),
            elapsed_seconds=shadow_elapsed,
            max_write_latency_seconds=0.0,
            compaction_elapsed_seconds=0.0,
            shadow_elapsed_seconds=shadow_elapsed,
            segments_pending_total=shadow_health.segments_pending_total,
            segments_per_partition_max=shadow_health.segments_per_partition_max,
            normalized_partition_row_count=shadow_health.normalized_partition_row_count,
            min_rows_per_second=min_rows_per_second,
            max_write_latency_slo=max_write_latency_slo,
            max_compaction_elapsed_slo=max_compaction_elapsed_slo,
            max_shadow_elapsed_slo=max_shadow_elapsed_slo,
        )

    evidence = StorageBenchmarkEvidence(
        generated_at=datetime.now(timezone.utc).isoformat(),
        synthetic_case=synthetic_case,
        replay_case=replay_case,
        concurrent_compaction_case=concurrent_case,
        shadow_scoped_case=shadow_case,
        slo=slo,
        pass_ok=all(
            case.pass_ok
            for case in (
                synthetic_case,
                replay_case,
                concurrent_case,
                shadow_case,
            )
        ),
    )
    write_json_report(output_path, asdict(evidence))
    return evidence
