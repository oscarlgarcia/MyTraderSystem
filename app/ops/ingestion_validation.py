from __future__ import annotations

import io
import json
import tempfile
import time
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
from typing import Callable, Sequence
import hashlib

import httpx

from app.common.dto import MarketEvent
from app.ingestion.backfill import _interval_to_ms, fetch_klines, normalize_kline_row
from app.ingestion.pipeline import collect_events
from app.ingestion.sinks import ParquetEventSink
from app.ingestion.sources import StaticSource
from app.ingestion.storage import ParquetWriter, read_parquet
from app.marketdata.instruments import ensure_default_instruments
from app.marketdata.models import BarEvent
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


CanaryFetchRows = Callable[..., list[list[object]]]


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
