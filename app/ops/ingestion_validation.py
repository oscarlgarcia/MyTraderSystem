from __future__ import annotations

import io
import json
import tempfile
import time
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

from app.common.dto import MarketEvent
from app.ingestion.pipeline import collect_events
from app.ingestion.sinks import ParquetEventSink
from app.ingestion.sources import StaticSource
from app.ingestion.storage import ParquetWriter
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
class CanaryEvidence:
    baseline: ValidationRun
    candidate: ValidationRun
    diffs: dict[str, float]
    pass_ok: bool
    comparison_reason: str


def _cfg(base_dir: Path) -> SimpleNamespace:
    return SimpleNamespace(
        env="test",
        data_dir=base_dir.resolve(),
        log_level="INFO",
        ws_base="wss://stream.binance.com:9443",
        rest_base="https://api.binance.com",
        symbols=["BTCUSDT"],
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
) -> CanaryEvidence:
    events = _events(event_count, duplicate_edge=True)

    def execute(version: str) -> ValidationRun:
        with tempfile.TemporaryDirectory() as tmp_dir:
            base_dir = Path(tmp_dir)
            buffer = io.StringIO()
            collect_events(
                mode="live",
                cfg=_cfg(base_dir),
                max_events=event_count + 1,
                duration_s=0,
                logger=get_logger(name=f"ops.canary.{version}", level="INFO", stream=buffer),
                source=StaticSource(events=events),
                snapshot_enabled=False,
                summary_logging=True,
                dedup_enabled=True,
                batch_size=16,
                pipeline_version=version,
                shadow_mode=False,
            )
            return _extract_run(_json_lines(buffer), pipeline_version=version, shadow_mode=False)

    baseline = execute(baseline_version)
    candidate = execute(candidate_version)
    diffs = {
        "events_persisted": float(candidate.events_persisted - baseline.events_persisted),
        "duplicates": float(candidate.duplicates - baseline.duplicates),
        "gaps": float(candidate.gaps - baseline.gaps),
        "processing_latency_seconds": float(candidate.processing_latency_seconds - baseline.processing_latency_seconds),
        "write_latency_seconds": float(candidate.write_latency_seconds - baseline.write_latency_seconds),
    }
    pass_ok = diffs["events_persisted"] == 0.0 and diffs["duplicates"] == 0.0 and diffs["gaps"] == 0.0
    reason = "counts_match" if pass_ok else "semantic_diff_detected"
    evidence = CanaryEvidence(
        baseline=baseline,
        candidate=candidate,
        diffs=diffs,
        pass_ok=pass_ok,
        comparison_reason=reason,
    )
    write_json_report(output_path, asdict(evidence))
    return evidence
