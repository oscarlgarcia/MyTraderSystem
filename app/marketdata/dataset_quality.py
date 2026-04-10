from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Literal

from app.ingestion.storage import read_parquet
from app.marketdata.dataset_contracts import DatasetPartitionRef, parse_normalized_partition_path


QualityStatus = Literal["healthy", "degraded", "failed"]
IncidentSeverity = Literal["info", "warn", "error"]


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass(frozen=True, slots=True)
class DatasetIncident:
    dataset_id: str
    severity: IncidentSeverity
    incident_type: str
    reason: str
    event_count: int
    recorded_at: str


@dataclass(frozen=True, slots=True)
class DatasetQualityReport:
    dataset_id: str
    env: str
    venue: str
    symbol: str
    stream_type: str
    partition_date: str
    partition_path: str
    row_count: int
    duplicate_keys: int
    missing_provider_ts: int
    missing_raw_lineage: int
    out_of_order_rows: int
    incident_count: int
    score: float
    status: QualityStatus
    incidents: tuple[DatasetIncident, ...]
    generated_at: str


@dataclass(frozen=True, slots=True)
class DatasetQualityRegistry:
    generated_at: str
    reports: tuple[DatasetQualityReport, ...]


def dataset_quality_registry_path(base_dir: Path, env: str) -> Path:
    return Path(base_dir) / env / "catalog" / "dataset-quality.json"


def dataset_incident_log_path(base_dir: Path, env: str) -> Path:
    return Path(base_dir) / env / "catalog" / "dataset-incidents.jsonl"


def _record_incident(
    incidents: list[DatasetIncident],
    ref: DatasetPartitionRef,
    severity: IncidentSeverity,
    incident_type: str,
    reason: str,
    count: int,
) -> None:
    if count <= 0:
        return
    incidents.append(
        DatasetIncident(
            dataset_id=ref.dataset_id,
            severity=severity,
            incident_type=incident_type,
            reason=reason,
            event_count=count,
            recorded_at=_utc_now(),
        )
    )


def build_dataset_quality_report(normalized_path: Path) -> DatasetQualityReport:
    ref = parse_normalized_partition_path(Path(normalized_path))
    rows = read_parquet(Path(normalized_path)).to_pylist()
    seen_keys: set[tuple[object, ...]] = set()
    duplicates = 0
    missing_provider_ts = 0
    missing_raw_lineage = 0
    out_of_order_rows = 0
    previous_exchange_ts = None
    for row in rows:
        key = (
            row.get("exchange_ts"),
            row.get("source_id"),
            row.get("trade_id"),
            row.get("symbol"),
            row.get("feed_type"),
        )
        if key in seen_keys:
            duplicates += 1
        seen_keys.add(key)
        if row.get("provider_ts") is None:
            missing_provider_ts += 1
        if row.get("raw_run_id") in (None, "") or row.get("raw_ingestion_seq") in (None, ""):
            missing_raw_lineage += 1
        current_ts = row.get("exchange_ts")
        if previous_exchange_ts is not None and current_ts is not None and current_ts < previous_exchange_ts:
            out_of_order_rows += 1
        if current_ts is not None:
            previous_exchange_ts = current_ts
    incidents: list[DatasetIncident] = []
    _record_incident(incidents, ref, "warn", "duplicate_rows", "duplicate natural keys detected", duplicates)
    _record_incident(
        incidents,
        ref,
        "warn",
        "missing_provider_ts",
        "rows missing provider_ts reduce temporal confidence",
        missing_provider_ts,
    )
    _record_incident(
        incidents,
        ref,
        "error",
        "missing_raw_lineage",
        "rows missing raw lineage cannot be fully audited",
        missing_raw_lineage,
    )
    _record_incident(
        incidents,
        ref,
        "error",
        "out_of_order_rows",
        "rows are not monotonically ordered by exchange_ts",
        out_of_order_rows,
    )
    score = 100.0
    score -= duplicates * 5.0
    score -= missing_provider_ts * 1.0
    score -= missing_raw_lineage * 3.0
    score -= out_of_order_rows * 10.0
    score = max(0.0, score)
    status: QualityStatus = "healthy"
    if any(item.severity == "error" for item in incidents):
        status = "failed" if score < 75.0 else "degraded"
    elif incidents:
        status = "degraded"
    return DatasetQualityReport(
        dataset_id=ref.dataset_id,
        env=ref.env,
        venue=ref.venue,
        symbol=ref.symbol,
        stream_type=ref.stream_type,
        partition_date=ref.partition_date,
        partition_path=ref.partition_path,
        row_count=len(rows),
        duplicate_keys=duplicates,
        missing_provider_ts=missing_provider_ts,
        missing_raw_lineage=missing_raw_lineage,
        out_of_order_rows=out_of_order_rows,
        incident_count=len(incidents),
        score=score,
        status=status,
        incidents=tuple(incidents),
        generated_at=_utc_now(),
    )


def build_dataset_quality_registry(normalized_paths: list[Path]) -> DatasetQualityRegistry:
    reports = tuple(sorted((build_dataset_quality_report(path) for path in normalized_paths), key=lambda item: item.dataset_id))
    return DatasetQualityRegistry(generated_at=_utc_now(), reports=reports)


def write_dataset_quality_registry(path: Path, registry: DatasetQualityRegistry) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "generated_at": registry.generated_at,
        "reports": [asdict(report) for report in registry.reports],
    }
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return path


def append_dataset_incidents(path: Path, reports: tuple[DatasetQualityReport, ...]) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        for report in reports:
            for incident in report.incidents:
                handle.write(json.dumps(asdict(incident), ensure_ascii=False))
                handle.write("\n")
    return path


def read_dataset_quality_registry(path: Path) -> DatasetQualityRegistry:
    resolved = Path(path)
    if not resolved.exists():
        return DatasetQualityRegistry(generated_at=_utc_now(), reports=())
    payload = json.loads(resolved.read_text(encoding="utf-8"))
    reports: list[DatasetQualityReport] = []
    for raw in payload.get("reports", ()):
        reports.append(
            DatasetQualityReport(
                dataset_id=raw["dataset_id"],
                env=raw["env"],
                venue=raw["venue"],
                symbol=raw["symbol"],
                stream_type=raw["stream_type"],
                partition_date=raw["partition_date"],
                partition_path=raw["partition_path"],
                row_count=int(raw["row_count"]),
                duplicate_keys=int(raw["duplicate_keys"]),
                missing_provider_ts=int(raw["missing_provider_ts"]),
                missing_raw_lineage=int(raw["missing_raw_lineage"]),
                out_of_order_rows=int(raw["out_of_order_rows"]),
                incident_count=int(raw["incident_count"]),
                score=float(raw["score"]),
                status=raw["status"],
                incidents=tuple(DatasetIncident(**incident) for incident in raw.get("incidents", ())),
                generated_at=raw["generated_at"],
            )
        )
    return DatasetQualityRegistry(generated_at=str(payload.get("generated_at") or _utc_now()), reports=tuple(reports))
