from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import date, datetime, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from app.ingestion.storage import list_normalized_partition_paths
from app.marketdata.dataset_contracts import parse_normalized_partition_path
from app.marketdata.query import HistoricalQueryRequest, query_rows


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass(frozen=True, slots=True)
class StorageLifecycleEntry:
    dataset_id: str
    partition_path: str
    age_days: int
    tier: str


@dataclass(frozen=True, slots=True)
class StorageLifecycleReport:
    generated_at: str
    entries: tuple[StorageLifecycleEntry, ...]


@dataclass(frozen=True, slots=True)
class StorageLifecycleAction:
    dataset_id: str
    tier: str
    partition_path: str
    downsampled_path: str | None
    sampled_rows: int
    action: str


@dataclass(frozen=True, slots=True)
class StorageLifecycleExecutionReport:
    generated_at: str
    actions: tuple[StorageLifecycleAction, ...]


def storage_lifecycle_report_path(base_dir: Path, env: str) -> Path:
    return Path(base_dir) / env / "catalog" / "storage-lifecycle.json"


def storage_lifecycle_execution_path(base_dir: Path, env: str) -> Path:
    return Path(base_dir) / env / "catalog" / "storage-lifecycle-actions.json"


def build_storage_lifecycle_report(base_dir: Path, env: str, *, today: date | None = None, hot_days: int = 7, warm_days: int = 30) -> StorageLifecycleReport:
    today = today or datetime.now(timezone.utc).date()
    entries: list[StorageLifecycleEntry] = []
    for path in list_normalized_partition_paths(Path(base_dir), env):
        ref = parse_normalized_partition_path(path)
        partition_day = date.fromisoformat(ref.partition_date)
        age_days = max(0, (today - partition_day).days)
        tier = "hot" if age_days <= hot_days else "warm" if age_days <= warm_days else "cold"
        entries.append(StorageLifecycleEntry(dataset_id=ref.dataset_id, partition_path=str(path), age_days=age_days, tier=tier))
    return StorageLifecycleReport(generated_at=_utc_now(), entries=tuple(sorted(entries, key=lambda item: item.dataset_id)))


def write_storage_lifecycle_report(path: Path, report: StorageLifecycleReport) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps({"generated_at": report.generated_at, "entries": [asdict(item) for item in report.entries]}, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    return path


def _downsampled_partition_path(base_dir: Path, env: str, *, ref) -> Path:
    return (
        Path(base_dir)
        / env
        / "derived"
        / "downsampled"
        / f"venue={ref.venue}"
        / f"stream_type={ref.stream_type}"
        / f"symbol={ref.symbol}"
        / f"date={ref.partition_date}"
        / "data.parquet"
    )


def apply_storage_lifecycle(base_dir: Path, env: str, *, sample_every: int = 10) -> StorageLifecycleExecutionReport:
    report = build_storage_lifecycle_report(base_dir, env)
    actions: list[StorageLifecycleAction] = []
    for entry in report.entries:
        ref = parse_normalized_partition_path(Path(entry.partition_path))
        rows = query_rows(
            HistoricalQueryRequest(
                base_dir=Path(base_dir),
                env=env,
                stream_type=ref.stream_type,
                symbol=ref.symbol,
                venue=ref.venue,
            )
        )
        partition_rows = [row for row in rows if str(row.get("partition_path")) == entry.partition_path]
        if entry.tier == "hot":
            actions.append(
                StorageLifecycleAction(
                    dataset_id=entry.dataset_id,
                    tier=entry.tier,
                    partition_path=entry.partition_path,
                    downsampled_path=None,
                    sampled_rows=0,
                    action="retain_hot",
                )
            )
            continue
        sampled = [row for index, row in enumerate(partition_rows) if index % max(1, sample_every) == 0]
        if not sampled and not partition_rows:
            actions.append(
                StorageLifecycleAction(
                    dataset_id=entry.dataset_id,
                    tier=entry.tier,
                    partition_path=entry.partition_path,
                    downsampled_path=None,
                    sampled_rows=0,
                    action="no_rows_available",
                )
            )
            continue
        out_path = _downsampled_partition_path(base_dir, env, ref=ref)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        pq.write_table(
            pa.Table.from_pylist(sampled if sampled else partition_rows[:1]),
            out_path,
        )
        actions.append(
            StorageLifecycleAction(
                dataset_id=entry.dataset_id,
                tier=entry.tier,
                partition_path=entry.partition_path,
                downsampled_path=str(out_path),
                sampled_rows=len(sampled if sampled else partition_rows[:1]),
                action="downsampled_copy",
            )
        )
    return StorageLifecycleExecutionReport(generated_at=_utc_now(), actions=tuple(actions))


def write_storage_lifecycle_execution_report(path: Path, report: StorageLifecycleExecutionReport) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            {"generated_at": report.generated_at, "actions": [asdict(item) for item in report.actions]},
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    return path
