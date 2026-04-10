from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import date, datetime, timezone
from pathlib import Path

from app.ingestion.storage import list_normalized_partition_paths
from app.marketdata.dataset_contracts import parse_normalized_partition_path


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


def storage_lifecycle_report_path(base_dir: Path, env: str) -> Path:
    return Path(base_dir) / env / "catalog" / "storage-lifecycle.json"


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
