from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

from app.ingestion.storage import (
    PARTITION_DATA_FILENAME,
    list_normalized_partition_paths,
    partition_compaction_failure_path,
    partition_segments_dir,
    read_parquet,
)
from app.observability.alerts import emit_operational_alert


@dataclass(frozen=True, slots=True)
class PartitionStorageHealth:
    partition_path: str
    feed_type: str
    venue: str
    symbol: str
    day: str
    segments_pending: int
    compaction_lag_seconds: float
    compaction_failures_total: int
    normalized_partition_row_count: int
    has_compacted_snapshot: bool


@dataclass(frozen=True, slots=True)
class StorageHealthReport:
    env: str
    partitions: tuple[PartitionStorageHealth, ...]
    segments_pending_total: int
    segments_per_partition_max: int
    compaction_lag_seconds: float
    compaction_failures_total: int
    normalized_partition_row_count: int

    @property
    def has_pending_compaction(self) -> bool:
        return self.segments_pending_total > 0


def collect_storage_health(
    base_dir: Path,
    env: str,
    *,
    now: datetime | None = None,
    partition_paths: Iterable[Path | str] | None = None,
) -> StorageHealthReport:
    observed_at = now or datetime.now(timezone.utc)
    selected_partition_paths = _resolve_partition_paths(base_dir, env, partition_paths=partition_paths)
    partition_reports = tuple(
        partition_storage_health(partition_path, now=observed_at)
        for partition_path in selected_partition_paths
    )
    return StorageHealthReport(
        env=env,
        partitions=partition_reports,
        segments_pending_total=sum(item.segments_pending for item in partition_reports),
        segments_per_partition_max=max((item.segments_pending for item in partition_reports), default=0),
        compaction_lag_seconds=max((item.compaction_lag_seconds for item in partition_reports), default=0.0),
        compaction_failures_total=sum(item.compaction_failures_total for item in partition_reports),
        normalized_partition_row_count=sum(item.normalized_partition_row_count for item in partition_reports),
    )


def partition_storage_health(partition_path: Path, *, now: datetime | None = None) -> PartitionStorageHealth:
    observed_at = now or datetime.now(timezone.utc)
    path = Path(partition_path)
    segments = sorted(partition_segments_dir(path).glob("*.parquet"))
    compacted_snapshot = path / PARTITION_DATA_FILENAME
    oldest_segment_ts = min((_mtime_as_utc(segment) for segment in segments), default=None)
    compaction_lag_seconds = 0.0
    if oldest_segment_ts is not None:
        compaction_lag_seconds = max(0.0, (observed_at - oldest_segment_ts).total_seconds())
    failure_path = partition_compaction_failure_path(path)
    failures_total = _jsonl_line_count(failure_path)
    row_count = read_parquet(path).num_rows
    return PartitionStorageHealth(
        partition_path=str(path),
        feed_type=path.parents[3].name,
        venue=path.parents[1].name.split("=", 1)[1],
        symbol=path.parents[0].name.split("=", 1)[1],
        day=path.name.split("=", 1)[1],
        segments_pending=len(segments),
        compaction_lag_seconds=compaction_lag_seconds,
        compaction_failures_total=failures_total,
        normalized_partition_row_count=row_count,
        has_compacted_snapshot=compacted_snapshot.exists(),
    )


def storage_health_payload(report: StorageHealthReport) -> dict[str, object]:
    return {
        "env": report.env,
        "segments_pending_total": report.segments_pending_total,
        "segments_per_partition_max": report.segments_per_partition_max,
        "compaction_lag_seconds": report.compaction_lag_seconds,
        "compaction_failures_total": report.compaction_failures_total,
        "normalized_partition_row_count": report.normalized_partition_row_count,
        "partitions": [
            {
                "partition_path": item.partition_path,
                "feed_type": item.feed_type,
                "venue": item.venue,
                "symbol": item.symbol,
                "day": item.day,
                "segments_pending": item.segments_pending,
                "compaction_lag_seconds": item.compaction_lag_seconds,
                "compaction_failures_total": item.compaction_failures_total,
                "normalized_partition_row_count": item.normalized_partition_row_count,
                "has_compacted_snapshot": item.has_compacted_snapshot,
            }
            for item in report.partitions
        ],
    }


def emit_storage_health_alerts(
    logger,
    report: StorageHealthReport,
    *,
    backlog_segment_threshold: int = 5,
    critical_compaction_lag_seconds: float = 900.0,
) -> None:
    if report.compaction_failures_total > 0:
        emit_operational_alert(
            logger,
            alert_type="compaction_failure_detected",
            observed=report.compaction_failures_total,
            extra=storage_health_payload(report),
        )
    if (
        report.segments_pending_total > 0
        and (
            report.segments_per_partition_max >= backlog_segment_threshold
            or report.compaction_lag_seconds >= critical_compaction_lag_seconds
        )
    ):
        emit_operational_alert(
            logger,
            alert_type="compaction_backlog_high",
            observed=report.segments_per_partition_max,
            extra=storage_health_payload(report),
        )


def assert_storage_health_for_runtime(
    base_dir: Path,
    env: str,
    *,
    critical_compaction_lag_seconds: float = 900.0,
) -> StorageHealthReport:
    report = collect_storage_health(base_dir, env)
    if report.compaction_failures_total > 0:
        raise ValueError(
            "Unsafe production configuration: storage compaction has recorded failures"
        )
    if report.compaction_lag_seconds >= critical_compaction_lag_seconds:
        raise ValueError(
            "Unsafe production configuration: storage compaction lag exceeds the critical threshold"
        )
    return report


def _mtime_as_utc(path: Path) -> datetime:
    return datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc)


def _jsonl_line_count(path: Path) -> int:
    if not path.exists():
        return 0
    with path.open("r", encoding="utf-8") as handle:
        return sum(1 for line in handle if line.strip())


def _resolve_partition_paths(
    base_dir: Path,
    env: str,
    *,
    partition_paths: Iterable[Path | str] | None = None,
) -> list[Path]:
    if partition_paths is None:
        return list_normalized_partition_paths(Path(base_dir), env)
    return [Path(path).resolve() for path in partition_paths]
