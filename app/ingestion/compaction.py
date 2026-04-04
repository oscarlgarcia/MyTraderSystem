from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable

import pyarrow.parquet as pq

from app.ingestion.storage import (
    PARTITION_DATA_FILENAME,
    normalized_partition_data_path,
    normalized_partition_path,
    partition_compaction_failure_path,
    partition_segments_dir,
    record_compaction_failure,
    read_parquet,
)
from app.ingestion.storage_health import PartitionStorageHealth, collect_storage_health

RETAINED_SEGMENTS_DIRNAME = "retained-segments"


@dataclass(frozen=True, slots=True)
class CompactionJobPolicy:
    batch_limit: int = 25
    retry_attempts: int = 2
    min_segments_pending: int = 2
    min_compaction_lag_seconds: float = 300.0
    retain_compacted_segments: int = 0
    remove_segments: bool = True
    dry_run: bool = False


@dataclass(frozen=True, slots=True)
class CompactionCandidate:
    partition_path: str
    feed_type: str
    venue: str
    symbol: str
    day: str
    segments_pending: int
    compaction_lag_seconds: float
    compaction_failures_total: int
    normalized_partition_row_count: int

    @classmethod
    def from_health(cls, health: PartitionStorageHealth) -> "CompactionCandidate":
        return cls(
            partition_path=health.partition_path,
            feed_type=health.feed_type,
            venue=health.venue,
            symbol=health.symbol,
            day=health.day,
            segments_pending=health.segments_pending,
            compaction_lag_seconds=health.compaction_lag_seconds,
            compaction_failures_total=health.compaction_failures_total,
            normalized_partition_row_count=health.normalized_partition_row_count,
        )


@dataclass(frozen=True, slots=True)
class CompactionAttemptResult:
    partition_path: str
    symbol: str
    day: str
    status: str
    attempt_count: int
    retained_segments: int
    output_path: str | None = None
    error: str | None = None


@dataclass(frozen=True, slots=True)
class CompactionJobReport:
    env: str
    policy: CompactionJobPolicy
    selected: tuple[CompactionCandidate, ...]
    results: tuple[CompactionAttemptResult, ...]

    @property
    def compacted_partitions(self) -> int:
        return sum(1 for item in self.results if item.status == "compacted")

    @property
    def failed_partitions(self) -> int:
        return sum(1 for item in self.results if item.status == "failed")

    @property
    def planned_partitions(self) -> int:
        return len(self.selected)


def compact_partition(
    base_dir: Path,
    env: str,
    *,
    source: str,
    symbol: str,
    day: str,
    venue: str = "BINANCE",
    remove_segments: bool = True,
    retain_compacted_segments: int = 0,
) -> Path:
    partition_path = normalized_partition_path(
        base_dir,
        env,
        source=source,
        symbol=symbol,
        day=day,
        venue=venue,
    )
    if not partition_path.exists():
        raise FileNotFoundError(partition_path)

    out_path = normalized_partition_data_path(
        base_dir,
        env,
        source=source,
        symbol=symbol,
        day=day,
        venue=venue,
    )
    try:
        table = read_parquet(partition_path)
        _write_table_atomic(table, out_path)
    except Exception as exc:
        record_compaction_failure(partition_path, exc)
        raise

    if remove_segments:
        _finalize_compacted_segments(
            partition_path,
            retain_compacted_segments=max(0, int(retain_compacted_segments)),
        )
    _clear_compaction_failures(partition_path)
    return out_path


def compact_environment(
    base_dir: Path,
    env: str,
    *,
    remove_segments: bool = True,
    retain_compacted_segments: int = 0,
    partition_paths: Iterable[Path | str] | None = None,
) -> list[Path]:
    base = Path(base_dir)
    outputs: list[Path] = []
    for candidate in select_compaction_candidates(base, env, partition_paths=partition_paths):
        outputs.append(
            compact_partition(
                base,
                env,
                source=_source_for_feed_type(candidate.feed_type),
                symbol=candidate.symbol,
                day=candidate.day,
                venue=candidate.venue,
                remove_segments=remove_segments,
                retain_compacted_segments=retain_compacted_segments,
            )
        )
    return outputs


def select_compaction_candidates(
    base_dir: Path,
    env: str,
    *,
    batch_limit: int | None = None,
    min_segments_pending: int = 2,
    min_compaction_lag_seconds: float = 300.0,
    partition_paths: Iterable[Path | str] | None = None,
) -> list[CompactionCandidate]:
    allowed_partition_paths = _normalize_partition_paths(partition_paths)
    report = collect_storage_health(base_dir, env, partition_paths=allowed_partition_paths)
    candidates = [
        CompactionCandidate.from_health(item)
        for item in report.partitions
        if (
            allowed_partition_paths is None
            or str(Path(item.partition_path).resolve()) in allowed_partition_paths
        )
        and (
            item.segments_pending >= max(1, int(min_segments_pending))
            or item.compaction_lag_seconds >= max(0.0, float(min_compaction_lag_seconds))
        )
    ]
    candidates.sort(
        key=lambda item: (
            -item.compaction_failures_total,
            -item.compaction_lag_seconds,
            -item.segments_pending,
            item.partition_path,
        )
    )
    if batch_limit is not None:
        return candidates[: max(0, int(batch_limit))]
    return candidates


def run_compaction_job(
    base_dir: Path,
    env: str,
    *,
    policy: CompactionJobPolicy | None = None,
    partition_paths: Iterable[Path | str] | None = None,
) -> CompactionJobReport:
    effective_policy = policy or CompactionJobPolicy()
    candidates = tuple(
        select_compaction_candidates(
            base_dir,
            env,
            batch_limit=effective_policy.batch_limit,
            min_segments_pending=effective_policy.min_segments_pending,
            min_compaction_lag_seconds=effective_policy.min_compaction_lag_seconds,
            partition_paths=partition_paths,
        )
    )
    results: list[CompactionAttemptResult] = []
    for candidate in candidates:
        if effective_policy.dry_run:
            results.append(
                CompactionAttemptResult(
                    partition_path=candidate.partition_path,
                    symbol=candidate.symbol,
                    day=candidate.day,
                    status="planned",
                    attempt_count=0,
                    retained_segments=effective_policy.retain_compacted_segments,
                )
            )
            continue
        attempts = 0
        last_error: Exception | None = None
        while attempts <= max(0, int(effective_policy.retry_attempts)):
            attempts += 1
            try:
                output_path = compact_partition(
                    Path(base_dir),
                    env,
                    source=_source_for_feed_type(candidate.feed_type),
                    symbol=candidate.symbol,
                    day=candidate.day,
                    venue=candidate.venue,
                    remove_segments=effective_policy.remove_segments,
                    retain_compacted_segments=effective_policy.retain_compacted_segments,
                )
                results.append(
                    CompactionAttemptResult(
                        partition_path=candidate.partition_path,
                        symbol=candidate.symbol,
                        day=candidate.day,
                        status="compacted",
                        attempt_count=attempts,
                        retained_segments=effective_policy.retain_compacted_segments,
                        output_path=str(output_path),
                    )
                )
                last_error = None
                break
            except Exception as exc:  # pragma: no cover - branch exercised by tests via failure result
                last_error = exc
        if last_error is not None:
            results.append(
                CompactionAttemptResult(
                    partition_path=candidate.partition_path,
                    symbol=candidate.symbol,
                    day=candidate.day,
                    status="failed",
                    attempt_count=max(1, attempts),
                    retained_segments=effective_policy.retain_compacted_segments,
                    error=str(last_error),
                )
            )
    return CompactionJobReport(
        env=env,
        policy=effective_policy,
        selected=candidates,
        results=tuple(results),
    )


def _finalize_compacted_segments(partition_path: Path, *, retain_compacted_segments: int) -> None:
    segments_dir = partition_segments_dir(partition_path)
    segments = sorted(segments_dir.glob("*.parquet"))
    if not segments:
        return
    if retain_compacted_segments > 0:
        retained_dir = _retained_segments_dir(partition_path)
        retained_dir.mkdir(parents=True, exist_ok=True)
        run_dir = retained_dir / datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
        run_dir.mkdir(parents=True, exist_ok=True)
        for segment in segments:
            segment.replace(run_dir / segment.name)
        _trim_retained_segments(retained_dir, retain_compacted_segments)
    else:
        for segment in segments:
            segment.unlink()
    try:
        segments_dir.rmdir()
    except OSError:
        pass


def _retained_segments_dir(partition_path: Path) -> Path:
    return partition_path / RETAINED_SEGMENTS_DIRNAME


def _trim_retained_segments(retained_dir: Path, retain_compacted_segments: int) -> None:
    run_dirs = sorted(
        (path for path in retained_dir.iterdir() if path.is_dir()),
        key=lambda path: path.name,
        reverse=True,
    )
    for old_run in run_dirs[retain_compacted_segments:]:
        for segment in old_run.glob("*.parquet"):
            segment.unlink()
        try:
            old_run.rmdir()
        except OSError:
            pass


def _clear_compaction_failures(partition_path: Path) -> None:
    failure_path = partition_compaction_failure_path(partition_path)
    if failure_path.exists():
        failure_path.unlink()


def _normalize_partition_paths(partition_paths: Iterable[Path | str] | None) -> set[str] | None:
    if partition_paths is None:
        return None
    normalized = {str(Path(path).resolve()) for path in partition_paths}
    return normalized or set()


def _source_for_feed_type(feed_type: str) -> str:
    return "trade" if feed_type == "trades" else "kline" if feed_type == "bars" else feed_type


def _write_table_atomic(table, out_path: Path) -> None:
    out_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = out_path.with_name(f"{PARTITION_DATA_FILENAME}.tmp")
    try:
        pq.write_table(table, tmp_path, use_dictionary=False)
        tmp_path.replace(out_path)
    except Exception:
        try:
            if tmp_path.exists():
                tmp_path.unlink()
        except OSError:
            pass
        raise
