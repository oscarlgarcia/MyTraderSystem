from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from app.common.dto import MarketEvent
from app.ingestion.compaction import (
    CompactionJobPolicy,
    RETAINED_SEGMENTS_DIRNAME,
    run_compaction_job,
    select_compaction_candidates,
)
from app.ingestion.storage import ParquetWriter, normalized_partition_path, partition_segments_dir, read_parquet


def _event(ts: datetime, *, symbol: str = "BTCUSDT") -> MarketEvent:
    return MarketEvent(symbol=symbol, event_ts=ts, price=100.0, size=1.0, source="trade")


def _seed_partition(tmp_path: Path, *, symbol: str = "BTCUSDT", rows: int = 3) -> Path:
    writer = ParquetWriter(base_dir=tmp_path, env="dev", flush_size=1)
    base = datetime(2024, 1, 1, tzinfo=timezone.utc)
    for offset in range(rows):
        writer.add(_event(base + timedelta(minutes=offset), symbol=symbol))
    partition_path = normalized_partition_path(
        tmp_path,
        "dev",
        source="trade",
        symbol=symbol,
        day="2024-01-01",
    )
    return partition_path


def test_select_compaction_candidates_respects_batch_limit_and_priority(tmp_path: Path):
    btc_partition = _seed_partition(tmp_path, symbol="BTCUSDT", rows=3)
    eth_partition = _seed_partition(tmp_path, symbol="ETHUSDT", rows=2)

    old_ts = (datetime.now(timezone.utc) - timedelta(hours=1)).timestamp()
    for segment in sorted(partition_segments_dir(btc_partition).glob("*.parquet")):
        os.utime(segment, (old_ts, old_ts))
    for segment in sorted(partition_segments_dir(eth_partition).glob("*.parquet")):
        os.utime(segment, (old_ts + 1800, old_ts + 1800))

    candidates = select_compaction_candidates(
        tmp_path,
        "dev",
        batch_limit=1,
        min_segments_pending=2,
        min_compaction_lag_seconds=60.0,
    )

    assert len(candidates) == 1
    assert candidates[0].symbol == "BTCUSDT"


def test_select_compaction_candidates_can_be_scoped_to_partition_paths(tmp_path: Path):
    btc_partition = _seed_partition(tmp_path, symbol="BTCUSDT", rows=3)
    _seed_partition(tmp_path, symbol="ETHUSDT", rows=3)

    candidates = select_compaction_candidates(
        tmp_path,
        "dev",
        batch_limit=10,
        min_segments_pending=2,
        min_compaction_lag_seconds=0.0,
        partition_paths=(btc_partition,),
    )

    assert len(candidates) == 1
    assert candidates[0].symbol == "BTCUSDT"
    assert candidates[0].partition_path == str(btc_partition.resolve())


def test_run_compaction_job_dry_run_does_not_remove_segments(tmp_path: Path):
    partition_path = _seed_partition(tmp_path, rows=2)

    report = run_compaction_job(
        tmp_path,
        "dev",
        policy=CompactionJobPolicy(dry_run=True, batch_limit=10, min_segments_pending=2, min_compaction_lag_seconds=0.0),
    )

    assert report.planned_partitions == 1
    assert report.results[0].status == "planned"
    assert len(sorted(partition_segments_dir(partition_path).glob("*.parquet"))) == 2


def test_run_compaction_job_scopes_work_to_requested_partitions(tmp_path: Path):
    btc_partition = _seed_partition(tmp_path, symbol="BTCUSDT", rows=3)
    eth_partition = _seed_partition(tmp_path, symbol="ETHUSDT", rows=3)

    report = run_compaction_job(
        tmp_path,
        "dev",
        partition_paths=(btc_partition,),
        policy=CompactionJobPolicy(
            batch_limit=10,
            retry_attempts=0,
            min_segments_pending=2,
            min_compaction_lag_seconds=0.0,
        ),
    )

    assert report.compacted_partitions == 1
    assert report.failed_partitions == 0
    assert report.results[0].symbol == "BTCUSDT"
    assert not partition_segments_dir(btc_partition).exists()
    assert partition_segments_dir(eth_partition).exists()


def test_run_compaction_job_archives_retained_segments_without_breaking_reads(tmp_path: Path):
    partition_path = _seed_partition(tmp_path, rows=3)

    report = run_compaction_job(
        tmp_path,
        "dev",
        policy=CompactionJobPolicy(
            batch_limit=10,
            retry_attempts=0,
            min_segments_pending=2,
            min_compaction_lag_seconds=0.0,
            retain_compacted_segments=1,
        ),
    )

    assert report.compacted_partitions == 1
    assert report.failed_partitions == 0
    assert not partition_segments_dir(partition_path).exists()
    retained_root = partition_path / RETAINED_SEGMENTS_DIRNAME
    retained_runs = [path for path in retained_root.iterdir() if path.is_dir()]
    assert len(retained_runs) == 1
    assert len(list(retained_runs[0].glob("*.parquet"))) == 3
    assert read_parquet(partition_path).num_rows == 3


def test_run_compaction_job_retries_partition_and_reports_attempt_count(monkeypatch, tmp_path: Path):
    _seed_partition(tmp_path, rows=2)
    attempts = {"n": 0}
    original = __import__("app.ingestion.compaction", fromlist=["compact_partition"]).compact_partition

    def flaky_compact_partition(*args, **kwargs):
        attempts["n"] += 1
        if attempts["n"] == 1:
            raise RuntimeError("temporary failure")
        return original(*args, **kwargs)

    monkeypatch.setattr("app.ingestion.compaction.compact_partition", flaky_compact_partition)

    report = run_compaction_job(
        tmp_path,
        "dev",
        policy=CompactionJobPolicy(
            batch_limit=10,
            retry_attempts=1,
            min_segments_pending=2,
            min_compaction_lag_seconds=0.0,
        ),
    )

    assert report.compacted_partitions == 1
    assert report.results[0].attempt_count == 2


def test_compaction_script_help_runs():
    import subprocess
    import sys

    result = subprocess.run(
        [sys.executable, "scripts/ingestion_compact.py", "--help"],
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0
    assert "--dry-run" in result.stdout
