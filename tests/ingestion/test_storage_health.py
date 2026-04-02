import io
import json
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from app import main
from app.common.dto import MarketEvent
from app.ingestion.compaction import compact_partition
from app.ingestion.storage import ParquetWriter, normalized_partition_path, partition_compaction_failure_path
from app.ingestion.storage_health import (
    assert_storage_health_for_runtime,
    collect_storage_health,
    emit_storage_health_alerts,
)
from app.observability.logger import get_logger


def _event(ts: datetime, *, symbol: str = "BTCUSDT") -> MarketEvent:
    return MarketEvent(symbol=symbol, event_ts=ts, price=100.0, size=1.0, source="trade")


def test_storage_health_report_exposes_partition_metrics(tmp_path: Path):
    base = datetime(2024, 1, 1, tzinfo=timezone.utc)
    writer = ParquetWriter(base_dir=tmp_path, env="dev", flush_size=1)
    writer.add(_event(base))
    writer.add(_event(base + timedelta(minutes=1)))

    partition_path = normalized_partition_path(
        tmp_path,
        "dev",
        source="trade",
        symbol="BTCUSDT",
        day="2024-01-01",
    )
    segments = sorted((partition_path / "segments").glob("*.parquet"))
    old_ts = (datetime.now(timezone.utc) - timedelta(minutes=30)).timestamp()
    for segment in segments:
        os.utime(segment, (old_ts, old_ts))

    report = collect_storage_health(tmp_path, "dev")

    assert report.segments_pending_total == 2
    assert report.segments_per_partition_max == 2
    assert report.compaction_lag_seconds >= 60.0
    assert report.compaction_failures_total == 0
    assert report.normalized_partition_row_count == 2
    assert len(report.partitions) == 1
    partition = report.partitions[0]
    assert partition.symbol == "BTCUSDT"
    assert partition.segments_pending == 2
    assert partition.normalized_partition_row_count == 2


def test_storage_health_emits_backlog_and_failure_alerts(tmp_path: Path):
    base = datetime(2024, 1, 1, tzinfo=timezone.utc)
    writer = ParquetWriter(base_dir=tmp_path, env="dev", flush_size=1)
    writer.add(_event(base))
    writer.add(_event(base + timedelta(minutes=1)))

    partition_path = normalized_partition_path(
        tmp_path,
        "dev",
        source="trade",
        symbol="BTCUSDT",
        day="2024-01-01",
    )
    segments = sorted((partition_path / "segments").glob("*.parquet"))
    old_ts = (datetime.now(timezone.utc) - timedelta(hours=1)).timestamp()
    for segment in segments:
        os.utime(segment, (old_ts, old_ts))
    failure_path = partition_compaction_failure_path(partition_path)
    failure_path.write_text(json.dumps({"ts": datetime.now(timezone.utc).isoformat(), "error": "disk full"}) + "\n", encoding="utf-8")

    report = collect_storage_health(tmp_path, "dev")
    buffer = io.StringIO()
    logger = get_logger(name="test.storage.health", level="INFO", stream=buffer)

    emit_storage_health_alerts(logger, report, backlog_segment_threshold=2, critical_compaction_lag_seconds=60.0)

    alerts = [json.loads(line) for line in buffer.getvalue().splitlines() if line.strip()]
    alert_types = [record["alert_type"] for record in alerts if record["message"] == "operational alert"]
    assert "compaction_backlog_high" in alert_types
    assert "compaction_failure_detected" in alert_types


def test_compaction_failure_is_recorded_for_storage_health(monkeypatch, tmp_path: Path):
    base = datetime(2024, 1, 1, tzinfo=timezone.utc)
    writer = ParquetWriter(base_dir=tmp_path, env="dev", flush_size=2)
    writer.add(_event(base))
    writer.add(_event(base + timedelta(minutes=1)))

    def broken_write(*args, **kwargs):
        raise RuntimeError("disk full")

    monkeypatch.setattr("app.ingestion.compaction.pq.write_table", broken_write)

    with pytest.raises(RuntimeError):
        compact_partition(tmp_path, "dev", source="trade", symbol="BTCUSDT", day="2024-01-01")

    partition_path = normalized_partition_path(
        tmp_path,
        "dev",
        source="trade",
        symbol="BTCUSDT",
        day="2024-01-01",
    )
    failure_path = partition_compaction_failure_path(partition_path)
    assert failure_path.exists()

    report = collect_storage_health(tmp_path, "dev")
    assert report.compaction_failures_total == 1


def test_production_mode_blocks_when_compaction_lag_is_critical(monkeypatch, tmp_path: Path):
    base = datetime(2024, 1, 1, tzinfo=timezone.utc)
    writer = ParquetWriter(base_dir=tmp_path, env="prod", flush_size=1)
    writer.add(_event(base))

    partition_path = normalized_partition_path(
        tmp_path,
        "prod",
        source="trade",
        symbol="BTCUSDT",
        day="2024-01-01",
    )
    segment = next((partition_path / "segments").glob("*.parquet"))
    old_ts = (datetime.now(timezone.utc) - timedelta(hours=1)).timestamp()
    os.utime(segment, (old_ts, old_ts))

    cfg = main.load_config("dev")
    cfg = type(cfg)(
        env="prod",
        data_dir=tmp_path.resolve(),
        log_level=cfg.log_level,
        ws_base=cfg.ws_base,
        rest_base=cfg.rest_base,
        symbols=cfg.symbols,
    )
    runtime = {
        "production_mode": True,
        "fast_path": False,
        "allow_live_fallback": False,
        "error_policy": "fail_fast",
        "ingest_dedup": True,
        "summary_logging": True,
        "ingest_backpressure_policy": "pause",
        "ingest_stream_types": ("kline",),
    }

    monkeypatch.setattr(main, "validate_live_feed_support", lambda *args, **kwargs: None)
    metadata_path = tmp_path / "metadata" / "instruments" / "env=prod" / "venue=BINANCE" / "latest.json"
    metadata_path.parent.mkdir(parents=True, exist_ok=True)
    metadata_path.write_text(
        json.dumps(
            {
                "metadata_snapshot_mode": "runtime",
                "drift": {"material": False},
            }
        ),
        encoding="utf-8",
    )

    with pytest.raises(ValueError, match="storage compaction lag exceeds the critical threshold"):
        main._validate_operational_security(cfg, mode="live", runtime=runtime)


def test_assert_storage_health_for_runtime_allows_clean_storage(tmp_path: Path):
    report = assert_storage_health_for_runtime(tmp_path, "dev")
    assert report.segments_pending_total == 0
    assert report.compaction_lag_seconds == 0.0
