import io
import json
from datetime import datetime, timezone
from pathlib import Path
from unittest import mock

from app.common.dto import MarketEvent
from app.ingestion import pipeline
from app.ingestion.shadow import (
    affected_shadow_partitions,
    build_shadow_snapshot,
    compare_shadow_snapshots,
)
from app.ingestion.sinks import ParquetEventSink
from app.ingestion.sources import StaticSource
from app.ingestion.storage import ParquetWriter
from app.observability.logger import get_logger


def _event(*, symbol: str, ts: str, price: float) -> MarketEvent:
    return MarketEvent(
        symbol=symbol,
        event_ts=datetime.fromisoformat(ts),
        price=price,
        size=1.0,
        source="trade",
        metadata={"venue": "BINANCE"},
    )


def _write_events(base_dir: Path, schema_version: str, events: list[MarketEvent]) -> None:
    sink = ParquetEventSink(
        ParquetWriter(
            base_dir=base_dir,
            env="dev",
            flush_size=1,
            dedup=True,
            schema_version=schema_version,
        )
    )
    sink.add(events)
    sink.close()


def test_build_shadow_snapshot_can_scope_to_requested_partitions(tmp_path: Path) -> None:
    current = _event(symbol="BTCUSDT", ts="2023-11-14T00:00:00+00:00", price=100.0)
    unrelated = _event(symbol="ETHUSDT", ts="2023-11-14T00:05:00+00:00", price=200.0)

    _write_events(tmp_path, "v2", [current])
    _write_events(tmp_path, "v1", [current, unrelated])

    primary_full = build_shadow_snapshot(
        tmp_path,
        env="dev",
        pipeline_version="v2",
        gaps_total=0,
        processing_latency_seconds=0.1,
        write_latency_seconds=0.1,
    )
    shadow_full = build_shadow_snapshot(
        tmp_path,
        env="dev",
        pipeline_version="v1",
        gaps_total=0,
        processing_latency_seconds=0.1,
        write_latency_seconds=0.1,
    )
    assert compare_shadow_snapshots(primary_full, shadow_full).significant is True

    scope = affected_shadow_partitions([current])
    primary_scoped = build_shadow_snapshot(
        tmp_path,
        env="dev",
        pipeline_version="v2",
        gaps_total=0,
        processing_latency_seconds=0.1,
        write_latency_seconds=0.1,
        partition_keys=scope,
    )
    shadow_scoped = build_shadow_snapshot(
        tmp_path,
        env="dev",
        pipeline_version="v1",
        gaps_total=0,
        processing_latency_seconds=0.1,
        write_latency_seconds=0.1,
        partition_keys=scope,
    )

    comparison = compare_shadow_snapshots(primary_scoped, shadow_scoped)
    assert comparison.significant is False
    assert comparison.primary.scope_mode == "partition_scope"
    assert comparison.primary.scope_partitions == scope
    assert comparison.shadow.scope_partitions == scope


def test_pipeline_shadow_mode_compares_only_affected_partitions(tmp_path: Path) -> None:
    current = _event(symbol="BTCUSDT", ts="2023-11-14T00:00:00+00:00", price=100.0)
    unrelated = _event(symbol="ETHUSDT", ts="2023-11-13T00:00:00+00:00", price=200.0)
    _write_events(tmp_path, "v1", [unrelated])

    cfg = mock.Mock(
        env="dev",
        ws_base="wss://x",
        rest_base="https://x",
        symbols=["BTCUSDT"],
        data_dir=tmp_path,
        log_level="INFO",
    )
    buffer = io.StringIO()
    logger = get_logger(name="test.ingest.shadow.scope", level="INFO", stream=buffer)

    out = pipeline.collect_events(
        mode="live",
        cfg=cfg,
        max_events=10,
        duration_s=0,
        logger=logger,
        dedup_enabled=True,
        snapshot_enabled=False,
        summary_logging=True,
        source=StaticSource(events=[current]),
        shadow_mode=True,
        pipeline_version="v2",
        stream_types=("kline",),
    )

    assert out == [current]
    comparison_path = tmp_path / "shadow" / "env=dev" / "comparisons.jsonl"
    payload = json.loads(comparison_path.read_text(encoding="utf-8").splitlines()[-1])
    assert payload["significant"] is False
    assert payload["primary"]["scope_mode"] == "partition_scope"
    assert payload["primary"]["scope_partitions"] == ["trades:BINANCE:BTCUSDT:2023-11-14"]

    alerts = [json.loads(line) for line in buffer.getvalue().splitlines() if line.strip()]
    shadow_alerts = [
        record
        for record in alerts
        if record.get("message") == "operational alert" and record.get("alert_type") == "shadow_semantic_diff"
    ]
    assert shadow_alerts == []
