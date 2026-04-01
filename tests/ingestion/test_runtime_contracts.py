import io
import json
import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest import mock

import pytest

from app.common.dto import MarketEvent
from app.ingestion import pipeline
from app.ingestion.errors import IngestionError
from app.ingestion.resilience import ResilientRunner
from app.ingestion.shadow import ShadowComparison, ShadowPromotionError, ShadowSnapshot
from app.ingestion.sources import StaticSource
from app.ingestion.storage import legacy_partition_path, normalized_partition_path
from app.observability.logger import get_logger


def _ev(ts_offset: int, price: float) -> MarketEvent:
    return MarketEvent(
        symbol="BTCUSDT",
        event_ts=datetime.fromtimestamp(1700000000 + ts_offset, tz=timezone.utc),
        price=price,
        size=1.0,
        source="trade",
    )


def _json_lines(buffer: io.StringIO) -> list[dict[str, object]]:
    return [json.loads(line) for line in buffer.getvalue().splitlines() if line.strip()]


class DummySink:
    def __init__(self):
        self.items = []

    def add(self, batch):
        if isinstance(batch, list):
            self.items.extend(batch)
            return
        self.items.append(batch)

    def close(self):
        return None


def test_live_collect_events_returns_processed_events_after_flush(monkeypatch):
    cfg = mock.Mock(env="dev", ws_base="wss://x", rest_base="https://x", symbols=["BTCUSDT"], data_dir=".", log_level="INFO")
    events = [_ev(0, 100), _ev(60, 101)]

    out = pipeline.collect_events(
        mode="live",
        cfg=cfg,
        max_events=10,
        duration_s=0,
        logger=mock.Mock(),
        snapshot_enabled=False,
        source=StaticSource(events=events),
        sink=DummySink(),
    )

    assert out == events


def test_live_failure_fail_fast_by_default(monkeypatch):
    cfg = mock.Mock(env="dev", ws_base="wss://x", rest_base="https://x", symbols=["BTCUSDT"], data_dir=".", log_level="INFO")
    synthetic = mock.Mock(return_value=[_ev(0, 100)])
    monkeypatch.setattr(pipeline, "_synthetic_events", synthetic)

    class BrokenSource:
        def stream(self, end_time=None):
            del end_time
            raise RuntimeError("ws down")

        def snapshot(self):
            return None

    with pytest.raises(IngestionError) as exc_info:
        pipeline.collect_events(mode="live", cfg=cfg, duration_s=0, logger=mock.Mock(), source=BrokenSource(), sink=DummySink())

    assert exc_info.value.category == "source"
    synthetic.assert_not_called()


def test_runner_with_dedup_off_and_gap_does_not_crash():
    base = datetime(2024, 1, 1, tzinfo=timezone.utc)
    stream_events = [
        MarketEvent(symbol="BTCUSDT", event_ts=base, price=100.0, size=1.0, source="kline"),
        MarketEvent(symbol="BTCUSDT", event_ts=base + timedelta(seconds=10), price=101.0, size=1.0, source="kline"),
    ]
    snapshot_events = [
        MarketEvent(symbol="BTCUSDT", event_ts=base + timedelta(seconds=5), price=100.5, size=1.0, source="kline"),
    ]
    handled = []

    def stream():
        for event in stream_events:
            yield event

    runner = ResilientRunner(
        stream_fn=stream,
        snapshot_fn=lambda: snapshot_events,
        dedup_enabled=False,
        lag_threshold_seconds=2,
        sleeper=lambda _seconds: None,
    )
    runner.run(lambda event: handled.append(event), stop_on_complete=True)

    assert len(handled) == 3


def test_summary_metrics_match_processed_events(monkeypatch):
    cfg = mock.Mock(env="dev", ws_base="wss://x", rest_base="https://x", symbols=["BTCUSDT"], data_dir=".", log_level="INFO")
    event_a = _ev(0, 100)
    event_b = _ev(60, 101)
    events = [event_a, event_a, event_b]
    buffer = io.StringIO()
    logger = get_logger(name="test.ingest.runtime.summary", level="INFO", stream=buffer)

    out = pipeline.collect_events(
        mode="live",
        cfg=cfg,
        max_events=10,
        duration_s=0,
        logger=logger,
        dedup_enabled=True,
        snapshot_enabled=False,
        summary_logging=True,
        source=StaticSource(events=events),
        sink=DummySink(),
    )

    assert out == [event_a, event_b]
    summary = next(record for record in _json_lines(buffer) if record["message"] == "ingestion summary")
    assert summary["events_in"] == 3
    assert summary["events_out"] == 2
    assert summary["events_persisted"] == 2
    assert summary["duplicates_dropped"] == 1
    assert summary["buffer_overflows"] == 0
    assert summary["backpressure_policy"] == "pause"
    assert summary["dedup_on"] is True
    assert summary["batch_size"] == 1


def test_shadow_mode_writes_v1_and_v2_and_persists_comparison(tmp_path: Path):
    cfg = mock.Mock(env="dev", ws_base="wss://x", rest_base="https://x", symbols=["BTCUSDT"], data_dir=tmp_path, log_level="INFO")
    events = [_ev(0, 100), _ev(60, 101)]

    out = pipeline.collect_events(
        mode="live",
        cfg=cfg,
        max_events=10,
        duration_s=0,
        logger=get_logger(name="test.ingest.shadow", level="INFO", stream=io.StringIO()),
        dedup_enabled=True,
        snapshot_enabled=False,
        summary_logging=True,
        source=StaticSource(events=events),
        shadow_mode=True,
        pipeline_version="v2",
    )

    assert out == events
    assert normalized_partition_path(tmp_path, "dev", source="trade", symbol="BTCUSDT", day="2023-11-14").exists()
    assert legacy_partition_path(tmp_path, "dev", "BTCUSDT", "2023-11-14").exists()
    comparison_path = tmp_path / "shadow" / "env=dev" / "comparisons.jsonl"
    assert comparison_path.exists()
    payload = json.loads(comparison_path.read_text(encoding="utf-8").splitlines()[-1])
    assert payload["primary"]["pipeline_version"] == "v2"
    assert payload["shadow"]["pipeline_version"] == "v1"
    assert payload["significant"] is False


def test_shadow_block_on_diff_raises_promotion_error(monkeypatch, tmp_path: Path):
    cfg = mock.Mock(env="dev", ws_base="wss://x", rest_base="https://x", symbols=["BTCUSDT"], data_dir=tmp_path, log_level="INFO")

    def fake_compare(primary, shadow):
        del primary, shadow
        return ShadowComparison(
            primary=ShadowSnapshot("v2", 2, 0, 0, 0.1, 0.1),
            shadow=ShadowSnapshot("v1", 1, 0, 0, 0.1, 0.1),
            diffs={
                "events_persisted": 1.0,
                "duplicates_total": 0.0,
                "gaps_total": 0.0,
                "processing_latency_seconds": 0.0,
                "write_latency_seconds": 0.0,
            },
            significant=True,
        )

    monkeypatch.setattr(pipeline, "compare_shadow_snapshots", fake_compare)

    with pytest.raises(ShadowPromotionError):
        pipeline.collect_events(
            mode="live",
            cfg=cfg,
            max_events=10,
            duration_s=0,
            logger=get_logger(name="test.ingest.shadow.block", level="INFO", stream=io.StringIO()),
            dedup_enabled=True,
            snapshot_enabled=False,
            summary_logging=True,
            source=StaticSource(events=[_ev(0, 100)]),
            shadow_mode=True,
            shadow_block_on_diff=True,
            pipeline_version="v2",
        )
