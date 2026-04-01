import pytest
from unittest import mock
from app.ingestion import pipeline
from app.common.dto import MarketEvent
from app.marketdata.models import TradeEvent
from datetime import datetime, timedelta, timezone
import logging
import io
import json
import time

from app.ingestion.sources import StaticSource
from app.ingestion.sinks import ParquetEventSink
from app.ingestion.storage import ParquetWriter
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


def test_dry_collect_events_returns_typed_trade_events_by_default():
    cfg = mock.Mock(env="dev", ws_base="", rest_base="", symbols=["BTCUSDT"], data_dir=".", log_level="INFO")

    out = pipeline.collect_events(mode="dry", cfg=cfg, max_events=2, logger=mock.Mock())

    assert len(out) == 2
    assert all(isinstance(event, TradeEvent) for event in out)


def test_compute_features_after_flag_off(monkeypatch):
    cfg = mock.Mock(env="dev", ws_base="", rest_base="", symbols=["BTCUSDT"], data_dir=".", log_level="INFO")
    events = [_ev(0, 100)]
    monkeypatch.setattr(pipeline, "_synthetic_events", lambda n: events)
    feats_mock = mock.Mock(return_value=[])
    monkeypatch.setattr(pipeline, "run_feature_pipeline", feats_mock)

    out = pipeline.collect_events(mode="dry", cfg=cfg, max_events=1, logger=mock.Mock(), compute_features_after=False)
    assert out == events
    feats_mock.assert_not_called()


def test_compute_features_after_flag_on(monkeypatch, caplog):
    caplog.set_level("INFO")
    cfg = mock.Mock(env="dev", ws_base="", rest_base="", symbols=["BTCUSDT"], data_dir=".", log_level="INFO")
    events = [_ev(0, 100), _ev(60, 101)]
    monkeypatch.setattr(pipeline, "_synthetic_events", lambda n: events)
    feats_mock = mock.Mock(return_value=[1, 2])
    monkeypatch.setattr(pipeline, "run_feature_pipeline", feats_mock)

    out = pipeline.collect_events(
        mode="dry",
        cfg=cfg,
        max_events=2,
        logger=mock.Mock(),
        compute_features_after=True,
        max_buffer=5,
        dedup_enabled=True,
    )
    assert out == events
    feats_mock.assert_called_once()


def test_live_handler_dedups_before_writer_add():
    writer = mock.Mock()
    stats = {"written": 0, "duplicates_dropped": 0}
    handler = pipeline._build_live_handler(writer, stats, max_events=10, dedup_enabled=True)
    event = _ev(0, 100)

    handler(event)
    handler(event)

    writer.add.assert_called_once_with([event])
    assert stats["written"] == 1
    assert stats["duplicates_dropped"] == 1


def test_live_handler_keeps_duplicates_when_flag_off():
    writer = mock.Mock()
    stats = {"written": 0, "duplicates_dropped": 0}
    handler = pipeline._build_live_handler(writer, stats, max_events=10, dedup_enabled=False)
    event = _ev(0, 100)

    handler(event)
    handler(event)

    assert writer.add.call_count == 2
    assert stats["written"] == 2
    assert stats["duplicates_dropped"] == 0


def test_live_handler_batches_writer_add_calls():
    writer = mock.Mock()
    stats = {"written": 0, "duplicates_dropped": 0}
    handler = pipeline._build_live_handler(writer, stats, max_events=20, dedup_enabled=False, batch_size=4)

    for index in range(10):
        handler(_ev(index * 60, 100 + index))
    handler.close()

    assert writer.add.call_count == 3
    assert stats["written"] == 10


def test_live_handler_flushes_partial_batch_on_close():
    writer = mock.Mock()
    stats = {"written": 0, "duplicates_dropped": 0}
    handler = pipeline._build_live_handler(writer, stats, max_events=10, dedup_enabled=False, batch_size=4)

    handler(_ev(0, 100))
    handler(_ev(60, 101))
    handler.close()

    writer.add.assert_called_once()
    batch = writer.add.call_args[0][0]
    assert len(batch) == 2
    assert stats["written"] == 2


def test_live_handler_flushes_partial_batch_when_max_events_reached():
    writer = mock.Mock()
    stats = {"written": 0, "duplicates_dropped": 0}
    handler = pipeline._build_live_handler(writer, stats, max_events=3, dedup_enabled=False, batch_size=4)

    handler(_ev(0, 100))
    handler(_ev(60, 101))
    with pytest.raises(StopIteration):
        handler(_ev(120, 102))

    writer.add.assert_called_once()
    batch = writer.add.call_args[0][0]
    assert len(batch) == 3
    assert stats["written"] == 3


def test_buffer_warn_emitted_once(monkeypatch, caplog):
    cfg = mock.Mock(env="dev", ws_base="wss://x", rest_base="https://x", symbols=["BTCUSDT"], data_dir=".", log_level="INFO")
    logger = logging.getLogger("test.ingest.buffer_warn")
    caplog.set_level("WARNING")

    pipeline.collect_events(
        mode="live",
        cfg=cfg,
        logger=logger,
        max_events=1,
        duration_s=0,
        max_buffer=0,
        buffer_warn_threshold=0,
        summary_logging=False,
        source=StaticSource(events=[_ev(0, 100)]),
        sink=DummySink(),
    )

    warnings = [record for record in caplog.records if record.message == "ingestion buffer pressure warning"]
    assert len(warnings) == 1


def test_latency_warn_emitted_once(monkeypatch, caplog):
    cfg = mock.Mock(env="dev", ws_base="wss://x", rest_base="https://x", symbols=["BTCUSDT"], data_dir=".", log_level="INFO")
    logger = logging.getLogger("test.ingest.latency_warn")
    caplog.set_level("WARNING")
    old_event = MarketEvent(
        symbol="BTCUSDT",
        event_ts=datetime.fromtimestamp(1_700_000_000, tz=timezone.utc),
        price=100.0,
        size=1.0,
        source="trade",
    )

    pipeline.collect_events(
        mode="live",
        cfg=cfg,
        logger=logger,
        max_events=2,
        duration_s=0,
        lag_warn_threshold=0.0,
        summary_logging=False,
        source=StaticSource(events=[old_event]),
        sink=DummySink(),
    )

    warnings = [record for record in caplog.records if record.message == "ingestion latency warning"]
    assert len(warnings) == 1


def test_dry_emits_aggregated_ingestion_summary(monkeypatch):
    cfg = mock.Mock(env="dev", ws_base="", rest_base="", symbols=["BTCUSDT"], data_dir=".", log_level="INFO")
    events = [_ev(0, 100), _ev(60, 101)]
    buffer = io.StringIO()
    logger = get_logger(name="test.ingest.summary.dry", level="INFO", stream=buffer)

    monkeypatch.setattr(pipeline, "_synthetic_events", lambda n: events)

    out = pipeline.collect_events(mode="dry", cfg=cfg, max_events=2, logger=logger)

    assert out == events
    logs = _json_lines(buffer)
    summary = next(record for record in logs if record["message"] == "ingestion summary")
    assert summary["mode"] == "dry"
    assert summary["env"] == "dev"
    assert summary["events_in"] == 2
    assert summary["events_out"] == 2
    assert summary["events_persisted"] == 2
    assert summary["reconnects"] == 0
    assert summary["buffer_skipped"] == 0
    assert summary["buffer_overflows"] == 0
    assert summary["backpressure_policy"] == "pause"
    assert summary["max_latency_seconds"] == 0.0
    assert summary["dedup_on"] is True
    assert summary["batch_size"] == 1


def test_live_keeps_original_log_and_adds_aggregated_summary(monkeypatch):
    cfg = mock.Mock(env="dev", ws_base="wss://x", rest_base="https://x", symbols=["BTCUSDT"], data_dir=".", log_level="INFO")
    buffer = io.StringIO()
    logger = get_logger(name="test.ingest.summary.live", level="INFO", stream=buffer)
    events = [_ev(0, 100), _ev(60, 101)]

    out = pipeline.collect_events(
        mode="live",
        cfg=cfg,
        logger=logger,
        max_events=10,
        duration_s=0,
        dedup_enabled=True,
        batch_size=4,
        snapshot_enabled=False,
        summary_logging=True,
        source=StaticSource(events=events),
        sink=DummySink(),
    )

    assert out == events
    logs = _json_lines(buffer)
    assert any(record["message"] == "ingestion live complete" for record in logs)
    summary = next(record for record in logs if record["message"] == "ingestion summary")
    assert summary["mode"] == "live"
    assert summary["env"] == "dev"
    assert summary["events_in"] == 2
    assert summary["events_out"] == 2
    assert summary["events_persisted"] == 2
    assert summary["source_events_in"] == 2
    assert summary["events_valid"] == 2
    assert summary["events_invalid"] == 0
    assert summary["events_dedup_skipped"] == 0
    assert summary["events_buffer_dropped"] == 0
    assert summary["snapshot_runs"] == 0
    assert summary["snapshot_rows"] == 0
    assert summary["reconnects"] == 0
    assert summary["buffer_skipped"] == 0
    assert summary["buffer_overflows"] == 0
    assert summary["backpressure_policy"] == "pause"
    assert isinstance(summary["max_latency_seconds"], float)
    assert isinstance(summary["processing_latency_seconds"], float)
    assert isinstance(summary["write_latency_seconds"], float)
    assert summary["dedup_on"] is True
    assert summary["batch_size"] == 4
    health = next(record for record in logs if record["message"] == "ingestion health")
    assert health["result"] == "ok"
    assert health["events_persisted"] == 2


def test_summary_metrics_reflect_real_pipeline_path():
    cfg = mock.Mock(env="dev", ws_base="wss://x", rest_base="https://x", symbols=["BTCUSDT"], data_dir=".", log_level="INFO")
    buffer = io.StringIO()
    logger = get_logger(name="test.ingest.summary.metrics", level="INFO", stream=buffer)
    event_a = _ev(0, 100)
    source = StaticSource(events=[event_a, event_a])

    out = pipeline.collect_events(
        mode="live",
        cfg=cfg,
        logger=logger,
        max_events=10,
        duration_s=0,
        dedup_enabled=True,
        snapshot_enabled=False,
        summary_logging=True,
        source=source,
        sink=DummySink(),
    )

    assert out == [event_a]
    summary = next(record for record in _json_lines(buffer) if record["message"] == "ingestion summary")
    assert summary["source_events_in"] == 2
    assert summary["events_valid"] == 2
    assert summary["events_invalid"] == 0
    assert summary["events_dedup_skipped"] == 1
    assert summary["events_buffer_dropped"] == 0
    assert summary["events_persisted"] == 1
    assert summary["reconnects"] == 0
    assert summary["temporal_policy"] == "accept"
    assert summary["event_gap_seconds"] == 0.0
    assert summary["gaps_total"] == 0
    assert summary["gap_irreparable_total"] == 0


def test_sink_latency_metric_records_flush_cost(monkeypatch, tmp_path):
    cfg = mock.Mock(env="dev", ws_base="wss://x", rest_base="https://x", symbols=["BTCUSDT"], data_dir=tmp_path, log_level="INFO")
    buffer = io.StringIO()
    logger = get_logger(name="test.ingest.summary.write_latency", level="INFO", stream=buffer)
    sink = ParquetEventSink(ParquetWriter(base_dir=tmp_path, env="dev", flush_size=10))
    real_write = pipeline.ParquetWriter.flush

    def slow_flush(self):
        time.sleep(0.01)
        return real_write(self)

    monkeypatch.setattr(pipeline.ParquetWriter, "flush", slow_flush)

    out = pipeline.collect_events(
        mode="live",
        cfg=cfg,
        logger=logger,
        max_events=10,
        duration_s=0,
        dedup_enabled=False,
        snapshot_enabled=False,
        summary_logging=True,
        source=StaticSource(events=[_ev(0, 100)]),
        sink=sink,
    )

    assert len(out) == 1
    summary = next(record for record in _json_lines(buffer) if record["message"] == "ingestion summary")
    assert summary["events_persisted"] == 1
    assert summary["write_latency_seconds"] > 0.0
    stream_metric = next(metric for metric in summary["stream_metrics"] if metric["symbol"] == "BTCUSDT")
    assert stream_metric["normalized_write_latency"] > 0.0


def test_late_event_metrics_are_reported_separately():
    cfg = mock.Mock(env="dev", ws_base="wss://x", rest_base="https://x", symbols=["BTCUSDT"], data_dir=".", log_level="INFO")
    buffer = io.StringIO()
    logger = get_logger(name="test.ingest.summary.temporal", level="INFO", stream=buffer)
    base = datetime(2024, 1, 1, tzinfo=timezone.utc)
    events = [
        MarketEvent(symbol="BTCUSDT", event_ts=base + timedelta(seconds=20), price=101.0, size=1.0, source="trade"),
        MarketEvent(symbol="BTCUSDT", event_ts=base + timedelta(seconds=5), price=100.0, size=1.0, source="trade"),
    ]

    out = pipeline.collect_events(
        mode="live",
        cfg=cfg,
        logger=logger,
        max_events=10,
        duration_s=0,
        dedup_enabled=True,
        snapshot_enabled=False,
        summary_logging=True,
        temporal_policy="accept",
        source=StaticSource(events=events),
        sink=DummySink(),
    )

    assert out == events
    summary = next(record for record in _json_lines(buffer) if record["message"] == "ingestion summary")
    assert summary["processing_latency_seconds"] >= 0.0
    assert summary["event_gap_seconds"] == 0.0
    assert summary["gaps_total"] == 0
    assert summary["gap_irreparable_total"] == 0
    assert summary["late_events"] == 1
    assert summary["out_of_order_events"] == 1
    assert summary["late_events_dropped"] == 0
    assert summary["late_event_max_delay_seconds"] == 15.0


def test_stream_metrics_make_incident_attributable_to_specific_stream():
    cfg = mock.Mock(env="dev", ws_base="wss://x", rest_base="https://x", symbols=["BTCUSDT"], data_dir=".", log_level="INFO")
    buffer = io.StringIO()
    logger = get_logger(name="test.ingest.summary.stream_metrics", level="INFO", stream=buffer)
    base = datetime(2024, 1, 1, tzinfo=timezone.utc)
    events = [
        MarketEvent(symbol="BTCUSDT", event_ts=base, price=100.0, size=1.0, source="trade", metadata={"trade_id": "1"}),
        MarketEvent(symbol="BTCUSDT", event_ts=base + timedelta(seconds=1), price=101.0, size=1.0, source="trade", metadata={"trade_id": "3"}),
    ]

    out = pipeline.collect_events(
        mode="live",
        cfg=cfg,
        logger=logger,
        max_events=10,
        duration_s=0,
        dedup_enabled=True,
        snapshot_enabled=False,
        summary_logging=True,
        source=StaticSource(events=events),
        sink=DummySink(),
        error_policy="fail_fast",
    )

    assert len(out) == 2
    logs = _json_lines(buffer)
    summary = next(record for record in logs if record["message"] == "ingestion summary")
    health = next(record for record in logs if record["message"] == "ingestion health")
    stream_metric = next(metric for metric in summary["stream_metrics"] if metric["symbol"] == "BTCUSDT")

    assert stream_metric["venue"] == "BINANCE"
    assert stream_metric["stream_type"] == "trade"
    assert stream_metric["messages_in_total"] == 2
    assert stream_metric["gaps_total"] == 1
    assert stream_metric["gap_irreparable_total"] == 1
    assert "BINANCE:BTCUSDT:trade" in health["streams_degraded"]
