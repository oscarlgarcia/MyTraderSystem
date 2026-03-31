import io
import json
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace

import pytest

from app.common.dto import MarketEvent
from app.ingestion.checkpoints import CheckpointStore
from app.ingestion.pipeline import collect_events
from app.ingestion.sinks import ParquetEventSink
from app.ingestion.sources import BinanceSource, SourceStats, StaticSource
from app.ingestion.storage import ParquetWriter, normalized_partition_path, read_parquet
from app.observability.logger import clear_trace_id, get_logger, set_trace_id


def _cfg(tmp_path: Path):
    return SimpleNamespace(
        env="test",
        data_dir=tmp_path.resolve(),
        log_level="INFO",
        ws_base="wss://stream.binance.com:9443",
        rest_base="https://api.binance.com",
        symbols=["BTCUSDT"],
    )


def _event(ts: datetime, price: float) -> MarketEvent:
    return MarketEvent(
        symbol="BTCUSDT",
        event_ts=ts,
        price=price,
        size=1.0,
        source="trade",
    )


def _json_lines(buffer: io.StringIO) -> list[dict[str, object]]:
    return [json.loads(line) for line in buffer.getvalue().splitlines() if line.strip()]


class FlakyReconnectSource:
    def __init__(self, events: list[MarketEvent]):
        self.events = events
        self.attempts = 0
        self.stats = SourceStats()

    def stream(self, end_time=None):
        del end_time
        self.attempts += 1
        if self.attempts == 1:
            first = self.events[0]
            self.stats.source_events_in += 1
            self.stats.events_valid += 1
            yield first
            raise RuntimeError("temporary stream drop")
        for event in self.events:
            self.stats.source_events_in += 1
            self.stats.events_valid += 1
            yield event

    def snapshot(self):
        return None


class ExplodingSink:
    def __init__(self):
        self.items = []

    def add(self, batch):
        if isinstance(batch, list):
            self.items.extend(batch)
        else:
            self.items.append(batch)
        raise OSError("disk full")

    def close(self):
        return None


class FailOnceOnCloseSink:
    def __init__(self, inner):
        self.inner = inner
        self.failed = False

    def add(self, batch):
        self.inner.add(batch)

    def close(self):
        self.inner.close()
        if not self.failed:
            self.failed = True
            raise OSError("simulated sink close failure")

    @property
    def persisted_count(self):
        return self.inner.persisted_count

    @property
    def write_latency_seconds(self):
        return self.inner.write_latency_seconds


class RecordingErrorSink:
    def __init__(self):
        self.records = []

    def write(self, raw_message, error, context=None):
        self.records.append((raw_message, error, context or {}))


class RecordingSink:
    def __init__(self):
        self.items = []
        self.persisted_count = 0
        self.write_latency_seconds = 0.0

    def add(self, batch):
        if isinstance(batch, list):
            self.items.extend(batch)
            self.persisted_count += len(batch)
        else:
            self.items.append(batch)
            self.persisted_count += 1

    def close(self):
        return None


@pytest.mark.slow
def test_end_to_end_live_mock_with_reconnect_checkpoint_and_sink_flush(tmp_path):
    cfg = _cfg(tmp_path)
    base = datetime(2024, 1, 1, tzinfo=timezone.utc)
    events = [
        _event(base, 100.0),
        _event(base + timedelta(seconds=1), 101.0),
    ]
    source = FlakyReconnectSource(events)
    sink = ParquetEventSink(ParquetWriter(base_dir=tmp_path, env="test", flush_size=10, dedup=True))
    checkpoint_store = CheckpointStore(tmp_path / "checkpoint.json")
    buffer = io.StringIO()
    logger = get_logger(name="test.readiness.e2e", level="INFO", stream=buffer)
    set_trace_id("readiness-e2e")
    try:
        out = collect_events(
            mode="live",
            cfg=cfg,
            max_events=10,
            duration_s=0,
            logger=logger,
            source=source,
            sink=sink,
            checkpoint_store=checkpoint_store,
            snapshot_enabled=False,
            summary_logging=True,
        )
    finally:
        clear_trace_id()

    assert out == events
    assert source.attempts == 2
    checkpoint = checkpoint_store.load()
    assert checkpoint is not None
    assert checkpoint.last_event_ts == events[-1].event_ts

    out_path = normalized_partition_path(tmp_path, "test", source="trade", symbol="BTCUSDT", day="2024-01-01")
    table = read_parquet(out_path)
    assert table.num_rows == 2

    summary = next(line for line in _json_lines(buffer) if line["message"] == "ingestion summary")
    assert summary["reconnects"] == 1
    assert summary["events_persisted"] == 2
    assert summary["trace_id"] == "readiness-e2e"


@pytest.mark.slow
def test_overload_policy_under_10k_mock_events(tmp_path):
    cfg = _cfg(tmp_path)
    base = datetime(2024, 1, 1, tzinfo=timezone.utc)
    events = [_event(base + timedelta(milliseconds=index), 100.0 + index) for index in range(10_000)]
    buffer = io.StringIO()
    logger = get_logger(name="test.readiness.overload", level="INFO", stream=buffer)
    sink = RecordingSink()

    started = time.perf_counter()
    out = collect_events(
        mode="live",
        cfg=cfg,
        max_events=10_000,
        duration_s=0,
        logger=logger,
        source=StaticSource(events=events),
        sink=sink,
        snapshot_enabled=False,
        max_buffer=32,
        backpressure_policy="drop_newest",
        batch_size=1,
        summary_logging=True,
    )
    elapsed = time.perf_counter() - started

    summary = next(line for line in _json_lines(buffer) if line["message"] == "ingestion summary")
    assert len(out) < 10_000
    assert sink.persisted_count == len(out)
    assert summary["events_buffer_dropped"] > 0
    assert summary["buffer_overflows"] > 0
    assert summary["backpressure_policy"] == "drop_newest"
    assert elapsed < 5.0


@pytest.mark.slow
def test_restart_after_partial_failure_preserves_consistency(tmp_path):
    cfg = _cfg(tmp_path)
    base = datetime(2024, 1, 1, tzinfo=timezone.utc)
    events = [
        _event(base, 100.0),
        _event(base + timedelta(seconds=1), 101.0),
        _event(base + timedelta(seconds=2), 102.0),
    ]
    checkpoint_store = CheckpointStore(tmp_path / "restart-checkpoint.json")
    first_sink = FailOnceOnCloseSink(
        ParquetEventSink(ParquetWriter(base_dir=tmp_path, env="test", flush_size=2, dedup=True))
    )

    with pytest.raises(Exception):
        collect_events(
            mode="live",
            cfg=cfg,
            max_events=10,
            duration_s=0,
            logger=get_logger(name="test.readiness.restart.fail", level="INFO", stream=io.StringIO()),
            source=StaticSource(events=events),
            sink=first_sink,
            checkpoint_store=checkpoint_store,
            snapshot_enabled=False,
            summary_logging=True,
        )

    assert checkpoint_store.load() is None

    second_sink = ParquetEventSink(ParquetWriter(base_dir=tmp_path, env="test", flush_size=2, dedup=True))
    out = collect_events(
        mode="live",
        cfg=cfg,
        max_events=10,
        duration_s=0,
        logger=get_logger(name="test.readiness.restart.ok", level="INFO", stream=io.StringIO()),
        source=StaticSource(events=events),
        sink=second_sink,
        checkpoint_store=checkpoint_store,
        snapshot_enabled=False,
        summary_logging=True,
    )

    assert out == events
    out_path = normalized_partition_path(tmp_path, "test", source="trade", symbol="BTCUSDT", day="2024-01-01")
    table = read_parquet(out_path)
    assert table.num_rows == 3
    checkpoint = checkpoint_store.load()
    assert checkpoint is not None
    assert checkpoint.last_event_ts == events[-1].event_ts


@pytest.mark.slow
def test_corrupt_input_and_sink_failure_leave_system_diagnosable(tmp_path):
    cfg = _cfg(tmp_path)
    raw_invalid = json.dumps({"stream": "btcusdt@trade", "data": {"s": "BTCUSDT", "E": 1710000000000, "p": "bad"}})
    raw_valid = json.dumps({"stream": "btcusdt@trade", "data": {"s": "BTCUSDT", "E": 1710000000000, "p": "100", "q": "1"}})
    error_sink = RecordingErrorSink()
    source = BinanceSource(
        cfg,
        ws_stream=lambda *_args, **_kwargs: iter([raw_invalid, raw_valid]),
        error_sink=error_sink,
    )
    buffer = io.StringIO()
    logger = get_logger(name="test.readiness.diagnosable", level="INFO", stream=buffer)
    set_trace_id("readiness-failure")
    try:
        with pytest.raises(Exception):
            collect_events(
                mode="live",
                cfg=cfg,
                max_events=10,
                duration_s=0,
                logger=logger,
                source=source,
                sink=ExplodingSink(),
                snapshot_enabled=False,
                summary_logging=True,
                error_policy="fail_fast",
            )
    finally:
        clear_trace_id()

    logs = _json_lines(buffer)
    summary = next(line for line in logs if line["message"] == "ingestion summary")
    failure = next(line for line in logs if line["message"] == "ingestion failed")
    health = next(line for line in logs if line["message"] == "ingestion health")
    assert len(error_sink.records) == 1
    assert failure["error_category"] == "sink"
    assert summary["result"] == "failed"
    assert summary["events_invalid"] == 1
    assert summary["events_persisted"] == 0
    assert summary["trace_id"] == "readiness-failure"
    assert health["result"] == "failed"
