import io
import json
from unittest import mock

from app.ingestion import pipeline
from app.ingestion.sinks import ErrorSink
from app.ingestion.sources import BinanceSource
from app.observability.logger import get_logger


def _raw_message(data: dict, stream: str = "btcusdt@trade") -> str:
    return json.dumps({"stream": stream, "data": data})


class RecordingErrorSink:
    def __init__(self):
        self.records = []

    def write(self, raw_message, error, context=None):
        self.records.append((raw_message, error, context or {}))


class FailingErrorSink:
    def __init__(self):
        self.calls = 0

    def write(self, raw_message, error, context=None):
        del raw_message, error, context
        self.calls += 1
        raise OSError("dlq unavailable")


class RecordingSink:
    def __init__(self):
        self.items = []

    def add(self, event):
        if isinstance(event, list):
            self.items.extend(event)
            return
        self.items.append(event)

    def close(self):
        return None


def test_missing_required_field_goes_to_error_sink():
    cfg = mock.Mock(env="dev", ws_base="wss://x", rest_base="https://x", symbols=["BTCUSDT"], data_dir=".", log_level="INFO")
    error_sink = RecordingErrorSink()
    source = BinanceSource(
        cfg,
        ws_stream=lambda *_args, **_kwargs: iter(
            [
                _raw_message({"s": "BTCUSDT", "E": 1710000000000, "p": "100"}),
                _raw_message({"s": "BTCUSDT", "E": 1710000000000, "p": "100", "q": "1"}),
            ]
        ),
        error_sink=error_sink,
    )
    sink = RecordingSink()

    events = pipeline.collect_events(mode="live", cfg=cfg, duration_s=0, source=source, sink=sink)

    assert len(events) == 1
    assert len(error_sink.records) == 1
    _raw, error, context = error_sink.records[0]
    assert error.category == "parse"
    assert context["stage"] == "stream"


def test_invalid_numeric_field_is_rejected_and_counted():
    cfg = mock.Mock(env="dev", ws_base="wss://x", rest_base="https://x", symbols=["BTCUSDT"], data_dir=".", log_level="INFO")
    error_sink = RecordingErrorSink()
    source = BinanceSource(
        cfg,
        ws_stream=lambda *_args, **_kwargs: iter(
            [
                _raw_message({"s": "BTCUSDT", "E": 1710000000000, "p": "abc", "q": "1"}),
                _raw_message({"s": "BTCUSDT", "E": 1710000000000, "p": "100", "q": "1"}),
            ]
        ),
        error_sink=error_sink,
    )
    sink = RecordingSink()
    buffer = io.StringIO()
    logger = get_logger(name="test.ingest.validation.counted", level="INFO", stream=buffer)

    events = pipeline.collect_events(mode="live", cfg=cfg, duration_s=0, source=source, sink=sink, logger=logger)

    assert len(events) == 1
    assert source.stats.rejected_payloads == 1
    summary = next(json.loads(line) for line in buffer.getvalue().splitlines() if json.loads(line)["message"] == "ingestion summary")
    assert summary["rejected_payloads"] == 1
    assert summary["events_invalid"] == 1
    assert summary["events_dedup_skipped"] == 0


def test_invalid_payload_metrics_are_separate_from_dedup_metrics():
    cfg = mock.Mock(env="dev", ws_base="wss://x", rest_base="https://x", symbols=["BTCUSDT"], data_dir=".", log_level="INFO")
    error_sink = RecordingErrorSink()
    source = BinanceSource(
        cfg,
        ws_stream=lambda *_args, **_kwargs: iter(
            [
                _raw_message({"s": "BTCUSDT", "E": 1710000000000, "p": "oops", "q": "1"}),
                _raw_message({"s": "BTCUSDT", "E": 1710000000000, "p": "100", "q": "1"}),
                _raw_message({"s": "BTCUSDT", "E": 1710000000000, "p": "100", "q": "1"}),
            ]
        ),
        error_sink=error_sink,
    )
    sink = RecordingSink()
    buffer = io.StringIO()
    logger = get_logger(name="test.ingest.validation.vs_dedup", level="INFO", stream=buffer)

    events = pipeline.collect_events(
        mode="live",
        cfg=cfg,
        duration_s=0,
        source=source,
        sink=sink,
        logger=logger,
        dedup_enabled=True,
    )

    assert len(events) == 1
    summary = next(json.loads(line) for line in buffer.getvalue().splitlines() if json.loads(line)["message"] == "ingestion summary")
    assert summary["source_events_in"] == 3
    assert summary["events_valid"] == 2
    assert summary["events_invalid"] == 1
    assert summary["events_dedup_skipped"] == 1


def test_unknown_event_type_does_not_kill_stream():
    cfg = mock.Mock(env="dev", ws_base="wss://x", rest_base="https://x", symbols=["BTCUSDT"], data_dir=".", log_level="INFO")
    error_sink = RecordingErrorSink()
    source = BinanceSource(
        cfg,
        ws_stream=lambda *_args, **_kwargs: iter(
            [
                _raw_message({"e": "foo", "s": "BTCUSDT"}, stream="btcusdt@foo"),
                _raw_message({"s": "BTCUSDT", "E": 1710000000000, "p": "100", "q": "1"}),
            ]
        ),
        error_sink=error_sink,
    )
    sink = RecordingSink()

    events = pipeline.collect_events(mode="live", cfg=cfg, duration_s=0, source=source, sink=sink)

    assert len(events) == 1
    assert len(error_sink.records) == 1
    assert error_sink.records[0][1].category == "parse"


def test_error_sink_failure_is_contained():
    cfg = mock.Mock(env="dev", ws_base="wss://x", rest_base="https://x", symbols=["BTCUSDT"], data_dir=".", log_level="INFO")
    error_sink = FailingErrorSink()
    source = BinanceSource(
        cfg,
        ws_stream=lambda *_args, **_kwargs: iter(
            [
                _raw_message({"s": "BTCUSDT", "E": 1710000000000, "p": "abc", "q": "1"}),
                _raw_message({"s": "BTCUSDT", "E": 1710000000000, "p": "100", "q": "1"}),
            ]
        ),
        error_sink=error_sink,
    )
    sink = RecordingSink()

    events = pipeline.collect_events(mode="live", cfg=cfg, duration_s=0, source=source, sink=sink)

    assert len(events) == 1
    assert source.stats.rejected_payloads == 1
    assert source.stats.error_sink_failures == 1
    assert error_sink.calls == 1
