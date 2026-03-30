import io
import json
from datetime import datetime, timezone
from unittest import mock

import pytest

from app.common.dto import MarketEvent
from app.ingestion import pipeline
from app.ingestion.errors import IngestionError
from app.ingestion.sources import StaticSource
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


class BrokenSource:
    def __init__(self):
        self.calls = 0

    def stream(self, end_time=None):
        del end_time
        self.calls += 1
        raise IngestionError("source", "transient", "temporary source failure")

    def snapshot(self):
        return None


class FailingSink:
    def add(self, event):
        del event
        raise OSError("disk full")

    def close(self):
        return None


def test_source_error_retries_then_fails_under_fail_fast():
    cfg = mock.Mock(env="dev", ws_base="wss://x", rest_base="https://x", symbols=["BTCUSDT"], data_dir=".", log_level="INFO")
    source = BrokenSource()

    with pytest.raises(IngestionError) as exc_info:
        pipeline.collect_events(
            mode="live",
            cfg=cfg,
            duration_s=0,
            logger=mock.Mock(),
            source=source,
            sink=mock.Mock(add=lambda event: None, close=lambda: None),
            error_policy="fail_fast",
        )

    assert exc_info.value.category == "source"
    assert exc_info.value.severity == "transient"
    assert source.calls == 2


def test_sink_error_is_not_masked_as_source_error():
    cfg = mock.Mock(env="dev", ws_base="wss://x", rest_base="https://x", symbols=["BTCUSDT"], data_dir=".", log_level="INFO")
    source = StaticSource(events=[_ev(0, 100)])

    with pytest.raises(IngestionError) as exc_info:
        pipeline.collect_events(
            mode="live",
            cfg=cfg,
            duration_s=0,
            logger=mock.Mock(),
            source=source,
            sink=FailingSink(),
            error_policy="fail_fast",
        )

    assert exc_info.value.category == "sink"
    assert exc_info.value.severity == "permanent"


def test_allow_fallback_requires_explicit_flag():
    cfg = mock.Mock(env="dev", ws_base="wss://x", rest_base="https://x", symbols=["BTCUSDT"], data_dir=".", log_level="INFO")
    source = BrokenSource()
    fallback_events = [_ev(0, 100), _ev(60, 101)]
    buffer = io.StringIO()
    logger = get_logger(name="test.ingest.error_policy.fallback", level="INFO", stream=buffer)

    with pytest.raises(IngestionError):
        pipeline.collect_events(
            mode="live",
            cfg=cfg,
            duration_s=0,
            logger=logger,
            source=source,
            sink=mock.Mock(add=lambda event: None, close=lambda: None),
        )

    source = BrokenSource()
    with mock.patch.object(pipeline, "_synthetic_events", return_value=fallback_events):
        out = pipeline.collect_events(
            mode="live",
            cfg=cfg,
            duration_s=0,
            logger=logger,
            source=source,
            sink=mock.Mock(add=lambda event: None, close=lambda: None),
            error_policy="allow_fallback",
        )

    assert out == fallback_events
    summary = next(record for record in _json_lines(buffer) if record["message"] == "ingestion summary" and record["result"] == "fallback")
    assert summary["error_policy"] == "allow_fallback"
    assert summary["error_category"] == "source"
