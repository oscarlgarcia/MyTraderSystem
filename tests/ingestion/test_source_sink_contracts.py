import io
import json
from datetime import datetime, timezone
from unittest import mock

from app.common.dto import MarketEvent
from app.ingestion import pipeline
from app.ingestion.sources import BinanceSource, StaticSource
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


class RecordingSink:
    def __init__(self):
        self.items = []
        self.closed = False

    def add(self, event):
        if isinstance(event, list):
            self.items.extend(event)
            return
        self.items.append(event)

    def close(self):
        self.closed = True


def test_pipeline_runs_against_source_and_sink_contracts():
    cfg = mock.Mock(env="dev", ws_base="wss://x", rest_base="https://x", symbols=["BTCUSDT"], data_dir=".", log_level="INFO")
    events = [_ev(0, 100), _ev(60, 101)]
    source = StaticSource(events=events)
    sink = RecordingSink()

    out = pipeline.collect_events(
        mode="live",
        cfg=cfg,
        max_events=10,
        duration_s=0,
        logger=mock.Mock(),
        snapshot_enabled=False,
        source=source,
        sink=sink,
    )

    assert out == events
    assert sink.items == events
    assert sink.closed is True


def test_binance_source_preserves_current_behavior():
    cfg = mock.Mock(env="dev", ws_base="wss://stream.binance.com:9443", rest_base="https://api.binance.com", symbols=["BTCUSDT"], data_dir=".", log_level="INFO")
    seen = {}
    event = _ev(0, 100)

    def fake_ws_stream(url: str, end_time=None):
        seen["url"] = url
        seen["end_time"] = end_time
        yield event

    source = BinanceSource(cfg, ws_stream=fake_ws_stream)
    out = list(source.stream(end_time=123.0))

    assert out == [event]
    assert "btcusdt@trade" in seen["url"]
    assert "btcusdt@kline_1m" in seen["url"]
    assert seen["end_time"] == 123.0


def test_snapshot_optional_on_source():
    cfg = mock.Mock(env="dev", ws_base="wss://x", rest_base="https://x", symbols=["BTCUSDT"], data_dir=".", log_level="INFO")
    events = [_ev(0, 100), _ev(10, 101)]
    source = StaticSource(events=events, snapshot_events=None)
    sink = RecordingSink()
    buffer = io.StringIO()
    logger = get_logger(name="test.ingest.source.optional_snapshot", level="INFO", stream=buffer)

    out = pipeline.collect_events(
        mode="live",
        cfg=cfg,
        max_events=10,
        duration_s=0,
        logger=logger,
        source=source,
        sink=sink,
        snapshot_enabled=True,
    )

    assert out == events
    summary = next(record for record in _json_lines(buffer) if record["message"] == "ingestion summary")
    assert summary["events_in"] == 2
    assert summary["events_out"] == 2
