import json
from datetime import datetime, timezone
from unittest import mock

import pytest

from app.ingestion import pipeline
from app.ingestion.errors import IngestionError
from app.ingestion.sources import BinanceSource
from app.marketdata.raw_sink import JsonlRawSink, RawRecord
from app.observability.logger import clear_trace_id, set_trace_id


def _read_jsonl(path):
    with path.open("r", encoding="utf-8") as handle:
        return [json.loads(line) for line in handle if line.strip()]


def test_jsonl_raw_sink_writes_append_only_layout(tmp_path):
    sink = JsonlRawSink(tmp_path / "raw", env="test")
    record = RawRecord(
        payload={"foo": "bar"},
        venue="binance",
        stream_type="trade",
        symbol="btcusdt",
        exchange_ts=datetime(2024, 1, 1, tzinfo=timezone.utc),
        receive_ts=datetime(2024, 1, 1, 0, 0, 1, tzinfo=timezone.utc),
        trace_id="trace-1",
    )

    path = sink.write(record)

    assert path == (
        tmp_path
        / "raw"
        / "env=test"
        / "venue=BINANCE"
        / "stream_type=trade"
        / "symbol=BTCUSDT"
        / "date=2024-01-01"
        / "events.jsonl"
    )
    rows = _read_jsonl(path)
    assert rows[0]["payload"] == {"foo": "bar"}
    assert rows[0]["trace_id"] == "trace-1"
    assert rows[0]["ingestion_seq"] == 1


def test_jsonl_raw_sink_assigns_monotonic_ingestion_seq_per_run(tmp_path):
    sink = JsonlRawSink(tmp_path / "raw", env="test")
    base = datetime(2024, 1, 1, tzinfo=timezone.utc)

    first_path = sink.write(
        RawRecord(
            payload={"foo": 1},
            venue="BINANCE",
            stream_type="trade",
            symbol="BTCUSDT",
            exchange_ts=base,
            receive_ts=base,
        )
    )
    second_path = sink.write(
        RawRecord(
            payload={"foo": 2},
            venue="BINANCE",
            stream_type="kline",
            symbol="ETHUSDT",
            exchange_ts=base,
            receive_ts=base,
        )
    )

    first_rows = _read_jsonl(first_path)
    second_rows = _read_jsonl(second_path)

    assert first_rows[0]["ingestion_seq"] == 1
    assert second_rows[0]["ingestion_seq"] == 2


def test_valid_stream_message_is_persisted_to_raw_landing(tmp_path):
    cfg = mock.Mock(
        env="test",
        ws_base="wss://x",
        rest_base="https://x",
        symbols=["BTCUSDT"],
        data_dir=tmp_path,
        log_level="INFO",
    )
    raw_sink = JsonlRawSink(tmp_path / "raw", env="test")
    source = BinanceSource(
        cfg,
        ws_stream=lambda *_args, **_kwargs: iter(
            ['{"stream":"btcusdt@trade","data":{"s":"BTCUSDT","E":1704067200000,"p":"100","q":"1","t":7}}']
        ),
        raw_sink=raw_sink,
    )

    set_trace_id("trace-raw-1")
    try:
        events = list(source.stream(end_time=1.0))
    finally:
        clear_trace_id()

    assert len(events) == 1
    path = raw_sink.path_for(
        RawRecord(
            payload={},
            venue="BINANCE",
            stream_type="trade",
            symbol="BTCUSDT",
            exchange_ts=datetime(2024, 1, 1, tzinfo=timezone.utc),
            receive_ts=events[0].receive_ts,
        )
    )
    rows = _read_jsonl(path)
    assert len(rows) == 1
    assert rows[0]["symbol"] == "BTCUSDT"
    assert rows[0]["stream_type"] == "trade"
    assert rows[0]["trace_id"] == "trace-raw-1"
    assert rows[0]["ingestion_seq"] == 1
    assert rows[0]["payload"]["data"]["t"] == 7


def test_sink_failure_preserves_raw_record(tmp_path):
    cfg = mock.Mock(
        env="test",
        ws_base="wss://x",
        rest_base="https://x",
        symbols=["BTCUSDT"],
        data_dir=tmp_path,
        log_level="INFO",
    )
    raw_sink = JsonlRawSink(tmp_path / "raw", env="test")
    source = BinanceSource(
        cfg,
        ws_stream=lambda *_args, **_kwargs: iter(
            ['{"stream":"btcusdt@trade","data":{"s":"BTCUSDT","E":1704067200000,"p":"100","q":"1","t":8}}']
        ),
        raw_sink=raw_sink,
    )

    class FailingSink:
        def add(self, event):
            del event
            raise OSError("normalized sink failed")

        def close(self):
            return None

    with pytest.raises(IngestionError, match="normalized sink failed"):
        pipeline.collect_events(
            mode="live",
            cfg=cfg,
            duration_s=0,
            source=source,
            sink=FailingSink(),
            snapshot_enabled=False,
        )

    path = raw_sink.path_for(
        RawRecord(
            payload={},
            venue="BINANCE",
            stream_type="trade",
            symbol="BTCUSDT",
            exchange_ts=datetime(2024, 1, 1, tzinfo=timezone.utc),
            receive_ts=datetime(2024, 1, 1, tzinfo=timezone.utc),
        )
    )
    rows = _read_jsonl(path)
    assert len(rows) == 1
    assert rows[0]["payload"]["data"]["t"] == 8
