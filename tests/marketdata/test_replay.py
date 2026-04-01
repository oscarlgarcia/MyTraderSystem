from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest import mock

import pytest

from app.marketdata import NORMALIZER_VERSION
from app.marketdata.models import TradeEvent, ensure_legacy_market_event
from app.marketdata.raw_sink import JsonlRawSink, RawRecord
from app.marketdata.replay import ReplaySource, normalize_replay_record, read_raw_entries


def _trade_envelope(symbol: str, event_ms: int, trade_id: int, price: str) -> dict:
    return {
        "stream": f"{symbol.lower()}@trade",
        "data": {
            "s": symbol,
            "E": event_ms,
            "p": price,
            "q": "1",
            "t": trade_id,
        },
    }


def _write_raw_trade(
    sink: JsonlRawSink,
    *,
    symbol: str,
    event_ts: datetime,
    receive_ts: datetime,
    trade_id: int,
    price: str,
) -> Path:
    return sink.write(
        RawRecord(
            payload=_trade_envelope(symbol, int(event_ts.timestamp() * 1000), trade_id, price),
            venue="BINANCE",
            stream_type="trade",
            symbol=symbol,
            exchange_ts=event_ts,
            receive_ts=receive_ts,
            trace_id="trace-replay",
            source_id=str(trade_id),
        )
    )


def test_replay_preserves_raw_order_from_append_only_file(tmp_path):
    sink = JsonlRawSink(tmp_path / "raw", env="test")
    base = datetime(2024, 1, 1, tzinfo=timezone.utc)
    _write_raw_trade(sink, symbol="BTCUSDT", event_ts=base, receive_ts=base + timedelta(seconds=1), trade_id=1, price="100")
    _write_raw_trade(sink, symbol="BTCUSDT", event_ts=base + timedelta(seconds=2), receive_ts=base + timedelta(seconds=2), trade_id=2, price="101")
    _write_raw_trade(sink, symbol="BTCUSDT", event_ts=base + timedelta(seconds=1), receive_ts=base + timedelta(seconds=3), trade_id=3, price="102")

    source = ReplaySource(base_dir=tmp_path / "raw", env="test", symbol="BTCUSDT", stream_types=("trade",))
    out = list(source.stream())

    assert [event.trade_id for event in out] == ["1", "2", "3"]


def test_replay_can_filter_by_window_stream_and_symbol(tmp_path):
    sink = JsonlRawSink(tmp_path / "raw", env="test")
    base = datetime(2024, 1, 1, tzinfo=timezone.utc)
    _write_raw_trade(sink, symbol="BTCUSDT", event_ts=base, receive_ts=base + timedelta(seconds=1), trade_id=1, price="100")
    _write_raw_trade(sink, symbol="ETHUSDT", event_ts=base + timedelta(minutes=1), receive_ts=base + timedelta(minutes=1, seconds=1), trade_id=2, price="200")
    sink.write(
        RawRecord(
            payload={
                "s": "BTCUSDT",
                "E": int((base + timedelta(minutes=2)).timestamp() * 1000),
                "k": {
                    "t": int((base + timedelta(minutes=1)).timestamp() * 1000),
                    "T": int((base + timedelta(minutes=2)).timestamp() * 1000),
                    "o": "100",
                    "h": "101",
                    "l": "99",
                    "c": "100",
                    "q": "10",
                },
            },
            venue="BINANCE",
            stream_type="kline",
            symbol="BTCUSDT",
            exchange_ts=base + timedelta(minutes=2),
            receive_ts=base + timedelta(minutes=2, seconds=1),
        )
    )

    source = ReplaySource(
        base_dir=tmp_path / "raw",
        env="test",
        symbol="BTCUSDT",
        stream_types=("trade",),
        start_ts=base - timedelta(seconds=1),
        end_ts=base + timedelta(seconds=30),
    )
    out = list(source.stream())

    assert len(out) == 1
    assert isinstance(out[0], TradeEvent)
    assert out[0].symbol == "BTCUSDT"


def test_raw_plus_normalizer_version_produces_deterministic_normalized_output(tmp_path):
    sink = JsonlRawSink(tmp_path / "raw", env="test")
    base = datetime(2024, 1, 1, tzinfo=timezone.utc)
    path = _write_raw_trade(
        sink,
        symbol="BTCUSDT",
        event_ts=base,
        receive_ts=base + timedelta(seconds=1),
        trade_id=11,
        price="100",
    )

    entry = read_raw_entries(tmp_path / "raw", "test")[0]
    first = ensure_legacy_market_event(normalize_replay_record(entry.record, normalizer_version=NORMALIZER_VERSION))
    second = ensure_legacy_market_event(normalize_replay_record(entry.record, normalizer_version=NORMALIZER_VERSION))

    assert path.exists()
    assert entry.record.ingestion_seq == 1
    assert first == second
    assert first.metadata["normalizer_version"] == NORMALIZER_VERSION


def test_replay_rejects_unsupported_normalizer_version(tmp_path):
    sink = JsonlRawSink(tmp_path / "raw", env="test")
    base = datetime(2024, 1, 1, tzinfo=timezone.utc)
    _write_raw_trade(
        sink,
        symbol="BTCUSDT",
        event_ts=base,
        receive_ts=base + timedelta(seconds=1),
        trade_id=99,
        price="100",
    )

    entry = read_raw_entries(tmp_path / "raw", "test")[0]

    with pytest.raises(ValueError, match="unsupported normalizer_version"):
        normalize_replay_record(entry.record, normalizer_version="v999")


def test_step_by_step_replay_uses_sleeper_between_events(tmp_path):
    sink = JsonlRawSink(tmp_path / "raw", env="test")
    base = datetime(2024, 1, 1, tzinfo=timezone.utc)
    _write_raw_trade(sink, symbol="BTCUSDT", event_ts=base, receive_ts=base + timedelta(seconds=1), trade_id=1, price="100")
    _write_raw_trade(sink, symbol="BTCUSDT", event_ts=base + timedelta(seconds=1), receive_ts=base + timedelta(seconds=2), trade_id=2, price="101")
    sleeper = mock.Mock()

    source = ReplaySource(
        base_dir=tmp_path / "raw",
        env="test",
        symbol="BTCUSDT",
        stream_types=("trade",),
        speed="step-by-step",
        step_seconds=0.5,
        sleeper=sleeper,
    )

    list(source.stream())

    sleeper.assert_called_once_with(0.5)
