import pytest
from datetime import datetime, timezone

from app.common.dto import MarketEvent
from app.ingestion.client import (
    build_streams,
    build_ws_url,
    normalize_kline,
    normalize_kline_typed,
    normalize_trade,
    normalize_trade_typed,
    parse_message,
    parse_typed_message,
    register_normalizer,
    register_stream_builder,
)


def test_normalize_trade_symbol_and_utc():
    payload = {"s": "ethusdt", "E": 1710000000000, "p": "1000.5", "q": "2.0"}
    ev = normalize_trade(payload)
    assert ev.symbol == "ETHUSDT"
    assert ev.event_ts.tzinfo is not None
    assert ev.source == "trade"


def test_negative_price_rejected():
    payload = {"s": "BTCUSDT", "E": 1710000000000, "p": "-1", "q": "1.0"}
    with pytest.raises(ValueError):
        normalize_trade(payload)


def test_build_ws_url_formats_streams():
    url = build_ws_url("wss://stream.binance.com:9443", ["BTCUSDT"])
    assert "btcusdt@aggTrade" in url and "btcusdt@kline_1m" in url


def test_parse_message_dispatches_kline():
    msg = {
        "stream": "btcusdt@kline_1m",
        "data": {
            "e": "kline",
            "E": 1710000000000,
            "s": "BTCUSDT",
            "k": {"c": "1200", "q": "10"},
        },
    }
    ev = parse_message(json_dumps(msg))
    assert ev.source == "kline"


def json_dumps(obj):
    import json
    return json.dumps(obj)


def test_parse_message_trade():
    msg = {
        "stream": "btcusdt@aggTrade",
        "data": {"s": "BTCUSDT", "E": 1710000000000, "p": "100", "q": "1"},
    }
    ev = parse_message(json_dumps(msg))
    assert ev.source == "trade"
    assert ev.symbol == "BTCUSDT"


def test_parse_typed_trade_message_captures_exchange_receive_and_process_timestamps():
    receive_ts = datetime(2024, 1, 1, 0, 0, 2, tzinfo=timezone.utc)
    process_ts = datetime(2024, 1, 1, 0, 0, 3, tzinfo=timezone.utc)
    msg = {
        "stream": "btcusdt@aggTrade",
        "data": {"e": "aggTrade", "s": "BTCUSDT", "E": 1704067200000, "p": "100", "q": "1", "a": 42, "f": 40, "l": 42},
    }

    ev = parse_typed_message(json_dumps(msg), receive_ts=receive_ts, process_ts=process_ts)

    assert ev.exchange_ts == datetime(2024, 1, 1, 0, 0, tzinfo=timezone.utc)
    assert ev.provider_ts is None
    assert ev.receive_ts == receive_ts
    assert ev.process_ts == process_ts
    assert ev.exchange_ts <= ev.receive_ts <= ev.process_ts


def test_parse_typed_kline_message_maps_exchange_ts_from_close_time():
    receive_ts = datetime(2024, 1, 1, 0, 2, tzinfo=timezone.utc)
    process_ts = datetime(2024, 1, 1, 0, 2, 1, tzinfo=timezone.utc)
    msg = {
        "stream": "btcusdt@kline_1m",
        "data": {
            "e": "kline",
            "E": 1704067255000,
            "s": "BTCUSDT",
            "k": {
                "t": 1704067200000,
                "T": 1704067250000,
                "o": "100",
                "h": "101",
                "l": "99",
                "c": "100.5",
                "q": "10",
                "i": "1m",
            },
        },
    }

    ev = parse_typed_message(json_dumps(msg), receive_ts=receive_ts, process_ts=process_ts)

    assert ev.exchange_ts == datetime(2024, 1, 1, 0, 0, 50, tzinfo=timezone.utc)
    assert ev.provider_ts == datetime(2024, 1, 1, 0, 0, 55, tzinfo=timezone.utc)
    assert ev.open_ts == datetime(2024, 1, 1, 0, 0, tzinfo=timezone.utc)
    assert ev.close_ts == datetime(2024, 1, 1, 0, 0, 50, tzinfo=timezone.utc)
    assert ev.exchange_ts <= ev.receive_ts <= ev.process_ts


def test_parse_message_unknown_stream_raises():
    msg = {"stream": "unknown", "data": {"foo": "bar"}}
    with pytest.raises(KeyError):
        parse_message(json_dumps(msg))


def test_normalize_kline_negative_price():
    payload = {"s": "BTCUSDT", "E": 1710000000000, "k": {"c": "-1", "q": "1"}}
    with pytest.raises(ValueError):
        normalize_kline(payload)


def test_build_ws_url_dedupes_and_orders():
    url = build_ws_url("wss://example/stream", ["ETHUSDT", "ethusdt", "BTCUSDT"])
    assert url.count("ethusdt@aggTrade") == 1
    assert url.index("ethusdt@aggTrade") < url.index("btcusdt@aggTrade")


def test_register_normalizer_used_by_parse_message():
    def normalize_foo(payload):
        return MarketEvent(
            symbol="FOO",
            event_ts=datetime.fromtimestamp(1700000000, tz=timezone.utc),
            price=1.0,
            size=1.0,
            source="foo",
        )

    register_normalizer("foo", normalize_foo)
    msg = {"stream": "foo@bar", "data": {"e": "foo"}}
    ev = parse_message(json_dumps(msg))
    assert ev.source == "foo"


def test_register_stream_builder_generates_custom_stream_and_parse_message():
    def build_foo(symbol: str) -> str:
        return f"{symbol}@foo"

    def normalize_foo(payload):
        return MarketEvent(
            symbol="FOO",
            event_ts=datetime.fromtimestamp(1700000000, tz=timezone.utc),
            price=1.0,
            size=1.0,
            source="foo",
        )

    register_stream_builder("foo", build_foo)
    register_normalizer("foo", normalize_foo)

    streams = build_streams(["BTCUSDT"], stream_types=("foo",))
    url = build_ws_url("wss://example/stream", ["BTCUSDT"], stream_types=("foo",))
    event = parse_message(json_dumps({"stream": "btcusdt@foo", "data": {"e": "foo"}}))

    assert streams == ["btcusdt@foo"]
    assert "btcusdt@foo" in url
    assert event.source == "foo"


def test_default_streams_unchanged_without_custom_types():
    streams = build_streams(["BTCUSDT"])
    assert streams == ["btcusdt@aggTrade", "btcusdt@kline_1m"]


def test_normalize_trade_typed_sets_process_ts_when_not_provided():
    payload = {"s": "BTCUSDT", "E": 1710000000000, "p": "100", "q": "1", "t": 1}

    ev = normalize_trade_typed(payload)

    assert ev.exchange_ts.tzinfo is not None
    assert ev.provider_ts is None
    assert ev.process_ts is not None
    assert ev.metadata["base_asset"] == "BTC"
    assert ev.metadata["quote_asset"] == "USDT"
    assert ev.metadata["contract_type"] == "spot"


def test_normalize_kline_typed_sets_receive_ts_when_provided():
    receive_ts = datetime(2024, 1, 1, 0, 2, tzinfo=timezone.utc)
    payload = {
        "s": "BTCUSDT",
        "E": 1710000060000,
        "k": {
            "t": 1710000000000,
            "T": 1710000060000,
            "o": "100",
            "h": "101",
            "l": "99",
            "c": "100",
            "q": "1",
        },
    }

    ev = normalize_kline_typed(payload, receive_ts=receive_ts)

    assert ev.receive_ts == receive_ts
    assert ev.provider_ts is None
    assert ev.metadata["base_asset"] == "BTC"
    assert ev.metadata["quote_asset"] == "USDT"


def test_normalize_trade_typed_rejects_unknown_instrument_symbol():
    payload = {"s": "BTCUNKNOWN", "E": 1710000000000, "p": "100", "q": "1", "t": 1}

    with pytest.raises(KeyError, match="unsupported instrument"):
        normalize_trade_typed(payload)
