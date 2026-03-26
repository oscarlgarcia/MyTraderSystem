import pytest

from app.ingestion.client import build_ws_url, normalize_kline, normalize_trade, parse_message


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
    assert "btcusdt@trade" in url and "btcusdt@kline_1m" in url


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
