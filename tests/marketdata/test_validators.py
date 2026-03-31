from datetime import datetime, timedelta, timezone

import pytest

from app.marketdata.models import BarEvent, BookEvent, TradeEvent
from app.marketdata.validators import (
    validate_bar_event,
    validate_book_event,
    validate_kline_payload,
    validate_trade_event,
    validate_trade_payload,
)


def test_validate_trade_payload_rejects_non_finite_numeric_fields():
    payload = {"s": "BTCUSDT", "E": 1710000000000, "p": "nan", "q": "1"}

    with pytest.raises(ValueError, match="price must be finite"):
        validate_trade_payload(payload)


def test_validate_trade_event_rejects_absurd_future_timestamp():
    now = datetime(2024, 1, 1, tzinfo=timezone.utc)
    event = TradeEvent(
        symbol="BTCUSDT",
        exchange_ts=now + timedelta(minutes=10),
        venue="BINANCE",
        price=100.0,
        size=1.0,
    )

    with pytest.raises(ValueError, match="exchange_ts is too far in the future"):
        validate_trade_event(event, now=now)


def test_validate_bar_event_rejects_close_ts_before_open_ts():
    event = BarEvent(
        symbol="BTCUSDT",
        exchange_ts=datetime(2024, 1, 1, 0, 1, tzinfo=timezone.utc),
        venue="BINANCE",
        open=100.0,
        high=101.0,
        low=99.0,
        close=100.0,
        volume=1.0,
        open_ts=datetime(2024, 1, 1, 0, 1, tzinfo=timezone.utc),
        close_ts=datetime(2024, 1, 1, 0, 0, tzinfo=timezone.utc),
    )

    with pytest.raises(ValueError, match="close_ts cannot be earlier than open_ts"):
        validate_bar_event(event)


def test_validate_kline_payload_rejects_inconsistent_ohlc():
    payload = {
        "s": "BTCUSDT",
        "E": 1710000000000,
        "k": {
            "t": 1710000000000,
            "T": 1710000060000,
            "o": "100",
            "h": "101",
            "l": "99",
            "c": "120",
            "q": "1",
        },
    }

    with pytest.raises(ValueError, match="close must be within"):
        validate_kline_payload(payload)


def test_validate_book_event_rejects_process_ts_before_receive_ts():
    event = BookEvent(
        symbol="BTCUSDT",
        exchange_ts=datetime(2024, 1, 1, tzinfo=timezone.utc),
        receive_ts=datetime(2024, 1, 1, 0, 0, 1, tzinfo=timezone.utc),
        process_ts=datetime(2024, 1, 1, 0, 0, 0, tzinfo=timezone.utc),
        venue="BINANCE",
        bid_price=100.0,
        bid_size=1.0,
        ask_price=101.0,
        ask_size=1.0,
    )

    with pytest.raises(ValueError, match="process_ts cannot be earlier than receive_ts"):
        validate_book_event(event)
