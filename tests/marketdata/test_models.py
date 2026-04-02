from datetime import datetime, timedelta, timezone
from unittest import mock

import pytest

from app.common.dto import MarketEvent
from app.ingestion import pipeline
from app.ingestion.sources import StaticSource
from app.marketdata.models import (
    BarEvent,
    BookEvent,
    EXPERIMENTAL_MARKETDATA_SOURCES,
    SUPPORTED_MARKETDATA_SOURCES,
    TradeEvent,
    ensure_legacy_market_event,
    is_supported_marketdata_source,
    legacy_market_event_to_bar,
    legacy_market_event_to_trade,
    typed_event_to_legacy,
)


def test_trade_event_construction_and_validation():
    event = TradeEvent(
        symbol="btcusdt",
        exchange_ts=datetime(2024, 1, 1, tzinfo=timezone.utc),
        receive_ts=datetime(2024, 1, 1, tzinfo=timezone.utc),
        process_ts=datetime(2024, 1, 1, tzinfo=timezone.utc),
        venue="binance",
        price=100.0,
        size=1.5,
        trade_id="123",
        side="buy",
    )

    assert event.symbol == "BTCUSDT"
    assert event.venue == "BINANCE"
    assert event.event_type == "trade"
    assert event.source == "trade"
    assert event.event_ts == event.exchange_ts


def test_bar_event_construction_and_validation():
    event = BarEvent(
        symbol="ethusdt",
        exchange_ts=datetime(2024, 1, 1, 0, 1, tzinfo=timezone.utc),
        venue="binance",
        open=100.0,
        high=105.0,
        low=99.0,
        close=102.0,
        volume=10.0,
        interval="1m",
        open_ts=datetime(2024, 1, 1, 0, 0, tzinfo=timezone.utc),
        close_ts=datetime(2024, 1, 1, 0, 1, tzinfo=timezone.utc),
    )

    assert event.symbol == "ETHUSDT"
    assert event.event_type == "bar"
    assert event.source == "kline"
    assert event.price == 102.0
    assert event.size == 10.0
    assert event.volume_kind == "quote"


def test_book_event_construction_and_validation():
    event = BookEvent(
        symbol="BTCUSDT",
        exchange_ts=datetime(2024, 1, 1, tzinfo=timezone.utc),
        venue="binance",
        bid_price=100.0,
        bid_size=1.0,
        ask_price=101.0,
        ask_size=2.0,
        sequence_id="456",
    )

    assert event.event_type == "book"
    assert event.source == "book"
    assert event.price == 100.5
    assert event.size == 3.0


def test_book_event_is_explicitly_out_of_supported_ingestion_scope():
    assert "trade" in SUPPORTED_MARKETDATA_SOURCES
    assert "kline" in SUPPORTED_MARKETDATA_SOURCES
    assert "book" in EXPERIMENTAL_MARKETDATA_SOURCES
    assert is_supported_marketdata_source("book") is False


def test_bar_event_rejects_inconsistent_ohlc():
    with pytest.raises(ValueError, match="close must be within"):
        BarEvent(
            symbol="BTCUSDT",
            exchange_ts=datetime(2024, 1, 1, tzinfo=timezone.utc),
            open=100.0,
            high=101.0,
            low=99.0,
            close=120.0,
            volume=1.0,
        )


def test_bar_event_rejects_unknown_volume_kind():
    with pytest.raises(ValueError, match="volume_kind"):
        BarEvent(
            symbol="BTCUSDT",
            exchange_ts=datetime(2024, 1, 1, tzinfo=timezone.utc),
            open=100.0,
            high=101.0,
            low=99.0,
            close=100.0,
            volume=1.0,
            volume_kind="ticks",  # type: ignore[arg-type]
        )


def test_legacy_market_event_to_trade_adapter():
    legacy = MarketEvent(
        symbol="BTCUSDT",
        event_ts=datetime(2024, 1, 1, tzinfo=timezone.utc),
        price=100.0,
        size=1.0,
        source="trade",
    )

    typed = legacy_market_event_to_trade(legacy, trade_id="abc", side="sell")

    assert typed.price == legacy.price
    assert typed.size == legacy.size
    assert typed.trade_id == "abc"
    assert typed.side == "sell"


def test_legacy_market_event_to_bar_adapter():
    legacy = MarketEvent(
        symbol="BTCUSDT",
        event_ts=datetime(2024, 1, 1, 0, 1, tzinfo=timezone.utc),
        price=100.0,
        size=20.0,
        source="kline",
    )

    typed = legacy_market_event_to_bar(legacy, interval="1m")

    assert typed.open == 100.0
    assert typed.high == 100.0
    assert typed.low == 100.0
    assert typed.close == 100.0
    assert typed.volume == 20.0
    assert typed.volume_kind == "quote"


def test_typed_event_to_legacy_trade_roundtrip():
    receive_ts = datetime(2024, 1, 1, 0, 0, 1, tzinfo=timezone.utc)
    process_ts = datetime(2024, 1, 1, 0, 0, 2, tzinfo=timezone.utc)
    trade = TradeEvent(
        symbol="BTCUSDT",
        exchange_ts=datetime(2024, 1, 1, tzinfo=timezone.utc),
        receive_ts=receive_ts,
        process_ts=process_ts,
        venue="BINANCE",
        price=100.0,
        size=1.0,
        trade_id="abc",
    )

    legacy = typed_event_to_legacy(trade)

    assert legacy.source == "trade"
    assert legacy.price == 100.0
    assert legacy.metadata["trade_id"] == "abc"
    assert legacy.metadata["receive_ts"] == receive_ts.isoformat()
    assert legacy.metadata["process_ts"] == process_ts.isoformat()

    roundtrip = legacy_market_event_to_trade(legacy)
    assert roundtrip.exchange_ts == trade.exchange_ts
    assert roundtrip.receive_ts == receive_ts
    assert roundtrip.process_ts == process_ts


def test_typed_event_to_legacy_bar_roundtrip():
    receive_ts = datetime(2024, 1, 1, 0, 1, 1, tzinfo=timezone.utc)
    process_ts = datetime(2024, 1, 1, 0, 1, 2, tzinfo=timezone.utc)
    bar = BarEvent(
        symbol="BTCUSDT",
        exchange_ts=datetime(2024, 1, 1, 0, 1, tzinfo=timezone.utc),
        receive_ts=receive_ts,
        process_ts=process_ts,
        venue="BINANCE",
        open=100.0,
        high=105.0,
        low=99.0,
        close=103.0,
        volume=12.0,
        interval="1m",
        close_ts=datetime(2024, 1, 1, 0, 1, tzinfo=timezone.utc),
    )

    legacy = ensure_legacy_market_event(bar)

    assert legacy.source == "kline"
    assert legacy.price == 103.0
    assert legacy.size == 12.0
    assert legacy.metadata["interval"] == "1m"
    assert legacy.metadata["volume_kind"] == "quote"
    assert legacy.metadata["receive_ts"] == receive_ts.isoformat()
    assert legacy.metadata["process_ts"] == process_ts.isoformat()

    roundtrip = legacy_market_event_to_bar(legacy)
    assert roundtrip.exchange_ts == bar.exchange_ts
    assert roundtrip.receive_ts == receive_ts
    assert roundtrip.process_ts == process_ts
    assert roundtrip.volume_kind == "quote"


def test_pipeline_accepts_typed_trade_events_without_legacy_coercion():
    typed_events = [
        TradeEvent(
            symbol="BTCUSDT",
            exchange_ts=datetime(2024, 1, 1, tzinfo=timezone.utc),
            venue="BINANCE",
            price=100.0,
            size=1.0,
            trade_id="1",
        ),
        TradeEvent(
            symbol="BTCUSDT",
            exchange_ts=datetime(2024, 1, 1, tzinfo=timezone.utc) + timedelta(seconds=1),
            venue="BINANCE",
            price=101.0,
            size=1.0,
            trade_id="2",
        ),
    ]
    cfg = mock.Mock(env="dev", ws_base="wss://x", rest_base="https://x", symbols=["BTCUSDT"], data_dir=".", log_level="INFO")

    class RecordingSink:
        def __init__(self):
            self.items = []

        def add(self, batch):
            if isinstance(batch, list):
                self.items.extend(batch)
            else:
                self.items.append(batch)

        def close(self):
            return None

    sink = RecordingSink()
    out = pipeline.collect_events(
        mode="live",
        cfg=cfg,
        max_events=10,
        duration_s=0,
        logger=mock.Mock(),
        source=StaticSource(events=typed_events),
        sink=sink,
        snapshot_enabled=False,
    )

    assert all(isinstance(item, TradeEvent) for item in out)
    assert [event.price for event in out] == [100.0, 101.0]
    assert [event.trade_id for event in out] == ["1", "2"]
    assert all(isinstance(item, TradeEvent) for item in sink.items)
