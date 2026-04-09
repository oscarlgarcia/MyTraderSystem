from datetime import datetime, timezone
from unittest import mock

import pytest

from app.marketdata.connectors.binance import (
    BinanceBarNormalizer,
    BinanceTradeNormalizer,
    assert_binance_payload_schema,
    build_binance_stream,
    normalize_binance_event,
    snapshot_payload_from_row,
)
from app.marketdata.errors import SchemaDriftError
from app.marketdata.connectors.binance_sources import BinanceBarSource, BinanceTradeSource
from app.marketdata.models import BarEvent, TradeEvent


def _cfg():
    return mock.Mock(
        env="dev",
        ws_base="wss://stream.binance.com:9443",
        rest_base="https://api.binance.com",
        symbols=["BTCUSDT"],
        data_dir=".",
        log_level="INFO",
    )


def test_binance_trade_normalizer_returns_trade_event():
    event = BinanceTradeNormalizer.normalize_typed(
        {"s": "BTCUSDT", "E": 1704067200000, "p": "100", "q": "1", "a": 7, "f": 7, "l": 7},
        receive_ts=datetime(2024, 1, 1, 0, 0, 1, tzinfo=timezone.utc),
        process_ts=datetime(2024, 1, 1, 0, 0, 2, tzinfo=timezone.utc),
    )

    assert isinstance(event, TradeEvent)
    assert event.trade_id == "7"
    assert event.source_id == "7"
    assert event.metadata["aggregate_trade_id"] == "7"
    assert "instrument_catalog_version" in event.metadata
    assert "instrument_snapshot" in event.metadata
    assert event.metadata["metadata_source"] in {"venue_snapshot", "venue_runtime_snapshot"}
    assert "venue_snapshot_version" in event.metadata
    assert event.provider_ts is None


def test_binance_bar_snapshot_payload_builder_and_normalizer():
    payload = snapshot_payload_from_row(
        "kline",
        "BTCUSDT",
        [1704067200000, "100", "101", "99", "100.5", "10", 1704067250000, "2500"],
        interval="1m",
    )
    event = BinanceBarNormalizer.normalize_typed(payload)

    assert isinstance(event, BarEvent)
    assert event.interval == "1m"
    assert event.close == 100.5
    assert event.volume == 2500.0
    assert event.volume_kind == "quote"
    assert event.metadata["volume_kind"] == "quote"
    assert event.metadata["volume_semantics"] == "quote_asset_volume"
    assert "instrument_catalog_version" in event.metadata
    assert "\"symbol\":\"BTCUSDT\"" in event.metadata["instrument_snapshot"]
    assert event.metadata["metadata_source"] in {"venue_snapshot", "venue_runtime_snapshot"}
    assert event.provider_ts is None


def test_binance_bar_normalizer_sets_provider_ts_when_feed_exposes_distinct_event_time():
    event = BinanceBarNormalizer.normalize_typed(
        {
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
        }
    )

    assert event.exchange_ts == datetime(2024, 1, 1, 0, 0, 50, tzinfo=timezone.utc)
    assert event.provider_ts == datetime(2024, 1, 1, 0, 0, 55, tzinfo=timezone.utc)


def test_trade_schema_drift_raises_typed_error_for_unexpected_shape():
    with pytest.raises(SchemaDriftError, match="schema drift detected"):
        assert_binance_payload_schema(
            "trade",
            {
                "s": "BTCUSDT",
                "E": 1704067200000,
                "p": "100",
                "q": "1",
                "t": 9,
                "unexpected": {"nested": "field"},
            },
        )


def test_trade_schema_allows_internal_snapshot_metadata_fields():
    assert_binance_payload_schema(
        "trade",
        {
            "s": "BTCUSDT",
            "E": 1704067200000,
            "p": "100",
            "q": "1",
            "a": 9,
            "_backfill_endpoint": "aggTrades",
            "_historical_trade_kind": "aggregate_trade",
        },
    )


def test_kline_schema_drift_raises_typed_error_for_unexpected_nested_field():
    with pytest.raises(SchemaDriftError, match="schema drift detected"):
        assert_binance_payload_schema(
            "kline",
            {
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
                    "schema": {"version": 2},
                },
            },
        )


def test_normalize_binance_event_dispatches_by_feed():
    event = normalize_binance_event(
        "trade",
        {"s": "BTCUSDT", "E": 1704067200000, "p": "100", "q": "1", "t": 9},
    )

    assert isinstance(event, TradeEvent)


def test_build_binance_stream_uses_feed_specific_builder():
    assert build_binance_stream("trade", "BTCUSDT") == "btcusdt@aggTrade"
    assert build_binance_stream("kline", "BTCUSDT") == "btcusdt@kline_1m"


def test_binance_trade_source_is_feed_scoped():
    source = BinanceTradeSource(cfg=_cfg())

    assert source.stream_types == ("trade",)


def test_binance_bar_source_is_feed_scoped():
    source = BinanceBarSource(cfg=_cfg())

    assert source.stream_types == ("kline",)
