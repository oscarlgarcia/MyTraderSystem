from datetime import datetime, timezone

from app.ingestion.dedup import Deduplicator
from app.marketdata.models import TradeEvent


def _trade(trade_id: str) -> TradeEvent:
    ts = datetime(2024, 1, 1, tzinfo=timezone.utc)
    return TradeEvent(
        symbol="BTCUSDT",
        exchange_ts=ts,
        receive_ts=ts,
        process_ts=ts,
        venue="BINANCE",
        price=100.0,
        size=1.0,
        trade_id=trade_id,
    )


def test_same_timestamp_price_and_size_with_distinct_native_ids_are_not_collapsed():
    dedup = Deduplicator(ttl_seconds=60.0, max_entries=10)
    first = _trade("1001")
    second = _trade("1002")

    assert dedup.is_duplicate(first) is False
    dedup.remember(first)
    assert dedup.is_duplicate(second) is False


def test_same_native_id_is_treated_as_duplicate_even_if_payload_shape_matches():
    dedup = Deduplicator(ttl_seconds=60.0, max_entries=10)
    first = _trade("2001")
    second = _trade("2001")

    assert dedup.is_duplicate(first) is False
    dedup.remember(first)
    assert dedup.is_duplicate(second) is True
