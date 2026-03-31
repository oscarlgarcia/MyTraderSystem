from datetime import datetime, timezone

from app.common.dto import MarketEvent
from app.ingestion import backfill, pipeline
from app.ingestion.dedup import Deduplicator


def _ev(ts_offset: int, price: float) -> MarketEvent:
    return MarketEvent(
        symbol="BTCUSDT",
        event_ts=datetime.fromtimestamp(1700000000 + ts_offset, tz=timezone.utc),
        price=price,
        size=1.0,
        source="trade",
    )


class DummySink:
    def __init__(self):
        self.items = []

    def add(self, batch):
        if isinstance(batch, list):
            self.items.extend(batch)
            return
        self.items.append(batch)

    def close(self):
        return None


def test_live_and_backfill_share_same_dedup_semantics():
    event_a = _ev(0, 100.0)
    event_b = _ev(0, 100.0)
    event_c = _ev(60, 101.0)

    sink = DummySink()
    stats = {"written": 0, "duplicates_dropped": 0}
    handler = pipeline._build_live_handler(sink, stats, max_events=10, dedup_enabled=True)
    handler(event_a)
    handler(event_b)
    handler(event_c)
    handler.close()

    backfill_unique, backfill_dropped = backfill.deduplicate_events([event_a, event_b, event_c])

    assert sink.items == backfill_unique
    assert stats["duplicates_dropped"] == backfill_dropped == 1


def test_dedup_ttl_expires_old_keys():
    clock = {"now": 100.0}
    dedup = Deduplicator(ttl_seconds=5.0, max_entries=10, now_fn=lambda: clock["now"])
    event = _ev(0, 100.0)

    assert dedup.is_duplicate(event) is False
    dedup.remember(event)
    assert dedup.is_duplicate(event) is True

    clock["now"] = 106.0
    assert dedup.is_duplicate(event) is False


def test_distinct_events_are_not_collapsed():
    dedup = Deduplicator(ttl_seconds=60.0, max_entries=10)
    event_a = _ev(0, 100.0)
    event_b = _ev(0, 100.0)
    event_c = MarketEvent(
        symbol="BTCUSDT",
        event_ts=event_a.event_ts,
        price=100.0,
        size=2.0,
        source="trade",
    )

    assert dedup.is_duplicate(event_a) is False
    dedup.remember(event_a)
    assert dedup.is_duplicate(event_b) is True
    assert dedup.is_duplicate(event_c) is False


def test_dedup_capacity_bounds_memory():
    clock = {"now": 100.0}
    dedup = Deduplicator(ttl_seconds=None, max_entries=2, now_fn=lambda: clock["now"])

    first = _ev(0, 100.0)
    second = _ev(60, 101.0)
    third = _ev(120, 102.0)

    dedup.remember(first)
    dedup.remember(second)
    dedup.remember(third)

    assert len(dedup) == 2
    assert dedup.is_duplicate(first) is False
