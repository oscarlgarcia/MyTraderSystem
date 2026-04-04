from datetime import datetime, timezone

from app.common.dto import MarketEvent
from app.features.engine import FeatureEngine


def _ev(ts_offset: int, price: float) -> MarketEvent:
    return MarketEvent(
        symbol="BTCUSDT",
        event_ts=datetime.fromtimestamp(1700000000 + ts_offset, tz=timezone.utc),
        price=price,
        size=1.0,
        source="trade",
    )


def test_engine_exposes_latest_and_temporal_lookup():
    engine = FeatureEngine(window=2, cache_capacity=4)
    events = [_ev(0, 100), _ev(60, 101)]
    out = engine.update_batch(events)
    latest = engine.get_latest("BTCUSDT")
    at_first = engine.get_at("BTCUSDT", events[0].event_ts)
    assert latest is not None and latest.values["price"] == 101
    assert at_first is not None and at_first.values["price"] == out[0].values["price"]


def test_engine_handles_empty_batch():
    engine = FeatureEngine(window=2)
    assert engine.update_batch([]) == []


def test_engine_metrics_count_outputs():
    engine = FeatureEngine(window=2)
    out = engine.update_batch([_ev(0, 100), _ev(60, 101)])
    assert len(out) == 2
    assert engine.metrics["features_out"] == 2
