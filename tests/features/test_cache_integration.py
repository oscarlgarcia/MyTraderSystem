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


def test_engine_cache_exposes_latest_feature_vector():
    engine = FeatureEngine(window=2, cache_capacity=2)
    engine.update(_ev(0, 100))
    engine.update(_ev(60, 101))
    latest = engine.get_latest("BTCUSDT")
    assert latest is not None
    assert latest.values["price"] == 101
