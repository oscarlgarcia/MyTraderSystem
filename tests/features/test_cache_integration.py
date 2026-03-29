from datetime import datetime, timezone

from app.features.cache import FeatureCache
from app.features.store import FeatureState
from app.common.dto import MarketEvent


def _ev(ts_offset: int, price: float) -> MarketEvent:
    return MarketEvent(
        symbol="BTCUSDT",
        event_ts=datetime.fromtimestamp(1700000000 + ts_offset, tz=timezone.utc),
        price=price,
        size=1.0,
        source="trade",
    )


def test_state_puts_into_cache():
    cache = FeatureCache(capacity_per_symbol=2)
    state = FeatureState(window=2, cache=cache)
    state.update(_ev(0, 100))
    state.update(_ev(60, 101))
    latest = cache.get_latest("BTCUSDT")
    assert latest is not None
    assert latest.values["price"] == 101
