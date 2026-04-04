import math
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


def test_valid_series_keeps_length_and_window_marker():
    engine = FeatureEngine(window=3)
    fvs = engine.update_batch([_ev(i * 60, 100 + i) for i in range(10)])
    assert len(fvs) == 10
    assert all("price" in fv.values for fv in fvs)
    assert all(math.isfinite(fv.values["price"]) for fv in fvs)
    assert all("window_max" in fv.values for fv in fvs)


def test_nan_is_dropped_and_counted():
    engine = FeatureEngine(window=2)
    fvs = engine.update_batch([_ev(0, 100), _ev(60, float("nan")), _ev(120, 102)])
    assert len(fvs) == 2
    assert engine.metrics["dropped_non_finite"] == 1


def test_non_finite_price_discards_and_keeps_following_values():
    engine = FeatureEngine(window=2)
    fvs = engine.update_batch([_ev(0, float("inf")), _ev(60, 100)])
    assert len(fvs) == 1
    assert engine.metrics["dropped_non_finite"] == 1
    assert fvs[0].values["price"] == 100
