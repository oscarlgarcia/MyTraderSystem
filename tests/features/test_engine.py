from datetime import datetime, timezone
import pytest

from app.features.engine import FeatureEngine
from app.common.dto import MarketEvent


def _ev(ts_offset: int, price: float, sym: str = "BTCUSDT") -> MarketEvent:
    return MarketEvent(
        symbol=sym,
        event_ts=datetime.fromtimestamp(1700000000 + ts_offset, tz=timezone.utc),
        price=price,
        size=1.0,
        source="trade",
    )


def test_update_and_get_latest():
    eng = FeatureEngine(window=2)
    eng.update(_ev(0, 100))
    eng.update(_ev(60, 101))
    latest = eng.get_latest("BTCUSDT")
    assert latest is not None
    assert latest.values["price"] == 101


def test_get_at_no_data_returns_none():
    eng = FeatureEngine(window=2)
    assert eng.get_at("BTCUSDT", datetime.fromtimestamp(1700000000, tz=timezone.utc)) is None


def test_invalid_price_is_discarded():
    eng = FeatureEngine(window=2)
    fv = eng.update(_ev(0, float("nan")))
    assert fv is None
    assert eng.get_latest("BTCUSDT") is None
