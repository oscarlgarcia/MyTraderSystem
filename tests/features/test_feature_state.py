from datetime import datetime, timezone

from app.common.dto import MarketEvent
from app.features.runtime import FeatureRuntimeEngine, build_legacy_runtime_feature_set


def _ev(ts_offset: int, price: float, sym: str = "BTCUSDT") -> MarketEvent:
    return MarketEvent(
        symbol=sym,
        event_ts=datetime.fromtimestamp(1700000000 + ts_offset, tz=timezone.utc),
        price=price,
        size=1.0,
        source="trade",
    )


def _engine(window: int = 3) -> FeatureRuntimeEngine:
    return FeatureRuntimeEngine(feature_set=build_legacy_runtime_feature_set(window=window))


def test_incremental_updates_window_and_ret():
    engine = _engine(window=3)
    events = [_ev(i * 60, p) for i, p in enumerate([100, 101, 103, 104, 105])]
    out = engine.update_batch(events)
    last = out[-1]
    assert round(last.values["sma_3"], 2) == round((103 + 104 + 105) / 3, 2)
    assert "ret_1" in last.values


def test_multi_symbol_state_isolated():
    engine = _engine(window=2)
    f1 = engine.update(_ev(0, 100, "BTCUSDT"))
    f2 = engine.update(_ev(60, 200, "ETHUSDT"))
    f3 = engine.update(_ev(120, 102, "BTCUSDT"))
    assert f1 is not None and f2 is not None and f3 is not None
    assert f1.values["price"] == 100
    assert f2.values["price"] == 200
    assert f3.values["sma_2"] == (100 + 102) / 2


def test_new_runtime_has_no_previous_price_state():
    first = _engine(window=2)
    f1 = first.update(_ev(0, 100))
    assert f1 is not None

    second = _engine(window=2)
    f2 = second.update(_ev(60, 101))
    assert f2 is not None
    assert "ret_1" not in f2.values
