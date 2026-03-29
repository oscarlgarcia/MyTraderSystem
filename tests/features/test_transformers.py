from datetime import datetime, timezone
import math

from app.features.store import FeatureState, TRANSFORMERS, register_aggregator
from app.common.dto import MarketEvent


def _ev(ts_offset: int, price: float) -> MarketEvent:
    return MarketEvent(
        symbol="BTCUSDT",
        event_ts=datetime.fromtimestamp(1700000000 + ts_offset, tz=timezone.utc),
        price=price,
        size=1.0,
        source="trade",
    )


def test_pipeline_clip_and_scale():
    state = FeatureState(window=2, transformers=["clip_non_finite", "scale_price_2x"])
    ev = _ev(0, 100)
    fv = state.update(ev)
    assert fv is not None
    assert fv.values["price"] == 200  # scaled


def test_invalid_transformer_raises():
    state = FeatureState(window=2, transformers=["missing_one"])
    ev = _ev(0, 100)
    try:
        state.update(ev)
        assert False, "Expected ValueError"
    except ValueError:
        pass


def test_empty_transformers_leaves_values():
    state = FeatureState(window=2, transformers=[])
    ev = _ev(0, 100)
    fv = state.update(ev)
    assert fv is not None
    assert fv.values["price"] == 100
