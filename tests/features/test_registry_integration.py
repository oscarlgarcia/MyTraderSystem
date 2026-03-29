from datetime import datetime, timezone

from app.features.registry import FeatureRegistry
from app.common.dto import MarketEvent


def _ev(ts_offset: int, price: float) -> MarketEvent:
    return MarketEvent(
        symbol="BTCUSDT",
        event_ts=datetime.fromtimestamp(1700000000 + ts_offset, tz=timezone.utc),
        price=price,
        size=1.0,
        source="trade",
    )


def test_build_feature_state_from_registry():
    reg = FeatureRegistry()
    reg.register_feature_set(
        name="default",
        version="1.0.0",
        description="baseline",
        windows=[3],
        aggregators=["sma"],
        transformers=[],
    )
    state = reg.build_feature_state("default", "1.0.0")
    events = [_ev(i * 60, p) for i, p in enumerate([100, 101, 103])]
    out = [state.update(ev) for ev in events]
    assert out[-1] is not None
    assert "sma_3" in out[-1].values
