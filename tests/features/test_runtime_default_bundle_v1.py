from datetime import datetime, timezone

from app.common.dto import MarketEvent
from app.features.engine import FeatureEngine


def _event(offset: int, price: float) -> MarketEvent:
    return MarketEvent(
        symbol="BTCUSDT",
        event_ts=datetime.fromtimestamp(1700000000 + offset, tz=timezone.utc),
        price=price,
        size=1.0,
        source="trade",
    )


def test_default_runtime_bundle_exposes_strategy_sma_and_extended_windows():
    engine = FeatureEngine()
    out = engine.update_batch([_event(i * 60, price) for i, price in enumerate([100.0, 101.0, 102.0, 103.0, 104.0])])
    assert out[-1] is not None
    assert "sma_3" in out[-1].values
    assert "sma_5" in out[-1].values
    assert "ema_20" not in out[-1].values


def test_basic_strategy_view_falls_back_to_sma5_when_sma3_is_missing():
    engine = FeatureEngine(window=5, windows=[5], aggregators=["sma"])
    out = engine.update_batch([_event(i * 60, price) for i, price in enumerate([100.0, 101.0, 102.0, 103.0, 104.0])])
    assert out[-1] is not None
    assert "sma_3" not in out[-1].values
    assert "sma_5" in out[-1].values
