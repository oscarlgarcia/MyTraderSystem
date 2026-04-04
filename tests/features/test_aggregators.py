from datetime import datetime, timezone
import math
import time

from app.common.dto import MarketEvent
from app.features.engine import FeatureEngine


def _ev(ts_offset: int, price: float, sym: str = "BTCUSDT") -> MarketEvent:
    return MarketEvent(
        symbol=sym,
        event_ts=datetime.fromtimestamp(1700000000 + ts_offset, tz=timezone.utc),
        price=price,
        size=1.0,
        source="trade",
    )


def test_builtin_rolling_aggregators_compute_expected_values():
    engine = FeatureEngine(window=3, windows=[3], aggregators=["sma", "ema"])
    out = engine.update_batch([_ev(i * 60, p) for i, p in enumerate([1, 2, 3])])
    assert out[-1] is not None
    assert math.isclose(out[-1].values["sma_3"], 2.0, rel_tol=1e-9)
    assert "ema_3" in out[-1].values


def test_engine_performance_basic():
    engine = FeatureEngine(window=3)
    events = [_ev(i, 100 + i % 5) for i in range(1000)]
    t0 = time.perf_counter()
    out = engine.update_batch(events)
    elapsed = time.perf_counter() - t0
    assert len(out) == 1000
    assert elapsed < 0.5
