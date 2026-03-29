from datetime import datetime, timezone
import math
import time

from app.common.dto import MarketEvent
from app.features.store import FeatureState, register_aggregator


def _ev(ts_offset: int, price: float, sym: str = "BTCUSDT") -> MarketEvent:
    return MarketEvent(
        symbol=sym,
        event_ts=datetime.fromtimestamp(1700000000 + ts_offset, tz=timezone.utc),
        price=price,
        size=1.0,
        source="trade",
    )


def test_custom_variance_aggregator():
    def var_agg(symbol, prices, window, state):
        data = prices if isinstance(prices, list) else list(prices)
        if len(data) < window:
            return None, state
        tail = data[-window:]
        mean = sum(tail) / window
        var = sum((x - mean) ** 2 for x in tail) / window
        return var, state

    register_aggregator("var", var_agg)
    state = FeatureState(window=3, aggregators=["sma", "var"])
    events = [_ev(i * 60, p) for i, p in enumerate([1, 2, 3])]
    out = [state.update(ev) for ev in events]
    assert out[-1] is not None
    assert "var_3" in out[-1].values
    assert math.isclose(out[-1].values["var_3"], 2 / 3, rel_tol=1e-9)


def test_performance_basic():
    state = FeatureState(window=3)
    events = [_ev(i, 100 + i % 5) for i in range(1000)]
    t0 = time.perf_counter()
    out = [state.update(ev) for ev in events]
    elapsed = time.perf_counter() - t0
    assert len(out) == 1000
    # umbral laxo para unit: 0.5s en 1000 eventos
    assert elapsed < 0.5
