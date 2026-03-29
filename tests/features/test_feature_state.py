from datetime import datetime, timezone
from app.features.store import FeatureState
from app.common.dto import MarketEvent


def _ev(ts_offset: int, price: float, sym: str = "BTCUSDT") -> MarketEvent:
    return MarketEvent(
        symbol=sym,
        event_ts=datetime.fromtimestamp(1700000000 + ts_offset, tz=timezone.utc),
        price=price,
        size=1.0,
        source="trade",
    )


def test_incremental_updates_window_and_ret():
    state = FeatureState(window=3)
    events = [_ev(i * 60, p) for i, p in enumerate([100, 101, 103, 104, 105])]
    out = [state.update(ev) for ev in events]
    assert len(out) == 5
    last = out[-1]
    assert last is not None
    assert round(last.values["sma_3"], 2) == round((103 + 104 + 105) / 3, 2)
    assert "ret_1" in last.values


def test_multi_symbol_state_isolated():
    state = FeatureState(window=2)
    e1 = _ev(0, 100, "BTCUSDT")
    e2 = _ev(60, 200, "ETHUSDT")
    e3 = _ev(120, 102, "BTCUSDT")
    f1 = state.update(e1)
    f2 = state.update(e2)
    f3 = state.update(e3)
    assert f1.values["price"] == 100
    assert f2.values["price"] == 200
    # BTC ventana solo tiene precios de BTC
    # ventana BTC tiene 100 y 102 => sí hay sma_2; lo importante es aislamiento
    assert f3.values["sma_2"] == (100 + 102) / 2


def test_reset_clears_prev_price():
    state = FeatureState(window=2)
    e1 = _ev(0, 100)
    e2 = _ev(60, 101)
    f1 = state.update(e1)
    state.reset()
    f2 = state.update(e2)
    assert f1 is not None
    assert f2 is not None
    assert "ret_1" not in f2.values  # sin precio previo tras reset
