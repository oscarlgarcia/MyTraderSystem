from datetime import datetime, timezone
import math

from app.common.dto import MarketEvent
from app.features.pipeline import run_feature_pipeline


def _ev(ts_offset: int, price: float, symbol: str = "BTCUSDT") -> MarketEvent:
    return MarketEvent(
        symbol=symbol,
        event_ts=datetime.fromtimestamp(1700000000 + ts_offset, tz=timezone.utc),
        price=price,
        size=1.0,
        source="trade",
    )


def test_empty_events_returns_empty():
    assert run_feature_pipeline([], window=5) == []


def test_alignment_per_event():
    events = [_ev(i * 60, 100 + i) for i in range(5)]
    fvs = run_feature_pipeline(events, window=3)
    assert len(fvs) == 5
    assert fvs[-1].values["price"] == events[-1].price


def test_window_trim_and_sma():
    events = [_ev(i * 60, price) for i, price in enumerate([100, 101, 102, 103, 104])]
    fvs = run_feature_pipeline(events, window=3)
    assert math.isclose(fvs[-1].values["sma_3"], (102 + 103 + 104) / 3, rel_tol=1e-9)


def test_skip_non_finite():
    events = [_ev(0, 100), _ev(60, float("nan")), _ev(120, 102)]
    fvs = run_feature_pipeline(events, window=2)
    assert len(fvs) == 2
    assert fvs[-1].values["price"] == 102
