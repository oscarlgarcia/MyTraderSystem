from datetime import datetime, timezone
import math

from app.common.dto import MarketEvent
from app.features.pipeline import run_feature_pipeline


def _ev(ts_offset: int, price: float) -> MarketEvent:
    return MarketEvent(
        symbol="BTCUSDT",
        event_ts=datetime.fromtimestamp(1700000000 + ts_offset, tz=timezone.utc),
        price=price,
        size=1.0,
        source="trade",
    )


def test_growing_series_has_positive_ret_and_correct_sma():
    events = [_ev(i * 60, p) for i, p in enumerate([100, 101, 103, 104, 105])]
    fvs = run_feature_pipeline(events, window=3)
    last = fvs[-1]
    assert last.values["ret_1"] > 0
    assert math.isclose(last.values["sma_3"], (103 + 104 + 105) / 3, rel_tol=1e-9)


def test_price_zero_has_no_ret():
    events = [_ev(0, 100), _ev(60, 0), _ev(120, 102)]
    fvs = run_feature_pipeline(events, window=2)
    assert "ret_1" not in fvs[1].values
    assert "ret_1" in fvs[2].values


def test_incomplete_window_has_no_sma():
    events = [_ev(0, 100), _ev(60, 101)]
    fvs = run_feature_pipeline(events, window=3)
    assert "sma_3" not in fvs[-1].values
