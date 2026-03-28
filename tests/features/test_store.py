from datetime import datetime, timezone

from app.common.dto import MarketEvent
from app.features.store import compute_features


def _ev(ts_offset, price):
    return MarketEvent(
        symbol="BTCUSDT",
        event_ts=datetime.fromtimestamp(1700000000 + ts_offset, tz=timezone.utc),
        price=price,
        size=1.0,
        source="trade",
    )


def test_compute_features_sma_and_ret():
    events = [_ev(0, 100), _ev(60, 101), _ev(120, 103), _ev(180, 104)]
    fvs = compute_features(events, windows=(3,))
    assert len(fvs) == 4
    last = fvs[-1]
    assert "sma_3" in last.values
    assert round(last.values["sma_3"], 2) == round((101 + 103 + 104) / 3, 2)
    assert "ret_1" in last.values
    assert last.values["ret_1"] != 0
