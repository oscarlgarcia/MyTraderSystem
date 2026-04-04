from datetime import datetime, timezone

from app.common.dto import MarketEvent
from app.features.pipeline import run_feature_pipeline


def _ev(ts_offset, price):
    return MarketEvent(
        symbol="BTCUSDT",
        event_ts=datetime.fromtimestamp(1700000000 + ts_offset, tz=timezone.utc),
        price=price,
        size=1.0,
        source="trade",
    )


def test_pipeline_sma_and_ret():
    events = [_ev(0, 100), _ev(60, 101), _ev(120, 103), _ev(180, 104)]
    fvs = run_feature_pipeline(events, window=3)
    assert len(fvs) == 4
    last = fvs[-1]
    assert "sma_3" in last.values
    assert round(last.values["sma_3"], 2) == round((101 + 103 + 104) / 3, 2)
    assert "ret_1" in last.values
    assert last.values["ret_1"] != 0
