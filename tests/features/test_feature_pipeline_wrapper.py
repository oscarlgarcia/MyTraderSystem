from app.features.pipeline import run_feature_pipeline
from app.common.dto import MarketEvent
from datetime import datetime, timezone


def _ev(ts_offset: int, price: float, sym: str = "BTCUSDT") -> MarketEvent:
    return MarketEvent(
        symbol=sym,
        event_ts=datetime.fromtimestamp(1700000000 + ts_offset, tz=timezone.utc),
        price=price,
        size=1.0,
        source="trade",
    )


def test_wrapper_returns_same_length(caplog):
    caplog.set_level("INFO")
    events = [_ev(i * 60, 100 + i) for i in range(5)]
    feats = run_feature_pipeline(events, window=3)
    assert len(feats) == len(events)
    assert any("feature pipeline done" in rec.message for rec in caplog.records)
