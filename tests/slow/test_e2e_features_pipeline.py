import io
from datetime import datetime, timezone

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


def test_e2e_mock_backfill_to_features(caplog):
    caplog.set_level("INFO")
    # backfill mock: 5 eventos
    events = [_ev(i * 60, 100 + i) for i in range(5)]

    feats = run_feature_pipeline(events, window=3)

    assert len(feats) == 5
    assert all(f.symbol == "BTCUSDT" for f in feats)
    assert any("feature pipeline done" in rec.message for rec in caplog.records)
