from datetime import datetime, timezone

from app.common.dto import MarketEvent
from app.features.materialization import FeatureMaterializer


def _ev(symbol, offset, price, *, available_offset=None, source="trade"):
    ts = datetime.fromtimestamp(1700000000 + offset, tz=timezone.utc)
    available = datetime.fromtimestamp(1700000000 + (available_offset if available_offset is not None else offset), tz=timezone.utc)
    return MarketEvent(symbol=symbol, event_ts=ts, price=price, size=1.0, source=source, available_ts=available)


def test_prepare_events_uses_asof_join_without_future_leakage():
    materializer = FeatureMaterializer()
    primary = [_ev("BTCUSDT", 60, 101.0, available_offset=60)]
    auxiliary = [
        _ev("BTCUSDT", 30, 99.0, available_offset=30, source="kline"),
        _ev("BTCUSDT", 90, 105.0, available_offset=90, source="kline"),
    ]
    prepared = materializer._prepare_events(primary, auxiliary_events={"aux": auxiliary})
    event, cutoff = prepared[0]
    assert event.metadata["join:aux:present"] == "true"
    assert event.metadata["join:aux:event_ts"] == auxiliary[0].event_ts.isoformat()
    assert event.metadata["join:aux:available_ts"] == auxiliary[0].available_ts.isoformat()
    assert cutoff == primary[0].available_ts
