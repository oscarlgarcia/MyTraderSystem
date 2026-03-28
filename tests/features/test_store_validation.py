import math
from datetime import datetime, timezone

import pytest

from app.common.dto import MarketEvent
from app.features import store
from app.features.store import compute_features


def _ev(ts_offset: int, price: float) -> MarketEvent:
    return MarketEvent(
        symbol="BTCUSDT",
        event_ts=datetime.fromtimestamp(1700000000 + ts_offset, tz=timezone.utc),
        price=price,
        size=1.0,
        source="trade",
    )


def test_valid_series_keeps_length(caplog):
    events = [_ev(i * 60, 100 + i) for i in range(10)]
    fvs = compute_features(events, window=3)
    assert len(fvs) == 10
    assert all("price" in fv.values for fv in fvs)
    assert all(math.isfinite(fv.values["price"]) for fv in fvs)
    assert all("window_max" in fv.values for fv in fvs)


def test_nan_is_dropped_and_logged(caplog):
    caplog.set_level("INFO")
    events = [_ev(0, 100), _ev(60, float("nan")), _ev(120, 102)]
    fvs = compute_features(events, window=2)
    assert len(fvs) == 2  # nan descartado
    assert any("features discarded" in rec.message for rec in caplog.records)


def test_missing_required_key_drops_feature(caplog, monkeypatch):
    caplog.set_level("INFO")
    monkeypatch.setattr(store, "REQUIRED_KEYS", {"price", "sma_5"})
    events = [_ev(i * 60, 100 + i) for i in range(5)]
    fvs = compute_features(events, window=2)
    assert len(fvs) == 0  # faltó sma_5
    assert any("features discarded" in rec.message for rec in caplog.records)


def test_negative_price_discards_and_counts(caplog):
    caplog.set_level("INFO")
    events = [_ev(0, -1), _ev(60, 100)]
    fvs = compute_features(events, window=2)
    assert len(fvs) == 1
    assert any("features discarded" in rec.message for rec in caplog.records)
