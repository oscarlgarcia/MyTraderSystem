import itertools
from datetime import datetime, timezone

import pytest

from app.features.engine import FeatureEngine
from app.common.dto import MarketEvent


def _ev(ts_offset: int, price: float, sym: str = "BTCUSDT") -> MarketEvent:
    return MarketEvent(
        symbol=sym,
        event_ts=datetime.fromtimestamp(1700000000 + ts_offset, tz=timezone.utc),
        price=price,
        size=1.0,
        source="trade",
    )


def test_nan_increments_drop_and_not_features():
    eng = FeatureEngine(window=2)
    eng.update(_ev(0, float("nan")))
    assert eng.metrics["events_in"] == 1
    assert eng.metrics["dropped_non_finite"] == 1
    assert eng.metrics["features_out"] == 0


def test_latency_monotonic(monkeypatch):
    # simulate perf_counter progression
    seq = itertools.cycle([0.0, 0.001, 1.0, 1.003])  # durations: 0.001, 0.003

    def fake_perf_counter():
        return next(seq)

    monkeypatch.setattr("app.features.engine.time.perf_counter", fake_perf_counter)
    eng = FeatureEngine(window=2)
    eng.update(_ev(0, 100))
    eng.update(_ev(60, 101))
    assert eng.metrics["compute_latency_max"] >= 0.0029  # tolerancia a float error
    assert eng.avg_latency() > 0
    # ensure max does not decrease
    prev_max = eng.metrics["compute_latency_max"]
    eng.update(_ev(120, 102))
    assert eng.metrics["compute_latency_max"] >= prev_max
