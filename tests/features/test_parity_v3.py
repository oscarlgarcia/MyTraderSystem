from datetime import datetime, timezone

import pytest

from app.common.dto import FeatureVector, MarketEvent
from app.features.definitions import build_legacy_feature_set_definition
from app.features.parity import run_parity_check


def _ev(offset, price):
    ts = datetime.fromtimestamp(1700000000 + offset, tz=timezone.utc)
    return MarketEvent(symbol="BTCUSDT", event_ts=ts, price=price, size=1.0, source="trade", available_ts=ts)


def test_parity_uses_batch_and_incremental_paths(tmp_path):
    feature_set = build_legacy_feature_set_definition(name="default", version="1.0.0", description="baseline", windows=[2, 3], aggregators=["sma", "ema"], transformers=[])
    report = run_parity_check([_ev(0, 100), _ev(60, 101), _ev(120, 102)], feature_set=feature_set, offline_store_path=tmp_path / "offline.sqlite", online_store_path=tmp_path / "online.sqlite")
    assert report.pass_ok


def test_parity_reports_mismatch_from_independent_batch_executor(monkeypatch, tmp_path):
    feature_set = build_legacy_feature_set_definition(name="default", version="1.0.0", description="baseline", windows=[2], aggregators=["sma"], transformers=[])

    def fake_execute(self, events, *, feature_set):
        ts = datetime.fromtimestamp(1700000000, tz=timezone.utc)
        return [FeatureVector(symbol="BTCUSDT", ts=ts, available_ts=ts, values={"price": 999.0, "ret_1": 0.0, "sma_2": 999.0, "window_max": 2.0}, feature_set_name=feature_set.name, feature_set_version=feature_set.version)]

    monkeypatch.setattr("app.features.parity.BatchFeatureExecutor.execute", fake_execute)
    report = run_parity_check([_ev(0, 100), _ev(60, 101)], feature_set=feature_set, offline_store_path=tmp_path / "offline.sqlite", online_store_path=tmp_path / "online.sqlite")
    assert not report.pass_ok
    assert any(mismatch.feature_name == "price" for mismatch in report.mismatches)
