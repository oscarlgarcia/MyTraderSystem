from datetime import datetime, timezone

import pytest

from app.common.dto import MarketEvent
from app.features.pit import ensure_point_in_time_safe
from app.features.offline_store import OfflineFeatureStore
from app.features.definitions import build_legacy_feature_set_definition
from app.features.materialization import FeatureMaterializer


def _ev(offset, price, available_offset=None):
    base = 1700000000
    event_ts = datetime.fromtimestamp(base + offset, tz=timezone.utc)
    available_ts = datetime.fromtimestamp(base + (available_offset if available_offset is not None else offset), tz=timezone.utc)
    return MarketEvent(symbol="BTCUSDT", event_ts=event_ts, price=price, size=1.0, source="trade", available_ts=available_ts)


def test_point_in_time_guard_raises_for_future_data():
    decision_ts = datetime.fromtimestamp(1700000000, tz=timezone.utc)
    available_ts = datetime.fromtimestamp(1700000060, tz=timezone.utc)
    with pytest.raises(ValueError):
        ensure_point_in_time_safe(decision_ts=decision_ts, available_ts=available_ts, context="unit")


def test_offline_store_query_respects_available_ts(tmp_path):
    store = OfflineFeatureStore(tmp_path / "offline.sqlite")
    feature_set = build_legacy_feature_set_definition(name="default", version="1.0.0", description="baseline", windows=[2], aggregators=["sma"], transformers=[])
    materializer = FeatureMaterializer()
    events = [_ev(0, 100, available_offset=0), _ev(60, 101, available_offset=300)]
    materializer.materialize(events, feature_set=feature_set, store=store, run_id="pit")
    decision_ts = datetime.fromtimestamp(1700000100, tz=timezone.utc)
    fv = store.get_point_in_time(symbol="BTCUSDT", decision_ts=decision_ts, feature_set_name="default", feature_set_version="1.0.0")
    assert fv is not None
    assert fv.ts == datetime.fromtimestamp(1700000000, tz=timezone.utc)
