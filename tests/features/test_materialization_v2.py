from datetime import datetime, timezone

from app.common.dto import MarketEvent
from app.features.definitions import build_legacy_feature_set_definition
from app.features.materialization import FeatureMaterializer
from app.features.offline_store import OfflineFeatureStore


def _ev(offset, price):
    ts = datetime.fromtimestamp(1700000000 + offset, tz=timezone.utc)
    return MarketEvent(symbol="BTCUSDT", event_ts=ts, price=price, size=1.0, source="trade")


def test_materializer_persists_lineage_and_queryable_vectors(tmp_path):
    store = OfflineFeatureStore(tmp_path / "offline.sqlite")
    feature_set = build_legacy_feature_set_definition(name="default", version="1.0.0", description="baseline", windows=[3], aggregators=["sma"], transformers=[])
    out = FeatureMaterializer().materialize([_ev(0, 100), _ev(60, 101), _ev(120, 102)], feature_set=feature_set, store=store, run_id="run-1")
    assert out
    assert all(fv.lineage_id for fv in out)
    snap = store.get_point_in_time(symbol="BTCUSDT", decision_ts=out[-1].ts, feature_set_name="default", feature_set_version="1.0.0")
    assert snap is not None
    assert snap.feature_set_name == "default"
