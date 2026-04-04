from datetime import datetime, timezone

from app.common.dto import FeatureVector
from app.features.online_store import OnlineFeatureStore
from app.features.serving import FeatureServingService
from app.features.shadow import ShadowServingService


def _fv(offset, price):
    ts = datetime.fromtimestamp(1700000000 + offset, tz=timezone.utc)
    return FeatureVector(symbol="BTCUSDT", ts=ts, available_ts=ts, values={"price": price}, feature_set_name="default", feature_set_version="1.0.0", lineage_id="bundle")


def test_shadow_service_compares_primary_and_shadow(tmp_path):
    online_a = OnlineFeatureStore(tmp_path / "a.sqlite")
    online_b = OnlineFeatureStore(tmp_path / "b.sqlite")
    fv = _fv(0, 100.0)
    online_a.upsert(fv)
    online_b.upsert(fv)
    svc_a = FeatureServingService(online_store=online_a)
    svc_b = FeatureServingService(online_store=online_b)
    shadow = ShadowServingService(primary=svc_a, shadow=svc_b)
    report = shadow.get_latest_servable(symbol="BTCUSDT", decision_ts=fv.ts, feature_set_name="default", feature_set_version="1.0.0")
    assert report.pass_ok
