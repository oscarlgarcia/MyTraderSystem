from datetime import datetime, timezone

from app.common.dto import FeatureVector
from app.features.offline_store import OfflineFeatureStore
from app.features.online_store import OnlineFeatureStore
from app.features.serving import FeatureServingService, ServingPolicy


def _fv(offset, available_offset=None, flags=()):
    ts = datetime.fromtimestamp(1700000000 + offset, tz=timezone.utc)
    available_ts = datetime.fromtimestamp(1700000000 + (available_offset if available_offset is not None else offset), tz=timezone.utc)
    return FeatureVector(symbol="BTCUSDT", ts=ts, available_ts=available_ts, values={"price": 100.0}, feature_set_name="default", feature_set_version="1.0.0", lineage_id="bundle", quality_flags=tuple(flags))


def test_serving_returns_ok_for_latest_servable(tmp_path):
    online = OnlineFeatureStore(tmp_path / "online.sqlite")
    offline = OfflineFeatureStore(tmp_path / "offline.sqlite")
    fv = _fv(0)
    online.upsert(fv)
    offline.put_many([fv])
    service = FeatureServingService(online_store=online, offline_store=offline)
    result = service.get_latest_servable(symbol="BTCUSDT", decision_ts=datetime.fromtimestamp(1700000060, tz=timezone.utc), feature_set_name="default", feature_set_version="1.0.0")
    assert result.status == "ok"


def test_serving_degrades_on_quality_flags(tmp_path):
    online = OnlineFeatureStore(tmp_path / "online.sqlite")
    fv = _fv(0, flags=("price:above_max",))
    online.upsert(fv)
    service = FeatureServingService(online_store=online, policy=ServingPolicy(on_invalid="degrade"))
    result = service.get_latest_servable(symbol="BTCUSDT", decision_ts=datetime.fromtimestamp(1700000060, tz=timezone.utc), feature_set_name="default", feature_set_version="1.0.0")
    assert result.status == "degrade"
