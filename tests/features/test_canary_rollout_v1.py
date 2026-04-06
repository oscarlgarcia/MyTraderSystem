from datetime import datetime, timezone

from app.common.dto import FeatureVector
from app.features.online_store_memory import MemoryOnlineFeatureStore
from app.features.rollout import CanaryServingService, FeatureRolloutController
from app.features.serving import FeatureServingService


def test_canary_serving_routes_to_declared_version():
    ts = datetime(2024, 1, 1, tzinfo=timezone.utc)
    primary_store = MemoryOnlineFeatureStore()
    canary_store = MemoryOnlineFeatureStore()
    primary_store.upsert(FeatureVector(symbol="BTCUSDT", ts=ts, available_ts=ts, values={"price": 100.0}, feature_set_name="default", feature_set_version="1.0.0", lineage_id="bundle-a"))
    canary_store.upsert(FeatureVector(symbol="BTCUSDT", ts=ts, available_ts=ts, values={"price": 101.0}, feature_set_name="default", feature_set_version="1.1.0", lineage_id="bundle-b"))
    service = CanaryServingService(
        primary=FeatureServingService(online_store=primary_store),
        canary=FeatureServingService(online_store=canary_store),
        rollout=FeatureRolloutController(canary_fraction=1.0),
        canary_version="1.1.0",
    )
    served = service.get_latest_servable(
        feature_set_name="default",
        active_version="1.0.0",
        symbol="BTCUSDT",
        decision_ts=ts,
    )
    assert served.decision.route == "canary"
    assert served.result.feature_vector is not None
    assert served.result.feature_vector.feature_set_version == "1.1.0"
