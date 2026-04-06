import json
from datetime import datetime, timezone

from app.common.dto import FeatureVector
from app.features.online_store_memory import MemoryOnlineFeatureStore
from app.features.rollout import CanaryServingService, FeatureRolloutController
from app.features.rollout_audit import CanaryAuditStore
from app.features.serving import FeatureServingService


def test_canary_serving_persists_routing_audit(tmp_path):
    ts = datetime(2024, 1, 1, tzinfo=timezone.utc)
    primary_store = MemoryOnlineFeatureStore()
    canary_store = MemoryOnlineFeatureStore()
    entity_keys = {"symbol": "BTCUSDT", "account": "paper"}
    primary_store.upsert(FeatureVector(symbol="BTCUSDT", ts=ts, available_ts=ts, values={"price": 100.0}, feature_set_name="default", feature_set_version="1.0.0", lineage_id="bundle-a", entity_keys=entity_keys))
    canary_store.upsert(FeatureVector(symbol="BTCUSDT", ts=ts, available_ts=ts, values={"price": 101.0}, feature_set_name="default", feature_set_version="1.1.0", lineage_id="bundle-b", entity_keys=entity_keys))
    audit_store = CanaryAuditStore(tmp_path / "canary_audit.jsonl")
    service = CanaryServingService(
        primary=FeatureServingService(online_store=primary_store),
        canary=FeatureServingService(online_store=canary_store),
        rollout=FeatureRolloutController(canary_fraction=1.0),
        canary_version="1.1.0",
        audit_store=audit_store,
    )
    served = service.get_latest_servable(
        feature_set_name="default",
        active_version="1.0.0",
        symbol="BTCUSDT",
        entity_keys=entity_keys,
        decision_ts=ts,
    )
    assert served.decision.route == "canary"
    rows = [json.loads(line) for line in audit_store.path.read_text(encoding="utf-8").splitlines()]
    assert rows[-1]["route"] == "canary"
    assert '"account":"paper"' in rows[-1]["entity_scope"]
