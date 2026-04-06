from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone

from app.common.dto import FeatureVector
from app.features.online_store_memory import MemoryOnlineFeatureStore
from app.features.serving import FeatureServingService


def test_serving_handles_basic_concurrency_smoke():
    ts = datetime(2024, 1, 1, tzinfo=timezone.utc)
    store = MemoryOnlineFeatureStore()
    service = FeatureServingService(online_store=store)

    def writer(i: int) -> None:
        vector = FeatureVector(
            symbol="BTCUSDT",
            ts=ts,
            available_ts=ts,
            values={"price": 100.0 + i},
            feature_set_name="default",
            feature_set_version="1.0.0",
            lineage_id=f"bundle-{i}",
            entity_keys={"symbol": "BTCUSDT", "account": "paper"},
        )
        store.upsert(vector)

    def reader() -> str:
        result = service.get_latest_servable(
            decision_ts=ts,
            feature_set_name="default",
            feature_set_version="1.0.0",
            entity_keys={"symbol": "BTCUSDT", "account": "paper"},
        )
        return result.status

    with ThreadPoolExecutor(max_workers=8) as executor:
        for i in range(20):
            executor.submit(writer, i)
        statuses = list(executor.map(lambda _: reader(), range(20)))
    assert all(status in {"ok", "fail"} for status in statuses)
    assert "ok" in statuses
