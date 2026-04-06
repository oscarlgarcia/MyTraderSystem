from datetime import datetime, timezone

from app.common.dto import FeatureVector
from app.features.online_store_factory import OnlineStoreConfig, create_online_store


def _vector() -> FeatureVector:
    ts = datetime(2024, 1, 1, tzinfo=timezone.utc)
    return FeatureVector(
        symbol="BTCUSDT",
        ts=ts,
        available_ts=ts,
        values={"price": 100.0},
        feature_set_name="default",
        feature_set_version="1.0.0",
        lineage_id="bundle",
        entity_keys={"symbol": "BTCUSDT", "account": "paper"},
    )


def test_online_store_factory_creates_memory_backend():
    store = create_online_store(OnlineStoreConfig(backend="memory"))
    vector = _vector()
    store.upsert(vector)
    assert store.get_latest(feature_set_name="default", feature_set_version="1.0.0", entity_keys=vector.entity_keys) is not None


def test_online_store_factory_creates_json_backend(tmp_path):
    store = create_online_store(OnlineStoreConfig(backend="json_file", path=tmp_path / "online.json"))
    vector = _vector()
    store.upsert(vector)
    assert store.get_snapshot_before(
        cutoff_ts=vector.available_ts,
        feature_set_name="default",
        feature_set_version="1.0.0",
        entity_keys=vector.entity_keys,
    ) is not None
