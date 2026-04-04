from datetime import datetime, timedelta, timezone

from app.common.dto import FeatureVector
from app.features.online_store import OnlineFeatureStore
from app.features.serving import FeatureServingService


def _vector(index: int) -> FeatureVector:
    ts = datetime(2024, 1, 1, tzinfo=timezone.utc) + timedelta(minutes=index)
    return FeatureVector(
        symbol="BTCUSDT",
        ts=ts,
        available_ts=ts,
        values={"price": 100.0 + index},
        feature_set_name="default",
        feature_set_version="1.0.0",
        lineage_id=f"bundle-{index}",
    )


def test_online_store_prunes_to_max_rows_per_scope(tmp_path):
    store = OnlineFeatureStore(tmp_path / "online.sqlite", history_max_rows_per_scope=2)
    for index in range(4):
        store.upsert(_vector(index))
    history = store.get_recent_history(symbol="BTCUSDT", feature_set_name="default", feature_set_version="1.0.0", limit=10)
    assert len(history) == 2
    assert [item.values["price"] for item in history] == [103.0, 102.0]


def test_online_store_prunes_by_retention_window(tmp_path):
    store = OnlineFeatureStore(tmp_path / "online.sqlite", history_retention_seconds=None)
    for index in range(4):
        store.upsert(_vector(index))
    store.history_retention_seconds = 90
    deleted = store.prune_history(now=datetime(2024, 1, 1, 0, 4, tzinfo=timezone.utc))
    assert deleted >= 2
    history = store.get_recent_history(symbol="BTCUSDT", feature_set_name="default", feature_set_version="1.0.0", limit=10)
    assert all(item.available_ts >= datetime(2024, 1, 1, 0, 2, 30, tzinfo=timezone.utc) for item in history)


def test_serving_exposes_snapshot_before_cutoff(tmp_path):
    store = OnlineFeatureStore(tmp_path / "online.sqlite")
    for index in range(4):
        store.upsert(_vector(index))
    service = FeatureServingService(online_store=store)
    snapshot = service.get_snapshot_before(
        symbol="BTCUSDT",
        cutoff_ts=datetime(2024, 1, 1, 0, 2, 30, tzinfo=timezone.utc),
        feature_set_name="default",
        feature_set_version="1.0.0",
    )
    assert snapshot is not None
    assert snapshot.values["price"] == 102.0
