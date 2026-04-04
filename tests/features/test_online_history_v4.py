from datetime import datetime, timedelta, timezone

from app.common.dto import FeatureVector
from app.features.online_store import OnlineFeatureStore
from app.features.serving import FeatureServingService


def _fv(offset):
    ts = datetime.fromtimestamp(1700000000 + offset, tz=timezone.utc)
    return FeatureVector(
        symbol="BTCUSDT",
        ts=ts,
        available_ts=ts,
        values={"price": 100.0 + offset},
        feature_set_name="default",
        feature_set_version="1.0.0",
        lineage_id=f"bundle-{offset}",
    )


def test_online_store_persists_recent_history(tmp_path):
    store = OnlineFeatureStore(tmp_path / "online.sqlite")
    for offset in (0, 60, 120):
        store.upsert(_fv(offset))
    history = store.get_recent_history(symbol="BTCUSDT", feature_set_name="default", feature_set_version="1.0.0", limit=2)
    assert len(history) == 2
    assert history[0].ts > history[1].ts


def test_serving_exposes_recent_history(tmp_path):
    store = OnlineFeatureStore(tmp_path / "online.sqlite")
    base = datetime(2024, 1, 1, tzinfo=timezone.utc)
    for index in range(3):
        ts = base + timedelta(minutes=index)
        store.upsert(
            FeatureVector(
                symbol="BTCUSDT",
                ts=ts,
                available_ts=ts,
                values={"price": 100.0 + index},
                feature_set_name="default",
                feature_set_version="1.0.0",
                lineage_id=f"bundle-{index}",
            )
        )
    service = FeatureServingService(online_store=store)
    history = service.get_recent_history(symbol="BTCUSDT", feature_set_name="default", feature_set_version="1.0.0", limit=3)
    assert len(history) == 3


def test_serving_exposes_history_range(tmp_path):
    store = OnlineFeatureStore(tmp_path / "online.sqlite")
    base = datetime(2024, 1, 1, tzinfo=timezone.utc)
    for index in range(4):
        ts = base + timedelta(minutes=index)
        store.upsert(
            FeatureVector(
                symbol="BTCUSDT",
                ts=ts,
                available_ts=ts,
                values={"price": 100.0 + index},
                feature_set_name="default",
                feature_set_version="1.0.0",
                lineage_id=f"bundle-{index}",
            )
        )
    service = FeatureServingService(online_store=store)
    history = service.get_history_range(
        symbol="BTCUSDT",
        feature_set_name="default",
        feature_set_version="1.0.0",
        start_ts=base + timedelta(minutes=1),
        end_ts=base + timedelta(minutes=2),
    )
    assert [item.values["price"] for item in history] == [101.0, 102.0]
