from datetime import datetime, timezone

from app.common.dto import FeatureVector
from app.features.definitions import FeatureDefinition, FeatureSetDefinition
from app.features.offline_store import OfflineFeatureStore
from app.features.online_store import OnlineFeatureStore


def test_feature_definition_accepts_composite_entity_keys():
    feature = FeatureDefinition(
        name="x",
        version="1.0.0",
        description="ok",
        owner="test",
        entity_keys=("symbol", "account"),
    )
    assert feature.entity_keys == ("symbol", "account")


def test_feature_set_definition_accepts_composite_entity_keys():
    feature_set = FeatureSetDefinition(
        name="default",
        version="1.0.0",
        description="ok",
        entity_keys=("symbol", "account"),
    )
    assert feature_set.entity_keys == ("symbol", "account")


def test_stores_accept_composite_entity_keys(tmp_path):
    ts = datetime(2024, 1, 1, tzinfo=timezone.utc)
    vector = FeatureVector(
        symbol="BTCUSDT",
        ts=ts,
        available_ts=ts,
        values={"price": 100.0},
        entity_keys={"symbol": "BTCUSDT", "account": "paper"},
    )
    offline = OfflineFeatureStore(tmp_path / "offline.sqlite")
    online = OnlineFeatureStore(tmp_path / "online.sqlite")
    offline.put_many([vector])
    online.upsert(vector)

    loaded_offline = offline.get_point_in_time(
        entity_keys={"symbol": "BTCUSDT", "account": "paper"},
        decision_ts=ts,
        feature_set_name="legacy",
        feature_set_version="legacy",
    )
    loaded_online = online.get_latest(
        entity_keys={"symbol": "BTCUSDT", "account": "paper"},
        feature_set_name="legacy",
        feature_set_version="legacy",
    )
    assert loaded_offline is not None
    assert loaded_online is not None
