from datetime import datetime, timezone

import pytest

from app.common.dto import FeatureVector
from app.features.definitions import FeatureDefinition, FeatureSetDefinition
from app.features.offline_store import OfflineFeatureStore
from app.features.online_store import OnlineFeatureStore


def test_feature_definition_rejects_non_symbol_entity_keys():
    with pytest.raises(ValueError, match="symbol-scoped"):
        FeatureDefinition(
            name="x",
            version="1.0.0",
            description="bad",
            owner="test",
            entity_keys=("symbol", "account"),
        )


def test_feature_set_definition_rejects_non_symbol_entity_keys():
    with pytest.raises(ValueError, match="symbol-scoped"):
        FeatureSetDefinition(
            name="default",
            version="1.0.0",
            description="bad",
            entity_keys=("symbol", "account"),
        )


def test_stores_reject_non_symbol_entity_keys(tmp_path):
    ts = datetime(2024, 1, 1, tzinfo=timezone.utc)
    vector = FeatureVector(
        symbol="BTCUSDT",
        ts=ts,
        available_ts=ts,
        values={"price": 100.0},
        entity_keys={"symbol": "BTCUSDT", "account": "paper"},
    )
    with pytest.raises(ValueError, match="symbol-scoped"):
        OfflineFeatureStore(tmp_path / "offline.sqlite").put_many([vector])
    with pytest.raises(ValueError, match="symbol-scoped"):
        OnlineFeatureStore(tmp_path / "online.sqlite").upsert(vector)
