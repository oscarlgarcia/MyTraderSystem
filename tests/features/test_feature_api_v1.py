from datetime import datetime, timezone

from fastapi.testclient import TestClient

from app.common.dto import FeatureVector
from app.features.api import create_feature_store_api
from app.features.online_store import OnlineFeatureStore


def _vector() -> FeatureVector:
    ts = datetime(2024, 1, 1, tzinfo=timezone.utc)
    return FeatureVector(
        symbol="BTCUSDT",
        ts=ts,
        available_ts=ts,
        values={"price": 100.0},
        feature_set_name="default",
        feature_set_version="1.0.0",
        lineage_id="bundle-1",
    )


def test_feature_store_api_roundtrip(tmp_path):
    store = OnlineFeatureStore(tmp_path / "online.sqlite")
    client = TestClient(create_feature_store_api(online_store=store))
    fv = _vector()

    response = client.post("/vectors", json={"vector": {
        "symbol": fv.symbol,
        "ts": fv.ts.isoformat(),
        "available_ts": fv.available_ts.isoformat(),
        "source_cutoff_ts": fv.source_cutoff_ts.isoformat(),
        "values": fv.values,
        "feature_set_name": fv.feature_set_name,
        "feature_set_version": fv.feature_set_version,
        "lineage_id": fv.lineage_id,
        "quality_flags": [],
        "entity_keys": fv.entity_keys,
    }})
    assert response.status_code == 200

    latest = client.get(
        "/vectors/latest",
        params={
            "symbol": "BTCUSDT",
            "feature_set_name": "default",
            "feature_set_version": "1.0.0",
        },
    )
    assert latest.status_code == 200
    assert latest.json()["vector"]["values"]["price"] == 100.0


def test_feature_store_api_snapshot_before_returns_404_when_missing(tmp_path):
    store = OnlineFeatureStore(tmp_path / "online.sqlite")
    client = TestClient(create_feature_store_api(online_store=store))
    response = client.get(
        "/vectors/snapshot_before",
        params={
            "symbol": "BTCUSDT",
            "feature_set_name": "default",
            "feature_set_version": "1.0.0",
            "cutoff_ts": datetime(2024, 1, 1, tzinfo=timezone.utc).isoformat(),
        },
    )
    assert response.status_code == 404
