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


def test_feature_store_api_exposes_catalog_filters(tmp_path):
    store = OnlineFeatureStore(tmp_path / "online.sqlite")
    client = TestClient(create_feature_store_api(online_store=store))

    response = client.get("/catalog/features", params={"family": "trend", "status": "implemented"})
    assert response.status_code == 200
    names = {item["feature_name"] for item in response.json()["features"]}
    assert "trend.sma.3" in names
    assert "trend.ema.20" in names


def test_feature_store_api_lists_catalog_dimensions(tmp_path):
    store = OnlineFeatureStore(tmp_path / "online.sqlite")
    client = TestClient(create_feature_store_api(online_store=store))

    families = client.get("/catalog/families")
    bundles = client.get("/catalog/bundles")
    strategies = client.get("/catalog/strategy-families")

    assert families.status_code == 200
    assert "trend" in families.json()["families"]
    assert bundles.status_code == 200
    assert "core_market_bundle" in bundles.json()["bundles"]
    assert strategies.status_code == 200
    assert "momentum" in strategies.json()["strategy_families"]


def test_feature_store_api_roundtrip_still_works_with_catalog_enabled(tmp_path):
    store = OnlineFeatureStore(tmp_path / "online.sqlite")
    client = TestClient(create_feature_store_api(online_store=store))
    fv = _vector()

    response = client.post(
        "/vectors",
        json={
            "vector": {
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
            }
        },
    )
    assert response.status_code == 200
