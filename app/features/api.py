from __future__ import annotations

import json
from contextlib import asynccontextmanager
from datetime import datetime

from fastapi import FastAPI, HTTPException, Query

from app.features.catalog import FeatureCatalog, get_default_feature_catalog
from app.features.online_store_base import FeatureOnlineStore
from app.features.online_store_codec import deserialize_feature_vector, serialize_feature_vector


def _parse_entity_keys(entity_keys: str | None) -> dict[str, str] | None:
    if not entity_keys:
        return None
    payload = json.loads(entity_keys)
    if not isinstance(payload, dict):
        raise HTTPException(status_code=400, detail="entity_keys must be a JSON object")
    return {str(key): str(value) for key, value in payload.items()}


def create_feature_store_api(*, online_store: FeatureOnlineStore, catalog: FeatureCatalog | None = None) -> FastAPI:
    catalog = catalog or get_default_feature_catalog()

    @asynccontextmanager
    async def _lifespan(_app: FastAPI):
        yield
        close = getattr(online_store, "close", None)
        if callable(close):
            close()

    app = FastAPI(title="MyTraderSystem Feature Store", version="0.1.0", lifespan=_lifespan)

    @app.get("/healthz")
    def healthz() -> dict[str, bool]:
        return {"ok": True}

    @app.get("/catalog/features")
    def get_catalog_features(
        family: str | None = None,
        strategy_family: str | None = None,
        phase: str | None = None,
        source_scope: str | None = None,
        status: str | None = None,
        bundle_name: str | None = None,
    ) -> dict[str, object]:
        features = catalog.list_features(
            family=family,
            strategy_family=strategy_family,
            phase=phase,
            source_scope=source_scope,
            status=status,
            bundle_name=bundle_name,
        )
        return {"features": [item.to_dict() for item in features]}

    @app.get("/catalog/families")
    def get_catalog_families() -> dict[str, object]:
        return {"families": list(catalog.list_families())}

    @app.get("/catalog/strategy-families")
    def get_catalog_strategy_families() -> dict[str, object]:
        return {"strategy_families": list(catalog.list_strategy_families())}

    @app.get("/catalog/source-scopes")
    def get_catalog_source_scopes() -> dict[str, object]:
        return {"source_scopes": list(catalog.list_source_scopes())}

    @app.get("/catalog/bundles")
    def get_catalog_bundles() -> dict[str, object]:
        return {"bundles": list(catalog.list_bundles())}

    @app.post("/vectors")
    def upsert_vector(payload: dict[str, object]) -> dict[str, bool]:
        vector_payload = payload.get("vector")
        if not isinstance(vector_payload, dict):
            raise HTTPException(status_code=400, detail="payload must include vector")
        online_store.upsert(deserialize_feature_vector(vector_payload))
        return {"ok": True}

    @app.get("/vectors/latest")
    def get_latest(
        feature_set_name: str,
        feature_set_version: str,
        symbol: str | None = None,
        entity_keys: str | None = None,
    ) -> dict[str, object]:
        vector = online_store.get_latest(
            symbol=symbol,
            entity_keys=_parse_entity_keys(entity_keys),
            feature_set_name=feature_set_name,
            feature_set_version=feature_set_version,
        )
        if vector is None:
            raise HTTPException(status_code=404, detail="vector not found")
        return {"vector": serialize_feature_vector(vector)}

    @app.get("/vectors/latest_servable")
    def get_latest_servable(
        decision_ts: str,
        feature_set_name: str,
        feature_set_version: str,
        symbol: str | None = None,
        entity_keys: str | None = None,
    ) -> dict[str, object]:
        vector = online_store.get_latest_servable(
            decision_ts=datetime.fromisoformat(decision_ts),
            symbol=symbol,
            entity_keys=_parse_entity_keys(entity_keys),
            feature_set_name=feature_set_name,
            feature_set_version=feature_set_version,
        )
        if vector is None:
            raise HTTPException(status_code=404, detail="vector not found")
        return {"vector": serialize_feature_vector(vector)}

    @app.get("/vectors/history/recent")
    def get_recent_history(
        feature_set_name: str,
        feature_set_version: str,
        limit: int = Query(default=10, ge=1, le=10_000),
        symbol: str | None = None,
        entity_keys: str | None = None,
    ) -> dict[str, object]:
        vectors = online_store.get_recent_history(
            symbol=symbol,
            entity_keys=_parse_entity_keys(entity_keys),
            feature_set_name=feature_set_name,
            feature_set_version=feature_set_version,
            limit=limit,
        )
        return {"vectors": [serialize_feature_vector(vector) for vector in vectors]}

    @app.get("/vectors/history/range")
    def get_history_range(
        feature_set_name: str,
        feature_set_version: str,
        start_ts: str,
        end_ts: str,
        symbol: str | None = None,
        entity_keys: str | None = None,
    ) -> dict[str, object]:
        vectors = online_store.get_history_range(
            symbol=symbol,
            entity_keys=_parse_entity_keys(entity_keys),
            feature_set_name=feature_set_name,
            feature_set_version=feature_set_version,
            start_ts=datetime.fromisoformat(start_ts),
            end_ts=datetime.fromisoformat(end_ts),
        )
        return {"vectors": [serialize_feature_vector(vector) for vector in vectors]}

    @app.get("/vectors/snapshot_before")
    def get_snapshot_before(
        cutoff_ts: str,
        feature_set_name: str,
        feature_set_version: str,
        symbol: str | None = None,
        entity_keys: str | None = None,
    ) -> dict[str, object]:
        vector = online_store.get_snapshot_before(
            cutoff_ts=datetime.fromisoformat(cutoff_ts),
            symbol=symbol,
            entity_keys=_parse_entity_keys(entity_keys),
            feature_set_name=feature_set_name,
            feature_set_version=feature_set_version,
        )
        if vector is None:
            raise HTTPException(status_code=404, detail="vector not found")
        return {"vector": serialize_feature_vector(vector)}

    return app
