from __future__ import annotations

import json
from datetime import datetime
from typing import Any

import httpx

from app.common.dto import FeatureVector
from app.features.online_store_base import FeatureOnlineStore
from app.features.online_store_codec import deserialize_feature_vector, serialize_feature_vector


class RemoteHttpOnlineFeatureStore(FeatureOnlineStore):
    def __init__(self, base_url: str, *, timeout_seconds: float = 5.0) -> None:
        self.base_url = base_url.rstrip("/")
        self.timeout_seconds = timeout_seconds
        self._client = httpx.Client(timeout=self.timeout_seconds)

    def close(self) -> None:
        self._client.close()

    def _request(self, method: str, path: str, *, params: dict[str, Any] | None = None, json_body: dict[str, Any] | None = None) -> httpx.Response:
        return self._client.request(
            method,
            f"{self.base_url}{path}",
            params=params,
            json=json_body,
        )

    def _vector_params(
        self,
        *,
        feature_set_name: str,
        feature_set_version: str,
        symbol: str | None = None,
        entity_keys: dict[str, str] | None = None,
    ) -> dict[str, str]:
        params = {
            "feature_set_name": feature_set_name,
            "feature_set_version": feature_set_version,
        }
        if symbol is not None:
            params["symbol"] = symbol
        if entity_keys is not None:
            params["entity_keys"] = json.dumps(entity_keys, ensure_ascii=False, sort_keys=True)
        return params

    def _decode_optional_vector(self, response: httpx.Response) -> FeatureVector | None:
        if response.status_code == 404:
            return None
        response.raise_for_status()
        payload = response.json().get("vector")
        if payload is None:
            return None
        return deserialize_feature_vector(payload)

    def _decode_vector_list(self, response: httpx.Response) -> list[FeatureVector]:
        response.raise_for_status()
        payload = response.json().get("vectors", [])
        return [deserialize_feature_vector(item) for item in payload]

    def upsert(self, fv: FeatureVector) -> None:
        response = self._request("POST", "/vectors", json_body={"vector": serialize_feature_vector(fv)})
        response.raise_for_status()

    def get_latest(
        self,
        *,
        symbol: str | None = None,
        feature_set_name: str,
        feature_set_version: str,
        entity_keys: dict[str, str] | None = None,
    ) -> FeatureVector | None:
        response = self._request(
            "GET",
            "/vectors/latest",
            params=self._vector_params(
                symbol=symbol,
                entity_keys=entity_keys,
                feature_set_name=feature_set_name,
                feature_set_version=feature_set_version,
            ),
        )
        return self._decode_optional_vector(response)

    def get_latest_servable(
        self,
        *,
        decision_ts: datetime,
        feature_set_name: str,
        feature_set_version: str,
        symbol: str | None = None,
        entity_keys: dict[str, str] | None = None,
    ) -> FeatureVector | None:
        params = self._vector_params(
            symbol=symbol,
            entity_keys=entity_keys,
            feature_set_name=feature_set_name,
            feature_set_version=feature_set_version,
        )
        params["decision_ts"] = decision_ts.isoformat()
        response = self._request("GET", "/vectors/latest_servable", params=params)
        return self._decode_optional_vector(response)

    def get_recent_history(
        self,
        *,
        feature_set_name: str,
        feature_set_version: str,
        limit: int = 10,
        symbol: str | None = None,
        entity_keys: dict[str, str] | None = None,
    ) -> list[FeatureVector]:
        params = self._vector_params(
            symbol=symbol,
            entity_keys=entity_keys,
            feature_set_name=feature_set_name,
            feature_set_version=feature_set_version,
        )
        params["limit"] = str(limit)
        response = self._request("GET", "/vectors/history/recent", params=params)
        return self._decode_vector_list(response)

    def get_history_range(
        self,
        *,
        feature_set_name: str,
        feature_set_version: str,
        start_ts: datetime,
        end_ts: datetime,
        symbol: str | None = None,
        entity_keys: dict[str, str] | None = None,
    ) -> list[FeatureVector]:
        params = self._vector_params(
            symbol=symbol,
            entity_keys=entity_keys,
            feature_set_name=feature_set_name,
            feature_set_version=feature_set_version,
        )
        params["start_ts"] = start_ts.isoformat()
        params["end_ts"] = end_ts.isoformat()
        response = self._request("GET", "/vectors/history/range", params=params)
        return self._decode_vector_list(response)

    def get_snapshot_before(
        self,
        *,
        cutoff_ts: datetime,
        feature_set_name: str,
        feature_set_version: str,
        symbol: str | None = None,
        entity_keys: dict[str, str] | None = None,
    ) -> FeatureVector | None:
        params = self._vector_params(
            symbol=symbol,
            entity_keys=entity_keys,
            feature_set_name=feature_set_name,
            feature_set_version=feature_set_version,
        )
        params["cutoff_ts"] = cutoff_ts.isoformat()
        response = self._request("GET", "/vectors/snapshot_before", params=params)
        return self._decode_optional_vector(response)
