from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path
from threading import RLock

from app.common.dto import FeatureVector
from app.features.entity_codec import entity_scope, normalize_entity_keys
from app.features.online_store_base import FeatureOnlineStore
from app.features.pit import feature_vector_is_servable_at


def _serialize_vector(vector: FeatureVector) -> dict[str, object]:
    return {
        "symbol": vector.symbol,
        "ts": vector.ts.isoformat(),
        "available_ts": vector.available_ts.isoformat(),
        "source_cutoff_ts": vector.source_cutoff_ts.isoformat(),
        "values": dict(vector.values),
        "feature_set_name": vector.feature_set_name,
        "feature_set_version": vector.feature_set_version,
        "lineage_id": vector.lineage_id,
        "quality_flags": list(vector.quality_flags),
        "entity_keys": dict(vector.entity_keys),
    }


def _deserialize_vector(payload: dict[str, object]) -> FeatureVector:
    return FeatureVector(
        symbol=str(payload["symbol"]),
        ts=datetime.fromisoformat(str(payload["ts"])),
        available_ts=datetime.fromisoformat(str(payload["available_ts"])),
        source_cutoff_ts=datetime.fromisoformat(str(payload["source_cutoff_ts"])),
        values={str(key): float(value) for key, value in dict(payload["values"]).items()},
        feature_set_name=str(payload["feature_set_name"]),
        feature_set_version=str(payload["feature_set_version"]),
        lineage_id=str(payload.get("lineage_id", "")),
        quality_flags=tuple(payload.get("quality_flags", [])),
        entity_keys={str(key): str(value) for key, value in dict(payload.get("entity_keys", {})).items()},
    )


class JsonOnlineFeatureStore(FeatureOnlineStore):
    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._lock = RLock()
        self._latest: dict[tuple[str, str, str], FeatureVector] = {}
        self._history: dict[tuple[str, str, str], list[FeatureVector]] = {}
        self._load()

    def _scope_key(
        self,
        *,
        feature_set_name: str,
        feature_set_version: str,
        symbol: str | None = None,
        entity_keys: dict[str, str] | None = None,
    ) -> tuple[str, str, str]:
        scope = entity_scope(normalize_entity_keys(entity_keys, symbol=symbol), symbol=symbol)
        return (scope, feature_set_name, feature_set_version)

    def _load(self) -> None:
        if not self.path.exists():
            return
        payload = json.loads(self.path.read_text(encoding="utf-8"))
        for raw_key, vector_payload in payload.get("latest", {}).items():
            scope, feature_set_name, feature_set_version = raw_key.split("|", 2)
            self._latest[(scope, feature_set_name, feature_set_version)] = _deserialize_vector(vector_payload)
        for raw_key, vector_payloads in payload.get("history", {}).items():
            scope, feature_set_name, feature_set_version = raw_key.split("|", 2)
            self._history[(scope, feature_set_name, feature_set_version)] = [_deserialize_vector(item) for item in vector_payloads]

    def _dump(self) -> None:
        payload = {
            "latest": {"|".join(key): _serialize_vector(value) for key, value in self._latest.items()},
            "history": {"|".join(key): [_serialize_vector(item) for item in values] for key, values in self._history.items()},
        }
        self.path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")

    def upsert(self, fv: FeatureVector) -> None:
        key = self._scope_key(
            feature_set_name=fv.feature_set_name,
            feature_set_version=fv.feature_set_version,
            symbol=fv.symbol,
            entity_keys=fv.entity_keys,
        )
        with self._lock:
            self._latest[key] = fv
            history = self._history.setdefault(key, [])
            history.append(fv)
            history.sort(key=lambda item: (item.ts, item.available_ts))
            self._dump()

    def get_latest(self, *, symbol: str | None = None, feature_set_name: str, feature_set_version: str, entity_keys: dict[str, str] | None = None):
        with self._lock:
            return self._latest.get(
                self._scope_key(
                    symbol=symbol,
                    entity_keys=entity_keys,
                    feature_set_name=feature_set_name,
                    feature_set_version=feature_set_version,
                )
            )

    def get_latest_servable(self, *, decision_ts: datetime, feature_set_name: str, feature_set_version: str, symbol: str | None = None, entity_keys: dict[str, str] | None = None):
        fv = self.get_latest(symbol=symbol, entity_keys=entity_keys, feature_set_name=feature_set_name, feature_set_version=feature_set_version)
        if fv is None:
            return None
        return fv if feature_vector_is_servable_at(fv, decision_ts) else None

    def get_recent_history(self, *, feature_set_name: str, feature_set_version: str, limit: int = 10, symbol: str | None = None, entity_keys: dict[str, str] | None = None):
        with self._lock:
            history = self._history.get(
                self._scope_key(
                    symbol=symbol,
                    entity_keys=entity_keys,
                    feature_set_name=feature_set_name,
                    feature_set_version=feature_set_version,
                ),
                [],
            )
            return list(reversed(history[-limit:]))

    def get_history_range(self, *, feature_set_name: str, feature_set_version: str, start_ts: datetime, end_ts: datetime, symbol: str | None = None, entity_keys: dict[str, str] | None = None):
        with self._lock:
            history = self._history.get(
                self._scope_key(
                    symbol=symbol,
                    entity_keys=entity_keys,
                    feature_set_name=feature_set_name,
                    feature_set_version=feature_set_version,
                ),
                [],
            )
            return [item for item in history if start_ts <= item.ts <= end_ts]

    def get_snapshot_before(self, *, cutoff_ts: datetime, feature_set_name: str, feature_set_version: str, symbol: str | None = None, entity_keys: dict[str, str] | None = None):
        with self._lock:
            history = self._history.get(
                self._scope_key(
                    symbol=symbol,
                    entity_keys=entity_keys,
                    feature_set_name=feature_set_name,
                    feature_set_version=feature_set_version,
                ),
                [],
            )
            eligible = [item for item in history if item.available_ts <= cutoff_ts and item.ts <= cutoff_ts]
            return eligible[-1] if eligible else None
