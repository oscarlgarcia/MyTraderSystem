from __future__ import annotations

from datetime import datetime

from app.common.dto import FeatureVector
from app.features.entity_codec import entity_scope, normalize_entity_keys
from app.features.online_store_base import FeatureOnlineStore
from app.features.pit import feature_vector_is_servable_at


class MemoryOnlineFeatureStore(FeatureOnlineStore):
    def __init__(self) -> None:
        self._latest: dict[tuple[str, str, str], FeatureVector] = {}
        self._history: dict[tuple[str, str, str], list[FeatureVector]] = {}

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

    def upsert(self, fv: FeatureVector) -> None:
        key = self._scope_key(
            feature_set_name=fv.feature_set_name,
            feature_set_version=fv.feature_set_version,
            symbol=fv.symbol,
            entity_keys=fv.entity_keys,
        )
        self._latest[key] = fv
        history = self._history.setdefault(key, [])
        history.append(fv)
        history.sort(key=lambda item: (item.ts, item.available_ts))

    def get_latest(self, *, symbol: str | None = None, feature_set_name: str, feature_set_version: str, entity_keys: dict[str, str] | None = None):
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
