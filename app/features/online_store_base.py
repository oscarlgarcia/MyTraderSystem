from __future__ import annotations

from datetime import datetime
from typing import Optional, Protocol

from app.common.dto import FeatureVector


class FeatureOnlineStore(Protocol):
    def upsert(self, fv: FeatureVector) -> None:
        ...

    def get_latest(
        self,
        *,
        symbol: str | None = None,
        feature_set_name: str,
        feature_set_version: str,
        entity_keys: dict[str, str] | None = None,
    ) -> Optional[FeatureVector]:
        ...

    def get_latest_servable(
        self,
        *,
        decision_ts: datetime,
        feature_set_name: str,
        feature_set_version: str,
        symbol: str | None = None,
        entity_keys: dict[str, str] | None = None,
    ) -> Optional[FeatureVector]:
        ...

    def get_recent_history(
        self,
        *,
        feature_set_name: str,
        feature_set_version: str,
        limit: int = 10,
        symbol: str | None = None,
        entity_keys: dict[str, str] | None = None,
    ) -> list[FeatureVector]:
        ...

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
        ...

    def get_snapshot_before(
        self,
        *,
        cutoff_ts: datetime,
        feature_set_name: str,
        feature_set_version: str,
        symbol: str | None = None,
        entity_keys: dict[str, str] | None = None,
    ) -> Optional[FeatureVector]:
        ...
