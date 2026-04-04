from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Optional, Tuple

from app.common.dto import FeatureVector
from app.features.offline_store import OfflineFeatureStore
from app.features.online_store import OnlineFeatureStore


@dataclass(frozen=True)
class ServingPolicy:
    max_staleness_seconds: float = 300.0
    on_missing: str = "fail"
    on_invalid: str = "degrade"


@dataclass(frozen=True)
class ServingResult:
    status: str
    feature_vector: Optional[FeatureVector]
    reason: str = ""
    staleness_seconds: float = 0.0


class FeatureServingService:
    def __init__(self, *, online_store: OnlineFeatureStore, offline_store: OfflineFeatureStore | None = None, policy: ServingPolicy | None = None) -> None:
        self.online_store = online_store
        self.offline_store = offline_store
        self.policy = policy or ServingPolicy()

    def get_latest_servable(self, *, symbol: str, decision_ts: datetime, feature_set_name: str, feature_set_version: str) -> ServingResult:
        fv = self.online_store.get_latest_servable(
            symbol=symbol,
            decision_ts=decision_ts,
            feature_set_name=feature_set_name,
            feature_set_version=feature_set_version,
        )
        if fv is None:
            return ServingResult(status="fail" if self.policy.on_missing == "fail" else "degrade", feature_vector=None, reason="missing_or_future")
        staleness = max(0.0, (decision_ts - fv.available_ts).total_seconds())
        if staleness > self.policy.max_staleness_seconds:
            return ServingResult(status="fail" if self.policy.on_missing == "fail" else "degrade", feature_vector=fv, reason="stale", staleness_seconds=staleness)
        if fv.quality_flags:
            return ServingResult(status=self.policy.on_invalid, feature_vector=fv, reason="quality_flags", staleness_seconds=staleness)
        return ServingResult(status="ok", feature_vector=fv, staleness_seconds=staleness)

    def get_point_in_time(self, *, symbol: str, decision_ts: datetime, feature_set_name: str, feature_set_version: str) -> ServingResult:
        if self.offline_store is None:
            return ServingResult(status="fail", feature_vector=None, reason="offline_store_unavailable")
        fv = self.offline_store.get_point_in_time(symbol=symbol, decision_ts=decision_ts, feature_set_name=feature_set_name, feature_set_version=feature_set_version)
        if fv is None:
            return ServingResult(status="fail" if self.policy.on_missing == "fail" else "degrade", feature_vector=None, reason="point_in_time_missing")
        if fv.quality_flags:
            return ServingResult(status=self.policy.on_invalid, feature_vector=fv, reason="quality_flags")
        return ServingResult(status="ok", feature_vector=fv)
