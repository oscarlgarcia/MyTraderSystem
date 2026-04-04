from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Optional

from app.common.dto import FeatureVector
from app.features.metrics import FeatureMetrics
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
    def __init__(
        self,
        *,
        online_store: OnlineFeatureStore,
        offline_store: OfflineFeatureStore | None = None,
        policy: ServingPolicy | None = None,
        metrics: FeatureMetrics | None = None,
        logger: logging.Logger | None = None,
    ) -> None:
        self.online_store = online_store
        self.offline_store = offline_store
        self.policy = policy or ServingPolicy()
        self.metrics = metrics or FeatureMetrics()
        self.logger = logger or logging.getLogger("features.serving")

    def _record_result(self, *, symbol: str, decision_ts: datetime, feature_set_name: str, feature_set_version: str, result: ServingResult, elapsed: float) -> ServingResult:
        self.metrics.serving_requests += 1
        self.metrics.serving_latency_total += elapsed
        self.metrics.serving_latency_max = max(self.metrics.serving_latency_max, elapsed)
        if result.status == "fail":
            self.metrics.serving_failures += 1
        if result.status == "degrade":
            self.metrics.serving_degraded += 1
        if result.reason == "stale":
            self.metrics.stale_serves += 1
        if result.reason == "quality_flags":
            self.metrics.invalid_serves += 1
        feature_vector = result.feature_vector
        self.logger.info(
            "feature serving request",
            extra={
                "symbol": symbol,
                "decision_ts": decision_ts.isoformat(),
                "feature_set_name": feature_set_name,
                "feature_set_version": feature_set_version,
                "status": result.status,
                "reason": result.reason,
                "staleness_seconds": result.staleness_seconds,
                "lineage_id": feature_vector.lineage_id if feature_vector else "",
                "quality_flags": list(feature_vector.quality_flags) if feature_vector else [],
                "elapsed_seconds": elapsed,
            },
        )
        return result

    def get_latest_servable(self, *, symbol: str, decision_ts: datetime, feature_set_name: str, feature_set_version: str) -> ServingResult:
        start = time.perf_counter()
        fv = self.online_store.get_latest_servable(
            symbol=symbol,
            decision_ts=decision_ts,
            feature_set_name=feature_set_name,
            feature_set_version=feature_set_version,
        )
        if fv is None:
            result = ServingResult(
                status="fail" if self.policy.on_missing == "fail" else "degrade",
                feature_vector=None,
                reason="missing_or_future",
            )
            return self._record_result(
                symbol=symbol,
                decision_ts=decision_ts,
                feature_set_name=feature_set_name,
                feature_set_version=feature_set_version,
                result=result,
                elapsed=time.perf_counter() - start,
            )
        staleness = max(0.0, (decision_ts - fv.available_ts).total_seconds())
        if staleness > self.policy.max_staleness_seconds:
            result = ServingResult(
                status="fail" if self.policy.on_missing == "fail" else "degrade",
                feature_vector=fv,
                reason="stale",
                staleness_seconds=staleness,
            )
            return self._record_result(
                symbol=symbol,
                decision_ts=decision_ts,
                feature_set_name=feature_set_name,
                feature_set_version=feature_set_version,
                result=result,
                elapsed=time.perf_counter() - start,
            )
        if fv.quality_flags:
            result = ServingResult(
                status=self.policy.on_invalid,
                feature_vector=fv,
                reason="quality_flags",
                staleness_seconds=staleness,
            )
            return self._record_result(
                symbol=symbol,
                decision_ts=decision_ts,
                feature_set_name=feature_set_name,
                feature_set_version=feature_set_version,
                result=result,
                elapsed=time.perf_counter() - start,
            )
        result = ServingResult(status="ok", feature_vector=fv, staleness_seconds=staleness)
        return self._record_result(
            symbol=symbol,
            decision_ts=decision_ts,
            feature_set_name=feature_set_name,
            feature_set_version=feature_set_version,
            result=result,
            elapsed=time.perf_counter() - start,
        )

    def get_point_in_time(self, *, symbol: str, decision_ts: datetime, feature_set_name: str, feature_set_version: str) -> ServingResult:
        start = time.perf_counter()
        if self.offline_store is None:
            result = ServingResult(status="fail", feature_vector=None, reason="offline_store_unavailable")
            return self._record_result(
                symbol=symbol,
                decision_ts=decision_ts,
                feature_set_name=feature_set_name,
                feature_set_version=feature_set_version,
                result=result,
                elapsed=time.perf_counter() - start,
            )
        fv = self.offline_store.get_point_in_time(
            symbol=symbol,
            decision_ts=decision_ts,
            feature_set_name=feature_set_name,
            feature_set_version=feature_set_version,
        )
        if fv is None:
            result = ServingResult(
                status="fail" if self.policy.on_missing == "fail" else "degrade",
                feature_vector=None,
                reason="point_in_time_missing",
            )
            return self._record_result(
                symbol=symbol,
                decision_ts=decision_ts,
                feature_set_name=feature_set_name,
                feature_set_version=feature_set_version,
                result=result,
                elapsed=time.perf_counter() - start,
            )
        if fv.quality_flags:
            result = ServingResult(status=self.policy.on_invalid, feature_vector=fv, reason="quality_flags")
            return self._record_result(
                symbol=symbol,
                decision_ts=decision_ts,
                feature_set_name=feature_set_name,
                feature_set_version=feature_set_version,
                result=result,
                elapsed=time.perf_counter() - start,
            )
        result = ServingResult(status="ok", feature_vector=fv)
        return self._record_result(
            symbol=symbol,
            decision_ts=decision_ts,
            feature_set_name=feature_set_name,
            feature_set_version=feature_set_version,
            result=result,
            elapsed=time.perf_counter() - start,
        )
