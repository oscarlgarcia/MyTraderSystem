from __future__ import annotations

import logging
import time
from dataclasses import dataclass
from datetime import datetime
from typing import Optional

from app.common.dto import FeatureVector
from app.features.metrics import FeatureMetrics
from app.features.offline_store import OfflineFeatureStore
from app.features.online_store_base import FeatureOnlineStore
from app.features.releases import FeatureReleaseRegistry
from app.features.validation_profiles import get_validation_profile


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
        online_store: FeatureOnlineStore,
        offline_store: OfflineFeatureStore | None = None,
        policy: ServingPolicy | None = None,
        metrics: FeatureMetrics | None = None,
        logger: logging.Logger | None = None,
        release_registry: FeatureReleaseRegistry | None = None,
        release_registry_path: str | None = None,
    ) -> None:
        self.online_store = online_store
        self.offline_store = offline_store
        self.policy = policy or ServingPolicy()
        self.metrics = metrics or FeatureMetrics()
        self.logger = logger or logging.getLogger("features.serving")
        self.release_registry = release_registry or (FeatureReleaseRegistry(release_registry_path) if release_registry_path else None)

    def _resolve_version(self, *, feature_set_name: str, feature_set_version: str | None) -> str:
        if feature_set_version:
            return feature_set_version
        if self.release_registry is None:
            raise ValueError("feature_set_version is required when no release registry is configured")
        released = self.release_registry.get(feature_set_name)
        if released is None:
            raise ValueError(f"no active release found for feature set {feature_set_name}")
        return released.active_version

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

    def get_latest_servable(
        self,
        *,
        decision_ts: datetime,
        feature_set_name: str,
        feature_set_version: str | None = None,
        symbol: str | None = None,
        entity_keys: dict[str, str] | None = None,
    ) -> ServingResult:
        start = time.perf_counter()
        resolved_version = self._resolve_version(feature_set_name=feature_set_name, feature_set_version=feature_set_version)
        fv = self.online_store.get_latest_servable(
            decision_ts=decision_ts,
            feature_set_name=feature_set_name,
            feature_set_version=resolved_version,
            symbol=symbol,
            entity_keys=entity_keys,
        )
        resolved_symbol = symbol or (entity_keys or {}).get("symbol", "")
        if fv is None:
            result = ServingResult(
                status="fail" if self.policy.on_missing == "fail" else "degrade",
                feature_vector=None,
                reason="missing_or_future",
            )
            return self._record_result(
                symbol=resolved_symbol,
                decision_ts=decision_ts,
                feature_set_name=feature_set_name,
                feature_set_version=resolved_version,
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
                symbol=resolved_symbol or fv.symbol,
                decision_ts=decision_ts,
                feature_set_name=feature_set_name,
                feature_set_version=resolved_version,
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
                symbol=resolved_symbol or fv.symbol,
                decision_ts=decision_ts,
                feature_set_name=feature_set_name,
                feature_set_version=resolved_version,
                result=result,
                elapsed=time.perf_counter() - start,
            )
        profile = get_validation_profile("paper")
        invalid_ratio = self.metrics.invalid_serves / max(self.metrics.serving_requests, 1)
        if invalid_ratio > profile.max_invalid_ratio:
            result = ServingResult(
                status="degrade",
                feature_vector=fv,
                reason="invalid_ratio",
                staleness_seconds=staleness,
            )
            return self._record_result(
                symbol=resolved_symbol or fv.symbol,
                decision_ts=decision_ts,
                feature_set_name=feature_set_name,
                feature_set_version=resolved_version,
                result=result,
                elapsed=time.perf_counter() - start,
            )
        result = ServingResult(status="ok", feature_vector=fv, staleness_seconds=staleness)
        return self._record_result(
            symbol=resolved_symbol or fv.symbol,
            decision_ts=decision_ts,
            feature_set_name=feature_set_name,
            feature_set_version=resolved_version,
            result=result,
            elapsed=time.perf_counter() - start,
        )

    def get_point_in_time(
        self,
        *,
        decision_ts: datetime,
        feature_set_name: str,
        feature_set_version: str | None = None,
        symbol: str | None = None,
        entity_keys: dict[str, str] | None = None,
    ) -> ServingResult:
        start = time.perf_counter()
        resolved_version = self._resolve_version(feature_set_name=feature_set_name, feature_set_version=feature_set_version)
        resolved_symbol = symbol or (entity_keys or {}).get("symbol", "")
        if self.offline_store is None:
            result = ServingResult(status="fail", feature_vector=None, reason="offline_store_unavailable")
            return self._record_result(
                symbol=resolved_symbol,
                decision_ts=decision_ts,
                feature_set_name=feature_set_name,
                feature_set_version=resolved_version,
                result=result,
                elapsed=time.perf_counter() - start,
            )
        fv = self.offline_store.get_point_in_time(
            decision_ts=decision_ts,
            feature_set_name=feature_set_name,
            feature_set_version=resolved_version,
            symbol=symbol,
            entity_keys=entity_keys,
        )
        if fv is None:
            result = ServingResult(
                status="fail" if self.policy.on_missing == "fail" else "degrade",
                feature_vector=None,
                reason="point_in_time_missing",
            )
            return self._record_result(
                symbol=resolved_symbol,
                decision_ts=decision_ts,
                feature_set_name=feature_set_name,
                feature_set_version=resolved_version,
                result=result,
                elapsed=time.perf_counter() - start,
            )
        if fv.quality_flags:
            result = ServingResult(status=self.policy.on_invalid, feature_vector=fv, reason="quality_flags")
            return self._record_result(
                symbol=resolved_symbol or fv.symbol,
                decision_ts=decision_ts,
                feature_set_name=feature_set_name,
                feature_set_version=resolved_version,
                result=result,
                elapsed=time.perf_counter() - start,
            )
        result = ServingResult(status="ok", feature_vector=fv)
        return self._record_result(
            symbol=resolved_symbol or fv.symbol,
            decision_ts=decision_ts,
            feature_set_name=feature_set_name,
            feature_set_version=resolved_version,
            result=result,
            elapsed=time.perf_counter() - start,
        )

    def get_recent_history(
        self,
        *,
        feature_set_name: str,
        feature_set_version: str,
        limit: int = 10,
        symbol: str | None = None,
        entity_keys: dict[str, str] | None = None,
    ) -> list[FeatureVector]:
        return self.online_store.get_recent_history(
            feature_set_name=feature_set_name,
            feature_set_version=feature_set_version,
            limit=limit,
            symbol=symbol,
            entity_keys=entity_keys,
        )

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
        return self.online_store.get_history_range(
            feature_set_name=feature_set_name,
            feature_set_version=feature_set_version,
            start_ts=start_ts,
            end_ts=end_ts,
            symbol=symbol,
            entity_keys=entity_keys,
        )

    def get_snapshot_before(
        self,
        *,
        cutoff_ts: datetime,
        feature_set_name: str,
        feature_set_version: str,
        symbol: str | None = None,
        entity_keys: dict[str, str] | None = None,
    ) -> FeatureVector | None:
        return self.online_store.get_snapshot_before(
            cutoff_ts=cutoff_ts,
            feature_set_name=feature_set_name,
            feature_set_version=feature_set_version,
            symbol=symbol,
            entity_keys=entity_keys,
        )
