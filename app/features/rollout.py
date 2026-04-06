from __future__ import annotations

import hashlib
from dataclasses import dataclass
from datetime import datetime

from app.features.serving import FeatureServingService, ServingResult


@dataclass(frozen=True)
class CanaryDecision:
    route: str
    version: str


@dataclass(frozen=True)
class CanaryServingResult:
    decision: CanaryDecision
    result: ServingResult


class FeatureRolloutController:
    def __init__(self, *, canary_fraction: float = 0.0) -> None:
        self.canary_fraction = max(0.0, min(canary_fraction, 1.0))

    def choose_version(
        self,
        *,
        feature_set_name: str,
        active_version: str,
        canary_version: str | None,
        scope: str,
    ) -> CanaryDecision:
        if not canary_version or self.canary_fraction <= 0:
            return CanaryDecision(route="active", version=active_version)
        token = hashlib.sha256(f"{feature_set_name}|{scope}".encode("utf-8")).hexdigest()
        bucket = int(token[:8], 16) / 0xFFFFFFFF
        if bucket < self.canary_fraction:
            return CanaryDecision(route="canary", version=canary_version)
        return CanaryDecision(route="active", version=active_version)


class CanaryServingService:
    def __init__(
        self,
        *,
        primary: FeatureServingService,
        canary: FeatureServingService,
        rollout: FeatureRolloutController,
        canary_version: str,
    ) -> None:
        self.primary = primary
        self.canary = canary
        self.rollout = rollout
        self.canary_version = canary_version

    def get_latest_servable(
        self,
        *,
        feature_set_name: str,
        active_version: str,
        symbol: str,
        decision_ts: datetime,
        entity_keys: dict[str, str] | None = None,
    ) -> CanaryServingResult:
        scope = symbol if entity_keys is None else "|".join(f"{key}={value}" for key, value in sorted(entity_keys.items()))
        decision = self.rollout.choose_version(
            feature_set_name=feature_set_name,
            active_version=active_version,
            canary_version=self.canary_version,
            scope=scope,
        )
        service = self.canary if decision.route == "canary" else self.primary
        result = service.get_latest_servable(
            symbol=symbol,
            entity_keys=entity_keys,
            decision_ts=decision_ts,
            feature_set_name=feature_set_name,
            feature_set_version=decision.version,
        )
        return CanaryServingResult(decision=decision, result=result)
