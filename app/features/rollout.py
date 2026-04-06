from __future__ import annotations

import hashlib
from dataclasses import dataclass
from datetime import datetime

from app.features.entity_codec import entity_scope, normalize_entity_keys
from app.features.rollout_audit import CanaryAuditStore
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
        audit_store: CanaryAuditStore | None = None,
    ) -> None:
        self.primary = primary
        self.canary = canary
        self.rollout = rollout
        self.canary_version = canary_version
        self.audit_store = audit_store

    def get_latest_servable(
        self,
        *,
        feature_set_name: str,
        active_version: str,
        symbol: str,
        decision_ts: datetime,
        entity_keys: dict[str, str] | None = None,
    ) -> CanaryServingResult:
        normalized_keys = normalize_entity_keys(entity_keys, symbol=symbol)
        scope = entity_scope(normalized_keys, symbol=symbol)
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
        if self.audit_store is not None:
            self.audit_store.append(
                feature_set_name=feature_set_name,
                route=decision.route,
                version=decision.version,
                symbol=symbol,
                entity_scope=scope,
                decision_ts=decision_ts,
                status=result.status,
            )
        return CanaryServingResult(decision=decision, result=result)


@dataclass(frozen=True)
class RolloutPromotionPolicy:
    max_shadow_failures: int = 0
    max_invalid_ratio: float = 0.05
    min_audited_requests: int = 1
    require_benchmark_pass: bool = True


@dataclass(frozen=True)
class RolloutPromotionDecision:
    action: str
    pass_ok: bool
    reasons: tuple[str, ...]


def evaluate_rollout_promotion(
    *,
    policy: RolloutPromotionPolicy,
    audited_requests: int,
    shadow_failures: int,
    invalid_ratio: float,
    benchmark_pass_ok: bool,
) -> RolloutPromotionDecision:
    reasons: list[str] = []
    if audited_requests < policy.min_audited_requests:
        reasons.append("insufficient_audited_requests")
    if shadow_failures > policy.max_shadow_failures:
        reasons.append("shadow_failure_budget_exceeded")
    if invalid_ratio > policy.max_invalid_ratio:
        reasons.append("invalid_ratio_budget_exceeded")
    if policy.require_benchmark_pass and not benchmark_pass_ok:
        reasons.append("benchmark_thresholds_not_met")
    if reasons:
        action = "rollback" if ("shadow_failure_budget_exceeded" in reasons or "invalid_ratio_budget_exceeded" in reasons) else "hold"
        return RolloutPromotionDecision(action=action, pass_ok=False, reasons=tuple(reasons))
    return RolloutPromotionDecision(action="promote", pass_ok=True, reasons=())
