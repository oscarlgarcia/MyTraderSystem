from __future__ import annotations

from dataclasses import dataclass

from app.features.online_store_factory import LIVE_READY_ONLINE_BACKENDS


@dataclass(frozen=True)
class FeatureLiveReadinessPolicy:
    allowed_online_backends: tuple[str, ...] = LIVE_READY_ONLINE_BACKENDS
    allowed_observability_sinks: tuple[str, ...] = ("http",)
    max_shadow_failures: int = 0
    max_invalid_ratio: float = 0.01
    require_serving_soak: bool = True
    require_rollout_audit: bool = True
    require_contract_validation: bool = True
    require_benchmark_pass: bool = True


@dataclass(frozen=True)
class FeatureLiveReadinessInputs:
    online_backend: str
    observability_sink: str
    serving_soak_pass_ok: bool
    rollout_audit_enabled: bool
    contract_validation_pass_ok: bool
    benchmark_pass_ok: bool
    shadow_failures: int = 0
    invalid_ratio: float = 0.0


@dataclass(frozen=True)
class FeatureLiveReadinessDecision:
    pass_ok: bool
    action: str
    reasons: tuple[str, ...]


def evaluate_feature_live_readiness(
    *,
    inputs: FeatureLiveReadinessInputs,
    policy: FeatureLiveReadinessPolicy = FeatureLiveReadinessPolicy(),
) -> FeatureLiveReadinessDecision:
    reasons: list[str] = []
    if inputs.online_backend not in policy.allowed_online_backends:
        reasons.append("online_backend_not_live_ready")
    if inputs.observability_sink not in policy.allowed_observability_sinks:
        reasons.append("observability_sink_not_live_ready")
    if policy.require_serving_soak and not inputs.serving_soak_pass_ok:
        reasons.append("serving_soak_not_passed")
    if policy.require_rollout_audit and not inputs.rollout_audit_enabled:
        reasons.append("rollout_audit_not_enabled")
    if policy.require_contract_validation and not inputs.contract_validation_pass_ok:
        reasons.append("training_serving_contract_not_validated")
    if policy.require_benchmark_pass and not inputs.benchmark_pass_ok:
        reasons.append("feature_benchmark_not_passed")
    if inputs.shadow_failures > policy.max_shadow_failures:
        reasons.append("shadow_failure_budget_exceeded")
    if inputs.invalid_ratio > policy.max_invalid_ratio:
        reasons.append("invalid_ratio_budget_exceeded")
    if reasons:
        hard_failures = {
            "online_backend_not_live_ready",
            "observability_sink_not_live_ready",
            "serving_soak_not_passed",
            "training_serving_contract_not_validated",
            "invalid_ratio_budget_exceeded",
            "shadow_failure_budget_exceeded",
        }
        action = "no_go" if any(reason in hard_failures for reason in reasons) else "hold"
        return FeatureLiveReadinessDecision(pass_ok=False, action=action, reasons=tuple(reasons))
    return FeatureLiveReadinessDecision(pass_ok=True, action="go", reasons=())
