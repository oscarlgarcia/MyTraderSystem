from app.features.live_readiness import (
    FeatureLiveReadinessInputs,
    evaluate_feature_live_readiness,
)


def test_live_readiness_requires_non_local_backend_and_operational_controls():
    decision = evaluate_feature_live_readiness(
        inputs=FeatureLiveReadinessInputs(
            online_backend="local_sqlite",
            observability_sink="jsonl",
            serving_soak_pass_ok=False,
            rollout_audit_enabled=False,
            contract_validation_pass_ok=False,
            benchmark_pass_ok=False,
            shadow_failures=1,
            invalid_ratio=0.02,
        )
    )
    assert decision.pass_ok is False
    assert decision.action == "no_go"
    assert "online_backend_not_live_ready" in decision.reasons
    assert "observability_sink_not_live_ready" in decision.reasons
    assert "serving_soak_not_passed" in decision.reasons
    assert "training_serving_contract_not_validated" in decision.reasons


def test_live_readiness_passes_when_preconditions_are_met():
    decision = evaluate_feature_live_readiness(
        inputs=FeatureLiveReadinessInputs(
            online_backend="http",
            observability_sink="http",
            serving_soak_pass_ok=True,
            rollout_audit_enabled=True,
            contract_validation_pass_ok=True,
            benchmark_pass_ok=True,
            shadow_failures=0,
            invalid_ratio=0.0,
        )
    )
    assert decision.pass_ok is True
    assert decision.action == "go"
    assert decision.reasons == ()
