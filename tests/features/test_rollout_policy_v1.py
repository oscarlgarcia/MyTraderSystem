from app.features.rollout import RolloutPromotionPolicy, evaluate_rollout_promotion


def test_rollout_policy_promotes_only_when_budgets_are_met():
    policy = RolloutPromotionPolicy(max_shadow_failures=0, max_invalid_ratio=0.05, min_audited_requests=2)
    ok = evaluate_rollout_promotion(
        policy=policy,
        audited_requests=3,
        shadow_failures=0,
        invalid_ratio=0.01,
        benchmark_pass_ok=True,
    )
    assert ok.pass_ok is True
    assert ok.action == "promote"

    blocked = evaluate_rollout_promotion(
        policy=policy,
        audited_requests=1,
        shadow_failures=1,
        invalid_ratio=0.2,
        benchmark_pass_ok=False,
    )
    assert blocked.pass_ok is False
    assert blocked.action == "rollback"
    assert "shadow_failure_budget_exceeded" in blocked.reasons
