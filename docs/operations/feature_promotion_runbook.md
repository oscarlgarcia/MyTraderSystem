# Feature Promotion Runbook

## Prerequisites

- `docs/validation/feature_release_gates.json` generated and `PASS`
- `docs/validation/feature_observability.json` refreshed and reviewed
- `docs/validation/feature_shadow_summary.json` refreshed and within budget
- `docs/validation/feature_serving_soak.json` refreshed and within SLO
- `docs/validation/feature_serving_concurrency.json` refreshed and within SLO
- training bundle metadata registered and contract_validation passing

## Promotion Decision

- Promote only if parity, benchmark, observability, contract validation and training bundle checks are green
- For live, also require shadow, soak, concurrency and rollout audit evidence

## Live Readiness Gates

- `online_backend_not_live_ready` must not appear
- `observability_sink_not_live_ready` must not appear
- `training_serving_contract_not_validated` must not appear
- `shadow_failure_budget_exceeded` must not appear
- `invalid_ratio_budget_exceeded` must not appear

## Promotion Procedure

1. Refresh artifacts.
2. Run feature release gates.
3. Review shadow and serving artifacts.
4. Publish the feature release.
5. Monitor post-promotion metrics and decision audits.

## Abort Conditions

- stale artifact
- parity mismatch
- benchmark threshold breach
- contract_validation failure
- shadow divergence above budget
- serving soak or concurrency failure

## Artifact Freshness And Invalidation

- Artifacts older than the approved freshness window are invalid.
- Any schema change, training bundle change or serving backend change invalidates previous feature release evidence.

