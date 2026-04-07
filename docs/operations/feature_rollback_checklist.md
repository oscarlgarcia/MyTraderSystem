# Feature Rollback Checklist

## When Rollback Is Mandatory

- `shadow_failure_budget_exceeded`
- `training_serving_contract_not_validated`
- parity mismatch after promotion
- sustained serving soak failure
- concurrency instability in the promoted version

## Immediate Actions

1. Freeze new promotions.
2. Capture current metrics and active feature release state.
3. Preserve recent decision audit and observability artifacts.

## Required Evidence To Preserve

- `docs/validation/feature_release_gates.json`
- `docs/validation/feature_shadow_summary.json`
- `docs/validation/feature_serving_soak.json`
- `docs/validation/feature_serving_concurrency.json`
- release audit trail and decision audit trail

## Rollback Procedure

1. Execute feature release rollback.
2. Confirm active version reverted.
3. Re-run smoke validation on the previous version.
4. Resume only after parity and contract checks recover.

## Retry Rules

- Do not retry the promotion until the failed artifact is regenerated.
- Do not retry if the training bundle or feature schema changed without new evidence.

