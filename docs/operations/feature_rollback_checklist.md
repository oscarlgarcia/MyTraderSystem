# Feature Rollback Checklist

Canonical detailed checklist: `docs/operations/features/feature_rollback_checklist.md`.

## When Rollback Is Mandatory

- `shadow_failure_budget_exceeded`
- `training_serving_contract_not_validated`
- serving soak or concurrency evidence failing

## Immediate Actions

1. Freeze new feature promotions.
2. Preserve current release, shadow and serving evidence.

## Required Evidence To Preserve

- `docs/validation/feature_release_gates.json`
- `docs/validation/feature_shadow_summary.json`
- `docs/validation/feature_serving_soak.json`

## Rollback Procedure

1. Restore the previous approved feature release.
2. Validate that serving and contracts return to green.

## Retry Rules

- retry only after the failed artifact is regenerated and green again
