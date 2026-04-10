# Feature Promotion Runbook

Canonical detailed runbook: `docs/operations/features/feature_promotion_runbook.md`.

## Prerequisites

- `docs/validation/feature_release_gates.json`
- `docs/validation/feature_observability.json`
- `docs/validation/feature_shadow_summary.json`
- `docs/validation/feature_serving_soak.json`
- `docs/validation/feature_serving_concurrency.json`
- training bundle metadata present
- `contract_validation` green

## Promotion Decision

- promote only if release gates, observability, shadow and serving evidence are green

## Live Readiness Gates

- serving soak and concurrency green
- shadow divergence within budget
- contract validation green

## Promotion Procedure

1. Refresh artifacts.
2. Review gates and training bundle.
3. Approve live release only if the contract stays green.

## Abort Conditions

- stale artifact
- contract_validation failure
- serving or shadow evidence out of budget

## Artifact Freshness And Invalidation

- any training bundle or serving change invalidates prior evidence
