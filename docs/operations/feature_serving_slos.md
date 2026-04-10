# Feature Serving SLOs

Canonical detailed document: `docs/operations/features/feature_serving_slos.md`.

## Paper Baseline

- `feature_serving_soak.json.pass_ok`
- `feature_serving_concurrency.json.pass_ok`

## Live Baseline

- `feature_release_gates.json.pass_ok`
- `live_readiness.pass_ok`
- shadow failure budget within limit
- invalid ratio budget within limit

## Rollback Triggers

- `feature_serving_soak.json.pass_ok` false
- `feature_serving_concurrency.json.pass_ok` false
- `feature_release_gates.json.pass_ok` false
- shadow failure budget exceeded
- invalid ratio budget exceeded
