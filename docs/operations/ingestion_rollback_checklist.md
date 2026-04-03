# Ingestion Rollback Checklist

## When Rollback Is Mandatory

Rollback is mandatory if any of the following occurs during the live promotion window:

- `docs/validation/ingestion_release_gates.json` is not `PASS`
- `docs/validation/ingestion_live_drill_report.json` is not `PASS`
- `gap_irreparable` is active
- `shadow_semantic_diff` is active
- `compaction_failure_detected` is active
- `reconnect_storm` is active and unresolved
- `provider_metadata_drift` is material and unresolved
- replay parity, manifest integrity, or dataset promotion becomes invalid

## Immediate Actions

1. Freeze promotion immediately.
2. Stop any new live cutover steps.
3. Keep the previous live ingestion version as the active baseline.
4. Preserve all raw, normalized, and validation evidence from the failed window.
5. Record the rollback decision timestamp and operator.

## Required Evidence To Preserve

- `docs/validation/ingestion_release_gates.json`
- `docs/validation/ingestion_live_drill_report.json`
- `docs/validation/ingestion_failure_injection.json`
- `docs/validation/ingestion_canary_report.json`
- `docs/validation/ingestion_ws_canary_report.json`
- `docs/validation/ingestion_storage_benchmark.json`
- `docs/validation/ingestion_soak_evidence.json`
- `docs/validation/ingestion_replay_parity.json`
- `docs/validation/approved_ingestion_datasets.json`
- any relevant runtime logs and comparisons generated during the failed attempt

## Rollback Procedure

1. Mark the candidate promotion as aborted.
2. Revert to the previously approved live ingestion version and keep the candidate disabled.
3. Confirm the baseline run still satisfies the latest live support matrix.
4. Re-run the minimum health checks required to prove the baseline is safe:
   - REST canary
   - WS canary
   - release gates for the restored baseline if required by the incident scope
5. Confirm critical alerts are cleared or explicitly explained.
6. Open an incident record that references:
   - rollback trigger
   - impacted artifacts
   - dataset and scope
   - operator
   - next remediation step

## Retry Rules

Do not retry promotion until all of the following are true:

- the root cause is identified,
- stale artifacts are invalidated and regenerated,
- failed artifacts are regenerated and reviewed,
- waivers are re-approved if still relevant,
- release gates are green again,
- live drill is rerun and passes again.

## Artifact Invalidation Rules

Any artifact used by the failed promotion attempt is invalid for the next attempt if:

- it is older than the enforced TTL,
- it was generated before the rollback,
- the dataset, schema, manifest, metadata snapshot, or config changed after it was produced,
- a waiver tied to the artifact expired,
- the artifact was produced under a different target or scope.

## Exit Criteria

Rollback is considered complete only if all of the following are true:

- the previous approved live version is active,
- the candidate version is not serving live traffic,
- critical alerts are cleared or documented,
- rollback evidence is preserved,
- stale artifacts are marked invalid,
- the next promotion attempt starts from a fresh evidence set.
