# Ingestion Rollback Checklist

Canonical detailed checklist: `docs/operations/ingestion/ingestion_rollback_checklist.md`.

## When Rollback Is Mandatory

- `gap_irreparable`
- `shadow_semantic_diff`
- `compaction_failure_detected`
- `provider_metadata_drift`
- catalog, serving or publication no longer match the approved baseline

## Immediate Actions

1. Freeze new promotion or cutover activity.
2. Preserve the failing artifacts before regeneration.
3. Snapshot the current curated serving and publication state.

## Required Evidence To Preserve

- `docs/validation/ingestion_release_gates.json`
- `docs/validation/ingestion_live_drill_report.json`
- dataset contracts, quality reports and incident logs
- publication outputs and curated serving evidence

## Rollback Procedure

1. Restore the previously approved ingestion baseline.
2. Re-run catalog, quality, curated and publication refresh commands.
3. Re-check the restored baseline against release gates.

## Retry Rules

- do not retry until the triggering condition is removed
- do not retry on stale artifacts are invalidated and regenerated
- do not retry while quality, serving or publication remain inconsistent

## Artifact Invalidation Rules

- stale artifacts are invalidated and regenerated
- any schema, connector or metadata drift invalidates the previous promotion evidence

## Exit Criteria

- approved baseline restored
- evidence preserved and linked to the incident
- refreshed gates green again before any new promotion attempt
