# Ingestion Promotion Runbook

Canonical detailed runbook: `docs/operations/ingestion/ingestion_promotion_runbook.md`.

## Prerequisites

- `docs/validation/approved_ingestion_datasets.json`
- `docs/validation/ingestion_release_gates.json`
- `docs/validation/ingestion_live_drill_report.json`
- `docs/validation/ingestion_failure_injection.json`
- `ingestion_operational_evidence`
- latest `paper` closure reviewed and approved before any `live` promotion
- the promotable scope remains `trade` + `kline`
- `trade` requires `replay` evidence and exact recovery evidence
- `book` remains excluded from the supported live runtime contract

## Promotion Decision

- Promote only when `trade` + `kline` are green across contracts, quality, serving and release gates.
- Promotion stays blocked for `book`, stale artifact conditions, manifest mismatch or material provider metadata drift.

## Allowed Waivers

- minor informational alerts that do not change delivery semantics
- non-material metadata deltas with explicit approval

## Forbidden Waivers

- stale artifact
- manifest mismatch
- material provider metadata drift
- failed strict contract checks

## Promotion Procedure

1. Refresh dataset catalog, quality, service levels and curated serving.
2. Verify publication outputs and snapshot freshness for `trade` and `kline`.
3. Review `docs/validation/ingestion_release_gates.json`.
4. Review `docs/validation/ingestion_live_drill_report.json`.
5. Review `docs/validation/ingestion_failure_injection.json`.
6. Approve promotion only if the validated contract stays strict.

## Abort Conditions

- stale artifact
- manifest mismatch
- material provider metadata drift
- failed strict delivery or quality evidence

## Rollback Trigger

- any condition from the rollback checklist
- live evidence turning red after decision but before cutover

## Artifact Freshness And Invalidation

- artifacts older than the approved window are stale artifact inputs
- regenerated evidence is mandatory after schema changes, vendor changes or serving changes
- strict evidence wins over cached or manually copied artifacts
