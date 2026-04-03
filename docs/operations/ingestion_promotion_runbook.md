# Ingestion Promotion Runbook

## Scope

- This runbook governs promotion of the ingestion module from paper validation to live.
- Live promotion is allowed only for the supported live scope declared in `app/marketdata/support_matrix.py`.
- The supported live scope is `kline`-only. `trade` and `book` are not valid live promotion targets.
- Promotion is blocked unless the current target is `live` and every required artifact is fresh and `PASS`.

## Required Inputs

- `docs/validation/approved_ingestion_datasets.json`
- `docs/validation/ingestion_replay_parity.json`
- `docs/validation/ingestion_canary_report.json`
- `docs/validation/ingestion_ws_canary_report.json`
- `docs/validation/ingestion_storage_benchmark.json`
- `docs/validation/ingestion_vendor_contracts.json`
- `docs/validation/ingestion_soak_evidence.json`
- `docs/validation/ingestion_failure_injection.json`
- `docs/validation/ingestion_release_gates.json`
- `docs/validation/ingestion_live_drill_report.json`
- `docs/operations/ingestion_rollback_checklist.md`
- `docs/ops/live_cutover.md`

## Artifact Freshness And Invalidation

- Every required validation artifact must be generated within the active TTL window enforced by gates and drill checks.
- An artifact is stale if:
  - it is older than the current TTL,
  - it was generated for a different target or dataset than the one being promoted,
  - any upstream dependency was regenerated after the artifact was produced,
  - a waiver attached to the artifact expired,
  - a relevant config, metadata, schema, manifest, or contract changed after the artifact was produced.
- A stale artifact is invalid. It must not be reused for live promotion.
- After any failed attempt, rerun the affected validation step and replace the stale artifact before retrying.

## Prerequisites

Promotion may start only if all of the following are true:

1. The dataset to be promoted is registered in `docs/validation/approved_ingestion_datasets.json`.
2. Dataset promotion for the target dataset passed in `strict` mode.
3. Replay parity passed with `pass_ok = true`, `order_match = true`, and `manifest_ok = true`.
4. REST canary passed and is fresh.
5. WS canary passed and is fresh.
6. Storage benchmark passed and is fresh.
7. Vendor contracts passed and are fresh.
8. Soak evidence passed and is fresh.
9. Failure injection evidence passed and is fresh.
10. Release gates passed for `target = live`.
11. Live drill passed with:
    - `overall_status = PASS`
    - `promote_ready = true`
    - `rollback_ready = true`
12. Rollback checklist and live cutover runbook exist and match the current promotion flow.

## Promotion Decision

Promote only if all prerequisites are satisfied and no critical alert is active.

Abort promotion immediately if any of the following occurs:

- any required artifact is missing,
- any required artifact is stale,
- any required artifact is not `PASS`,
- release gates are not `PASS` for `live`,
- live drill is not `PASS`,
- provider metadata drift is material and unresolved,
- manifest mismatch or replay parity mismatch is present,
- a critical alert is active during the promotion window.

## Allowed Waivers

A waiver is allowed only when all of the following are true:

- it is explicit,
- it is time-bounded,
- it identifies the exact artifact or decision being waived,
- it states the accepted risk,
- it records the approver,
- it records an expiration timestamp,
- it does not contradict a forbidden waiver rule below.

Allowed waiver examples:

- extending a non-live informational note in the promotion record,
- delaying a non-blocking documentation refresh after a successful promotion,
- accepting a non-critical warning that is outside the live gate criteria and explicitly documented.

## Forbidden Waivers

Waivers are forbidden for:

- missing required artifacts,
- stale required artifacts,
- manifest mismatch,
- replay parity failure,
- failed dataset promotion in `strict`,
- failed release gates,
- failed live drill,
- failed failure injection evidence,
- material provider metadata drift,
- unsupported live scope,
- any critical alert that requires abort or rollback.

## Promotion Procedure

1. Confirm the exact dataset and live scope being promoted.
2. Verify the dataset is listed in `docs/validation/approved_ingestion_datasets.json`.
3. Review `docs/validation/ingestion_replay_parity.json`.
4. Review `docs/validation/ingestion_canary_report.json`.
5. Review `docs/validation/ingestion_ws_canary_report.json`.
6. Review `docs/validation/ingestion_storage_benchmark.json`.
7. Review `docs/validation/ingestion_vendor_contracts.json`.
8. Review `docs/validation/ingestion_soak_evidence.json`.
9. Review `docs/validation/ingestion_failure_injection.json`.
10. Run live release gates and confirm `overall_status = PASS`.
11. Run the live drill and confirm `overall_status = PASS`.
12. Promote only after every check above is fresh, consistent, and green.

## Required Commands

Run these commands from the project root in the production-ready environment:

```powershell
docker compose exec app poetry run python scripts/promote_ingestion_dataset.py --target live --contract-mode strict
docker compose exec app poetry run python scripts/check_replay_parity.py
docker compose exec app poetry run python scripts/ingestion_canary.py --mode rest-baseline
docker compose exec app poetry run python scripts/ingestion_ws_canary.py --symbol BTCUSDT --stream-type kline --interval 1m
docker compose exec app poetry run python scripts/ingestion_storage_benchmark.py
docker compose exec app poetry run python scripts/ingestion_vendor_contracts.py
docker compose exec app poetry run python scripts/ingestion_soak.py --mode ws-live --symbol BTCUSDT --stream-type kline --interval 1m
docker compose exec app poetry run python scripts/ingestion_failure_injection.py
docker compose exec app poetry run python scripts/ingestion_release_gates.py --target live
docker compose exec app poetry run python scripts/ingestion_live_drill.py --env dev
```

## Abort Conditions

Do not continue promotion if any of the following is true:

- a required artifact is missing or stale,
- `pass_ok != true` on any required artifact,
- live drill shows `promote_ready = false`,
- release gates show `pass_ok = false`,
- a critical alert requires rollback,
- a waiver request touches a forbidden waiver rule.

## Rollback Trigger

If promotion was started and any abort condition appears during or after cutover:

1. freeze promotion immediately,
2. keep the current evidence set,
3. execute `docs/operations/ingestion_rollback_checklist.md`,
4. do not retry until stale or failed artifacts are regenerated and reviewed.

## Promotion Record

Each promotion attempt must retain:

- the exact commands executed,
- the artifact set used for the decision,
- the target dataset and scope,
- any waiver record,
- the promote / abort / rollback decision,
- the approver and timestamp.
