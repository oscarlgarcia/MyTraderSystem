# Ingestion Promotion Runbook

## Preconditions

- `docs/validation/ingestion_canary_report.json` exists and is fresh.
- `docs/validation/ingestion_ws_canary_report.json` exists and is fresh.
- `docs/validation/ingestion_replay_parity.json` exists and is fresh.
- `docs/validation/ingestion_storage_benchmark.json` exists and is fresh.
- `docs/validation/ingestion_soak_evidence.json` exists and is fresh for paper/live.
- `docs/validation/ingestion_vendor_contracts.json` exists and is fresh for paper/live.
- `docs/validation/ingestion_release_gates.json` is `PASS`.
- `docs/validation/ingestion_live_drill_report.json` is `PASS` for live promotion.

## Promotion Steps

1. Run dataset promotion in `strict` mode for each dataset that will feed backtesting or paper validation.
2. Run canary and ws-live canary artifacts.
3. Run replay parity artifact generation.
4. Run storage benchmark and soak validation.
5. Run vendor network contract tests.
6. Run release gates for the target environment.
7. For live, execute the live drill and rerun release gates.
8. Promote only if all required artifacts are fresh and `PASS`.

## Waiver Rules

- A waiver must be explicit, time-bounded, and recorded with:
  - artifact name,
  - reason,
  - risk accepted,
  - approver,
  - expiration time.
- No waiver is allowed for:
  - manifest mismatch,
  - replay parity failure,
  - material provider metadata drift,
  - failed live drill.

## Rollback

1. Stop promotion and mark the target run as invalid.
2. Preserve all artifacts and logs for the failed promotion window.
3. Execute `docs/operations/ingestion_rollback_checklist.md`.
4. Regenerate stale artifacts before any retry.
