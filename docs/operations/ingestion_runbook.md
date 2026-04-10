# Ingestion Runbook

Canonical detailed runbook: `docs/operations/ingestion/ingestion_runbook.md`.

## Scope

- `live` support today is `trade` + `kline`.
- `paper` support today is `trade` + `kline`, with `trade` backed by `replay`.
- `book` remains excluded from the approved paper and live contract until dedicated runtime, recovery and serving evidence exist.
- `exact recovery` is required for promotable live feeds.
- `ingestion_operational_evidence` is the release-facing artifact family used to prove operational readiness.

## Operational Notes

- Use `kline` as the default live bootstrap feed.
- Use `trade` when replay and exact recovery evidence are present.
- Do not promote `book` using the current operational contract.
- Promotion and rollback procedures live in:
  - `docs/operations/ingestion_promotion_runbook.md`
  - `docs/operations/ingestion_rollback_checklist.md`
