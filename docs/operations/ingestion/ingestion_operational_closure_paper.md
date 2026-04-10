# Ingestion Operational Closure Playbook - Paper

## Objetivo
Ejecutar el cierre operativo estandar de ingestion para `paper` sobre el scope soportado hoy y refrescar la capa de catalogo, quality y curated serving que alimenta paper trading y research.

## Scope soportado
- feeds soportados: `trade`, `kline`
- feed excluido: `book`
- `trade` mantiene continuidad con `aggregate_trade_id`
- `manual` solo sirve para runs informativos; el cierre operativo final exige `scheduled` o `pipeline`

## Prerrequisitos
- entorno activo del ejemplo: `dev`
- ejecutar desde la raiz del repo con `poetry run python` o con el `python` operativo equivalente
- disponer de raw y normalized del dataset candidato
- disponer de `runner context` persistido con:
  - `execution_ref`
  - `channel`
  - `schedule_name`
  - `job_id`
  - `job_url`
- disponer de `surface manifest` persistido para:
  - runtime
  - alerts
  - logs
  - promotion
- si se va a refrescar catalogo/quality/serving, tener activos:
  - `python -m app.controlplane.api --env dev --host 127.0.0.1 --port 8000`
  - `python -m app.controlplane.worker --env dev`

## Variables del caso estandar
- `output_dir`: `docs/validation/operational/paper`
- `runner_id`: `ingestion-paper-closure`
- `trigger`: `scheduled_paper_cycle`
- `provenance_source`: `ingestion_operational_cycle`
- `runner_context_path`: `ops/runner-context/paper-dev.json`
- `surface_manifest_path`: `ops/observability/paper-dev-surfaces.json`

## Comando de ejecucion

```powershell
poetry run python scripts/ingestion_operational_cycle.py `
  --target paper `
  --env dev `
  --runtime-env dev `
  --runtime-base-dir data/dev `
  --raw-base-dir data/dev/raw `
  --normalized-path trade=data/dev/normalized/trades/env=dev/venue=BINANCE/symbol=BTCUSDT/date=2026-04-09 `
  --normalized-path kline=data/dev/normalized/bars/env=dev/venue=BINANCE/symbol=BTCUSDT/date=2026-04-09 `
  --symbol BTCUSDT `
  --stream-types trade,kline `
  --interval 1m `
  --output-dir docs/validation/operational/paper `
  --runner-id ingestion-paper-closure `
  --trigger scheduled_paper_cycle `
  --provenance-source ingestion_operational_cycle `
  --runner-context-path ops/runner-context/paper-dev.json `
  --surface-manifest ops/observability/paper-dev-surfaces.json `
  --benchmark-min-rows-per-second 1
```

## Cierre posterior obligatorio

```powershell
Invoke-RestMethod -Method Post -Uri 'http://127.0.0.1:8000/api/commands/catalog-refresh' -Form @{ requested_by = 'ingestion-paper-closure' }
Invoke-RestMethod -Method Post -Uri 'http://127.0.0.1:8000/api/commands/quality-refresh' -Form @{ requested_by = 'ingestion-paper-closure' }
Invoke-RestMethod -Method Post -Uri 'http://127.0.0.1:8000/api/commands/curated-refresh' -Form @{ stream_type = 'trade'; symbol = 'BTCUSDT'; requested_by = 'ingestion-paper-closure' }
Invoke-RestMethod -Method Post -Uri 'http://127.0.0.1:8000/api/commands/curated-refresh' -Form @{ stream_type = 'kline'; symbol = 'BTCUSDT'; requested_by = 'ingestion-paper-closure' }
Invoke-RestMethod -Method Post -Uri 'http://127.0.0.1:8000/api/commands/benchmark-serving' -Form @{ stream_type = 'trade'; symbol = 'BTCUSDT'; requested_by = 'ingestion-paper-closure' }
Invoke-RestMethod -Method Post -Uri 'http://127.0.0.1:8000/api/commands/benchmark-serving' -Form @{ stream_type = 'kline'; symbol = 'BTCUSDT'; requested_by = 'ingestion-paper-closure' }
Invoke-RestMethod -Method Post -Uri 'http://127.0.0.1:8000/api/commands/publish-snapshot' -Form @{ stream_type = 'trade'; symbol = 'BTCUSDT'; requested_by = 'ingestion-paper-closure' }
Invoke-RestMethod -Method Post -Uri 'http://127.0.0.1:8000/api/commands/publish-snapshot' -Form @{ stream_type = 'kline'; symbol = 'BTCUSDT'; requested_by = 'ingestion-paper-closure' }
```

## Artefactos esperados
- cierre operativo:
  - `docs/validation/operational/paper/ingestion_operational_cycle_paper.json`
  - `docs/validation/operational/paper/ingestion_readiness_paper_trade.json`
  - `docs/validation/operational/paper/ingestion_readiness_paper_kline.json`
  - `docs/validation/operational/paper/ingestion_operational_governance_paper.json`
  - `docs/validation/operational/paper/ingestion_operational_history_paper.jsonl`
  - `docs/validation/operational/paper/ingestion_observability_verification_paper_trade.json`
  - `docs/validation/operational/paper/ingestion_observability_verification_paper_kline.json`
  - `docs/validation/operational/paper/ingestion_operational_evidence_paper_trade.json`
  - `docs/validation/operational/paper/ingestion_operational_evidence_paper_kline.json`
  - `docs/validation/operational/paper/ingestion_release_gates_paper_trade.json`
  - `docs/validation/operational/paper/ingestion_release_gates_paper_kline.json`
- catalogo y serving:
  - `data/dev/catalog/datasets.json`
  - `data/dev/catalog/dataset-contracts.json`
  - `data/dev/catalog/dataset-quality.json`
  - `data/dev/catalog/dataset-incidents.jsonl`
  - `data/dev/catalog/venue-capabilities.json`
  - `data/dev/catalog/delivery-contracts.json`
  - `data/dev/serving/marketdata.sqlite`
  - `data/dev/publication/venue=BINANCE/stream_type=trade/events.jsonl`
  - `data/dev/publication/venue=BINANCE/stream_type=kline/events.jsonl`

## Que verificar exactamente
1. El cierre paper termina con `overall_status = PASS` y `channel != manual`.
2. `trade` y `kline` tienen release gates finales en verde.
3. `data/dev/catalog/datasets.json` y `data/dev/catalog/dataset-contracts.json` existen y documentan ambos feeds.
4. `data/dev/catalog/dataset-quality.json` existe y el refresh de quality no deja incidentes criticos nuevos.
5. `data/dev/serving/marketdata.sqlite` existe y contiene serving curado para `trade` y `kline`.
6. Los benchmarks de serving para ambos feeds devuelven `pass_ok = true`.
7. Los snapshots publicados existen en `data/dev/publication/...`.

## Criterio de decision
- `GO`
  - ambos perfiles paper en `PASS`
  - catalogo, quality y curated serving refrescados
  - publication actualizada
  - observabilidad externa verificada
- `NO-GO`
  - cualquier artefacto operativo faltante
  - falta de refresh de catalogo/quality/curated tras el cierre
  - `book` aparece en scope
  - benchmarks de serving fallidos

## Referencias
- promotion: `runbook://docs/operations/ingestion/ingestion_promotion_runbook.md`
- rollback: `runbook://docs/operations/ingestion/ingestion_rollback_checklist.md`
- happy path de paper: `runbook://docs/operations/application/paper_happy_path_runbook.md`
