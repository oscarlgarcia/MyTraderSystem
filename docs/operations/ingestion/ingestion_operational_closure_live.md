# Ingestion Operational Closure Playbook - Live

## Objetivo
Ejecutar el cierre operativo estandar de ingestion para `live` sobre el scope soportado hoy y dejar actualizados los artefactos de gobierno del dato, quality, service levels, gap fill, serving, publication y lifecycle que ahora forman parte del baseline promotable.

## Scope soportado
- feeds soportados: `trade`, `kline`
- feed excluido del cierre live: `book`
- expansion futura catalogada pero no promotable hoy: `bookTicker -> quotes/book`
- `trade live` mantiene continuidad sobre `aggregate trade`:
  - websocket: `@aggTrade`
  - cursor: `aggregate_trade_id`
  - recovery REST: `/api/v3/aggTrades`
- el cierre final solo acepta `channel = scheduled` o `channel = pipeline`

## Prerrequisitos
- entorno del ejemplo: `dev`
- ejecutar desde la raiz del repo con `poetry run python` o con el `python` operativo equivalente
- disponer de raw y normalized exactos del dataset candidato
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
  - cutover
- si se va a refrescar catalogo/quality/serving/publication, tener activos:
  - `python -m app.controlplane.api --env dev --host 127.0.0.1 --port 8000`
  - `python -m app.controlplane.worker --env dev`
- disponer de conectividad para que el runtime pueda reabrir feeds si la `subscription revision` cambia durante el cierre

## Variables del caso estandar
- `output_dir`: `docs/validation/operational/live`
- `runner_id`: `ingestion-live-closure`
- `trigger`: `scheduled_live_cycle`
- `provenance_source`: `ingestion_operational_cycle`
- `runner_context_path`: `ops/runner-context/live-dev.json`
- `surface_manifest_path`: `ops/observability/live-dev-surfaces.json`

## Comando de ejecucion

```powershell
poetry run python scripts/ingestion_operational_cycle.py `
  --target live `
  --env dev `
  --runtime-env dev `
  --runtime-base-dir data/dev `
  --raw-base-dir data/dev/raw `
  --normalized-path trade=data/dev/normalized/trades/env=dev/venue=BINANCE/symbol=BTCUSDT/date=2026-04-09 `
  --normalized-path kline=data/dev/normalized/bars/env=dev/venue=BINANCE/symbol=BTCUSDT/date=2026-04-09 `
  --symbol BTCUSDT `
  --stream-types trade,kline `
  --interval 1m `
  --output-dir docs/validation/operational/live `
  --runner-id ingestion-live-closure `
  --trigger scheduled_live_cycle `
  --provenance-source ingestion_operational_cycle `
  --runner-context-path ops/runner-context/live-dev.json `
  --surface-manifest ops/observability/live-dev-surfaces.json
```

## Cierre posterior obligatorio
Tras un cierre live verde hay que refrescar la capa nueva de gobierno y serving. El orden importa porque `service levels`, `gap fill` y `publication` dependen de contracts, quality y curated serving ya materializados. Ejecutar:

```powershell
Invoke-RestMethod -Method Post -Uri 'http://127.0.0.1:8000/api/commands/catalog-refresh' -Form @{ requested_by = 'ingestion-live-closure' }
Invoke-RestMethod -Method Post -Uri 'http://127.0.0.1:8000/api/commands/quality-refresh' -Form @{ requested_by = 'ingestion-live-closure' }
Invoke-RestMethod -Method Post -Uri 'http://127.0.0.1:8000/api/commands/service-levels-refresh' -Form @{ requested_by = 'ingestion-live-closure' }
Invoke-RestMethod -Method Post -Uri 'http://127.0.0.1:8000/api/commands/gap-fill' -Form @{ requested_by = 'ingestion-live-closure' }
Invoke-RestMethod -Method Post -Uri 'http://127.0.0.1:8000/api/commands/curated-refresh' -Form @{ stream_type = 'trade'; symbol = 'BTCUSDT'; requested_by = 'ingestion-live-closure' }
Invoke-RestMethod -Method Post -Uri 'http://127.0.0.1:8000/api/commands/curated-refresh' -Form @{ stream_type = 'kline'; symbol = 'BTCUSDT'; requested_by = 'ingestion-live-closure' }
Invoke-RestMethod -Method Post -Uri 'http://127.0.0.1:8000/api/commands/benchmark-serving' -Form @{ stream_type = 'trade'; symbol = 'BTCUSDT'; requested_by = 'ingestion-live-closure' }
Invoke-RestMethod -Method Post -Uri 'http://127.0.0.1:8000/api/commands/benchmark-serving' -Form @{ stream_type = 'kline'; symbol = 'BTCUSDT'; requested_by = 'ingestion-live-closure' }
Invoke-RestMethod -Method Post -Uri 'http://127.0.0.1:8000/api/commands/publish-snapshot' -Form @{ stream_type = 'trade'; symbol = 'BTCUSDT'; requested_by = 'ingestion-live-closure' }
Invoke-RestMethod -Method Post -Uri 'http://127.0.0.1:8000/api/commands/publish-snapshot' -Form @{ stream_type = 'kline'; symbol = 'BTCUSDT'; requested_by = 'ingestion-live-closure' }
Invoke-RestMethod -Method Post -Uri 'http://127.0.0.1:8000/api/commands/storage-lifecycle-apply' -Form @{ requested_by = 'ingestion-live-closure' }
```

## Artefactos esperados
- cierre operativo:
  - `docs/validation/operational/live/ingestion_operational_cycle_live.json`
  - `docs/validation/operational/live/ingestion_readiness_live_trade.json`
  - `docs/validation/operational/live/ingestion_readiness_live_kline.json`
  - `docs/validation/operational/live/ingestion_operational_governance_live.json`
  - `docs/validation/operational/live/ingestion_operational_history_live.jsonl`
  - `docs/validation/operational/live/ingestion_observability_verification_live_trade.json`
  - `docs/validation/operational/live/ingestion_observability_verification_live_kline.json`
  - `docs/validation/operational/live/ingestion_operational_evidence_pre_drill_live_trade.json`
  - `docs/validation/operational/live/ingestion_operational_evidence_pre_drill_live_kline.json`
  - `docs/validation/operational/live/ingestion_operational_evidence_live_trade.json`
  - `docs/validation/operational/live/ingestion_operational_evidence_live_kline.json`
  - `docs/validation/operational/live/ingestion_release_gates_live_trade.json`
  - `docs/validation/operational/live/ingestion_release_gates_live_kline.json`
  - `docs/validation/operational/live/ingestion_live_drill_report_live_trade.json`
  - `docs/validation/operational/live/ingestion_live_drill_report_live_kline.json`
- catalogo y contracts:
  - `data/dev/catalog/datasets.json`
  - `data/dev/catalog/dataset-contracts.json`
  - `data/dev/catalog/dataset-quality.json`
  - `data/dev/catalog/dataset-incidents.jsonl`
  - `data/dev/catalog/dataset-service-levels.json`
  - `data/dev/catalog/gap-fill-plan.json`
  - `data/dev/catalog/venue-capabilities.json`
  - `data/dev/catalog/delivery-contracts.json`
  - `data/dev/catalog/storage-lifecycle.json`
  - `data/dev/catalog/storage-lifecycle-actions.json`
  - `data/dev/catalog/security-baseline.json`
  - `data/dev/catalog/future-scope.json`
- serving/publication:
  - `data/dev/serving/marketdata.sqlite`
  - `data/dev/publication/venue=BINANCE/stream_type=trade/events.jsonl`
  - `data/dev/publication/venue=BINANCE/stream_type=kline/events.jsonl`

## Que verificar exactamente
1. El ciclo live termina con `overall_status = PASS` y `channel != manual`.
2. `trade` y `kline` tienen evidence final fresca y `book` sigue marcado como `excluded`.
3. `data/dev/catalog/datasets.json` y `data/dev/catalog/dataset-contracts.json` existen y contienen `trade` y `kline`.
4. `data/dev/catalog/dataset-quality.json` refleja score materializado y `data/dev/catalog/dataset-incidents.jsonl` existe aunque no haya incidentes relevantes.
5. `data/dev/catalog/dataset-service-levels.json` existe y ambos feeds quedan en `within_slo` o con una desviacion justificada antes del `GO`.
6. `data/dev/catalog/gap-fill-plan.json` no deja ventanas abiertas sin accion o evidencia de aceptacion operativa.
7. `data/dev/serving/marketdata.sqlite` existe y fue refrescado despues del cierre; el latest curado preserva `dataset_version` y `lineage_id`.
8. Los benchmarks de serving para `trade` y `kline` devolvieron `pass_ok = true`, con `snapshot_hit_ok = true` y `publication_scan_ok = true`.
9. Se publicaron snapshots en `data/dev/publication/...` para ambos feeds, incluyendo `delivery_contract_version` y lineage reproducible.
10. `data/dev/catalog/storage-lifecycle-actions.json` existe aunque no aplique ninguna accion destructiva; el cierre debe dejar evidencia del lifecycle evaluado.

## Criterio de decision
- `GO`
  - ambos perfiles live en `PASS`
  - catalogo, contracts, quality, service levels, gap fill y curated serving refrescados
  - publication actualizada con lineage/versionado
  - observabilidad externa verificada
  - live drill verde
- `NO-GO`
  - cualquier artefacto operativo faltante
  - falta de refresh de catalogo/quality/service levels/gap fill/curated tras el cierre
  - `marketdata.sqlite` ausente o benchmark de serving fallido
  - `service level` en `breached` sin excepcion operativa explicitada
  - huecos abiertos sin plan o sin evidencia de remediacion
  - `book` en scope o `channel = manual`

## Referencias
- promotion: `runbook://docs/operations/ingestion/ingestion_promotion_runbook.md`
- rollback: `runbook://docs/operations/ingestion/ingestion_rollback_checklist.md`
- happy path de produccion: `runbook://docs/operations/application/production_happy_path_runbook.md`
