# Ingestion Operational Closure Playbook - Live

## Objetivo
Ejecutar el caso estandar de cierre operativo de ingestion para `live` sobre el scope soportado hoy: `trade` + `kline`.

## Scope soportado
- feeds soportados: `trade`, `kline`
- feed excluido: `book`
- canal valido para promotion final: `scheduled` o `pipeline`
- `manual` solo sirve para simulacion o diagnostico y debe terminar en `NO-GO`

## Prerrequisitos
- entorno activo: `dev` para el ejemplo
- ejecutar desde la raiz del repo con `poetry`
- disponer de raw y normalized exactos del dataset candidato
- disponer de runtime base dir real para gates y shadow:
  - `data/dev`
- disponer de surfaces externas verificables para:
  - runtime
  - alerts
  - logs
  - promotion
  - cutover

## Variables del caso estandar
- `execution_ref`: `live-dev-btcusdt-20260409T100000Z`
- `channel`: `scheduled`
- `output_dir`: `docs/validation/operational/live`
- `runner_id`: `ingestion-live-closure`
- `trigger`: `scheduled_live_cycle`
- `provenance_source`: `ingestion_operational_cycle`

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
  --execution-ref live-dev-btcusdt-20260409T100000Z `
  --channel scheduled `
  --runtime-owner team-ingestion `
  --runtime-surface-ref grafana://ingestion/live/runtime `
  --runtime-verification-ref ops://live/runtime/20260409T100000Z `
  --alerts-owner team-ingestion-oncall `
  --alerts-surface-ref pagerduty://ingestion/live/alerts `
  --alerts-verification-ref ops://live/alerts/20260409T100000Z `
  --logs-owner team-observability `
  --logs-surface-ref loki://ingestion/live/logs `
  --logs-verification-ref ops://live/logs/20260409T100000Z `
  --promotion-owner team-ingestion `
  --promotion-surface-ref runbook://docs/operations/ingestion_promotion_runbook.md `
  --promotion-verification-ref ops://live/promotion/20260409T100000Z `
  --cutover-owner team-ingestion `
  --cutover-surface-ref runbook://docs/ops/live_cutover.md `
  --cutover-verification-ref ops://live/cutover/20260409T100000Z
```

## Salida esperada en consola
- linea inicial:
  - `ingestion operational cycle: PASS (live)`
- campos visibles:
  - `execution_ref: live-dev-btcusdt-20260409T100000Z`
  - `channel: scheduled`
  - `stream_types: trade, kline`
- pasos por perfil:
  - `live_trade: PASS`
  - `live_kline: PASS`

## Artefactos esperados
- manifest del ciclo:
  - `docs/validation/operational/live/ingestion_operational_cycle_live.json`
- reports de readiness:
  - `docs/validation/operational/live/ingestion_readiness_live_trade.json`
  - `docs/validation/operational/live/ingestion_readiness_live_kline.json`
- verificacion de observabilidad:
  - `docs/validation/operational/live/ingestion_observability_verification_live_trade.json`
  - `docs/validation/operational/live/ingestion_observability_verification_live_kline.json`
- evidence agregada y gates:
  - `docs/validation/operational/live/ingestion_operational_evidence_pre_drill_live_trade.json`
  - `docs/validation/operational/live/ingestion_operational_evidence_pre_drill_live_kline.json`
  - `docs/validation/operational/live/ingestion_operational_evidence_live_trade.json`
  - `docs/validation/operational/live/ingestion_operational_evidence_live_kline.json`
  - `docs/validation/operational/live/ingestion_release_gates_pre_drill_live_trade.json`
  - `docs/validation/operational/live/ingestion_release_gates_pre_drill_live_kline.json`
  - `docs/validation/operational/live/ingestion_release_gates_live_trade.json`
  - `docs/validation/operational/live/ingestion_release_gates_live_kline.json`
- drills y runtime evidence:
  - `docs/validation/operational/live/ingestion_live_drill_report_live_trade.json`
  - `docs/validation/operational/live/ingestion_live_drill_report_live_kline.json`
  - rest/ws canary, soak y failure injection por perfil

## Que verificar exactamente
1. Manifest del ciclo
- `overall_status = PASS`
- `pass_ok = true`
- `execution_ref = live-dev-btcusdt-20260409T100000Z`
- `channel = scheduled`
- `stream_types = ["trade", "kline"]`
- cada paso tiene `returncode = 0`

2. Observability verification por perfil
- `pass_ok = true`
- existen surfaces para:
  - `ingestion.live.runtime`
  - `ingestion.live.alerts`
  - `ingestion.live.logs`
  - `ingestion.live.promotion`
  - `ingestion.live.cutover`
- cada surface tiene:
  - `owner`
  - `surface_ref`
  - `verification_mode`
  - `verification_ref`
  - `verified_at`
  - `pass_ok = true`

3. Predrill evidence
- `target = live`
- `phase = predrill`
- `evidence_origin = operational_runtime`
- `provenance.execution_ref = live-dev-btcusdt-20260409T100000Z`
- `provenance.channel = scheduled`
- `observability.verification_artifact_path` presente
- `failure_injection` y `soak` presentes y frescos

4. Final evidence
- `target = live`
- `phase = final`
- `pass_ok = true`
- `provenance.derived_in_process = false`
- `observability.verification_source` no es `inline_contract_derivation`
- `excluded_feed_policy.book = "excluded"`

5. Release gates finales por perfil
- `overall_status = PASS`
- bloques en `pass`:
  - `operational_evidence`
  - `operational_observability`
  - `live_drill`
  - `support_matrix`
  - `exact_recovery`
  - `live_scope`
- `trade`:
  - `canary_rest.required = false`
  - `canary_ws.required = true`
  - `paper_soak.status = pass`
- `kline`:
  - `canary_rest.status = pass`
  - `canary_ws.status = pass`
  - `paper_soak.status = pass`

6. Live drill
- `drill_executed = true`
- `promote_ready = true`
- `rollback_ready = true`
- `overall_status = PASS`

## Criterio de decision
- `GO`
  - ambos perfiles live en `PASS`
  - predrill y final evidence persistidas
  - observabilidad externa verificada
  - live drill verde
  - provenance operacional valida
- `GO CONDICIONAL`
  - no aplica como cierre operativo final
- `NO-GO`
  - cualquier artifact faltante
  - `channel = manual`
  - `execution_ref` vacio
  - live drill no verde
  - `book` en scope
  - surfaces externas incompletas
  - gates finales no verdes

## Que revisar si falla
- si falla `live_trade`
  - revisar `ingestion_ws_canary_report_live_trade.json`
  - revisar `ingestion_soak_evidence_live_trade.json`
  - revisar `ingestion_failure_injection_live_trade.json`
  - revisar `ingestion_live_drill_report_live_trade.json`
- si falla `live_kline`
  - revisar `ingestion_canary_report_live_kline.json`
  - revisar `ingestion_ws_canary_report_live_kline.json`
  - revisar `ingestion_soak_evidence_live_kline.json`
  - revisar `ingestion_failure_injection_live_kline.json`
  - revisar `ingestion_live_drill_report_live_kline.json`
- si falla `operational_observability`
  - revisar owners, refs y verification refs de runtime, alerts, logs, promotion y cutover
- si falla `operational_evidence`
  - revisar `execution_ref`, `channel`, `verification_artifact_path` y freshness de artifacts runtime

## Checklist final
- `trade` en `PASS`
- `kline` en `PASS`
- `book` no aparece en ningun artifact
- `channel` no es `manual`
- `execution_ref` unico y visible
- observabilidad externa verificada
- predrill gates en verde
- live drill en verde
- final gates en verde
