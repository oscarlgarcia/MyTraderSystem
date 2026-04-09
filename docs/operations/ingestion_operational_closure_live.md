# Ingestion Operational Closure Playbook - Live

## Objetivo
Ejecutar el caso estandar de cierre operativo de ingestion para `live` sobre el scope soportado hoy: `trade` + `kline`.

## Scope soportado
- feeds soportados: `trade`, `kline`
- feed excluido: `book`
- `trade live` usa dominio canónico `aggregate trade` extremo a extremo:
  - websocket: `@aggTrade`
  - cursor: `aggregate_trade_id`
  - recovery REST: `/api/v3/aggTrades`
- canal valido para promotion final: `scheduled` o `pipeline`
- `manual` solo sirve para simulacion o diagnostico y debe terminar en `NO-GO`

## Prerrequisitos
- entorno activo: `dev` para el ejemplo
- ejecutar desde la raiz del repo con `poetry run python` o, si `poetry` no esta disponible en el host, con el `python` del entorno operativo equivalente
- disponer de raw y normalized exactos del dataset candidato
- disponer de `runner context` persistido, por JSON o por variables de entorno, con:
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
- disponer de runtime base dir real para gates y shadow:
  - `data/dev`

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

## Defaults canónicos de cierre live
- `trade ws canary`
  - `max_events = 12`
  - `duration_seconds = 120`
  - `reconnect_after_events = 4`
  - `induced_reconnects = 1`
- `trade soak`
  - `mode = ws-live`
  - `iterations = 3`
  - `events_per_iteration = 200`
  - `duration_seconds = 180`
  - `reconnect_after_events = 100`
  - `induced_reconnects = 1`
- `kline`
  - usa el perfil canónico del ciclo operativo sin overrides manuales

## Ejemplo minimo de runner context

```json
{
  "execution_ref": "live-dev-btcusdt-20260409T100000Z",
  "channel": "scheduled",
  "schedule_name": "ingestion-live-cadence",
  "job_id": "live-job-20260409-1000",
  "job_url": "https://ops.example/pipelines/ingestion-live/20260409-1000",
  "owner": "team-ingestion-oncall"
}
```

## Ejemplo minimo de surface manifest

```json
{
  "runtime": {
    "owner": "team-ingestion",
    "surface_ref": "grafana://ingestion/live/runtime",
    "verification_ref": "ops://live/runtime/20260409T100000Z"
  },
  "alerts": {
    "owner": "team-ingestion-oncall",
    "surface_ref": "pagerduty://ingestion/live/alerts",
    "verification_ref": "ops://live/alerts/20260409T100000Z"
  },
  "logs": {
    "owner": "team-observability",
    "surface_ref": "loki://ingestion/live/logs",
    "verification_ref": "ops://live/logs/20260409T100000Z"
  },
  "promotion": {
    "owner": "team-ingestion",
    "surface_ref": "runbook://docs/operations/ingestion_promotion_runbook.md",
    "verification_ref": "ops://live/promotion/20260409T100000Z"
  },
  "cutover": {
    "owner": "team-ingestion",
    "surface_ref": "runbook://docs/ops/live_cutover.md",
    "verification_ref": "ops://live/cutover/20260409T100000Z"
  }
}
```

## Nota operativa real
- Un run real puede terminar en `NO-GO` aunque el codigo este cerrado:
  - por ejemplo, una `ws canary` live puede fallar por `gaps_detected`, `gap_irreparable_detected` o `streams_degraded_detected`
  - ese resultado debe interpretarse como evidencia operacional valida, no como motivo para relajar gates

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
- governance/cadence:
  - `docs/validation/operational/live/ingestion_operational_governance_live.json`
  - `docs/validation/operational/live/ingestion_operational_history_live.jsonl`
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
- `cadence_state = bootstrap` o `healthy`
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
- `governance.schedule_name = ingestion-live-cadence`
- `governance.job_id` no vacio
- `governance.job_url` no vacio
- `governance.context_source` presente y coherente con el origen real del contexto
- `governance.cadence_state = bootstrap` o `healthy`
- `governance.pass_ok = true`
- `observability.verification_artifact_path` presente
- `failure_injection` y `soak` presentes y frescos

4. Final evidence
- `target = live`
- `phase = final`
- `pass_ok = true`
- `provenance.derived_in_process = false`
- `observability.verification_source` no es `inline_contract_derivation` ni `manual_surface_check`
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
  - el artifact WS confirma continuidad en `aggregate trade`
- `kline`:
  - `canary_rest.status = pass`
  - `canary_ws.status = pass`
  - `paper_soak.status = pass`

6. Live drill
- `drill_executed = true`
- `promote_ready = true`
- `rollback_ready = true`
- `overall_status = PASS`

7. Artefactos runtime de `trade`
- `ingestion_ws_canary_report_live_trade.json`
  - `stream_type = "trade"`
  - `continuity.cursor_kind = "aggregate_trade_id"` o metadata equivalente por stream
  - `continuity.gap_irreparable = 0`
  - `continuity.streams_degraded = []`
- `ingestion_soak_evidence_live_trade.json`
  - `pass_ok = true`
  - `max_gap_irreparable = 0`
  - `max_streams_degraded = 0`
- `ingestion_readiness_live_trade.json`
  - `overall_status = PASS`
  - el drill final aparece en verde
- stream metrics de `trade`
  - `aggregate_trade_id` visible en el flujo real
  - `recovery_window_rows_received > 0` cuando hubo recovery reparable
  - `gap_irreparable_total = 0`
  - `streams_degraded = []`

## Criterio de decision
- `GO`
  - ambos perfiles live en `PASS`
  - predrill y final evidence persistidas
  - observabilidad externa verificada
  - live drill verde
  - provenance operacional valida
  - 3 ciclos consecutivos verdes en `dev proving`
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
- `context_source` visible en governance
- governance/cadence verde
- observabilidad externa verificada
- predrill gates en verde
- live drill en verde
- final gates en verde
