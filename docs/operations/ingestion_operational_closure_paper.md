# Ingestion Operational Closure Playbook - Paper

## Objetivo
Ejecutar el caso estandar de cierre operativo de ingestion para `paper` sobre el scope soportado hoy: `trade` + `kline`.

## Scope soportado
- feeds soportados: `trade`, `kline`
- feed excluido: `book`
- canal valido para cierre operativo final: `scheduled` o `pipeline`
- `manual` solo sirve para runs informativos y debe terminar en `NO-GO`

## Prerrequisitos
- entorno activo: `dev`
- ejecutar desde la raiz del repo con `poetry run python` o, si `poetry` no esta disponible en el host, con el `python` del entorno operativo equivalente
- disponer de raw y normalized ya generados para el dataset candidato
- disponer de un `runner context` persistido, por JSON o por variables de entorno, con:
  - `execution_ref`
  - `channel`
  - `schedule_name`
  - `job_id`
  - `job_url`
- disponer de un `surface manifest` persistido con refs verificables para:
  - runtime
  - alerts
  - logs
  - promotion
- tener rutas exactas de normalized por feed:
  - `trade`: `data/dev/normalized/trades/env=dev/venue=BINANCE/symbol=BTCUSDT/date=2026-04-09`
  - `kline`: `data/dev/normalized/bars/env=dev/venue=BINANCE/symbol=BTCUSDT/date=2026-04-09`

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

## Ejemplo minimo de runner context

```json
{
  "execution_ref": "paper-dev-btcusdt-20260409T090000Z",
  "channel": "scheduled",
  "schedule_name": "ingestion-paper-cadence",
  "job_id": "paper-job-20260409-0900",
  "job_url": "https://ops.example/pipelines/ingestion-paper/20260409-0900",
  "owner": "team-ingestion"
}
```

## Ejemplo minimo de surface manifest

```json
{
  "runtime": {
    "owner": "team-ingestion",
    "surface_ref": "grafana://ingestion/paper/runtime",
    "verification_ref": "ops://paper/runtime/20260409T090000Z"
  },
  "alerts": {
    "owner": "team-ingestion-oncall",
    "surface_ref": "pagerduty://ingestion/paper/alerts",
    "verification_ref": "ops://paper/alerts/20260409T090000Z"
  },
  "logs": {
    "owner": "team-observability",
    "surface_ref": "loki://ingestion/paper/logs",
    "verification_ref": "ops://paper/logs/20260409T090000Z"
  },
  "promotion": {
    "owner": "team-ingestion",
    "surface_ref": "runbook://docs/operations/ingestion_promotion_runbook.md",
    "verification_ref": "ops://paper/promotion/20260409T090000Z"
  }
}
```

## Salida esperada en consola
- linea inicial:
  - `ingestion operational cycle: PASS (paper)`
- campos visibles:
- `execution_ref: paper-dev-btcusdt-20260409T090000Z`
- `channel: scheduled`
- `cadence_state: bootstrap` o `healthy`
- `stream_types: trade, kline`
- pasos por perfil:
  - `paper_trade: PASS`
  - `paper_kline: PASS`

## Artefactos esperados
- manifest del ciclo:
  - `docs/validation/operational/paper/ingestion_operational_cycle_paper.json`
- reports de readiness:
  - `docs/validation/operational/paper/ingestion_readiness_paper_trade.json`
  - `docs/validation/operational/paper/ingestion_readiness_paper_kline.json`
- governance/cadence:
  - `docs/validation/operational/paper/ingestion_operational_governance_paper.json`
  - `docs/validation/operational/paper/ingestion_operational_history_paper.jsonl`
- verificacion de observabilidad:
  - `docs/validation/operational/paper/ingestion_observability_verification_paper_trade.json`
  - `docs/validation/operational/paper/ingestion_observability_verification_paper_kline.json`
- gates y evidence agregada:
  - `docs/validation/operational/paper/ingestion_operational_evidence_paper_trade.json`
  - `docs/validation/operational/paper/ingestion_operational_evidence_paper_kline.json`
  - `docs/validation/operational/paper/ingestion_release_gates_paper_trade.json`
  - `docs/validation/operational/paper/ingestion_release_gates_paper_kline.json`
- evidencia adicional por feed:
  - `trade`: replay parity, benchmark, vendor contracts
  - `kline`: replay parity, rest canary, ws canary, benchmark, vendor contracts, soak

## Que verificar exactamente
1. Manifest del ciclo
- `overall_status = PASS`
- `pass_ok = true`
- `execution_ref = paper-dev-btcusdt-20260409T090000Z`
- `channel = scheduled`
- `cadence_state = bootstrap` o `healthy`
- `stream_types = ["trade", "kline"]`
- cada paso tiene `returncode = 0`

2. Observability verification por perfil
- `pass_ok = true`
- `verification_source` no vacio
- existen surfaces para:
  - `ingestion.paper.runtime`
  - `ingestion.paper.alerts`
  - `ingestion.paper.logs`
  - `ingestion.paper.promotion`
- cada surface tiene:
  - `owner`
  - `surface_ref`
  - `verification_mode`
  - `verification_ref`
  - `verified_at`
  - `pass_ok = true`

3. Operational evidence por perfil
- `pass_ok = true`
- `phase = final`
- `target = paper`
- `evidence_origin = paper_operational`
- `provenance.source = ingestion_operational_cycle`
- `provenance.execution_ref = paper-dev-btcusdt-20260409T090000Z`
- `provenance.channel = scheduled`
- `provenance.derived_in_process = false`
- `governance.artifact_path` presente
- `governance.schedule_name = ingestion-paper-cadence`
- `governance.job_id` no vacio
- `governance.job_url` no vacio
- `governance.context_source` presente y coherente con el origen real del contexto
- `governance.cadence_state = bootstrap` o `healthy`
- `governance.pass_ok = true`
- `excluded_feed_policy.book = "excluded"`
- `observability.verification_artifact_path` presente
- `observability.verification_source` distinto de `inline_contract_derivation` y `manual_surface_check`

4. Release gates por perfil
- `overall_status = PASS`
- bloque `operational_evidence` en `pass`
- bloque `operational_observability` en `pass`
- `trade`:
  - `canary_rest.required = false`
  - `canary_ws.required = false`
  - `paper_soak.required = false`
- `kline`:
  - `canary_rest.status = pass`
  - `canary_ws.status = pass`
  - `paper_soak.status = pass`

## Criterio de decision
- `GO`
  - ambos perfiles en `PASS`
  - evidence persistida
  - provenance valida
  - observabilidad externa verificada
- `GO CONDICIONAL`
  - no aplica como cierre operativo final
- `NO-GO`
  - cualquier artifact faltante
  - `channel = manual`
  - `execution_ref` vacio
  - `book` en scope
  - surfaces externas incompletas
  - release gates no verdes

## Que revisar si falla
- si falla `paper_trade`
  - revisar `ingestion_replay_parity_paper_trade.json`
  - revisar `ingestion_storage_benchmark_paper_trade.json`
  - revisar `ingestion_vendor_contracts_paper_trade.json`
- si falla `paper_kline`
  - revisar `ingestion_canary_report_paper_kline.json`
  - revisar `ingestion_ws_canary_report_paper_kline.json`
  - revisar `ingestion_soak_evidence_paper_kline.json`
- si falla `operational_observability`
  - revisar owners, refs y verification refs del artifact de observabilidad
- si falla `operational_evidence`
  - revisar `execution_ref`, `channel` y `verification_artifact_path`

## Checklist final
- `trade` en `PASS`
- `kline` en `PASS`
- `book` no aparece en ningun artifact
- `channel` no es `manual`
- `execution_ref` unico y visible
- `context_source` visible en governance
- governance/cadence verde
- observabilidad externa verificada
- gates finales en verde
