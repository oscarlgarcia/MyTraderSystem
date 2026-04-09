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
- ejecutar desde la raiz del repo con `poetry`
- disponer de raw y normalized ya generados para el dataset candidato
- tener rutas exactas de normalized por feed:
  - `trade`: `data/dev/normalized/trades/env=dev/venue=BINANCE/symbol=BTCUSDT/date=2026-04-09`
  - `kline`: `data/dev/normalized/bars/env=dev/venue=BINANCE/symbol=BTCUSDT/date=2026-04-09`
- disponer de surfaces externas verificables para:
  - runtime
  - alerts
  - logs
  - promotion

## Variables del caso estandar
- `execution_ref`: `paper-dev-btcusdt-20260409T090000Z`
- `channel`: `scheduled`
- `output_dir`: `docs/validation/operational/paper`
- `runner_id`: `ingestion-paper-closure`
- `trigger`: `scheduled_paper_cycle`
- `provenance_source`: `ingestion_operational_cycle`

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
  --execution-ref paper-dev-btcusdt-20260409T090000Z `
  --channel scheduled `
  --runtime-owner team-ingestion `
  --runtime-surface-ref grafana://ingestion/paper/runtime `
  --runtime-verification-ref ops://paper/runtime/20260409T090000Z `
  --alerts-owner team-ingestion-oncall `
  --alerts-surface-ref pagerduty://ingestion/paper/alerts `
  --alerts-verification-ref ops://paper/alerts/20260409T090000Z `
  --logs-owner team-observability `
  --logs-surface-ref loki://ingestion/paper/logs `
  --logs-verification-ref ops://paper/logs/20260409T090000Z `
  --promotion-owner team-ingestion `
  --promotion-surface-ref runbook://docs/operations/ingestion_promotion_runbook.md `
  --promotion-verification-ref ops://paper/promotion/20260409T090000Z
```

## Salida esperada en consola
- linea inicial:
  - `ingestion operational cycle: PASS (paper)`
- campos visibles:
  - `execution_ref: paper-dev-btcusdt-20260409T090000Z`
  - `channel: scheduled`
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
- `excluded_feed_policy.book = "excluded"`
- `observability.verification_artifact_path` presente
- `observability.verification_source` distinto de `inline_contract_derivation`

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
- observabilidad externa verificada
- gates finales en verde
