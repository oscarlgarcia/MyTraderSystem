# Live Cutover Runbook

## Objetivo
Procedimiento formal de promocion de ingestion desde paper a live con rollback controlado y tiempos maximos de decision.

## Artefactos obligatorios antes de promover
- `docs/validation/ingestion_release_gates.json`
- `docs/validation/ingestion_canary_report.json`
- `docs/validation/ingestion_ws_canary_report.json`
- `docs/validation/ingestion_storage_benchmark.json`
- `docs/validation/ingestion_live_drill_report.json`
- `docs/operations/ingestion/ingestion_rollback_checklist.md`

## Tiempos maximos de decision
- decision de promocion: `15` minutos desde el cierre del release gate
- ack de alerta critica: `2` minutos
- decision de rollback: `5` minutos desde la primera alerta critica relevante

## Secuencia formal
1. Ejecutar release gate:
   - `docker compose exec app poetry run python -m app.main --release-gates --release-gates-target live`
2. Validar canary REST:
   - `docker compose exec app poetry run python scripts/ingestion_canary.py --mode rest-baseline`
3. Validar canary WS real:
   - `docker compose exec app poetry run python scripts/ingestion_canary.py --mode ws-live --symbol BTCUSDT --max-events 2 --duration-seconds 130`
4. Validar benchmark operativo:
   - `docker compose exec app poetry run python scripts/ingestion_storage_benchmark.py`
5. Ejecutar drill formal:
   - `docker compose exec app poetry run python scripts/ingestion_live_drill.py`
6. Revisar `docs/validation/ingestion_live_drill_report.json`
7. Solo promocionar si:
   - `overall_status = PASS`
   - `promote_ready = true`
   - `rollback_ready = true`

## Reglas de promocion
- no promover si falta cualquier artifact obligatorio
- no promover si `release_gates.pass_ok != true`
- no promover si cualquiera de los canaries falla
- no promover si el benchmark de storage falla
- no promover si el drill marca `promote_ready = false`

## Reglas de rollback
- rollback inmediato si aparece cualquiera de estas alertas:
  - `gap_irreparable`
  - `shadow_semantic_diff`
  - `compaction_failure_detected`
  - `reconnect_storm`
- rollback condicionado a revision de metadata si aparece:
  - `provider_metadata_drift`

## Respuesta ante alertas criticas
- `gap_irreparable`
  - congelar promocion
  - revertir a pipeline previo
  - preservar artifacts y logs
- `shadow_semantic_diff`
  - bloquear promocion
  - mantener candidata solo en shadow
  - revisar `comparisons.jsonl`
- `compaction_failure_detected`
  - detener live cutover
  - revisar `compaction-failures.jsonl`
  - reintentar solo tras limpiar storage health
- `reconnect_storm`
  - verificar vendor/network
  - no continuar con live mientras no se estabilice el stream
- `provider_metadata_drift`
  - revisar cambios de tick size / precision / contract metadata
  - no promocionar hasta aceptar o corregir el drift

## Salida del drill
- `PASS`
  - checklist completa
  - promotion permitida
  - rollback documentado y listo
- `FAIL`
  - no se permite live
  - usar rollback checklist y conservar evidencia
