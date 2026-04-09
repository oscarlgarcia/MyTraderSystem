# Ingestion Runbook

## Objetivo
Runbook operativo minimo para validar el modulo de ingestion antes de promoverlo a live. Todo el flujo usa fuentes mock/deterministas salvo el canary real cuando se ejecute expresamente.
- El scope live soportado hoy es `trade` + `kline`. `trade` exige exact recovery, handoff historico-live y evidencia runtime continua; `book` no es un objetivo valido de promotion live.
- La promotion ya no se apoya solo en tests del repo. Debe existir un artefacto agregado de evidence operativa fresco en `docs/validation/ingestion_operational_evidence_<target>.json` o en el path equivalente del profile ejecutado.
- La observabilidad externa exigida por contrato se resume en cuatro superficies minimas por target: `ingestion.<target>.runtime`, `ingestion.<target>.alerts`, `ingestion.<target>.logs` e `ingestion.<target>.promotion`. Para `live` se exige ademas `ingestion.live.cutover`.
- Un gate ya no debe aceptar evidence derivada inline dentro de `run_release_gates`. El artefacto agregado debe venir persistido desde `scripts/ingestion_operational_evidence.py`, con `provenance.source`, `provenance.runner_id`, `provenance.trigger`, `provenance.generated_by` y `provenance.derived_in_process = false`.

## Cierre operativo estandar
- El flujo operativo recomendado ya no es lanzar piezas sueltas una a una, sino ejecutar el orquestador:
  - `poetry run python scripts/ingestion_operational_cycle.py`
- Ese script ejecuta por perfil:
  - `replay_parity`
  - canaries
  - benchmark
  - vendor contracts
  - `ingestion_observability_verify.py`
  - `ingestion_operational_evidence.py`
  - `ingestion_release_gates.py`
  - para `live`, tambien:
    - `failure_injection`
    - predrill gates
    - `ingestion_live_drill.py`
- Los playbooks ejecutables del caso estandar quedan en:
  - `docs/operations/ingestion_operational_closure_paper.md`
  - `docs/operations/ingestion_operational_closure_live.md`
- `book` no es un feed valido para este flujo y cualquier intento debe tratarse como `NO-GO`.

## Comandos base
- Shell del contenedor:
  - `docker compose exec app bash`
- Suite rapida:
  - `docker compose exec app poetry run pytest -q`
- Suite de readiness:
  - `docker compose exec app poetry run pytest tests/slow/test_ingestion_readiness.py -m slow -q`
- Soak determinista:
  - `docker compose exec app poetry run python scripts/ingestion_soak.py`
- Canary determinista:
  - `docker compose exec app poetry run python scripts/ingestion_canary.py --mode rest-baseline`
- Canary WS real:
  - `docker compose exec app poetry run python scripts/ingestion_canary.py --mode ws-live --symbol BTCUSDT --max-events 2 --duration-seconds 130`
- Benchmark de storage segmentado:
  - `docker compose exec app poetry run python scripts/ingestion_storage_benchmark.py`
- Release gating consolidado:
  - `docker compose exec app poetry run python -m app.main --release-gates --release-gates-target paper`
  - `docker compose exec app poetry run python -m app.main --release-gates --release-gates-target live`
- Compactacion offline:
  - `docker compose exec app poetry run python scripts/ingestion_compact.py --env dev --dry-run`
  - `docker compose exec app poetry run python scripts/ingestion_compact.py --env dev --batch-limit 10 --retain-compacted-segments 1`
- Quarantine / DLQ:
  - `docker compose exec app poetry run python -m app.ops.quarantine_cli --base-dir . list --symbol BTCUSDT --stream-type kline`
  - `docker compose exec app poetry run python -m app.ops.quarantine_cli --base-dir . replay --env dev --record-id ingestion-dlq.jsonl:1 --write-normalized --report-path docs/validation/quarantine_replay_report.json`

## Artefactos de evidencia
- `docs/validation/ingestion_soak_evidence.json`
- `docs/validation/ingestion_canary_report.json`
- `docs/validation/ingestion_ws_canary_report.json`
- `docs/validation/ingestion_storage_benchmark.json`
- `docs/validation/ingestion_vendor_contracts.json`
- `docs/validation/ingestion_operational_evidence_paper.json`
- `docs/validation/ingestion_operational_evidence_pre_drill_live.json`
- `docs/validation/ingestion_operational_evidence_live.json`
- `docs/validation/ingestion_release_gates.json`
- `docs/validation/ingestion_live_drill_report.json`
- `docs/validation/quarantine_replay_report.json`
- `<data_dir>/shadow/env=<env>/comparisons.jsonl`
- `<data_dir>/normalized/.../data.parquet`
- `<data_dir>/normalized/.../retained-segments/`

## Senales a revisar
- `ingestion summary`
  - `result`
  - `events_persisted`
  - `events_invalid`
  - `events_dedup_skipped`
  - `events_buffer_dropped`
  - `snapshot_runs`
  - `snapshot_duplicates_skipped`
  - `processing_latency_seconds`
  - `write_latency_seconds`
  - `event_gap_seconds`
  - `gaps_total`
  - `gap_irreparable_total`
  - `stream_metrics`
- `ingestion health`
  - `result`
  - `streams_observed`
  - `streams_degraded`
- `storage health`
  - `segments_pending_total`
  - `segments_per_partition_max`
  - `compaction_lag_seconds`
  - `compaction_failures_total`
  - `normalized_partition_row_count`

## Procedimiento minimo de validacion
1. Ejecutar `pytest -q`.
2. Ejecutar `pytest tests/slow/test_ingestion_readiness.py -m slow -q`.
3. Ejecutar `python scripts/ingestion_soak.py`.
4. Ejecutar `python scripts/ingestion_canary.py --mode rest-baseline`.
5. Ejecutar `python scripts/ingestion_canary.py --mode ws-live --symbol BTCUSDT --max-events 2 --duration-seconds 130`.
6. Ejecutar `python scripts/ingestion_compact.py --env dev --dry-run`.
7. Ejecutar `python scripts/ingestion_storage_benchmark.py`.
8. Ejecutar `python -m app.main --release-gates --release-gates-target paper`.
9. Ejecutar `python scripts/ingestion_live_drill.py`.
10. Verificar que no hay `FAILED` y que todos los scripts devuelven exit code `0`.
11. Revisar `docs/validation/ingestion_operational_evidence_paper.json` o `docs/validation/ingestion_operational_evidence_live.json` segun el target:
   - `pass_ok = true`
   - `evidence_origin` coherente con el target
   - `provenance.source`, `provenance.runner_id`, `provenance.trigger` y `provenance.generated_by` presentes
   - `provenance.derived_in_process = false`
   - `excluded_feed_policy.book = "excluded"`
   - `observability.pass_ok = true`
   - cada surface en `observability.external_surfaces` lleva `owner`, `surface_ref`, `verification_mode`, `verified_at`, `verification_ref` y `pass_ok = true`
   - todos los artefactos requeridos llevan `fresh = true`
12. Revisar `docs/validation/ingestion_soak_evidence.json`:
   - `pass_ok = true`
   - `max_gaps = 0`
   - `max_gap_irreparable = 0`
13. Revisar `docs/validation/ingestion_canary_report.json`:
   - `pass_ok = true`
   - `diffs.events_persisted = 0`
   - `diffs.duplicates = 0`
   - `diffs.gaps = 0`
14. Revisar `docs/validation/ingestion_ws_canary_report.json`:
   - `pass_ok = true`
   - `reconnects_observed >= reconnects_target`
   - existe `continuity`
   - existen `gaps`, `duplicates` y `reconnects` en el reporte
15. Revisar `docs/validation/ingestion_storage_benchmark.json`:
   - `pass_ok = true`
   - existe `slo`
   - los cuatro casos (`synthetic_case`, `replay_case`, `concurrent_compaction_case`, `shadow_scoped_case`) quedan medidos
16. Revisar `docs/validation/ingestion_release_gates.json`:
   - `overall_status = PASS`
   - existe `blocks`
   - cada bloque deja `status`, `required`, `reasons`
17. Revisar `docs/validation/ingestion_live_drill_report.json`:
   - `drill_executed = true`
   - `checklist_completed = true`
   - `rollback_ready = true`
   - `promote_ready = true` solo si el cutover es aprobable
18. Revisar el reporte de compactacion:
   - `failed_partitions = 0`
   - `planned_partitions` consistente con el estado de `segments/`
19. Si hay backlog real, ejecutar el job sin `--dry-run` con `--batch-limit` acotado y confirmar:
   - se publica `data.parquet`
   - el path activo `segments/` queda vacio o eliminado
   - `retained-segments/` solo existe si se pidio retencion
   - no queda `compaction-failures.jsonl` nuevo
20. Si existe `schema-drift-quarantine.jsonl` o `ingestion-dlq.jsonl`, inspeccionar antes de promover:
   - `python -m app.ops.quarantine_cli --base-dir . list --limit 20`
   - si se corrige un payload, reinyectarlo con `replay`
   - verificar en el reporte si `normalized_modified = true`

## Interpretacion operativa
- Si falla `test_end_to_end_live_mock_with_reconnect_checkpoint_and_sink_flush`
  - problema probable en reconnect, checkpoint o persistencia final.
- Si falla `test_overload_policy_under_10k_mock_events`
  - la politica de saturacion no es estable o el sistema se degrada peor de lo documentado.
- Si falla `test_restart_after_partial_failure_preserves_consistency`
  - no hay garantias suficientes ante reinicio/fallo parcial.
- Si falla `test_corrupt_input_and_sink_failure_leave_system_diagnosable`
  - la observabilidad no alcanza para incidente real.
- Si falla el soak
  - no se debe promover el pipeline aunque la suite slow pase.
- Si falla el canary
  - no se debe promover la version candidata; revisar `comparisons.jsonl` y usar rollback.
- Si falla la compactacion
  - no se debe confiar en lecturas largas de normalized hasta inspeccionar `compaction-failures.jsonl`.

## Criterio operativo de salida
- `GO` solo si:
  - suite normal verde
  - suite slow verde
  - soak determinista verde
  - canary determinista verde
  - canary WS real verde
  - benchmark de storage verde
  - compactacion sin backlog critico ni fallos
  - no hay corrupcion de Parquet
  - los summaries de fallo siguen siendo diagnosticables
- `GO CONDICIONAL` solo si:
  - no hay diferencias semanticas (`events_persisted`, `duplicates`, `gaps`)
  - y la unica desviacion es de latencia, documentada y aceptada explicitamente
- `NO-GO` si:
  - cualquier test slow falla
  - soak o canary fallan
  - hay perdida silenciosa
  - hay fallo no diagnosticable
  - hay diferencias semanticas entre baseline y candidata
  - `compaction_lag_seconds` supera el umbral critico
  - `compaction_failures_total > 0`

## Alertas operativas minimas
- `reconnect_storm` (warning, umbral 3):
  - comprobar conectividad hacia el vendor
  - revisar si el websocket esta inestable o si hay rate limit
- `gap_detected` (warning, umbral 1):
  - revisar `stream_metrics` del stream afectado
  - confirmar si el recovery aplicable se ejecuto
- `gap_irreparable` (error, umbral 1):
  - tratar el stream como degradado
  - no asumir continuidad para paper/live hasta rehacer bootstrap o reiniciar la sesion
- `heartbeat_missed` (warning, umbral 1):
  - verificar watchdog y latencia de red
  - confirmar que hubo reconnect posterior
- `dlq_spike` (warning, umbral 3):
  - inspeccionar `data/errors/ingestion-dlq.jsonl`
  - buscar schema drift o payloads corruptos por stream
- `sink_failure` (error, umbral 1):
  - detener confianza en persistencia
  - revisar raw sink, normalized sink o error sink segun `sink_component`
- `compaction_backlog_high` (warning, umbral 1):
  - ejecutar el job de compactacion antes de seguir confiando en sesiones largas
  - revisar `segments_pending_total`, `segments_per_partition_max` y `compaction_lag_seconds`
- `compaction_failure_detected` (error, umbral 1):
  - tratar normalized storage como degradado
  - inspeccionar `compaction-failures.jsonl` y reintentar solo tras corregir la causa

## Campos estandar de alerta
- `message = operational alert`
- `alert_type`
- `alert_severity`
- `observed`
- `threshold`
- `recommended_action`

## Rollback minimo
- Ver checklist detallada en `docs/operations/ingestion_rollback_checklist.md`.
- Ver procedimiento formal de cutover en `docs/ops/live_cutover.md`.
- Regla practica:
  1. si `ingestion_canary_report.json` muestra diffs relevantes, no promocionar
  2. volver a `--ingest-pipeline-version v1`
  3. mantener `--ingest-shadow-mode` activo si se quiere seguir comparando sin exponer `v2`
  4. no borrar evidencias `shadow/` ni `docs/validation/*.json` hasta cerrar el incidente
  5. no borrar `retained-segments/` ni `compaction-failures.jsonl` hasta cerrar el incidente de storage

## Limitaciones conocidas
- Los benchmarks son deterministas y locales; no sustituyen soak test largo ni canary real contra vendor.
- La suite no usa Binance real en CI a proposito.

## Scope operativo por feed
- `paper` soporta `trade` y `kline`.
- `trade` en `paper` se valida por `replay`, parity y contratos del vendor; no debe promocionarse como feed `live`.
- `live` soporta `trade` + `kline`.
- `trade` en `live` exige exact recovery, handoff historico-live y evidencia runtime fresca.
- `book` queda fuera de `paper` y `live`.
- Los artifacts operativos se consideran stale tras 24 horas para `rest/ws canary`, `vendor_contracts`, `soak`, `failure_injection` y `live_drill`; `replay` parity y `storage_benchmark` admiten 7 dias como maximo.
- Un gate paper/live ya no debe aceptarse si solo puede derivar la evidence en proceso o si faltan `owner`, `surface_ref`, `verification_mode`, `verified_at` o `verification_ref` en las superficies declaradas del contrato de observabilidad.
