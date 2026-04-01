# Ingestion Runbook

## Objetivo
Runbook operativo minimo para validar el modulo de ingestion antes de promoverlo a live. Todo el flujo usa fuentes mock/deterministas; no depende de Binance real.

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
  - `docker compose exec app poetry run python scripts/ingestion_canary.py`

## Artefactos de evidencia
- `docs/validation/ingestion_soak_evidence.json`
- `docs/validation/ingestion_canary_report.json`
- `<data_dir>/shadow/env=<env>/comparisons.jsonl`

## Señales a revisar
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

## Procedimiento mínimo de validación
1. Ejecutar `pytest -q`.
2. Ejecutar `pytest tests/slow/test_ingestion_readiness.py -m slow -q`.
3. Ejecutar `python scripts/ingestion_soak.py`.
4. Ejecutar `python scripts/ingestion_canary.py`.
5. Verificar que no hay `FAILED` y que ambos scripts devuelven exit code `0`.
6. Revisar `docs/validation/ingestion_soak_evidence.json`:
   - `pass_ok = true`
   - `max_gaps = 0`
   - `max_gap_irreparable = 0`
7. Revisar `docs/validation/ingestion_canary_report.json`:
   - `pass_ok = true`
   - `diffs.events_persisted = 0`
   - `diffs.duplicates = 0`
   - `diffs.gaps = 0`
8. Confirmar que los tests lentos dejan evidencia de:
   - reconnect con checkpoint
   - sobrecarga controlada bajo 10k eventos
   - reinicio tras fallo parcial sin corrupcion
   - fallo compuesto diagnosticable

## Interpretación operativa
- Si falla `test_end_to_end_live_mock_with_reconnect_checkpoint_and_sink_flush`
  - problema probable en reconnect, checkpoint o persistencia final.
- Si falla `test_overload_policy_under_10k_mock_events`
  - politica de saturacion no es estable o el sistema se degrada peor de lo documentado.
- Si falla `test_restart_after_partial_failure_preserves_consistency`
  - no hay garantias suficientes ante reinicio/fallo parcial.
- Si falla `test_corrupt_input_and_sink_failure_leave_system_diagnosable`
  - la observabilidad no alcanza para incidente real.
- Si falla el soak
  - no se debe promover el pipeline aunque la suite slow pase.
- Si falla el canary
  - no se debe promover la version candidata; revisar `comparisons.jsonl` y usar rollback.

## Criterio operativo de salida
- `GO` solo si:
  - suite normal verde
  - suite slow verde
  - soak determinista verde
  - canary determinista verde
  - no hay corrupción de Parquet
  - los summaries de fallo siguen siendo diagnosticables
- `GO CONDICIONAL` solo si:
  - no hay diferencias semánticas (`events_persisted`, `duplicates`, `gaps`)
  - y la única desviación es de latencia, documentada y aceptada explícitamente
- `NO-GO` si:
  - cualquier test slow falla
  - soak o canary fallan
  - hay pérdida silenciosa
  - hay fallo no diagnosticable
  - hay diferencias semánticas entre baseline y candidata

## Alertas operativas mínimas
- `reconnect_storm` (warning, umbral 3):
  - comprobar conectividad hacia el vendor
  - revisar si el websocket esta inestable o si hay rate limit
- `gap_detected` (warning, umbral 1):
  - revisar `stream_metrics` del stream afectado
  - confirmar si el recovery aplicable se ejecuto
- `gap_irreparable` (error, umbral 1):
  - tratar el stream como degradado
  - no asumir continuidad para paper/live hasta rehacer bootstrap o reiniciar la sesión
- `heartbeat_missed` (warning, umbral 1):
  - verificar watchdog y latencia de red
  - confirmar que hubo reconnect posterior
- `dlq_spike` (warning, umbral 3):
  - inspeccionar `data/errors/ingestion-dlq.jsonl`
  - buscar schema drift o payloads corruptos por stream
- `sink_failure` (error, umbral 1):
  - detener confianza en persistencia
  - revisar raw sink, normalized sink o error sink según `sink_component`

## Campos estándar de alerta
- `message = operational alert`
- `alert_type`
- `alert_severity`
- `observed`
- `threshold`
- `recommended_action`

## Rollback mínimo
- Ver checklist detallada en `docs/operations/ingestion_rollback_checklist.md`.
- Regla práctica:
  1. si `ingestion_canary_report.json` muestra diffs relevantes, no promocionar
  2. volver a `--ingest-pipeline-version v1`
  3. mantener `--ingest-shadow-mode` activo si se quiere seguir comparando sin exponer `v2`
  4. no borrar evidencias `shadow/` ni `docs/validation/*.json` hasta cerrar el incidente

## Limitaciones conocidas
- Los benchmarks son deterministas y locales; no sustituyen soak test largo ni canary real contra vendor.
- La suite no usa Binance real en CI a propósito.
