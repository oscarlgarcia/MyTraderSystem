# Ingestion Runbook

## Objetivo
Runbook minimo para operar la ingestion en entorno controlado, reproducir incidencias y ejecutar la validacion final de readiness sin depender de Binance real.

## Comandos base
- Shell del contenedor:
  - `docker compose exec app bash`
- Suite rapida:
  - `docker compose exec app poetry run pytest -q`
- Suite de readiness:
  - `docker compose exec app poetry run pytest tests/slow/test_ingestion_readiness.py -m slow -q`
- Solo benchmark/control de sobrecarga:
  - `docker compose exec app poetry run pytest tests/slow/test_ingestion_readiness.py -m slow -k overload_policy_under_10k_mock_events -q`

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
  - `late_events`
- `ingestion health`
  - usarlo como estado final por ejecucion

## Procedimiento minimo de validacion
1. Ejecutar `pytest -q`.
2. Ejecutar `pytest tests/slow/test_ingestion_readiness.py -m slow -q`.
3. Verificar que no hay `FAILED`.
4. Confirmar que los tests lentos dejan evidencia de:
   - reconnect con checkpoint
   - sobrecarga controlada bajo 10k eventos
   - reinicio tras fallo parcial sin corrupcion
   - fallo compuesto diagnosticable
5. En el benchmark de sobrecarga, verificar ademas que el run termina dentro del umbral local documentado por el test (<5s en Docker CI/local) y sin colgar el proceso.

## Interpretacion operativa
- Si falla `test_end_to_end_live_mock_with_reconnect_checkpoint_and_sink_flush`
  - problema probable en reconnect, checkpoint o persistencia final.
- Si falla `test_overload_policy_under_10k_mock_events`
  - politica de saturacion no es estable o el sistema se degrada peor de lo documentado.
- Si falla `test_restart_after_partial_failure_preserves_consistency`
  - no hay garantias suficientes ante reinicio/fallo parcial.
- Si falla `test_corrupt_input_and_sink_failure_leave_system_diagnosable`
  - la observabilidad no alcanza para incidente real.

## Criterio operativo de salida
- `GO` solo si:
  - suite normal verde
  - suite slow verde
  - no hay corrupción de Parquet
  - los summaries de fallo siguen siendo diagnosticables
- `NO-GO` si:
  - cualquier test slow falla
  - hay pérdida silenciosa
  - hay fallo no diagnosticable

## Limitaciones conocidas
- Los benchmarks son deterministas y locales; no sustituyen soak test largo ni canary real.
- La suite no usa Binance real en CI a propósito.
