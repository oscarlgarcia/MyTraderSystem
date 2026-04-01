# Ingestion Rollback Checklist

## Cuándo aplicar rollback
- `ingestion_canary_report.json` con `pass_ok = false`
- `comparisons.jsonl` con diferencias relevantes en:
  - `events_persisted`
  - `duplicates_total`
  - `gaps_total`
- alerta `gap_irreparable`
- alerta `sink_failure`
- degradación no explicada en `streams_degraded`

## Pasos
1. Detener la promoción de la versión candidata.
2. Reconfigurar:
   - `--ingest-pipeline-version v1`
   - opcional: mantener `--ingest-shadow-mode` para seguir comparando contra `v2`
3. Confirmar que la nueva ejecución deja:
   - `ingestion health.result = ok`
   - `streams_degraded = []`
4. Guardar evidencia:
   - `docs/validation/ingestion_canary_report.json`
   - `<data_dir>/shadow/env=<env>/comparisons.jsonl`
5. No borrar raw landing ni artifacts de shadow hasta cerrar el incidente.

## Criterio de salida del rollback
- baseline `v1` vuelve a pasar canary local
- no hay diferencias semánticas en conteos/duplicados/gaps
- las alertas críticas desaparecen o quedan explicadas
