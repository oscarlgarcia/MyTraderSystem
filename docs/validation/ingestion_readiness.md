# Ingestion Readiness Checklist

## Estado esperado para considerar readiness técnica

### Corrección
- [ ] `pytest -q` pasa completo
- [ ] `tests/slow/test_ingestion_readiness.py` pasa completo
- [ ] `python scripts/ingestion_soak.py` devuelve `0`
- [ ] `python scripts/ingestion_canary.py --refresh-baseline` devuelve `0`
- [ ] existe ADR aprobada para el alcance historico: `docs/adr/ADR-0001-historical-market-data-scope.md`
- [ ] el alcance historico soportado esta declarado explicitamente y alineado con la ADR: bars-only (`kline`); trade historical no se promete
- [ ] cada run/backfill persiste `metadata/instruments/.../runs/<trace_id>.json` y no hay alertas `provider_metadata_drift` sin revisar
- [ ] no hay duplicados no explicados en reconnect/restart/handoff
- [ ] no hay corrupción de `data.parquet`

### Resiliencia
- [ ] reconnect con `checkpoint_store` preserva continuidad observable
- [ ] recovery exacto de bars no duplica borde reciente
- [ ] trades sin recovery exacto se marcan `gap_irreparable`
- [ ] reinicio tras fallo parcial preserva consistencia final

### Presión y límites
- [ ] 10k eventos mock bajo política de sobrecarga producen degradación visible, no silenciosa
- [ ] la política configurada queda reflejada en métricas y logs
- [ ] el soak determinista deja `pass_ok=true`

### Observabilidad
- [ ] `ingestion summary` presente en runs relevantes
- [ ] `ingestion health` presente al cierre
- [ ] `stream_metrics` y `streams_degraded` identifican el stream afectado
- [ ] `operational alert` aparece cuando corresponde

### Migración / canary
- [ ] `ingestion_canary_report.json` existe
- [ ] `ingestion_canary_baseline.json` existe
- [ ] `comparisons.jsonl` existe si hubo `shadow_mode`
- [ ] `comparison_reason = semantic_match`
- [ ] `diffs.row_count = 0`
- [ ] `diffs.projection_checksum_match = true`

### Seguridad operativa
- [ ] `--production-mode` rechaza defaults inseguros
- [ ] logs saneados, sin secretos
- [ ] `data_dir` validado y escribible

## Evidencias mínimas a conservar
- salida de `pytest -q`
- salida de `pytest tests/slow/test_ingestion_readiness.py -m slow -q`
- `docs/validation/ingestion_soak_evidence.json`
- `docs/validation/ingestion_canary_baseline.json`
- `docs/validation/ingestion_canary_report.json`
- commit validado
- fecha de ejecución
- logs JSON del run si hubo fallo

## Go / No-Go para live
- `GO`
  - todos los checks anteriores marcados
  - soak y canary verdes
  - sin diferencias semánticas entre baseline y candidata
- `GO CONDICIONAL`
  - solo si la única desviación está en latencia
  - y existe aceptación explícita del riesgo
- `NO-GO`
  - cualquier fallo en readiness
  - corrupción
  - pérdida silenciosa
  - `gap_irreparable` sin mitigación
  - diferencias semánticas entre baseline y candidata



