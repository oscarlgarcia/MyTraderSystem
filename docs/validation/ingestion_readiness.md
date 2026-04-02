# Ingestion Readiness Checklist

## Estado esperado para considerar readiness tecnica

### Correccion
- [ ] `pytest -q` pasa completo
- [ ] `tests/slow/test_ingestion_readiness.py` pasa completo
- [ ] `python scripts/ingestion_soak.py` devuelve `0`
- [ ] `python scripts/ingestion_canary.py --mode rest-baseline --refresh-baseline` devuelve `0`
- [ ] `python scripts/ingestion_canary.py --mode ws-live --symbol BTCUSDT --max-events 2 --duration-seconds 130` devuelve `0`
- [ ] `python scripts/ingestion_storage_benchmark.py` devuelve `0`
- [ ] `python -m app.main --release-gates --release-gates-target paper` devuelve `0`
- [ ] existe ADR aprobada para el alcance historico: `docs/adr/ADR-0001-historical-market-data-scope.md`
- [ ] el alcance historico soportado esta declarado explicitamente y alineado con la ADR: bars-only (`kline`); trade historical no se promete
- [ ] cada run/backfill persiste `metadata/instruments/.../runs/<trace_id>.json` y no hay alertas `provider_metadata_drift` sin revisar
- [ ] no hay duplicados no explicados en reconnect/restart/handoff
- [ ] no hay corrupcion de `data.parquet`

### Resiliencia
- [ ] reconnect con `checkpoint_store` preserva continuidad observable
- [ ] recovery exacto de bars no duplica borde reciente
- [ ] trades sin recovery exacto se marcan `gap_irreparable`
- [ ] reinicio tras fallo parcial preserva consistencia final

### Presion y limites
- [ ] 10k eventos mock bajo politica de sobrecarga producen degradacion visible, no silenciosa
- [ ] la politica configurada queda reflejada en metricas y logs
- [ ] el soak determinista deja `pass_ok=true`
- [ ] el benchmark de storage deja `pass_ok=true`

### Observabilidad
- [ ] `ingestion summary` presente en runs relevantes
- [ ] `ingestion health` presente al cierre
- [ ] `stream_metrics` y `streams_degraded` identifican el stream afectado
- [ ] `operational alert` aparece cuando corresponde

### Migracion / canary
- [ ] `ingestion_canary_report.json` existe
- [ ] `ingestion_ws_canary_report.json` existe
- [ ] `ingestion_canary_baseline.json` existe
- [ ] `ingestion_storage_benchmark.json` existe
- [ ] `ingestion_release_gates.json` existe
- [ ] `comparisons.jsonl` existe si hubo `shadow_mode`
- [ ] `comparison_reason = semantic_match`
- [ ] `diffs.row_count = 0`
- [ ] `diffs.projection_checksum_match = true`

### Seguridad operativa
- [ ] `--production-mode` rechaza defaults inseguros
- [ ] logs saneados, sin secretos
- [ ] `data_dir` validado y escribible

## Evidencias minimas a conservar
- salida de `pytest -q`
- salida de `pytest tests/slow/test_ingestion_readiness.py -m slow -q`
- `docs/validation/ingestion_soak_evidence.json`
- `docs/validation/ingestion_canary_baseline.json`
- `docs/validation/ingestion_canary_report.json`
- `docs/validation/ingestion_ws_canary_report.json`
- `docs/validation/ingestion_storage_benchmark.json`
- `docs/validation/ingestion_release_gates.json`
- commit validado
- fecha de ejecucion
- logs JSON del run si hubo fallo

## Go / No-Go para live
- `GO`
  - todos los checks anteriores marcados
  - soak y canary verdes
  - benchmark de storage verde
  - sin diferencias semanticas entre baseline y candidata
- `GO CONDICIONAL`
  - solo si la unica desviacion esta en latencia
  - y existe aceptacion explicita del riesgo
- `NO-GO`
  - cualquier fallo en readiness
  - corrupcion
  - perdida silenciosa
  - `gap_irreparable` sin mitigacion
  - diferencias semanticas entre baseline y candidata
