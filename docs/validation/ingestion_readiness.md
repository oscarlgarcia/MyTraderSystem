# Ingestion Readiness Checklist

## Estado esperado para considerar readiness tecnica

### Correccion
- [ ] `pytest -q` pasa completo
- [ ] `tests/slow/test_ingestion_readiness.py` pasa completo
- [ ] no hay duplicados no explicados en tests de reconnect/restart
- [ ] no hay corrupción de `data.parquet`

### Resiliencia
- [ ] reconnect con `checkpoint_store` preserva continuidad observable
- [ ] resync por snapshot no duplica eventos recientes
- [ ] reinicio tras fallo parcial preserva consistencia final

### Presion y limites
- [ ] 10k eventos mock bajo politica de sobrecarga producen degradacion visible, no silenciosa
- [ ] la politica configurada queda reflejada en métricas y logs
- [ ] el benchmark local de 10k eventos termina dentro del umbral documentado (<5s en Docker local/CI)

### Observabilidad
- [ ] `ingestion summary` presente en runs relevantes
- [ ] `ingestion health` presente al cierre
- [ ] fallo compuesto deja `error_category`, `events_invalid`, `events_persisted`, `trace_id`

### Seguridad operativa
- [ ] `--production-mode` rechaza defaults inseguros
- [ ] logs saneados, sin secretos
- [ ] `data_dir` validado y escribible

## Suite de readiness
- `test_end_to_end_live_mock_with_reconnect_checkpoint_and_sink_flush`
- `test_overload_policy_under_10k_mock_events`
- `test_restart_after_partial_failure_preserves_consistency`
- `test_corrupt_input_and_sink_failure_leave_system_diagnosable`

## Evidencias minimas a conservar
- salida de `pytest -q`
- salida de `pytest tests/slow/test_ingestion_readiness.py -m slow -q`
- commit validado
- fecha de ejecucion
- logs JSON del run si hubo fallo

## Decision
- `GO`: todos los checks marcados
- `GO CONDICIONAL`: solo si falla algo no crítico y queda mitigacion explícita
- `NO-GO`: cualquier fallo en readiness, corrupción, pérdida silenciosa o falta de diagnóstico
