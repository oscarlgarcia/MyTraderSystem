# Production Happy Path Runbook

## Objetivo
Levantar la aplicacion en produccion real para empezar a hacer trading sobre el path soportado hoy.

## Supuestos
- `production-mode` exige `--env prod` y `--mode live`
- el runtime actual no es daemon puro; para dejarlo corriendo hay que usar un `--max-events` muy alto y supervisor externo
- el path live soportado hoy debe limitarse a feeds promotables; el baseline mas conservador es `kline`

## Comando principal

```powershell
python -m app `
  --env prod `
  --mode live `
  --production-mode `
  --max-events 1000000000 `
  --ingest-stream-types kline `
  --feature-audit-path docs/validation/feature_decision_audit_live.jsonl `
  --feature-consumer-name prod-live-strategy `
  --feature-consumer-kind strategy `
  --feature-dataset-id live-runtime-btcusdt `
  --feature-schema-hash <PROMOTED_FEATURE_SCHEMA_HASH> `
  --feature-training-bundle-id <PROMOTED_TRAINING_BUNDLE_ID> `
  --feature-training-bundle-registry /var/lib/mytradersystem/data/prod/feature-store/training-bundles
```

## Sidecars recomendados

```powershell
python -m app.controlplane.api --env prod --host 127.0.0.1 --port 8000
python -m app.controlplane.worker --env prod
```

## Que verificar antes de lanzar
1. Existe metadata de instrumentos en `data/prod/metadata/instruments/env=prod/venue=BINANCE/latest.json`.
2. El training bundle promocionado existe en el registry indicado.
3. El feature release activo y el `feature_schema_hash` coinciden.
4. Si se quiere bootstrap/serving de ingestion, tambien existen:
   - `data/prod/catalog/datasets.json`
   - `data/prod/catalog/dataset-contracts.json`
   - `data/prod/serving/marketdata.sqlite`

## Limitaciones conocidas
- no usar `--fast-path`
- no usar `--allow-live-fallback`
- el proceso debe quedar supervisado por systemd, container restart policy o equivalente
