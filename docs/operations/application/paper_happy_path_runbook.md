# Paper Happy Path Runbook

## Objetivo
Levantar la aplicacion en `paper` con el bundle de features existente para empezar a probar la estrategia en modo no productivo.

## Comando principal

```powershell
python -m app `
  --env dev `
  --mode paper `
  --max-events 1000000000 `
  --ingest-stream-types trade,kline `
  --feature-audit-path docs/validation/feature_decision_audit_paper.jsonl `
  --feature-consumer-name paper-runtime-strategy `
  --feature-consumer-kind strategy `
  --feature-dataset-id paper-runtime-btcusdt `
  --feature-schema-hash 27c5133305845d68af4062d03dec1c32d61c8d3083c35e382a4dd702e200fd18 `
  --feature-training-bundle-id legacy-legacy-paper `
  --feature-training-bundle-registry data/dev/feature-store/training-bundles
```

## Sidecars recomendados

```powershell
python -m app.controlplane.api --env dev --host 127.0.0.1 --port 8000
python -m app.controlplane.worker --env dev
```

## Verificaciones minimas
1. Existe `data/dev/feature-store/training-bundles/legacy-legacy-paper.json`.
2. `docs/validation/feature_releases.json` sigue marcando `legacy` como release activa para paper.
3. Si se quiere usar query/snapshot de ingestion, existen:
   - `data/dev/catalog/datasets.json`
   - `data/dev/serving/marketdata.sqlite`

## Limitaciones conocidas
- sigue siendo un proceso acotado por `--max-events`
- el path paper no sustituye un cierre operativo de ingestion ni la publication de snapshots
