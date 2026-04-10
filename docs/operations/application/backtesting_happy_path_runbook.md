# Backtesting Happy Path Runbook

## Objetivo
Ejecutar el path actual de backtesting con el stack que existe hoy. No hay un entrypoint unico `app.backtesting`; el happy path es composable.

## Paso 1: construir historico

```powershell
python -m app.ingestion.backfill --env dev --symbol BTCUSDT --stream-type kline --interval 1m --start 2026-04-01T00:00:00Z --end 2026-04-01T01:00:00Z --dedup
python -m app.ingestion.backfill --env dev --symbol BTCUSDT --stream-type trade --start 2026-04-01T00:00:00Z --end 2026-04-01T01:00:00Z --dedup
```

## Paso 2: validar el dataset

```powershell
python scripts/check_normalized_contract.py --env dev --symbol BTCUSDT --stream-type kline
python scripts/check_replay_parity.py --env dev --symbol BTCUSDT --stream-type trade
```

## Paso 3: consultar el dataset

```powershell
python -m app.controlplane.api --env dev --host 127.0.0.1 --port 8000
python -m app.controlplane.worker --env dev
Invoke-RestMethod -Method Get -Uri 'http://127.0.0.1:8000/api/datasets/catalog'
Invoke-RestMethod -Method Get -Uri 'http://127.0.0.1:8000/api/datasets/query?symbol=BTCUSDT&stream_type=kline'
Invoke-RestMethod -Method Get -Uri 'http://127.0.0.1:8000/api/datasets/replay-report?symbol=BTCUSDT&stream_type=trade'
```

## Resultado esperado
- raw y normalized generados para `trade` y `kline`
- contracts y replay parity en verde
- dataset visible por la query API
