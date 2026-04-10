# Ingestion Runbook

## Objetivo
Indice operativo del modulo de ingestion. Reune los runbooks de cierre, promotion, rollback y las superficies nuevas de catalogo, quality, serving y publication introducidas en la capa de gobierno del dato.

## Scope actual
- runtime soportado hoy: `trade`, `kline`
- `trade live` exige exact recovery, handoff historico-live, WS canary y evidencia runtime fresca
- `book` sigue fuera del scope promotable
- `bookTicker -> quotes/book` solo existe como expansion futura y no entra en el happy path actual

## Runbooks del modulo
- cierre paper: `docs/operations/ingestion/ingestion_operational_closure_paper.md`
- cierre live: `docs/operations/ingestion/ingestion_operational_closure_live.md`
- promotion: `docs/operations/ingestion/ingestion_promotion_runbook.md`
- rollback: `docs/operations/ingestion/ingestion_rollback_checklist.md`

## Runbooks relacionados
- app production happy path: `docs/operations/application/production_happy_path_runbook.md`
- app paper happy path: `docs/operations/application/paper_happy_path_runbook.md`
- app research happy path: `docs/operations/application/research_happy_path_runbook.md`
- app backtesting happy path: `docs/operations/application/backtesting_happy_path_runbook.md`
- features: `docs/operations/features/feature_promotion_runbook.md`

## Comandos base
- control plane web:

```powershell
python -m app.controlplane.api --env dev --host 127.0.0.1 --port 8000
```

- control plane worker:

```powershell
python -m app.controlplane.worker --env dev
```

- cierre operativo paper:

```powershell
poetry run python scripts/ingestion_operational_cycle.py --target paper --env dev --runtime-env dev --runtime-base-dir data/dev --raw-base-dir data/dev/raw --normalized-path trade=data/dev/normalized/trades/env=dev/venue=BINANCE/symbol=BTCUSDT/date=2026-04-09 --normalized-path kline=data/dev/normalized/bars/env=dev/venue=BINANCE/symbol=BTCUSDT/date=2026-04-09 --symbol BTCUSDT --stream-types trade,kline --interval 1m --output-dir docs/validation/operational/paper --runner-id ingestion-paper-closure --trigger scheduled_paper_cycle --provenance-source ingestion_operational_cycle --runner-context-path ops/runner-context/paper-dev.json --surface-manifest ops/observability/paper-dev-surfaces.json --benchmark-min-rows-per-second 1
```

- cierre operativo live:

```powershell
poetry run python scripts/ingestion_operational_cycle.py --target live --env dev --runtime-env dev --runtime-base-dir data/dev --raw-base-dir data/dev/raw --normalized-path trade=data/dev/normalized/trades/env=dev/venue=BINANCE/symbol=BTCUSDT/date=2026-04-09 --normalized-path kline=data/dev/normalized/bars/env=dev/venue=BINANCE/symbol=BTCUSDT/date=2026-04-09 --symbol BTCUSDT --stream-types trade,kline --interval 1m --output-dir docs/validation/operational/live --runner-id ingestion-live-closure --trigger scheduled_live_cycle --provenance-source ingestion_operational_cycle --runner-context-path ops/runner-context/live-dev.json --surface-manifest ops/observability/live-dev-surfaces.json
```

- backfill historico:

```powershell
python -m app.ingestion.backfill --env dev --symbol BTCUSDT --stream-type kline --interval 1m --start 2026-04-01T00:00:00Z --end 2026-04-01T01:00:00Z --dedup
python -m app.ingestion.backfill --env dev --symbol BTCUSDT --stream-type trade --start 2026-04-01T00:00:00Z --end 2026-04-01T01:00:00Z --dedup
```

## Artefactos que ahora forman parte del baseline operativo
- validacion operacional:
  - `docs/validation/operational/paper/...`
  - `docs/validation/operational/live/...`
- catalogo:
  - `data/<env>/catalog/datasets.json`
  - `data/<env>/catalog/dataset-contracts.json`
  - `data/<env>/catalog/dataset-quality.json`
  - `data/<env>/catalog/dataset-incidents.jsonl`
  - `data/<env>/catalog/venue-capabilities.json`
  - `data/<env>/catalog/delivery-contracts.json`
- serving/publication:
  - `data/<env>/serving/marketdata.sqlite`
  - `data/<env>/publication/venue=BINANCE/stream_type=<feed>/events.jsonl`
- control plane:
  - `data/<env>/control-plane/subscriptions.json`

## Criterios minimos antes de promover
1. `trade` y `kline` cierran en verde para el target.
2. Catalogo y contracts refrescados tras el cierre.
3. Quality score actualizado y sin incidentes criticos nuevos.
4. Curated serving refrescado y benchmark de serving en verde.
5. Snapshot publicado para los feeds que se van a consumir.
