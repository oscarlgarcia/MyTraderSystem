# Ingestion Promotion Runbook

## Scope
- este runbook gobierna la promotion de ingestion desde `paper` a `live`
- el scope promotable hoy sigue siendo `trade` + `kline`
- `book` y derivados siguen fuera hasta tener runtime, recovery y contracts dedicados
- la promotion ahora exige no solo evidence operativa, sino tambien gobierno del dato y surfaces de serving

## Required Inputs
- `docs/validation/approved_ingestion_datasets.json`
- `docs/validation/operational/paper/...`
- `docs/validation/operational/live/...`
- `docs/operations/ingestion/ingestion_operational_closure_paper.md`
- `docs/operations/ingestion/ingestion_operational_closure_live.md`
- `docs/operations/ingestion/ingestion_rollback_checklist.md`
- `docs/ops/live_cutover.md`
- `data/<env>/catalog/datasets.json`
- `data/<env>/catalog/dataset-contracts.json`
- `data/<env>/catalog/dataset-quality.json`
- `data/<env>/catalog/dataset-incidents.jsonl`
- `data/<env>/catalog/venue-capabilities.json`
- `data/<env>/catalog/delivery-contracts.json`
- `data/<env>/catalog/storage-lifecycle.json`
- `data/<env>/catalog/security-baseline.json`
- `data/<env>/catalog/future-scope.json`
- `data/<env>/serving/marketdata.sqlite`
- `data/<env>/publication/venue=BINANCE/stream_type=trade/events.jsonl`
- `data/<env>/publication/venue=BINANCE/stream_type=kline/events.jsonl`

## Promotion Prerequisites
1. El cierre paper y el cierre live del dataset candidato estan en `PASS`.
2. `data/<env>/catalog/datasets.json` y `data/<env>/catalog/dataset-contracts.json` fueron refrescados despues del ultimo cierre operativo.
3. `data/<env>/catalog/dataset-quality.json` existe y no contiene degradacion critica sin waiver valido.
4. `data/<env>/serving/marketdata.sqlite` existe y corresponde al refresh posterior al cierre live.
5. Los snapshots publicados para `trade` y `kline` existen en `data/<env>/publication/...`.
6. El capability registry confirma que `trade` y `kline` son promotables y `book` sigue excluido.

## Promotion Procedure
1. Ejecutar y revisar el cierre paper.
2. Ejecutar y revisar el cierre live.
3. Confirmar que catalogo, contracts, quality, delivery contracts y storage lifecycle quedaron refrescados.
4. Confirmar que el curated serving store existe y fue benchmarkeado.
5. Confirmar que los snapshots de publication se publicaron para los feeds candidatos.
6. Promover solo si todos los artefactos anteriores son frescos, consistentes y verdes.

## Promotion Blockers
- falta cualquier artefacto del catalogo
- falta `marketdata.sqlite`
- falta publication de alguno de los feeds que se van a consumir
- `book` aparece como soportado en el capability registry del target
- quality score degradado sin waiver explicito
- cierres operativos no frescos o no verdes

## Required Commands

```powershell
poetry run python scripts/ingestion_operational_cycle.py --target paper --env dev --runtime-env dev --runtime-base-dir data/dev --raw-base-dir data/dev/raw --normalized-path trade=data/dev/normalized/trades/env=dev/venue=BINANCE/symbol=BTCUSDT/date=2026-04-09 --normalized-path kline=data/dev/normalized/bars/env=dev/venue=BINANCE/symbol=BTCUSDT/date=2026-04-09 --symbol BTCUSDT --stream-types trade,kline --interval 1m --output-dir docs/validation/operational/paper --runner-id ingestion-paper-closure --trigger scheduled_paper_cycle --provenance-source ingestion_operational_cycle --runner-context-path ops/runner-context/paper-dev.json --surface-manifest ops/observability/paper-dev-surfaces.json --benchmark-min-rows-per-second 1
poetry run python scripts/ingestion_operational_cycle.py --target live --env dev --runtime-env dev --runtime-base-dir data/dev --raw-base-dir data/dev/raw --normalized-path trade=data/dev/normalized/trades/env=dev/venue=BINANCE/symbol=BTCUSDT/date=2026-04-09 --normalized-path kline=data/dev/normalized/bars/env=dev/venue=BINANCE/symbol=BTCUSDT/date=2026-04-09 --symbol BTCUSDT --stream-types trade,kline --interval 1m --output-dir docs/validation/operational/live --runner-id ingestion-live-closure --trigger scheduled_live_cycle --provenance-source ingestion_operational_cycle --runner-context-path ops/runner-context/live-dev.json --surface-manifest ops/observability/live-dev-surfaces.json
Invoke-RestMethod -Method Post -Uri 'http://127.0.0.1:8000/api/commands/catalog-refresh' -Form @{ requested_by = 'ingestion-promotion' }
Invoke-RestMethod -Method Post -Uri 'http://127.0.0.1:8000/api/commands/quality-refresh' -Form @{ requested_by = 'ingestion-promotion' }
Invoke-RestMethod -Method Post -Uri 'http://127.0.0.1:8000/api/commands/curated-refresh' -Form @{ stream_type = 'trade'; symbol = 'BTCUSDT'; requested_by = 'ingestion-promotion' }
Invoke-RestMethod -Method Post -Uri 'http://127.0.0.1:8000/api/commands/curated-refresh' -Form @{ stream_type = 'kline'; symbol = 'BTCUSDT'; requested_by = 'ingestion-promotion' }
Invoke-RestMethod -Method Post -Uri 'http://127.0.0.1:8000/api/commands/publish-snapshot' -Form @{ stream_type = 'trade'; symbol = 'BTCUSDT'; requested_by = 'ingestion-promotion' }
Invoke-RestMethod -Method Post -Uri 'http://127.0.0.1:8000/api/commands/publish-snapshot' -Form @{ stream_type = 'kline'; symbol = 'BTCUSDT'; requested_by = 'ingestion-promotion' }
```

## Exit Criteria
- catalogo, contracts, quality, serving y publication refrescados
- cierres paper/live verdes
- capability registry coherente con el scope promotable
- rollback checklist listo antes del cutover
