# Operación de Ingestion (Runbooks + Artefactos)

## Orden de arranque recomendado (paper/live)
1) Proceso principal (pipeline completo):
- `python -m app --env <env> --mode <dry|paper|live> ...`

2) Sidecars operativos (recomendados para paper/live):
- `python -m app.controlplane.api --env <env> --host 127.0.0.1 --port 8000`
- `python -m app.controlplane.worker --env <env>`

Motivo:
- la API expone comandos operativos.
- el worker ejecuta refresh y remediaciones (catálogo/quality/service-levels/gap-fill/serving/publication/benchmarks).

## Artefactos esperados por entorno
Persistencia base:
- `data/<env>/raw/...`
- `data/<env>/normalized/...`

Gobierno y serving:
- `data/<env>/catalog/datasets.json`
- `data/<env>/catalog/dataset-contracts.json`
- `data/<env>/catalog/dataset-quality.json`
- `data/<env>/catalog/dataset-incidents.jsonl`
- `data/<env>/catalog/dataset-service-levels.json`
- `data/<env>/catalog/gap-fill-plan.json`
- `data/<env>/catalog/delivery-contracts.json`
- `data/<env>/serving/marketdata.sqlite`
- `data/<env>/publication/venue=BINANCE/stream_type=trade/events.jsonl`
- `data/<env>/publication/venue=BINANCE/stream_type=kline/events.jsonl`

## Runbooks (fuentes de verdad)
Cierre operativo:
- `docs/operations/ingestion/ingestion_operational_closure_paper.md`
- `docs/operations/ingestion/ingestion_operational_closure_live.md`

Happy paths:
- `docs/operations/application/paper_happy_path_runbook.md`
- `docs/operations/application/production_happy_path_runbook.md`
- `docs/operations/application/research_happy_path_runbook.md`
- `docs/operations/application/backtesting_happy_path_runbook.md`

## Regla de soporte
- No considerar `book` soportado operativamente salvo que exista un plan explícito y gates/artefactos equivalentes a `trade/kline`.

