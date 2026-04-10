# Contexto y Estado (Handoff a Claude Code)

Fecha de referencia: 2026-04-10 (Europe/Madrid).

Este repo (`MyTraderSystem`) es un bootstrap de una plataforma de trading algorítmico con un pipeline en 3 modos:
- `dry`: determinista, sin IO externo, apto para CI y validación local.
- `paper`: market data live con ejecución paper y auditoría obligatoria.
- `live`: path operativo más estricto disponible.

Entrada principal:
- `python -m app ...` (pipeline completo; ingestion es la primera fase).

## Mapa rápido del repo (superficies relevantes)
- `app/ingestion/`: ingest runner, backfill, sinks, checkpoints, resiliencia.
- `app/marketdata/`: modelos canónicos, contracts, catálogo, quality, query, snapshot, serving, publication.
- `app/controlplane/`: API operativa (FastAPI), worker de operaciones y store/auditoría.
- `scripts/`: tooling operativo (cierre ingestion, canary, soak, verificación observabilidad, docs search sync).
- `tests/`: suites unitarias e integración (incluye `slow`, `ops`, `network`).
- `docs/operations/`: runbooks (paper/live, cierre operativo, promotion, rollback).
- `docs-html/`: portal documental HTML estático + búsqueda (`docs_search_sync`).

## Commits clave (anclas de continuidad)
Estos SHAs resumen el trabajo ya consolidado. Úsalos como puntos de referencia al investigar regressions.
- `d32e852`: docs FAQ landing + onboarding ingestion (nuevas páginas FAQ + reindexación de búsqueda).
- `45c9b67`: hardening ingestion gap-fill, lineage y ops.
- `d402939`: ingestion governance y serving stack.
- `016d1f2`: reorganización de runbooks operativos por módulo.

## Scope operativo formal hoy
- Soportado `paper`: `trade`, `kline`.
- Soportado `live`: `trade`, `kline`.
- Excluido operativamente: `book` (puede haber piezas experimentales, pero no deben tratarse como soportadas).

## Invariantes "Do Not Regress" (ingestion governance/serving)
Tras un ciclo operativo (o tras comandos del control plane), deben poder materializarse estos artefactos por entorno:
- Contracts y catálogo: `data/<env>/catalog/dataset-contracts.json`, `data/<env>/catalog/datasets.json`.
- Calidad e incidentes: `data/<env>/catalog/dataset-quality.json`, `data/<env>/catalog/dataset-incidents.jsonl`.
- Service levels y gap fill: `data/<env>/catalog/dataset-service-levels.json`, `data/<env>/catalog/gap-fill-plan.json`.
- Serving y snapshots: `data/<env>/serving/marketdata.sqlite`.
- Publication: `data/<env>/publication/venue=BINANCE/stream_type=trade/events.jsonl` y `.../stream_type=kline/events.jsonl`.

Si alguno de estos falta después del refresh correcto, tratarlo como degradación operativa o regression.

