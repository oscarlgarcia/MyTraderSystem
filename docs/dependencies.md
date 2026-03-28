# Dependencias y acoplamientos — MyTraderSystem

## Módulos y dependencias internas
- `app.config` → stdlib (json, argparse, pathlib); consumido por main, runner, backfill, inspect.
- `app.common.dto` → stdlib (dataclasses); consumido por todos los módulos de dominio.
- `app.observability.logger` → stdlib logging/json/contextvars; usado por main, runner, backfill, resilience.
- `app.ingestion.client` → stdlib json/datetime; depende de `common.dto`.
- `app.ingestion.resilience` → stdlib time/contextvars; depende de `common.dto`.
- `app.ingestion.storage` → pyarrow; depende de `common.dto`.
- `app.ingestion.runner` → websockets, httpx, storage, resilience, logger.
- `app.ingestion.backfill` → httpx (REST), storage, logger, dto.
- `app.ingestion.inspect` → pyarrow.dataset; depende de config.
- Tests → pytest, httpx mocks, pyarrow.

## Dependencias externas
- `httpx` (REST)
- `websockets` (WS)
- `pyarrow` (Parquet)
- `pytest` (dev/test)
- `poetry` (gestión)

## Acoplamientos fuertes
- `runner` ↔ `storage`/`resilience`: ingesta en vivo escribe con `ParquetWriter` y controla flujo con `ResilientRunner`.
- `backfill` ↔ `storage`: escribe Parquet en el mismo layout; deduplicación implementada en storage.
- `dto` ↔ todos los módulos: cambios en DTO impactan a todos.
- `config` ↔ CLIs: claves/validación compartidas en runner/backfill/inspect.
- `pyarrow` ↔ storage/inspect: formato persistencia/lectura depende de esta lib.
- `logger` ↔ escritura de archivos: rotación obligatoria al usar `log_file`; si falla la ruta, fallback a stdout.

## Acoplamientos débiles
- `logger` inyectable (`get_logger`); `stream` personalizable.
- `resilience` envuelve `stream_fn`; se puede cambiar manteniendo interfaz.
- `backfill` usa endpoints configurables; proveedor no está hardcodeado.

## Riesgos
- Cambios en DTO o schema Parquet rompen consumidores.
- Compatibilidad de `pyarrow` (especialmente en Windows).
- Cambios de API del proveedor (Binance) afectarían normalización.
- Dedup: OOM si se supera umbral; mitigado con `max_dedup_rows` y ruta incremental.
- ResilientRunner sin backpressure puede bloquear si downstream se enlentece.

## Oportunidades de desacoplar
- Interfaces claras: `EventSink` (Parquet impl), `MarketSource` (REST/WS), `FeatureService`.
- Versionar schema Parquet en metadata y validar compat.
- Validador central de DTO separando normalización/validación.
- Configurar métricas/observabilidad como plugin (futuro) sin cambiar lógica de dominio.
