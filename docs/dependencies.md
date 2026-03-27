# Dependencias y acoplamientos — MyTraderSystem

## Módulos y dependencias internas
- `app.config` → stdlib (json, argparse, pathlib); consumido por main, runner, backfill, inspect.
- `app.common.dto` → stdlib (dataclasses); consumido por todos los módulos de dominio.
- `app.observability.logger` → stdlib logging/json/contextvars; usado por main, runner, backfill, resilience.
- `app.ingestion.client` → stdlib json/datetime; dependencias internas: `common.dto`.
- `app.ingestion.resilience` → stdlib time/contextvars; depende de `common.dto`.
- `app.ingestion.storage` → pyarrow; depende de `common.dto`.
- `app.ingestion.runner` → websockets, httpx (config endpoints), storage, resilience, logger.
- `app.ingestion.backfill` → httpx (REST), storage, logger, dto.
- `app.ingestion.inspect` → pyarrow.dataset; depende de config.
- Tests → pytest, httpx mocks, pyarrow.

## Dependencias externas (librerías)
- `httpx` (REST)
- `websockets` (WS)
- `pyarrow` (Parquet)
- `pytest` (dev/test)
- `poetry` (gestión)

## Acoplamientos fuertes
- `runner` ↔ `storage`/`resilience`: la ingesta en vivo escribe directamente con `ParquetWriter` y usa `ResilientRunner` para control de flujo.
- `backfill` ↔ `storage`: escribe Parquet en el mismo layout; deduplicación implementada en storage.
- `dto` ↔ todos los módulos: cambios en DTO rompen consumidores.
- `config` ↔ todos los CLIs: forma de claves y validación está integrada en runner/backfill/inspect.
- `pyarrow` ↔ storage/inspect: formato de persistencia y lectura depende de esta lib.

## Acoplamientos débiles
- `logger` está inyectado por función (`get_logger`) y se puede reemplazar con mínimo impacto.
- `resilience` se usa como envoltura de stream_fn; se podría sustituir por otro componente si mantiene la misma interfaz.
- `backfill` consume REST vía httpx; la fuente (Binance) es configurable por endpoint, no está hardcodeada en lógica de negocio.

## Riesgos
- Cambio de esquema en `dto` afecta a todos los módulos (acoplamiento fuerte).
- `pyarrow` versión/compatibilidad (especialmente en Windows) puede romper lectura/escritura.
- Dependencia en Binance API shape (klines) en `backfill`: cambios del proveedor romperían normalización.
- Uso compartido de layout Parquet por runner y backfill: dedup/orden deben ser consistentes; divergencia genera datos corruptos.
- `ResilientRunner` acoplado a callbacks sin backpressure ni colas; si crece la lógica downstream, puede bloquearse.

## Oportunidades de desacoplar
- Introducir interfaces claras (puertos) para storage: `EventSink` con implementación Parquet; permitir futuro sink (S3, DB) sin tocar runner/backfill.
- Encapsular proveedor REST/WS en adaptadores intercambiables (Strategy Pattern) con contratos explícitos para klines/trades.
- Separar validación de DTO de normalización: un validador reutilizable reduciría acoplamiento en client/backfill.
- Inyectar `logger` y `resilience` vía parámetros/config para tests y futuras integraciones sin modificar código.
- Definir un contrato de feature store (`FeatureService`) para desacoplar cálculo de features del pipeline de ingest/backfill.
