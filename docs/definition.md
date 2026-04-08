# Definición técnica y funcional (versión inicial)

## Objetivo
Centralizar la descripción de componentes y sus responsabilidades en la fase temprana (Fase 1–2).

## Componentes
- **common**: DTOs, utilidades compartidas (normalización de símbolos, timestamps UTC).
- **ingestion**: conectores de mercado; entrada de datos normalizados a `MarketEvent`.
- **ingestion.client**: adaptadores Binance/Bybit (trade/kline 1m), normalización a DTO, construcción de URLs de suscripción.
- **ingestion.backfill**: descarga historica REST de `kline` y `trade` (via Binance `aggTrades`), normaliza a eventos canonicos y soporta escritura raw + normalized.
- **ingestion.runner**: ingesta en vivo puntual WS/REST, resiliencia básica, escritura Parquet.
- **features**: cálculo y serving de `FeatureVector` para estrategias (stub).
- **strategy**: generación de `Signal` a partir de features (stub).
- **risk**: valida `Signal` y produce `OrderIntent` respetando límites (stub).
- **execution**: adapta `OrderIntent` a órdenes de exchange y devuelve `ExecutionReport` (stub).
- **portfolio**: mantiene `PortfolioState`, P&L y reconciliación (stub).
- **observability**: logging/metrics/tracing; usa `TraceContext`.
- **observability.logger**: formateo JSON, `trace_id` por contexto, niveles configurables y handler stdout/archivo opcional.
- **main.run / run_cycle**: arranque end-to-end (stub) que carga config, crea trace_id y recorre orden fijo de pasos (ingestion→features→strategy→risk→execution→portfolio) sin I/O real.
- **ops**: orquestación, configuración, CLI.
- **config**: carga de configuración por entorno (dev/test), validación mínima y overrides por env vars.

## Relaciones y flujo (conceptual)
`MarketEvent` -> `FeatureVector` -> `Signal` -> `OrderIntent` -> `ExecutionReport` -> `PortfolioState`

## Contratos (resumen)
- `MarketEvent`: evento de mercado UTC, símbolo normalizado, price/size ≥ 0.
- `FeatureVector`: features derivados en UTC.
- `Signal`: side buy/sell/flat, size ≥ 0, confidence [0,1], ttl opcional.
- `OrderIntent`: quantity > 0, TIF por defecto GTC, strategy_id obligatorio.
- `ExecutionReport`: estado de orden, filled_qty/avg_price ≥ 0, correlación por client_order_id.
- `PortfolioState`: posiciones, cash, P&L; método `total_value()`.
- `TraceContext`: trace_id (+ span_id opcional) para correlación.
- `AppConfig`: env, data_dir, log_level; se carga desde `config.<env>.yaml` con override por env vars.
- Log records: JSON con `ts`, `level`, `logger`, `module`, `message`, `trace_id` opcional y extras seguros.
- Ingesta live: `normalize_trade`/`normalize_kline` validan precio/tamaño≥0, timestamps UTC; el runtime live soportado hoy es `trade` + `kline`. `trade` usa exact recovery e historical-to-live handoff por `aggregate_trade_id`; `kline` mantiene exact recovery verificada por ventanas de barras. `book` sigue fuera del scope live; `parse_message` despacha según tipo de stream.
- Backfill: fetch paginado de `kline` REST y `aggTrades`, normalizacion a eventos canonicos; dry-run (sin escritura) y modo persistente que deduplica y detecta huecos cuando aplica antes de escribir Parquet.
- Storage: `ParquetWriter` con buffer y partición `data/<env>/symbol=<SYM>/date=<YYYY-MM-DD>/data.parquet`; `read_parquet` para smoke.
- Resiliencia: `ResilientRunner` con backoff exponencial (cap 8s), detección de gap por timestamp, snapshot opcional y métricas (reconnects, last_lag_seconds).

## Backfill histórico (vista rápida)
- **ingestion.backfill**: descarga historica REST de `kline` y `trade` (via Binance `aggTrades`), normaliza a eventos canonicos y soporta escritura raw + normalized.
- Métricas de salida: `rows`, `expected`, `gaps`, `dry_run`, rango y símbolo.
- Dedup y detección de huecos: compara `event_ts` contra intervalo esperado; gaps > intervalo se reportan en log.
- Make targets: `backfill-dev` (dry-run), `backfill-dev-write` (escribe Parquet), configurables via vars `SYMBOL/START/END/FEED_TYPE/INTERVAL/BATCH`.

## Supuestos actuales
- Todos los timestamps deben ser timezone-aware en UTC.
- Normalización de símbolos a MAYÚSCULAS sin espacios.
- Sin dependencias externas para DTOs (solo stdlib).
- Configs en YAML compatible con JSON para evitar dependencias; validación mínima de claves requeridas.
- Docker: la imagen copia `pyproject.toml` y `poetry.lock` y ejecuta `poetry install`; el código se monta por volumen para pruebas rápidas.
- Nota de versiones: el lock se genera con Python 3.11; para hosts con Python 3.13 usar Docker o un venv 3.11/3.12.

## Pruebas mínimas
- Import de paquetes y `python -m app` sale con código 0.
- Validaciones de DTOs (cantidades positivas, timestamps UTC, confidence en rango).
- Round-trip de normalización de símbolo y cálculo de `total_value`.
- Backfill F1: `--dry-run` descarga rango corto y reporta conteo > 0.

## Evolución esperada
- Scope operativo por feed:
  - `paper` soporta `trade` y `kline`
  - `trade` en `paper` se valida por `replay`, parity, contratos del vendor y storage validation
  - `live` soporta `trade` + `kline`
- `trade` en `live` exige exact recovery, canary WS, soak runtime y handoff historico-live
- `book` queda fuera de `paper` y `live` hasta que exista runtime, schema y recovery dedicados
- la promotion paper/live consolida artifacts en `ingestion_operational_evidence*.json`, donde se bloquea evidence stale, origen incoherente, evidence derivada inline y ausencia de `owner`/`surface_ref`/`verification_ref` en las surfaces externas de observabilidad
- Añadir serialización (p.ej. JSON) manteniendo contratos.
- Extender `MarketEvent` con profundidad de libro/funding cuando ingestion lo necesite.
- Añadir campos de riesgo (p.ej. risk_level) y de ejecución (p.ej. slippage estimado) respetando compatibilidad hacia atrás.
- Añadir soporte a más entornos/config remotas si aparece vault o secret manager.
