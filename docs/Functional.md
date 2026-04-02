# Functional Specification – MyTraderSystem

## Proposito del sistema
Proveer una plataforma de trading personal que permita:
- Ingerir datos de mercado (en vivo e historicos) para analisis y backtesting.
- Generar features, senales, simulacion de ejecucion y estado de portafolio de manera reproducible.
- Operar inicialmente en modo “paper” con visibilidad y control total de datos y metricas.

## Actores
- **Trader/Usuario**: lanza ingesta, backfill y revisa resultados.
- **Ingesta WS/REST**: fuente de datos de mercado en vivo (ej. Binance testnet).
- **Backfill REST**: fuente historica (`kline` y `trade` via Binance `aggTrades`).
- **Almacenamiento Parquet**: destino de datos normalizados.
- **Feature Store (inicial)**: consumidor de eventos para generar features.
- **Observabilidad**: receptor de logs estructurados.
- **Pipeline run (dry/live)**: orquestacion end-to-end en `python -m app` con modo determinista y modo live acotado.

## Casos de uso principales
0. **Pipeline run**: ejecutar `python -m app --mode dry|live` para recorrer ingestion -> features -> estrategia -> riesgo -> ejecucion (paper) -> portfolio.
1. **Ingesta en vivo puntual**: recibir trades/klines durante una ventana, normalizar, persistir y loguear metricas.
2. **Backfill historico**: descargar `kline` o `trade` de un rango, normalizar, detectar huecos cuando aplica, deduplicar y (opcional) escribir Parquet.
3. **Inspeccion de datos**: consultar rapidamente eventos almacenados filtrando por simbolo/fecha.
4. **Pipeline de features (inicial)**: a partir de eventos, calcular features basicas para backtesting (en memoria).

## Flujos detallados
### Ingesta en vivo (happy path)
1. Usuario ejecuta `make ingest-dev` con duracion definida.
2. Sistema carga config (env, endpoints, simbolos) y crea `trace_id`.
3. Se abre WS y se suscribe a trades/klines.
4. Cada mensaje se normaliza a `MarketEvent` (UTC, simbolo en mayusculas).
5. Se escribe en Parquet particionado por simbolo/fecha; flush segun `flush_size`.
6. Al finalizar la duracion: se cierran conexiones, se fuerza flush y se emiten metricas (events_written, reconnects, lag).

### Ingesta en vivo (alternativos)
- Timeout/reconexion con backoff; metricas de reconnects.
- Datos invalidos (precio/size negativos) se descartan con log.

### Backfill historico (happy path)
1. Usuario ejecuta `make backfill-dev` (dry-run) o `backfill-dev-write` con rango start/end.
2. Sistema carga config REST y normaliza simbolo.
3. Pagina `kline` REST o `aggTrades` REST hasta cubrir el rango; para `kline` calcula `expected` vs `received`.
4. Para `kline` detecta huecos (gaps) por intervalo; para `trade` preserva orden temporal e identidad de `aggTrades`.
5. Si no es dry-run, deduplica contra archivos existentes y escribe Parquet ordenado.
6. Log final incluye rows, expected y gaps cuando aplican, rango y `feed_type`.

### Inspeccion de datos (happy path)
1. Usuario ejecuta `make inspect-dev` o CLI con filtros de simbolo/fecha/limit.
2. Sistema lee Parquet del layout actual y devuelve filas en JSON (stdout).

### Feature Store inicial (happy path)
1. Usuario pasa una lista de `MarketEvent` (de backfill o ingest buffer).
2. Se calculan features simples (SMA, retorno log) por simbolo y timestamp.
3. Se devuelven `FeatureVector` alineados temporalmente; se loguea conteo de features generados/omitidos.

### Pipeline run (dry/live)
1. Usuario ejecuta `python -m app --mode dry|live --max-events N [--duration S]`.
2. Modo dry: genera eventos sinteticos deterministas en memoria.
3. Modo live: usa WS/REST existentes con `ResilientRunner`; escribe Parquet acotado por `max_events`/`duration`.
4. Se calculan features -> senales -> riesgo -> ejecucion paper -> estado de portafolio.
5. Log final `pipeline ok` con metricas (events/features/signals/orders/fills/positions/cash).

## Reglas de negocio
- Timestamps timezone-aware en UTC.
- Simbolos en mayusculas sin espacios.
- Precio y tamanos no negativos.
- Particionado de datos: `data/<env>/symbol=<SYM>/date=<YYYY-MM-DD>/data.parquet`.
- Deduplicacion en backfill: clave (symbol, event_ts, price, size, source).
- Intervalos soportados en backfill: {1m,3m,5m,15m,30m,1h}.
- Backoff en errores WS/REST: reintentos limitados; fallo expuesto en logs.
- Logs persistentes: `log_file` rota 5MB, 3 backups; fallback a stdout si falla.
- Resiliencia: `ResilientRunner` expone metricas de buffer/lag y descarta eventos si `max_buffer` se supera.
- Pipeline dry: sin IO externo, determinista.
- Pipeline live: WS/REST + Parquet acotado y tolerante a fallo (fallback a dry en error).

## Validaciones
- Config: claves requeridas env/data_dir/log_level/ws_base/rest_base/symbols, log_level permitido, endpoints con esquema valido.
- Datos: precio/size >= 0; timestamps aware; simbolo normalizado.
- Backfill: intervalo valido; expected vs received; gaps calculados; dry-run no escribe.
- Parquet: schema estable (string/timestamp(ms,UTC)/float/map); orden temporal tras dedup.

## Edge cases
- Rango de backfill sin datos: rows=0, se loguea y no se escribe.
- Huecos en datos: gaps>0 reportados; archivos escritos solo con lo disponible.
- Reejecucion del mismo rango: dedup evita crecimiento de filas.
- Conexion WS sin mensajes: runner termina al cumplir duracion sin errores.
- Inspect sin match: “sin filas” y exit 0/1 segun configuracion actual.
- Pipeline live sin red/disco: se hace fallback a modo dry y se loguea warning.
