# Functional Specification – MyTraderSystem

## Propósito del sistema
Proveer una plataforma de trading personal que permita:
- Ingerir datos de mercado (en vivo y de forma histórica) para análisis y backtesting.
- Generar features, señales, simulación de ejecución y estado de portafolio de manera reproducible.
- Operar inicialmente en modo “paper” con visibilidad y control total de datos y métricas.

## Actores
- **Trader/Usuario**: lanza ingesta, backfill y revisa resultados.
- **Ingesta WS/REST**: fuente de datos de mercado en vivo (ej. Binance testnet).
- **Backfill REST**: fuente histórica (klines).
- **Almacenamiento Parquet**: destino de datos normalizados.
- **Feature Store (inicial)**: consumidor de eventos para generar features.
- **Observabilidad**: receptor de logs estructurados.

## Casos de uso principales
1. **Ingesta en vivo puntual**: recibir trades/klines durante una ventana, normalizar, persistir y loguear métricas.
2. **Backfill histórico**: descargar klines de un rango, normalizar, detectar huecos, deduplicar y (opcional) escribir Parquet.
3. **Inspección de datos**: consultar rápidamente eventos almacenados filtrando por símbolo/fecha.
4. **Pipeline de features (inicial)**: a partir de eventos, calcular features básicas para backtesting (en memoria).

## Flujos detallados
### Ingesta en vivo (happy path)
1. Usuario ejecuta `make ingest-dev` con duración definida.
2. Sistema carga config (env, endpoints, símbolos) y crea `trace_id`.
3. Se abre WS y se suscribe a trades/klines.
4. Cada mensaje se normaliza a `MarketEvent` (UTC, símbolo en mayúsculas).
5. Se escribe en Parquet particionado por símbolo/fecha; flush según `flush_size`.
6. Al finalizar la duración: se cierran conexiones, se fuerza flush y se emiten métricas (events_written, reconnects, lag).

### Ingesta en vivo (alternativos)
- **Timeout/reconexión**: si la conexión cae, se reintenta con backoff hasta el máximo; se loguean reconexiones.
- **Datos inválidos**: precios o tamaños negativos se descartan con error en log y sin persistencia.

### Backfill histórico (happy path)
1. Usuario ejecuta `make backfill-dev` (dry-run) o `backfill-dev-write` con rango start/end.
2. Sistema carga config REST y normaliza símbolo.
3. Pagina klines REST hasta cubrir el rango; calcula `expected` vs `received`.
4. Detecta huecos (gaps) por intervalo; los reporta en log.
5. Si no es dry-run, deduplica contra archivos existentes y escribe Parquet ordenado.
6. Log final incluye rows, expected, gaps, rango e intervalo.

### Backfill histórico (alternativos)
- **429/5xx**: se reintenta con backoff limitado; tras agotar reintentos, falla con mensaje claro.
- **Intervalo no soportado**: se rechaza la ejecución con error.
- **Dry-run**: nunca escribe disco; sólo calcula métricas.

### Inspección de datos (happy path)
1. Usuario ejecuta `make inspect-dev` o CLI con filtros de símbolo/fecha/limit.
2. Sistema lee Parquet del layout actual y devuelve filas en JSON (stdout).

### Feature Store inicial (happy path)
1. Usuario pasa una lista de `MarketEvent` (de backfill o ingest buffer).
2. Se calculan features simples (p.ej., SMA, retornos) por símbolo y timestamp.
3. Se devuelven `FeatureVector` alineados temporalmente; se loguea conteo de features generados/omitidos.

## Reglas de negocio
- Todos los timestamps deben ser timezone-aware en UTC.
- Símbolos se normalizan a MAYÚSCULAS sin espacios.
- Precios y tamaños no pueden ser negativos; eventos inválidos no se persisten.
- Particionado de datos: `data/<env>/symbol=<SYM>/date=<YYYY-MM-DD>/data.parquet`.
- Deduplicación en backfill: clave (symbol, event_ts, price, size, source).
- Intervalos soportados en backfill: {1m, 3m, 5m, 15m, 30m, 1h}.
- Backoff en errores WS/REST: reintentos limitados; el fallo se expone en logs.

## Validaciones
- Config: claves requeridas (env, data_dir, log_level, ws_base, rest_base, symbols), log_level permitido, endpoints con esquema válido.
- Datos: precio/size ≥ 0; timestamps aware; símbolo normalizado.
- Backfill: intervalo válido; expected vs received; gaps calculados; dry-run no escribe.
- Parquet: schema estable (string/timestamp(ms,UTC)/float/map); orden temporal tras dedup.

## Edge cases
- Rango de backfill sin datos: rows=0, se loguea y no se escribe.
- Huecos en datos: gaps>0 reportados; archivos escritos sólo con lo disponible.
- Reejecución del mismo rango: dedup garantiza que el número de filas no crece.
- Conexión WS sin mensajes: runner termina al cumplir la duración sin errores.
- Inspect con filtros que no matchean: se devuelve “sin filas” (stdout) y exit 0/1 según decisión actual.

## Criterios de aceptación (por caso de uso)
- Ingesta en vivo: `python -m app.ingestion.runner --env dev --duration X` termina con exit 0, logs con events_written>0 (si la fuente entrega datos) y archivos Parquet presentes; sin exceptions.
- Backfill dry-run: exit 0, log con rows, expected, gaps; ningún archivo creado.
- Backfill con escritura: tras la ejecución existen archivos Parquet en el layout, `num_rows == received` y `gaps` reflejado en log; reejecutar el mismo comando mantiene el mismo `num_rows`.
- Inspección: comando CLI devuelve filas filtradas en JSON y respeta `limit`.
- Features iniciales: dada una serie corta conocida, las features calculadas coinciden con valores esperados (SMA/retornos) y se loguea el conteo de features generados.
