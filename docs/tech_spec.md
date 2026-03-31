# Especificacion tecnica y funcional

## Componentes principales
- **Ingestion**: normaliza `MarketEvent` desde WS/REST, escribe Parquet en modo live y provee fixtures dry.
- **Backfill**: descarga klines REST para rangos historicos, ordena por timestamp y puede deduplicar con `--dedup` antes de persistir.
- **Clave compartida de identidad**: `app.ingestion.client._key(event)` define la identidad canonica del evento para live, backfill y dedup en Parquet.
- **Streams registrables**: `app.ingestion.client.register_stream_builder(stream_type, fn)` permite extender `build_streams`/`build_ws_url` a tipos adicionales sin romper el default Binance (`trade`, `kline`).
- **Validacion de payloads**:
  - `app.ingestion.client.validate_trade_payload`
  - `app.ingestion.client.validate_kline_payload`
  - `app.ingestion.client.register_payload_validator`
  - `parse_message` valida antes de normalizar y vuelve a validar el `MarketEvent` resultante.
- **Sources/Sinks de ingestion**:
  - `app.ingestion.sources.Source`: contrato minimo `stream(end_time)` / `snapshot()`.
  - `app.ingestion.sources.BinanceSource`: implementacion por defecto WS/REST.
  - `app.ingestion.sources.StaticSource`: fuente en memoria para tests.
  - `app.ingestion.sinks.EventSink`: contrato de salida.
  - `app.ingestion.sinks.ParquetEventSink`: adaptador del writer actual.
  - `app.ingestion.sinks.ErrorSink`: contrato de trazado para rechazos de payload.
  - `app.ingestion.sinks.JsonlErrorSink`: DLQ local JSONL.
- **Feature Store (`app/features/store.py`)**:
  - Entrada: lista de `MarketEvent` por simbolo o llamadas incrementales.
  - Proceso: ventana deslizante configurable; calculos `price`, `ret_1`, agregadores registrados (`sma`, `ema`, `max`, `min`) y transformers opcionales.
  - Validacion: `price` requerido y finito; eventos invalidos se descartan y se registra el conteo.
  - Salida: lista de `FeatureVector` alineados con los eventos validos.
- **Feature Engine (`app/features/engine.py`)**:
  - API: `update`, `update_batch`, `get_latest`, `get_at`, `get_batch`.
  - Internamente compone `FeatureState` + `FeatureCache`.
  - Observabilidad ligera: `events_in`, `features_out`, `dropped_non_finite`, `transform_errors`, `latency_max/avg`.
- **Feature pipeline wrapper (`app/features/pipeline.py`)**:
  - `run_feature_pipeline(events, window=5, engine=None)` calcula features con `FeatureEngine` y loguea metricas.
- **Feature storage (`app/features/storage.py`)**:
  - Persistencia batch opcional en JSON (`save`, `load`) con `storage_version` y metadatos de `feature_set`.
- **Strategy / Risk / Execution / Portfolio**:
  - `FeatureVector -> Signal -> OrderIntent -> ExecutionReport -> PortfolioState`.

## Interfaces relevantes
- `app.ingestion.pipeline.collect_events(mode, cfg, max_events, duration_s, logger, compute_features_after, max_buffer, dedup_enabled) -> list[MarketEvent]`
  - `dedup_enabled=True` activa deduplicacion live en `ResilientRunner` y una segunda barrera defensiva antes de `writer.add`.
  - `batch_size` controla el lote local antes de escribir en live; el handler hace flush del lote incompleto al cerrar.
  - `error_policy` define el comportamiento de fallo de live:
    - `fail_fast`: propaga el error
    - `allow_fallback`: solo errores `source` degradan a `dry`
    - `degraded`: solo errores `source` devuelven `[]`
  - `allow_live_fallback=False` se conserva como compatibilidad; internamente resuelve a `allow_fallback` si no se pasa `error_policy`.
  - `source` y `sink` permiten ejecutar el pipeline contra mocks sin tocar Binance ni Parquet.
- `app.ingestion.sources.BinanceSource(cfg).stream(end_time) -> Iterable[MarketEvent]`
  - Reutiliza `build_ws_url` y `parse_message`, preservando el comportamiento Binance actual.
  - Si recibe raw payload invalido o tipo desconocido, lo registra en `ErrorSink` y continua.
- `app.ingestion.sources.BinanceSource(cfg).snapshot() -> Iterable[MarketEvent]`
  - Reutiliza el snapshot REST de klines en un unico punto.
  - Si una fila individual de snapshot es invalida, la rechaza y continua con el resto.
- `app.main._resolve_runtime_options(args) -> dict[str, object]`
  - Deriva el runtime efectivo. Con `--fast-path`, fuerza `trace_steps=False`, `ingest_dedup=False`, `snapshot_enabled=False`, `summary_logging=False` y `ingest_batch_size >= 256`.
  - Propaga `ingest_lag_warn` y `ingest_buffer_warn` como umbrales opt-in para alertas de operacion.
  - Propaga `allow_live_fallback` para debugging controlado; no se activa por defecto.
- `app.ingestion.client.build_ws_url(ws_base, symbols, stream_types=None) -> str`
  - Si `stream_types` es `None`, usa los builders por defecto `trade` y `kline`.
  - Para tipos nuevos, requiere `register_stream_builder(...)` y un `register_normalizer(...)` compatible con `parse_message`.
- `app.ingestion.backfill.run(argv=None, sink=None) -> int`
  - `--dedup` activa deduplicacion previa a sink con la misma clave `_key`.
- `app.features.store.compute_features(events, window=5, windows=None, aggregators=None, feature_set=None, cache=None) -> list[FeatureVector]`

## Ingestion y backfill
- **Live**:
  - `ResilientRunner` maneja reconexion, buffer y lag.
  - `collect_events` ya no depende directamente de Binance ni de `ParquetWriter`; consume un `Source` y un `EventSink`.
  - Los errores se clasifican como `source`, `parse`, `validation` o `sink`, y como `transient` o `permanent`.
  - Solo `source/transient` se reintentan en `ResilientRunner`.
  - Los rechazos de payload no fatales incrementan `rejected_payloads` y se reflejan en `ingestion summary`.
  - La deduplicacion se aplica con la misma clave `_key` en dos puntos:
    - al procesar el stream para evitar reprocesado;
    - justo antes de `writer.add` para evitar duplicados en Parquet live si llegan por una ruta no filtrada.
  - El handler local puede agrupar eventos antes de llamar a `writer.add`.
  - Metricas logueadas al final: `events_written`, `duplicates_dropped`, `batch_size`, `reconnects`, `buffer_skipped`, `max_latency_seconds`.
  - Ademas se emite `ingestion summary` como JSON consolidado con:
    - `events_in`: eventos observados por source/snapshot antes de dedup del runner
    - `events_out`: eventos unicos realmente entregados al pipeline y devueltos por `collect_events`
    - `duplicates_dropped`: suma de duplicados filtrados por runner y por la barrera defensiva previa al writer
    - `reconnects`, `buffer_skipped`, `max_latency_seconds`, `dedup_on`, `batch_size`
  - Si live falla, el comportamiento depende de `error_policy`. Errores `sink` nunca se convierten en fallback/degraded.
  - En `fast-path`, se omite el resumen de cierre de ingest live para reducir overhead de logging, pero se mantiene el log final de `pipeline ok`.
  - Si `buffer_skipped > ingest_buffer_warn` o `max_latency_seconds > ingest_lag_warn`, `collect_events` emite un `WARNING` una vez por ciclo live.
- **Backfill**:
  - `fetch_klines` pagina con manejo simple de `429`, `5xx` y timeout.
  - `normalize_kline_row` valida payload y genera `MarketEvent`.
  - Con `--dedup`, aplica deduplicacion con `_key` antes del sink y registra `duplicates_dropped`.
  - Sin `--dedup`, conserva duplicados tras normalizar y ordenar.

## Altas tasas y tuning operativo
- **Flags relevantes**:
  - `--ingest-batch-size N`: agrupa eventos antes de `writer.add`; reduce llamadas de IO.
  - `--no-ingest-dedup`: desactiva dedup live en `ResilientRunner` y en la barrera previa al writer.
  - `--fast-path`: modo experimental que fuerza `ingest_dedup=False`, `snapshot_enabled=False`, `summary_logging=False`, `trace_steps=False` y `ingest_batch_size >= 256`.
  - `--ingest-lag-warn S`, `--ingest-buffer-warn N`: emiten `WARNING` una vez por ciclo si se superan los umbrales.
- **Recetas**:
  - perfil estable: batch medio (`32-128`), dedup on, snapshot on.
  - perfil throughput: `--fast-path` o `--ingest-batch-size 256 --no-ingest-dedup`.
- **Trade-offs**:
  - batch alto reduce overhead por llamada pero retrasa flush.
  - dedup off mejora eventos/s pero permite repetidos en logs y Parquet.
  - snapshot off evita coste de resync pero deja huecos si el stream llega tarde o se corta.
  - summary logging off reduce ruido y overhead, pero quita el cierre agregado de ingest.

## Extension de tipos de stream
- `register_stream_builder(stream_type, fn)` registra como construir el sufijo de stream por simbolo.
- `register_normalizer(stream_type, fn)` registra como convertir el payload raw a `MarketEvent`.
- Flujo minimo:
  1. registrar builder (`foo -> {symbol}@foo`);
  2. registrar normalizer `foo`;
  3. construir URL con `build_ws_url(..., stream_types=("trade", "foo"))`;
  4. dejar que `parse_message` enrute por el tipo registrado.
- Restriccion actual: la extension es por tipo de stream, no por adaptadores completos de exchange; sigue siendo el mismo contrato Binance-compatible de URL multiplexada.

## Supuestos y limites
- No se anaden dependencias externas adicionales.
- La deduplicacion usa la tupla `(symbol, event_ts, price, size, source)` como identidad canonica.
- La deduplicacion de backfill es opt-in; la de live sigue controlada por `--ingest-dedup`.
- `ParquetWriter(dedup=True)` sigue actuando como barrera defensiva sobre particiones ya existentes.
- El resumen agregado de ingest no introduce un sistema nuevo de metricas; reutiliza los contadores ya disponibles en `ResilientRunner` y en el handler live.

## Relaciones
`WS/REST -> MarketEvent -> dedup/Parquet -> FeatureVector -> Strategy -> Risk -> Execution -> Portfolio -> Logs/Metrics`
