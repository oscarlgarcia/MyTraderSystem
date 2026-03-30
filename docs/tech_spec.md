# Especificacion tecnica y funcional

## Componentes principales
- **Ingestion**: normaliza `MarketEvent` desde WS/REST, escribe Parquet en modo live y provee fixtures dry.
- **Backfill**: descarga klines REST para rangos historicos, ordena por timestamp y puede deduplicar con `--dedup` antes de persistir.
- **Clave compartida de identidad**: `app.ingestion.client._key(event)` define la identidad canonica del evento para live, backfill y dedup en Parquet.
- **Streams registrables**: `app.ingestion.client.register_stream_builder(stream_type, fn)` permite extender `build_streams`/`build_ws_url` a tipos adicionales sin romper el default Binance (`trade`, `kline`).
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
- `app.main._resolve_runtime_options(args) -> dict[str, object]`
  - Deriva el runtime efectivo. Con `--fast-path`, fuerza `trace_steps=False`, `ingest_dedup=False`, `snapshot_enabled=False`, `summary_logging=False` y `ingest_batch_size >= 256`.
- `app.ingestion.client.build_ws_url(ws_base, symbols, stream_types=None) -> str`
  - Si `stream_types` es `None`, usa los builders por defecto `trade` y `kline`.
  - Para tipos nuevos, requiere `register_stream_builder(...)` y un `register_normalizer(...)` compatible con `parse_message`.
- `app.ingestion.backfill.run(argv=None, sink=None) -> int`
  - `--dedup` activa deduplicacion previa a sink con la misma clave `_key`.
- `app.features.store.compute_features(events, window=5, windows=None, aggregators=None, feature_set=None, cache=None) -> list[FeatureVector]`

## Ingestion y backfill
- **Live**:
  - `ResilientRunner` maneja reconexion, buffer y lag.
  - La deduplicacion se aplica con la misma clave `_key` en dos puntos:
    - al procesar el stream para evitar reprocesado;
    - justo antes de `writer.add` para evitar duplicados en Parquet live si llegan por una ruta no filtrada.
  - El handler local puede agrupar eventos antes de llamar a `writer.add`.
  - Metricas logueadas al final: `events_written`, `duplicates_dropped`, `batch_size`, `reconnects`, `buffer_skipped`, `max_latency_seconds`.
  - En `fast-path`, se omite el resumen de cierre de ingest live para reducir overhead de logging, pero se mantiene el log final de `pipeline ok`.
- **Backfill**:
  - `fetch_klines` pagina con manejo simple de `429`, `5xx` y timeout.
  - `normalize_kline_row` valida payload y genera `MarketEvent`.
  - Con `--dedup`, aplica deduplicacion con `_key` antes del sink y registra `duplicates_dropped`.
  - Sin `--dedup`, conserva duplicados tras normalizar y ordenar.

## Supuestos y limites
- No se anaden dependencias externas adicionales.
- La deduplicacion usa la tupla `(symbol, event_ts, price, size, source)` como identidad canonica.
- La deduplicacion de backfill es opt-in; la de live sigue controlada por `--ingest-dedup`.
- `ParquetWriter(dedup=True)` sigue actuando como barrera defensiva sobre particiones ya existentes.

## Relaciones
`WS/REST -> MarketEvent -> dedup/Parquet -> FeatureVector -> Strategy -> Risk -> Execution -> Portfolio -> Logs/Metrics`
