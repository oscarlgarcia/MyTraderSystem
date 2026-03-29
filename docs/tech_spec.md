# Especificación técnica y funcional

## Componentes principales
- **Ingestion**: normaliza `MarketEvent` desde WS/REST; escribe Parquet en modo live; provee fixtures dry.
- **Backfill**: descarga klines REST para rangos históricos; deduplica y opcionalmente escribe Parquet.
- **Feature Store (app/features/store.py)**:
  - Entrada: lista de `MarketEvent` por símbolo o llamadas incrementales.
  - Proceso: ventana deslizante (tamaño configurable, default 5); cálculos `price`, `ret_1` (log-return seguro, omite si prev o actual <=0), agregadores registrados (`sma`, `ema`, `max`, `min` por ventana), transformers opcionales (clip, scale, drop).
  - Validación: claves requeridas (`price`) presentes y finitas; se añade metadato `window_max`; eventos inválidos se descartan y se loguea `features discarded` con conteo.
  - Salida: lista de `FeatureVector` alineados uno a uno con los eventos válidos.
  - Restricciones: sin IO, solo stdlib; descarta precios no finitos; limita memoria con `deque(maxlen)`; no numpy/pandas.
- Registro de agregadores: `register_aggregator(name, fn)` donde fn recibe (symbol, prices, window, state) y devuelve (valor, state_actualizado).
- Transformadores/pipeline: `FeatureState` acepta `transformers` (lista de nombres); registry `TRANSFORMERS` incluye `clip_non_finite`, `scale_price_2x`, `drop_window_max`; se aplican en orden al `FeatureVector`.
- Feature Registry: `FeatureRegistry` permite registrar conjuntos de features (name, version, description, windows, aggregators, transformers) y consultarlos o listar versiones.
- Integración registry → estado: `build_feature_state(name, version)` crea `FeatureState` configurado según el feature set registrado.
- Feature Cache (app/features/cache.py): cache in-memory LRU por símbolo con índice temporal; APIs `put`, `get_latest(symbol)`, `get_at(symbol, ts, tolerance)` con expulsión por capacidad.
- **Feature Engine (app/features/engine.py)**:
  - Fachada pública con métodos `update(event)`, `update_batch(events)`, `get_latest(symbol)`, `get_at(symbol, ts, tolerance=None)`, `get_batch(symbol)`.
  - Construye internamente `FeatureState` + `FeatureCache` y opcionalmente aplica un `FeatureSet` registrado (ventanas/aggregadores/transformers).
  - No thread-safe por diseño (ingesta single-thread).
- **Feature pipeline wrapper (app/features/pipeline.py)**:
  - `run_feature_pipeline(events, window=5)` ejecuta `compute_features` y loguea métricas (`events_in`, `features_out`, `window`).
  - Usado opcionalmente tras ingest/backfill cuando se habilita `--features-after-ingest`.
- **E2E mock (tests/slow/test_e2e_features_pipeline.py)**:
  - Verifica que 5 eventos mock producen 5 `FeatureVector` y se loguea `feature pipeline done`.
- **Ingesta/Resilience**:
  - Flags CLI: `--ingest-max-buffer` (default 10k), `--no-ingest-dedup` para throughput (riesgo de duplicados).
  - Métricas: `reconnects`, `buffer_skipped`, `max_latency_seconds` se loguean al cerrar ingest live.
- **Demo de ingesta**:
  - `python -m app.ingestion.demo --env dev --duration 30 --max-events 200` corre stream real, escribe Parquet y ejecuta features, mostrando métricas (events, features, latency).
- **Trazas de pipeline**:
  - Flag CLI `--trace-steps` (default off) añade logs `pipeline step` con `phase` y `status` (start/done) y conteos.
- **Strategy**: consume `FeatureVector` y genera `Signal` (reglas simples).
- **Risk**: filtra señales y crea `OrderIntent` según límites.
- **Execution (paper)**: simula fills inmediatos; produce `ExecutionReport`.
- **Portfolio**: actualiza posiciones y cash a partir de reports.
- **Observability**: logging estructurado JSON con `trace_id`.

## Interfaces
- `compute_features(events: list[MarketEvent], window: int = 5, windows: Iterable[int] | None = None) -> list[FeatureVector]`
  - Eventos vacíos → lista vacía.
  - `price` siempre presente y finito; `ret_1` solo si prev>0 y precio actual>0; `sma_{w}` solo cuando hay al menos w precios; `window_max` siempre presente.
  - Elementos inválidos (sin claves requeridas o con valores no finitos) se descartan y se registra el total descartado.

## Supuestos y límites
- No se añaden dependencias externas.
- Config YAML no cambia; el tamaño de ventana es argumento de función.
- Memoria acotada por `deque(maxlen=window)` por símbolo.

## Relaciones
Ingesta/Backfill → MarketEvent → compute_features → FeatureVector → Strategy → Risk → Execution → Portfolio → Logs/Métricas.
