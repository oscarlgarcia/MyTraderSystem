# Especificación técnica y funcional

## Componentes principales
- **Ingestion**: normaliza `MarketEvent` desde WS/REST; escribe Parquet en modo live; provee fixtures dry.
- **Backfill**: descarga klines REST para rangos históricos; deduplica y opcionalmente escribe Parquet.
- **Feature Store (app/features/store.py)**:
  - Entrada: lista de `MarketEvent` por símbolo.
  - Proceso: ventana deslizante (tamaño configurable, default 5); cálculos `price`, `ret_1`, `sma_window`.
  - Salida: lista de `FeatureVector` alineados uno a uno con los eventos válidos.
  - Restricciones: sin IO, solo stdlib; descarta precios no finitos; limita memoria con `deque(maxlen)`.
- **Strategy**: consume `FeatureVector` y genera `Signal` (reglas simples).
- **Risk**: filtra señales y crea `OrderIntent` según límites.
- **Execution (paper)**: simula fills inmediatos; produce `ExecutionReport`.
- **Portfolio**: actualiza posiciones y cash a partir de reports.
- **Observability**: logging estructurado JSON con `trace_id`.

## Interfaces
- `compute_features(events: list[MarketEvent], window: int = 5) -> list[FeatureVector]`
  - Eventos vacíos → lista vacía.
  - Cada `FeatureVector.values` siempre incluye `price`; incluye `ret_1` si hay precio previo; incluye `sma_{window}` cuando la ventana está completa.

## Supuestos y límites
- No se añaden dependencias externas.
- Config YAML no cambia; el tamaño de ventana es argumento de función.
- Memoria acotada por `deque(maxlen=window)` por símbolo.

## Relaciones
Ingesta/Backfill → MarketEvent → compute_features → FeatureVector → Strategy → Risk → Execution → Portfolio → Logs/Métricas.
