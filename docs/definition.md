# Definición técnica y funcional (versión inicial)

## Objetivo
Centralizar la descripción de componentes y sus responsabilidades en la fase temprana (Fase 1.3).

## Componentes
- **common**: DTOs, utilidades compartidas (normalización de símbolos, timestamps UTC).
- **ingestion**: conectores de mercado; entrada de datos normalizados a `MarketEvent`.
- **features**: cálculo y serving de `FeatureVector` para estrategias.
- **strategy**: generación de `Signal` a partir de features.
- **risk**: valida `Signal` y produce `OrderIntent` respetando límites.
- **execution**: adapta `OrderIntent` a órdenes de exchange y devuelve `ExecutionReport`.
- **portfolio**: mantiene `PortfolioState`, P&L y reconciliación.
- **observability**: logging/metrics/tracing; usa `TraceContext`.
- **ops**: orquestación, configuración, CLI.

## Relaciones y flujo (conceptual)
`MarketEvent` -> `FeatureVector` -> `Signal` -> `OrderIntent` -> `ExecutionReport` -> `PortfolioState`

## Contratos (resumen)
- `MarketEvent`: evento de mercado UTC, símbolo normalizado, price/size >=0.
- `FeatureVector`: features derivados en UTC.
- `Signal`: side buy/sell/flat, size >=0, confidence [0,1], ttl opcional.
- `OrderIntent`: quantity >0, TIF por defecto GTC, strategy_id obligatorio.
- `ExecutionReport`: estado de orden, filled_qty/avg_price >=0, correlación por client_order_id.
- `PortfolioState`: posiciones, cash, P&L; método `total_value()`.
- `TraceContext`: trace_id (+ span_id opcional) para correlación.

## Supuestos actuales
- Todos los timestamps deben ser timezone-aware en UTC.
- Normalización de símbolos a MAYÚSCULAS sin espacios.
- Sin dependencias externas para DTOs (solo stdlib).

## Pruebas mínimas
- Import de paquetes y `python -m app` sale con código 0.
- Validaciones de DTOs (cantidades positivas, timestamps UTC, confidence en rango).
- Round-trip de normalización de símbolo y cálculo de `total_value`.

## Evolución esperada
- Añadir serialización (p.ej. JSON) manteniendo contratos.
- Extender `MarketEvent` con profundidad de libro/funding cuando ingestion lo necesite.
- Añadir campos de riesgo (p.ej. risk_level) y de ejecución (p.ej. slippage estimado) respetando compatibilidad hacia atrás.
