# Definición técnica y funcional (versión inicial)

## Objetivo
Centralizar la descripción de componentes y sus responsabilidades en la fase temprana (Fase 1.3).

## Componentes
- **common**: DTOs, utilidades compartidas (normalización de símbolos, timestamps UTC).
- **ingestion**: conectores de mercado; entrada de datos normalizados a `MarketEvent`.
- **ingestion.client**: adaptadores Binance/Bybit (trade/kline 1m), normalización a DTO, construcción de URLs de suscripción.
- **features**: cálculo y serving de `FeatureVector` para estrategias.
- **strategy**: generación de `Signal` a partir de features.
- **risk**: valida `Signal` y produce `OrderIntent` respetando límites.
- **execution**: adapta `OrderIntent` a órdenes de exchange y devuelve `ExecutionReport`.
- **portfolio**: mantiene `PortfolioState`, P&L y reconciliación.
- **observability**: logging/metrics/tracing; usa `TraceContext`.
- **observability.logger**: formateo JSON, `trace_id` por contexto, niveles configurables y handler stdout/archivo opcional.
- **main.run / run_cycle**: arranque end-to-end (stub) que carga config, crea trace_id y recorre orden fijo de pasos (ingestion→features→strategy→risk→execution→portfolio) sin I/O real.
- **ops**: orquestación, configuración, CLI.
- **config**: carga de configuración por entorno (dev/test), validación mínima y overrides por env vars.

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
- `AppConfig`: env, data_dir, log_level; se carga desde `config.<env>.yaml` con override por env vars.
- Log records: JSON con `ts`, `level`, `logger`, `module`, `message`, `trace_id` opcional y extras seguros.
- `run_cycle`: orden de pasos y recorder para pruebas; ningún efecto externo.
- Ingesta: `normalize_trade`/`normalize_kline` validan precio/tamaño>=0, timestamps UTC; `build_ws_url` arma streams trade+kline por símbolo; `parse_message` despacha según tipo de stream.

## Supuestos actuales
- Todos los timestamps deben ser timezone-aware en UTC.
- Normalización de símbolos a MAYÚSCULAS sin espacios.
- Sin dependencias externas para DTOs (solo stdlib).
- Configs en YAML compatible con JSON para evitar dependencias; validación mínima de claves requeridas.
- Docker: la imagen copia `pyproject.toml` y `poetry.lock` y ejecuta `poetry install`; el código se monta por volumen para pruebas rápidas.

## Pruebas mínimas
- Import de paquetes y `python -m app` sale con código 0.
- Validaciones de DTOs (cantidades positivas, timestamps UTC, confidence en rango).
- Round-trip de normalización de símbolo y cálculo de `total_value`.

## Evolución esperada
- Añadir serialización (p.ej. JSON) manteniendo contratos.
- Extender `MarketEvent` con profundidad de libro/funding cuando ingestion lo necesite.
- Añadir campos de riesgo (p.ej. risk_level) y de ejecución (p.ej. slippage estimado) respetando compatibilidad hacia atrás.
- Añadir soporte a más entornos/config remotas si aparece vault o secret manager.
