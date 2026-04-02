# ADR-0002 - Raw trade history scope for Binance historical ingestion

- Estado: Aprobada
- Fecha: 2026-04-02

## Contexto

El backfill historico de `trade` actualmente usa Binance REST `aggTrades`. Ese feed es util para backtesting trade-driven de granularidad media, pero no equivale a raw trade history tick-by-tick del venue.

La auditoria tecnica detecto que tratar `aggTrades` como si fuese raw trade history introduce riesgo cuantitativo directo en research y backtesting.

## Decision

Se aprueba mantener el soporte historico de Binance `trade` como `aggregate_trade` y no reclamar raw trade history.

Consecuencias explicitas:

- `trade` historical queda tipado como `aggregate_trade`.
- Se persisten `historical_trade_kind=aggregate_trade` y el endpoint `aggTrades` en raw, replay y normalized.
- Ningun contrato publico del modulo debe describir este dataset como raw trade history.
- Si el producto necesita raw trade history real, debe abrirse una decision posterior con evidencia de una fuente viable distinta o adicional.

## Razonamiento

- Permite backtesting reproducible sobre el alcance hoy implementado.
- Evita vender semantica falsa a research.
- No bloquea el roadmap inmediato bars + aggregate trades.
- Evita introducir una implementacion live/historical de trade no defendible solo para satisfacer nomenclatura.

## Implicaciones

- El modulo es apto para backtesting serio solo si los consumidores entienden que `trade historical` en Binance significa `aggregate_trade`.
- Estrategias que requieran microestructura real o orden tick exacto deben quedar fuera de alcance con este dataset.
- Una futura decision sobre raw trade history debera cubrir proveedor, retention, replay parity y garantias temporales.
