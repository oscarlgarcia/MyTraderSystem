# ADR-0001: Alcance historico de market data para ingestion

- Estado: Aprobada
- Fecha: 2026-04-02
- Firmado por: Arquitectura de ingestion / market data

## Decision

El alcance historico soportado por `app.ingestion.backfill` queda fijado de forma
definitiva en **`kline` y `trade`**.

`trade` historical backfill forma parte del contrato soportado actual del
modulo de ingestion. La implementacion usa Binance REST `aggTrades`, marca ese
origen historico en metadata y mantiene parity `raw -> replay -> normalized`.

## Contexto

El modulo ya soportaba backfill historico de `kline`, pero faltaba una decision
arquitectonica final para `trade` historical. Esa ambiguedad bloqueaba el
roadmap tick/trade y mantenia una discrepancia entre necesidades de research y
el contrato publico de ingestion.

## Consecuencias

- Backtesting historico aprobado hoy:
  - datasets de `kline`
  - datasets de `trade` normalizados desde Binance `aggTrades`
- El contrato historico deja de ser bars-only y pasa a exigir parity `raw -> replay -> normalized` para ambos feeds.
- Ningun feed, doc, CLI o checklist puede contradecir esta decision.
- Si en el futuro se sustituye `aggTrades` por otra fuente historica de trades, la decision debe actualizarse con una ADR posterior y evidencia nueva.
