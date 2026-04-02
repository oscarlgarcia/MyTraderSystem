# ADR-0001: Alcance historico de market data para ingestion

- Estado: Aprobada
- Fecha: 2026-04-02
- Firmado por: Arquitectura de ingestion / market data

## Decision

El alcance historico soportado por `app.ingestion.backfill` queda fijado de forma
definitiva en **bars-only (`kline`)**.

`trade` historical backfill **no** forma parte del contrato soportado actual del
modulo de ingestion. No debe prometerse en CLI, documentacion, readiness ni
gating operativo mientras no exista una implementacion real con parity
`raw -> replay -> normalized`.

## Contexto

El modulo ya hace enforcement de bars-only en runtime y CLI, pero faltaba una
decision arquitectonica final. Esa ambiguedad dejaba abierto un roadmap
incorrecto para research tick/trade-based y podia inducir a asumir una
capacidad historica que no existe.

## Consecuencias

- Backtesting serio aprobado hoy:
  - solo para datasets historicos de `kline`.
- `trade` historical sigue fuera de alcance:
  - cualquier necesidad de backtesting tick/trade requiere una nueva epica de
    implementacion, no documentacion adicional.
- Ningun feed, doc, CLI o checklist puede contradecir esta decision.
- Si en el futuro se aprueba historical trade, esta ADR debe reemplazarse por
  una nueva decision junto con cambios de codigo, tests y support matrix.
