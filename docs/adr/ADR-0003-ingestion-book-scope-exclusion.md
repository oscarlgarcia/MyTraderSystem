# ADR-0003 - Exclusion operativa del feed `book` en ingestion

## Estado
Aceptada

## Contexto
El modulo de ingestion soporta hoy `trade` y `kline` para `paper` y `live`, con recovery exacta verificada, handoff historico-live y gates operativos alineados. `book` sigue sin runtime dedicado, schema typed first-class, recovery exacta ni contrato de promotion comparable.

## Decision
- `book` queda formalmente fuera del scope soportado de `paper` y `live`.
- La exclusion debe reflejarse de forma consistente en:
  - `app/marketdata/support_matrix.py`
  - `app/ops/release_gates.py`
  - `app/ops/operational_evidence.py`
  - runbooks y documentacion del portal
- Ningun gate, runbook o pagina de modulo debe tratar `book` como candidato valido de promotion mientras no exista un proyecto tecnico dedicado para:
  - runtime
  - schema
  - persistence
  - recovery/handoff
  - evidence operativa

## Consecuencias
- Se reduce deuda conceptual y se evita sobreprometer soporte live.
- La promotion de ingestion queda acotada a `trade` + `kline`.
- Si el negocio necesita `book`, debe abrirse un backlog dedicado y no una reactivacion implicita del feed.
