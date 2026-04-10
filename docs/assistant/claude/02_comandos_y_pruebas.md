# Comandos y Pruebas (Cómo Validar Cambios)

## Instalación y lint (local/Poetry)
Si no se usa Docker:
- `make install`
- `make lint` (compila bytecode: `python -m compileall app`)

## Tests (local/Poetry)
- Fast suite: `make test`
  - usa `PYTEST_FAST_ARGS` del `Makefile` y excluye `tests/slow`, `tests/network`, `tests/ops`.
- Full suite: `make test-all`

## Tests (Docker)
- Fast suite (efímero): `make docker-test`
- Full suite (persistente): `make docker-test-all`
- Slow suite: `make docker-test-slow`
- Ingestion strict readiness: `make docker-test-ingestion-strict`

## Pytest markers
Definidos en `pyproject.toml`:
- `slow`: escenarios end-to-end/readiness largos.
- `network`: tests que llaman endpoints reales del vendor.

Regla operativa:
- en CI/local por defecto, evitar `network` salvo necesidad explícita.

