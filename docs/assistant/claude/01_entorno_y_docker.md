# Entorno y Docker (Referencia Canónica)

## Python/Poetry
El proyecto declara `python = ^3.11` (ver `pyproject.toml`). La imagen Docker fija `python:3.11-slim` (ver `Dockerfile`).

Recomendación: usar Docker como entorno de referencia para evitar mismatch con el Python local.

## Docker (compose)
Archivo: `docker-compose.yml`.

Características:
- monta el repo en `/workspace` (bind-mount).
- mantiene `.venv` en un volumen `poetry_venv` para reutilizar dependencias.
- servicios:
  - `app`: contenedor persistente para iteración (por defecto `tail -f /dev/null`).
  - `webUI`: levanta `app.controlplane.api` en `0.0.0.0:8000`.
  - `control-plane-worker`: levanta `app.controlplane.worker`.

## Comandos canónicos (Makefile)
- Build: `make docker-build`
- Up: `make docker-up`
- Shell (contenedor persistente): `make docker-exec`
- Shell efímera: `make docker-shell`
- Down: `make docker-down`

Notas:
- si se edita código, el contenedor lo ve por bind-mount.
- si cambian dependencias, reconstruir o reinstalar dentro del contenedor.

