# MyTraderSystem

Bootstrap for a personal trading platform (Fase 1.1: repo y toolchain).

## Requisitos
- Python 3.11+
- [Poetry](https://python-poetry.org/)

## Uso rápido
```bash
make install
make lint
make test
make run-dev
```

`run-dev` imprime un stub de pipeline para validar que el entorno arranca.

## Sandbox Docker
- Construir imagen: `make docker-build`
- Shell interactiva con código bind-mount: `make docker-shell`
- Ejecutar tests dentro del contenedor: `make docker-test` (instala proyecto en editable y corre pytest)

El `docker-compose.yml` monta el repo en `/workspace`, por lo que cualquier cambio local se refleja inmediatamente dentro del contenedor para ejecutar pruebas.

### Usuarios Windows (sin Make)
- Build: `pwsh scripts/docker-build.ps1`
- Tests: `pwsh scripts/docker-test.ps1`
- Shell interactiva: `pwsh scripts/docker-test.ps1 -Shell` (abre bash dentro del contenedor ya levantado con `docker compose up -d`)
