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
make ingest-dev   # ingesta puntual 10 min contra testnet, escribe en data/dev
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

## Cómo probar la app (al cierre de cada fase)
- Local: `make install && make test` y `make run-dev` (debe imprimir "pipeline stub ok").
- Docker: `make docker-test` o `docker compose run --rm app sh -c "poetry install && poetry run pytest"`; validar `docker compose exec app python -m app --env dev`.
Actualiza estas instrucciones al completar cada fase con los comandos vigentes para ejecutar pruebas.

### Ejecutar con configuración
```bash
python -m app --env dev   # usa config.dev.yaml
python -m app --env test  # usa config.test.yaml
python -m app.ingestion.runner --env dev --duration 600  # ingesta puntual
python -m app.ingestion.inspect --env dev --limit 10    # inspecciona últimos eventos almacenados
```
Puedes sobrescribir el directorio de datos con `APP_DATA_DIR=/ruta python -m app --env dev`.

### Logging estructurado
Ejemplo de salida:
```
{"ts": "...Z", "level": "INFO", "logger": "app", "module": "main", "message": "pipeline stub ok", "trace_id": "<uuid>", "env": "dev", "data_dir": "data/dev", "steps": ["ingestion","features","strategy","risk","execution","portfolio"]}
```
El nivel se controla vía `log_level` en la config (dev=INFO, test=WARNING).

### Dependencias y Docker
- Tras añadir dependencias en `pyproject.toml`, reconstruye la imagen: `make docker-build` (copia `pyproject.toml` y `poetry.lock` para cache).
- Ejecutar tests en contenedor con deps nuevas: `make docker-test` (incluye `poetry install` dentro).
- Si usas Python 3.13 local, instala/ejecuta via Docker (la resolución de `pyarrow` se bloquea en 3.13; la imagen usa Python 3.11).
- Si ves `poetry: not found` en el contenedor, fuerza rebuild sin caché: `docker compose build --no-cache --pull`.
