# MyTraderSystem

Bootstrap for a personal trading platform (Fase 1.1: repo y toolchain).

## Requisitos
- Python 3.11+
- [Poetry](https://python-poetry.org/)

## Uso rapido
```bash
make install                      # instala dependencias (poetry)
make lint                         # lint/format
make test                         # pytest
make run-dev                      # pipeline dry end-to-end (sin IO externo)
make ingest-dev                   # ingesta puntual 10 min contra testnet, escribe en data/dev
make backfill-dev                 # backfill en memoria (ejemplo 1h BTCUSDT)

python -m app --env dev --mode dry                          # pipeline determinista (default)
python -m app --env dev --mode live --duration 30 --max-events 200  # pipeline live acotado WS/REST+Parquet
python -m app --env dev --mode dry --trace-steps                   # pipeline con trazas start/done por fase
python -m app --env dev --mode live --features-after-ingest        # ejecuta feature pipeline tras ingesta (solo log)
python -m app --env dev --mode live --ingest-max-buffer 20000      # ajusta buffer del runner (escalabilidad)
python -m app --env dev --mode live --no-ingest-dedup              # desactiva dedup para throughput (riesgo duplicados)
python -m app.ingestion.runner --env dev --duration 600     # ingesta puntual WS con flush
python -m app.ingestion.inspect --env dev --limit 10        # inspeccion rapida de Parquet
python -m app.ingestion.backfill --env dev --symbol BTCUSDT \
  --start 2024-01-01T00:00:00+00:00 --end 2024-01-01T01:00:00+00:00 \
  --interval 1m --batch 500 --dry-run                       # backfill en memoria
python -m app.ingestion.backfill --env dev --symbol BTCUSDT \
  --start 2024-01-01T00:00:00+00:00 --end 2024-01-01T01:00:00+00:00 \
  --interval 1m --batch 500 --dedup                         # backfill escribiendo Parquet sin duplicados
python -m app.ingestion.backfill --help                     # recordatorio de flags disponibles
python -m app.ingestion.demo --env dev --duration 30 --max-events 200  # demo en vivo con resumen de métricas
```

`run-dev` ejecuta el pipeline en modo dry para validar que el entorno arranca (sin IO externo).

## Sandbox Docker
- Construir imagen: `make docker-build`
- Shell interactiva con codigo bind-mount: `make docker-shell`
- Ejecutar tests dentro del contenedor: `make docker-test` (instala proyecto en editable y corre pytest)

El `docker-compose.yml` monta el repo en `/workspace`, por lo que cualquier cambio local se refleja inmediatamente dentro del contenedor para ejecutar pruebas.

### Usuarios Windows (sin Make)
- Build: `pwsh scripts/docker-build.ps1`
- Tests: `pwsh scripts/docker-test.ps1`
- Shell interactiva: `pwsh scripts/docker-test.ps1 -Shell` (abre bash dentro del contenedor ya levantado con `docker compose up -d`)

## Como probar la app
- Local: `make install && make test` y `make run-dev` (debe imprimir "pipeline ok").
- Docker: `make docker-test` o `docker compose run --rm app sh -c "poetry install && poetry run pytest"`; validar `docker compose exec app python -m app --env dev`.
- Prueba completa (suite entera): `python -m pytest` desde la raiz del repo.

### Ejecutar con configuracion (detalle)
- `python -m app --env dev --mode dry`  
  Pipeline completo en memoria (determinista, sin IO). Usa `config.dev.yaml`.
- `python -m app --env dev --mode live --duration 30 --max-events 200`  
  Pipeline live limitado: WS/REST + escritura Parquet acotada por eventos/duracion.
- `python -m app --env test --mode dry`  
  Igual que arriba pero leyendo `config.test.yaml`.
- `python -m app.ingestion.runner --env dev --duration 600`  
  Ingesta puntual con ResilientRunner y flush a Parquet.
- `python -m app.ingestion.inspect --env dev --limit 10`  
  Lista rapidamente filas de Parquet (filtros opcionales por simbolo/fecha).
- `python -m app.ingestion.backfill ... --dry-run`  
  Descarga klines, calcula expected/gaps sin escribir disco.
- `python -m app.ingestion.backfill ... --dedup`  
  Deduplica por la misma clave de ingest live y escribe Parquet ordenado para el rango indicado.
- `python -m app.ingestion.backfill ...` (sin `--dry-run` ni `--dedup`)  
  Escribe el lote tal cual llega tras normalizar/ordenar; util cuando se quiere inspeccionar duplicados.

### Documentacion
- [Functional](docs/Functional.md)
- [Use Cases](docs/useCase.md)
- [Dependencies](docs/dependencies.md)

### Backfill historico
- Seco (no escribe): `make backfill-dev START=2024-01-01T00:00:00+00:00 END=2024-01-01T01:00:00+00:00 SYMBOL=BTCUSDT`
- Escribe Parquet: `make backfill-dev-write START=2024-01-01T00:00:00+00:00 END=2024-01-01T01:00:00+00:00 SYMBOL=BTCUSDT`
- Campos clave: `INTERVAL` (soportados: 1m,3m,5m,15m,30m,1h), `BATCH` (<=1000).

Puedes sobrescribir el directorio de datos con `APP_DATA_DIR=/ruta python -m app --env dev`.

### Logging estructurado
Ejemplo de salida:
```
{"ts": "...Z", "level": "INFO", "logger": "app", "module": "main", "message": "pipeline ok", "trace_id": "<uuid>", "env": "dev", "mode": "dry", "metrics": {"events": 50, "features": 50, "signals": 50, "orders": 3, "fills": 3, "positions": {"BTCUSDT": 0.5}, "cash": 9950.0}}
```
El nivel se controla via `log_level` en la config (dev=INFO, test=WARNING).

### Feature Store inicial
- Cálculos: `price`, `ret_1` (log), `sma_N` (ventana configurable).
- Uso CLI: `python -m app --env dev --features-after-ingest` (solo logging de features tras ingesta).
- Uso directo en código (batch): `from app.features.pipeline import run_feature_pipeline`; pasar lista de `MarketEvent`.
- Uso incremental con caché: 
  ```python
  from app.features.engine import FeatureEngine
  eng = FeatureEngine(window=3)
  eng.update(ev)                      # devuelve FeatureVector o None
  latest = eng.get_latest(ev.symbol)  # lookup rápido en memoria
  historical = eng.get_at(ev.symbol, ev.event_ts)
  ```
- Pipeline batch reutilizando engine/caché:
  ```python
from app.features.pipeline import run_feature_pipeline
features = run_feature_pipeline(events, window=3)              # engine interno
# o reutilizando uno existente para lookups posteriores
eng = FeatureEngine(window=3)
features = run_feature_pipeline(events, window=3, engine=eng)
latest = eng.get_latest("BTCUSDT")
eng.log_metrics()  # imprime métricas básicas (latencia, drops, etc.)
```
- Persistencia offline (opcional):
  ```python
  from app.features.storage import save, load
  save(features, "out/features.json", feature_set=("default", "1.0.0"))
  features2, fs = load("out/features.json")
  ```
- Test E2E mock: `python -m pytest tests/slow/test_e2e_features_pipeline.py`.
- Registry: puedes registrar un feature set y crear el estado con `from app.features.registry import FeatureRegistry; state = FeatureRegistry().build_feature_state("default","1.0.0")` para calcular features incrementales.

### Dependencias y Docker
- Tras anadir dependencias en `pyproject.toml`, reconstruye la imagen: `make docker-build` (copia `pyproject.toml` y `poetry.lock` para cache).
- Ejecutar tests en contenedor con deps nuevas: `make docker-test` (incluye `poetry install` dentro).
- Si usas Python 3.13 local, instala/ejecuta via Docker (la resolucion de `pyarrow` se bloquea en 3.13; la imagen usa Python 3.11).
- Si ves `poetry: not found` en el contenedor, fuerza rebuild sin cache: `docker compose build --no-cache --pull`.
