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
python -m app --env dev --mode live --ingest-batch-size 50         # agrupa escrituras al writer en lotes locales
python -m app --env dev --mode live --ingest-backpressure-policy drop_newest  # politica de saturacion: pause/drop_oldest/drop_newest/fail
python -m app --env dev --mode live --no-ingest-dedup              # desactiva dedup para throughput (riesgo duplicados)
python -m app --env dev --mode live --allow-live-fallback          # permite fallback explicito a dry si live falla
python -m app --env dev --mode live --error-policy degraded        # politica explicita de error: fail_fast, allow_fallback, degraded
python -m app --env dev --mode live --fast-path                    # experimental: throughput alto, menos garantias
python -m app --env dev --mode live --ingest-lag-warn 2 --ingest-buffer-warn 0  # experimental: WARNINGs de presion/latencia
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
- Levantar contenedor persistente listo para pruebas: `make docker-up`
- Shell contra el contenedor persistente: `make docker-exec`
- Bajar contenedor persistente: `make docker-down`
- Shell efimera con codigo bind-mount: `make docker-shell`
- Ejecutar tests dentro del contenedor: `make docker-test` (instala proyecto en editable y corre pytest)
- Ejecutar la suite completa en el contenedor persistente: `make docker-test-all`

El `docker-compose.yml` monta el repo en `/workspace` y mantiene `.venv` en un volumen dedicado, de modo que el contenedor persistente queda vivo y con dependencias reutilizables entre ejecuciones.

### Usuarios Windows (sin Make)
- Build: `pwsh scripts/docker-build.ps1`
- Levantar/bajar contenedor persistente: `pwsh scripts/docker-up.ps1` / `pwsh scripts/docker-up.ps1 -Down`
- Tests efimeros: `pwsh scripts/docker-test.ps1`
- Tests en contenedor persistente: `pwsh scripts/docker-test.ps1 -Persistent`
- Shell interactiva: `pwsh scripts/docker-test.ps1 -Shell` (abre bash dentro del contenedor ya levantado)

## Como probar la app
- Local: `make install && make test` y `make run-dev` (debe imprimir "pipeline ok").
- Docker: `make docker-test` o `docker compose run --rm app sh -c "poetry install && poetry run pytest"`; validar `docker compose exec app python -m app --env dev`.
- Docker persistente: `make docker-up`, luego `make docker-test-all` y `make docker-exec` para iterar sin recrear el contenedor.
- Prueba completa (suite entera): `python -m pytest` desde la raiz del repo.

### Ejecutar con configuracion (detalle)
- `python -m app --env dev --mode dry`  
  Pipeline completo en memoria (determinista, sin IO). Usa `config.dev.yaml`.
- `python -m app --env dev --mode live --duration 30 --max-events 200`  
  Pipeline live limitado: WS/REST + escritura Parquet acotada por eventos/duracion.
- `python -m app --env dev --mode live --ingest-batch-size 50`  
  Agrupa eventos en lotes locales antes de llamar al writer; reduce IO a costa de algo mas de latencia por lote.
- `python -m app --env dev --mode live --ingest-backpressure-policy drop_newest`  
  Politica de saturacion del runner:
  - `pause` (default): drena parcialmente y ralentiza la lectura
  - `drop_oldest`: expulsa el mas antiguo para hacer hueco
  - `drop_newest`: descarta el evento entrante
  - `fail`: aborta con error de `sink`
- `python -m app --env dev --mode live --allow-live-fallback`  
  Permite fallback explicito a `dry` si la ingesta real falla. Sin este flag, live ahora falla fuerte por defecto.
- Checkpoint live minimo: las ejecuciones reales de live persisten `last_event_ts` y una ventana corta de claves dedup en `<data_dir>/<env>/state/ingestion-checkpoint.json`. Si el checkpoint esta corrupto, live arranca con estado vacio y emite un warning explicito.
- `python -m app --env dev --mode live --error-policy degraded`  
  Politica explicita de error para live:
  - `fail_fast` (default): propaga el error
  - `allow_fallback`: solo errores de fuente degradan a `dry`
  - `degraded`: solo errores de fuente devuelven `[]` y quedan logueados como degradacion
- Deduplicacion live/backfill/sink: ahora comparten `Deduplicator`, con identidad por defecto `(symbol, event_ts, price, size, source)`, TTL corto y capacidad acotada para limitar memoria. El checkpoint persiste solo una ventana reciente de esta identidad.
- Persistencia Parquet: cada particion se escribe con `tmp + rename`. Si una escritura falla, el `data.parquet` previo queda intacto y el writer conserva en memoria solo los eventos no confirmados.
- `python -m app --env dev --mode live --fast-path`  
  Modo experimental de alto throughput: fuerza `dedup` off, `snapshot` off, logs live minimos (sin resumen agregado de ingest), `trace_steps` off y batch size grande.
- `python -m app --env dev --mode live --ingest-lag-warn 2 --ingest-buffer-warn 0`  
  Umbrales experimentales de WARNING para latencia y eventos descartados por buffer; por defecto no emiten alertas.
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
- Extender streams: registra un builder con `register_stream_builder("foo", lambda symbol: f"{symbol}@foo")` y construye la URL con `build_ws_url(ws_base, symbols, stream_types=("trade", "foo"))`.
- Contrato de fuentes/sinks: live ahora se ejecuta sobre un `Source` (`BinanceSource` por defecto) y un `EventSink` (`ParquetEventSink` por defecto), lo que permite tests con mocks sin tocar Binance ni Parquet.
- Payloads corruptos: `BinanceSource` valida por tipo antes de normalizar y escribe rechazos en un DLQ local JSONL (`<data_dir>/errors/ingestion-dlq.jsonl`) sin matar el stream por errores no fatales.

### Altas tasas
- Receta conservadora:
  ```bash
  python -m app --env dev --mode live --duration 60 --max-events 5000 --ingest-batch-size 64
  ```
  Mantiene dedup y snapshot; reduce IO agrupando escrituras.
- Receta throughput alto (experimental):
  ```bash
  python -m app --env dev --mode live --duration 60 --max-events 20000 --fast-path
  ```
  Activa batch grande, desactiva dedup live, snapshot, `trace_steps` y resumenes de ingest.
- Receta manual equivalente a `fast-path`:
  ```bash
  python -m app --env dev --mode live --duration 60 --max-events 20000 --ingest-batch-size 256 --no-ingest-dedup
  ```
  Reduce coste por evento, pero no desactiva snapshot ni logs por si quieres comparar.
- Alertas operativas:
  ```bash
  python -m app --env dev --mode live --ingest-lag-warn 2 --ingest-buffer-warn 0
  ```
  Emite `WARNING` si hay lag alto o descartes por buffer.
- Riesgos:
  - `--no-ingest-dedup` o `--fast-path`: puede persistir duplicados.
  - batch size alto: baja llamadas a IO, sube latencia de flush y riesgo de perder el lote en memoria si el proceso cae.
- `--fast-path`: pierde resync por snapshot y oculta resumenes de ingest para priorizar throughput.
 - La deduplicacion no pretende exactly-once: TTL y capacidad acotada priorizan contencion operativa y memoria estable sobre memoria infinita.

### Registrar una fuente/tipo nuevo
```python
from datetime import datetime, timezone

from app.common.dto import MarketEvent
from app.ingestion.client import (
    build_ws_url,
    register_normalizer,
    register_stream_builder,
)


def normalize_foo(payload: dict) -> MarketEvent:
    return MarketEvent(
        symbol=str(payload["symbol"]).upper(),
        event_ts=datetime.fromtimestamp(payload["ts"], tz=timezone.utc),
        price=float(payload["price"]),
        size=float(payload["size"]),
        source="foo",
        metadata={"raw_type": "foo"},
    )


register_stream_builder("foo", lambda symbol: f"{symbol.lower()}@foo")
register_normalizer("foo", normalize_foo)

url = build_ws_url("wss://stream.binance.com:9443", ["BTCUSDT"], stream_types=("trade", "foo"))
print(url)
```
- El builder define el fragmento de URL del stream.
- El normalizer debe devolver `MarketEvent`.
- Si no pasas `stream_types`, se conserva el comportamiento por defecto (`trade` + `kline`).

Ejemplo de uso con contratos `Source/Sink` en tests:
```python
from app.ingestion.pipeline import collect_events
from app.ingestion.sources import StaticSource

source = StaticSource(events=[ev1, ev2], snapshot_events=None)
sink = RecordingSink()
events = collect_events("live", cfg, duration_s=0, source=source, sink=sink)
```

### Documentacion
- [Functional](docs/Functional.md)
- [Use Cases](docs/useCase.md)
- [Dependencies](docs/dependencies.md)
- [Runbook de ingestion](docs/operations/ingestion_runbook.md)
- [Checklist de readiness](docs/validation/ingestion_readiness.md)

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

En ejecuciones normales de ingest (`dry` y `live`) se emiten dos logs finales: `ingestion summary` e `ingestion health`. El summary consolida `source_events_in`, `events_valid`, `events_invalid`, `events_dedup_skipped`, `events_buffer_dropped`, `events_persisted`, `snapshot_runs`, `snapshot_rows`, `snapshot_duplicates_skipped`, `processing_latency_seconds`, `write_latency_seconds`, `event_gap_seconds`, `late_events`, `late_event_max_delay_seconds`, `temporal_policy` y `reconnects`, junto con los contadores legacy (`events_in`, `events_out`, `buffer_*`, `duplicates_dropped`, `result`, `error_policy`). `ingestion health` resume el estado operativo final de la ejecucion con el mismo `trace_id`. En live se mantiene tambien `ingestion live complete` por compatibilidad; `--fast-path` omite estos resumenes para reducir overhead. Live ya no degrada silenciosamente a `dry`: el comportamiento depende de la politica explicita (`fail_fast`, `allow_fallback`, `degraded`). La semantica temporal se controla con `--ingest-temporal-policy {accept,drop,fail}`: por defecto se aceptan eventos tardios/fuera de orden, se contabilizan por separado y no se mezclan con la latencia de proceso. Los checkpoints solo se guardan tras un cierre limpio del sink; no ofrecen exactly-once.

### Seguridad operativa minima
- Los ficheros `config.<env>.yaml` no deben contener secretos. Si aparece una clave tipo `password`, `token`, `secret`, `api_key`, `authorization` o similar, `load_config()` falla.
- La interfaz explicita para secretos queda reservada a variables de entorno `APP_SECRET_*`; helper disponible: `app.config.get_secret_env(...)`.
- `--production-mode` activa validaciones estrictas:
  - exige `--mode live`
  - rechaza `--allow-live-fallback`
  - rechaza `--fast-path`
  - rechaza `error_policy` distinto de `fail_fast`
  - rechaza `--no-ingest-dedup`
  - rechaza politicas de backpressure con perdida (`drop_oldest`, `drop_newest`)
  - exige `data_dir` absoluto y escribible
- El logger JSON sanea payloads y extras anidados para evitar que credenciales o tokens terminen en stdout o en ficheros de log.

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
