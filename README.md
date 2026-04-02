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
python -m app --env dev --mode live --ingest-max-buffer 20000      # ajusta buffer del runner (escalabilidad)
python -m app --env dev --mode live --ingest-batch-size 50         # agrupa escrituras al writer en lotes locales
python -m app --env dev --mode live --ingest-backpressure-policy drop_newest  # politica de saturacion: pause/drop_oldest/drop_newest/fail
python -m app --env dev --mode live --ingest-stream-types kline               # limita el live a feeds concretos soportados
python -m app --env dev --mode live --no-ingest-dedup              # desactiva dedup para throughput (riesgo duplicados)
python -m app --env dev --mode live --allow-live-fallback          # permite fallback explicito a dry si live falla
python -m app --env dev --mode live --error-policy degraded        # politica explicita de error: fail_fast, allow_fallback, degraded
python -m app --env dev --mode live --fast-path                    # experimental: throughput alto, menos garantias
python -m app --env dev --mode live --ingest-lag-warn 2 --ingest-buffer-warn 0  # experimental: WARNINGs de presion/latencia
python -m app.ingestion.runner --env dev --duration 600     # ingesta puntual WS con flush
python -m app.ingestion.inspect --env dev --limit 10        # inspeccion rapida de Parquet
python -m app.ingestion.backfill --env dev --symbol BTCUSDT \
  --start 2024-01-01T00:00:00+00:00 --end 2024-01-01T01:00:00+00:00 \
  --interval 1m --batch 500 --dry-run                       # backfill bars-only (`kline`) en memoria
python -m app.ingestion.backfill --env dev --symbol BTCUSDT \
  --start 2024-01-01T00:00:00+00:00 --end 2024-01-01T01:00:00+00:00 \
  --interval 1m --batch 500 --dedup                         # backfill bars-only escribiendo raw + Parquet sin duplicados
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
- `python -m app --env dev --mode live --ingest-stream-types kline`  
  Selecciona los feeds live a ingerir. La matriz actual de soporte es:
  - `trade`: live bloqueado hasta que exista recovery exacto
  - `kline`: soporta live y handoff, pero no declara recovery exacto mientras el resync siga basado en snapshot REST acotado
  - `book`: no soporta live
  En cualquier `mode=live`, el arranque rechaza feeds sin `supports_live`. En `--production-mode`, ademas exige `supports_exact_recovery` y `supports_handoff`.
  Con la matriz actual, `mode=live` no productivo queda limitado a `--ingest-stream-types kline`; `--production-mode` rechaza todos los feeds hasta que exista un recovery exacto real para `kline`.
- Checkpoint live minimo: las ejecuciones reales de live persisten en `<data_dir>/<env>/state/ingestion-checkpoint.json`:
  - `last_event_ts` global maximo por compatibilidad
  - cursores/watermarks por stream `(venue, symbol, stream_type)`
  - una ventana corta de claves dedup por stream
  - si el checkpoint esta corrupto, live arranca con estado vacio y emite un warning explicito
- `python -m app --env dev --mode live --error-policy degraded`  
  Politica explicita de error para live:
  - `fail_fast` (default): propaga el error
  - `allow_fallback`: solo errores de fuente degradan a `dry`
  - `degraded`: solo errores de fuente devuelven `[]` y quedan logueados como degradacion
- Deduplicacion live/backfill/sink: ahora comparten `Deduplicator` con jerarquia explicita de identidad:
  - primero IDs nativos (`trade_id`, `sequence_id`, `source_id`) si el feed los trae
  - fallback heuristico `(symbol, event_ts, price, size, source)` solo cuando no existe identidad nativa
  - TTL corto y capacidad acotada para limitar memoria
  - el checkpoint persiste una ventana reciente de estas identidades
- Persistencia Parquet normalized v2: el path online ya no relee ni reconstruye la particion completa por lote. Escribe segmentos append-only nuevos y deja la compactacion/merge profundo para un paso offline. Si una escritura falla, solo se pierde el segmento temporal del lote en curso y el writer conserva en memoria solo los eventos no confirmados.
- Layout normalized v2:
  - `data/normalized/trades/env=<env>/venue=<venue>/symbol=<symbol>/date=<yyyy-mm-dd>/segments/segment-*.parquet`
  - `data/normalized/bars/env=<env>/venue=<venue>/symbol=<symbol>/date=<yyyy-mm-dd>/segments/segment-*.parquet`
  - opcionalmente, tras compactacion offline:
    - `.../date=<yyyy-mm-dd>/data.parquet`
  - cada dataset `v2` incluye `normalizer_version` como columna y como metadata de Parquet; la politica actual es global y fija `v1`
  - `normalized/trades` ya persiste columnas first-class:
    - `trade_id`
    - `side`
    - `exchange_ts`
    - `receive_ts`
    - `process_ts`
    - `source_id`
  - `normalized/bars` ya persiste columnas first-class:
    - `open`
    - `high`
    - `low`
    - `close`
    - `volume`
    - `volume_kind`
    - `interval`
    - `open_ts`
    - `close_ts`
    - `exchange_ts`
    - `receive_ts`
    - `process_ts`
    - `source_id`
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
  Descarga klines, calcula expected/gaps sin escribir disco. El alcance historico soportado queda fijado de forma definitiva en bars-only (`kline`) por `docs/adr/ADR-0001-historical-market-data-scope.md`.
- `python -m app.ingestion.backfill ... --dedup`  
  Deduplica por la misma clave de ingest live, escribe raw append-only en `data/raw/...` y normalized typed en Parquet para el rango indicado.
- `python -m app.ingestion.backfill ...` (sin `--dry-run` ni `--dedup`)  
  Escribe raw + normalized del lote tal cual llega tras normalizar/ordenar; util cuando se quiere inspeccionar duplicados.
- Extender streams: registra un builder con `register_stream_builder("foo", lambda symbol: f"{symbol}@foo")` y construye la URL con `build_ws_url(ws_base, symbols, stream_types=("trade", "foo"))`.
- Contrato de fuentes/sinks: live ahora se ejecuta sobre un `Source` (`BinanceSource` por defecto) y un `EventSink` (`ParquetEventSink` por defecto), lo que permite tests con mocks sin tocar Binance ni Parquet.
- Contrato canonico tipado: `app.marketdata.models` introduce `TradeEvent` y `BarEvent` como surface soportada de ingestion/storage/runtime. `BookEvent` se conserva como placeholder experimental para trabajo futuro de depth/quotes, pero hoy esta fuera de scope publico y `normalized` rechaza ese feed explicitamente. El hot path de `collect_events(...)` ya opera con eventos tipados; los adapters legacy quedan acotados a capas de compatibilidad explicita como storage legacy o consumidores antiguos.
- Semantica de `BarEvent.volume`: el contrato tipado expone `volume_kind` para que el consumidor no tenga que inferir unidades. En el scope soportado hoy (`Binance` `kline`), `volume_kind="quote"` y `volume` representa quote asset volume. En snapshots REST completos se toma la columna de quote asset volume; en fixtures legacy reducidos se mantiene el fallback compatible ya existente.
- Orquestacion desacoplada:
  - `app.ingestion.service.run_ingestion_service(...)` ejecuta solo ingestion
  - `app.main.run_trading_cycle(...)` ejecuta features, strategy, risk, execution y portfolio a partir de eventos ya ingeridos
  - `app.main.run_cycle(...)` queda como wrapper de composicion para compatibilidad
- Catalogo autoritativo de instrumentos: `app.marketdata.instrument_loader` carga un snapshot del `exchangeInfo` del venue y `app.marketdata.instruments` lo usa como source-of-truth para construir el `InstrumentCatalog` runtime. Cada run/backfill persiste el snapshot autoritativo en `data/.../metadata/instruments/...`, compara contra `latest.json` y emite `provider_metadata_drift` cuando cambian `tick_size`, `precision`, `contract_type` o metadata equivalente. La normalizacion persiste `base_asset`, `quote_asset`, `contract_type`, `tick_size`, `step_size`, `price_precision`, `size_precision`, `metadata_source`, `venue_snapshot_version`, `instrument_catalog_version` e `instrument_snapshot`, y el parquet `normalized` añade tambien `instrument_catalog_snapshot_hash` e `instrument_catalog_snapshot` a nivel de schema metadata. Si un simbolo no esta soportado por el snapshot autoritativo, la normalizacion falla rapido.
- Compactacion offline: `app.ingestion.compaction.compact_partition(...)` fusiona segmentos de una particion, aplica dedup/proyeccion de lectura y publica `data.parquet` como snapshot compactado.
- Adapters por feed/venue:
  - `app.marketdata.connectors.binance.BinanceTradeNormalizer`
  - `app.marketdata.connectors.binance.BinanceBarNormalizer`
  - `app.marketdata.connectors.binance_sources.BinanceTradeSource`
  - `app.marketdata.connectors.binance_sources.BinanceBarSource`
  `sources.py` mantiene la infraestructura generica de source/heartbeat/retry, pero la logica feed-specific de Binance deja de estar mezclada en el mismo bloque.
- Validacion explicita por tipo: `app.marketdata.validators` aplica checks por feed (`trade`, `kline`, `book`) para `NaN`, `inf`, signos, OHLC inconsistente y timestamps absurdos. En `BinanceSource`, los payloads raw invalidados van al DLQ; los eventos tipados invalidados inyectados desde fuentes custom fallan rapido.
- Semantica temporal explicita: el contrato tipado usa `exchange_ts`, `receive_ts` y `process_ts`. En Binance:
  - `trade.exchange_ts` <- payload `E`
  - `bar.exchange_ts` <- `k.T` si existe, si no `E`
  - `receive_ts` se captura en el borde WS/REST
  - `process_ts` se fija al normalizar y se persiste tambien en raw landing
- Watermark temporal por stream: `ResilientRunner` ya no compara todos los eventos contra un unico `last_event_ts`. Mantiene estado temporal por `(venue, symbol, stream_type)` para evitar falsos `late` o gaps cuando se intercalan varios instrumentos en el mismo run.
- Compatibilidad legacy: mientras exista `MarketEvent`, `event_ts` se interpreta temporalmente como `exchange_ts`; `receive_ts` y `process_ts` se conservan en `metadata` durante la migracion.
- Raw landing append-only: el wiring live por defecto persiste cada mensaje raw valido en `data/raw/env=<env>/venue=<venue>/stream_type=<stream>/symbol=<symbol>/date=<yyyy-mm-dd>/events.jsonl` antes de entregarlo al sink normalized. El registro incluye `payload`, `venue`, `stream_type`, `symbol`, `exchange_ts`, `receive_ts`, `process_ts`, `run_id`, `ingestion_seq`, `trace_id` y `source_id`.
- Replay determinista: `app.marketdata.replay.ReplaySource` relee raw landing y re-normaliza en orden determinista para backtesting y debugging. Prioriza `run_id + ingestion_seq` cuando ambos existen y mantiene fallback legacy por `receive_ts + path + line_no` para raws antiguos. Preserva `process_ts` cuando el raw lo trae; para raws legacy sin ese campo hace fallback explicito a `receive_ts`. Expone detector de metadata parcial para raws sin contrato fuerte completo. Soporta filtros por `symbol`, `stream_type`, ventana temporal y modos `full-speed` / `step-by-step`.
  - `ReplaySource` exige una `normalizer_version` explicita; hoy solo se soporta `v1`, compartida por todo el feed normalizado.
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
- Si no pasas `stream_types`, el runtime live usa por defecto `kline`.

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
- Escribe raw + Parquet: `make backfill-dev-write START=2024-01-01T00:00:00+00:00 END=2024-01-01T01:00:00+00:00 SYMBOL=BTCUSDT`
- Campos clave: `INTERVAL` (soportados: 1m,3m,5m,15m,30m,1h), `BATCH` (<=1000).
- Alcance soportado: solo bars (`kline`). Historical backfill de `trade` no esta implementado ni soportado.
- Decision arquitectonica vigente: `docs/adr/ADR-0001-historical-market-data-scope.md` fija bars-only como contrato historico oficial hasta nueva ADR con implementacion real.

Puedes sobrescribir el directorio de datos con `APP_DATA_DIR=/ruta python -m app --env dev`.

### Logging estructurado
Ejemplo de salida:
```
{"ts": "...Z", "level": "INFO", "logger": "app", "module": "main", "message": "pipeline ok", "trace_id": "<uuid>", "env": "dev", "mode": "dry", "metrics": {"events": 50, "features": 50, "signals": 50, "orders": 3, "fills": 3, "positions": {"BTCUSDT": 0.5}, "cash": 9950.0}}
```
El nivel se controla via `log_level` en la config (dev=INFO, test=WARNING).

En ejecuciones normales de ingest (`dry` y `live`) se emiten dos logs finales: `ingestion summary` e `ingestion health`. El summary consolida `source_events_in`, `events_valid`, `events_invalid`, `events_dedup_skipped`, `events_buffer_dropped`, `events_persisted`, `snapshot_runs`, `snapshot_rows`, `snapshot_duplicates_skipped`, `processing_latency_seconds`, `write_latency_seconds`, `event_gap_seconds`, `gaps_total`, `gap_irreparable_total`, `late_events`, `late_event_max_delay_seconds`, `temporal_policy` y `reconnects`, junto con los contadores legacy (`events_in`, `events_out`, `buffer_*`, `duplicates_dropped`, `result`, `error_policy`). `ingestion health` resume el estado operativo final de la ejecucion con el mismo `trace_id`. En live se mantiene tambien `ingestion live complete` por compatibilidad; `--fast-path` omite estos resumenes para reducir overhead. Live ya no degrada silenciosamente a `dry`: el comportamiento depende de la politica explicita (`fail_fast`, `allow_fallback`, `degraded`). La semantica temporal se controla con `--ingest-temporal-policy {accept,drop,fail}`: por defecto se aceptan eventos tardios/fuera de orden, se contabilizan por separado y no se mezclan con la latencia de proceso. El gap detection ahora distingue entre:
- `sequence_gap_detection`: fuerte, cuando existe cursor numerico (`trade_id` o `sequence_id`) y se rompe la secuencia esperada.
- `weak_gap_detection`: heuristico, cuando solo se observa un hueco temporal mayor que el umbral.
  - El recovery ya es especifico por feed:
    - `trade` no intenta rellenarse con snapshots de `kline`; si aparece un gap fuerte sin recovery exacto, se marca `gap_irreparable`.
  - `kline` puede usar snapshot REST de barras del mismo `venue/symbol/stream_type`, filtrando el borde para no duplicar eventos recientes. El request ya se calcula desde el gap observado (`start/end/limit`), pero mientras ese resync siga dependiendo de snapshot REST del vendor no se considera recovery exacto.
  - El handoff historico -> live ya tiene contrato explicito:
    - `HandoffSource` emite primero un bootstrap historico por ventana y luego entrega el stream live.
    - Deduplica el solape de borde con la misma identidad fuerte usada por live/storage.
  - Metricas temporales y de promotion safety adicionales:
    - `invalid_timestamp_total`
    - `exchange_receive_skew_seconds`
    - `receive_process_skew_seconds`
    - `shadow_row_diff_total`
    - `shadow_checksum_diff_total`
  - Alertas operativas adicionales:
    - `invalid_timestamp_detected`
    - `shadow_semantic_diff`
  - Antes de aceptar el primer tramo live de cada stream, compara las ultimas filas historicas emitidas con la cabecera live por identidad y timestamps para clasificar solape, regresion o gap de borde de forma determinista.
  - Si el primer evento live no garantiza continuidad respecto al ultimo bootstrap, marca `handoff_inconsistent` y deja que la politica operativa (`fail_fast` / `degraded`) decida si aborta o degrada.
- Liveness de conectores:
  - `BinanceSource` define heartbeat esperado por feed (`trade`, `kline`, `book`) y deriva un watchdog de inactividad para el websocket compartido.
  - Si no entran frames dentro del umbral, intenta `ping/pong` antes de declarar timeout.
  - Los errores de conector (`timeout`, `connection closed`, `OSError`) se clasifican de forma uniforme como `source/transient`, de modo que el runner reintenta con backoff + jitter.
  - El snapshot REST de recovery usa retry por endpoint:
    - `429`: politica mas permisiva
    - `5xx` / timeout / connect error: politica corta con backoff exponencial + jitter inyectable
  - Si los retries del snapshot se agotan, emite `snapshot_retry_exhausted`.
  - Un circuit breaker simple (`closed/open/half-open`) evita tormentas de retries mientras el endpoint REST sigue degradado.
- Observabilidad per-stream:
  - Todas las vistas por stream usan etiquetas obligatorias: `venue`, `symbol`, `stream_type`.
  - `ingestion summary` ahora incluye `stream_metrics`, una lista agregada por stream con:
    - `messages_in_total`
    - `messages_invalid_total`
    - `invalid_timestamp_total`
    - `duplicates_total`
    - `gaps_total`
    - `gap_irreparable_total`
    - `reconnects_total`
    - `heartbeat_missed_total`
    - `buffer_dropped_total`
    - `raw_write_latency`
    - `normalized_write_latency`
    - `exchange_receive_skew_seconds`
    - `receive_process_skew_seconds`
  - `ingestion health` añade `streams_observed` y `streams_degraded` para localizar rapidamente el stream afectado.
  - A nivel de ejecucion tambien se exponen:
    - `exchange_receive_skew_seconds`
    - `receive_process_skew_seconds`
    - `invalid_timestamp_total`
    - `shadow_row_diff_total`
    - `shadow_checksum_diff_total`
  - Las alertas operativas minimas salen como logs `operational alert` con `alert_type`, `alert_severity`, `observed`, `threshold` y `recommended_action`.
  - Tipos soportados y umbrales por defecto:
    - `reconnect_storm` (`warning`, 3 reconnects del mismo stream)
    - `gap_detected` (`warning`, 1 gap)
    - `gap_irreparable` (`error`, 1 gap irreparable)
    - `heartbeat_missed` (`warning`, 1 watchdog timeout)
    - `snapshot_retry_exhausted` (`error`, 1 agotamiento de retries REST o breaker abierto)
    - `invalid_timestamp_detected` (`warning`, 1 rechazo temporal invalido)
    - `shadow_semantic_diff` (`error`, 1 divergencia semantica entre primary y shadow)
    - `dlq_spike` (`warning`, 3 payloads invalidos del mismo stream)
    - `sink_failure` (`error`, 1 fallo de raw/error/normalized sink)
- Shadow mode / doble escritura:
  - `--ingest-pipeline-version {v1,v2}` elige la version principal del sink normalized.
  - `--ingest-shadow-mode` activa doble escritura sobre la version contraria cuando se usa el sink Parquet por defecto.
  - `--ingest-shadow-block-on-diff` aborta si el comparador detecta diferencias relevantes entre primary y shadow.
  - Las diferencias se persisten en `<data_dir>/shadow/env=<env>/comparisons.jsonl`.
  - En runtime, la comparacion se acota por defecto a las particiones afectadas por el lote/promocion actual.
  - El modo full-scan se mantiene como validacion offline para barridos completos del dataset.
  - El comparador shadow ya no se limita a conteos:
    - `row_count`
    - `identity set`
    - `checksum` por particion
    - `min/max event_ts`
    - `gaps_total`
  - Estrategia de migracion recomendada:
    1. ejecutar `v2` con `shadow_mode` hacia `v1`
    2. revisar `comparisons.jsonl`
    3. activar `--ingest-shadow-block-on-diff` antes de promocionar
    4. rollback: volver a `--ingest-pipeline-version v1` y desactivar shadow
- Validacion live local:
  - `python scripts/ingestion_soak.py`
  - `python scripts/ingestion_canary.py --refresh-baseline`
  - generan:
    - `docs/validation/ingestion_soak_evidence.json`
    - `docs/validation/ingestion_canary_baseline.json`
    - `docs/validation/ingestion_canary_report.json`
Los checkpoints solo se guardan tras un cierre limpio del sink; no ofrecen exactly-once.

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
