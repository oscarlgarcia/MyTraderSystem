# Use Cases — MyTraderSystem

## UC1: Ingesta en vivo puntual
- **Actor**: Trader/Usuario
- **Precondiciones**: Config válida (`config.<env>.yaml`); conexión a internet; WS endpoint operativo.
- **Flujo principal**:
  1. Usuario ejecuta `python -m app.ingestion.runner --env <env> --duration <segundos>`.
  2. Sistema carga config, crea `trace_id` y abre WS suscribiéndose a trades/klines.
  3. Cada mensaje se normaliza a `MarketEvent` y se escribe en Parquet particionado.
  4. Al cumplir la duración, se fuerza flush y se emite log de métricas.
- **Flujos alternativos**:
  - Reconexión automática si la conexión WS cae (backoff limitado).
  - Sin mensajes recibidos: termina por duración y loguea `events_written=0`.
- **Errores posibles**: WS no disponible; payload inválido (precios/tamaños negativos); disco sin espacio.
- **Resultado esperado**: Archivos Parquet creados en `data/<env>/symbol=.../date=...`; log final con `events_written`, `reconnects`, `last_lag_seconds`, `elapsed_secs`.

## UC2: Backfill histórico
- **Actor**: Trader/Usuario
- **Precondiciones**: Config REST válida; rango start/end en ISO UTC; intervalo soportado (1m/3m/5m/15m/30m/1h).
- **Flujo principal**:
  1. Usuario ejecuta `python -m app.ingestion.backfill --env <env> --symbol <SYM> --start <ISO> --end <ISO> --interval <int> --batch <n> [--dry-run]`.
  2. Sistema pagina klines REST, normaliza a `MarketEvent`.
  3. Calcula `expected` vs `received` y detecta gaps.
  4. Si no es `--dry-run`, deduplica contra archivo existente y escribe Parquet ordenado.
  5. Log final resume rows, expected, gaps, rango e intervalo.
- **Flujos alternativos**:
  - `--dry-run`: no escribe disco; sólo métricas.
  - Reintentos ante 429/5xx con backoff limitado.
  - Rango sin datos: rows=0 se reporta y no se escribe.
- **Errores posibles**: intervalo no soportado; fechas sin TZ; agotados reintentos HTTP; fallo de escritura.
- **Resultado esperado**: Parquet sin duplicados; reejecución del mismo rango no aumenta filas; métricas de gaps visibles en logs.

## UC3: Inspección de datos almacenados
- **Actor**: Trader/QA
- **Precondiciones**: Parquet existente en `data/<env>`.
- **Flujo principal**:
  1. Usuario ejecuta `python -m app.ingestion.inspect --env <env> [--symbol SYM] [--date YYYY-MM-DD] [--limit N]`.
  2. Sistema carga los archivos que cumplen filtros y los devuelve en JSON (stdout).
- **Flujos alternativos**:
  - Sin filtros: devuelve primeras filas disponibles.
  - Filtros que no coinciden: mensaje “Sin filas”.
- **Errores posibles**: ruta de datos inexistente; Parquet corrupto.
- **Resultado esperado**: Filas listadas en stdout respetando filtros y limit; código de salida 0/1 según configuración actual.

## UC4: Cálculo de features iniciales (en memoria)
- **Actor**: Trader/Desarrollador de estrategia
- **Precondiciones**: Lista de `MarketEvent` ya normalizados; ventanas definidas en código (SMA/retornos).
- **Flujo principal**:
  1. Se llama a `features.store.compute_features(events)`.
  2. Para cada símbolo se calcula SMA y retorno log en la ventana configurada.
  3. Se emiten `FeatureVector` con claves esperadas y se loguea conteo.
- **Flujos alternativos**:
  - Ventana incompleta: features pueden omitirse o marcarse como None según política.
- **Errores posibles**: precio 0 (retorno indefinido) → ValueError/log; lista vacía.
- **Resultado esperado**: Lista de `FeatureVector` alineados temporalmente; sin efectos secundarios ni escrituras.

## UC5: Inspección/logging de métricas
- **Actor**: Operaciones/QA
- **Precondiciones**: Ejecución reciente de ingest o backfill.
- **Flujo principal**:
  1. Revisar logs JSON emitidos a stdout/archivo.
  2. Confirmar presencia de `trace_id`, `rows/events_written`, `gaps/expected` (backfill), `reconnects/lag` (ingest).
- **Flujos alternativos**:
  - Uso de `inspect-dev` para validar datos concretos si métricas sugieren anormalidades.
- **Errores posibles**: Logs truncados por configuración; ausencia de trace_id por error de contexto.
- **Resultado esperado**: Métricas legibles y correlacionables por trace_id para QA y troubleshooting.
