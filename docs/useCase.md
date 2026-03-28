# Use Cases — MyTraderSystem

## UC0: Pipeline run (dry/live)
- **Actor**: Trader/Usuario
- **Precondiciones**: Config valida (`config.<env>.yaml`); para live, red y disco disponibles.
- **Flujo principal**:
  1. Ejecutar `python -m app --env <env> --mode dry|live [--max-events N] [--duration S]`.
  2. El sistema crea `trace_id`, carga config y recolecta eventos (sinteticos en dry, WS/REST en live).
  3. Calcula features (SMA, retorno), genera senales, aplica riesgo, ejecuta en paper y actualiza portafolio.
  4. Emite log `pipeline ok` con metricas agregadas.
- **Flujos alternativos**:
  - Live falla (sin red/disco): degrada a modo dry y loguea warning.
  - Sin datos suficientes para ventanas: se omiten features y senales quedan flat.
- **Errores posibles**: configuracion invalida; precio <= 0; duracion agotada sin mensajes.
- **Resultado esperado**: exit 0, log con metricas; en live opcionalmente archivos Parquet escritos.

## UC1: Ingesta en vivo puntual
- **Actor**: Trader/Usuario
- **Precondiciones**: Config valida; conexion a internet; WS operativo.
- **Flujo principal**:
  1. `python -m app.ingestion.runner --env <env> --duration <segundos>`.
  2. Se abre WS y se normalizan mensajes a `MarketEvent`.
  3. Se escriben en Parquet.
  4. Al terminar duracion se flushea y loguea metricas (events_written, reconnects, lag).
- **Flujos alternativos**: reconexion con backoff; sin mensajes => events_written=0.
- **Errores posibles**: WS caido; payload invalido; disco sin espacio.
- **Resultado esperado**: archivos Parquet en layout; log final con metricas; exit 0.

## UC2: Backfill historico
- **Actor**: Trader/Usuario
- **Precondiciones**: Config REST valida; rango start/end en ISO UTC; intervalo soportado.
- **Flujo principal**:
  1. `python -m app.ingestion.backfill --env <env> --symbol <SYM> --start <ISO> --end <ISO> --interval <int> --batch <n> [--dry-run]`.
  2. Se normalizan klines y se calculan expected vs received.
  3. Se detectan gaps.
  4. Si no es dry-run, se deduplica y escribe Parquet ordenado.
  5. Log final resume rows/expected/gaps/rango/intervalo.
- **Flujos alternativos**: reintentos 429/5xx; dry-run no escribe.
- **Errores posibles**: intervalo no soportado; fechas sin TZ; agotados reintentos HTTP; fallo de escritura.
- **Resultado esperado**: Parquet sin duplicados; reejecucion no crece filas; metricas de gaps visibles.

## UC3: Inspeccion de datos almacenados
- **Actor**: Trader/QA
- **Precondiciones**: Parquet existente en `data/<env>`.
- **Flujo principal**:
  1. `python -m app.ingestion.inspect --env <env> [--symbol SYM] [--date YYYY-MM-DD] [--limit N]`.
  2. Carga archivos que cumplen filtros y devuelve filas en JSON.
- **Flujos alternativos**: sin filtros => primeras filas; sin match => “sin filas”.
- **Errores posibles**: ruta inexistente; Parquet corrupto.
- **Resultado esperado**: filas listadas; exit 0/1 segun configuracion actual.

## UC4: Calculo de features iniciales
- **Actor**: Trader/Desarrollador de estrategia
- **Precondiciones**: Lista de `MarketEvent` normalizados; ventanas definidas.
- **Flujo principal**:
  1. Llamar a `features.store.compute_features(events)`.
  2. Calcular SMA y retorno log.
  3. Devolver `FeatureVector` y loguear conteo.
- **Flujos alternativos**: ventana incompleta -> features omitidos.
- **Errores posibles**: precio <= 0 -> ValueError/log; lista vacia.
- **Resultado esperado**: lista de `FeatureVector`; sin efectos secundarios.

## UC5: Inspeccion/logging de metricas
- **Actor**: Operaciones/QA
- **Precondiciones**: Ejecucion reciente.
- **Flujo principal**: revisar logs JSON con `trace_id`, metricas de ingest/backfill/pipeline.
- **Errores posibles**: logs truncados; ausencia de trace_id.
- **Resultado esperado**: metricas legibles y correlacionables.
