# Backlog / Sugerencias

- Reemplazar stubs de `run_cycle` por implementaciones reales (ingestion WS/REST, features, estrategia, riesgo, ejecucion, portfolio). [hecho]
- Fase 3.1 Feature Store: ventana deslizante en memoria + compute_features pura. [hecho]
- Fase 3.2 Feature Store: SMA + retornos log en compute_features, ventanas configurables. [hecho]
- Fase 3.3 Feature Store: API get_features en vivo / actualizacion incremental. [pendiente]
- Extender CLI con comandos para modos `paper` y `live`.
- Serializacion JSON de DTOs para debugging e integracion con futuros buses.
- Validar config contra esquema (ej. `jsonschema`) cuando se permita dependencia externa ligera.
- Completar ingestion en vivo: conectar WS testnet real, manejo de reconexion y flush a Parquet (Fase 2.2/2.3).
- Anadir pruebas de integracion usando websockets/httpx mockeados para asegurar compatibilidad con endpoints Binance.
- Evaluar compresion snappy y optimizar tamano/velocidad en Parquet una vez establecida la ingesta.
- Anadir tool de lectura rapida (CLI) para inspeccionar particiones y conteos.
- Integrar ResilientRunner con WS real y snapshot REST (httpx) en flujo continuo; exponer metricas en logger/CLI.
- Resiliencia operativa: metricas de buffer/lag y skips en Runner.
- Backfill historico: F1 CLI + fetch paginado en memoria.
- Backfill historico: F2 escribir Parquet + dedup + deteccion de huecos.
- Backfill historico: F3 metricas de calidad + dry-run avanzado.
- Backfill historico: F4 Make target + doc + prueba slow.
- Hardening inmediato: manejo de errores backfill, fallback logger, dedup umbral.
