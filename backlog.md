# Backlog / Sugerencias

- Reemplazar stubs de `run_cycle` por implementaciones reales (ingestión WS/REST, features, estrategia, riesgo, ejecución, portfolio).
- Añadir rotación de logs (RotatingFileHandler) cuando haya logs persistentes.
- Extender CLI con comandos para modos `paper` y `live`.
- Serialización JSON de DTOs para debugging e integración con futuros buses.
- Validar config contra esquema (p.ej. `jsonschema`) cuando se permita dependencia externa ligera.
- Completar ingestión en vivo: conectar WS testnet real, manejo de reconexión y flush a Parquet (Fase 2.2/2.3).
- Añadir pruebas de integración usando websockets/httpx mockeados para asegurar compatibilidad con endpoints Binance.
- Evaluar compresión snappy y optimizar tamaño/velocidad en Parquet una vez establecida la ingesta.
- Añadir tool de lectura rápida (CLI) para inspeccionar particiones y conteos.
- Integrar ResilientRunner con WS real y snapshot REST (httpx) en flujo continuo; exponer métricas en logger/CLI.
- Backfill histórico: F1 CLI + fetch paginado en memoria ✅
- Backfill histórico: F2 escribir Parquet + dedup + detección de huecos ⬜
- Backfill histórico: F3 métricas de calidad + dry-run avanzado ✅
- Backfill histórico: F4 Make target + doc + prueba slow ✅
