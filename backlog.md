# Backlog / Sugerencias

- Reemplazar stubs de `run_cycle` por implementaciones reales (ingestión WS/REST, features, estrategia, riesgo, ejecución, portfolio).
- Añadir rotación de logs (RotatingFileHandler) cuando haya logs persistentes.
- Extender CLI con comandos para modos `paper` y `live`.
- Serialización JSON de DTOs para debugging e integración con futuros buses.
- Validar config contra esquema (p.ej. `jsonschema`) cuando se permita dependencia externa ligera.
- Completar ingestión en vivo: conectar WS testnet real, manejo de reconexión y flush a Parquet (Fase 2.2/2.3).
- Añadir pruebas de integración usando websockets/httpx mockeados para asegurar compatibilidad con endpoints Binance.
