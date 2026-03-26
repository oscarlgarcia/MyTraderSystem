# Arquitectura (Fase 1.1)

```mermaid
flowchart LR
  Dev["Dev CLI (make/poetry)"]
  Main["app.main.run()"]
  Stdout["stdout"]

  Dev -->|"make run-dev"| Main --> Stdout

subgraph Packages
  common
  ingestion
  features
  strategy
  risk
  execution
  portfolio
  observability
  ops
end

Main -. imports .-> common
Main -. imports .-> ingestion
Main -. imports .-> features
Main -. imports .-> strategy
Main -. imports .-> risk
Main -. imports .-> execution
Main -. imports .-> portfolio
Main -. imports .-> observability
Main -. imports .-> ops
```

Alcance actual: validar toolchain y entrada al proceso. No hay lógica de dominio ni integraciones externas.
