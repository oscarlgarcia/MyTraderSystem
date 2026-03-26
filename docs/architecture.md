# Arquitectura (Fase 1.1)

```mermaid
flowchart LR
  Dev["Dev CLI (make/poetry)"]
  Main["app.main.run()"]
  Config["config loader"]
  Stdout["stdout"]

  Dev -->|"make run-dev"| Main --> Stdout
  Main -->|"load_config"| Config

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

subgraph DTOs
  MarketEvent
  FeatureVector
  Signal
  OrderIntent
  ExecutionReport
  PortfolioState
  TraceContext
end

common --> DTOs
Config --> common
```

## Componentes principales (snapshot fase 1.3)
- `common`: DTOs y utilidades transversales (`normalize_symbol`, `utc_now`, validaciones).
- `ingestion`: (stub) conectores de datos de mercado.
- `features`: (stub) cálculo/serving de features.
- `strategy`: (stub) generación de señales.
- `risk`: (stub) chequeos pre/post trade.
- `execution`: (stub) envío de órdenes/adapter exchanges.
- `portfolio`: (stub) P&L, balances, reconciliación.
- `observability`: (stub) logging/metrics.
- `ops`: (stub) configuración/CLI/runbooks.

## Diagrama de clases (DTOs)
```mermaid
classDiagram
  class TraceContext{
    +trace_id: str
    +span_id: str?
  }
  class MarketEvent{
    +symbol: str
    +event_ts: datetime (UTC)
    +price: float
    +size: float
    +source: trade|kline|book
    +metadata: dict
  }
  class FeatureVector{
    +symbol: str
    +ts: datetime (UTC)
    +values: dict[str,float]
  }
  class Signal{
    +symbol: str
    +ts: datetime (UTC)
    +side: buy|sell|flat
    +size: float
    +confidence: float [0,1]
    +ttl_seconds: int?
    +strategy_id: str
  }
  class OrderIntent{
    +symbol: str
    +ts: datetime (UTC)
    +side: buy|sell
    +quantity: float
    +price_limit: float?
    +time_in_force: GTC|IOC|FOK
    +intent_id: str
    +strategy_id: str
  }
  class ExecutionReport{
    +symbol: str
    +ts: datetime (UTC)
    +status: accepted|partial|filled|rejected|cancelled
    +filled_qty: float
    +avg_price: float
    +client_order_id: str
    +exchange_order_id: str?
    +reason: str?
  }
  class PortfolioState{
    +ts: datetime (UTC)
    +positions: dict[str,float]
    +cash: float
    +unrealized_pnl: float
    +realized_pnl: float
    +open_orders: list[str]
    +total_value(): float
  }
```

## Diagrama de secuencia (happy-path stub)
```mermaid
sequenceDiagram
  participant Dev as Developer
  participant Main as app.main
  participant Config as config loader
  participant Common as common/DTOs

  Dev->>Main: python -m app
  Main->>Config: load_config(env=dev|test)
  Config-->>Main: AppConfig(env,data_dir,log_level)
  Main->>Common: crea TraceContext(trace_id="bootstrap")
  Main-->>Dev: imprime "pipeline stub ok"
```

Alcance actual: validar toolchain y entrada al proceso. Aún sin lógica de dominio ni integraciones externas.
