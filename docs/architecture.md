# Arquitectura (vista simple)

```mermaid
flowchart LR
    A[MarketEvent stream] --> B[FeatureState<br/>ventanas deslizantes]
    B --> C[Aggregators/Transformers]
    C --> D[FeatureEngine<br/>API: update/get_*]
    D --> E[FeatureCache<br/>LRU por símbolo]
    E --> F[FeatureVector]
    F --> G[Strategy]
    G --> H[Risk]
    H --> I[Execution (paper/live)]
    I --> J[Portfolio]
    classDef trace fill:#e0f7fa,stroke:#26a69a;
    T[Trace optional<br/>--trace-steps<br/>phase=start/done]:::trace
    P[run_feature_pipeline<br/>--features-after-ingest]:::trace
    A --> T
    B --> T
    C --> T
    D --> T
    E --> T
    F --> T
    G --> T
    H --> T
    I --> T
    J --> T
    A --> P
```

- **FeatureState** (app/features/store.py): ventanas por símbolo, log-return seguro, agregadores registrados (sma/ema/max/min) y transformers opcionales; actualiza estado evento a evento.
- **FeatureEngine** (app/features/engine.py): fachada de consumo (`update`, `update_batch`, `get_latest`, `get_at`, `get_batch`); compone FeatureState + FeatureCache y acepta `FeatureSet` registrado.
- **FeatureCache**: LRU in-memory por símbolo con índice temporal para lookups rápidos.
- Los componentes downstream (strategy, risk) consumen `FeatureVector` ya alineados por evento.
