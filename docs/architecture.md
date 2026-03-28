# Arquitectura (vista simple)

```mermaid
flowchart LR
    A[MarketEvent stream] --> B[compute_features (ventana deslizante)]
    B --> C[FeatureVector (price, ret_1, sma_N)]
    C --> D[Strategy]
    D --> E[Risk]
    E --> F[Execution (paper/live)]
    F --> G[Portfolio]
    classDef trace fill:#e0f7fa,stroke:#26a69a;
    T[Trace optional<br/>--trace-steps<br/>phase=start/done]:::trace
    A --> T
    B --> T
    C --> T
    D --> T
    E --> T
    F --> T
    G --> T
```

- **compute_features** (app/features/store.py): función pura, ventana configurable, sin IO.
- Ventana: deque acotada por símbolo para limitar memoria.
- Los componentes downstream (strategy, risk) consumen `FeatureVector` ya alineados por evento.
