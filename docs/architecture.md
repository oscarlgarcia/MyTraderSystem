# Arquitectura (vista simple)

```mermaid
flowchart LR
    A[MarketEvent stream] --> B[compute_features (ventana deslizante)]
    B --> C[FeatureVector]
    C --> D[Strategy]
    D --> E[Risk]
    E --> F[Execution (paper/live)]
    F --> G[Portfolio]
```

- **compute_features** (app/features/store.py): función pura, ventana configurable, sin IO.
- Ventana: deque acotada por símbolo para limitar memoria.
- Los componentes downstream (strategy, risk) consumen `FeatureVector` ya alineados por evento.
