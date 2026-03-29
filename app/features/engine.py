"""
FeatureEngine: fachada de acceso a FeatureState + Cache.
No thread-safe por diseño (single-threaded ingest).
"""

from __future__ import annotations

from typing import Iterable, List, Optional

from app.common.dto import FeatureVector, MarketEvent
from app.features.store import FeatureState, compute_features
from app.features.cache import FeatureCache
from app.features.registry import FeatureSet


class FeatureEngine:
    def __init__(
        self,
        window: int = 5,
        windows: Iterable[int] | None = None,
        aggregators: Iterable[str] | None = None,
        transformers: Iterable[str] | None = None,
        feature_set: Optional[FeatureSet] = None,
        cache_capacity: int = 1000,
    ) -> None:
        self.cache = FeatureCache(capacity_per_symbol=cache_capacity)
        if feature_set:
            window = feature_set.windows[0] if feature_set.windows else window
            windows = feature_set.windows
            aggregators = feature_set.aggregators
            transformers = feature_set.transformers
        self.state = FeatureState(
            window=window, windows=windows, aggregators=aggregators, transformers=transformers, cache=self.cache
        )

    def update(self, event: MarketEvent) -> FeatureVector | None:
        return self.state.update(event)

    def update_batch(self, events: Iterable[MarketEvent]) -> List[FeatureVector]:
        out: List[FeatureVector] = []
        for ev in events:
            fv = self.update(ev)
            if fv:
                out.append(fv)
        return out

    def get_latest(self, symbol: str) -> FeatureVector | None:
        return self.cache.get_latest(symbol)

    def get_at(self, symbol: str, ts, tolerance: float | None = None) -> FeatureVector | None:
        return self.cache.get_at(symbol, ts, tolerance)

    def get_batch(self, symbol: str) -> List[FeatureVector]:
        od = self.cache.data.get(symbol)
        if not od:
            return []
        return list(od.values())
