"""FeatureEngine facade over FeatureRuntimeEngine + cache/serving helpers."""

from __future__ import annotations

import logging
import math
import time
from typing import Iterable, List, Optional

from app.common.dto import FeatureVector, MarketEvent
from app.features.cache import FeatureCache
from app.features.definitions import FeatureSetDefinition
from app.features.runtime import FeatureRuntimeEngine, build_legacy_runtime_feature_set


class FeatureEngine:
    def __init__(
        self,
        window: int = 5,
        windows: Iterable[int] | None = None,
        aggregators: Iterable[str] | None = None,
        transformers: Iterable[str] | None = None,
        feature_set: Optional[FeatureSetDefinition] = None,
        cache_capacity: int = 1000,
        out_of_order_policy: str = "reject",
    ) -> None:
        self.cache = FeatureCache(capacity_per_symbol=cache_capacity)
        feature_set = feature_set or build_legacy_runtime_feature_set(
            window=window,
            windows=windows,
            aggregators=aggregators,
            transformers=transformers,
        )
        self.runtime = FeatureRuntimeEngine(feature_set=feature_set, cache=self.cache, out_of_order_policy=out_of_order_policy)
        self.metrics = {
            "events_in": 0,
            "features_out": 0,
            "dropped_non_finite": 0,
            "transform_errors": 0,
            "compute_latency_total": 0.0,
            "compute_latency_max": 0.0,
        }

    def update(self, event: MarketEvent) -> FeatureVector | None:
        self.metrics["events_in"] += 1
        if not math.isfinite(event.price):
            self.metrics["dropped_non_finite"] += 1
            return None
        start = time.perf_counter()
        try:
            fv = self.runtime.update(event)
        except Exception as exc:
            self.metrics["transform_errors"] += 1
            logging.getLogger("features.engine").warning("feature update failed", exc_info=exc)
            return None
        elapsed = time.perf_counter() - start
        self.metrics["compute_latency_total"] += elapsed
        self.metrics["compute_latency_max"] = max(self.metrics["compute_latency_max"], elapsed)
        if fv:
            self.metrics["features_out"] += 1
        return fv

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

    def avg_latency(self) -> float:
        if self.metrics["features_out"] == 0:
            return 0.0
        return self.metrics["compute_latency_total"] / self.metrics["features_out"]

    def log_metrics(self, logger: Optional[logging.Logger] = None) -> None:
        logger = logger or logging.getLogger("features.engine")
        logger.info("feature engine metrics", extra={**self.metrics, "latency_avg": self.avg_latency()})
