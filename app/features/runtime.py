from __future__ import annotations

import logging
import math
import time
from datetime import datetime
from typing import Dict, Iterable, List, Optional

from app.common.dto import FeatureVector, MarketEvent
from app.features.cache import FeatureCache
from app.features.definitions import FeatureSetDefinition, build_legacy_feature_set_definition
from app.features.metrics import FeatureMetrics
from app.features.planner import FeaturePlanner
from app.features.state import RuntimeStateStore

logger = logging.getLogger("features.runtime")


def _event_ts(event) -> datetime:
    ts = getattr(event, "event_ts", None)
    if ts is None:
        ts = getattr(event, "exchange_ts")
    return ts


def _available_ts(event) -> datetime:
    ts = getattr(event, "available_ts", None)
    if ts is not None:
        return ts
    return _event_ts(event)


class FeatureRuntimeEngine:
    def __init__(
        self,
        *,
        feature_set: FeatureSetDefinition,
        cache: FeatureCache | None = None,
        out_of_order_policy: str = "reject",
    ) -> None:
        self.feature_set = feature_set
        self.cache = cache or FeatureCache()
        self.plan = FeaturePlanner().build_plan(feature_set)
        effective_window = max(feature_set.windows) if feature_set.windows else 5
        self.state = RuntimeStateStore(effective_window=effective_window)
        self.metrics = FeatureMetrics()
        self.out_of_order_policy = out_of_order_policy

    def _compute_from_event(self, event: MarketEvent, *, record_event: bool = True) -> FeatureVector | None:
        if not isinstance(event.price, (int, float)) or not math.isfinite(event.price):
            self.metrics.dropped_non_finite += 1
            return None
        event_ts = _event_ts(event)
        event_available_ts = _available_ts(event)
        prices = self.state.prices[event.symbol]
        prices.append(float(event.price))
        if record_event:
            self.state.recent_events[event.symbol].append(event)
            max_recent = max(self.state.effective_window * 4, 32)
            if len(self.state.recent_events[event.symbol]) > max_recent:
                self.state.recent_events[event.symbol] = self.state.recent_events[event.symbol][-max_recent:]
        values: Dict[str, float] = {}
        context: Dict[str, float] = {}
        for node in self.plan.nodes:
            computed = node.compute(event=event, price_history=prices, context=context, runtime_state=self.state)
            if computed:
                values.update(computed)
                context.update(computed)
        fv = FeatureVector(
            symbol=event.symbol,
            ts=event_ts,
            available_ts=event_available_ts,
            source_cutoff_ts=event_available_ts,
            values=values,
            feature_set_name=self.feature_set.name,
            feature_set_version=self.feature_set.version,
        )
        if event.price > 0:
            self.state.previous_price[event.symbol] = float(event.price)
        self.state.watermarks[event.symbol] = event_available_ts
        self.cache.put(fv)
        return fv

    def _recompute_symbol(self, symbol: str) -> List[FeatureVector]:
        events = sorted(self.state.recent_events[symbol], key=lambda ev: (ev.available_ts, ev.event_ts))
        self.state.prices[symbol].clear()
        self.state.previous_price[symbol] = None
        for key in [key for key in self.state.agg_state if key[1] == symbol]:
            self.state.agg_state.pop(key, None)
        out: List[FeatureVector] = []
        self.state.recent_events[symbol] = list(events)
        for ev in events:
            fv = self._compute_from_event(ev, record_event=False)
            if fv is not None:
                out.append(fv)
        return out

    def update(self, event: MarketEvent) -> FeatureVector | None:
        self.metrics.events_in += 1
        watermark = self.state.watermarks.get(event.symbol)
        event_available_ts = _available_ts(event)
        if watermark is not None and event_available_ts < watermark:
            if self.out_of_order_policy == "reject":
                self.metrics.transform_errors += 1
                return None
            if self.out_of_order_policy == "recompute":
                self.state.recent_events[event.symbol].append(event)
                recomputed = self._recompute_symbol(event.symbol)
                if recomputed:
                    self.metrics.features_out += 1
                    return recomputed[-1]
                return None
        start = time.perf_counter()
        fv = self._compute_from_event(event)
        elapsed = time.perf_counter() - start
        self.metrics.compute_latency_total += elapsed
        self.metrics.compute_latency_max = max(self.metrics.compute_latency_max, elapsed)
        if fv is not None:
            self.metrics.features_out += 1
        return fv

    def update_batch(self, events: Iterable[MarketEvent]) -> List[FeatureVector]:
        out: List[FeatureVector] = []
        for event in events:
            fv = self.update(event)
            if fv is not None:
                out.append(fv)
        return out

    def restore_state(self, state: RuntimeStateStore) -> None:
        self.state = state

    def avg_latency(self) -> float:
        if self.metrics.features_out == 0:
            return 0.0
        return self.metrics.compute_latency_total / self.metrics.features_out


def build_legacy_runtime_feature_set(
    *,
    window: int = 5,
    windows: Iterable[int] | None = None,
    aggregators: Iterable[str] | None = None,
    transformers: Iterable[str] | None = None,
) -> FeatureSetDefinition:
    return build_legacy_feature_set_definition(
        name="legacy",
        version="legacy",
        description="Legacy runtime feature set",
        windows=tuple(windows or [window]),
        aggregators=tuple(aggregators or ["sma", "ema", "max", "min"]),
        transformers=tuple(transformers or []),
    )
