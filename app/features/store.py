"""Legacy-compatible feature calculations backed by the V2 runtime."""

from __future__ import annotations

import logging
import warnings
from datetime import datetime
from typing import Callable, Dict, Iterable, List, Optional, Sequence, Tuple

from app.common.dto import FeatureVector, MarketEvent
from app.features.cache import FeatureCache
from app.features.definitions import FeatureSetDefinition, build_legacy_feature_set_definition
from app.features.runtime import FeatureRuntimeEngine, build_legacy_runtime_feature_set

logger = logging.getLogger("features.store")
REQUIRED_KEYS = {"price"}
AggregatorFn = Callable[[str, Sequence[float], int, Dict[Tuple[str, str, int], float]], Tuple[float | None, Dict[Tuple[str, str, int], float]]]
AGGREGATORS: Dict[str, AggregatorFn] = {}
TransformerFn = Callable[[FeatureVector], FeatureVector]
TRANSFORMERS: Dict[str, TransformerFn] = {}


def _warn_legacy_api(name: str) -> None:
    warnings.warn(f"app.features.store.{name} is legacy; migrate to V2 runtime/materialization/store APIs", DeprecationWarning, stacklevel=2)


def _is_finite_price(price: float) -> bool:
    import math

    return isinstance(price, (int, float)) and math.isfinite(price)


def register_aggregator(name: str, fn: AggregatorFn) -> None:
    _warn_legacy_api("register_aggregator")
    AGGREGATORS[name] = fn


class FeatureState:
    """Compatibility wrapper over FeatureRuntimeEngine."""

    def __init__(
        self,
        window: int = 5,
        windows: Iterable[int] | None = None,
        aggregators: Iterable[str] | None = None,
        transformers: Iterable[str] | None = None,
        cache: Optional[FeatureCache] = None,
        feature_set: FeatureSetDefinition | None = None,
        out_of_order_policy: str = "reject",
    ) -> None:
        _warn_legacy_api("FeatureState")
        self.cache = cache or FeatureCache()
        self.feature_set = feature_set or build_legacy_runtime_feature_set(
            window=window,
            windows=windows,
            aggregators=aggregators,
            transformers=transformers,
        )
        self.runtime = FeatureRuntimeEngine(feature_set=self.feature_set, cache=self.cache, out_of_order_policy=out_of_order_policy)
        self.dropped_invalid = 0
        self.aggregators = list(self.feature_set.aggregators)
        self.transformers = list(self.feature_set.transformers)
        self.window_set = list(self.feature_set.windows)
        self.effective_window = max(self.feature_set.windows) if self.feature_set.windows else window
        self.prices = self.runtime.state.prices
        self.prev_valid_price = self.runtime.state.previous_price
        self.agg_state = self.runtime.state.agg_state

    def reset(self) -> None:
        self.runtime.state.reset()
        self.cache.data.clear()
        self.dropped_invalid = 0

    def update(self, ev: MarketEvent) -> FeatureVector | None:
        fv = self.runtime.update(ev)
        if fv is None:
            self.dropped_invalid += 1
            return None
        missing = REQUIRED_KEYS - set(fv.values.keys())
        if missing:
            self.dropped_invalid += 1
            return None
        for t_name in self.transformers:
            t_fn = TRANSFORMERS.get(t_name)
            if not t_fn:
                raise ValueError(f"Transformer '{t_name}' not registered")
            fv = t_fn(fv)
        if self.cache:
            self.cache.put(fv)
        return fv


def compute_features(
    events: Sequence[MarketEvent],
    window: int = 5,
    windows: Iterable[int] | None = None,
    aggregators: Iterable[str] | None = None,
    feature_set: Optional[FeatureSetDefinition] = None,
    cache: Optional[FeatureCache] = None,
) -> List[FeatureVector]:
    _warn_legacy_api("compute_features")
    if not events:
        return []
    if feature_set is None:
        feature_set = build_legacy_runtime_feature_set(window=window, windows=windows, aggregators=aggregators)
    state = FeatureState(feature_set=feature_set, cache=cache)
    results: List[FeatureVector] = []
    by_symbol: Dict[str, List[MarketEvent]] = {}
    for ev in events:
        by_symbol.setdefault(ev.symbol, []).append(ev)
    for sym in sorted(by_symbol.keys()):
        for ev in sorted(by_symbol[sym], key=lambda e: (e.available_ts, e.event_ts)):
            fv = state.update(ev)
            if fv:
                results.append(fv)
    if state.dropped_invalid:
        logger.info("features discarded", extra={"dropped": state.dropped_invalid})
    return results


def _agg_sma(symbol: str, prices: Sequence[float], window: int, state: Dict[Tuple[str, str, int], float]):
    data = prices if isinstance(prices, list) else list(prices)
    if len(data) < window:
        return None, state
    return sum(data[-window:]) / window, state


def _agg_ema(symbol: str, prices: Sequence[float], window: int, state: Dict[Tuple[str, str, int], float]):
    data = prices if isinstance(prices, list) else list(prices)
    if not data:
        return None, state
    key = ("ema", symbol, window)
    prev = state.get(key)
    alpha = 2 / (window + 1)
    current = data[-1]
    ema_val = current if prev is None else alpha * current + (1 - alpha) * prev
    state = dict(state)
    state[key] = ema_val
    return ema_val, state


def _agg_max(symbol: str, prices: Sequence[float], window: int, state: Dict[Tuple[str, str, int], float]):
    data = prices if isinstance(prices, list) else list(prices)
    if len(data) < window:
        return None, state
    return max(data[-window:]), state


def _agg_min(symbol: str, prices: Sequence[float], window: int, state: Dict[Tuple[str, str, int], float]):
    data = prices if isinstance(prices, list) else list(prices)
    if len(data) < window:
        return None, state
    return min(data[-window:]), state


def _t_clip_non_finite(fv: FeatureVector) -> FeatureVector:
    clean = {k: v for k, v in fv.values.items() if _is_finite_price(v)}
    return FeatureVector(
        symbol=fv.symbol,
        ts=fv.ts,
        available_ts=fv.available_ts,
        source_cutoff_ts=fv.source_cutoff_ts,
        values=clean,
        feature_set_name=fv.feature_set_name,
        feature_set_version=fv.feature_set_version,
        lineage_id=fv.lineage_id,
        quality_flags=fv.quality_flags,
        entity_keys=fv.entity_keys,
    )


def _t_scale_price(fv: FeatureVector, factor: float = 1.0) -> FeatureVector:
    new_vals = dict(fv.values)
    if "price" in new_vals:
        new_vals["price"] = new_vals["price"] * factor
    return FeatureVector(
        symbol=fv.symbol,
        ts=fv.ts,
        available_ts=fv.available_ts,
        source_cutoff_ts=fv.source_cutoff_ts,
        values=new_vals,
        feature_set_name=fv.feature_set_name,
        feature_set_version=fv.feature_set_version,
        lineage_id=fv.lineage_id,
        quality_flags=fv.quality_flags,
        entity_keys=fv.entity_keys,
    )


def _t_drop_keys(keys: Iterable[str]) -> TransformerFn:
    def _inner(fv: FeatureVector) -> FeatureVector:
        new_vals = {k: v for k, v in fv.values.items() if k not in keys}
        return FeatureVector(
            symbol=fv.symbol,
            ts=fv.ts,
            available_ts=fv.available_ts,
            source_cutoff_ts=fv.source_cutoff_ts,
            values=new_vals,
            feature_set_name=fv.feature_set_name,
            feature_set_version=fv.feature_set_version,
            lineage_id=fv.lineage_id,
            quality_flags=fv.quality_flags,
            entity_keys=fv.entity_keys,
        )

    return _inner


register_aggregator("sma", _agg_sma)
register_aggregator("ema", _agg_ema)
register_aggregator("max", _agg_max)
register_aggregator("min", _agg_min)

TRANSFORMERS["clip_non_finite"] = _t_clip_non_finite
TRANSFORMERS["scale_price_2x"] = lambda fv: _t_scale_price(fv, factor=2.0)
TRANSFORMERS["drop_window_max"] = _t_drop_keys(["window_max"])
