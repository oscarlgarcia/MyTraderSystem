"""
Feature Store incremental: ventanas por símbolo y actualización evento a evento.
"""

from __future__ import annotations

import math
import logging
from collections import defaultdict, deque
from datetime import datetime
from typing import Callable, Deque, Dict, Iterable, List, Sequence, Tuple

from app.common.dto import FeatureVector, MarketEvent

logger = logging.getLogger("features.store")
REQUIRED_KEYS = {"price"}
AggregatorFn = Callable[[str, Sequence[float], int, Dict[Tuple[str, str, int], float]], Tuple[float | None, Dict[Tuple[str, str, int], float]]]
AGGREGATORS: Dict[str, AggregatorFn] = {}


def _is_finite_price(price: float) -> bool:
    return isinstance(price, (int, float)) and math.isfinite(price)


def _sma(prices: Sequence[float], window: int) -> float | None:
    if len(prices) < window:
        return None
    data = prices if isinstance(prices, list) else list(prices)
    return sum(data[-window:]) / window


def _log_return(prev_price: float | None, current_price: float) -> float | None:
    if prev_price is None:
        return None
    if prev_price <= 0 or current_price <= 0:
        return None
    return math.log(current_price / prev_price)


def register_aggregator(name: str, fn: AggregatorFn) -> None:
    AGGREGATORS[name] = fn


class FeatureState:
    """Mantiene ventanas por símbolo y calcula features incrementales."""

    def __init__(self, window: int = 5, windows: Iterable[int] | None = None, aggregators: Iterable[str] | None = None) -> None:
        window_set = set(windows) if windows is not None else set()
        window_set.add(window)
        self.window_set = sorted(window_set)
        self.effective_window = max(self.window_set)
        self.prices: Dict[str, Deque[float]] = defaultdict(lambda: deque(maxlen=self.effective_window))
        self.prev_valid_price: Dict[str, float | None] = defaultdict(lambda: None)
        self.dropped_invalid = 0
        self.aggregators = list(aggregators) if aggregators is not None else ["sma", "ema", "max", "min"]
        # estado interno de agregadores (ej. EMA)
        self.agg_state: Dict[Tuple[str, str, int], float] = {}

    def reset(self) -> None:
        self.prices.clear()
        self.prev_valid_price.clear()
        self.dropped_invalid = 0
        self.agg_state.clear()

    def update(self, ev: MarketEvent) -> FeatureVector | None:
        if not _is_finite_price(ev.price):
            self.dropped_invalid += 1
            return None

        prices = self.prices[ev.symbol]
        prices.append(ev.price)

        values: Dict[str, float] = {"price": ev.price}

        prev = self.prev_valid_price[ev.symbol]
        ret = _log_return(prev, ev.price)
        if ret is not None:
            values["ret_1"] = ret

        for w in self.window_set:
            for agg_name in self.aggregators:
                agg_fn = AGGREGATORS.get(agg_name)
                if not agg_fn:
                    continue
                val, new_state = agg_fn(ev.symbol, prices, w, self.agg_state)
                # merge state
                self.agg_state.update(new_state)
                if val is not None:
                    values[f"{agg_name}_{w}"] = val

        missing = REQUIRED_KEYS - set(values.keys())
        if missing or not all(_is_finite_price(v) for v in values.values()):
            self.dropped_invalid += 1
            return None

        values["window_max"] = float(self.effective_window)
        fv = FeatureVector(
            symbol=ev.symbol,
            ts=ev.event_ts if isinstance(ev.event_ts, datetime) else datetime.fromisoformat(str(ev.event_ts)),
            values=values,
        )

        if ev.price > 0:
            self.prev_valid_price[ev.symbol] = ev.price
        return fv


def compute_features(
    events: Sequence[MarketEvent],
    window: int = 5,
    windows: Iterable[int] | None = None,
    aggregators: Iterable[str] | None = None,
) -> List[FeatureVector]:
    """
    Calcula features incrementales sobre una lista de eventos.
    Retorna un FeatureVector por evento válido en orden de llegada.
    """
    if not events:
        return []

    state = FeatureState(window=window, windows=windows, aggregators=aggregators)
    results: List[FeatureVector] = []
    # agrupar por símbolo y ordenar dentro para mantener determinismo
    by_symbol: Dict[str, List[MarketEvent]] = defaultdict(list)
    for ev in events:
        by_symbol[ev.symbol].append(ev)
    for sym in sorted(by_symbol.keys()):
        for ev in sorted(by_symbol[sym], key=lambda e: e.event_ts):
            fv = state.update(ev)
            if fv:
                results.append(fv)

    if state.dropped_invalid:
        logger.info("features discarded", extra={"dropped": state.dropped_invalid})
    return results
def _agg_sma(symbol: str, prices: Sequence[float], window: int, state: Dict[Tuple[str, str, int], float]):
    val = _sma(prices, window)
    return val, state


def _agg_ema(symbol: str, prices: Sequence[float], window: int, state: Dict[Tuple[str, str, int], float]):
    key = ("ema", symbol, window)
    prev = state.get(key)
    if not prices:
        return None, state
    alpha = 2 / (window + 1)
    current = prices[-1]
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


# registro por defecto
register_aggregator("sma", _agg_sma)
register_aggregator("ema", _agg_ema)
register_aggregator("max", _agg_max)
register_aggregator("min", _agg_min)
