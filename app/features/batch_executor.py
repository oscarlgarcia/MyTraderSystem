from __future__ import annotations

import math
from collections import defaultdict
from datetime import datetime
from typing import Dict, Iterable, List

from app.common.dto import FeatureVector, MarketEvent
from app.features.dag import topological_sort
from app.features.definitions import FeatureNodeDefinition, FeatureSetDefinition


def _event_ts(event) -> datetime:
    ts = getattr(event, "event_ts", None)
    if ts is None:
        ts = getattr(event, "exchange_ts")
    return ts


def _available_ts(event) -> datetime:
    ts = getattr(event, "available_ts", None)
    if ts is None:
        ts = _event_ts(event)
    return ts


def _rolling_agg(values: List[float], aggregator: str) -> float:
    if not values:
        return 0.0
    name = aggregator.lower()
    if name == "sma":
        return sum(values) / len(values)
    if name == "max":
        return max(values)
    if name == "min":
        return min(values)
    from app.features.store import AGGREGATORS  # lazy import for legacy compatibility

    fn = AGGREGATORS.get(name)
    if fn is None:
        raise ValueError(f"unsupported batch aggregator {aggregator}")
    return float(fn(values))


class BatchFeatureExecutor:
    def execute(self, events: Iterable[MarketEvent], *, feature_set: FeatureSetDefinition) -> List[FeatureVector]:
        ordered_nodes = topological_sort(feature_set.node_definitions)
        ordered_events = sorted(list(events), key=lambda event: (event.symbol, _available_ts(event), _event_ts(event)))
        history: Dict[str, List[MarketEvent]] = defaultdict(list)
        previous_price: Dict[str, float | None] = defaultdict(lambda: None)
        agg_state: Dict[tuple[str, str, int], float] = {}
        outputs: List[FeatureVector] = []
        for event in ordered_events:
            symbol_history = history[event.symbol]
            symbol_history.append(event)
            price_history = [float(item.price) for item in symbol_history]
            context: Dict[str, float] = {}
            values: Dict[str, float] = {}
            for node in ordered_nodes:
                values_for_node = self._compute_node(
                    node,
                    event=event,
                    price_history=price_history,
                    context=context,
                    previous_price=previous_price,
                    agg_state=agg_state,
                )
                values.update(values_for_node)
                context.update(values_for_node)
            if event.price > 0:
                previous_price[event.symbol] = float(event.price)
            outputs.append(
                FeatureVector(
                    symbol=event.symbol,
                    ts=_event_ts(event),
                    available_ts=_available_ts(event),
                    source_cutoff_ts=_available_ts(event),
                    values=values,
                    feature_set_name=feature_set.name,
                    feature_set_version=feature_set.version,
                )
            )
        return outputs

    def _compute_node(
        self,
        node: FeatureNodeDefinition,
        *,
        event,
        price_history: List[float],
        context: Dict[str, float],
        previous_price: Dict[str, float | None],
        agg_state: Dict[tuple[str, str, int], float],
    ) -> Dict[str, float]:
        kind = node.kind
        if kind == "price":
            return {node.outputs[0]: float(event.price)}
        if kind == "return":
            current_price = float(event.price)
            prev = previous_price[event.symbol]
            if prev is None or prev <= 0 or current_price <= 0:
                return {}
            return {node.outputs[0]: math.log(current_price / prev)}
        if kind == "rolling_aggregator":
            window = int(node.params.get("window", 1))
            aggregator = str(node.params.get("aggregator", "sma"))
            if len(price_history) < window:
                return {}
            window_values = price_history[-window:] if window > 0 else price_history
            key = node.outputs[0]
            if aggregator == "ema":
                state_key = (event.symbol, aggregator, window)
                prev = agg_state.get(state_key)
                alpha = 2.0 / (window + 1.0)
                current = window_values[-1]
                value = current if prev is None else alpha * current + (1.0 - alpha) * prev
                agg_state[state_key] = value
                return {key: value}
            return {key: _rolling_agg(window_values, aggregator)}
        if kind == "constant":
            return {node.outputs[0]: float(node.params.get("value", 0.0))}
        raise ValueError(f"unsupported batch node kind {kind}")
