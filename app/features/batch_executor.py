from __future__ import annotations

import math
from collections import defaultdict
from datetime import datetime
from typing import Deque, Dict, Iterable, List
from collections import deque

from app.common.dto import FeatureVector, MarketEvent
from app.features.dag import topological_sort
from app.features.definitions import FeatureNodeDefinition, FeatureSetDefinition
from app.features.entity_codec import entity_scope, normalize_entity_keys, primary_symbol


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
    from app.features.legacy_store_v1 import AGGREGATORS  # lazy import for legacy compatibility

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
        node_history: Dict[tuple[str, str], Deque[float]] = defaultdict(lambda: deque(maxlen=512))
        outputs: List[FeatureVector] = []
        for event in ordered_events:
            keys = normalize_entity_keys(
                {
                    key: event.metadata.get(key) or event.metadata.get(f"entity:{key}")
                    for key in feature_set.entity_keys
                    if key != "symbol"
                },
                symbol=event.symbol,
                required_keys=feature_set.entity_keys,
            )
            scope = entity_scope(keys)
            symbol_history = history[scope]
            symbol_history.append(event)
            price_history = [float(item.price) for item in symbol_history]
            context: Dict[str, float] = {}
            values: Dict[str, float] = {}
            for node in ordered_nodes:
                values_for_node = self._compute_node(
                    node,
                    event=event,
                    scope=scope,
                    price_history=price_history,
                    context=context,
                    previous_price=previous_price,
                    agg_state=agg_state,
                    node_history=node_history,
                )
                values.update(values_for_node)
                context.update(values_for_node)
            for name, value in values.items():
                node_history[(scope, name)].append(float(value))
            if event.price > 0:
                previous_price[scope] = float(event.price)
            outputs.append(
                FeatureVector(
                    symbol=primary_symbol(keys, fallback_symbol=event.symbol),
                    ts=_event_ts(event),
                    available_ts=_available_ts(event),
                    source_cutoff_ts=_available_ts(event),
                    values=values,
                    feature_set_name=feature_set.name,
                    feature_set_version=feature_set.version,
                    entity_keys=keys,
                )
            )
        return outputs

    def _compute_node(
        self,
        node: FeatureNodeDefinition,
        *,
        event,
        scope: str,
        price_history: List[float],
        context: Dict[str, float],
        previous_price: Dict[str, float | None],
        agg_state: Dict[tuple[str, str, int], float],
        node_history: Dict[tuple[str, str], Deque[float]],
    ) -> Dict[str, float]:
        kind = node.kind
        if kind == "price":
            return {node.outputs[0]: float(event.price)}
        if kind == "return":
            current_price = float(event.price)
            prev = previous_price[scope]
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
                state_key = (scope, aggregator, window)
                prev = agg_state.get(state_key)
                alpha = 2.0 / (window + 1.0)
                current = window_values[-1]
                value = current if prev is None else alpha * current + (1.0 - alpha) * prev
                agg_state[state_key] = value
                return {key: value}
            return {key: _rolling_agg(window_values, aggregator)}
        if kind == "constant":
            return {node.outputs[0]: float(node.params.get("value", 0.0))}
        if kind == "lag":
            source = str(node.params.get("source") or (node.dependencies[0] if node.dependencies else "price"))
            periods = int(node.params.get("periods", 1))
            history = node_history[(scope, source)]
            if len(history) < periods:
                return {}
            return {node.outputs[0]: float(list(history)[-periods])}
        if kind == "zscore":
            source = str(node.params.get("source") or (node.dependencies[0] if node.dependencies else "price"))
            window = int(node.params.get("window", 5))
            history = node_history[(scope, source)]
            if len(history) < window:
                return {}
            recent = list(history)[-window:]
            mean = sum(recent) / len(recent)
            variance = sum((item - mean) ** 2 for item in recent) / len(recent)
            if variance <= 0 or source not in context:
                return {}
            return {node.outputs[0]: (float(context[source]) - mean) / math.sqrt(variance)}
        if kind == "volatility":
            source = str(node.params.get("source") or (node.dependencies[0] if node.dependencies else "ret_1"))
            window = int(node.params.get("window", 5))
            history = node_history[(scope, source)]
            if len(history) < window:
                return {}
            recent = list(history)[-window:]
            mean = sum(recent) / len(recent)
            variance = sum((item - mean) ** 2 for item in recent) / len(recent)
            return {node.outputs[0]: math.sqrt(max(variance, 0.0))}
        if kind == "metadata_join":
            alias = str(node.params.get("alias", "aux"))
            field_name = str(node.params.get("field", "price"))
            raw = event.metadata.get(f"join:{alias}:{field_name}")
            if raw in (None, ""):
                return {}
            return {node.outputs[0]: float(raw)}
        raise ValueError(f"unsupported batch node kind {kind}")
