from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Iterable, Tuple

from app.features.definitions import FeatureNodeDefinition


@dataclass(frozen=True)
class BaseNode:
    definition: FeatureNodeDefinition

    @property
    def name(self) -> str:
        return self.definition.name

    @property
    def dependencies(self) -> Tuple[str, ...]:
        return self.definition.dependencies

    def compute(self, *, event, price_history, context: Dict[str, Any], runtime_state) -> Dict[str, float]:
        raise NotImplementedError


class PriceNode(BaseNode):
    def compute(self, *, event, price_history, context: Dict[str, Any], runtime_state) -> Dict[str, float]:
        return {self.definition.outputs[0]: float(event.price)}


class ReturnNode(BaseNode):
    def compute(self, *, event, price_history, context: Dict[str, Any], runtime_state) -> Dict[str, float]:
        prev = runtime_state.previous_price[event.symbol]
        if prev is None or prev <= 0 or event.price <= 0:
            return {}
        import math

        return {self.definition.outputs[0]: math.log(event.price / prev)}


class RollingAggregatorNode(BaseNode):
    def compute(self, *, event, price_history, context: Dict[str, Any], runtime_state) -> Dict[str, float]:
        window = int(self.definition.params["window"])
        agg = str(self.definition.params["aggregator"])
        if len(price_history) < window:
            return {}
        data = list(price_history)[-window:]
        key = self.definition.outputs[0]
        if agg == "sma":
            return {key: sum(data) / window}
        if agg == "ema":
            state_key = ("ema", event.symbol, window)
            prev = runtime_state.agg_state.get(state_key)
            alpha = 2 / (window + 1)
            current = data[-1]
            value = current if prev is None else alpha * current + (1 - alpha) * prev
            runtime_state.agg_state[state_key] = value
            return {key: value}
        if agg == "max":
            return {key: max(data)}
        if agg == "min":
            return {key: min(data)}
        from app.features import store as legacy_store
        custom = legacy_store.AGGREGATORS.get(agg)
        if custom is None:
            raise ValueError(f"unknown rolling aggregator: {agg}")
        value, new_state = custom(event.symbol, data, window, runtime_state.agg_state)
        runtime_state.agg_state.update(new_state)
        return {} if value is None else {key: value}


class ConstantNode(BaseNode):
    def compute(self, *, event, price_history, context: Dict[str, Any], runtime_state) -> Dict[str, float]:
        return {self.definition.outputs[0]: float(self.definition.params["value"])}


NODE_CLASS_BY_KIND = {
    "price": PriceNode,
    "return": ReturnNode,
    "rolling_aggregator": RollingAggregatorNode,
    "constant": ConstantNode,
}


def build_node(definition: FeatureNodeDefinition) -> BaseNode:
    cls = NODE_CLASS_BY_KIND.get(definition.kind)
    if cls is None:
        raise ValueError(f"unsupported node kind: {definition.kind}")
    return cls(definition)
