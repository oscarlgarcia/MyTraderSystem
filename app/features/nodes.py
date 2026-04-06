from __future__ import annotations

from dataclasses import dataclass
import math
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

    def _scope(self, *, event, runtime_state) -> str:
        return runtime_state.scope_for_event(event)


class PriceNode(BaseNode):
    def compute(self, *, event, price_history, context: Dict[str, Any], runtime_state) -> Dict[str, float]:
        return {self.definition.outputs[0]: float(event.price)}


class ReturnNode(BaseNode):
    def compute(self, *, event, price_history, context: Dict[str, Any], runtime_state) -> Dict[str, float]:
        prev = runtime_state.previous_price[self._scope(event=event, runtime_state=runtime_state)]
        if prev is None or prev <= 0 or event.price <= 0:
            return {}
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
            state_key = ("ema", self._scope(event=event, runtime_state=runtime_state), window)
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
        value, new_state = custom(self._scope(event=event, runtime_state=runtime_state), data, window, runtime_state.agg_state)
        runtime_state.agg_state.update(new_state)
        return {} if value is None else {key: value}


class ConstantNode(BaseNode):
    def compute(self, *, event, price_history, context: Dict[str, Any], runtime_state) -> Dict[str, float]:
        return {self.definition.outputs[0]: float(self.definition.params["value"])}


class LagNode(BaseNode):
    def compute(self, *, event, price_history, context: Dict[str, Any], runtime_state) -> Dict[str, float]:
        source = str(self.definition.params.get("source") or (self.definition.dependencies[0] if self.definition.dependencies else "price"))
        periods = int(self.definition.params.get("periods", 1))
        if periods < 1:
            raise ValueError("lag periods must be >= 1")
        history = runtime_state.node_history[(self._scope(event=event, runtime_state=runtime_state), source)]
        if len(history) < periods:
            return {}
        return {self.definition.outputs[0]: float(list(history)[-periods])}


class ZScoreNode(BaseNode):
    def compute(self, *, event, price_history, context: Dict[str, Any], runtime_state) -> Dict[str, float]:
        source = str(self.definition.params.get("source") or (self.definition.dependencies[0] if self.definition.dependencies else "price"))
        window = int(self.definition.params.get("window", 5))
        if window < 2:
            return {}
        history = runtime_state.node_history[(self._scope(event=event, runtime_state=runtime_state), source)]
        if len(history) < window:
            return {}
        recent = list(history)[-window:]
        mean = sum(recent) / len(recent)
        variance = sum((item - mean) ** 2 for item in recent) / len(recent)
        if variance <= 0:
            return {}
        current = context.get(source)
        if current is None:
            return {}
        return {self.definition.outputs[0]: (float(current) - mean) / math.sqrt(variance)}


class VolatilityNode(BaseNode):
    def compute(self, *, event, price_history, context: Dict[str, Any], runtime_state) -> Dict[str, float]:
        source = str(self.definition.params.get("source") or (self.definition.dependencies[0] if self.definition.dependencies else "ret_1"))
        window = int(self.definition.params.get("window", 5))
        if window < 2:
            return {}
        history = runtime_state.node_history[(self._scope(event=event, runtime_state=runtime_state), source)]
        if len(history) < window:
            return {}
        recent = list(history)[-window:]
        mean = sum(recent) / len(recent)
        variance = sum((item - mean) ** 2 for item in recent) / len(recent)
        return {self.definition.outputs[0]: math.sqrt(max(variance, 0.0))}


class MetadataJoinNode(BaseNode):
    def compute(self, *, event, price_history, context: Dict[str, Any], runtime_state) -> Dict[str, float]:
        alias = str(self.definition.params.get("alias", "aux"))
        field_name = str(self.definition.params.get("field", "price"))
        raw = event.metadata.get(f"join:{alias}:{field_name}")
        if raw in (None, ""):
            return {}
        return {self.definition.outputs[0]: float(raw)}


NODE_CLASS_BY_KIND = {
    "price": PriceNode,
    "return": ReturnNode,
    "rolling_aggregator": RollingAggregatorNode,
    "constant": ConstantNode,
    "lag": LagNode,
    "zscore": ZScoreNode,
    "volatility": VolatilityNode,
    "metadata_join": MetadataJoinNode,
}


def build_node(definition: FeatureNodeDefinition) -> BaseNode:
    cls = NODE_CLASS_BY_KIND.get(definition.kind)
    if cls is None:
        raise ValueError(f"unsupported node kind: {definition.kind}")
    return cls(definition)
