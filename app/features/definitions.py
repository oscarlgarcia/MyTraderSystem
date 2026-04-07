from __future__ import annotations

from dataclasses import asdict, dataclass, field
import hashlib
import json
from typing import Any, Dict, Iterable, Tuple


@dataclass(frozen=True)
class AuxiliaryInputDefinition:
    alias: str
    description: str
    entity_keys: Tuple[str, ...] = ("symbol",)
    availability_field: str = "available_ts"
    event_ts_field: str = "event_ts"
    required: bool = False


@dataclass(frozen=True)
class FeatureDefinition:
    name: str
    version: str
    description: str
    owner: str
    entity_keys: Tuple[str, ...] = ("symbol",)
    inputs: Tuple[str, ...] = ()
    frequency: str = "event"
    lookback: int = 0
    warmup: int = 0
    materialization_mode: str = "offline_online"
    availability_semantics: str = "available_ts<=decision_ts"
    dtype: str = "float"
    tags: Tuple[str, ...] = ()
    validation_policy: Dict[str, Any] = field(default_factory=dict)
    metadata: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not tuple(self.entity_keys):
            raise ValueError("entity_keys must not be empty")

    @property
    def definition_hash(self) -> str:
        payload = json.dumps(asdict(self), sort_keys=True, default=str, separators=(",", ":"))
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class FeatureNodeDefinition:
    name: str
    kind: str
    outputs: Tuple[str, ...]
    dependencies: Tuple[str, ...] = ()
    params: Dict[str, Any] = field(default_factory=dict)
    owner: str = "platform"
    description: str = ""

    @property
    def definition_hash(self) -> str:
        payload = json.dumps(asdict(self), sort_keys=True, default=str, separators=(",", ":"))
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()


@dataclass(frozen=True)
class FeatureSetDefinition:
    name: str
    version: str
    description: str
    owner: str = "platform"
    windows: Tuple[int, ...] = ()
    aggregators: Tuple[str, ...] = ()
    transformers: Tuple[str, ...] = ()
    entity_keys: Tuple[str, ...] = ("symbol",)
    frequency: str = "event"
    materialization_mode: str = "offline_online"
    availability_semantics: str = "available_ts<=decision_ts"
    feature_definitions: Tuple[FeatureDefinition, ...] = ()
    node_definitions: Tuple[FeatureNodeDefinition, ...] = ()
    auxiliary_inputs: Tuple[AuxiliaryInputDefinition, ...] = ()
    tags: Tuple[str, ...] = ()
    metadata: Dict[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not tuple(self.entity_keys):
            raise ValueError("entity_keys must not be empty")
        for auxiliary_input in self.auxiliary_inputs:
            if not tuple(auxiliary_input.entity_keys):
                raise ValueError("auxiliary input entity_keys must not be empty")

    @property
    def definition_hash(self) -> str:
        payload = {
            "name": self.name,
            "version": self.version,
            "description": self.description,
            "owner": self.owner,
            "windows": self.windows,
            "aggregators": self.aggregators,
            "transformers": self.transformers,
            "entity_keys": self.entity_keys,
            "frequency": self.frequency,
            "materialization_mode": self.materialization_mode,
            "availability_semantics": self.availability_semantics,
            "feature_definitions": [asdict(fd) for fd in self.feature_definitions],
            "node_definitions": [asdict(nd) for nd in self.node_definitions],
            "auxiliary_inputs": [asdict(aux) for aux in self.auxiliary_inputs],
            "tags": self.tags,
            "metadata": self.metadata,
        }
        raw = json.dumps(payload, sort_keys=True, default=str, separators=(",", ":"))
        return hashlib.sha256(raw.encode("utf-8")).hexdigest()

    def feature_names(self) -> Tuple[str, ...]:
        if self.feature_definitions:
            return tuple(fd.name for fd in self.feature_definitions)
        return tuple()


DEFAULT_OWNER = "quant-platform"


def build_legacy_feature_set_definition(
    *,
    name: str,
    version: str,
    description: str,
    windows: Iterable[int],
    aggregators: Iterable[str],
    transformers: Iterable[str],
    owner: str = DEFAULT_OWNER,
) -> FeatureSetDefinition:
    win_tuple = tuple(sorted(set(int(w) for w in windows)))
    agg_tuple = tuple(aggregators)
    transformer_tuple = tuple(transformers)
    feature_defs = [
        FeatureDefinition(
            name="price",
            version=version,
            description="Observed price",
            owner=owner,
            inputs=("price",),
            lookback=0,
            warmup=0,
            validation_policy={"parity_tolerance": 0.0},
        ),
        FeatureDefinition(
            name="ret_1",
            version=version,
            description="One-step log return",
            owner=owner,
            inputs=("price",),
            lookback=1,
            warmup=1,
            validation_policy={"parity_tolerance": 1e-12},
        ),
    ]
    node_defs = [
        FeatureNodeDefinition(name="price", kind="price", outputs=("price",)),
        FeatureNodeDefinition(name="ret_1", kind="return", outputs=("ret_1",), dependencies=("price",), params={"steps": 1}),
    ]
    for window in win_tuple:
        for agg in agg_tuple:
            fname = f"{agg}_{window}"
            feature_defs.append(
                FeatureDefinition(
                    name=fname,
                    version=version,
                    description=f"{agg} over rolling window {window}",
                    owner=owner,
                    inputs=("price",),
                    lookback=window,
                    warmup=window,
                    validation_policy={"parity_tolerance": 1e-9},
                )
            )
            node_defs.append(
                FeatureNodeDefinition(
                    name=fname,
                    kind="rolling_aggregator",
                    outputs=(fname,),
                    dependencies=("price",),
                    params={"window": window, "aggregator": agg},
                )
            )
    feature_defs.append(
        FeatureDefinition(
            name="window_max",
            version=version,
            description="Effective rolling window used by runtime",
            owner=owner,
            inputs=(),
            lookback=max(win_tuple) if win_tuple else 0,
            warmup=0,
            dtype="int",
            validation_policy={"parity_tolerance": 0.0},
        )
    )
    node_defs.append(
        FeatureNodeDefinition(name="window_max", kind="constant", outputs=("window_max",), params={"value": max(win_tuple) if win_tuple else 0})
    )
    return FeatureSetDefinition(
        name=name,
        version=version,
        description=description,
        owner=owner,
        windows=win_tuple,
        aggregators=agg_tuple,
        transformers=transformer_tuple,
        feature_definitions=tuple(feature_defs),
        node_definitions=tuple(node_defs),
    )
