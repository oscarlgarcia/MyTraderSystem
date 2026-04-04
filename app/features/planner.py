from __future__ import annotations

from dataclasses import dataclass
from typing import List

from app.features.dag import topological_sort
from app.features.definitions import FeatureSetDefinition, FeatureNodeDefinition
from app.features.nodes import BaseNode, build_node


@dataclass
class FeaturePlan:
    feature_set: FeatureSetDefinition
    ordered_node_definitions: List[FeatureNodeDefinition]
    nodes: List[BaseNode]


class FeaturePlanner:
    def build_plan(self, feature_set: FeatureSetDefinition) -> FeaturePlan:
        ordered_defs = topological_sort(feature_set.node_definitions)
        return FeaturePlan(feature_set=feature_set, ordered_node_definitions=ordered_defs, nodes=[build_node(node) for node in ordered_defs])
