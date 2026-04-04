from __future__ import annotations

from collections import defaultdict, deque
from typing import Dict, Iterable, List, Tuple

from app.features.definitions import FeatureNodeDefinition


class FeatureDagError(ValueError):
    pass


def topological_sort(nodes: Iterable[FeatureNodeDefinition]) -> List[FeatureNodeDefinition]:
    nodes_list = list(nodes)
    by_name = {node.name: node for node in nodes_list}
    indegree: Dict[str, int] = {node.name: 0 for node in nodes_list}
    edges: Dict[str, List[str]] = defaultdict(list)
    for node in nodes_list:
        for dep in node.dependencies:
            if dep not in by_name:
                continue
            edges[dep].append(node.name)
            indegree[node.name] += 1
    queue = deque(sorted(name for name, degree in indegree.items() if degree == 0))
    out: List[FeatureNodeDefinition] = []
    while queue:
        name = queue.popleft()
        out.append(by_name[name])
        for child in sorted(edges.get(name, [])):
            indegree[child] -= 1
            if indegree[child] == 0:
                queue.append(child)
    if len(out) != len(nodes_list):
        raise FeatureDagError("cyclic feature dependency detected")
    return out
