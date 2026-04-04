import pytest

from app.features.dag import FeatureDagError, topological_sort
from app.features.definitions import FeatureNodeDefinition


def test_topological_sort_orders_dependencies():
    nodes = [
        FeatureNodeDefinition(name="b", kind="constant", outputs=("b",), dependencies=("a",)),
        FeatureNodeDefinition(name="a", kind="constant", outputs=("a",)),
    ]
    ordered = topological_sort(nodes)
    assert [node.name for node in ordered] == ["a", "b"]


def test_topological_sort_rejects_cycles():
    nodes = [
        FeatureNodeDefinition(name="a", kind="constant", outputs=("a",), dependencies=("b",)),
        FeatureNodeDefinition(name="b", kind="constant", outputs=("b",), dependencies=("a",)),
    ]
    with pytest.raises(FeatureDagError):
        topological_sort(nodes)
