"""Feature package exports both legacy and V2 surfaces."""

from app.features.catalog import FeatureCatalog, FeatureCatalogEntry, FeatureFamily, SourceScope, StrategyFamily, get_default_feature_catalog
from app.features.definitions import AuxiliaryInputDefinition, FeatureDefinition, FeatureNodeDefinition, FeatureSetDefinition
from app.features.definition_registry import DefinitionRegistry
from app.features.engine import FeatureEngine
from app.features.registry import FeatureRegistry

__all__ = [
    "FeatureDefinition",
    "FeatureNodeDefinition",
    "FeatureSetDefinition",
    "AuxiliaryInputDefinition",
    "FeatureCatalog",
    "FeatureCatalogEntry",
    "FeatureFamily",
    "SourceScope",
    "StrategyFamily",
    "get_default_feature_catalog",
    "DefinitionRegistry",
    "FeatureEngine",
    "FeatureRegistry",
]
