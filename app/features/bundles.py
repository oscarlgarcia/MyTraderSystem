from __future__ import annotations

from app.features.definitions import FeatureSetDefinition, build_legacy_feature_set_definition


DEFAULT_RUNTIME_WINDOWS = (3, 5, 10, 20, 50)
DEFAULT_RUNTIME_AGGREGATORS = ("sma", "ema", "max", "min")


def build_default_runtime_feature_set() -> FeatureSetDefinition:
    return build_legacy_feature_set_definition(
        name="legacy",
        version="legacy",
        description="Expanded default runtime feature set",
        windows=DEFAULT_RUNTIME_WINDOWS,
        aggregators=DEFAULT_RUNTIME_AGGREGATORS,
        transformers=(),
    )
