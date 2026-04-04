"""Compatibility registry backed by immutable feature-set definitions."""

from __future__ import annotations

from pathlib import Path
import os
from typing import Any, Dict, Tuple
import warnings

from app.features.definition_registry import DefinitionRegistry
from app.features.definitions import FeatureSetDefinition, build_legacy_feature_set_definition
from app.features.runtime import FeatureRuntimeEngine

FeatureSet = FeatureSetDefinition
LEGACY_ENV_VAR = "APP_ALLOW_LEGACY_FEATURES_V1"


def _warn_legacy_registry(name: str) -> None:
    warnings.warn(f"app.features.registry.{name} is legacy; migrate to DefinitionRegistry/FeatureSetDefinition APIs", DeprecationWarning, stacklevel=2)


def _require_legacy_opt_in(name: str) -> None:
    if os.getenv(LEGACY_ENV_VAR) != "1":
        raise RuntimeError(
            f"app.features.registry.{name} is blocked by default; set {LEGACY_ENV_VAR}=1 only for explicit compatibility work"
        )
    _warn_legacy_registry(name)


class FeatureRegistry:
    def __init__(self, storage_dir: str | Path | None = None) -> None:
        _require_legacy_opt_in("FeatureRegistry")
        self._registry = DefinitionRegistry(storage_dir=storage_dir)

    def register_feature_set(
        self,
        name: str,
        version: str,
        description: str,
        windows: Any,
        aggregators: Any,
        transformers: Any,
        owner: str = "quant-platform",
    ) -> FeatureSet:
        fs = build_legacy_feature_set_definition(
            name=name,
            version=version,
            description=description,
            windows=windows,
            aggregators=aggregators,
            transformers=transformers,
            owner=owner,
        )
        return self._registry.register(fs)

    def register_definition(self, feature_set: FeatureSet, *, persist: bool = True) -> FeatureSet:
        return self._registry.register(feature_set, persist=persist)

    def get(self, name: str, version: str) -> FeatureSet | None:
        return self._registry.get(name, version)

    def list_versions(self, name: str) -> Dict[str, FeatureSet]:
        return self._registry.list_versions(name)

    def build_feature_state(self, name: str, version: str):
        fs = self.get(name, version)
        if not fs:
            raise KeyError(f"Feature set {name} v{version} no encontrado")
        from app.features.store import FeatureState
        from app.features.cache import FeatureCache

        cache = FeatureCache()
        return FeatureState(feature_set=fs, cache=cache)
