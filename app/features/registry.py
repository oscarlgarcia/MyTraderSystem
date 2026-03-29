"""
Feature Registry en memoria: describe y versiona configuraciones de features.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, Tuple, Any


@dataclass(frozen=True)
class FeatureSet:
    name: str
    version: str  # semver o etiqueta
    description: str
    windows: tuple[int, ...]
    aggregators: tuple[str, ...]
    transformers: tuple[str, ...]


class FeatureRegistry:
    def __init__(self) -> None:
        self._registry: Dict[Tuple[str, str], FeatureSet] = {}

    def register_feature_set(
        self,
        name: str,
        version: str,
        description: str,
        windows: Any,
        aggregators: Any,
        transformers: Any,
    ) -> FeatureSet:
        key = (name, version)
        if key in self._registry:
            raise ValueError(f"Feature set {name} v{version} ya existe")
        fs = FeatureSet(
            name=name,
            version=version,
            description=description,
            windows=tuple(windows),
            aggregators=tuple(aggregators),
            transformers=tuple(transformers),
        )
        self._registry[key] = fs
        return fs

    def get(self, name: str, version: str) -> FeatureSet | None:
        return self._registry.get((name, version))

    def list_versions(self, name: str) -> Dict[str, FeatureSet]:
        return {ver: fs for (n, ver), fs in self._registry.items() if n == name}

    def build_feature_state(self, name: str, version: str):
        from app.features.store import FeatureState  # local import to avoid circular
        from app.features.cache import FeatureCache

        fs = self.get(name, version)
        if not fs:
            raise KeyError(f"Feature set {name} v{version} no encontrado")
        window = fs.windows[0] if fs.windows else 5
        cache = FeatureCache()
        return FeatureState(
            window=window,
            windows=fs.windows,
            aggregators=fs.aggregators,
            transformers=fs.transformers,
            cache=cache,
        )
