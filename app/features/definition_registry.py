from __future__ import annotations

from dataclasses import asdict
import json
from pathlib import Path
from typing import Dict, Iterable, Optional, Tuple

from app.features.definitions import FeatureDefinition, FeatureNodeDefinition, FeatureSetDefinition


class DefinitionRegistryError(ValueError):
    pass


class DefinitionRegistry:
    def __init__(self, storage_dir: str | Path | None = None) -> None:
        self._registry: Dict[Tuple[str, str], FeatureSetDefinition] = {}
        self.storage_dir = Path(storage_dir) if storage_dir is not None else None
        if self.storage_dir is not None:
            self.storage_dir.mkdir(parents=True, exist_ok=True)
            self._load_from_disk()

    def _path_for(self, name: str, version: str) -> Path:
        if self.storage_dir is None:
            raise DefinitionRegistryError("registry storage_dir is not configured")
        return self.storage_dir / f"{name}__{version}.json"

    def _deserialize_feature_set(self, payload: dict) -> FeatureSetDefinition:
        feature_defs = tuple(FeatureDefinition(**item) for item in payload.get("feature_definitions", []))
        node_defs = tuple(FeatureNodeDefinition(**item) for item in payload.get("node_definitions", []))
        payload = dict(payload)
        payload["feature_definitions"] = feature_defs
        payload["node_definitions"] = node_defs
        for key in ("windows", "aggregators", "transformers", "entity_keys", "tags"):
            if key in payload and isinstance(payload[key], list):
                payload[key] = tuple(payload[key])
        return FeatureSetDefinition(**payload)

    def _load_from_disk(self) -> None:
        assert self.storage_dir is not None
        for path in self.storage_dir.glob("*.json"):
            payload = json.loads(path.read_text(encoding="utf-8"))
            fs = self._deserialize_feature_set(payload)
            self._registry[(fs.name, fs.version)] = fs

    def register(self, feature_set: FeatureSetDefinition, *, persist: bool = True) -> FeatureSetDefinition:
        key = (feature_set.name, feature_set.version)
        existing = self._registry.get(key)
        if existing is not None:
            if existing.definition_hash != feature_set.definition_hash:
                raise DefinitionRegistryError(f"immutable feature set conflict for {feature_set.name} v{feature_set.version}")
            return existing
        self._registry[key] = feature_set
        if persist and self.storage_dir is not None:
            path = self._path_for(feature_set.name, feature_set.version)
            path.write_text(json.dumps(asdict(feature_set), ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
        return feature_set

    def get(self, name: str, version: str) -> Optional[FeatureSetDefinition]:
        return self._registry.get((name, version))

    def list_versions(self, name: str) -> Dict[str, FeatureSetDefinition]:
        return {ver: fs for (n, ver), fs in self._registry.items() if n == name}

    def all(self) -> Iterable[FeatureSetDefinition]:
        return self._registry.values()
