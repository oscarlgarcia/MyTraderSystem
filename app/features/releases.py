from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import json
from typing import Dict, Optional


@dataclass(frozen=True)
class ReleasedFeatureSet:
    name: str
    active_version: str
    previous_version: str | None = None


class FeatureReleaseRegistry:
    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        if not self.path.exists():
            self.path.write_text("{}", encoding="utf-8")

    def _load(self) -> Dict[str, Dict[str, str | None]]:
        return json.loads(self.path.read_text(encoding="utf-8"))

    def _save(self, payload: Dict[str, Dict[str, str | None]]) -> None:
        self.path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")

    def activate(self, *, name: str, version: str) -> ReleasedFeatureSet:
        payload = self._load()
        previous = payload.get(name, {}).get("active_version")
        payload[name] = {"active_version": version, "previous_version": previous}
        self._save(payload)
        return ReleasedFeatureSet(name=name, active_version=version, previous_version=previous)

    def rollback(self, *, name: str) -> ReleasedFeatureSet:
        payload = self._load()
        current = payload.get(name)
        if not current or not current.get("previous_version"):
            raise ValueError(f"no rollback target for feature set {name}")
        payload[name] = {
            "active_version": current["previous_version"],
            "previous_version": current["active_version"],
        }
        self._save(payload)
        return ReleasedFeatureSet(name=name, active_version=payload[name]["active_version"], previous_version=payload[name]["previous_version"])

    def get(self, name: str) -> Optional[ReleasedFeatureSet]:
        payload = self._load().get(name)
        if not payload:
            return None
        return ReleasedFeatureSet(name=name, active_version=payload["active_version"], previous_version=payload.get("previous_version"))
