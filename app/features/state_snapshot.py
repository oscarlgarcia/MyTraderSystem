from __future__ import annotations

import json
from pathlib import Path
from typing import Optional

from app.features.state import RuntimeStateStore


class StateSnapshotStore:
    SCHEMA_VERSION = "v2"

    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)

    def save(self, state: RuntimeStateStore) -> None:
        payload = state.snapshot()
        payload["schema_version"] = self.SCHEMA_VERSION
        self.path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")

    def load(self) -> Optional[RuntimeStateStore]:
        if not self.path.exists():
            return None
        payload = json.loads(self.path.read_text(encoding="utf-8"))
        schema_version = payload.get("schema_version", "v1")
        if schema_version not in {"v1", self.SCHEMA_VERSION}:
            raise ValueError(f"unsupported runtime snapshot schema_version={schema_version}")
        return RuntimeStateStore.from_snapshot(payload)
