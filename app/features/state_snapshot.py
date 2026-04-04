from __future__ import annotations

import json
from pathlib import Path
from typing import Optional

from app.features.state import RuntimeStateStore


class StateSnapshotStore:
    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)

    def save(self, state: RuntimeStateStore) -> None:
        self.path.write_text(json.dumps(state.snapshot(), ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")

    def load(self) -> Optional[RuntimeStateStore]:
        if not self.path.exists():
            return None
        payload = json.loads(self.path.read_text(encoding="utf-8"))
        return RuntimeStateStore.from_snapshot(payload)
