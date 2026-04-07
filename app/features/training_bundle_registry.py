from __future__ import annotations

from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
import json
from pathlib import Path
from typing import Any, Dict, Optional


class TrainingBundleRegistryError(ValueError):
    pass


@dataclass(frozen=True)
class TrainingBundleRecord:
    bundle_id: str
    dataset_id: str
    feature_schema_hash: str
    feature_set_name: str
    feature_set_version: str
    feature_bundle_id: str = ""
    owner: str = "quant-platform"
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    metadata: Dict[str, Any] = field(default_factory=dict)


class TrainingBundleRegistry:
    def __init__(self, storage_dir: str | Path | None = None) -> None:
        self._registry: dict[str, TrainingBundleRecord] = {}
        self.storage_dir = Path(storage_dir) if storage_dir is not None else None
        if self.storage_dir is not None:
            self.storage_dir.mkdir(parents=True, exist_ok=True)
            self._load_from_disk()

    def _path_for(self, bundle_id: str) -> Path:
        if self.storage_dir is None:
            raise TrainingBundleRegistryError("registry storage_dir is not configured")
        return self.storage_dir / f"{bundle_id}.json"

    def _deserialize_record(self, payload: dict[str, Any]) -> TrainingBundleRecord:
        payload = dict(payload)
        created_at = payload.get("created_at")
        if isinstance(created_at, str):
            payload["created_at"] = datetime.fromisoformat(created_at)
        return TrainingBundleRecord(**payload)

    def _load_from_disk(self) -> None:
        assert self.storage_dir is not None
        for path in self.storage_dir.glob("*.json"):
            payload = json.loads(path.read_text(encoding="utf-8"))
            record = self._deserialize_record(payload)
            self._registry[record.bundle_id] = record

    def register(self, record: TrainingBundleRecord, *, persist: bool = True) -> TrainingBundleRecord:
        existing = self._registry.get(record.bundle_id)
        if existing is not None:
            if existing != record:
                raise TrainingBundleRegistryError(f"immutable training bundle conflict for {record.bundle_id}")
            return existing
        self._registry[record.bundle_id] = record
        if persist and self.storage_dir is not None:
            path = self._path_for(record.bundle_id)
            payload = asdict(record)
            payload["created_at"] = record.created_at.isoformat()
            path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True), encoding="utf-8")
        return record

    def get(self, bundle_id: str) -> Optional[TrainingBundleRecord]:
        return self._registry.get(bundle_id)

