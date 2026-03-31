"""
Local checkpoint persistence for live ingestion.

Keep this intentionally small: it stores the last observed event timestamp,
minimal dedup state, and lightweight execution metadata.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import Any

from app.config import AppConfig

CheckpointKey = tuple[str, datetime, float, float, str]

CHECKPOINT_VERSION = 1


@dataclass(frozen=True, slots=True)
class CheckpointState:
    last_event_ts: datetime | None = None
    seen_keys: tuple[CheckpointKey, ...] = ()
    metadata: dict[str, Any] = field(default_factory=dict)


def default_checkpoint_path(cfg: AppConfig) -> Path:
    return Path(cfg.data_dir) / cfg.env / "state" / "ingestion-checkpoint.json"


def _serialize_key(key: CheckpointKey) -> dict[str, Any]:
    symbol, event_ts, price, size, source = key
    return {
        "symbol": symbol,
        "event_ts": event_ts.isoformat(),
        "price": price,
        "size": size,
        "source": source,
    }


def _deserialize_key(raw: dict[str, Any]) -> CheckpointKey:
    return (
        str(raw["symbol"]),
        datetime.fromisoformat(str(raw["event_ts"])),
        float(raw["price"]),
        float(raw["size"]),
        str(raw["source"]),
    )


class CheckpointStore:
    def __init__(self, path: Path):
        self.path = Path(path)

    def load(self) -> CheckpointState | None:
        if not self.path.exists():
            return None
        try:
            payload = json.loads(self.path.read_text(encoding="utf-8"))
        except json.JSONDecodeError as exc:
            raise ValueError(f"Corrupt checkpoint file: {self.path}") from exc
        if int(payload.get("version", 0)) != CHECKPOINT_VERSION:
            raise ValueError(f"Unsupported checkpoint version in {self.path}")
        raw_keys = payload.get("seen_keys", [])
        if not isinstance(raw_keys, list):
            raise ValueError(f"Invalid checkpoint seen_keys payload in {self.path}")
        last_event_raw = payload.get("last_event_ts")
        metadata = payload.get("metadata", {})
        if not isinstance(metadata, dict):
            raise ValueError(f"Invalid checkpoint metadata payload in {self.path}")
        return CheckpointState(
            last_event_ts=datetime.fromisoformat(str(last_event_raw)) if last_event_raw else None,
            seen_keys=tuple(_deserialize_key(item) for item in raw_keys),
            metadata=metadata,
        )

    def save(self, state: CheckpointState) -> None:
        payload = {
            "version": CHECKPOINT_VERSION,
            "last_event_ts": state.last_event_ts.isoformat() if state.last_event_ts else None,
            "seen_keys": [_serialize_key(key) for key in state.seen_keys],
            "metadata": state.metadata,
        }
        self.path.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = self.path.with_suffix(self.path.suffix + ".tmp")
        tmp_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
        tmp_path.replace(self.path)
