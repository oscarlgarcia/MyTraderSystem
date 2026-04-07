"""
Local checkpoint persistence for live ingestion.

Keep this intentionally small: it stores the last observed event timestamp,
minimal dedup state, and lightweight execution metadata.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from app.config import AppConfig
from app.controlplane.telemetry import emit_control_plane_event
from app.ingestion.dedup import DedupStateEntry, EventIdentity, deserialize_identity, serialize_identity
from app.marketdata.temporal_state import CursorState, TemporalPartitionKey

CHECKPOINT_VERSION = 3


@dataclass(frozen=True, slots=True)
class CheckpointState:
    last_event_ts: datetime | None = None
    seen_entries: tuple[DedupStateEntry, ...] = ()
    stream_cursors: dict[str, CursorState] = field(default_factory=dict)
    metadata: dict[str, Any] = field(default_factory=dict)


def default_checkpoint_path(cfg: AppConfig) -> Path:
    return Path(cfg.data_dir) / cfg.env / "state" / "ingestion-checkpoint.json"


CheckpointKey = EventIdentity


class CheckpointStore:
    def __init__(self, path: Path, *, telemetry_dir: Path | None = None):
        self.path = Path(path)
        try:
            self.telemetry_dir = Path(telemetry_dir) if telemetry_dir not in (None, "") else None
        except (TypeError, ValueError):
            self.telemetry_dir = None

    @property
    def audit_path(self) -> Path:
        return self.path.with_name(f"{self.path.stem}-audit.jsonl")

    def load(self) -> CheckpointState | None:
        if not self.path.exists():
            return None
        try:
            payload = json.loads(self.path.read_text(encoding="utf-8"))
        except json.JSONDecodeError as exc:
            raise ValueError(f"Corrupt checkpoint file: {self.path}") from exc
        version = int(payload.get("version", 0))
        if version not in (1, 2, CHECKPOINT_VERSION):
            raise ValueError(f"Unsupported checkpoint version in {self.path}")
        raw_entries = payload.get("seen_entries", [])
        if version == 1:
            raw_keys = payload.get("seen_keys", [])
            if not isinstance(raw_keys, list):
                raise ValueError(f"Invalid checkpoint seen_keys payload in {self.path}")
            now = datetime.now().timestamp()
            seen_entries = tuple(DedupStateEntry(key=_deserialize_legacy_key(item), seen_at=now) for item in raw_keys)
        else:
            if not isinstance(raw_entries, list):
                raise ValueError(f"Invalid checkpoint seen_entries payload in {self.path}")
            seen_entries = tuple(_deserialize_entry(item) for item in raw_entries)
        stream_cursors = {}
        if version == CHECKPOINT_VERSION:
            raw_streams = payload.get("streams", {})
            if not isinstance(raw_streams, dict):
                raise ValueError(f"Invalid checkpoint streams payload in {self.path}")
            stream_cursors = {
                str(label): _deserialize_cursor_state(item)
                for label, item in raw_streams.items()
            }
            if stream_cursors:
                flattened_entries = []
                for cursor_state in stream_cursors.values():
                    flattened_entries.extend(cursor_state.seen_entries)
                seen_entries = tuple(flattened_entries)
        last_event_raw = payload.get("last_event_ts")
        metadata = payload.get("metadata", {})
        if not isinstance(metadata, dict):
            raise ValueError(f"Invalid checkpoint metadata payload in {self.path}")
        return CheckpointState(
            last_event_ts=datetime.fromisoformat(str(last_event_raw)) if last_event_raw else None,
            seen_entries=seen_entries,
            stream_cursors=stream_cursors,
            metadata=metadata,
        )

    def save(self, state: CheckpointState) -> None:
        payload = {
            "version": CHECKPOINT_VERSION,
            "last_event_ts": state.last_event_ts.isoformat() if state.last_event_ts else None,
            "seen_entries": [_serialize_entry(entry) for entry in state.seen_entries],
            "streams": {
                label: _serialize_cursor_state(cursor_state)
                for label, cursor_state in state.stream_cursors.items()
            },
            "metadata": state.metadata,
        }
        self.path.parent.mkdir(parents=True, exist_ok=True)
        tmp_path = self.path.with_suffix(self.path.suffix + ".tmp")
        tmp_path.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
        tmp_path.replace(self.path)

    def append_audit_event(self, payload: dict[str, Any]) -> None:
        self.audit_path.parent.mkdir(parents=True, exist_ok=True)
        with self.audit_path.open("a", encoding="utf-8", newline="\n") as handle:
            handle.write(json.dumps(payload, ensure_ascii=False, sort_keys=True, default=str))
            handle.write("\n")
        event_payload = dict(payload)
        event_payload["checkpoint_event_type"] = str(payload.get("event_type", "unknown"))
        event_payload["event_type"] = "checkpoint_audit_ref"
        event_payload["audit_path"] = str(self.audit_path)
        emit_control_plane_event("checkpoint_audit_ref", event_payload, telemetry_dir=self.telemetry_dir)

    def record_checkpoint_event(
        self,
        *,
        event_type: str,
        trace_id: str | None,
        state: CheckpointState | None,
        extra: dict[str, Any] | None = None,
    ) -> None:
        cursor_summaries = checkpoint_cursor_summaries(state)
        base_payload = {
            "recorded_at": datetime.now(timezone.utc).isoformat(),
            "event_type": event_type,
            "trace_id": trace_id,
            "checkpoint_path": str(self.path),
            "stream_key": "*",
            "checkpoint_found": state is not None,
            "checkpoint_last_event_ts": state.last_event_ts.isoformat() if state and state.last_event_ts else None,
            "checkpoint_cursor_count": len(cursor_summaries),
            "checkpoint_stream_keys": sorted(cursor_summaries),
            "checkpoint_metadata": dict(state.metadata) if state is not None else {},
        }
        if extra:
            base_payload.update(extra)
        self.append_audit_event(base_payload)
        for stream_key, cursor_summary in cursor_summaries.items():
            stream_payload = dict(base_payload)
            stream_payload.update(
                {
                    "event_type": f"{event_type}_stream",
                    "stream_key": stream_key,
                    **cursor_summary,
                }
            )
            self.append_audit_event(stream_payload)


def checkpoint_cursor_summaries(state: CheckpointState | None) -> dict[str, dict[str, Any]]:
    if state is None:
        return {}
    payload: dict[str, dict[str, Any]] = {}
    for label, cursor_state in state.stream_cursors.items():
        payload[str(label)] = {
            "cursor_kind": cursor_state.cursor_kind,
            "cursor_value": cursor_state.cursor_value,
            "cursor_last_event_ts": cursor_state.last_event_ts.isoformat() if cursor_state.last_event_ts else None,
            "cursor_seen_entry_count": len(cursor_state.seen_entries),
        }
    return payload


def _serialize_entry(entry: DedupStateEntry) -> dict[str, Any]:
    return {
        "key": serialize_identity(entry.key),
        "seen_at": float(entry.seen_at),
    }


def _deserialize_entry(raw: dict[str, Any]) -> DedupStateEntry:
    return DedupStateEntry(
        key=deserialize_identity(raw["key"]),
        seen_at=float(raw["seen_at"]),
    )


def _deserialize_legacy_key(raw: dict[str, Any]) -> EventIdentity:
    return (
        "heuristic",
        str(raw["symbol"]),
        datetime.fromisoformat(str(raw["event_ts"])),
        float(raw["price"]),
        float(raw["size"]),
        str(raw["source"]),
    )


def _serialize_cursor_state(state: CursorState) -> dict[str, Any]:
    return {
        "venue": state.partition.venue,
        "symbol": state.partition.symbol,
        "stream_type": state.partition.stream_type,
        "last_event_ts": state.last_event_ts.isoformat() if state.last_event_ts else None,
        "cursor_kind": state.cursor_kind,
        "cursor_value": state.cursor_value,
        "seen_entries": [_serialize_entry(entry) for entry in state.seen_entries],
    }


def _deserialize_cursor_state(raw: dict[str, Any]) -> CursorState:
    seen_entries_raw = raw.get("seen_entries", [])
    if not isinstance(seen_entries_raw, list):
        raise ValueError("Invalid cursor state seen_entries payload")
    return CursorState(
        partition=TemporalPartitionKey(
            venue=str(raw["venue"]).upper(),
            symbol=str(raw["symbol"]),
            stream_type=str(raw["stream_type"]),
        ),
        last_event_ts=datetime.fromisoformat(str(raw["last_event_ts"])) if raw.get("last_event_ts") else None,
        cursor_kind=str(raw["cursor_kind"]) if raw.get("cursor_kind") else None,
        cursor_value=str(raw["cursor_value"]) if raw.get("cursor_value") else None,
        seen_entries=tuple(_deserialize_entry(entry) for entry in seen_entries_raw),
    )
