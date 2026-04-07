from __future__ import annotations

import json
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Any


_TELEMETRY_DIR: Path | None = None


def _coerce_path(value: str | os.PathLike[str] | Path | None) -> Path | None:
    if value in (None, ""):
        return None
    try:
        return Path(value)
    except (TypeError, ValueError):
        return None


def configure_control_plane_telemetry(path: str | Path | None) -> None:
    global _TELEMETRY_DIR
    _TELEMETRY_DIR = _coerce_path(path)


def control_plane_telemetry_dir() -> Path | None:
    return _TELEMETRY_DIR


def emit_control_plane_event(
    event_type: str,
    payload: dict[str, Any],
    *,
    telemetry_dir: str | Path | None = None,
) -> None:
    target = _coerce_path(telemetry_dir) if telemetry_dir not in (None, "") else _TELEMETRY_DIR
    if target is None:
        return
    safe_payload = dict(payload)
    safe_payload.setdefault("event_type", event_type)
    safe_payload.setdefault("recorded_at", datetime.now(timezone.utc).isoformat())
    try:
        target.mkdir(parents=True, exist_ok=True)
        out_path = target / f"{event_type}.jsonl"
        with out_path.open("a", encoding="utf-8", newline="\n") as handle:
            handle.write(json.dumps(safe_payload, ensure_ascii=False, sort_keys=True, default=str))
            handle.write("\n")
    except OSError:
        return


def scope_label(*, venue: str, symbol: str, stream_type: str) -> str:
    return f"{str(venue).upper()}:{symbol}:{stream_type}"
