from __future__ import annotations

import json
import os
from dataclasses import asdict, dataclass
from pathlib import Path
from typing import Literal


OperationalChannel = Literal["manual", "scheduled", "pipeline"]

_RUNNER_CONTEXT_KEYS = (
    "execution_ref",
    "channel",
    "schedule_name",
    "job_id",
    "job_url",
    "owner",
)


@dataclass(frozen=True, slots=True)
class RunnerContext:
    execution_ref: str
    channel: OperationalChannel
    schedule_name: str
    job_id: str
    job_url: str
    owner: str | None
    source: str


def load_runner_context_from_file(path: Path) -> RunnerContext:
    payload = json.loads(Path(path).read_text(encoding="utf-8"))
    return _runner_context_from_mapping(payload, source=f"file:{Path(path)}")


def load_runner_context_from_env(*, prefix: str = "INGESTION_RUNNER_") -> RunnerContext:
    mapping = {
        "execution_ref": os.getenv(f"{prefix}EXECUTION_REF", ""),
        "channel": os.getenv(f"{prefix}CHANNEL", ""),
        "schedule_name": os.getenv(f"{prefix}SCHEDULE_NAME", ""),
        "job_id": os.getenv(f"{prefix}JOB_ID", ""),
        "job_url": os.getenv(f"{prefix}JOB_URL", ""),
        "owner": os.getenv(f"{prefix}OWNER", ""),
    }
    return _runner_context_from_mapping(mapping, source=f"env:{prefix}")


def resolve_runner_context(
    *,
    execution_ref: str | None = None,
    channel: str | None = None,
    schedule_name: str | None = None,
    job_id: str | None = None,
    job_url: str | None = None,
    owner: str | None = None,
    runner_context_path: Path | None = None,
    runner_context_from_env: bool = False,
    env_prefix: str = "INGESTION_RUNNER_",
) -> RunnerContext:
    file_context = load_runner_context_from_file(runner_context_path) if runner_context_path is not None else None
    env_context = load_runner_context_from_env(prefix=env_prefix) if runner_context_from_env else None

    def _pick(name: str) -> str | None:
        explicit = {
            "execution_ref": execution_ref,
            "channel": channel,
            "schedule_name": schedule_name,
            "job_id": job_id,
            "job_url": job_url,
            "owner": owner,
        }[name]
        if explicit not in (None, ""):
            return str(explicit)
        if file_context is not None:
            value = getattr(file_context, name)
            if value not in (None, ""):
                return str(value)
        if env_context is not None:
            value = getattr(env_context, name)
            if value not in (None, ""):
                return str(value)
        return None

    resolved_source = "cli"
    if runner_context_path is not None:
        resolved_source = f"file:{Path(runner_context_path)}"
    elif runner_context_from_env:
        resolved_source = f"env:{env_prefix}"

    merged = {
        "execution_ref": _pick("execution_ref") or "",
        "channel": _pick("channel") or "",
        "schedule_name": _pick("schedule_name") or "",
        "job_id": _pick("job_id") or "",
        "job_url": _pick("job_url") or "",
        "owner": _pick("owner"),
    }
    return _runner_context_from_mapping(merged, source=resolved_source)


def write_runner_context(path: Path, context: RunnerContext) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(asdict(context), ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return path


def _runner_context_from_mapping(mapping: dict[str, object], *, source: str) -> RunnerContext:
    payload = {key: mapping.get(key) for key in _RUNNER_CONTEXT_KEYS}
    missing = [key for key in ("execution_ref", "channel", "schedule_name", "job_id", "job_url") if str(payload.get(key) or "").strip() == ""]
    if missing:
        raise ValueError(f"runner context missing required fields: {', '.join(missing)}")
    channel = str(payload["channel"]).strip().lower()
    if channel not in {"manual", "scheduled", "pipeline"}:
        raise ValueError(f"unsupported runner context channel: {channel}")
    owner = str(payload["owner"]).strip() if payload.get("owner") not in (None, "") else None
    return RunnerContext(
        execution_ref=str(payload["execution_ref"]).strip(),
        channel=channel,  # type: ignore[arg-type]
        schedule_name=str(payload["schedule_name"]).strip(),
        job_id=str(payload["job_id"]).strip(),
        job_url=str(payload["job_url"]).strip(),
        owner=owner,
        source=str(source),
    )
