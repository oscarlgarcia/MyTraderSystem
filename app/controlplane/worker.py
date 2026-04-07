from __future__ import annotations

import argparse
import json
import time
from datetime import datetime, timezone
from typing import Callable

from app.config import load_config
from app.controlplane.builder import ReadModelBuilder
from app.controlplane.models import CommandAuditRecord, CommandRequestRecord
from app.controlplane.operations import execute_ack_alert, execute_replay_range, execute_resync_stream
from app.controlplane.store import ControlPlaneStore
from app.controlplane.store_factory import create_control_plane_store
from app.controlplane.telemetry import configure_control_plane_telemetry, emit_control_plane_event


CommandExecutor = Callable[..., dict]


def _now() -> str:
    return datetime.now(timezone.utc).isoformat()


def default_executor_registry() -> dict[str, CommandExecutor]:
    return {
        "ack_alert": execute_ack_alert,
        "replay_range": execute_replay_range,
        "resync_stream": execute_resync_stream,
    }


def process_next_command(
    *,
    store: ControlPlaneStore,
    cfg,
    worker_id: str,
    executors: dict[str, CommandExecutor] | None = None,
) -> bool:
    executors = executors or default_executor_registry()
    command = store.claim_next_command(worker_id=worker_id, started_at=_now())
    if command is None:
        return False
    _append_audit(store, command.command_id, "started", {"worker_id": worker_id, "status": command.status})
    try:
        executor = executors[command.command_type]
        if command.command_type == "ack_alert":
            result = executor(store=store, payload=command.payload, requested_by=command.requested_by)
        else:
            result = executor(cfg=cfg, payload=command.payload)
        result_summary = json.dumps(result, ensure_ascii=False, sort_keys=True, default=str)
        store.complete_command(
            command.command_id,
            status="succeeded",
            finished_at=_now(),
            result_summary=result_summary,
            error_summary=None,
        )
        _append_audit(store, command.command_id, "succeeded", result)
        emit_control_plane_event(
            "recovery_command_audit",
            {
                "command_id": command.command_id,
                "command_type": command.command_type,
                "scope": command.scope,
                "status": "succeeded",
                "requested_by": command.requested_by,
                "requested_at": command.requested_at,
                "result": result,
            },
        )
    except Exception as exc:
        store.complete_command(
            command.command_id,
            status="failed",
            finished_at=_now(),
            result_summary=None,
            error_summary=str(exc),
        )
        _append_audit(store, command.command_id, "failed", {"error": str(exc), "error_type": type(exc).__name__})
        emit_control_plane_event(
            "recovery_command_audit",
            {
                "command_id": command.command_id,
                "command_type": command.command_type,
                "scope": command.scope,
                "status": "failed",
                "requested_by": command.requested_by,
                "requested_at": command.requested_at,
                "error": str(exc),
                "error_type": type(exc).__name__,
            },
        )
    return True


def run_worker_loop(*, env: str | None = None, once: bool = False) -> int:
    cfg = load_config(env)
    configure_control_plane_telemetry(cfg.control_plane_telemetry_dir)
    store = create_control_plane_store(cfg)
    builder = ReadModelBuilder(cfg.control_plane_telemetry_dir, store)
    worker_id = f"control-plane-worker-{cfg.env}"
    while True:
        builder.sync_once()
        processed = process_next_command(store=store, cfg=cfg, worker_id=worker_id)
        if once:
            return 0 if processed else 1
        if not processed:
            time.sleep(max(0.1, float(cfg.control_plane_command_poll_interval_seconds)))


def _append_audit(store: ControlPlaneStore, command_id: str, event_type: str, payload: dict) -> None:
    store.append_command_audit(
        CommandAuditRecord(
            command_id=command_id,
            event_ts=_now(),
            event_type=event_type,
            payload=dict(payload),
        )
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Control-plane worker for ingestion UI commands.")
    parser.add_argument("--env", choices=["dev", "test", "prod"], default=None)
    parser.add_argument("--once", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    return run_worker_loop(env=args.env, once=bool(args.once))


if __name__ == "__main__":
    raise SystemExit(main())
