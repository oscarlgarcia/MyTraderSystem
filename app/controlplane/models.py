from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Literal


CommandStatus = Literal["pending", "running", "succeeded", "failed"]
AlertAckStatus = Literal["open", "acked"]


@dataclass(frozen=True, slots=True)
class RunRecord:
    run_id: str
    trace_id: str
    env: str
    mode: str
    result: str
    updated_at: str
    last_summary_at: str | None = None
    last_health_at: str | None = None
    summary: dict[str, Any] = field(default_factory=dict)
    health: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class StreamStatusRecord:
    scope: str
    venue: str
    symbol: str
    stream_type: str
    status: str
    run_id: str | None
    updated_at: str
    metrics: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class AlertRecord:
    alert_id: str
    trace_id: str | None
    env: str | None
    mode: str | None
    alert_type: str
    severity: str
    observed: float
    threshold: int
    recommended_action: str
    created_at: str
    payload: dict[str, Any] = field(default_factory=dict)
    ack_status: AlertAckStatus = "open"
    acked_at: str | None = None
    acked_by: str | None = None


@dataclass(frozen=True, slots=True)
class CheckpointSummaryRecord:
    stream_key: str
    checkpoint_path: str
    recorded_at: str
    checkpoint_last_event_ts: str | None = None
    cursor_kind: str | None = None
    cursor_value: str | None = None
    cursor_last_event_ts: str | None = None
    cursor_seen_entry_count: int = 0
    metadata: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class CommandRequestRecord:
    command_id: str
    command_type: str
    scope: str
    payload: dict[str, Any]
    requested_by: str
    requested_at: str
    status: CommandStatus = "pending"
    started_at: str | None = None
    finished_at: str | None = None
    result_summary: str | None = None
    error_summary: str | None = None


@dataclass(frozen=True, slots=True)
class CommandAuditRecord:
    command_id: str
    event_ts: str
    event_type: str
    payload: dict[str, Any] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class OverviewSnapshot:
    total_runs: int
    recent_runs: tuple[RunRecord, ...]
    streams_total: int
    streams_degraded: tuple[StreamStatusRecord, ...]
    alerts_open: tuple[AlertRecord, ...]
    checkpoints_total: int
    commands_pending: int
