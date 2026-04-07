from __future__ import annotations

from dataclasses import dataclass
from typing import Any

from app.controlplane.models import (
    AlertRecord,
    CheckpointSummaryRecord,
    CommandRequestRecord,
    OverviewSnapshot,
    RunRecord,
    StreamStatusRecord,
)


STREAM_METRIC_LABELS: tuple[tuple[str, str], ...] = (
    ("gaps_total", "gaps"),
    ("duplicates_total", "duplicates"),
    ("invalid_timestamp_total", "invalid ts"),
    ("heartbeat_missed_total", "heartbeat missed"),
    ("buffer_dropped_total", "buffer dropped"),
)


@dataclass(frozen=True, slots=True)
class RunSummaryView:
    run_id: str
    mode: str
    result: str
    updated_at: str
    summary_line: str
    attention_line: str | None


@dataclass(frozen=True, slots=True)
class AlertSummaryView:
    alert_id: str
    alert_type: str
    severity: str
    created_at: str
    scope: str | None
    recommended_action: str


@dataclass(frozen=True, slots=True)
class CommandSummaryView:
    command_id: str
    command_type: str
    scope: str
    status: str
    requested_at: str
    result_summary: str | None
    error_summary: str | None


@dataclass(frozen=True, slots=True)
class StreamView:
    scope: str
    venue: str
    symbol: str
    stream_type: str
    status: str
    severity: str
    updated_at: str
    freshness_label: str
    run_id: str | None
    issue_count: int
    key_metrics: tuple[tuple[str, int], ...]
    suggested_action: str
    checkpoint_cursor: str | None
    checkpoint_last_event_ts: str | None
    latest_run_result: str | None
    latest_run_updated_at: str | None
    recovery_params: dict[str, str]
    raw_metrics: dict[str, Any]


@dataclass(frozen=True, slots=True)
class StreamDetailView:
    stream: StreamView
    run: RunSummaryView | None
    checkpoint_path: str | None
    checkpoint_metadata: dict[str, Any]
    recommended_actions: tuple[str, ...]
    raw_metrics: dict[str, Any]


@dataclass(frozen=True, slots=True)
class OverviewView:
    total_runs: int
    streams_total: int
    degraded_streams_total: int
    open_alerts_total: int
    commands_pending: int
    checkpoints_total: int
    recent_runs: tuple[RunSummaryView, ...]
    attention_streams: tuple[StreamView, ...]
    open_alerts: tuple[AlertSummaryView, ...]


def build_run_summary(run: RunRecord) -> RunSummaryView:
    persisted = int(run.summary.get("events_persisted", 0) or 0)
    reconnects = int(run.summary.get("reconnects", 0) or 0)
    degraded = int(len(run.health.get("streams_degraded", []) or []))
    summary_line = f"{persisted} persisted, {reconnects} reconnects"
    attention_line = None
    if degraded > 0:
        attention_line = f"{degraded} degraded streams"
    elif run.result != "ok":
        attention_line = f"result={run.result}"
    return RunSummaryView(
        run_id=run.run_id,
        mode=run.mode,
        result=run.result,
        updated_at=run.updated_at,
        summary_line=summary_line,
        attention_line=attention_line,
    )


def build_alert_summary(alert: AlertRecord) -> AlertSummaryView:
    scope = None
    payload_scope = alert.payload.get("scope")
    if isinstance(payload_scope, str) and payload_scope:
        scope = payload_scope
    elif isinstance(alert.payload.get("stream"), str):
        scope = str(alert.payload["stream"])
    return AlertSummaryView(
        alert_id=alert.alert_id,
        alert_type=alert.alert_type,
        severity=alert.severity,
        created_at=alert.created_at,
        scope=scope,
        recommended_action=alert.recommended_action,
    )


def build_command_summary(command: CommandRequestRecord) -> CommandSummaryView:
    return CommandSummaryView(
        command_id=command.command_id,
        command_type=command.command_type,
        scope=command.scope,
        status=command.status,
        requested_at=command.requested_at,
        result_summary=command.result_summary,
        error_summary=command.error_summary,
    )


def build_stream_view(
    stream: StreamStatusRecord,
    *,
    checkpoint: CheckpointSummaryRecord | None = None,
    run: RunRecord | None = None,
) -> StreamView:
    key_metrics = tuple(
        (label, int(stream.metrics.get(key, 0) or 0))
        for key, label in STREAM_METRIC_LABELS
        if int(stream.metrics.get(key, 0) or 0) > 0
    )
    issue_count = sum(value for _label, value in key_metrics)
    if stream.status == "failed":
        severity = "critical"
    elif stream.status != "ok" or issue_count > 0:
        severity = "warning"
    else:
        severity = "healthy"
    suggested_action = "inspect"
    if stream.stream_type in {"kline", "trade"}:
        suggested_action = "resync" if stream.stream_type == "kline" else "replay"
    checkpoint_cursor = None
    checkpoint_last_event_ts = None
    if checkpoint is not None:
        checkpoint_cursor = " / ".join(
            part
            for part in (
                checkpoint.cursor_kind or "",
                checkpoint.cursor_value or "",
            )
            if part
        ) or None
        checkpoint_last_event_ts = checkpoint.cursor_last_event_ts or checkpoint.checkpoint_last_event_ts
    recovery_params = {
        "symbol": stream.symbol,
        "stream_type": stream.stream_type,
    }
    if stream.stream_type == "kline":
        recovery_params["interval"] = "1m"
    return StreamView(
        scope=stream.scope,
        venue=stream.venue,
        symbol=stream.symbol,
        stream_type=stream.stream_type,
        status=stream.status,
        severity=severity,
        updated_at=stream.updated_at,
        freshness_label=stream.updated_at,
        run_id=stream.run_id,
        issue_count=issue_count,
        key_metrics=key_metrics,
        suggested_action=suggested_action,
        checkpoint_cursor=checkpoint_cursor,
        checkpoint_last_event_ts=checkpoint_last_event_ts,
        latest_run_result=run.result if run else None,
        latest_run_updated_at=run.updated_at if run else None,
        recovery_params=recovery_params,
        raw_metrics=dict(stream.metrics),
    )


def build_stream_detail(
    stream: StreamStatusRecord,
    *,
    checkpoint: CheckpointSummaryRecord | None = None,
    run: RunRecord | None = None,
) -> StreamDetailView:
    stream_view = build_stream_view(stream, checkpoint=checkpoint, run=run)
    recommended_actions = ("inspect metrics",)
    if stream_view.suggested_action == "resync":
        recommended_actions = ("inspect metrics", "queue resync")
    elif stream_view.suggested_action == "replay":
        recommended_actions = ("inspect metrics", "queue replay")
    run_view = build_run_summary(run) if run is not None else None
    return StreamDetailView(
        stream=stream_view,
        run=run_view,
        checkpoint_path=checkpoint.checkpoint_path if checkpoint else None,
        checkpoint_metadata=dict(checkpoint.metadata) if checkpoint else {},
        recommended_actions=recommended_actions,
        raw_metrics=dict(stream.metrics),
    )


def build_overview_view(
    overview: OverviewSnapshot,
    *,
    checkpoints_by_scope: dict[str, CheckpointSummaryRecord],
    runs_by_id: dict[str, RunRecord],
) -> OverviewView:
    recent_runs = tuple(build_run_summary(run) for run in overview.recent_runs)
    attention_streams = tuple(
        build_stream_view(
            stream,
            checkpoint=checkpoints_by_scope.get(stream.scope),
            run=runs_by_id.get(stream.run_id) if stream.run_id else None,
        )
        for stream in overview.streams_degraded[:8]
    )
    alerts = tuple(build_alert_summary(alert) for alert in overview.alerts_open[:8])
    return OverviewView(
        total_runs=overview.total_runs,
        streams_total=overview.streams_total,
        degraded_streams_total=len(overview.streams_degraded),
        open_alerts_total=len(overview.alerts_open),
        commands_pending=overview.commands_pending,
        checkpoints_total=overview.checkpoints_total,
        recent_runs=recent_runs,
        attention_streams=attention_streams,
        open_alerts=alerts,
    )


def filter_streams(
    streams: list[StreamStatusRecord],
    *,
    status: str = "",
    symbol: str = "",
    stream_type: str = "",
    query: str = "",
) -> list[StreamStatusRecord]:
    status = status.strip().lower()
    symbol = symbol.strip().upper()
    stream_type = stream_type.strip().lower()
    query = query.strip().lower()
    filtered = []
    for stream in streams:
        if status and stream.status.lower() != status:
            continue
        if symbol and stream.symbol.upper() != symbol:
            continue
        if stream_type and stream.stream_type.lower() != stream_type:
            continue
        if query and query not in stream.scope.lower():
            continue
        filtered.append(stream)
    return filtered
