from __future__ import annotations

import hashlib
import json
from dataclasses import replace
from pathlib import Path
from typing import Any

from app.controlplane.models import AlertRecord, CheckpointSummaryRecord, RunRecord, StreamStatusRecord
from app.controlplane.store import ControlPlaneStore
from app.controlplane.telemetry import scope_label


DEGRADED_STREAM_KEYS = (
    "messages_invalid_total",
    "invalid_timestamp_total",
    "duplicates_total",
    "gaps_total",
    "gap_irreparable_total",
    "heartbeat_missed_total",
    "buffer_dropped_total",
)


class ReadModelBuilder:
    def __init__(self, telemetry_dir: str | Path, store: ControlPlaneStore) -> None:
        self.telemetry_dir = Path(telemetry_dir)
        self.store = store

    def sync_once(self) -> None:
        if not self.telemetry_dir.exists():
            return
        for path in sorted(self.telemetry_dir.glob("*.jsonl")):
            self._sync_file(path)

    def _sync_file(self, path: Path) -> None:
        offset = self.store.get_offset(path.name)
        last_processed = offset
        with path.open("r", encoding="utf-8") as handle:
            for line_no, raw_line in enumerate(handle, start=1):
                if line_no <= offset:
                    continue
                raw_line = raw_line.strip()
                if not raw_line:
                    last_processed = line_no
                    continue
                payload = json.loads(raw_line)
                self._apply_event(path.stem, payload)
                last_processed = line_no
        if last_processed != offset:
            self.store.update_offset(path.name, last_processed)

    def _apply_event(self, event_type: str, payload: dict[str, Any]) -> None:
        if event_type == "ingestion_summary":
            self._apply_summary(payload)
            return
        if event_type == "ingestion_health":
            self._apply_health(payload)
            return
        if event_type == "operational_alert":
            self._apply_alert(payload)
            return
        if event_type == "checkpoint_audit_ref":
            self._apply_checkpoint_ref(payload)

    def _apply_summary(self, payload: dict[str, Any]) -> None:
        run_id = self._run_id(payload)
        existing = self.store.get_run(run_id)
        summary = dict(payload)
        record = RunRecord(
            run_id=run_id,
            trace_id=payload.get("trace_id") or run_id,
            env=str(payload.get("env", "unknown")),
            mode=str(payload.get("mode", "unknown")),
            result=str(payload.get("result", "unknown")),
            updated_at=str(payload.get("recorded_at")),
            last_summary_at=str(payload.get("recorded_at")),
            last_health_at=existing.last_health_at if existing else None,
            summary=summary,
            health=existing.health if existing else {},
        )
        self.store.upsert_run(record)
        for metric in payload.get("stream_metrics", []) or []:
            if not isinstance(metric, dict):
                continue
            venue = str(metric.get("venue", "BINANCE")).upper()
            symbol = str(metric.get("symbol", "UNKNOWN"))
            stream_type = str(metric.get("stream_type", "unknown"))
            status = "degraded" if self._stream_degraded(metric) else "ok"
            self.store.upsert_stream_status(
                StreamStatusRecord(
                    scope=scope_label(venue=venue, symbol=symbol, stream_type=stream_type),
                    venue=venue,
                    symbol=symbol,
                    stream_type=stream_type,
                    status=status,
                    run_id=run_id,
                    updated_at=str(payload.get("recorded_at")),
                    metrics=dict(metric),
                )
            )

    def _apply_health(self, payload: dict[str, Any]) -> None:
        run_id = self._run_id(payload)
        existing = self.store.get_run(run_id)
        record = RunRecord(
            run_id=run_id,
            trace_id=payload.get("trace_id") or run_id,
            env=str(payload.get("env", "unknown")),
            mode=str(payload.get("mode", "unknown")),
            result=str(payload.get("result", existing.result if existing else "unknown")),
            updated_at=str(payload.get("recorded_at")),
            last_summary_at=existing.last_summary_at if existing else None,
            last_health_at=str(payload.get("recorded_at")),
            summary=existing.summary if existing else {},
            health=dict(payload),
        )
        self.store.upsert_run(record)
        degraded = {str(item) for item in payload.get("streams_degraded", []) or []}
        for scope in degraded:
            existing_stream = self.store.get_stream(scope)
            if existing_stream is None:
                parts = scope.split(":")
                if len(parts) != 3:
                    continue
                existing_stream = StreamStatusRecord(
                    scope=scope,
                    venue=parts[0],
                    symbol=parts[1],
                    stream_type=parts[2],
                    status="degraded",
                    run_id=run_id,
                    updated_at=str(payload.get("recorded_at")),
                    metrics={"degraded_by_health": True},
                )
            else:
                existing_stream = replace(existing_stream, status="degraded", updated_at=str(payload.get("recorded_at")))
            self.store.upsert_stream_status(existing_stream)

    def _apply_alert(self, payload: dict[str, Any]) -> None:
        digest = hashlib.sha1(
            json.dumps(payload, ensure_ascii=False, sort_keys=True, default=str).encode("utf-8")
        ).hexdigest()
        self.store.insert_alert(
            AlertRecord(
                alert_id=digest,
                trace_id=payload.get("trace_id"),
                env=payload.get("env"),
                mode=payload.get("mode"),
                alert_type=str(payload.get("alert_type", "unknown")),
                severity=str(payload.get("alert_severity", payload.get("severity", "warning"))),
                observed=float(payload.get("observed", 0)),
                threshold=int(payload.get("threshold", 0)),
                recommended_action=str(payload.get("recommended_action", "")),
                created_at=str(payload.get("recorded_at")),
                payload=dict(payload),
            )
        )

    def _apply_checkpoint_ref(self, payload: dict[str, Any]) -> None:
        stream_key = str(payload.get("stream_key", ""))
        if not stream_key or stream_key == "*":
            return
        self.store.upsert_checkpoint_summary(
            CheckpointSummaryRecord(
                stream_key=stream_key,
                checkpoint_path=str(payload.get("checkpoint_path", "")),
                recorded_at=str(payload.get("recorded_at")),
                checkpoint_last_event_ts=payload.get("checkpoint_last_event_ts"),
                cursor_kind=payload.get("cursor_kind"),
                cursor_value=payload.get("cursor_value"),
                cursor_last_event_ts=payload.get("cursor_last_event_ts"),
                cursor_seen_entry_count=int(payload.get("cursor_seen_entry_count", 0)),
                metadata=dict(payload.get("checkpoint_metadata", {})),
            )
        )

    def _run_id(self, payload: dict[str, Any]) -> str:
        trace_id = payload.get("trace_id")
        if trace_id not in (None, ""):
            return str(trace_id)
        return f"{payload.get('env', 'unknown')}:{payload.get('mode', 'unknown')}:{payload.get('recorded_at', 'unknown')}"

    def _stream_degraded(self, metric: dict[str, Any]) -> bool:
        return any(int(metric.get(key, 0) or 0) > 0 for key in DEGRADED_STREAM_KEYS)
