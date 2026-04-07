from __future__ import annotations

import json
import sqlite3
from pathlib import Path

from app.controlplane.models import (
    AlertRecord,
    CheckpointSummaryRecord,
    CommandAuditRecord,
    CommandRequestRecord,
    OverviewSnapshot,
    RunRecord,
    StreamStatusRecord,
)
from app.controlplane.store import ControlPlaneStore


def _json_load(value: str | None) -> dict:
    if not value:
        return {}
    return json.loads(value)


class SQLiteControlPlaneStore(ControlPlaneStore):
    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self.initialize()

    def _connect(self) -> sqlite3.Connection:
        conn = sqlite3.connect(self.path)
        conn.row_factory = sqlite3.Row
        return conn

    def initialize(self) -> None:
        with self._connect() as conn:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS runs (
                    run_id TEXT PRIMARY KEY,
                    trace_id TEXT NOT NULL,
                    env TEXT NOT NULL,
                    mode TEXT NOT NULL,
                    result TEXT NOT NULL,
                    updated_at TEXT NOT NULL,
                    last_summary_at TEXT,
                    last_health_at TEXT,
                    summary_json TEXT NOT NULL,
                    health_json TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS stream_status (
                    scope TEXT PRIMARY KEY,
                    venue TEXT NOT NULL,
                    symbol TEXT NOT NULL,
                    stream_type TEXT NOT NULL,
                    status TEXT NOT NULL,
                    run_id TEXT,
                    last_seen_at TEXT NOT NULL,
                    metrics_json TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS alerts (
                    alert_id TEXT PRIMARY KEY,
                    trace_id TEXT,
                    env TEXT,
                    mode TEXT,
                    alert_type TEXT NOT NULL,
                    severity TEXT NOT NULL,
                    observed REAL NOT NULL,
                    threshold INTEGER NOT NULL,
                    recommended_action TEXT NOT NULL,
                    created_at TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    ack_status TEXT NOT NULL,
                    acked_at TEXT,
                    acked_by TEXT
                );
                CREATE TABLE IF NOT EXISTS checkpoint_summary (
                    stream_key TEXT PRIMARY KEY,
                    checkpoint_path TEXT NOT NULL,
                    recorded_at TEXT NOT NULL,
                    checkpoint_last_event_ts TEXT,
                    cursor_kind TEXT,
                    cursor_value TEXT,
                    cursor_last_event_ts TEXT,
                    cursor_seen_entry_count INTEGER NOT NULL,
                    metadata_json TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS command_requests (
                    command_id TEXT PRIMARY KEY,
                    command_type TEXT NOT NULL,
                    scope TEXT NOT NULL,
                    payload_json TEXT NOT NULL,
                    requested_by TEXT NOT NULL,
                    requested_at TEXT NOT NULL,
                    started_at TEXT,
                    finished_at TEXT,
                    status TEXT NOT NULL,
                    result_summary TEXT,
                    error_summary TEXT
                );
                CREATE TABLE IF NOT EXISTS command_audit (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    command_id TEXT NOT NULL,
                    event_ts TEXT NOT NULL,
                    event_type TEXT NOT NULL,
                    payload_json TEXT NOT NULL
                );
                CREATE TABLE IF NOT EXISTS read_model_offsets (
                    source_name TEXT PRIMARY KEY,
                    last_line_number INTEGER NOT NULL
                );
                CREATE INDEX IF NOT EXISTS idx_runs_updated_at ON runs(updated_at DESC);
                CREATE INDEX IF NOT EXISTS idx_stream_status_status ON stream_status(status, last_seen_at DESC);
                CREATE INDEX IF NOT EXISTS idx_alerts_ack_status ON alerts(ack_status, created_at DESC);
                CREATE INDEX IF NOT EXISTS idx_command_requests_status ON command_requests(status, requested_at ASC);
                CREATE INDEX IF NOT EXISTS idx_command_audit_command_id ON command_audit(command_id, event_ts DESC);
                """
            )

    def get_offset(self, source_name: str) -> int:
        with self._connect() as conn:
            row = conn.execute(
                "SELECT last_line_number FROM read_model_offsets WHERE source_name = ?",
                (source_name,),
            ).fetchone()
        return int(row["last_line_number"]) if row else 0

    def update_offset(self, source_name: str, last_line_number: int) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO read_model_offsets(source_name, last_line_number)
                VALUES (?, ?)
                ON CONFLICT(source_name) DO UPDATE SET last_line_number=excluded.last_line_number
                """,
                (source_name, int(last_line_number)),
            )
            conn.commit()

    def upsert_run(self, record: RunRecord) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO runs(
                    run_id, trace_id, env, mode, result, updated_at, last_summary_at, last_health_at, summary_json, health_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(run_id) DO UPDATE SET
                    trace_id=excluded.trace_id,
                    env=excluded.env,
                    mode=excluded.mode,
                    result=excluded.result,
                    updated_at=excluded.updated_at,
                    last_summary_at=excluded.last_summary_at,
                    last_health_at=excluded.last_health_at,
                    summary_json=excluded.summary_json,
                    health_json=excluded.health_json
                """,
                (
                    record.run_id,
                    record.trace_id,
                    record.env,
                    record.mode,
                    record.result,
                    record.updated_at,
                    record.last_summary_at,
                    record.last_health_at,
                    json.dumps(record.summary, ensure_ascii=False, sort_keys=True),
                    json.dumps(record.health, ensure_ascii=False, sort_keys=True),
                ),
            )
            conn.commit()

    def upsert_stream_status(self, record: StreamStatusRecord) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO stream_status(scope, venue, symbol, stream_type, status, run_id, last_seen_at, metrics_json)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(scope) DO UPDATE SET
                    venue=excluded.venue,
                    symbol=excluded.symbol,
                    stream_type=excluded.stream_type,
                    status=excluded.status,
                    run_id=excluded.run_id,
                    last_seen_at=excluded.last_seen_at,
                    metrics_json=excluded.metrics_json
                """,
                (
                    record.scope,
                    record.venue,
                    record.symbol,
                    record.stream_type,
                    record.status,
                    record.run_id,
                    record.updated_at,
                    json.dumps(record.metrics, ensure_ascii=False, sort_keys=True),
                ),
            )
            conn.commit()

    def insert_alert(self, record: AlertRecord) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                INSERT OR IGNORE INTO alerts(
                    alert_id, trace_id, env, mode, alert_type, severity, observed, threshold, recommended_action,
                    created_at, payload_json, ack_status, acked_at, acked_by
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    record.alert_id,
                    record.trace_id,
                    record.env,
                    record.mode,
                    record.alert_type,
                    record.severity,
                    float(record.observed),
                    int(record.threshold),
                    record.recommended_action,
                    record.created_at,
                    json.dumps(record.payload, ensure_ascii=False, sort_keys=True),
                    record.ack_status,
                    record.acked_at,
                    record.acked_by,
                ),
            )
            conn.commit()

    def ack_alert(self, alert_id: str, *, acked_by: str, acked_at: str) -> bool:
        with self._connect() as conn:
            cursor = conn.execute(
                """
                UPDATE alerts
                SET ack_status='acked', acked_at=?, acked_by=?
                WHERE alert_id=? AND ack_status='open'
                """,
                (acked_at, acked_by, alert_id),
            )
            conn.commit()
        return cursor.rowcount > 0

    def upsert_checkpoint_summary(self, record: CheckpointSummaryRecord) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO checkpoint_summary(
                    stream_key, checkpoint_path, recorded_at, checkpoint_last_event_ts,
                    cursor_kind, cursor_value, cursor_last_event_ts, cursor_seen_entry_count, metadata_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(stream_key) DO UPDATE SET
                    checkpoint_path=excluded.checkpoint_path,
                    recorded_at=excluded.recorded_at,
                    checkpoint_last_event_ts=excluded.checkpoint_last_event_ts,
                    cursor_kind=excluded.cursor_kind,
                    cursor_value=excluded.cursor_value,
                    cursor_last_event_ts=excluded.cursor_last_event_ts,
                    cursor_seen_entry_count=excluded.cursor_seen_entry_count,
                    metadata_json=excluded.metadata_json
                """,
                (
                    record.stream_key,
                    record.checkpoint_path,
                    record.recorded_at,
                    record.checkpoint_last_event_ts,
                    record.cursor_kind,
                    record.cursor_value,
                    record.cursor_last_event_ts,
                    int(record.cursor_seen_entry_count),
                    json.dumps(record.metadata, ensure_ascii=False, sort_keys=True),
                ),
            )
            conn.commit()

    def enqueue_command(self, record: CommandRequestRecord) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO command_requests(
                    command_id, command_type, scope, payload_json, requested_by, requested_at,
                    started_at, finished_at, status, result_summary, error_summary
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    record.command_id,
                    record.command_type,
                    record.scope,
                    json.dumps(record.payload, ensure_ascii=False, sort_keys=True),
                    record.requested_by,
                    record.requested_at,
                    record.started_at,
                    record.finished_at,
                    record.status,
                    record.result_summary,
                    record.error_summary,
                ),
            )
            conn.commit()

    def get_command(self, command_id: str) -> CommandRequestRecord | None:
        with self._connect() as conn:
            row = conn.execute("SELECT * FROM command_requests WHERE command_id = ?", (command_id,)).fetchone()
        return self._command_from_row(row) if row else None

    def claim_next_command(self, *, worker_id: str, started_at: str) -> CommandRequestRecord | None:
        del worker_id
        with self._connect() as conn:
            row = conn.execute(
                """
                SELECT * FROM command_requests
                WHERE status='pending'
                ORDER BY requested_at ASC
                LIMIT 1
                """
            ).fetchone()
            if row is None:
                return None
            conn.execute(
                "UPDATE command_requests SET status='running', started_at=? WHERE command_id=? AND status='pending'",
                (started_at, row["command_id"]),
            )
            conn.commit()
        return self.get_command(str(row["command_id"]))

    def complete_command(
        self,
        command_id: str,
        *,
        status: str,
        finished_at: str,
        result_summary: str | None,
        error_summary: str | None,
    ) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                UPDATE command_requests
                SET status=?, finished_at=?, result_summary=?, error_summary=?
                WHERE command_id=?
                """,
                (status, finished_at, result_summary, error_summary, command_id),
            )
            conn.commit()

    def append_command_audit(self, record: CommandAuditRecord) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                INSERT INTO command_audit(command_id, event_ts, event_type, payload_json)
                VALUES (?, ?, ?, ?)
                """,
                (
                    record.command_id,
                    record.event_ts,
                    record.event_type,
                    json.dumps(record.payload, ensure_ascii=False, sort_keys=True),
                ),
            )
            conn.commit()

    def list_runs(self, *, limit: int = 50) -> list[RunRecord]:
        with self._connect() as conn:
            rows = conn.execute("SELECT * FROM runs ORDER BY updated_at DESC LIMIT ?", (int(limit),)).fetchall()
        return [self._run_from_row(row) for row in rows]

    def get_run(self, run_id: str) -> RunRecord | None:
        with self._connect() as conn:
            row = conn.execute("SELECT * FROM runs WHERE run_id=?", (run_id,)).fetchone()
        return self._run_from_row(row) if row else None

    def list_streams(self) -> list[StreamStatusRecord]:
        with self._connect() as conn:
            rows = conn.execute("SELECT * FROM stream_status ORDER BY status DESC, last_seen_at DESC").fetchall()
        return [self._stream_from_row(row) for row in rows]

    def get_stream(self, scope: str) -> StreamStatusRecord | None:
        with self._connect() as conn:
            row = conn.execute("SELECT * FROM stream_status WHERE scope=?", (scope,)).fetchone()
        return self._stream_from_row(row) if row else None

    def list_alerts(self, *, include_acked: bool = True) -> list[AlertRecord]:
        query = "SELECT * FROM alerts"
        if not include_acked:
            query += " WHERE ack_status='open'"
        query += " ORDER BY created_at DESC"
        with self._connect() as conn:
            rows = conn.execute(query).fetchall()
        return [self._alert_from_row(row) for row in rows]

    def list_checkpoints(self) -> list[CheckpointSummaryRecord]:
        with self._connect() as conn:
            rows = conn.execute("SELECT * FROM checkpoint_summary ORDER BY recorded_at DESC").fetchall()
        return [self._checkpoint_from_row(row) for row in rows]

    def list_command_audit(self, *, limit: int = 100) -> list[CommandAuditRecord]:
        with self._connect() as conn:
            rows = conn.execute(
                "SELECT command_id, event_ts, event_type, payload_json FROM command_audit ORDER BY event_ts DESC, id DESC LIMIT ?",
                (int(limit),),
            ).fetchall()
        return [
            CommandAuditRecord(
                command_id=str(row["command_id"]),
                event_ts=str(row["event_ts"]),
                event_type=str(row["event_type"]),
                payload=_json_load(row["payload_json"]),
            )
            for row in rows
        ]

    def overview(self) -> OverviewSnapshot:
        runs = self.list_runs(limit=10)
        streams = self.list_streams()
        alerts_open = self.list_alerts(include_acked=False)
        checkpoints = self.list_checkpoints()
        with self._connect() as conn:
            command_count = conn.execute(
                "SELECT COUNT(*) AS total FROM command_requests WHERE status IN ('pending', 'running')"
            ).fetchone()
        degraded = tuple(stream for stream in streams if stream.status != "ok")
        return OverviewSnapshot(
            total_runs=len(runs),
            recent_runs=tuple(runs),
            streams_total=len(streams),
            streams_degraded=degraded,
            alerts_open=tuple(alerts_open[:10]),
            checkpoints_total=len(checkpoints),
            commands_pending=int(command_count["total"]) if command_count else 0,
        )

    def _run_from_row(self, row: sqlite3.Row) -> RunRecord:
        return RunRecord(
            run_id=str(row["run_id"]),
            trace_id=str(row["trace_id"]),
            env=str(row["env"]),
            mode=str(row["mode"]),
            result=str(row["result"]),
            updated_at=str(row["updated_at"]),
            last_summary_at=row["last_summary_at"],
            last_health_at=row["last_health_at"],
            summary=_json_load(row["summary_json"]),
            health=_json_load(row["health_json"]),
        )

    def _stream_from_row(self, row: sqlite3.Row) -> StreamStatusRecord:
        return StreamStatusRecord(
            scope=str(row["scope"]),
            venue=str(row["venue"]),
            symbol=str(row["symbol"]),
            stream_type=str(row["stream_type"]),
            status=str(row["status"]),
            run_id=row["run_id"],
            updated_at=str(row["last_seen_at"]),
            metrics=_json_load(row["metrics_json"]),
        )

    def _alert_from_row(self, row: sqlite3.Row) -> AlertRecord:
        return AlertRecord(
            alert_id=str(row["alert_id"]),
            trace_id=row["trace_id"],
            env=row["env"],
            mode=row["mode"],
            alert_type=str(row["alert_type"]),
            severity=str(row["severity"]),
            observed=float(row["observed"]),
            threshold=int(row["threshold"]),
            recommended_action=str(row["recommended_action"]),
            created_at=str(row["created_at"]),
            payload=_json_load(row["payload_json"]),
            ack_status=str(row["ack_status"]),
            acked_at=row["acked_at"],
            acked_by=row["acked_by"],
        )

    def _checkpoint_from_row(self, row: sqlite3.Row) -> CheckpointSummaryRecord:
        return CheckpointSummaryRecord(
            stream_key=str(row["stream_key"]),
            checkpoint_path=str(row["checkpoint_path"]),
            recorded_at=str(row["recorded_at"]),
            checkpoint_last_event_ts=row["checkpoint_last_event_ts"],
            cursor_kind=row["cursor_kind"],
            cursor_value=row["cursor_value"],
            cursor_last_event_ts=row["cursor_last_event_ts"],
            cursor_seen_entry_count=int(row["cursor_seen_entry_count"]),
            metadata=_json_load(row["metadata_json"]),
        )

    def _command_from_row(self, row: sqlite3.Row) -> CommandRequestRecord:
        return CommandRequestRecord(
            command_id=str(row["command_id"]),
            command_type=str(row["command_type"]),
            scope=str(row["scope"]),
            payload=_json_load(row["payload_json"]),
            requested_by=str(row["requested_by"]),
            requested_at=str(row["requested_at"]),
            status=str(row["status"]),
            started_at=row["started_at"],
            finished_at=row["finished_at"],
            result_summary=row["result_summary"],
            error_summary=row["error_summary"],
        )
