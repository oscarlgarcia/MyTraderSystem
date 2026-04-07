from __future__ import annotations

from abc import ABC, abstractmethod

from app.controlplane.models import (
    AlertRecord,
    CheckpointSummaryRecord,
    CommandAuditRecord,
    CommandRequestRecord,
    OverviewSnapshot,
    RunRecord,
    StreamStatusRecord,
)


class ControlPlaneStore(ABC):
    @abstractmethod
    def initialize(self) -> None: ...

    @abstractmethod
    def get_offset(self, source_name: str) -> int: ...

    @abstractmethod
    def update_offset(self, source_name: str, last_line_number: int) -> None: ...

    @abstractmethod
    def upsert_run(self, record: RunRecord) -> None: ...

    @abstractmethod
    def upsert_stream_status(self, record: StreamStatusRecord) -> None: ...

    @abstractmethod
    def insert_alert(self, record: AlertRecord) -> None: ...

    @abstractmethod
    def ack_alert(self, alert_id: str, *, acked_by: str, acked_at: str) -> bool: ...

    @abstractmethod
    def upsert_checkpoint_summary(self, record: CheckpointSummaryRecord) -> None: ...

    @abstractmethod
    def enqueue_command(self, record: CommandRequestRecord) -> None: ...

    @abstractmethod
    def get_command(self, command_id: str) -> CommandRequestRecord | None: ...

    @abstractmethod
    def list_commands(self, *, limit: int = 20) -> list[CommandRequestRecord]: ...

    @abstractmethod
    def claim_next_command(self, *, worker_id: str, started_at: str) -> CommandRequestRecord | None: ...

    @abstractmethod
    def complete_command(
        self,
        command_id: str,
        *,
        status: str,
        finished_at: str,
        result_summary: str | None,
        error_summary: str | None,
    ) -> None: ...

    @abstractmethod
    def append_command_audit(self, record: CommandAuditRecord) -> None: ...

    @abstractmethod
    def list_runs(self, *, limit: int = 50) -> list[RunRecord]: ...

    @abstractmethod
    def get_run(self, run_id: str) -> RunRecord | None: ...

    @abstractmethod
    def list_streams(self) -> list[StreamStatusRecord]: ...

    @abstractmethod
    def get_stream(self, scope: str) -> StreamStatusRecord | None: ...

    @abstractmethod
    def list_alerts(self, *, include_acked: bool = True) -> list[AlertRecord]: ...

    @abstractmethod
    def list_checkpoints(self) -> list[CheckpointSummaryRecord]: ...

    @abstractmethod
    def list_command_audit(self, *, limit: int = 100) -> list[CommandAuditRecord]: ...

    @abstractmethod
    def overview(self) -> OverviewSnapshot: ...


class PostgresControlPlaneStore(ControlPlaneStore):
    """
    Reserved interface stub for future PostgreSQL support.
    """

    def __init__(self, dsn: str) -> None:
        self.dsn = dsn
        raise NotImplementedError("PostgreSQL control-plane backend is reserved for a future version")

    def initialize(self) -> None:
        raise NotImplementedError

    def get_offset(self, source_name: str) -> int:
        raise NotImplementedError

    def update_offset(self, source_name: str, last_line_number: int) -> None:
        raise NotImplementedError

    def upsert_run(self, record: RunRecord) -> None:
        raise NotImplementedError

    def upsert_stream_status(self, record: StreamStatusRecord) -> None:
        raise NotImplementedError

    def insert_alert(self, record: AlertRecord) -> None:
        raise NotImplementedError

    def ack_alert(self, alert_id: str, *, acked_by: str, acked_at: str) -> bool:
        raise NotImplementedError

    def upsert_checkpoint_summary(self, record: CheckpointSummaryRecord) -> None:
        raise NotImplementedError

    def enqueue_command(self, record: CommandRequestRecord) -> None:
        raise NotImplementedError

    def get_command(self, command_id: str) -> CommandRequestRecord | None:
        raise NotImplementedError

    def list_commands(self, *, limit: int = 20) -> list[CommandRequestRecord]:
        raise NotImplementedError

    def claim_next_command(self, *, worker_id: str, started_at: str) -> CommandRequestRecord | None:
        raise NotImplementedError

    def complete_command(
        self,
        command_id: str,
        *,
        status: str,
        finished_at: str,
        result_summary: str | None,
        error_summary: str | None,
    ) -> None:
        raise NotImplementedError

    def append_command_audit(self, record: CommandAuditRecord) -> None:
        raise NotImplementedError

    def list_runs(self, *, limit: int = 50) -> list[RunRecord]:
        raise NotImplementedError

    def get_run(self, run_id: str) -> RunRecord | None:
        raise NotImplementedError

    def list_streams(self) -> list[StreamStatusRecord]:
        raise NotImplementedError

    def get_stream(self, scope: str) -> StreamStatusRecord | None:
        raise NotImplementedError

    def list_alerts(self, *, include_acked: bool = True) -> list[AlertRecord]:
        raise NotImplementedError

    def list_checkpoints(self) -> list[CheckpointSummaryRecord]:
        raise NotImplementedError

    def list_command_audit(self, *, limit: int = 100) -> list[CommandAuditRecord]:
        raise NotImplementedError

    def overview(self) -> OverviewSnapshot:
        raise NotImplementedError
