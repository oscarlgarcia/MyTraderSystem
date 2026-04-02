from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Protocol

from app.ingestion.errors import IngestionError
from app.ingestion.storage import ParquetWriter
from app.marketdata.errors import MarketdataIncidentError, SchemaDriftError
from app.marketdata.models import IngestionEvent


class EventSink(Protocol):
    def add(self, event: IngestionEvent | Iterable[IngestionEvent]) -> None: ...
    def close(self) -> None: ...


class ErrorSink(Protocol):
    def write(self, raw_message: object, error: IngestionError, context: dict[str, object] | None = None) -> None: ...


class NullErrorSink:
    def write(self, raw_message: object, error: IngestionError, context: dict[str, object] | None = None) -> None:
        del raw_message, error, context


@dataclass
class JsonlErrorSink:
    path: Path
    schema_drift_path: Path | None = None

    def write(self, raw_message: object, error: IngestionError, context: dict[str, object] | None = None) -> None:
        target_path = self._target_path(error, context=context)
        target_path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "ts": datetime.now(timezone.utc).isoformat(),
            "error_category": error.category,
            "error_severity": error.severity,
            "error_type": type(error).__name__,
            "error_message": str(error),
            "raw_message": raw_message,
            "context": context or {},
        }
        if isinstance(error, MarketdataIncidentError):
            payload["incident"] = error.as_context()
        if isinstance(error, SchemaDriftError):
            payload["schema_drift"] = error.as_context()
        with target_path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(payload, default=str) + "\n")

    def _target_path(self, error: IngestionError, *, context: dict[str, object] | None) -> Path:
        if isinstance(error, SchemaDriftError):
            if self.schema_drift_path is not None:
                return self.schema_drift_path
            return self.path.parent / "schema-drift-quarantine.jsonl"
        if context is not None:
            quarantine_path = context.get("quarantine_path")
            if quarantine_path:
                return Path(str(quarantine_path))
        return self.path


class ParquetEventSink:
    def __init__(self, writer: ParquetWriter):
        self.writer = writer

    def add(self, event: IngestionEvent | Iterable[IngestionEvent]) -> None:
        self.writer.add(event)

    def close(self) -> None:
        self.writer.flush()

    @property
    def accepted_count(self) -> int:
        return self.writer.accepted_events

    @property
    def persisted_count(self) -> int:
        return self.writer.persisted_events

    @property
    def buffered_count(self) -> int:
        return self.writer.buffered_events

    @property
    def write_latency_seconds(self) -> float:
        return self.writer.max_write_latency_seconds

    @property
    def last_write_latency_seconds(self) -> float:
        return self.writer.last_write_latency_seconds

    @property
    def stream_write_metrics(self) -> dict[str, dict[str, object]]:
        return self.writer.stream_write_metrics


class MirroredEventSink:
    def __init__(self, primary: EventSink, shadow: EventSink):
        self.primary = primary
        self.shadow = shadow

    def add(self, event: IngestionEvent | Iterable[IngestionEvent]) -> None:
        self.primary.add(event)
        self.shadow.add(event)

    def close(self) -> None:
        try:
            self.primary.close()
        except Exception:
            try:
                self.shadow.close()
            except Exception:
                pass
            raise
        self.shadow.close()

    @property
    def persisted_count(self) -> int:
        return int(getattr(self.primary, "persisted_count", 0))

    @property
    def shadow_persisted_count(self) -> int:
        return int(getattr(self.shadow, "persisted_count", 0))

    @property
    def write_latency_seconds(self) -> float:
        return float(getattr(self.primary, "write_latency_seconds", 0.0))

    @property
    def shadow_write_latency_seconds(self) -> float:
        return float(getattr(self.shadow, "write_latency_seconds", 0.0))

    @property
    def stream_write_metrics(self) -> dict[str, dict[str, object]]:
        return dict(getattr(self.primary, "stream_write_metrics", {}))

    @property
    def shadow_stream_write_metrics(self) -> dict[str, dict[str, object]]:
        return dict(getattr(self.shadow, "stream_write_metrics", {}))
