from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, Protocol

from app.common.dto import MarketEvent
from app.ingestion.errors import IngestionError
from app.ingestion.storage import ParquetWriter


class EventSink(Protocol):
    def add(self, event: MarketEvent | Iterable[MarketEvent]) -> None: ...
    def close(self) -> None: ...


class ErrorSink(Protocol):
    def write(self, raw_message: object, error: IngestionError, context: dict[str, object] | None = None) -> None: ...


class NullErrorSink:
    def write(self, raw_message: object, error: IngestionError, context: dict[str, object] | None = None) -> None:
        del raw_message, error, context


@dataclass
class JsonlErrorSink:
    path: Path

    def write(self, raw_message: object, error: IngestionError, context: dict[str, object] | None = None) -> None:
        self.path.parent.mkdir(parents=True, exist_ok=True)
        payload = {
            "ts": datetime.now(timezone.utc).isoformat(),
            "error_category": error.category,
            "error_severity": error.severity,
            "error_message": str(error),
            "raw_message": raw_message,
            "context": context or {},
        }
        with self.path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(payload, default=str) + "\n")


class ParquetEventSink:
    def __init__(self, writer: ParquetWriter):
        self.writer = writer

    def add(self, event: MarketEvent | Iterable[MarketEvent]) -> None:
        self.writer.add(event)

    def close(self) -> None:
        self.writer.flush()
