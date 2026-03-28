from __future__ import annotations

from typing import Protocol

from app.common.dto import MarketEvent
from app.ingestion.storage import ParquetWriter


class EventSink(Protocol):
    def add(self, event: MarketEvent) -> None: ...
    def close(self) -> None: ...


class ParquetEventSink:
    def __init__(self, writer: ParquetWriter):
        self.writer = writer

    def add(self, event: MarketEvent) -> None:
        self.writer.add(event)

    def close(self) -> None:
        self.writer.flush()
