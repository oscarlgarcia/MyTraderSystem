"""
Source contracts and concrete ingestion sources.

Keep this layer small: it only knows how to fetch/stream normalized MarketEvent data.
"""

from __future__ import annotations

import json
import logging
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Callable, Iterable, Optional, Protocol

import httpx
from websockets.sync.client import connect

from app.common import validator
from app.common.dto import MarketEvent
from app.config import AppConfig
from app.ingestion.client import build_ws_url, normalize_kline, parse_message
from app.ingestion.errors import IngestionError, classify_error
from app.ingestion.sinks import ErrorSink, JsonlErrorSink, NullErrorSink


class Source(Protocol):
    def stream(self, end_time: float | None = None) -> Iterable[MarketEvent]: ...
    def snapshot(self) -> Optional[Iterable[MarketEvent]]: ...


@dataclass
class SourceStats:
    source_events_in: int = 0
    events_valid: int = 0
    events_invalid: int = 0
    snapshot_runs: int = 0
    snapshot_rows: int = 0
    rejected_payloads: int = 0
    error_sink_failures: int = 0


def _ws_stream(url: str, end_time: float | None = None) -> Iterable[str]:
    with connect(url) as ws:
        while True:
            if end_time and time.time() >= end_time:
                break
            try:
                raw = ws.recv(timeout=1)
            except TimeoutError:
                continue
            yield raw


def source_snapshot_fn(source: Source) -> Callable[[], Iterable[MarketEvent]]:
    def snapshot() -> Iterable[MarketEvent]:
        events = source.snapshot()
        if events is None:
            return []
        return events

    return snapshot


@dataclass
class BinanceSource:
    cfg: AppConfig
    ws_stream: Callable[[str, float | None], Iterable[object]] = _ws_stream
    http_get: Callable[..., httpx.Response] = httpx.get
    error_sink: ErrorSink = field(default_factory=NullErrorSink)
    stats: SourceStats = field(default_factory=SourceStats)

    def __post_init__(self) -> None:
        if isinstance(self.error_sink, NullErrorSink):
            self.error_sink = JsonlErrorSink(
                Path(self.cfg.data_dir) / "errors" / "ingestion-dlq.jsonl"
            )

    def _record_rejected(self, raw_message: object, error: IngestionError, *, context: dict[str, object]) -> None:
        self.stats.events_invalid += 1
        self.stats.rejected_payloads += 1
        try:
            self.error_sink.write(raw_message, error, context=context)
        except Exception as sink_exc:
            self.stats.error_sink_failures += 1
            logging.getLogger("ingest.source").warning(
                "error sink failed",
                extra={
                    "error": str(sink_exc),
                    "original_error": str(error),
                    "error_category": error.category,
                    "error_severity": error.severity,
                },
            )

    def stream(self, end_time: float | None = None) -> Iterable[MarketEvent]:
        url = build_ws_url(self.cfg.ws_base, self.cfg.symbols)
        try:
            for item in self.ws_stream(url, end_time=end_time):
                self.stats.source_events_in += 1
                if isinstance(item, MarketEvent):
                    validator.validate_market_payload(item.symbol, item.event_ts, item.price, item.size)
                    self.stats.events_valid += 1
                    yield item
                    continue
                try:
                    event = parse_message(str(item))
                    self.stats.events_valid += 1
                    yield event
                except (json.JSONDecodeError, KeyError) as exc:
                    self._record_rejected(
                        item,
                        IngestionError("parse", "permanent", str(exc)),
                        context={"stage": "stream", "url": url},
                    )
                    continue
                except ValueError as exc:
                    self._record_rejected(
                        item,
                        IngestionError("validation", "permanent", str(exc)),
                        context={"stage": "stream", "url": url},
                    )
                    continue
        except IngestionError:
            raise
        except Exception as exc:
            raise classify_error(exc, default_category="source") from exc

    def snapshot(self) -> Iterable[MarketEvent]:
        events: list[MarketEvent] = []
        try:
            self.stats.snapshot_runs += 1
            for symbol in self.cfg.symbols:
                url = f"{self.cfg.rest_base.rstrip('/')}/api/v3/klines"
                resp = self.http_get(url, params={"symbol": symbol, "interval": "1m", "limit": 5}, timeout=5.0)
                resp.raise_for_status()
                for row in resp.json():
                    self.stats.source_events_in += 1
                    payload = {"s": symbol, "E": int(row[6]), "k": {"c": row[4], "q": row[5]}}
                    try:
                        event = normalize_kline(payload)
                        events.append(event)
                        self.stats.events_valid += 1
                        self.stats.snapshot_rows += 1
                    except ValueError as exc:
                        self._record_rejected(
                            payload,
                            IngestionError("validation", "permanent", str(exc)),
                            context={"stage": "snapshot", "symbol": symbol},
                        )
                        continue
        except Exception as exc:
            raise classify_error(exc, default_category="source") from exc
        return events


@dataclass
class StaticSource:
    events: list[MarketEvent] = field(default_factory=list)
    snapshot_events: Optional[list[MarketEvent]] = None
    stats: SourceStats = field(default_factory=SourceStats)

    def stream(self, end_time: float | None = None) -> Iterable[MarketEvent]:
        del end_time
        for event in self.events:
            self.stats.source_events_in += 1
            self.stats.events_valid += 1
            yield event

    def snapshot(self) -> Optional[Iterable[MarketEvent]]:
        self.stats.snapshot_runs += 1
        if self.snapshot_events is None:
            return None
        self.stats.source_events_in += len(self.snapshot_events)
        self.stats.events_valid += len(self.snapshot_events)
        self.stats.snapshot_rows += len(self.snapshot_events)
        return self.snapshot_events
