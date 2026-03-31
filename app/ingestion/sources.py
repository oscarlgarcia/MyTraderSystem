"""
Source contracts and concrete ingestion sources.

Keep this layer small: it only knows how to fetch/stream normalized MarketEvent data.
"""

from __future__ import annotations

import json
import logging
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Callable, Iterable, Optional, Protocol

import httpx
from websockets.sync.client import connect

from app.common.dto import MarketEvent
from app.config import AppConfig
from app.ingestion.client import build_ws_url, normalize_kline_typed, parse_message_parts, parse_typed_message
from app.ingestion.errors import IngestionError, classify_error
from app.ingestion.sinks import ErrorSink, JsonlErrorSink, NullErrorSink
from app.marketdata.models import BaseMarketEvent, IngestionEvent
from app.marketdata.raw_sink import NullRawSink, RawRecord, RawSink
from app.marketdata.validators import validate_ingestion_event, validate_kline_payload
from app.observability.logger import get_trace_id


class Source(Protocol):
    def stream(self, end_time: float | None = None) -> Iterable[IngestionEvent]: ...
    def snapshot(self) -> Optional[Iterable[IngestionEvent]]: ...


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


def source_snapshot_fn(source: Source) -> Callable[[], Iterable[IngestionEvent]]:
    def snapshot() -> Iterable[IngestionEvent]:
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
    raw_sink: RawSink = field(default_factory=NullRawSink)
    stats: SourceStats = field(default_factory=SourceStats)

    def __post_init__(self) -> None:
        if isinstance(self.error_sink, NullErrorSink):
            self.error_sink = JsonlErrorSink(
                Path(self.cfg.data_dir) / "errors" / "ingestion-dlq.jsonl"
            )

    def _write_raw_record(
        self,
        *,
        payload: object,
        event: IngestionEvent,
        stream_type: str,
        receive_ts: datetime,
    ) -> None:
        if isinstance(self.raw_sink, NullRawSink):
            return
        record = RawRecord(
            payload=payload,
            venue=getattr(event, "venue", "BINANCE"),
            stream_type=stream_type,
            symbol=event.symbol,
            exchange_ts=event.event_ts,
            receive_ts=receive_ts,
            trace_id=get_trace_id(),
            source_id=getattr(event, "source_id", None),
        )
        try:
            self.raw_sink.write(record)
        except Exception as exc:
            raise IngestionError("sink", "transient", f"raw sink write failed: {exc}") from exc

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

    def stream(self, end_time: float | None = None) -> Iterable[IngestionEvent]:
        url = build_ws_url(self.cfg.ws_base, self.cfg.symbols)
        try:
            for item in self.ws_stream(url, end_time=end_time):
                self.stats.source_events_in += 1
                if isinstance(item, (MarketEvent, BaseMarketEvent)):
                    validate_ingestion_event(item)
                    self.stats.events_valid += 1
                    yield item
                    continue
                try:
                    receive_ts = datetime.now(timezone.utc)
                    payload, data, _stream, event_type = parse_message_parts(str(item))
                    event = parse_typed_message(
                        str(item),
                        receive_ts=receive_ts,
                        process_ts=receive_ts,
                        allowed_event_types=("trade", "kline"),
                    )
                    validate_ingestion_event(event)
                    self._write_raw_record(
                        payload=payload,
                        event=event,
                        stream_type=event_type,
                        receive_ts=receive_ts,
                    )
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

    def snapshot(self) -> Iterable[IngestionEvent]:
        events: list[IngestionEvent] = []
        try:
            self.stats.snapshot_runs += 1
            for symbol in self.cfg.symbols:
                url = f"{self.cfg.rest_base.rstrip('/')}/api/v3/klines"
                resp = self.http_get(url, params={"symbol": symbol, "interval": "1m", "limit": 5}, timeout=5.0)
                resp.raise_for_status()
                receive_ts = datetime.now(timezone.utc)
                for row in resp.json():
                    self.stats.source_events_in += 1
                    payload = {
                        "s": symbol,
                        "E": int(row[6]),
                        "k": {
                            "t": int(row[0]),
                            "T": int(row[6]),
                            "o": row[1],
                            "h": row[2],
                            "l": row[3],
                            "c": row[4],
                            "q": row[5],
                        },
                    }
                    try:
                        validate_kline_payload(payload)
                        event = normalize_kline_typed(
                            payload,
                            receive_ts=receive_ts,
                            process_ts=receive_ts,
                        )
                        validate_ingestion_event(event)
                        self._write_raw_record(
                            payload=payload,
                            event=event,
                            stream_type="kline",
                            receive_ts=receive_ts,
                        )
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
    events: list[IngestionEvent] = field(default_factory=list)
    snapshot_events: Optional[list[IngestionEvent]] = None
    stats: SourceStats = field(default_factory=SourceStats)

    def stream(self, end_time: float | None = None) -> Iterable[IngestionEvent]:
        del end_time
        for event in self.events:
            validate_ingestion_event(event)
            self.stats.source_events_in += 1
            self.stats.events_valid += 1
            yield event

    def snapshot(self) -> Optional[Iterable[IngestionEvent]]:
        self.stats.snapshot_runs += 1
        if self.snapshot_events is None:
            return None
        for event in self.snapshot_events:
            validate_ingestion_event(event)
        self.stats.source_events_in += len(self.snapshot_events)
        self.stats.events_valid += len(self.snapshot_events)
        self.stats.snapshot_rows += len(self.snapshot_events)
        return self.snapshot_events
