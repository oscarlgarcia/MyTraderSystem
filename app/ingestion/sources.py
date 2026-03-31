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
from app.ingestion.client import (
    DEFAULT_STREAM_TYPES,
    build_ws_url,
    normalize_kline_typed,
    parse_message_parts,
    parse_typed_message,
)
from app.ingestion.errors import IngestionError, classify_connector_error
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
    handoff_bootstrap_rows: int = 0
    handoff_overlap_dropped: int = 0
    handoff_inconsistent: int = 0


@dataclass(frozen=True, slots=True)
class HeartbeatPolicy:
    recv_timeout_seconds: float
    ping_interval_seconds: float
    ping_timeout_seconds: float
    inactivity_timeout_seconds: float
    expected_idle_seconds_by_stream: dict[str, float]


def heartbeat_policy_for_streams(stream_types: Iterable[str]) -> HeartbeatPolicy:
    expected_by_stream = {
        "trade": 30.0,
        "kline": 90.0,
        "book": 10.0,
    }
    stream_list = tuple(stream_types) or DEFAULT_STREAM_TYPES
    relevant = {
        stream_type: expected_by_stream.get(stream_type, 30.0)
        for stream_type in stream_list
    }
    inactivity_timeout = max(relevant.values())
    ping_interval = min(10.0, max(2.0, inactivity_timeout / 3.0))
    ping_timeout = min(5.0, max(1.0, ping_interval / 2.0))
    return HeartbeatPolicy(
        recv_timeout_seconds=1.0,
        ping_interval_seconds=ping_interval,
        ping_timeout_seconds=ping_timeout,
        inactivity_timeout_seconds=inactivity_timeout,
        expected_idle_seconds_by_stream=relevant,
    )


def _ws_stream(
    url: str,
    end_time: float | None = None,
    *,
    heartbeat: HeartbeatPolicy | None = None,
    monotonic_fn: Callable[[], float] = time.monotonic,
    connect_fn: Callable[[str], object] = connect,
) -> Iterable[str]:
    heartbeat_policy = heartbeat or heartbeat_policy_for_streams(DEFAULT_STREAM_TYPES)
    with connect_fn(url) as ws:
        last_activity = monotonic_fn()
        last_ping = last_activity
        while True:
            if end_time and time.time() >= end_time:
                break
            try:
                raw = ws.recv(timeout=heartbeat_policy.recv_timeout_seconds)
            except StopIteration:
                break
            except TimeoutError:
                now = monotonic_fn()
                idle_seconds = now - last_activity
                if idle_seconds >= heartbeat_policy.ping_interval_seconds and now - last_ping >= heartbeat_policy.ping_interval_seconds:
                    pong = ws.ping()
                    if not pong.wait(timeout=heartbeat_policy.ping_timeout_seconds):
                        raise TimeoutError(
                            f"websocket heartbeat timeout after {idle_seconds:.1f}s idle"
                        )
                    last_activity = monotonic_fn()
                    last_ping = last_activity
                    continue
                if idle_seconds >= heartbeat_policy.inactivity_timeout_seconds:
                    raise TimeoutError(
                        f"websocket inactivity watchdog exceeded {heartbeat_policy.inactivity_timeout_seconds:.1f}s"
                    )
                continue
            last_activity = monotonic_fn()
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
    ws_stream: Callable[..., Iterable[object]] = _ws_stream
    http_get: Callable[..., httpx.Response] = httpx.get
    error_sink: ErrorSink = field(default_factory=NullErrorSink)
    raw_sink: RawSink = field(default_factory=NullRawSink)
    stream_types: tuple[str, ...] = DEFAULT_STREAM_TYPES
    heartbeat_policy: HeartbeatPolicy | None = None
    ws_connect_fn: Callable[[str], object] = connect
    monotonic_fn: Callable[[], float] = time.monotonic
    stats: SourceStats = field(default_factory=SourceStats)

    def __post_init__(self) -> None:
        if isinstance(self.error_sink, NullErrorSink):
            self.error_sink = JsonlErrorSink(
                Path(self.cfg.data_dir) / "errors" / "ingestion-dlq.jsonl"
            )
        if self.heartbeat_policy is None:
            self.heartbeat_policy = heartbeat_policy_for_streams(self.stream_types)

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
        url = build_ws_url(self.cfg.ws_base, self.cfg.symbols, self.stream_types)
        try:
            if self.ws_stream is _ws_stream:
                stream_iter = self.ws_stream(
                    url,
                    end_time=end_time,
                    heartbeat=self.heartbeat_policy,
                    monotonic_fn=self.monotonic_fn,
                    connect_fn=self.ws_connect_fn,
                )
            else:
                stream_iter = self.ws_stream(url, end_time=end_time)
            for item in stream_iter:
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
            raise classify_connector_error(exc) from exc

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
            raise classify_connector_error(exc) from exc
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
