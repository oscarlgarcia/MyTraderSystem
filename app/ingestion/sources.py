"""
Source contracts and concrete ingestion sources.

Keep this layer small: it only knows how to fetch/stream normalized typed ingestion events.
"""

from __future__ import annotations

import json
import inspect
import logging
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Iterable, Optional, Protocol

import httpx
from websockets.sync.client import connect

from app.common.dto import MarketEvent
from app.config import AppConfig
from app.ingestion.circuit_breaker import CircuitBreaker
from app.ingestion.client import (
    DEFAULT_STREAM_TYPES,
    build_ws_url,
    parse_message_parts,
)
from app.ingestion.errors import IngestionError, classify_connector_error
from app.ingestion.sinks import ErrorSink, JsonlErrorSink, NullErrorSink
from app.marketdata.connectors.binance import BINANCE_FEED_NORMALIZERS, normalize_binance_event, snapshot_payload_from_row
from app.marketdata.models import BaseMarketEvent, IngestionEvent
from app.marketdata.instruments import ensure_default_instruments, persist_instrument_catalog_snapshot
from app.marketdata.raw_sink import NullRawSink, RawRecord, RawSink
from app.marketdata.recovery import RecoveryRequest
from app.marketdata.validators import validate_ingestion_event
from app.observability.alerts import emit_operational_alert, should_emit_threshold_alert
from app.observability.logger import get_trace_id


def _identity_delay(delay: float) -> float:
    return delay


class Source(Protocol):
    def stream(self, end_time: float | None = None) -> Iterable[IngestionEvent]: ...
    def snapshot(self, request: RecoveryRequest | None = None) -> Optional[Iterable[IngestionEvent]]: ...


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
    stream_metrics: dict[str, dict[str, object]] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class HeartbeatPolicy:
    recv_timeout_seconds: float
    ping_interval_seconds: float
    ping_timeout_seconds: float
    inactivity_timeout_seconds: float
    expected_idle_seconds_by_stream: dict[str, float]


def stream_metric_label(*, venue: str, symbol: str, stream_type: str) -> str:
    return f"{str(venue).upper()}:{symbol}:{stream_type}"


def _ensure_stream_metric(
    stats: SourceStats,
    *,
    venue: str,
    symbol: str,
    stream_type: str,
) -> dict[str, object]:
    label = stream_metric_label(venue=venue, symbol=symbol, stream_type=stream_type)
    metric = stats.stream_metrics.setdefault(
        label,
        {
            "venue": str(venue).upper(),
            "symbol": symbol,
            "stream_type": stream_type,
            "messages_in_total": 0,
            "messages_invalid_total": 0,
            "invalid_timestamp_total": 0,
            "duplicates_total": 0,
            "gaps_total": 0,
            "gap_irreparable_total": 0,
            "reconnects_total": 0,
            "heartbeat_missed_total": 0,
            "buffer_dropped_total": 0,
            "raw_write_latency": 0.0,
            "normalized_write_latency": 0.0,
            "exchange_receive_skew_seconds": 0.0,
            "receive_process_skew_seconds": 0.0,
            "recovery_window_rows_requested": 0,
            "recovery_window_rows_received": 0,
            "recovery_exactness_violation_total": 0,
        },
    )
    return metric


def _event_stream_context(event: IngestionEvent) -> tuple[str, str, str]:
    venue = getattr(event, "venue", "BINANCE")
    return str(venue).upper(), event.symbol, event.source


def _raw_message_context(raw_message: object) -> tuple[str, str, str]:
    venue = "BINANCE"
    symbol = "UNKNOWN"
    stream_type = "unknown"
    try:
        payload, data, stream_name, event_type = parse_message_parts(str(raw_message))
        del payload, stream_name
        symbol = str(data.get("s", symbol)).upper()
        stream_type = str(event_type)
    except Exception:
        return venue, symbol, stream_type
    return venue, symbol, stream_type


def _is_timestamp_validation_error(message: str) -> bool:
    lowered = str(message).lower()
    return (
        "timestamp" in lowered
        or lowered.startswith("e is too far in the future")
        or "k.t" in lowered
        or "receive_ts" in lowered
        or "process_ts" in lowered
        or "close_ts" in lowered
        or "open_ts" in lowered
    )


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


def source_snapshot_fn(source: Source) -> Callable[..., Iterable[IngestionEvent]]:
    def snapshot(*, request: RecoveryRequest | None = None) -> Iterable[IngestionEvent]:
        try:
            parameters = inspect.signature(source.snapshot).parameters
        except (TypeError, ValueError):
            parameters = {}
        if "request" in parameters or any(param.kind is inspect.Parameter.VAR_KEYWORD for param in parameters.values()):
            events = source.snapshot(request=request)
        else:
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
    snapshot_sleeper: Callable[[float], None] = time.sleep
    snapshot_jitter_fn: Callable[[float], float] = _identity_delay
    snapshot_retries_429: int = 3
    snapshot_retries_5xx: int = 2
    snapshot_backoff_base_seconds: float = 0.5
    snapshot_backoff_max_seconds: float = 4.0
    snapshot_default_limit: int = 500
    snapshot_breaker: CircuitBreaker | None = None
    stats: SourceStats = field(default_factory=SourceStats)

    def __post_init__(self) -> None:
        ensure_default_instruments(self.cfg.symbols, venue="BINANCE")
        logger = logging.getLogger("ingest.source")
        trace_id = get_trace_id() or f"binance-source-{int(time.time())}"
        catalog_state = persist_instrument_catalog_snapshot(
            base_dir=Path(self.cfg.data_dir),
            env=self.cfg.env,
            venue="BINANCE",
            run_label=trace_id,
        )
        logger.info(
            "instrument catalog snapshot persisted",
            extra={
                "trace_id": trace_id,
                "env": self.cfg.env,
                "venue": "BINANCE",
                "instrument_catalog_version": catalog_state.instrument_catalog_version,
                "instrument_catalog_snapshot_path": str(catalog_state.path),
            },
        )
        if catalog_state.drift is not None and catalog_state.drift.has_drift:
            emit_operational_alert(
                logger,
                alert_type="provider_metadata_drift",
                observed=1,
                extra={
                    "trace_id": trace_id,
                    "env": self.cfg.env,
                    "venue": "BINANCE",
                    "drift_mode": "material" if catalog_state.drift.material else "informational",
                    "drift_added_symbols": list(catalog_state.drift.added_symbols),
                    "drift_removed_symbols": list(catalog_state.drift.removed_symbols),
                    "drift_changed_symbols": list(catalog_state.drift.changed_symbols),
                    "drift_changed_fields_by_symbol": {
                        symbol: list(fields) for symbol, fields in catalog_state.drift.changed_fields_by_symbol.items()
                    },
                    "instrument_catalog_version": catalog_state.instrument_catalog_version,
                    "instrument_catalog_snapshot_path": str(catalog_state.path),
                },
            )
        if isinstance(self.error_sink, NullErrorSink):
            self.error_sink = JsonlErrorSink(
                Path(self.cfg.data_dir) / "errors" / "ingestion-dlq.jsonl"
            )
        if self.heartbeat_policy is None:
            self.heartbeat_policy = heartbeat_policy_for_streams(self.stream_types)
        if self.snapshot_breaker is None:
            self.snapshot_breaker = CircuitBreaker(
                failure_threshold=3,
                reset_timeout_seconds=30.0,
                monotonic_fn=self.monotonic_fn,
            )
        for symbol in self.cfg.symbols:
            for stream_type in self.stream_types:
                _ensure_stream_metric(self.stats, venue="BINANCE", symbol=symbol, stream_type=stream_type)

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
        venue, symbol, stream_type_label = _event_stream_context(event)
        metric = _ensure_stream_metric(self.stats, venue=venue, symbol=symbol, stream_type=stream_type_label)
        record = RawRecord(
            payload=payload,
            venue=venue,
            stream_type=stream_type,
            symbol=symbol,
            exchange_ts=event.event_ts,
            receive_ts=receive_ts,
            process_ts=getattr(event, "process_ts", None),
            trace_id=get_trace_id(),
            source_id=getattr(event, "source_id", None),
        )
        try:
            started = time.perf_counter()
            self.raw_sink.write(record)
            duration = max(0.0, time.perf_counter() - started)
            metric["raw_write_latency"] = max(float(metric["raw_write_latency"]), duration)
        except Exception as exc:
            logging.getLogger("ingest.source").error(
                "raw sink failed",
                extra={
                    "venue": venue,
                    "symbol": symbol,
                    "stream_type": stream_type_label,
                    "error": str(exc),
                },
            )
            emit_operational_alert(
                logging.getLogger("ingest.source"),
                alert_type="sink_failure",
                observed=1,
                extra={
                    "venue": venue,
                    "symbol": symbol,
                    "stream_type": stream_type_label,
                    "sink_component": "raw_sink",
                    "error": str(exc),
                },
            )
            raise IngestionError("sink", "transient", f"raw sink write failed: {exc}") from exc

    def _record_temporal_quality(self, event: IngestionEvent) -> None:
        venue, symbol, stream_type = _event_stream_context(event)
        metric = _ensure_stream_metric(self.stats, venue=venue, symbol=symbol, stream_type=stream_type)
        exchange_receive_skew = 0.0
        receive_process_skew = 0.0
        exchange_ts = getattr(event, "exchange_ts", getattr(event, "event_ts", None))
        receive_ts = getattr(event, "receive_ts", None)
        process_ts = getattr(event, "process_ts", None)
        if exchange_ts is not None and receive_ts is not None:
            exchange_receive_skew = max(0.0, (receive_ts - exchange_ts).total_seconds())
        if receive_ts is not None and process_ts is not None:
            receive_process_skew = max(0.0, (process_ts - receive_ts).total_seconds())
        metric["exchange_receive_skew_seconds"] = max(
            float(metric.get("exchange_receive_skew_seconds", 0.0)),
            exchange_receive_skew,
        )
        metric["receive_process_skew_seconds"] = max(
            float(metric.get("receive_process_skew_seconds", 0.0)),
            receive_process_skew,
        )

    def _record_rejected(self, raw_message: object, error: IngestionError, *, context: dict[str, object]) -> None:
        self.stats.events_invalid += 1
        self.stats.rejected_payloads += 1
        venue, symbol, stream_type = _raw_message_context(raw_message)
        metric = _ensure_stream_metric(self.stats, venue=venue, symbol=symbol, stream_type=stream_type)
        metric["messages_invalid_total"] = int(metric["messages_invalid_total"]) + 1
        if error.category == "validation" and _is_timestamp_validation_error(error.message):
            metric["invalid_timestamp_total"] = int(metric["invalid_timestamp_total"]) + 1
            emit_operational_alert(
                logging.getLogger("ingest.source"),
                alert_type="invalid_timestamp_detected",
                observed=int(metric["invalid_timestamp_total"]),
                extra={
                    "venue": venue,
                    "symbol": symbol,
                    "stream_type": stream_type,
                    "error_message": error.message,
                },
            )
        invalid_total = int(metric["messages_invalid_total"])
        if should_emit_threshold_alert("dlq_spike", invalid_total):
            emit_operational_alert(
                logging.getLogger("ingest.source"),
                alert_type="dlq_spike",
                observed=invalid_total,
                extra={
                    "venue": venue,
                    "symbol": symbol,
                    "stream_type": stream_type,
                    "error_category": error.category,
                    "error_severity": error.severity,
                },
            )
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
            emit_operational_alert(
                logging.getLogger("ingest.source"),
                alert_type="sink_failure",
                observed=self.stats.error_sink_failures,
                extra={
                    "venue": venue,
                    "symbol": symbol,
                    "stream_type": stream_type,
                    "sink_component": "error_sink",
                    "error": str(sink_exc),
                },
            )

    def _snapshot_retry_limit(self, exc: Exception) -> int:
        if isinstance(exc, httpx.HTTPStatusError):
            status = exc.response.status_code if exc.response is not None else None
            if status == 429:
                return self.snapshot_retries_429
            if status in {500, 502, 503, 504}:
                return self.snapshot_retries_5xx
            return 0
        if isinstance(exc, (TimeoutError, ConnectionError, httpx.TimeoutException, httpx.ConnectError, OSError)):
            return self.snapshot_retries_5xx
        return 0

    def _snapshot_retry_delay(self, attempt: int) -> float:
        base_delay = min(
            self.snapshot_backoff_base_seconds * (2 ** max(0, attempt - 1)),
            self.snapshot_backoff_max_seconds,
        )
        return max(0.0, float(self.snapshot_jitter_fn(base_delay)))

    def _emit_snapshot_retry_exhausted(
        self,
        *,
        symbol: str,
        url: str,
        observed: int,
        reason: str,
    ) -> None:
        emit_operational_alert(
            logging.getLogger("ingest.source"),
            alert_type="snapshot_retry_exhausted",
            observed=observed,
            extra={
                "venue": "BINANCE",
                "symbol": symbol,
                "stream_type": "kline",
                "endpoint": url,
                "breaker_state": self.snapshot_breaker.state if self.snapshot_breaker else "disabled",
                "reason": reason,
            },
        )

    def _snapshot_params(self, *, symbol: str, request: RecoveryRequest | None) -> dict[str, object]:
        interval = request.interval if request is not None and request.interval else "1m"
        if request is not None and request.limit is not None:
            limit = request.limit
        else:
            limit = self.snapshot_default_limit
        params: dict[str, object] = {
            "symbol": symbol,
            "interval": interval,
            "limit": min(max(int(limit), 1), 1000),
        }
        if request is not None and request.start_ts is not None:
            params["startTime"] = int(request.start_ts.timestamp() * 1000)
        if request is not None and request.end_ts is not None:
            params["endTime"] = int(request.end_ts.timestamp() * 1000)
        return params

    def _snapshot_get(self, *, symbol: str, url: str, params: dict[str, object]) -> httpx.Response:
        if self.snapshot_breaker is not None and not self.snapshot_breaker.allow_request():
            self._emit_snapshot_retry_exhausted(
                symbol=symbol,
                url=url,
                observed=self.snapshot_breaker.failure_count,
                reason="circuit_breaker_open",
            )
            raise IngestionError(
                "source",
                "transient",
                f"snapshot circuit breaker open for {symbol}",
            )

        retries = 0
        while True:
            try:
                response = self.http_get(
                    url,
                    params=params,
                    timeout=5.0,
                )
                response.raise_for_status()
                if self.snapshot_breaker is not None:
                    self.snapshot_breaker.record_success()
                return response
            except Exception as exc:
                retry_limit = self._snapshot_retry_limit(exc)
                if retries < retry_limit:
                    retries += 1
                    self.snapshot_sleeper(self._snapshot_retry_delay(retries))
                    continue
                if self.snapshot_breaker is not None:
                    self.snapshot_breaker.record_failure()
                    observed = self.snapshot_breaker.failure_count
                else:
                    observed = 1
                self._emit_snapshot_retry_exhausted(
                    symbol=symbol,
                    url=url,
                    observed=observed,
                    reason=type(exc).__name__,
                )
                raise classify_connector_error(exc) from exc

    def stream(self, end_time: float | None = None) -> Iterable[IngestionEvent]:
        url = build_ws_url(self.cfg.ws_base, self.cfg.symbols, self.stream_types)
        allowed_event_types = tuple(
            stream_type for stream_type in self.stream_types if stream_type in BINANCE_FEED_NORMALIZERS
        )
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
                    venue, symbol, stream_type = _event_stream_context(item)
                    metric = _ensure_stream_metric(self.stats, venue=venue, symbol=symbol, stream_type=stream_type)
                    metric["messages_in_total"] = int(metric["messages_in_total"]) + 1
                    self._record_temporal_quality(item)
                    self.stats.events_valid += 1
                    yield item
                    continue
                try:
                    receive_ts = datetime.now(timezone.utc)
                    payload, data, _stream, event_type = parse_message_parts(str(item))
                    if event_type not in allowed_event_types:
                        raise KeyError(f"Unknown event type: {event_type}")
                    event = normalize_binance_event(
                        event_type,
                        data,
                        receive_ts=receive_ts,
                        process_ts=None,
                    )
                    validate_ingestion_event(event)
                    metric = _ensure_stream_metric(
                        self.stats,
                        venue=getattr(event, "venue", "BINANCE"),
                        symbol=event.symbol,
                        stream_type=event_type,
                    )
                    metric["messages_in_total"] = int(metric["messages_in_total"]) + 1
                    self._record_temporal_quality(event)
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
            if isinstance(exc, TimeoutError):
                for symbol in self.cfg.symbols:
                    for stream_type in self.stream_types:
                        metric = _ensure_stream_metric(self.stats, venue="BINANCE", symbol=symbol, stream_type=stream_type)
                        metric["heartbeat_missed_total"] = int(metric["heartbeat_missed_total"]) + 1
                        metric["reconnects_total"] = int(metric["reconnects_total"]) + 1
                        emit_operational_alert(
                            logging.getLogger("ingest.source"),
                            alert_type="heartbeat_missed",
                            observed=int(metric["heartbeat_missed_total"]),
                            extra={
                                "venue": "BINANCE",
                                "symbol": symbol,
                                "stream_type": stream_type,
                            },
                        )
                        if should_emit_threshold_alert("reconnect_storm", int(metric["reconnects_total"])):
                            emit_operational_alert(
                                logging.getLogger("ingest.source"),
                                alert_type="reconnect_storm",
                                observed=int(metric["reconnects_total"]),
                                extra={
                                    "venue": "BINANCE",
                                    "symbol": symbol,
                                    "stream_type": stream_type,
                                },
                            )
            else:
                for symbol in self.cfg.symbols:
                    for stream_type in self.stream_types:
                        metric = _ensure_stream_metric(self.stats, venue="BINANCE", symbol=symbol, stream_type=stream_type)
                        metric["reconnects_total"] = int(metric["reconnects_total"]) + 1
                        if should_emit_threshold_alert("reconnect_storm", int(metric["reconnects_total"])):
                            emit_operational_alert(
                                logging.getLogger("ingest.source"),
                                alert_type="reconnect_storm",
                                observed=int(metric["reconnects_total"]),
                                extra={
                                    "venue": "BINANCE",
                                    "symbol": symbol,
                                    "stream_type": stream_type,
                                },
                            )
            raise classify_connector_error(exc) from exc

    def snapshot(self, request: RecoveryRequest | None = None) -> Iterable[IngestionEvent]:
        events: list[IngestionEvent] = []
        try:
            self.stats.snapshot_runs += 1
            for symbol in self.cfg.symbols:
                for stream_type in self.stream_types:
                    if request is not None:
                        if symbol != request.partition.symbol or stream_type != request.partition.stream_type:
                            continue
                    normalizer = BINANCE_FEED_NORMALIZERS.get(stream_type)
                    if normalizer is None or not getattr(normalizer, "supports_snapshot", False):
                        continue
                    url = f"{self.cfg.rest_base.rstrip('/')}/api/v3/klines"
                    params = self._snapshot_params(symbol=symbol, request=request)
                    resp = self._snapshot_get(symbol=symbol, url=url, params=params)
                    metric = _ensure_stream_metric(
                        self.stats,
                        venue="BINANCE",
                        symbol=symbol,
                        stream_type=stream_type,
                    )
                    if request is not None and request.limit is not None:
                        metric["recovery_window_rows_requested"] = int(metric["recovery_window_rows_requested"]) + int(request.limit)
                    receive_ts = datetime.now(timezone.utc)
                    rows = resp.json()
                    if request is not None and request.limit is not None:
                        metric["recovery_window_rows_received"] = int(metric["recovery_window_rows_received"]) + len(rows)
                        if len(rows) < int(request.limit):
                            metric["recovery_exactness_violation_total"] = int(metric["recovery_exactness_violation_total"]) + 1
                    for row in rows:
                        self.stats.source_events_in += 1
                        payload = snapshot_payload_from_row(stream_type, symbol, row, interval=str(params["interval"]))
                        try:
                            event = normalize_binance_event(
                                stream_type,
                                payload,
                                receive_ts=receive_ts,
                                process_ts=None,
                            )
                            validate_ingestion_event(event)
                            metric = _ensure_stream_metric(self.stats, venue=getattr(event, "venue", "BINANCE"), symbol=event.symbol, stream_type=stream_type)
                            metric["messages_in_total"] = int(metric["messages_in_total"]) + 1
                            self._record_temporal_quality(event)
                            self._write_raw_record(
                                payload=payload,
                                event=event,
                                stream_type=stream_type,
                                receive_ts=receive_ts,
                            )
                            events.append(event)
                            self.stats.events_valid += 1
                            self.stats.snapshot_rows += 1
                        except ValueError as exc:
                            self._record_rejected(
                                payload,
                                IngestionError("validation", "permanent", str(exc)),
                                context={"stage": "snapshot", "symbol": symbol, "stream_type": stream_type},
                            )
                            continue
        except IngestionError:
            raise
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
            venue, symbol, stream_type = _event_stream_context(event)
            metric = _ensure_stream_metric(self.stats, venue=venue, symbol=symbol, stream_type=stream_type)
            metric["messages_in_total"] = int(metric["messages_in_total"]) + 1
            yield event

    def snapshot(self, request: RecoveryRequest | None = None) -> Optional[Iterable[IngestionEvent]]:
        del request
        self.stats.snapshot_runs += 1
        if self.snapshot_events is None:
            return None
        for event in self.snapshot_events:
            validate_ingestion_event(event)
            venue, symbol, stream_type = _event_stream_context(event)
            metric = _ensure_stream_metric(self.stats, venue=venue, symbol=symbol, stream_type=stream_type)
            metric["messages_in_total"] = int(metric["messages_in_total"]) + 1
        self.stats.source_events_in += len(self.snapshot_events)
        self.stats.events_valid += len(self.snapshot_events)
        self.stats.snapshot_rows += len(self.snapshot_events)
        return self.snapshot_events
