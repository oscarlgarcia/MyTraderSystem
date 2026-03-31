"""
Deterministic replay from raw landing.
"""

from __future__ import annotations

import json
import time
from dataclasses import dataclass
from datetime import datetime
from pathlib import Path
from typing import Callable, Iterable, Literal

from app.common.dto import MarketEvent
from app.common.validator import ensure_aware_utc
from app.ingestion.client import normalize_kline_typed, normalize_trade_typed, parse_typed_message
from app.ingestion.sources import Source
from app.marketdata.models import BaseMarketEvent, IngestionEvent
from app.marketdata.normalization import NORMALIZER_VERSION, resolve_normalizer_version, stamp_normalizer_version
from app.marketdata.raw_sink import RawRecord
from app.marketdata.validators import validate_ingestion_event

ReplaySpeed = Literal["full-speed", "step-by-step"]
Sleeper = Callable[[float], None]


@dataclass(slots=True)
class ReplayEntry:
    record: RawRecord
    path: Path
    line_no: int


def _parse_ts(value: str | datetime) -> datetime:
    if isinstance(value, datetime):
        ts = value
    else:
        ts = datetime.fromisoformat(value)
    ensure_aware_utc(ts)
    return ts


def _record_from_dict(payload: dict) -> RawRecord:
    return RawRecord(
        payload=payload["payload"],
        venue=payload["venue"],
        stream_type=payload["stream_type"],
        symbol=payload["symbol"],
        exchange_ts=_parse_ts(payload["exchange_ts"]),
        receive_ts=_parse_ts(payload["receive_ts"]),
        trace_id=payload.get("trace_id"),
        source_id=payload.get("source_id"),
    )


def list_raw_files(
    base_dir: Path,
    env: str,
    *,
    venue: str | None = None,
    stream_types: Iterable[str] | None = None,
    symbol: str | None = None,
) -> list[Path]:
    root = Path(base_dir)
    venues = [f"venue={venue.upper()}"] if venue else ["venue=*"]
    streams = [f"stream_type={stream_type.lower()}" for stream_type in stream_types] if stream_types else ["stream_type=*"]
    symbols = [f"symbol={symbol.upper()}"] if symbol else ["symbol=*"]
    files: list[Path] = []
    for venue_glob in venues:
        for stream_glob in streams:
            for symbol_glob in symbols:
                files.extend(
                    root.glob(
                        f"env={env}/{venue_glob}/{stream_glob}/{symbol_glob}/date=*/events.jsonl"
                    )
                )
    return sorted(set(files))


def read_raw_entries(
    base_dir: Path,
    env: str,
    *,
    venue: str | None = None,
    stream_types: Iterable[str] | None = None,
    symbol: str | None = None,
    start_ts: datetime | None = None,
    end_ts: datetime | None = None,
) -> list[ReplayEntry]:
    if start_ts is not None:
        ensure_aware_utc(start_ts)
    if end_ts is not None:
        ensure_aware_utc(end_ts)
    entries: list[ReplayEntry] = []
    for path in list_raw_files(base_dir, env, venue=venue, stream_types=stream_types, symbol=symbol):
        with path.open("r", encoding="utf-8") as handle:
            for line_no, line in enumerate(handle, start=1):
                if not line.strip():
                    continue
                record = _record_from_dict(json.loads(line))
                if start_ts is not None and record.exchange_ts < start_ts:
                    continue
                if end_ts is not None and record.exchange_ts > end_ts:
                    continue
                entries.append(ReplayEntry(record=record, path=path, line_no=line_no))
    entries.sort(key=lambda entry: (entry.record.receive_ts, str(entry.path), entry.line_no))
    return entries


def normalize_replay_record(record: RawRecord, *, normalizer_version: str = NORMALIZER_VERSION) -> IngestionEvent:
    normalizer_version = resolve_normalizer_version(normalizer_version)
    payload = record.payload
    if isinstance(payload, str):
        payload = json.loads(payload)
    if isinstance(payload, dict) and ("stream" in payload or "data" in payload):
        event = parse_typed_message(
            json.dumps(payload),
            venue=record.venue,
            receive_ts=record.receive_ts,
            process_ts=record.receive_ts,
        )
    elif record.stream_type == "trade":
        event = normalize_trade_typed(
            payload,
            venue=record.venue,
            receive_ts=record.receive_ts,
            process_ts=record.receive_ts,
        )
    elif record.stream_type == "kline":
        event = normalize_kline_typed(
            payload,
            venue=record.venue,
            receive_ts=record.receive_ts,
            process_ts=record.receive_ts,
        )
    else:
        raise KeyError(f"unsupported replay stream_type: {record.stream_type}")
    if isinstance(event, BaseMarketEvent):
        stamp_normalizer_version(event.metadata, version=normalizer_version)
    elif isinstance(event, MarketEvent):
        stamp_normalizer_version(event.metadata, version=normalizer_version)
    validate_ingestion_event(event)
    return event


@dataclass
class ReplaySource(Source):
    base_dir: Path
    env: str
    venue: str | None = None
    stream_types: tuple[str, ...] | None = None
    symbol: str | None = None
    start_ts: datetime | None = None
    end_ts: datetime | None = None
    speed: ReplaySpeed = "full-speed"
    step_seconds: float = 0.0
    sleeper: Sleeper = time.sleep
    normalizer_version: str = NORMALIZER_VERSION

    def stream(self, end_time: float | None = None) -> Iterable[IngestionEvent]:
        first = True
        for entry in read_raw_entries(
            self.base_dir,
            self.env,
            venue=self.venue,
            stream_types=self.stream_types,
            symbol=self.symbol,
            start_ts=self.start_ts,
            end_ts=self.end_ts,
        ):
            if end_time is not None and time.time() >= end_time:
                break
            if not first and self.speed == "step-by-step":
                self.sleeper(self.step_seconds)
            first = False
            yield normalize_replay_record(entry.record, normalizer_version=self.normalizer_version)

    def snapshot(self) -> None:
        return None
