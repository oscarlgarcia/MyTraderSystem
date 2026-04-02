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
from app.marketdata.recovery import RecoveryRequest
from app.marketdata.raw_sink import RawRecord
from app.marketdata.validators import validate_ingestion_event

ReplaySpeed = Literal["full-speed", "step-by-step"]
Sleeper = Callable[[float], None]


@dataclass(slots=True)
class ReplayEntry:
    record: RawRecord
    path: Path
    line_no: int


@dataclass(frozen=True, slots=True)
class ReplayOrderAmbiguity:
    reason: str
    path: Path
    line_no: int


def _replay_sort_key(entry: ReplayEntry) -> tuple:
    if entry.record.run_id is not None and entry.record.ingestion_seq is not None:
        return (
            0,
            str(entry.record.run_id),
            int(entry.record.ingestion_seq),
            entry.record.receive_ts,
            str(entry.path),
            entry.line_no,
        )
    return (1, entry.record.receive_ts, str(entry.path), entry.line_no)


def detect_replay_order_ambiguities(entries: Iterable[ReplayEntry]) -> list[ReplayOrderAmbiguity]:
    ambiguities: list[ReplayOrderAmbiguity] = []
    for entry in entries:
        has_run_id = entry.record.run_id is not None
        has_ingestion_seq = entry.record.ingestion_seq is not None
        if has_run_id != has_ingestion_seq:
            ambiguities.append(
                ReplayOrderAmbiguity(
                    reason="partial_order_metadata",
                    path=entry.path,
                    line_no=entry.line_no,
                )
            )
    return ambiguities


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
        provider_ts=_parse_ts(payload["provider_ts"]) if payload.get("provider_ts") not in (None, "") else None,
        receive_ts=_parse_ts(payload["receive_ts"]),
        process_ts=_parse_ts(payload["process_ts"]) if payload.get("process_ts") not in (None, "") else None,
        run_id=payload.get("run_id"),
        ingestion_seq=int(payload["ingestion_seq"]) if payload.get("ingestion_seq") is not None else None,
        trace_id=payload.get("trace_id"),
        source_id=payload.get("source_id"),
    )


def _record_replay_corruption(
    *,
    base_dir: Path,
    path: Path,
    line_no: int,
    line: str,
    error: json.JSONDecodeError,
) -> Path:
    out_path = Path(base_dir).parent / "errors" / "replay-corruption-dlq.jsonl"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "path": str(path),
        "line_no": line_no,
        "error_type": "ReplayRawCorruptionError",
        "error_message": str(error),
        "raw_line": line.rstrip("\n"),
    }
    with out_path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload, ensure_ascii=False))
        handle.write("\n")
    return out_path


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
                try:
                    record = _record_from_dict(json.loads(line))
                except json.JSONDecodeError as exc:
                    _record_replay_corruption(
                        base_dir=Path(base_dir),
                        path=path,
                        line_no=line_no,
                        line=line,
                        error=exc,
                    )
                    continue
                if start_ts is not None and record.exchange_ts < start_ts:
                    continue
                if end_ts is not None and record.exchange_ts > end_ts:
                    continue
                entries.append(ReplayEntry(record=record, path=path, line_no=line_no))
    entries.sort(key=_replay_sort_key)
    return entries


def normalize_replay_record(record: RawRecord, *, normalizer_version: str = NORMALIZER_VERSION) -> IngestionEvent:
    normalizer_version = resolve_normalizer_version(normalizer_version)
    replay_process_ts = record.process_ts or record.receive_ts
    payload = record.payload
    if isinstance(payload, str):
        payload = json.loads(payload)
    if isinstance(payload, dict) and ("stream" in payload or "data" in payload):
        event = parse_typed_message(
            json.dumps(payload),
            venue=record.venue,
            receive_ts=record.receive_ts,
            process_ts=replay_process_ts,
        )
    elif record.stream_type == "trade":
        event = normalize_trade_typed(
            payload,
            venue=record.venue,
            receive_ts=record.receive_ts,
            process_ts=replay_process_ts,
        )
    elif record.stream_type == "kline":
        event = normalize_kline_typed(
            payload,
            venue=record.venue,
            receive_ts=record.receive_ts,
            process_ts=replay_process_ts,
        )
    else:
        raise KeyError(f"unsupported replay stream_type: {record.stream_type}")
    if isinstance(event, BaseMarketEvent):
        event.provider_ts = record.provider_ts
        stamp_normalizer_version(event.metadata, version=normalizer_version)
        if record.run_id is not None:
            event.metadata["raw_run_id"] = str(record.run_id)
        if record.ingestion_seq is not None:
            event.metadata["raw_ingestion_seq"] = str(record.ingestion_seq)
        payload = record.payload if isinstance(record.payload, dict) else {}
        if isinstance(payload, dict) and payload.get("_historical_trade_kind") is not None:
            event.metadata.setdefault("historical_feed_kind", str(payload["_historical_trade_kind"]))
    elif isinstance(event, MarketEvent):
        stamp_normalizer_version(event.metadata, version=normalizer_version)
        if record.provider_ts is not None:
            event.metadata.setdefault("provider_ts", record.provider_ts.isoformat())
        if record.run_id is not None:
            event.metadata["raw_run_id"] = str(record.run_id)
        if record.ingestion_seq is not None:
            event.metadata["raw_ingestion_seq"] = str(record.ingestion_seq)
        payload = record.payload if isinstance(record.payload, dict) else {}
        if isinstance(payload, dict) and payload.get("_historical_trade_kind") is not None:
            event.metadata.setdefault("historical_feed_kind", str(payload["_historical_trade_kind"]))
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

    def snapshot(self, request: RecoveryRequest | None = None) -> None:
        del request
        return None
