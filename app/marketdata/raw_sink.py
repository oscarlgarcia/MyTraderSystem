"""
Append-only raw landing for valid market data messages.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Protocol
from uuid import uuid4

from app.common.dto import normalize_symbol
from app.common.validator import ensure_aware_utc


@dataclass(slots=True, kw_only=True)
class RawRecord:
    payload: Any
    venue: str
    stream_type: str
    symbol: str
    exchange_ts: datetime
    receive_ts: datetime
    provider_ts: datetime | None = None
    process_ts: datetime | None = None
    run_id: str | None = None
    ingestion_seq: int | None = None
    trace_id: str | None = None
    source_id: str | None = None

    def __post_init__(self) -> None:
        self.venue = str(self.venue).upper()
        self.stream_type = str(self.stream_type).lower()
        self.symbol = normalize_symbol(self.symbol)
        ensure_aware_utc(self.exchange_ts)
        if self.provider_ts is not None:
            ensure_aware_utc(self.provider_ts)
        ensure_aware_utc(self.receive_ts)
        if self.process_ts is not None:
            ensure_aware_utc(self.process_ts)
        if not self.venue:
            raise ValueError("venue must be non-empty")
        if not self.stream_type:
            raise ValueError("stream_type must be non-empty")
        if self.run_id is not None:
            self.run_id = str(self.run_id)
            if not self.run_id:
                raise ValueError("run_id must be non-empty")
        if self.ingestion_seq is not None:
            self.ingestion_seq = int(self.ingestion_seq)
            if self.ingestion_seq < 1:
                raise ValueError("ingestion_seq must be positive")
        if self.source_id is not None:
            self.source_id = str(self.source_id)

    @property
    def partition_date(self) -> str:
        return self.exchange_ts.date().isoformat()

    def to_dict(self) -> dict[str, Any]:
        payload = self.payload
        if isinstance(payload, str):
            try:
                payload = json.loads(payload)
            except json.JSONDecodeError:
                pass
        return {
            "payload": payload,
            "venue": self.venue,
            "stream_type": self.stream_type,
            "symbol": self.symbol,
            "exchange_ts": self.exchange_ts.isoformat(),
            "provider_ts": self.provider_ts.isoformat() if self.provider_ts is not None else None,
            "receive_ts": self.receive_ts.isoformat(),
            "process_ts": self.process_ts.isoformat() if self.process_ts is not None else None,
            "run_id": self.run_id,
            "ingestion_seq": self.ingestion_seq,
            "trace_id": self.trace_id,
            "source_id": self.source_id,
        }


class RawSink(Protocol):
    def write(self, record: RawRecord) -> Path | None: ...


class NullRawSink:
    def write(self, record: RawRecord) -> Path | None:
        del record
        return None


def _generate_run_id() -> str:
    timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
    return f"{timestamp}-{uuid4().hex[:6]}"


@dataclass(slots=True)
class JsonlRawSink:
    base_dir: Path
    env: str
    filename: str = "events.jsonl"
    run_id: str = field(default_factory=_generate_run_id)
    _next_ingestion_seq: int = field(default=1, init=False, repr=False)

    def path_for(self, record: RawRecord) -> Path:
        return (
            self.base_dir
            / f"env={self.env}"
            / f"venue={record.venue}"
            / f"stream_type={record.stream_type}"
            / f"symbol={record.symbol}"
            / f"date={record.partition_date}"
            / self.filename
        )

    def write(self, record: RawRecord) -> Path:
        if record.run_id is None:
            record.run_id = self.run_id
        if record.ingestion_seq is None:
            record.ingestion_seq = self._next_ingestion_seq
            self._next_ingestion_seq += 1
        else:
            self._next_ingestion_seq = max(self._next_ingestion_seq, int(record.ingestion_seq) + 1)
        path = self.path_for(record)
        path.parent.mkdir(parents=True, exist_ok=True)
        with path.open("a", encoding="utf-8", newline="\n") as handle:
            handle.write(json.dumps(record.to_dict(), ensure_ascii=False, default=str))
            handle.write("\n")
        return path


def build_raw_manifest(path: Path) -> dict[str, Any]:
    resolved = Path(path)
    raw_bytes = resolved.read_bytes()
    rows = [json.loads(line) for line in resolved.read_text(encoding="utf-8").splitlines() if line.strip()]
    exchange_timestamps = [row.get("exchange_ts") for row in rows if row.get("exchange_ts") not in (None, "")]
    run_ids = sorted({str(row["run_id"]) for row in rows if row.get("run_id") not in (None, "")})
    return {
        "path": str(resolved),
        "sha256": hashlib.sha256(raw_bytes).hexdigest(),
        "line_count": len(rows),
        "run_ids": run_ids,
        "first_exchange_ts": exchange_timestamps[0] if exchange_timestamps else None,
        "last_exchange_ts": exchange_timestamps[-1] if exchange_timestamps else None,
    }


def write_raw_manifest(path: Path) -> Path:
    resolved = Path(path)
    manifest_path = resolved.with_suffix(".manifest.json")
    manifest_path.write_text(json.dumps(build_raw_manifest(resolved), ensure_ascii=False, indent=2), encoding="utf-8")
    return manifest_path
