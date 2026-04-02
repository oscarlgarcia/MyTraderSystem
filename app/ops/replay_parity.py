from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from pathlib import Path

from app.ingestion.storage import read_parquet
from app.marketdata.replay import ReplaySource, read_raw_entries


@dataclass(frozen=True, slots=True)
class ReplayParityReport:
    raw_base_dir: str
    normalized_path: str
    env: str
    symbol: str
    stream_type: str
    raw_rows: int
    replay_rows: int
    normalized_rows: int
    replay_identity_count: int
    normalized_identity_count: int
    order_match: bool
    pass_ok: bool


def _metadata_dict(value: object) -> dict[str, str]:
    if value is None:
        return {}
    if isinstance(value, dict):
        return {str(key): str(item) for key, item in value.items()}
    out: dict[str, str] = {}
    if isinstance(value, list):
        for item in value:
            if isinstance(item, (list, tuple)) and len(item) == 2:
                out[str(item[0])] = str(item[1])
    return out


def _event_identity(event) -> tuple[object, ...]:
    metadata = getattr(event, "metadata", {}) or {}
    return (
        event.event_ts,
        str(getattr(event, "source_id", "") or ""),
        str(getattr(event, "trade_id", "") or ""),
        str(metadata.get("raw_run_id", "") or ""),
        int(metadata["raw_ingestion_seq"]) if metadata.get("raw_ingestion_seq") not in (None, "") else -1,
        event.symbol,
        event.source,
    )


def _row_identity(row: dict[str, object]) -> tuple[object, ...]:
    metadata = _metadata_dict(row.get("metadata"))
    return (
        row.get("event_ts") or row.get("exchange_ts"),
        str(row.get("source_id") or ""),
        str(row.get("trade_id") or ""),
        str(row.get("raw_run_id") or ""),
        int(row["raw_ingestion_seq"]) if row.get("raw_ingestion_seq") not in (None, "") else -1,
        str(row.get("symbol") or ""),
        str(row.get("source") or ""),
    )


def _ordered_unique(values: list[tuple[object, ...]]) -> list[tuple[object, ...]]:
    out: list[tuple[object, ...]] = []
    seen: set[tuple[object, ...]] = set()
    for value in values:
        if value in seen:
            continue
        seen.add(value)
        out.append(value)
    return out


def build_replay_parity_report(
    *,
    raw_base_dir: Path,
    normalized_path: Path,
    env: str,
    symbol: str,
    stream_type: str,
) -> ReplayParityReport:
    raw_entries = read_raw_entries(Path(raw_base_dir), env, symbol=symbol, stream_types=(stream_type,))
    replayed = list(ReplaySource(base_dir=Path(raw_base_dir), env=env, symbol=symbol, stream_types=(stream_type,)).stream())
    normalized_rows = read_parquet(Path(normalized_path)).to_pylist()

    replay_identities = _ordered_unique([_event_identity(event) for event in replayed])
    normalized_identities = [_row_identity(row) for row in normalized_rows]
    order_match = replay_identities == normalized_identities
    return ReplayParityReport(
        raw_base_dir=str(raw_base_dir),
        normalized_path=str(normalized_path),
        env=env,
        symbol=symbol,
        stream_type=stream_type,
        raw_rows=len(raw_entries),
        replay_rows=len(replayed),
        normalized_rows=len(normalized_rows),
        replay_identity_count=len(replay_identities),
        normalized_identity_count=len(normalized_identities),
        order_match=order_match,
        pass_ok=(order_match and len(replay_identities) == len(normalized_identities)),
    )


def write_replay_parity_report(path: Path, report: ReplayParityReport) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(asdict(report), ensure_ascii=False, indent=2, default=str), encoding="utf-8")
    return path
