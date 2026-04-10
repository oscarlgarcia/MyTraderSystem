from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import date, datetime, time, timedelta, timezone
from pathlib import Path
from typing import Iterable

import httpx

from app.ingestion.backfill import (
    fetch_klines,
    fetch_trades,
    normalize_kline_row,
    normalize_trade_row,
)
from app.ingestion.storage import ParquetWriter, normalized_partition_path
from app.marketdata.dataset_quality import DatasetQualityRegistry
from app.marketdata.dataset_contracts import DatasetPartitionRef, parse_normalized_partition_path


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def _day_bounds(day: date) -> tuple[datetime, datetime]:
    start = datetime.combine(day, time.min, tzinfo=timezone.utc)
    end = start + timedelta(days=1)
    return start, end


@dataclass(frozen=True, slots=True)
class GapFillCandidate:
    venue: str
    symbol: str
    stream_type: str
    start_date: str
    end_date: str
    missing_dates: tuple[str, ...]
    reason: str
    source_dataset_ids: tuple[str, ...]
    generated_at: str


@dataclass(frozen=True, slots=True)
class GapFillPlan:
    env: str
    generated_at: str
    candidates: tuple[GapFillCandidate, ...]


@dataclass(frozen=True, slots=True)
class GapFillExecutionReport:
    env: str
    generated_at: str
    executed_candidates: int
    recovered_events: int
    touched_partitions: tuple[str, ...]
    plan_path: str


def gap_fill_plan_path(base_dir: Path, env: str) -> Path:
    return Path(base_dir) / env / "catalog" / "gap-fill-plan.json"


def _missing_dates_for_group(refs: Iterable[DatasetPartitionRef]) -> tuple[str, ...]:
    ordered = sorted(date.fromisoformat(ref.partition_date) for ref in refs)
    if not ordered:
        return ()
    cursor = ordered[0]
    end = ordered[-1]
    present = set(ordered)
    missing: list[str] = []
    while cursor <= end:
        if cursor not in present:
            missing.append(cursor.isoformat())
        cursor += timedelta(days=1)
    return tuple(missing)


def build_gap_fill_plan(
    *,
    env: str,
    refs: Iterable[DatasetPartitionRef],
    quality_registry: DatasetQualityRegistry | None = None,
) -> GapFillPlan:
    grouped: dict[tuple[str, str, str], list[DatasetPartitionRef]] = {}
    for ref in refs:
        grouped.setdefault((ref.venue, ref.symbol, ref.stream_type), []).append(ref)

    candidates: list[GapFillCandidate] = []
    for (venue, symbol, stream_type), group_refs in sorted(grouped.items()):
        missing_dates = _missing_dates_for_group(group_refs)
        if missing_dates:
            candidates.append(
                GapFillCandidate(
                    venue=venue,
                    symbol=symbol,
                    stream_type=stream_type,
                    start_date=missing_dates[0],
                    end_date=missing_dates[-1],
                    missing_dates=missing_dates,
                    reason="missing_partitions",
                    source_dataset_ids=tuple(sorted(ref.dataset_id for ref in group_refs)),
                    generated_at=_utc_now(),
                )
            )

    if quality_registry is not None:
        for report in quality_registry.reports:
            if report.status == "healthy":
                continue
            candidates.append(
                GapFillCandidate(
                    venue=report.venue,
                    symbol=report.symbol,
                    stream_type=report.stream_type,
                    start_date=report.partition_date,
                    end_date=report.partition_date,
                    missing_dates=(report.partition_date,),
                    reason=f"quality_{report.status}",
                    source_dataset_ids=(report.dataset_id,),
                    generated_at=_utc_now(),
                )
            )

    deduped: dict[tuple[str, str, str, str, str, str], GapFillCandidate] = {}
    for candidate in candidates:
        key = (
            candidate.venue,
            candidate.symbol,
            candidate.stream_type,
            candidate.start_date,
            candidate.end_date,
            candidate.reason,
        )
        deduped.setdefault(key, candidate)

    return GapFillPlan(env=env, generated_at=_utc_now(), candidates=tuple(deduped.values()))


def write_gap_fill_plan(path: Path, plan: GapFillPlan) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            {
                "env": plan.env,
                "generated_at": plan.generated_at,
                "candidates": [asdict(item) for item in plan.candidates],
            },
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    return path


def read_gap_fill_plan(path: Path, *, env: str | None = None) -> GapFillPlan:
    resolved = Path(path)
    if not resolved.exists():
        return GapFillPlan(env=env or "unknown", generated_at=_utc_now(), candidates=())
    payload = json.loads(resolved.read_text(encoding="utf-8"))
    return GapFillPlan(
        env=str(payload.get("env") or env or "unknown"),
        generated_at=str(payload.get("generated_at") or _utc_now()),
        candidates=tuple(GapFillCandidate(**item) for item in payload.get("candidates", ())),
    )


def execute_gap_fill_candidate(
    *,
    base_dir: Path,
    env: str,
    rest_base: str,
    candidate: GapFillCandidate,
    interval: str = "1m",
    batch_limit: int = 1000,
) -> tuple[int, tuple[str, ...]]:
    writer = ParquetWriter(base_dir=Path(base_dir), env=env, dedup=True)
    recovered_events = 0
    touched: set[str] = set()
    with httpx.Client() as client:
        for missing_date in candidate.missing_dates:
            day = date.fromisoformat(missing_date)
            start_ts, end_ts = _day_bounds(day)
            start_ms = int(start_ts.timestamp() * 1000)
            end_ms = int(end_ts.timestamp() * 1000)
            if candidate.stream_type == "kline":
                rows = fetch_klines(
                    client,
                    rest_base,
                    candidate.symbol,
                    start_ms,
                    end_ms,
                    interval=interval,
                    limit=batch_limit,
                )
                for row in rows:
                    event = normalize_kline_row(candidate.symbol, row, interval=interval, venue=candidate.venue)
                    writer.add(event)
                    recovered_events += 1
                    touched.add(
                        str(
                            normalized_partition_path(
                                Path(base_dir),
                                env,
                                source=event.source,
                                symbol=event.symbol,
                                day=event.event_ts.date().isoformat(),
                                venue=getattr(event, "venue", candidate.venue),
                            )
                        )
                    )
            elif candidate.stream_type == "trade":
                rows = fetch_trades(
                    client,
                    rest_base,
                    candidate.symbol,
                    start_ms,
                    end_ms,
                    limit=batch_limit,
                )
                for row in rows:
                    event = normalize_trade_row(candidate.symbol, row, venue=candidate.venue)
                    writer.add(event)
                    recovered_events += 1
                    touched.add(
                        str(
                            normalized_partition_path(
                                Path(base_dir),
                                env,
                                source=event.source,
                                symbol=event.symbol,
                                day=event.event_ts.date().isoformat(),
                                venue=getattr(event, "venue", candidate.venue),
                            )
                        )
                    )
            else:
                continue
    writer.flush()
    return recovered_events, tuple(sorted(touched))
