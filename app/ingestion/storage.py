"""
Buffered Parquet writer for MarketEvent.

Partitioning: data/<env>/symbol=XYZ/date=YYYY-MM-DD.parquet
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
import time
from typing import Iterable, List

import pyarrow as pa
import pyarrow.parquet as pq

from app.common.dto import MarketEvent
from app.ingestion.dedup import identity_from_fields


def validate_output_path(base_dir: Path, *, require_absolute: bool = False) -> Path:
    resolved = Path(base_dir).expanduser()
    if require_absolute and not resolved.is_absolute():
        raise ValueError(f"data_dir must be absolute in production mode: {base_dir}")
    resolved = resolved.resolve()
    if resolved == Path(resolved.anchor):
        raise ValueError(f"data_dir cannot be filesystem root: {resolved}")
    if resolved.exists() and not resolved.is_dir():
        raise ValueError(f"data_dir must be a directory: {resolved}")
    resolved.mkdir(parents=True, exist_ok=True)
    probe = resolved / ".write_probe"
    try:
        probe.write_text("ok", encoding="utf-8")
        probe.unlink()
    except OSError as exc:
        raise ValueError(f"data_dir is not writable: {resolved}") from exc
    return resolved


@dataclass
class ParquetWriter:
    base_dir: Path
    env: str
    flush_size: int = 500
    max_dedup_rows: int = 200_000
    schema_version: str = "v1"
    dedup: bool = False
    buffer: List[MarketEvent] = field(default_factory=list)
    accepted_events: int = 0
    persisted_events: int = 0
    flush_count: int = 0
    total_write_latency_seconds: float = 0.0
    last_write_latency_seconds: float = 0.0
    max_write_latency_seconds: float = 0.0

    def __post_init__(self) -> None:
        self.base_dir = validate_output_path(self.base_dir)

    def add(self, event: MarketEvent | Iterable[MarketEvent]) -> None:
        if isinstance(event, MarketEvent):
            self.buffer.append(event)
            self.accepted_events += 1
        else:
            batch = list(event)
            self.buffer.extend(batch)
            self.accepted_events += len(batch)
        if len(self.buffer) >= self.flush_size:
            self.flush()

    def flush(self) -> None:
        if not self.buffer:
            return
        started = time.perf_counter()
        grouped = list(_group_events_by_partition(self.buffer).items())
        remaining: list[MarketEvent] = []
        persisted_now = 0
        try:
            for index, (partition_key, events) in enumerate(grouped):
                try:
                    _write_partition(
                        base_dir=self.base_dir,
                        env=self.env,
                        partition_key=partition_key,
                        events=events,
                        schema_version=self.schema_version,
                        dedup=self.dedup,
                        max_dedup_rows=self.max_dedup_rows,
                    )
                    persisted_now += len(events)
                except Exception:
                    remaining.extend(events)
                    for _, later_events in grouped[index + 1 :]:
                        remaining.extend(later_events)
                    self.buffer = remaining
                    raise

            self.buffer.clear()
            self.persisted_events += persisted_now
            self.flush_count += 1
        finally:
            duration = max(0.0, time.perf_counter() - started)
            self.last_write_latency_seconds = duration
            self.total_write_latency_seconds += duration
            if duration > self.max_write_latency_seconds:
                self.max_write_latency_seconds = duration

    @property
    def buffered_events(self) -> int:
        return len(self.buffer)


def _group_events_by_partition(events: List[MarketEvent]) -> dict[tuple[str, str], list[MarketEvent]]:
    grouped: dict[tuple[str, str], list[MarketEvent]] = {}
    for ev in events:
        day = ev.event_ts.date().isoformat()
        key = (ev.symbol, day)
        grouped.setdefault(key, []).append(ev)
    return grouped


def _partition_path(base_dir: Path, env: str, symbol: str, day: str) -> Path:
    return base_dir / env / f"symbol={symbol}" / f"date={day}" / "data.parquet"


def _to_table(events: List[MarketEvent]) -> pa.Table:
    events_sorted = sorted(events, key=lambda ev: ev.event_ts)
    data = {
        "symbol": [ev.symbol for ev in events_sorted],
        "event_ts": [pa.scalar(ev.event_ts).as_py() for ev in events_sorted],
        "price": [ev.price for ev in events_sorted],
        "size": [ev.size for ev in events_sorted],
        "source": [ev.source for ev in events_sorted],
        "metadata": [ev.metadata for ev in events_sorted],
    }
    schema = pa.schema(
        [
            ("symbol", pa.string()),
            ("event_ts", pa.timestamp("ms", tz="UTC")),
            ("price", pa.float64()),
            ("size", pa.float64()),
            ("source", pa.string()),
            ("metadata", pa.map_(pa.string(), pa.string())),
        ]
    )
    return pa.Table.from_pydict(data, schema=schema)


def _write_partition(
    *,
    base_dir: Path,
    env: str,
    partition_key: tuple[str, str],
    events: list[MarketEvent],
    schema_version: str,
    dedup: bool,
    max_dedup_rows: int,
) -> None:
    symbol, day = partition_key
    out_path = _partition_path(base_dir, env, symbol, day)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    table = _to_table(events)
    table = table.replace_schema_metadata({b"schema_version": schema_version.encode("utf-8")})
    merged = _merge_with_existing(out_path, table, dedup=dedup, max_dedup_rows=max_dedup_rows)
    _write_table_atomic(merged, out_path)


def _merge_with_existing(out_path: Path, new_table: pa.Table, *, dedup: bool, max_dedup_rows: int) -> pa.Table:
    if not out_path.exists():
        return new_table
    existing = pq.ParquetFile(out_path).read().cast(new_table.schema, safe=False)
    total_rows = existing.num_rows + new_table.num_rows
    if dedup:
        if total_rows <= max_dedup_rows:
            return _dedup_tables(existing, new_table)
        keys_existing = _key_set_from_table(existing)
        filtered_new = _filter_new_rows(new_table, keys_existing)
        return _concat_tables_ordered(existing, filtered_new)
    return _concat_tables_ordered(existing, new_table)


def _concat_tables_ordered(existing: pa.Table, new_table: pa.Table) -> pa.Table:
    if existing.num_rows == 0:
        return new_table
    if new_table.num_rows == 0:
        return existing
    last_ts = existing.column("event_ts")[-1].as_py()
    first_new_ts = new_table.column("event_ts")[0].as_py()
    combined = pa.concat_tables([existing, new_table], promote_options="default")
    if last_ts is not None and first_new_ts is not None and last_ts > first_new_ts:
        return combined.sort_by([("event_ts", "ascending")])
    return combined


def _write_table_atomic(table: pa.Table, out_path: Path) -> None:
    tmp_path = out_path.with_name(f"{out_path.name}.tmp")
    try:
        pq.write_table(table, tmp_path, use_dictionary=False)
        tmp_path.replace(out_path)
    except Exception:
        try:
            if tmp_path.exists():
                tmp_path.unlink()
        except OSError:
            pass
        raise


def read_parquet(path: Path) -> pa.Table:
    pf = pq.ParquetFile(path)
    table = pf.read()
    metadata = table.schema.metadata or {}
    version = metadata.get(b"schema_version")
    if version is None:
        raise ValueError("schema_version missing in parquet metadata")
    if version.decode("utf-8") != "v1":
        raise ValueError(f"unsupported schema_version: {version.decode('utf-8')}")
    return table


def _dedup_tables(existing: pa.Table, new: pa.Table) -> pa.Table:
    schema = existing.schema
    merged: list[dict] = []
    seen = set()

    def add_rows(rows: list[dict]) -> None:
        for row in rows:
            key = identity_from_fields(
                symbol=row["symbol"],
                event_ts=row["event_ts"],
                price=row["price"],
                size=row["size"],
                source=row["source"],
            )
            if key in seen:
                continue
            seen.add(key)
            merged.append(row)

    add_rows(existing.to_pylist())
    add_rows(new.to_pylist())
    merged.sort(key=lambda r: r["event_ts"])
    return pa.Table.from_pylist(merged, schema=schema)


def _key_set_from_table(tbl: pa.Table) -> set[tuple]:
    keys = set()
    cols = tbl.select(["symbol", "event_ts", "price", "size", "source"])
    for row in cols.to_pylist():
        keys.add(
            identity_from_fields(
                symbol=row["symbol"],
                event_ts=row["event_ts"],
                price=row["price"],
                size=row["size"],
                source=row["source"],
            )
        )
    return keys


def _filter_new_rows(tbl: pa.Table, existing_keys: set[tuple]) -> pa.Table:
    schema = tbl.schema
    filtered = []
    for row in tbl.to_pylist():
        key = identity_from_fields(
            symbol=row["symbol"],
            event_ts=row["event_ts"],
            price=row["price"],
            size=row["size"],
            source=row["source"],
        )
        if key in existing_keys:
            continue
        filtered.append(row)
    if not filtered:
        return pa.Table.from_pylist([], schema=schema)
    filtered.sort(key=lambda r: r["event_ts"])
    return pa.Table.from_pylist(filtered, schema=schema)
