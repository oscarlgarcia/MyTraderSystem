"""
Buffered Parquet writer for MarketEvent.

Partitioning: data/<env>/symbol=XYZ/date=YYYY-MM-DD.parquet
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime
from pathlib import Path
from typing import List

import pyarrow as pa
import pyarrow.parquet as pq

from app.common.dto import MarketEvent


@dataclass
class ParquetWriter:
    base_dir: Path
    env: str
    flush_size: int = 500
    max_dedup_rows: int = 200_000
    dedup: bool = False
    buffer: List[MarketEvent] = field(default_factory=list)

    def add(self, event: MarketEvent) -> None:
        self.buffer.append(event)
        if len(self.buffer) >= self.flush_size:
            self.flush()

    def flush(self) -> None:
        if not self.buffer:
            return
        # Partition per symbol per day
        grouped: dict[tuple[str, str], list[MarketEvent]] = {}
        for ev in self.buffer:
            day = ev.event_ts.date().isoformat()
            key = (ev.symbol, day)
            grouped.setdefault(key, []).append(ev)

        for (symbol, day), events in grouped.items():
            table = _to_table(events)
            out_dir = self.base_dir / self.env / f"symbol={symbol}" / f"date={day}"
            out_dir.mkdir(parents=True, exist_ok=True)
            out_path = out_dir / "data.parquet"
            if out_path.exists():
                pf = pq.ParquetFile(out_path)
                existing = pf.read().cast(table.schema, safe=False)
                total_rows = existing.num_rows + table.num_rows
                if self.dedup:
                    if total_rows <= self.max_dedup_rows:
                        table = _dedup_tables(existing, table)
                    else:
                        # Dedup sólo de nuevos vs existentes y mantener orden aproximado sin cargar todo.
                        keys_existing = _key_set_from_table(existing)
                        new_filtered = _filter_new_rows(table, keys_existing)
                        # si las nuevas empiezan después del último ts existente, podemos concatenar
                        last_ts = existing.column("event_ts")[-1].as_py() if existing.num_rows else None
                        first_new_ts = new_filtered.column("event_ts")[0].as_py() if new_filtered.num_rows else None
                        combined = [existing, new_filtered] if new_filtered.num_rows else [existing]
                        if last_ts and first_new_ts and last_ts > first_new_ts:
                            merged = pa.concat_tables(combined, promote_options="default")
                            merged = merged.sort_by([("event_ts", "ascending")])
                            table = merged
                        else:
                            table = pa.concat_tables(combined, promote_options="default")
                else:
                    table = pa.concat_tables([existing, table], promote_options="default")
            pq.write_table(table, out_path, use_dictionary=False)
        self.buffer.clear()


def _to_table(events: List[MarketEvent]) -> pa.Table:
    data = {
        "symbol": [ev.symbol for ev in events],
        "event_ts": [pa.scalar(ev.event_ts).as_py() for ev in events],
        "price": [ev.price for ev in events],
        "size": [ev.size for ev in events],
        "source": [ev.source for ev in events],
        "metadata": [ev.metadata for ev in events],
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


def read_parquet(path: Path) -> pa.Table:
    # Read single file without inferring partition directories.
    return pq.ParquetFile(path).read()


def _dedup_tables(existing: pa.Table, new: pa.Table) -> pa.Table:
    schema = existing.schema
    merged: list[dict] = []
    seen = set()

    def add_rows(rows: list[dict]) -> None:
        for row in rows:
            key = (row["symbol"], row["event_ts"], row["price"], row["size"], row["source"])
            if key in seen:
                continue
            seen.add(key)
            merged.append(row)

    add_rows(existing.to_pylist())
    add_rows(new.to_pylist())
    # Ordenar por timestamp para mantener consistencia
    merged.sort(key=lambda r: r["event_ts"])
    return pa.Table.from_pylist(merged, schema=schema)


def _key_set_from_table(tbl: pa.Table) -> set[tuple]:
    keys = set()
    cols = tbl.select(["symbol", "event_ts", "price", "size", "source"])
    for row in cols.to_pylist():
        keys.add((row["symbol"], row["event_ts"], row["price"], row["size"], row["source"]))
    return keys


def _filter_new_rows(tbl: pa.Table, existing_keys: set[tuple]) -> pa.Table:
    schema = tbl.schema
    filtered = []
    for row in tbl.to_pylist():
        key = (row["symbol"], row["event_ts"], row["price"], row["size"], row["source"])
        if key in existing_keys:
            continue
        filtered.append(row)
    if not filtered:
        return pa.Table.from_pylist([], schema=schema)
    filtered.sort(key=lambda r: r["event_ts"])
    return pa.Table.from_pylist(filtered, schema=schema)
