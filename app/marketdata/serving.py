from __future__ import annotations

import json
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from app.marketdata.query import HistoricalQueryRequest, query_rows


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def serving_db_path(base_dir: Path, env: str) -> Path:
    return Path(base_dir) / env / "serving" / "marketdata.sqlite"


@dataclass(frozen=True, slots=True)
class CuratedRefreshReport:
    env: str
    stream_type: str
    venue: str
    symbol: str | None
    refreshed_rows: int
    latest_exchange_ts: str | None
    db_path: str
    refreshed_at: str


class CuratedServingStore:
    def __init__(self, path: Path) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)
        self._initialize()

    def _connect(self) -> sqlite3.Connection:
        return sqlite3.connect(self.path)

    def _initialize(self) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                create table if not exists latest_events (
                    env text not null,
                    venue text not null,
                    stream_type text not null,
                    symbol text not null,
                    exchange_ts text,
                    row_json text not null,
                    refreshed_at text not null,
                    primary key (env, venue, stream_type, symbol)
                )
                """
            )
            conn.execute(
                """
                create table if not exists refresh_audit (
                    refresh_id integer primary key autoincrement,
                    env text not null,
                    venue text not null,
                    stream_type text not null,
                    symbol text,
                    refreshed_rows integer not null,
                    latest_exchange_ts text,
                    refreshed_at text not null
                )
                """
            )

    def upsert_latest(self, *, env: str, venue: str, stream_type: str, symbol: str, row: dict) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                insert into latest_events(env, venue, stream_type, symbol, exchange_ts, row_json, refreshed_at)
                values(?, ?, ?, ?, ?, ?, ?)
                on conflict(env, venue, stream_type, symbol) do update set
                    exchange_ts=excluded.exchange_ts,
                    row_json=excluded.row_json,
                    refreshed_at=excluded.refreshed_at
                """,
                (
                    env,
                    venue,
                    stream_type,
                    symbol,
                    row.get("exchange_ts").isoformat() if row.get("exchange_ts") is not None else None,
                    json.dumps(row, ensure_ascii=False, default=str),
                    _utc_now(),
                ),
            )

    def record_refresh(self, report: CuratedRefreshReport) -> None:
        with self._connect() as conn:
            conn.execute(
                """
                insert into refresh_audit(env, venue, stream_type, symbol, refreshed_rows, latest_exchange_ts, refreshed_at)
                values(?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    report.env,
                    report.venue,
                    report.stream_type,
                    report.symbol,
                    report.refreshed_rows,
                    report.latest_exchange_ts,
                    report.refreshed_at,
                ),
            )

    def latest(self, *, env: str, venue: str, stream_type: str, symbol: str) -> dict | None:
        with self._connect() as conn:
            row = conn.execute(
                """
                select row_json from latest_events
                where env=? and venue=? and stream_type=? and symbol=?
                """,
                (env, venue, stream_type, symbol),
            ).fetchone()
        if row is None:
            return None
        return json.loads(row[0])


def refresh_curated_store(
    *,
    base_dir: Path,
    env: str,
    stream_type: str,
    venue: str = "BINANCE",
    symbol: str | None = None,
    db_path: Path | None = None,
) -> CuratedRefreshReport:
    request = HistoricalQueryRequest(base_dir=Path(base_dir), env=env, stream_type=stream_type, symbol=symbol, venue=venue)
    rows = query_rows(request)
    store = CuratedServingStore(db_path or serving_db_path(base_dir, env))
    latest_ts = None
    latest_per_symbol: dict[str, dict] = {}
    for row in rows:
        row_symbol = str(row["symbol"])
        current_ts = row.get("exchange_ts")
        previous = latest_per_symbol.get(row_symbol)
        if previous is None:
            latest_per_symbol[row_symbol] = row
        else:
            previous_ts = previous.get("exchange_ts")
            if current_ts is not None and (previous_ts is None or current_ts >= previous_ts):
                latest_per_symbol[row_symbol] = row
        if current_ts is not None and (latest_ts is None or current_ts > latest_ts):
            latest_ts = current_ts
    for row_symbol, row in latest_per_symbol.items():
        store.upsert_latest(env=env, venue=venue, stream_type=stream_type, symbol=row_symbol, row=row)
    report = CuratedRefreshReport(
        env=env,
        stream_type=stream_type,
        venue=venue,
        symbol=symbol,
        refreshed_rows=len(rows),
        latest_exchange_ts=latest_ts.isoformat() if latest_ts is not None else None,
        db_path=str(store.path),
        refreshed_at=_utc_now(),
    )
    store.record_refresh(report)
    return report
