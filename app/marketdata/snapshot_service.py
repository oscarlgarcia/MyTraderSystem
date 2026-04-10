from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from app.marketdata.query import HistoricalQueryRequest, query_latest_row
from app.marketdata.serving import CuratedServingStore, serving_db_path


@dataclass(frozen=True, slots=True)
class SnapshotRequest:
    base_dir: Path
    env: str
    stream_type: str
    symbol: str
    venue: str = "BINANCE"
    prefer_curated: bool = True


def load_snapshot(request: SnapshotRequest) -> dict | None:
    if request.prefer_curated:
        store = CuratedServingStore(serving_db_path(request.base_dir, request.env))
        latest = store.latest(env=request.env, venue=request.venue, stream_type=request.stream_type, symbol=request.symbol)
        if latest is not None:
            return latest
    return query_latest_row(
        HistoricalQueryRequest(
            base_dir=request.base_dir,
            env=request.env,
            stream_type=request.stream_type,
            symbol=request.symbol,
            venue=request.venue,
        )
    )
