from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from time import perf_counter

from app.marketdata.query import HistoricalQueryRequest, query_rows
from app.marketdata.snapshot_service import SnapshotRequest, load_snapshot
from app.marketdata.publication import publication_path
from app.marketdata.serving import CuratedServingStore, serving_db_path


@dataclass(frozen=True, slots=True)
class MarketdataBenchmarkReport:
    env: str
    stream_type: str
    symbol: str
    query_rows: int
    query_latency_seconds: float
    serving_hit: bool
    serving_latency_seconds: float
    snapshot_hit: bool
    snapshot_latency_seconds: float
    publication_lines: int
    publication_scan_latency_seconds: float


def benchmark_query_and_serving(*, base_dir: Path, env: str, stream_type: str, symbol: str, venue: str = "BINANCE") -> MarketdataBenchmarkReport:
    query_request = HistoricalQueryRequest(base_dir=base_dir, env=env, stream_type=stream_type, symbol=symbol, venue=venue)
    started = perf_counter()
    rows = query_rows(query_request)
    query_latency = perf_counter() - started
    store = CuratedServingStore(serving_db_path(base_dir, env))
    started = perf_counter()
    latest = store.latest(env=env, venue=venue, stream_type=stream_type, symbol=symbol)
    serving_latency = perf_counter() - started
    started = perf_counter()
    snapshot = load_snapshot(SnapshotRequest(base_dir=base_dir, env=env, stream_type=stream_type, symbol=symbol, venue=venue))
    snapshot_latency = perf_counter() - started
    started = perf_counter()
    publication_file = publication_path(base_dir, env, stream_type=stream_type, venue=venue)
    publication_lines = 0
    if publication_file.exists():
        publication_lines = sum(1 for _ in publication_file.open("r", encoding="utf-8"))
    publication_latency = perf_counter() - started
    return MarketdataBenchmarkReport(
        env=env,
        stream_type=stream_type,
        symbol=symbol,
        query_rows=len(rows),
        query_latency_seconds=query_latency,
        serving_hit=latest is not None,
        serving_latency_seconds=serving_latency,
        snapshot_hit=snapshot is not None,
        snapshot_latency_seconds=snapshot_latency,
        publication_lines=publication_lines,
        publication_scan_latency_seconds=publication_latency,
    )
