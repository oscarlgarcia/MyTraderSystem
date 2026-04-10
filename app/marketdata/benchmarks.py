from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from time import perf_counter

from app.marketdata.query import HistoricalQueryRequest, query_rows
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


def benchmark_query_and_serving(*, base_dir: Path, env: str, stream_type: str, symbol: str, venue: str = "BINANCE") -> MarketdataBenchmarkReport:
    query_request = HistoricalQueryRequest(base_dir=base_dir, env=env, stream_type=stream_type, symbol=symbol, venue=venue)
    started = perf_counter()
    rows = query_rows(query_request)
    query_latency = perf_counter() - started
    store = CuratedServingStore(serving_db_path(base_dir, env))
    started = perf_counter()
    latest = store.latest(env=env, venue=venue, stream_type=stream_type, symbol=symbol)
    serving_latency = perf_counter() - started
    return MarketdataBenchmarkReport(
        env=env,
        stream_type=stream_type,
        symbol=symbol,
        query_rows=len(rows),
        query_latency_seconds=query_latency,
        serving_hit=latest is not None,
        serving_latency_seconds=serving_latency,
    )
