"""
Source contracts and concrete ingestion sources.

Keep this layer small: it only knows how to fetch/stream normalized MarketEvent data.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Callable, Iterable, Optional, Protocol

import httpx
from websockets.sync.client import connect

from app.common.dto import MarketEvent
from app.config import AppConfig
from app.ingestion.client import build_ws_url, normalize_kline, parse_message


class Source(Protocol):
    def stream(self, end_time: float | None = None) -> Iterable[MarketEvent]: ...
    def snapshot(self) -> Optional[Iterable[MarketEvent]]: ...


def _ws_stream(url: str, end_time: float | None = None) -> Iterable[MarketEvent]:
    with connect(url) as ws:
        while True:
            if end_time and time.time() >= end_time:
                break
            try:
                raw = ws.recv(timeout=1)
            except TimeoutError:
                continue
            yield parse_message(raw)


def source_snapshot_fn(source: Source) -> Callable[[], Iterable[MarketEvent]]:
    def snapshot() -> Iterable[MarketEvent]:
        events = source.snapshot()
        if events is None:
            return []
        return events

    return snapshot


@dataclass
class BinanceSource:
    cfg: AppConfig
    ws_stream: Callable[[str, float | None], Iterable[MarketEvent]] = _ws_stream
    http_get: Callable[..., httpx.Response] = httpx.get

    def stream(self, end_time: float | None = None) -> Iterable[MarketEvent]:
        url = build_ws_url(self.cfg.ws_base, self.cfg.symbols)
        yield from self.ws_stream(url, end_time=end_time)

    def snapshot(self) -> Iterable[MarketEvent]:
        events: list[MarketEvent] = []
        for symbol in self.cfg.symbols:
            url = f"{self.cfg.rest_base.rstrip('/')}/api/v3/klines"
            resp = self.http_get(url, params={"symbol": symbol, "interval": "1m", "limit": 5}, timeout=5.0)
            resp.raise_for_status()
            for row in resp.json():
                payload = {"s": symbol, "E": int(row[6]), "k": {"c": row[4], "q": row[5]}}
                events.append(normalize_kline(payload))
        return events


@dataclass
class StaticSource:
    events: list[MarketEvent] = field(default_factory=list)
    snapshot_events: Optional[list[MarketEvent]] = None

    def stream(self, end_time: float | None = None) -> Iterable[MarketEvent]:
        del end_time
        yield from self.events

    def snapshot(self) -> Optional[Iterable[MarketEvent]]:
        return self.snapshot_events
