"""
Resilient stream loop with simple reconnect/backoff and snapshot re-sync.

Designed to stay lightweight and testable (no threads, no event loop management here).
"""

from __future__ import annotations

import math
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Callable, Iterable, List, Optional, Set, Tuple

from app.common.dto import MarketEvent


SnapshotFn = Callable[[], Iterable[MarketEvent]]
StreamFn = Callable[[], Iterable[MarketEvent]]
Sleeper = Callable[[float], None]


def _key(ev: MarketEvent) -> Tuple[str, datetime, float, float, str]:
    return (ev.symbol, ev.event_ts, ev.price, ev.size, ev.source)


@dataclass
class ResilienceMetrics:
    reconnects: int = 0
    last_lag_seconds: float = 0.0


@dataclass
class ResilientRunner:
    stream_fn: StreamFn
    snapshot_fn: Optional[SnapshotFn] = None
    backoff_base: float = 1.0
    backoff_max: float = 8.0  # keep <10s per requirement
    lag_threshold_seconds: float = 5.0
    sleeper: Sleeper = time.sleep
    metrics: ResilienceMetrics = field(default_factory=ResilienceMetrics)
    last_event_ts: Optional[datetime] = None
    seen: Set[Tuple[str, datetime, float, float, str]] = field(default_factory=set)

    def run(self, handler: Callable[[MarketEvent], None], max_retries: Optional[int] = None) -> None:
        backoff = self.backoff_base
        while True:
            try:
                for ev in self.stream_fn():
                    self._process_event(ev, handler)
                    backoff = self.backoff_base  # reset on success
            except StopIteration:
                break
            except Exception:
                self.metrics.reconnects += 1
                if max_retries is not None and self.metrics.reconnects > max_retries:
                    raise
                self.sleeper(min(backoff, self.backoff_max))
                backoff = min(backoff * 2, self.backoff_max)
                continue

    def _process_event(self, ev: MarketEvent, handler: Callable[[MarketEvent], None]) -> None:
        # dedup
        k = _key(ev)
        if k in self.seen:
            return

        # gap detection
        if self.last_event_ts:
            lag = (ev.event_ts - self.last_event_ts).total_seconds()
            self.metrics.last_lag_seconds = max(self.metrics.last_lag_seconds, lag)
            if lag > self.lag_threshold_seconds and self.snapshot_fn:
                self._resync(handler)

        handler(ev)
        self.seen.add(k)
        self.last_event_ts = ev.event_ts

    def _resync(self, handler: Callable[[MarketEvent], None]) -> None:
        if not self.snapshot_fn:
            return
        for ev in self.snapshot_fn():
            k = _key(ev)
            if k in self.seen:
                continue
            handler(ev)
            self.seen.add(k)
            self.last_event_ts = max(self.last_event_ts or ev.event_ts, ev.event_ts)
