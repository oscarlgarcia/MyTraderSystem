"""
Resilient stream loop with simple reconnect/backoff and snapshot re-sync.

Designed to stay lightweight and testable (no threads, no event loop management here).
"""

from __future__ import annotations

import math
import time
import logging
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Callable, Deque, Iterable, List, Optional, Set, Tuple

from app.common.dto import MarketEvent
from app.ingestion.client import _key


SnapshotFn = Callable[[], Iterable[MarketEvent]]
StreamFn = Callable[[], Iterable[MarketEvent]]
Sleeper = Callable[[float], None]

@dataclass
class ResilienceMetrics:
    reconnects: int = 0
    last_lag_seconds: float = 0.0
    buffer_size: int = 0
    buffer_skipped: int = 0
    last_latency_seconds: float = 0.0
    max_latency_seconds: float = 0.0


@dataclass
class ResilientRunner:
    stream_fn: StreamFn
    snapshot_fn: Optional[SnapshotFn] = None
    backoff_base: float = 1.0
    backoff_max: float = 8.0  # keep <10s per requirement
    lag_threshold_seconds: float = 5.0
    max_lag_seconds: float = 10.0
    max_buffer: int = 10_000
    dedup_enabled: bool = True
    sleeper: Sleeper = time.sleep
    metrics: ResilienceMetrics = field(default_factory=ResilienceMetrics)
    last_event_ts: Optional[datetime] = None
    seen: Set[Tuple[str, datetime, float, float, str]] = field(default_factory=set)

    def run(
        self,
        handler: Callable[[MarketEvent], None],
        max_retries: Optional[int] = None,
        stop_on_complete: bool = False,
    ) -> None:
        backoff = self.backoff_base
        buffer: Deque[MarketEvent] = deque()
        while True:
            handled_this_cycle = 0
            try:
                for ev in self.stream_fn():
                    if len(buffer) >= self.max_buffer:
                        self.metrics.buffer_skipped += 1
                        continue
                    buffer.append(ev)
                    self.metrics.buffer_size = len(buffer)
                    while buffer:
                        current = buffer.popleft()
                        self._process_event(current, handler)
                        handled_this_cycle += 1
                    backoff = self.backoff_base  # reset on success
                if stop_on_complete:
                    # If we are in a finite run mode and consumed the stream (even empty), exit.
                    break
            except StopIteration:
                break
            except Exception as exc:
                # Generators that explicitly raise StopIteration in Python 3.11+ surface as
                # RuntimeError("generator raised StopIteration") due to PEP 479. Treat that
                # as a clean completion when stop_on_complete is requested.
                if stop_on_complete and isinstance(exc, RuntimeError) and "StopIteration" in str(exc):
                    break
                self.metrics.reconnects += 1
                if max_retries is not None and self.metrics.reconnects > max_retries:
                    raise
                self.sleeper(min(backoff, self.backoff_max))
                backoff = min(backoff * 2, self.backoff_max)
                continue

            # Extra guard: if stop_on_complete is requested and no events were processed (empty stream),
            # avoid looping forever.
            if stop_on_complete and handled_this_cycle == 0:
                break

    def _process_event(self, ev: MarketEvent, handler: Callable[[MarketEvent], None]) -> None:
        # dedup
        if self.dedup_enabled:
            k = _key(ev)
            if k in self.seen:
                return

        # gap detection
        if self.last_event_ts:
            lag = (ev.event_ts - self.last_event_ts).total_seconds()
            self.metrics.last_lag_seconds = max(self.metrics.last_lag_seconds, lag)
            if lag > self.lag_threshold_seconds and self.snapshot_fn:
                self._resync(handler)
                # The snapshot may have already delivered this event (or a duplicate),
                # so skip if it's now seen to avoid double handling.
                if k in self.seen:
                    return
            if lag > self.max_lag_seconds:
                # log via handler extra if it accepts 'warning' pattern? we can't assume; emit via logging module
                logging.getLogger("ingest.resilience").warning(
                    "Lag exceeds max_lag_seconds",
                    extra={"lag_seconds": lag, "max_lag_seconds": self.max_lag_seconds},
                )

        handler(ev)
        if self.dedup_enabled:
            self.seen.add(k)
        self.last_event_ts = ev.event_ts

        # latency from event_ts to processing time
        now = datetime.now(timezone.utc)
        latency = max(0.0, (now - ev.event_ts).total_seconds())
        self.metrics.last_latency_seconds = latency
        if latency > self.metrics.max_latency_seconds:
            self.metrics.max_latency_seconds = latency

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
