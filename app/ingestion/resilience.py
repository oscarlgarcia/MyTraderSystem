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
from typing import Callable, Deque, Iterable, List, Literal, Optional

from app.common.dto import MarketEvent
from app.ingestion.client import _key
from app.ingestion.checkpoints import CheckpointState
from app.ingestion.dedup import Deduplicator
from app.ingestion.errors import IngestionError, classify_error


SnapshotFn = Callable[[], Iterable[MarketEvent]]
StreamFn = Callable[[], Iterable[MarketEvent]]
Sleeper = Callable[[float], None]
BackpressurePolicy = Literal["pause", "drop_oldest", "drop_newest", "fail"]

@dataclass
class ResilienceMetrics:
    # events_in counts source/snapshot events observed before runner-level dedup.
    events_in: int = 0
    # events_out counts events delivered to the handler after runner-level dedup.
    events_out: int = 0
    # dedup_skipped counts duplicates filtered by the runner or snapshot re-sync path.
    dedup_skipped: int = 0
    reconnects: int = 0
    last_lag_seconds: float = 0.0
    buffer_size: int = 0
    buffer_skipped: int = 0
    buffer_overflows: int = 0
    buffer_pauses: int = 0
    buffer_drop_oldest: int = 0
    buffer_drop_newest: int = 0
    buffer_failures: int = 0
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
    backpressure_policy: BackpressurePolicy = "pause"
    backpressure_pause_seconds: float = 0.01
    read_burst_size: int = 64
    dedup_enabled: bool = True
    sleeper: Sleeper = time.sleep
    metrics: ResilienceMetrics = field(default_factory=ResilienceMetrics)
    last_event_ts: Optional[datetime] = None
    deduplicator: Deduplicator = field(default_factory=Deduplicator)

    def restore_checkpoint(self, state: CheckpointState | None) -> None:
        if state is None:
            return
        self.last_event_ts = state.last_event_ts
        self.deduplicator.restore_entries(state.seen_entries)

    def export_checkpoint(self, *, metadata: dict[str, object] | None = None) -> CheckpointState:
        return CheckpointState(
            last_event_ts=self.last_event_ts,
            seen_entries=self.deduplicator.export_entries(),
            metadata=dict(metadata or {}),
        )

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
                stream_iter = iter(self.stream_fn())
                stream_exhausted = False
                while not stream_exhausted:
                    read_this_burst = 0
                    while read_this_burst < max(1, self.read_burst_size):
                        try:
                            ev = next(stream_iter)
                        except StopIteration:
                            stream_exhausted = True
                            break
                        except RuntimeError as exc:
                            if "StopIteration" in str(exc):
                                stream_exhausted = True
                                break
                            raise
                        self._enqueue_event(buffer, ev, handler)
                        self.metrics.buffer_size = len(buffer)
                        read_this_burst += 1
                        backoff = self.backoff_base  # reset on success
                    handled_this_cycle += self._drain_buffer(buffer, handler)
                    self.metrics.buffer_size = len(buffer)
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
                err = classify_error(exc, default_category="source")
                if not err.retryable:
                    raise err
                self.metrics.reconnects += 1
                if max_retries is not None and self.metrics.reconnects > max_retries:
                    raise err
                self.sleeper(min(backoff, self.backoff_max))
                backoff = min(backoff * 2, self.backoff_max)
                continue

            # Extra guard: if stop_on_complete is requested and no events were processed (empty stream),
            # avoid looping forever.
            if stop_on_complete and handled_this_cycle == 0 and not buffer:
                break

    def _enqueue_event(
        self,
        buffer: Deque[MarketEvent],
        ev: MarketEvent,
        handler: Callable[[MarketEvent], None],
    ) -> None:
        if len(buffer) < self.max_buffer:
            buffer.append(ev)
            return
        self.metrics.buffer_overflows += 1
        logger = logging.getLogger("ingest.resilience")
        if self.backpressure_policy == "pause":
            self.metrics.buffer_pauses += 1
            self._drain_buffer(buffer, handler, limit=1)
            self.sleeper(self.backpressure_pause_seconds)
            buffer.append(ev)
            logger.warning(
                "backpressure pause applied",
                extra={
                    "backpressure_policy": self.backpressure_policy,
                    "buffer_size": len(buffer),
                    "buffer_pauses": self.metrics.buffer_pauses,
                },
            )
            return
        if self.backpressure_policy == "drop_oldest":
            if buffer:
                buffer.popleft()
            self.metrics.buffer_skipped += 1
            self.metrics.buffer_drop_oldest += 1
            buffer.append(ev)
            logger.warning(
                "backpressure drop_oldest applied",
                extra={
                    "backpressure_policy": self.backpressure_policy,
                    "buffer_size": len(buffer),
                    "buffer_skipped": self.metrics.buffer_skipped,
                    "buffer_drop_oldest": self.metrics.buffer_drop_oldest,
                },
            )
            return
        if self.backpressure_policy == "drop_newest":
            self.metrics.buffer_skipped += 1
            self.metrics.buffer_drop_newest += 1
            logger.warning(
                "backpressure drop_newest applied",
                extra={
                    "backpressure_policy": self.backpressure_policy,
                    "buffer_size": len(buffer),
                    "buffer_skipped": self.metrics.buffer_skipped,
                    "buffer_drop_newest": self.metrics.buffer_drop_newest,
                },
            )
            return
        self.metrics.buffer_failures += 1
        raise IngestionError(
            "sink",
            "transient",
            f"buffer overloaded with fail policy (size={len(buffer)}, max_buffer={self.max_buffer})",
        )

    def _drain_buffer(
        self,
        buffer: Deque[MarketEvent],
        handler: Callable[[MarketEvent], None],
        *,
        limit: int | None = None,
    ) -> int:
        handled = 0
        while buffer and (limit is None or handled < limit):
            current = buffer.popleft()
            try:
                self._process_event(current, handler)
            except StopIteration:
                raise
            except RuntimeError as exc:
                if "StopIteration" in str(exc):
                    raise StopIteration from exc
                raise classify_error(exc, default_category="sink") from exc
            except Exception as exc:
                raise classify_error(exc, default_category="sink") from exc
            handled += 1
        return handled

    def _process_event(self, ev: MarketEvent, handler: Callable[[MarketEvent], None]) -> None:
        self.metrics.events_in += 1
        k = _key(ev)
        # dedup
        if self.dedup_enabled:
            if self.deduplicator.contains_key(k):
                self.metrics.dedup_skipped += 1
                return

        # gap detection
        if self.last_event_ts:
            lag = (ev.event_ts - self.last_event_ts).total_seconds()
            self.metrics.last_lag_seconds = max(self.metrics.last_lag_seconds, lag)
            if lag > self.lag_threshold_seconds and self.snapshot_fn:
                self._resync(handler)
                # The snapshot may have already delivered this event (or a duplicate),
                # so skip if it's now seen to avoid double handling.
                if self.deduplicator.contains_key(k):
                    return
            if lag > self.max_lag_seconds:
                # log via handler extra if it accepts 'warning' pattern? we can't assume; emit via logging module
                logging.getLogger("ingest.resilience").warning(
                    "Lag exceeds max_lag_seconds",
                    extra={"lag_seconds": lag, "max_lag_seconds": self.max_lag_seconds},
                )

        handler(ev)
        self.metrics.events_out += 1
        if self.dedup_enabled:
            self.deduplicator.remember_key(k)
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
            self.metrics.events_in += 1
            k = _key(ev)
            if self.dedup_enabled and self.deduplicator.contains_key(k):
                self.metrics.dedup_skipped += 1
                continue
            handler(ev)
            self.metrics.events_out += 1
            if self.dedup_enabled:
                self.deduplicator.remember_key(k)
            self.last_event_ts = max(self.last_event_ts or ev.event_ts, ev.event_ts)
