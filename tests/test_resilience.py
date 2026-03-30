import io
import logging
from datetime import datetime, timedelta, timezone

import pytest

from app.common.dto import MarketEvent
from app.ingestion.resilience import ResilientRunner


def make_ev(ts: datetime) -> MarketEvent:
    return MarketEvent(symbol="BTCUSDT", event_ts=ts, price=1.0, size=1.0, source="trade")


def test_reconnect_after_drop(monkeypatch):
    calls = []
    attempts = {"n": 0}

    def stream():
        attempts["n"] += 1
        if attempts["n"] == 1:
            raise RuntimeError("drop")
        yield make_ev(datetime.now(timezone.utc))

    def handler(ev):
        calls.append(ev)

    sleeps = []
    runner = ResilientRunner(stream_fn=stream, sleeper=lambda s: sleeps.append(s), backoff_base=0.1, backoff_max=0.2)
    runner.run(handler, max_retries=2, stop_on_complete=True)
    assert runner.metrics.reconnects >= 1
    assert calls, "handler should be called after reconnection"
    assert all(s <= 0.2 for s in sleeps)


def test_resync_adds_snapshot_without_duplicates():
    base = datetime(2024, 1, 1, tzinfo=timezone.utc)
    stream_events = [
        make_ev(base),
        make_ev(base + timedelta(seconds=10)),  # gap > threshold
    ]
    snapshot_events = [
        make_ev(base + timedelta(seconds=5)),  # fills gap
        make_ev(base + timedelta(seconds=10)),  # duplicate
    ]
    handled = []

    def stream():
        for ev in stream_events:
            yield ev
        raise StopIteration

    runner = ResilientRunner(
        stream_fn=stream,
        snapshot_fn=lambda: snapshot_events,
        lag_threshold_seconds=2,
        sleeper=lambda s: None,
    )

    def handler(ev):
        handled.append(ev.event_ts)

    try:
        runner.run(handler, stop_on_complete=True)
    except StopIteration:
        pass

    # Expect 3 unique timestamps (0s,5s,10s) in order of handling
    assert len(handled) == 3
    assert handled[1] == base + timedelta(seconds=5)
    assert handled[2] == base + timedelta(seconds=10)


def test_backoff_capped():
    sleeps = []

    def stream():
        raise RuntimeError("drop")

    runner = ResilientRunner(stream_fn=stream, sleeper=lambda s: sleeps.append(s), backoff_base=5, backoff_max=8)
    try:
        runner.run(lambda ev: None, max_retries=2, stop_on_complete=True)
    except RuntimeError:
        pass
    assert sleeps
    assert all(s <= 8 for s in sleeps)


def test_max_retries_raises_after_limit():
    def stream():
        raise RuntimeError("always")

    runner = ResilientRunner(stream_fn=stream, sleeper=lambda s: None)
    with pytest.raises(RuntimeError):
        runner.run(lambda ev: None, max_retries=1, stop_on_complete=True)


def test_last_lag_updates():
    base = datetime(2024, 1, 1, tzinfo=timezone.utc)
    events = [make_ev(base), make_ev(base + timedelta(seconds=3))]

    def stream():
        for ev in events:
            yield ev

    runner = ResilientRunner(stream_fn=stream, snapshot_fn=None, lag_threshold_seconds=1, sleeper=lambda s: None)
    runner.run(lambda ev: None, stop_on_complete=True)
    assert runner.metrics.last_lag_seconds >= 3


def test_dedup_stream_duplicates():
    base = datetime(2024, 1, 1, tzinfo=timezone.utc)
    ev = make_ev(base)

    def stream():
        yield ev
        yield ev

    handled = []
    runner = ResilientRunner(stream_fn=stream, snapshot_fn=None, sleeper=lambda s: None)
    runner.run(lambda e: handled.append(e), stop_on_complete=True)
    assert len(handled) == 1


def test_stop_on_complete_empty_stream_exits():
    def stream():
        if False:
            yield  # pragma: no cover

    runner = ResilientRunner(stream_fn=stream, snapshot_fn=None, sleeper=lambda s: None)
    runner.run(lambda ev: None, stop_on_complete=True)
    assert runner.metrics.reconnects == 0


def test_gap_without_snapshot_skips_resync():
    base = datetime(2024, 1, 1, tzinfo=timezone.utc)
    events = [make_ev(base), make_ev(base + timedelta(seconds=10))]
    handled = []

    def stream():
        for ev in events:
            yield ev

    runner = ResilientRunner(stream_fn=stream, snapshot_fn=None, lag_threshold_seconds=2, sleeper=lambda s: None)
    runner.run(lambda ev: handled.append(ev), stop_on_complete=True)
    assert len(handled) == 2
    assert runner.metrics.last_lag_seconds >= 10


def test_buffer_skip_when_overflow():
    base = datetime(2024, 1, 1, tzinfo=timezone.utc)

    def stream():
        for i in range(5):
            yield make_ev(base + timedelta(seconds=i))

    handled = []

    def slow_handler(ev):
        handled.append(ev)
        # no sleep needed; skip logic is based on buffer size

    runner = ResilientRunner(stream_fn=stream, snapshot_fn=None, max_buffer=0, sleeper=lambda s: None)
    runner.run(slow_handler, stop_on_complete=True, max_retries=0)
    # se deberían haber descartado al menos 1 evento
    assert runner.metrics.buffer_skipped > 0
    assert runner.metrics.buffer_size <= 0


def test_warning_when_lag_exceeds():
    base = datetime(2024, 1, 1, tzinfo=timezone.utc)
    events = [make_ev(base), make_ev(base + timedelta(seconds=20))]

    def stream():
        for ev in events:
            yield ev

    buffer = io.StringIO()
    logger = logging.getLogger("ingest.resilience")
    handler = logging.StreamHandler(buffer)
    logger.handlers = [handler]
    logger.setLevel(logging.WARNING)
    logger.propagate = False
    try:
        runner = ResilientRunner(stream_fn=stream, snapshot_fn=None, max_lag_seconds=5, sleeper=lambda s: None)
        runner.run(lambda ev: None, stop_on_complete=True)
    finally:
        logger.handlers = []
        logger.propagate = True
    assert "Lag exceeds max_lag_seconds" in buffer.getvalue()


def test_latency_metrics_updated():
    base = datetime(2023, 1, 1, tzinfo=timezone.utc)
    events = [make_ev(base)]

    def stream():
        for ev in events:
            yield ev
        raise StopIteration

    runner = ResilientRunner(stream_fn=stream, snapshot_fn=None, sleeper=lambda s: None, lag_threshold_seconds=2)
    runner.run(lambda ev: None, stop_on_complete=True)
    assert runner.metrics.last_latency_seconds >= 0
    assert runner.metrics.max_latency_seconds >= runner.metrics.last_latency_seconds
