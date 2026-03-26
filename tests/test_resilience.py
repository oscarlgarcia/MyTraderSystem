from datetime import datetime, timedelta, timezone

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
    runner.run(handler, max_retries=2)
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
        runner.run(handler)
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
        runner.run(lambda ev: None, max_retries=2)
    except RuntimeError:
        pass
    assert sleeps
    assert all(s <= 8 for s in sleeps)
