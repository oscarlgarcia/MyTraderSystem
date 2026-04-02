import io
import json
import logging
from datetime import datetime, timedelta, timezone

import pytest

from app.common.dto import MarketEvent
from app.ingestion.errors import IngestionError
from app.ingestion.resilience import ResilientRunner
from app.marketdata.errors import VendorReplayStaleDataError
from app.marketdata.models import BarEvent, TradeEvent
from app.observability.logger import get_logger


def make_ev(ts: datetime) -> MarketEvent:
    return MarketEvent(symbol="BTCUSDT", event_ts=ts, price=1.0, size=1.0, source="trade")


def make_ev_for(symbol: str, ts: datetime) -> MarketEvent:
    return MarketEvent(symbol=symbol, event_ts=ts, price=1.0, size=1.0, source="trade")


def make_bar(symbol: str, ts: datetime) -> BarEvent:
    return BarEvent(
        symbol=symbol,
        exchange_ts=ts,
        receive_ts=ts,
        process_ts=ts,
        venue="BINANCE",
        open=100.0,
        high=101.0,
        low=99.0,
        close=100.5,
        volume=5.0,
        interval="1m",
        open_ts=ts - timedelta(minutes=1),
        close_ts=ts,
    )


def _json_lines(buffer: io.StringIO) -> list[dict[str, object]]:
    return [json.loads(line) for line in buffer.getvalue().splitlines() if line.strip()]


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
        make_bar("BTCUSDT", base),
        make_bar("BTCUSDT", base + timedelta(seconds=10)),  # gap > threshold
    ]
    snapshot_events = [
        make_bar("BTCUSDT", base + timedelta(seconds=5)),  # fills gap
        make_bar("BTCUSDT", base + timedelta(seconds=10)),  # duplicate
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
    assert runner.metrics.snapshot_runs == 1
    assert runner.metrics.snapshot_rows == 2
    assert runner.metrics.snapshot_duplicates_skipped == 1


def test_backoff_capped():
    sleeps = []

    def stream():
        raise RuntimeError("drop")

    runner = ResilientRunner(
        stream_fn=stream,
        sleeper=lambda s: sleeps.append(s),
        backoff_base=5,
        backoff_max=8,
        jitter_fn=lambda delay: delay,
    )
    with pytest.raises(IngestionError) as exc_info:
        runner.run(lambda ev: None, max_retries=2, stop_on_complete=True)
    assert sleeps
    assert all(s <= 8 for s in sleeps)
    assert exc_info.value.category == "source"


def test_max_retries_raises_after_limit():
    def stream():
        raise RuntimeError("always")

    runner = ResilientRunner(stream_fn=stream, sleeper=lambda s: None, jitter_fn=lambda delay: delay)
    with pytest.raises(IngestionError) as exc_info:
        runner.run(lambda ev: None, max_retries=1, stop_on_complete=True)
    assert exc_info.value.category == "source"


def test_retry_jitter_can_be_made_deterministic():
    sleeps = []

    def stream():
        raise RuntimeError("drop")

    runner = ResilientRunner(
        stream_fn=stream,
        sleeper=lambda seconds: sleeps.append(seconds),
        backoff_base=2.0,
        backoff_max=8.0,
        jitter_fn=lambda delay: delay + 0.5,
    )
    with pytest.raises(IngestionError):
        runner.run(lambda ev: None, max_retries=1, stop_on_complete=True)

    assert sleeps == [2.5]


def test_last_lag_updates():
    base = datetime(2024, 1, 1, tzinfo=timezone.utc)
    events = [make_ev(base), make_ev(base + timedelta(seconds=3))]

    def stream():
        for ev in events:
            yield ev

    runner = ResilientRunner(stream_fn=stream, snapshot_fn=None, lag_threshold_seconds=1, sleeper=lambda s: None)
    runner.run(lambda ev: None, stop_on_complete=True)
    assert runner.metrics.last_event_gap_seconds >= 3
    assert runner.metrics.max_event_gap_seconds >= 3


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
    assert runner.metrics.max_event_gap_seconds >= 10


def test_pause_policy_limits_ingestion_rate_under_pressure():
    base = datetime(2024, 1, 1, tzinfo=timezone.utc)
    sleeps = []
    handled = []

    def stream():
        for i in range(6):
            yield make_ev(base + timedelta(seconds=i))

    runner = ResilientRunner(
        stream_fn=stream,
        snapshot_fn=None,
        max_buffer=2,
        read_burst_size=6,
        backpressure_policy="pause",
        backpressure_pause_seconds=0.05,
        sleeper=lambda seconds: sleeps.append(seconds),
    )
    runner.run(lambda ev: handled.append(ev), stop_on_complete=True, max_retries=0)

    assert len(handled) == 6
    assert runner.metrics.buffer_pauses > 0
    assert sleeps


def test_drop_policy_counts_and_logs_losses():
    base = datetime(2024, 1, 1, tzinfo=timezone.utc)

    def stream():
        for i in range(6):
            yield make_ev(base + timedelta(seconds=i))

    handled = []
    buffer = io.StringIO()
    logger = logging.getLogger("ingest.resilience")
    handler = logging.StreamHandler(buffer)
    logger.handlers = [handler]
    logger.setLevel(logging.WARNING)
    logger.propagate = False
    try:
        runner = ResilientRunner(
            stream_fn=stream,
            snapshot_fn=None,
            max_buffer=2,
            read_burst_size=6,
            backpressure_policy="drop_newest",
            sleeper=lambda s: None,
        )
        runner.run(lambda ev: handled.append(ev), stop_on_complete=True, max_retries=0)
    finally:
        logger.handlers = []
        logger.propagate = True

    assert runner.metrics.buffer_skipped > 0
    assert runner.metrics.buffer_drop_newest > 0
    assert len(handled) < 6
    assert "backpressure drop_newest applied" in buffer.getvalue()


def test_fail_policy_aborts_cleanly_on_overload():
    base = datetime(2024, 1, 1, tzinfo=timezone.utc)

    def stream():
        for i in range(6):
            yield make_ev(base + timedelta(seconds=i))

    runner = ResilientRunner(
        stream_fn=stream,
        snapshot_fn=None,
        max_buffer=2,
        read_burst_size=6,
        backpressure_policy="fail",
        sleeper=lambda s: None,
    )

    with pytest.raises(IngestionError) as exc_info:
        runner.run(lambda ev: None, stop_on_complete=True, max_retries=0)
    assert exc_info.value.category == "sink"
    assert runner.metrics.buffer_failures > 0


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
    assert "Event gap exceeds max_lag_seconds" in buffer.getvalue()


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


def test_out_of_order_event_is_handled_per_policy():
    base = datetime(2024, 1, 1, tzinfo=timezone.utc)
    events = [make_ev(base + timedelta(seconds=10)), make_ev(base)]
    handled = []

    def stream():
        for ev in events:
            yield ev

    runner = ResilientRunner(
        stream_fn=stream,
        snapshot_fn=None,
        temporal_policy="drop",
        sleeper=lambda s: None,
    )
    runner.run(lambda ev: handled.append(ev), stop_on_complete=True)

    assert len(handled) == 1
    assert runner.metrics.out_of_order_events == 1
    assert runner.metrics.late_events == 1
    assert runner.metrics.late_events_dropped == 1
    assert runner.metrics.max_late_seconds >= 10


def test_snapshot_resync_does_not_duplicate_recent_stream_events():
    base = datetime(2024, 1, 1, tzinfo=timezone.utc)
    handled = []

    def stream():
        yield make_bar("BTCUSDT", base)
        yield make_bar("BTCUSDT", base + timedelta(seconds=12))

    snapshot_events = [
        make_bar("BTCUSDT", base + timedelta(seconds=5)),
        make_bar("BTCUSDT", base + timedelta(seconds=12)),
    ]

    runner = ResilientRunner(
        stream_fn=stream,
        snapshot_fn=lambda: snapshot_events,
        lag_threshold_seconds=2,
        sleeper=lambda s: None,
    )
    runner.run(lambda ev: handled.append(ev.event_ts), stop_on_complete=True)

    assert handled == [
        base,
        base + timedelta(seconds=5),
        base + timedelta(seconds=12),
    ]
    assert runner.metrics.snapshot_runs == 1
    assert runner.metrics.snapshot_duplicates_skipped == 1


def test_multi_symbol_interleaving_does_not_create_false_late_or_gap_metrics():
    base = datetime(2024, 1, 1, tzinfo=timezone.utc)
    events = [
        make_ev_for("BTCUSDT", base + timedelta(seconds=20)),
        make_ev_for("ETHUSDT", base + timedelta(seconds=5)),
        make_ev_for("BTCUSDT", base + timedelta(seconds=25)),
        make_ev_for("ETHUSDT", base + timedelta(seconds=10)),
    ]
    handled = []

    def stream():
        for ev in events:
            yield ev

    runner = ResilientRunner(
        stream_fn=stream,
        snapshot_fn=None,
        lag_threshold_seconds=2,
        sleeper=lambda s: None,
    )
    runner.run(lambda ev: handled.append((ev.symbol, ev.event_ts)), stop_on_complete=True)

    assert handled == [(ev.symbol, ev.event_ts) for ev in events]
    assert runner.metrics.late_events == 0
    assert runner.metrics.out_of_order_events == 0
    assert runner.metrics.max_event_gap_seconds == 5.0
    assert runner.metrics.last_event_gap_seconds == 5.0
    assert set(runner.metrics.temporal_streams) == {
        "BINANCE:BTCUSDT:trade",
        "BINANCE:ETHUSDT:trade",
    }
    assert runner.metrics.temporal_streams["BINANCE:BTCUSDT:trade"]["max_event_gap_seconds"] == 5.0
    assert runner.metrics.temporal_streams["BINANCE:ETHUSDT:trade"]["max_event_gap_seconds"] == 5.0


def test_broken_sequence_generates_detectable_gap():
    base = datetime(2024, 1, 1, tzinfo=timezone.utc)
    events = [
        TradeEvent(
            symbol="BTCUSDT",
            exchange_ts=base,
            receive_ts=base,
            process_ts=base,
            venue="BINANCE",
            source_id="1",
            price=100.0,
            size=1.0,
            trade_id="1",
        ),
        TradeEvent(
            symbol="BTCUSDT",
            exchange_ts=base + timedelta(seconds=1),
            receive_ts=base + timedelta(seconds=1),
            process_ts=base + timedelta(seconds=1),
            venue="BINANCE",
            source_id="3",
            price=101.0,
            size=1.0,
            trade_id="3",
        ),
    ]
    runner = ResilientRunner(
        stream_fn=lambda: iter(events),
        snapshot_fn=None,
        lag_threshold_seconds=60,
        sleeper=lambda s: None,
    )
    runner.run(lambda ev: None, stop_on_complete=True)

    stream_metrics = runner.metrics.temporal_streams["BINANCE:BTCUSDT:trade"]
    assert runner.metrics.gaps_total == 1
    assert runner.metrics.gap_irreparable_total == 1
    assert stream_metrics["gap_detected"] is True
    assert stream_metrics["gap_irreparable"] is True
    assert stream_metrics["last_gap_detection_mode"] == "sequence_gap_detection"
    assert stream_metrics["last_gap_missing_count"] == 1


def test_temporal_gap_heuristic_is_marked_weak_not_strong():
    base = datetime(2024, 1, 1, tzinfo=timezone.utc)
    events = [make_ev(base), make_ev(base + timedelta(seconds=10))]
    runner = ResilientRunner(
        stream_fn=lambda: iter(events),
        snapshot_fn=None,
        lag_threshold_seconds=2,
        sleeper=lambda s: None,
    )
    runner.run(lambda ev: None, stop_on_complete=True)

    stream_metrics = runner.metrics.temporal_streams["BINANCE:BTCUSDT:trade"]
    assert runner.metrics.gaps_total == 1
    assert runner.metrics.gap_irreparable_total == 0
    assert stream_metrics["gap_detected"] is True
    assert stream_metrics["gap_irreparable"] is False
    assert stream_metrics["last_gap_detection_mode"] == "weak_gap_detection"
    assert stream_metrics["last_gap_missing_count"] == 0
    assert stream_metrics["last_gap_seconds"] == 10.0


def test_gap_alerts_are_emitted_with_stream_context():
    base = datetime(2024, 1, 1, tzinfo=timezone.utc)
    buffer = io.StringIO()
    get_logger(name="ingest.resilience", level="INFO", stream=buffer)
    events = [
        TradeEvent(
            symbol="BTCUSDT",
            exchange_ts=base,
            receive_ts=base,
            process_ts=base,
            venue="BINANCE",
            price=100.0,
            size=1.0,
            trade_id="1",
        ),
        TradeEvent(
            symbol="BTCUSDT",
            exchange_ts=base + timedelta(seconds=1),
            receive_ts=base + timedelta(seconds=1),
            process_ts=base + timedelta(seconds=1),
            venue="BINANCE",
            price=101.0,
            size=1.0,
            trade_id="3",
        ),
    ]

    runner = ResilientRunner(
        stream_fn=lambda: iter(events),
        snapshot_fn=None,
        lag_threshold_seconds=0.5,
        sleeper=lambda _: None,
    )
    runner.run(lambda _event: None, stop_on_complete=True)

    alerts = [record for record in _json_lines(buffer) if record["message"] == "operational alert"]
    detected = next(record for record in alerts if record["alert_type"] == "gap_detected")
    irreparable = next(record for record in alerts if record["alert_type"] == "gap_irreparable")
    assert detected["venue"] == "BINANCE"
    assert detected["symbol"] == "BTCUSDT"
    assert detected["stream_type"] == "trade"
    assert detected["alert_severity"] == "warning"
    assert irreparable["alert_severity"] == "error"
    assert irreparable["error_type"] == "IrrecoverableGapError"


def test_trade_gap_without_exact_recovery_is_marked_irreparable_even_with_snapshot():
    base = datetime(2024, 1, 1, tzinfo=timezone.utc)
    trade_events = [
        TradeEvent(
            symbol="BTCUSDT",
            exchange_ts=base,
            receive_ts=base,
            process_ts=base,
            venue="BINANCE",
            source_id="1",
            price=100.0,
            size=1.0,
            trade_id="1",
        ),
        TradeEvent(
            symbol="BTCUSDT",
            exchange_ts=base + timedelta(seconds=1),
            receive_ts=base + timedelta(seconds=1),
            process_ts=base + timedelta(seconds=1),
            venue="BINANCE",
            source_id="3",
            price=101.0,
            size=1.0,
            trade_id="3",
        ),
    ]
    snapshot_events = [make_bar("BTCUSDT", base + timedelta(minutes=1))]
    handled = []
    runner = ResilientRunner(
        stream_fn=lambda: iter(trade_events),
        snapshot_fn=lambda: snapshot_events,
        lag_threshold_seconds=60,
        sleeper=lambda s: None,
    )
    runner.run(lambda ev: handled.append(ev), stop_on_complete=True)

    stream_metrics = runner.metrics.temporal_streams["BINANCE:BTCUSDT:trade"]
    assert len(handled) == 2
    assert runner.metrics.snapshot_runs == 0
    assert runner.metrics.gap_irreparable_total == 1
    assert stream_metrics["gap_irreparable"] is True
    assert stream_metrics["last_gap_detection_mode"] == "sequence_gap_detection"


def test_out_of_order_fail_policy_raises_typed_vendor_replay_error():
    base = datetime(2024, 1, 1, tzinfo=timezone.utc)
    events = [make_ev(base + timedelta(seconds=10)), make_ev(base)]

    runner = ResilientRunner(
        stream_fn=lambda: iter(events),
        snapshot_fn=None,
        temporal_policy="fail",
        sleeper=lambda _: None,
    )

    with pytest.raises(VendorReplayStaleDataError) as exc_info:
        runner.run(lambda _event: None, stop_on_complete=True)

    assert exc_info.value.error_type == "VendorReplayStaleDataError"
    assert exc_info.value.as_context()["stream_type"] == "trade"


def test_bar_recovery_uses_bar_snapshot_without_duplicate_edge():
    base = datetime(2024, 1, 1, tzinfo=timezone.utc)
    handled = []

    def stream():
        yield make_bar("BTCUSDT", base)
        yield make_bar("BTCUSDT", base + timedelta(seconds=12))

    snapshot_events = [
        make_bar("BTCUSDT", base + timedelta(seconds=5)),
        make_bar("BTCUSDT", base + timedelta(seconds=12)),
        make_bar("ETHUSDT", base + timedelta(seconds=5)),
    ]

    runner = ResilientRunner(
        stream_fn=stream,
        snapshot_fn=lambda: snapshot_events,
        lag_threshold_seconds=2,
        sleeper=lambda s: None,
    )
    runner.run(lambda ev: handled.append((ev.symbol, ev.event_ts)), stop_on_complete=True)

    assert handled == [
        ("BTCUSDT", base),
        ("BTCUSDT", base + timedelta(seconds=5)),
        ("BTCUSDT", base + timedelta(seconds=12)),
    ]
    assert runner.metrics.snapshot_runs == 1
    assert runner.metrics.snapshot_rows == 2
    assert runner.metrics.snapshot_duplicates_skipped == 1


def test_resync_records_recovery_cursor_audit_metadata():
    base = datetime(2024, 1, 1, tzinfo=timezone.utc)
    first = make_bar("BTCUSDT", base)
    first.source_id = "1"
    second = make_bar("BTCUSDT", base + timedelta(seconds=12))
    second.source_id = "3"
    recovered = make_bar("BTCUSDT", base + timedelta(seconds=5))
    recovered.source_id = "2"
    duplicate_edge = make_bar("BTCUSDT", base + timedelta(seconds=12))
    duplicate_edge.source_id = "3"

    runner = ResilientRunner(
        stream_fn=lambda: iter([first, second]),
        snapshot_fn=lambda: [recovered, duplicate_edge],
        lag_threshold_seconds=2,
        sleeper=lambda _: None,
    )
    runner.run(lambda _event: None, stop_on_complete=True)

    assert runner.metrics.recovery_audit_events_total == 1
    assert len(runner.recovery_audit_events) == 1
    audit = runner.recovery_audit_events[0]
    assert audit["stream_key"] == "BINANCE:BTCUSDT:kline"
    assert audit["recovery_request_start_ts"] == base.isoformat()
    assert audit["recovery_request_end_ts"] == (base + timedelta(seconds=12)).isoformat()
    assert audit["cursor_before_value"] == "1"
    assert audit["cursor_after_value"] == "3"
    assert audit["recovered_rows_delivered"] == 2
