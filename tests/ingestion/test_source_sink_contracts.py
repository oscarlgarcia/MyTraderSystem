import io
import json
from datetime import datetime, timezone
from pathlib import Path
from unittest import mock

import httpx
import pytest

from app.common.dto import MarketEvent
from app.ingestion.circuit_breaker import CircuitBreaker
from app.ingestion import pipeline
from app.ingestion.errors import IngestionError
from app.ingestion.resilience import ResilientRunner
from app.ingestion.sources import BinanceSource, StaticSource
from app.marketdata.recovery import RecoveryRequest
from app.marketdata.models import TradeEvent
from app.marketdata.temporal_state import TemporalPartitionKey
from app.marketdata.raw_sink import JsonlRawSink
from app.observability.logger import get_logger


def _ev(ts_offset: int, price: float) -> MarketEvent:
    return MarketEvent(
        symbol="BTCUSDT",
        event_ts=datetime.fromtimestamp(1700000000 + ts_offset, tz=timezone.utc),
        price=price,
        size=1.0,
        source="trade",
    )


def _json_lines(buffer: io.StringIO) -> list[dict[str, object]]:
    return [json.loads(line) for line in buffer.getvalue().splitlines() if line.strip()]


class RecordingSink:
    def __init__(self):
        self.items = []
        self.closed = False

    def add(self, event):
        if isinstance(event, list):
            self.items.extend(event)
            return
        self.items.append(event)

    def close(self):
        self.closed = True


def test_pipeline_runs_against_source_and_sink_contracts():
    cfg = mock.Mock(env="dev", ws_base="wss://x", rest_base="https://x", symbols=["BTCUSDT"], data_dir=".", log_level="INFO")
    events = [_ev(0, 100), _ev(60, 101)]
    source = StaticSource(events=events)
    sink = RecordingSink()

    out = pipeline.collect_events(
        mode="live",
        cfg=cfg,
        max_events=10,
        duration_s=0,
        logger=mock.Mock(),
        snapshot_enabled=False,
        source=source,
        sink=sink,
    )

    assert out == events
    assert sink.items == events
    assert sink.closed is True


def test_binance_source_preserves_current_behavior():
    cfg = mock.Mock(env="dev", ws_base="wss://stream.binance.com:9443", rest_base="https://api.binance.com", symbols=["BTCUSDT"], data_dir=".", log_level="INFO")
    seen = {}
    event = _ev(0, 100)

    def fake_ws_stream(url: str, end_time=None):
        seen["url"] = url
        seen["end_time"] = end_time
        yield event

    source = BinanceSource(cfg, ws_stream=fake_ws_stream)
    out = list(source.stream(end_time=123.0))

    assert out == [event]
    assert "btcusdt@trade" in seen["url"]
    assert "btcusdt@kline_1m" in seen["url"]
    assert seen["end_time"] == 123.0


def test_binance_source_captures_receive_and_process_timestamps_on_raw_stream():
    cfg = mock.Mock(env="dev", ws_base="wss://stream.binance.com:9443", rest_base="https://api.binance.com", symbols=["BTCUSDT"], data_dir=".", log_level="INFO")

    def fake_ws_stream(url: str, end_time=None):
        del url, end_time
        yield '{"stream":"btcusdt@trade","data":{"s":"BTCUSDT","E":1704067200000,"p":"100","q":"1","t":7}}'

    source = BinanceSource(cfg, ws_stream=fake_ws_stream)
    out = list(source.stream(end_time=123.0))

    assert len(out) == 1
    assert isinstance(out[0], TradeEvent)
    assert out[0].exchange_ts == datetime(2024, 1, 1, 0, 0, tzinfo=timezone.utc)
    assert out[0].receive_ts is not None
    assert out[0].process_ts is not None
    assert out[0].exchange_ts <= out[0].receive_ts <= out[0].process_ts
    metric = source.stats.stream_metrics["BINANCE:BTCUSDT:trade"]
    assert metric["exchange_receive_skew_seconds"] >= 0.0
    assert metric["receive_process_skew_seconds"] >= 0.0


def test_binance_source_only_emits_closed_kline_events_from_live_stream():
    cfg = mock.Mock(env="dev", ws_base="wss://stream.binance.com:9443", rest_base="https://api.binance.com", symbols=["BTCUSDT"], data_dir=".", log_level="INFO")

    def fake_ws_stream(url: str, end_time=None):
        del url, end_time
        yield '{"stream":"btcusdt@kline_1m","data":{"s":"BTCUSDT","E":1704067200000,"k":{"t":1704067140000,"T":1704067199999,"o":"100","h":"101","l":"99","c":"100.5","q":"5","i":"1m","x":false}}}'
        yield '{"stream":"btcusdt@kline_1m","data":{"s":"BTCUSDT","E":1704067260000,"k":{"t":1704067200000,"T":1704067259999,"o":"100.5","h":"102","l":"100","c":"101","q":"7","i":"1m","x":true}}}'

    source = BinanceSource(cfg, ws_stream=fake_ws_stream, stream_types=("kline",))
    out = list(source.stream())

    assert len(out) == 1
    event = out[0]
    assert event.source == "kline"
    assert event.close_ts == datetime(2024, 1, 1, 0, 0, 59, 999000, tzinfo=timezone.utc)
    metric = source.stats.stream_metrics["BINANCE:BTCUSDT:kline"]
    assert metric["messages_in_total"] == 1


def test_binance_source_records_per_stream_raw_latency(tmp_path: Path):
    cfg = mock.Mock(env="dev", ws_base="wss://stream.binance.com:9443", rest_base="https://api.binance.com", symbols=["BTCUSDT"], data_dir=tmp_path, log_level="INFO")

    def fake_ws_stream(url: str, end_time=None):
        del url, end_time
        yield '{"stream":"btcusdt@trade","data":{"s":"BTCUSDT","E":1704067200000,"p":"100","q":"1","t":7}}'

    source = BinanceSource(
        cfg,
        ws_stream=fake_ws_stream,
        raw_sink=JsonlRawSink(tmp_path / "raw", env="dev"),
    )
    out = list(source.stream())

    assert len(out) == 1
    metric = source.stats.stream_metrics["BINANCE:BTCUSDT:trade"]
    assert metric["messages_in_total"] == 1
    assert metric["raw_write_latency"] >= 0.0


def test_stream_without_heartbeat_triggers_reconnect():
    cfg = mock.Mock(env="dev", ws_base="wss://stream.binance.com:9443", rest_base="https://api.binance.com", symbols=["BTCUSDT"], data_dir=".", log_level="INFO")

    class FakeConnection:
        def __init__(self, mode: str):
            self.mode = mode

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def recv(self, timeout=None):
            del timeout
            if self.mode == "dead":
                raise TimeoutError("idle")
            if self.mode == "live":
                if getattr(self, "_sent", False):
                    raise StopIteration
                self._sent = True
                return '{"stream":"btcusdt@trade","data":{"s":"BTCUSDT","E":1704067200000,"p":"100","q":"1","t":7}}'
            raise RuntimeError("unexpected mode")

        def ping(self):
            class Pong:
                def wait(self, timeout=None):
                    del timeout
                    return False

            return Pong()

    attempts = {"n": 0}
    monotonic_values = iter([0.0, 1.0, 10.0, 10.0, 10.5, 10.5, 11.0])

    def fake_connect(_url: str):
        attempts["n"] += 1
        return FakeConnection("dead" if attempts["n"] == 1 else "live")

    source = BinanceSource(
        cfg,
        ws_connect_fn=fake_connect,
        monotonic_fn=lambda: next(monotonic_values),
    )
    runner = ResilientRunner(
        stream_fn=lambda: source.stream(),
        sleeper=lambda _seconds: None,
        jitter_fn=lambda delay: delay,
    )
    handled = []
    runner.run(lambda event: handled.append(event), stop_on_complete=True, max_retries=1)

    assert attempts["n"] == 2
    assert runner.metrics.reconnects == 1
    assert len(handled) == 1


def test_reconnect_storm_and_heartbeat_alerts_are_emitted():
    cfg = mock.Mock(env="dev", ws_base="wss://stream.binance.com:9443", rest_base="https://api.binance.com", symbols=["BTCUSDT"], data_dir=".", log_level="INFO")
    buffer = io.StringIO()
    get_logger(name="ingest.source", level="INFO", stream=buffer)

    class FakeConnection:
        def __init__(self, mode: str):
            self.mode = mode

        def __enter__(self):
            return self

        def __exit__(self, exc_type, exc, tb):
            return False

        def recv(self, timeout=None):
            del timeout
            if self.mode == "dead":
                raise TimeoutError("idle")
            if getattr(self, "_sent", False):
                raise StopIteration
            self._sent = True
            return '{"stream":"btcusdt@trade","data":{"s":"BTCUSDT","E":1704067200000,"p":"100","q":"1","t":7}}'

        def ping(self):
            class Pong:
                def wait(self, timeout=None):
                    del timeout
                    return False

            return Pong()

    attempts = {"n": 0}
    monotonic_values = iter([
        0.0, 1.0, 10.0, 10.0,
        20.0, 21.0, 30.0, 30.0,
        40.0, 41.0, 50.0, 50.0,
        60.0, 60.5, 61.0,
    ])

    def fake_connect(_url: str):
        attempts["n"] += 1
        return FakeConnection("dead" if attempts["n"] <= 3 else "live")

    source = BinanceSource(
        cfg,
        ws_connect_fn=fake_connect,
        monotonic_fn=lambda: next(monotonic_values),
    )
    runner = ResilientRunner(
        stream_fn=lambda: source.stream(),
        sleeper=lambda _seconds: None,
        jitter_fn=lambda delay: delay,
    )

    handled: list[object] = []
    runner.run(lambda event: handled.append(event), stop_on_complete=True, max_retries=3)

    alerts = [record for record in _json_lines(buffer) if record["message"] == "operational alert"]
    assert any(record["alert_type"] == "heartbeat_missed" for record in alerts)
    storm = next(record for record in alerts if record["alert_type"] == "reconnect_storm")
    assert storm["venue"] == "BINANCE"
    assert storm["symbol"] == "BTCUSDT"
    assert storm["stream_type"] == "trade"
    assert storm["threshold"] == 3


def test_dlq_spike_alert_is_emitted_after_repeated_invalid_payloads():
    cfg = mock.Mock(env="dev", ws_base="wss://stream.binance.com:9443", rest_base="https://api.binance.com", symbols=["BTCUSDT"], data_dir=".", log_level="INFO")
    buffer = io.StringIO()
    get_logger(name="ingest.source", level="INFO", stream=buffer)

    def fake_ws_stream(url: str, end_time=None):
        del url, end_time
        yield '{"stream":"btcusdt@trade","data":{"s":"BTCUSDT","E":1704067200000,"p":"NaN","q":"1","t":1}}'
        yield '{"stream":"btcusdt@trade","data":{"s":"BTCUSDT","E":1704067201000,"p":"NaN","q":"1","t":2}}'
        yield '{"stream":"btcusdt@trade","data":{"s":"BTCUSDT","E":1704067202000,"p":"NaN","q":"1","t":3}}'

    source = BinanceSource(cfg, ws_stream=fake_ws_stream)
    assert list(source.stream()) == []

    alerts = [record for record in _json_lines(buffer) if record["message"] == "operational alert"]
    spike = next(record for record in alerts if record["alert_type"] == "dlq_spike")
    assert spike["venue"] == "BINANCE"
    assert spike["symbol"] == "BTCUSDT"
    assert spike["stream_type"] == "trade"
    assert spike["observed"] == 3


def test_invalid_timestamp_alert_is_emitted_for_future_payload():
    cfg = mock.Mock(env="dev", ws_base="wss://stream.binance.com:9443", rest_base="https://api.binance.com", symbols=["BTCUSDT"], data_dir=".", log_level="INFO")
    buffer = io.StringIO()
    get_logger(name="ingest.source", level="INFO", stream=buffer)

    def fake_ws_stream(url: str, end_time=None):
        del url, end_time
        yield '{"stream":"btcusdt@trade","data":{"s":"BTCUSDT","E":4102444800000,"p":"100","q":"1","t":1}}'

    source = BinanceSource(cfg, ws_stream=fake_ws_stream)
    assert list(source.stream()) == []

    metric = source.stats.stream_metrics["BINANCE:BTCUSDT:trade"]
    assert metric["invalid_timestamp_total"] == 1
    alerts = [record for record in _json_lines(buffer) if record["message"] == "operational alert"]
    invalid_alert = next(record for record in alerts if record["alert_type"] == "invalid_timestamp_detected")
    assert invalid_alert["venue"] == "BINANCE"
    assert invalid_alert["symbol"] == "BTCUSDT"
    assert invalid_alert["stream_type"] == "trade"


def test_schema_drift_alert_is_emitted_and_payload_is_quarantined(tmp_path: Path):
    cfg = mock.Mock(
        env="dev",
        ws_base="wss://stream.binance.com:9443",
        rest_base="https://api.binance.com",
        symbols=["BTCUSDT"],
        data_dir=tmp_path,
        log_level="INFO",
    )
    buffer = io.StringIO()
    get_logger(name="ingest.source", level="INFO", stream=buffer)

    def fake_ws_stream(url: str, end_time=None):
        del url, end_time
        yield '{"stream":"btcusdt@trade","data":{"s":"BTCUSDT","E":1704067200000,"p":"100","q":"1","unexpected":{"x":1}}}'
        yield '{"stream":"btcusdt@trade","data":{"s":"BTCUSDT","E":1704067201000,"p":"100","q":"1","t":9}}'

    source = BinanceSource(cfg, ws_stream=fake_ws_stream)
    events = list(source.stream())

    assert len(events) == 1
    metric = source.stats.stream_metrics["BINANCE:BTCUSDT:trade"]
    assert metric["schema_drift_total"] == 1

    alerts = [record for record in _json_lines(buffer) if record["message"] == "operational alert"]
    schema_alert = next(record for record in alerts if record["alert_type"] == "schema_drift_detected")
    assert schema_alert["venue"] == "BINANCE"
    assert schema_alert["symbol"] == "BTCUSDT"
    assert schema_alert["stream_type"] == "trade"
    assert schema_alert["error_type"] == "SchemaDriftError"
    assert schema_alert["drift_mode"] == "blocking"
    assert "unexpected" in schema_alert["unexpected_paths"]

    quarantine_path = tmp_path / "errors" / "schema-drift-quarantine.jsonl"
    assert quarantine_path.exists()
    records = [json.loads(line) for line in quarantine_path.read_text(encoding="utf-8").splitlines() if line.strip()]
    assert len(records) == 1
    assert records[0]["error_type"] == "SchemaDriftError"



def test_snapshot_optional_on_source():
    cfg = mock.Mock(env="dev", ws_base="wss://x", rest_base="https://x", symbols=["BTCUSDT"], data_dir=".", log_level="INFO")
    events = [_ev(0, 100), _ev(10, 101)]
    source = StaticSource(events=events, snapshot_events=None)
    sink = RecordingSink()
    buffer = io.StringIO()
    logger = get_logger(name="test.ingest.source.optional_snapshot", level="INFO", stream=buffer)

    out = pipeline.collect_events(
        mode="live",
        cfg=cfg,
        max_events=10,
        duration_s=0,
        logger=logger,
        source=source,
        sink=sink,
        snapshot_enabled=True,
    )

    assert out == events
    summary = next(record for record in _json_lines(buffer) if record["message"] == "ingestion summary")
    assert summary["events_in"] == 2
    assert summary["events_out"] == 2


def test_snapshot_retries_use_injected_jitter_deterministically():
    cfg = mock.Mock(
        env="dev",
        ws_base="wss://stream.binance.com:9443",
        rest_base="https://api.binance.com",
        symbols=["BTCUSDT"],
        data_dir=".",
        log_level="INFO",
    )
    attempts = {"n": 0}
    sleeps: list[float] = []

    def fake_http_get(url: str, **kwargs):
        del kwargs
        attempts["n"] += 1
        request = httpx.Request("GET", url)
        if attempts["n"] <= 2:
            return httpx.Response(503, request=request)
        return httpx.Response(
            200,
            request=request,
            json=[[1704067200000, "100", "101", "99", "100.5", "5", 1704067259999]],
        )

    source = BinanceSource(
        cfg,
        http_get=fake_http_get,
        snapshot_sleeper=lambda seconds: sleeps.append(seconds),
        snapshot_jitter_fn=lambda delay: delay + 0.25,
        snapshot_retries_5xx=2,
        snapshot_backoff_base_seconds=1.0,
        snapshot_backoff_max_seconds=5.0,
    )

    events = list(source.snapshot())

    assert len(events) == 1
    assert attempts["n"] == 3
    assert sleeps == [1.25, 2.25]


def test_snapshot_retry_exhaustion_emits_specific_operational_alert():
    cfg = mock.Mock(
        env="dev",
        ws_base="wss://stream.binance.com:9443",
        rest_base="https://api.binance.com",
        symbols=["BTCUSDT"],
        data_dir=".",
        log_level="INFO",
    )
    buffer = io.StringIO()
    get_logger(name="ingest.source", level="INFO", stream=buffer)

    def fake_http_get(url: str, **kwargs):
        del kwargs
        request = httpx.Request("GET", url)
        return httpx.Response(503, request=request)

    source = BinanceSource(
        cfg,
        http_get=fake_http_get,
        snapshot_sleeper=lambda _seconds: None,
        snapshot_jitter_fn=lambda delay: delay,
        snapshot_retries_5xx=1,
        snapshot_breaker=CircuitBreaker(
            failure_threshold=1,
            reset_timeout_seconds=60.0,
            monotonic_fn=lambda: 0.0,
        ),
    )

    with pytest.raises(IngestionError) as exc_info:
        list(source.snapshot())

    assert exc_info.value.category == "source"
    alerts = [record for record in _json_lines(buffer) if record["message"] == "operational alert"]
    snapshot_alert = next(record for record in alerts if record["alert_type"] == "snapshot_retry_exhausted")
    assert snapshot_alert["venue"] == "BINANCE"
    assert snapshot_alert["symbol"] == "BTCUSDT"
    assert snapshot_alert["stream_type"] == "kline"
    assert snapshot_alert["alert_severity"] == "error"


def test_snapshot_circuit_breaker_fails_fast_after_threshold():
    cfg = mock.Mock(
        env="dev",
        ws_base="wss://stream.binance.com:9443",
        rest_base="https://api.binance.com",
        symbols=["BTCUSDT"],
        data_dir=".",
        log_level="INFO",
    )
    buffer = io.StringIO()
    get_logger(name="ingest.source", level="INFO", stream=buffer)
    attempts = {"n": 0}

    def fake_http_get(url: str, **kwargs):
        del kwargs
        attempts["n"] += 1
        request = httpx.Request("GET", url)
        return httpx.Response(503, request=request)

    breaker = CircuitBreaker(
        failure_threshold=1,
        reset_timeout_seconds=60.0,
        monotonic_fn=lambda: 0.0,
    )
    source = BinanceSource(
        cfg,
        http_get=fake_http_get,
        snapshot_sleeper=lambda _seconds: None,
        snapshot_retries_5xx=0,
        snapshot_breaker=breaker,
    )

    with pytest.raises(IngestionError):
        list(source.snapshot())
    with pytest.raises(IngestionError):
        list(source.snapshot())

    assert attempts["n"] == 1
    alerts = [record for record in _json_lines(buffer) if record["message"] == "operational alert"]
    assert sum(1 for record in alerts if record["alert_type"] == "snapshot_retry_exhausted") >= 2


@pytest.mark.parametrize(
    ("gap_minutes", "interval", "expected_limit"),
    [
        (1, "1m", 2),
        (5, "5m", 6),
        (12, "1m", 13),
    ],
)
def test_snapshot_request_uses_recovery_window_params_instead_of_fixed_limit(
    gap_minutes: int,
    interval: str,
    expected_limit: int,
):
    cfg = mock.Mock(
        env="dev",
        ws_base="wss://stream.binance.com:9443",
        rest_base="https://api.binance.com",
        symbols=["BTCUSDT"],
        data_dir=".",
        log_level="INFO",
    )
    captured: dict[str, object] = {}

    def fake_http_get(url: str, **kwargs):
        captured["url"] = url
        captured["params"] = dict(kwargs["params"])
        request = httpx.Request("GET", url, params=kwargs["params"])
        return httpx.Response(
            200,
            request=request,
            json=[[1704067200000, "100", "101", "99", "100.5", "5", 1704067259999]],
        )

    source = BinanceSource(cfg, http_get=fake_http_get, stream_types=("kline",))
    request = RecoveryRequest(
        partition=TemporalPartitionKey(venue="BINANCE", symbol="BTCUSDT", stream_type="kline"),
        start_ts=datetime(2024, 1, 1, 0, 0, tzinfo=timezone.utc),
        end_ts=datetime(2024, 1, 1, 0, gap_minutes, tzinfo=timezone.utc),
        interval=interval,
        limit=expected_limit,
        reason="weak_gap_detection",
    )

    events = list(source.snapshot(request=request))

    assert len(events) == 1
    assert captured["url"].endswith("/api/v3/klines")
    assert captured["params"]["symbol"] == "BTCUSDT"
    assert captured["params"]["interval"] == interval
    assert captured["params"]["limit"] == expected_limit
    assert captured["params"]["startTime"] == 1704067200000
    assert captured["params"]["endTime"] == 1704067200000 + (gap_minutes * 60 * 1000)
    metric = source.stats.stream_metrics["BINANCE:BTCUSDT:kline"]
    assert metric["recovery_window_rows_requested"] == expected_limit
    assert metric["recovery_window_rows_received"] == 1


def test_snapshot_request_records_exactness_violation_when_vendor_returns_short_window():
    cfg = mock.Mock(
        env="dev",
        ws_base="wss://stream.binance.com:9443",
        rest_base="https://api.binance.com",
        symbols=["BTCUSDT"],
        data_dir=".",
        log_level="INFO",
    )

    def fake_http_get(url: str, **kwargs):
        request = httpx.Request("GET", url, params=kwargs["params"])
        return httpx.Response(
            200,
            request=request,
            json=[[1704067200000, "100", "101", "99", "100.5", "5", 1704067259999]],
        )

    source = BinanceSource(cfg, http_get=fake_http_get, stream_types=("kline",))
    request = RecoveryRequest(
        partition=TemporalPartitionKey(venue="BINANCE", symbol="BTCUSDT", stream_type="kline"),
        start_ts=datetime(2024, 1, 1, 0, 0, tzinfo=timezone.utc),
        end_ts=datetime(2024, 1, 1, 0, 12, tzinfo=timezone.utc),
        interval="1m",
        limit=13,
        reason="weak_gap_detection",
    )

    events = list(source.snapshot(request=request))

    assert len(events) == 1
    metric = source.stats.stream_metrics["BINANCE:BTCUSDT:kline"]
    assert metric["recovery_window_rows_requested"] == 13
    assert metric["recovery_window_rows_received"] == 1
    assert metric["recovery_exactness_violation_total"] == 1


def test_sink_failure_alert_is_emitted_when_normalized_sink_fails():
    cfg = mock.Mock(env="dev", ws_base="wss://x", rest_base="https://x", symbols=["BTCUSDT"], data_dir=".", log_level="INFO")
    buffer = io.StringIO()
    logger = get_logger(name="test.ingest.alerts.sink_failure", level="INFO", stream=buffer)

    class FailingSink:
        def add(self, event):
            del event
            raise RuntimeError("disk full")

        def close(self):
            return None

    with pytest.raises(IngestionError):
        pipeline.collect_events(
            mode="live",
            cfg=cfg,
            max_events=10,
            duration_s=0,
            logger=logger,
            snapshot_enabled=False,
            source=StaticSource(events=[_ev(0, 100)]),
            sink=FailingSink(),
            error_policy="fail_fast",
        )

    alerts = [record for record in _json_lines(buffer) if record["message"] == "operational alert"]
    sink_failure = next(record for record in alerts if record["alert_type"] == "sink_failure")
    assert sink_failure["sink_component"] == "FailingSink"
    assert sink_failure["alert_severity"] == "error"
