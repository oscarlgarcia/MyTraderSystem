import json
import time
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from app import main
from app.common.dto import MarketEvent
from app.config import load_config
from app.observability.logger import get_logger
from app.ingestion import pipeline


def test_run_cycle_order_and_metrics():
    cfg = load_config("dev")
    recorder = []
    metrics = main.run_cycle(cfg=cfg, logger=get_logger(level="INFO"), mode="dry", max_events=10, recorder=recorder)
    assert recorder == ["ingestion", "features", "strategy", "risk", "execution", "portfolio"]
    assert metrics["events"] == 10
    assert metrics["fills"] >= 0


def test_run_returns_zero(monkeypatch):
    outputs = []

    class _Stream:
        def write(self, msg):
            outputs.append(msg)

        def flush(self):
            pass

    monkeypatch.setattr(main, "get_logger", lambda level=None: get_logger(stream=_Stream(), level="INFO"))
    rc = main.run()
    assert rc == 0
    log_lines = [l for l in outputs if l.strip()]
    assert log_lines, "expected at least one log line"
    payloads = [json.loads(line) for line in log_lines]
    payload = next(item for item in payloads if item["message"] == "pipeline ok")
    assert payload["message"] == "pipeline ok"
    assert payload["trace_id"]
    assert payload["env"] == "dev"


def test_run_respects_app_env(monkeypatch):
    monkeypatch.setenv("APP_ENV", "test")
    rc = main.run()
    assert rc == 0


def test_run_cycle_traces(monkeypatch):
    import io

    buffer = io.StringIO()
    logger = get_logger(stream=buffer, level="INFO")
    cfg = load_config("dev")

    main.run_cycle(cfg=cfg, logger=logger, mode="dry", max_events=3, recorder=[], trace_steps=True)
    out = buffer.getvalue()
    assert '"phase": "ingestion"' in out
    assert '"phase": "features"' in out


def test_trace_steps_false_emits_no_phase(monkeypatch):
    import io

    buffer = io.StringIO()
    logger = get_logger(stream=buffer, level="INFO")
    cfg = load_config("dev")
    main.run_cycle(cfg=cfg, logger=logger, mode="dry", max_events=2, recorder=[], trace_steps=False)
    out = buffer.getvalue()
    assert '"phase": "ingestion"' not in out


def test_features_after_ingest_runs_pipeline(monkeypatch, caplog):
    import io

    caplog.set_level("INFO")
    buffer = io.StringIO()
    logger = get_logger(stream=buffer, level="INFO")
    cfg = load_config("dev")

    main.run_cycle(
        cfg=cfg,
        logger=logger,
        mode="dry",
        max_events=2,
        recorder=[],
        trace_steps=False,
        compute_features_after_ingest=True,
    )
    assert any("feature pipeline done" in rec.message for rec in caplog.records)


def test_fast_path_derives_runtime_flags():
    args = SimpleNamespace(
        fast_path=True,
        trace_steps=True,
        features_after_ingest=False,
        ingest_max_buffer=10_000,
        ingest_dedup=True,
        ingest_batch_size=4,
        ingest_lag_warn=1.0,
        ingest_buffer_warn=2,
        allow_live_fallback=True,
    )

    runtime = main._resolve_runtime_options(args)

    assert runtime["fast_path"] is True
    assert runtime["trace_steps"] is False
    assert runtime["ingest_dedup"] is False
    assert runtime["snapshot_enabled"] is False
    assert runtime["summary_logging"] is False
    assert runtime["ingest_batch_size"] >= main.FAST_PATH_BATCH_SIZE
    assert runtime["ingest_lag_warn"] == 1.0
    assert runtime["ingest_buffer_warn"] == 2
    assert runtime["allow_live_fallback"] is True


def test_fast_path_mock_benchmark_improves_throughput():
    class DummyWriter:
        def __init__(self):
            self.calls = 0

        def add(self, batch):
            self.calls += 1
            for _ in range(1000):
                pass

    base = datetime(2024, 1, 1, tzinfo=timezone.utc)
    events = [
        MarketEvent(
            symbol="BTCUSDT",
            event_ts=base + timedelta(seconds=index),
            price=100 + index,
            size=1.0,
            source="trade",
        )
        for index in range(10_000)
    ]

    default_writer = DummyWriter()
    default_stats = {"written": 0, "duplicates_dropped": 0}
    default_handler = pipeline._build_live_handler(default_writer, default_stats, max_events=20_000, dedup_enabled=True, batch_size=1)

    start = time.perf_counter()
    for event in events:
        default_handler(event)
    default_handler.close()
    default_elapsed = time.perf_counter() - start

    fast_writer = DummyWriter()
    fast_stats = {"written": 0, "duplicates_dropped": 0}
    fast_handler = pipeline._build_live_handler(
        fast_writer,
        fast_stats,
        max_events=20_000,
        dedup_enabled=False,
        batch_size=main.FAST_PATH_BATCH_SIZE,
    )

    start = time.perf_counter()
    for event in events:
        fast_handler(event)
    fast_handler.close()
    fast_elapsed = time.perf_counter() - start

    assert fast_writer.calls < default_writer.calls
    assert fast_elapsed < default_elapsed * 0.7
