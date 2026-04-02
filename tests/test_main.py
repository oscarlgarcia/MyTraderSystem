import json
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from types import SimpleNamespace
import pytest
from app import main
from app.common.dto import MarketEvent
from app.config import load_config
from app.ingestion.service import run_ingestion_service
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


def test_run_release_gates_exits_before_pipeline(monkeypatch, tmp_path: Path):
    args = SimpleNamespace(
        env="dev",
        release_gates=True,
        release_gates_target="paper",
        release_gates_output=str(tmp_path / "release-gates.json"),
        release_gates_rest_canary_path=str(tmp_path / "rest.json"),
        release_gates_ws_canary_path=str(tmp_path / "ws.json"),
        ingest_stream_types=("kline",),
    )
    cfg = load_config("dev")
    cfg = type(cfg)(
        env=cfg.env,
        data_dir=tmp_path.resolve(),
        log_level=cfg.log_level,
        ws_base=cfg.ws_base,
        rest_base=cfg.rest_base,
        symbols=cfg.symbols,
    )

    class _Report:
        pass_ok = True
        target = "paper"
        overall_status = "PASS"
        blocks = ()

    monkeypatch.setattr(main, "parse_args", lambda: args)
    monkeypatch.setattr(main, "load_config", lambda env=None: cfg)
    monkeypatch.setattr(main, "run_release_gates", lambda **kwargs: _Report())
    monkeypatch.setattr(main, "run_cycle", lambda **kwargs: (_ for _ in ()).throw(AssertionError("run_cycle should not execute")))

    assert main.run() == 0


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


def test_run_ingestion_service_executes_without_trading_stages(monkeypatch):
    cfg = load_config("dev")
    logger = get_logger(level="INFO")
    base = datetime(2024, 1, 1, tzinfo=timezone.utc)
    expected_events = [
        MarketEvent(
            symbol="BTCUSDT",
            event_ts=base,
            price=100.0,
            size=1.0,
            source="trade",
        )
    ]

    monkeypatch.setattr("app.ingestion.service.collect_events", lambda **kwargs: expected_events)
    monkeypatch.setattr(main, "run_feature_pipeline", lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("features should not run")))
    monkeypatch.setattr(main, "generate_signals", lambda *args, **kwargs: (_ for _ in ()).throw(AssertionError("strategy should not run")))

    events = run_ingestion_service(cfg=cfg, logger=logger, mode="dry", max_events=1)

    assert events == expected_events


def test_run_cycle_composes_ingestion_and_trading_cycles(monkeypatch):
    cfg = load_config("dev")
    logger = get_logger(level="INFO")
    base = datetime(2024, 1, 1, tzinfo=timezone.utc)
    events = [
        MarketEvent(
            symbol="BTCUSDT",
            event_ts=base,
            price=100.0,
            size=1.0,
            source="trade",
        )
    ]
    called = {}

    def fake_ingestion(**kwargs):
        called["ingestion"] = kwargs["mode"]
        return events

    def fake_trading(input_events, **kwargs):
        called["trading"] = list(input_events)
        return {"events": len(input_events), "features": 0, "signals": 0, "orders": 0, "fills": 0, "positions": {}, "cash": 0.0}

    monkeypatch.setattr(main, "run_ingestion_service", fake_ingestion)
    monkeypatch.setattr(main, "run_trading_cycle", fake_trading)

    metrics = main.run_cycle(cfg=cfg, logger=logger, mode="dry", max_events=1)

    assert called["ingestion"] == "dry"
    assert called["trading"] == events
    assert metrics["events"] == 1


def test_fast_path_derives_runtime_flags():
    args = SimpleNamespace(
        fast_path=True,
        production_mode=True,
        trace_steps=True,
        ingest_max_buffer=10_000,
        ingest_dedup=True,
        ingest_batch_size=4,
        ingest_lag_warn=1.0,
        ingest_buffer_warn=2,
        ingest_backpressure_policy="drop_oldest",
        ingest_temporal_policy="fail",
        ingest_pipeline_version="v1",
        ingest_shadow_mode=True,
        ingest_shadow_block_on_diff=True,
        ingest_stream_types=("kline",),
        allow_live_fallback=True,
        error_policy="degraded",
    )

    runtime = main._resolve_runtime_options(args)

    assert runtime["fast_path"] is True
    assert runtime["production_mode"] is True
    assert runtime["trace_steps"] is False
    assert runtime["ingest_dedup"] is False
    assert runtime["snapshot_enabled"] is False
    assert runtime["summary_logging"] is False
    assert runtime["ingest_batch_size"] >= main.FAST_PATH_BATCH_SIZE
    assert runtime["ingest_lag_warn"] == 1.0
    assert runtime["ingest_buffer_warn"] == 2
    assert runtime["ingest_backpressure_policy"] == "drop_oldest"
    assert runtime["ingest_temporal_policy"] == "fail"
    assert runtime["ingest_pipeline_version"] == "v1"
    assert runtime["ingest_shadow_mode"] is True
    assert runtime["ingest_shadow_block_on_diff"] is True
    assert runtime["ingest_stream_types"] == ("kline",)
    assert runtime["allow_live_fallback"] is True
    assert runtime["error_policy"] == "degraded"


def test_run_cycle_does_not_pass_removed_feature_flag_to_ingestion(monkeypatch):
    cfg = load_config("dev")
    logger = get_logger(level="INFO")
    called = {}

    def fake_ingestion(**kwargs):
        called["kwargs"] = kwargs
        return []

    def fake_trading(events, **kwargs):
        return {"events": len(events), "features": 0, "signals": 0, "orders": 0, "fills": 0, "positions": {}, "cash": 0.0}

    monkeypatch.setattr(main, "run_ingestion_service", fake_ingestion)
    monkeypatch.setattr(main, "run_trading_cycle", fake_trading)

    main.run_cycle(cfg=cfg, logger=logger, mode="dry", max_events=1)

    assert "compute_features_after_ingest" not in called["kwargs"]


def test_production_mode_rejects_unsafe_fallback(tmp_path):
    cfg = load_config("dev")
    cfg = type(cfg)(
        env=cfg.env,
        data_dir=tmp_path.resolve(),
        log_level=cfg.log_level,
        ws_base=cfg.ws_base,
        rest_base=cfg.rest_base,
        symbols=cfg.symbols,
    )
    runtime = {
        "production_mode": True,
        "fast_path": False,
        "allow_live_fallback": True,
        "error_policy": None,
        "ingest_dedup": True,
        "summary_logging": True,
        "ingest_backpressure_policy": "pause",
        "ingest_stream_types": ("kline",),
    }

    with pytest.raises(ValueError, match="allow-live-fallback"):
        main._validate_operational_security(cfg, mode="live", runtime=runtime)


def test_production_mode_rejects_live_trade_without_exact_recovery(tmp_path):
    cfg = load_config("dev")
    cfg = type(cfg)(
        env=cfg.env,
        data_dir=tmp_path.resolve(),
        log_level=cfg.log_level,
        ws_base=cfg.ws_base,
        rest_base=cfg.rest_base,
        symbols=cfg.symbols,
    )
    runtime = {
        "production_mode": True,
        "fast_path": False,
        "allow_live_fallback": False,
        "error_policy": "fail_fast",
        "ingest_dedup": True,
        "summary_logging": True,
        "ingest_backpressure_policy": "pause",
        "ingest_stream_types": ("trade",),
    }

    with pytest.raises(ValueError, match="trade does not support live ingestion"):
        main._validate_operational_security(cfg, mode="live", runtime=runtime)


def test_live_mode_rejects_trade_feed_until_exact_recovery_exists(tmp_path):
    cfg = load_config("dev")
    cfg = type(cfg)(
        env=cfg.env,
        data_dir=tmp_path.resolve(),
        log_level=cfg.log_level,
        ws_base=cfg.ws_base,
        rest_base=cfg.rest_base,
        symbols=cfg.symbols,
    )
    runtime = {
        "production_mode": False,
        "fast_path": False,
        "allow_live_fallback": False,
        "error_policy": None,
        "ingest_dedup": True,
        "summary_logging": True,
        "ingest_backpressure_policy": "pause",
        "ingest_stream_types": ("trade",),
    }

    with pytest.raises(ValueError, match="trade does not support live ingestion"):
        main._validate_operational_security(cfg, mode="live", runtime=runtime)


def test_production_mode_accepts_live_kline_with_exact_verified_recovery(tmp_path):
    cfg = load_config("dev")
    cfg = type(cfg)(
        env=cfg.env,
        data_dir=tmp_path.resolve(),
        log_level=cfg.log_level,
        ws_base=cfg.ws_base,
        rest_base=cfg.rest_base,
        symbols=cfg.symbols,
    )
    runtime = {
        "production_mode": True,
        "fast_path": False,
        "allow_live_fallback": False,
        "error_policy": "fail_fast",
        "ingest_dedup": True,
        "summary_logging": True,
        "ingest_backpressure_policy": "pause",
        "ingest_stream_types": ("kline",),
    }

    main._validate_operational_security(cfg, mode="live", runtime=runtime)


def test_live_mode_rejects_feed_without_live_support(tmp_path):
    cfg = load_config("dev")
    cfg = type(cfg)(
        env=cfg.env,
        data_dir=tmp_path.resolve(),
        log_level=cfg.log_level,
        ws_base=cfg.ws_base,
        rest_base=cfg.rest_base,
        symbols=cfg.symbols,
    )
    runtime = {
        "production_mode": False,
        "fast_path": False,
        "allow_live_fallback": False,
        "error_policy": None,
        "ingest_dedup": True,
        "summary_logging": True,
        "ingest_backpressure_policy": "pause",
        "ingest_stream_types": ("book",),
    }

    with pytest.raises(ValueError, match="book does not support live ingestion"):
        main._validate_operational_security(cfg, mode="live", runtime=runtime)


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
