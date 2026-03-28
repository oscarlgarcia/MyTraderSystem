import json
from app import main
from app.config import load_config
from app.observability.logger import get_logger


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
    payload = json.loads(log_lines[0])
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
