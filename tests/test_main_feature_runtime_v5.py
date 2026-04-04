import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from app import main
from app.config import load_config
from app.observability.logger import get_logger


def test_run_cycle_supports_paper_mode(monkeypatch, tmp_path):
    cfg = load_config("dev")
    called = {}

    def _fake_ingestion(**kwargs):
        called["ingestion_mode"] = kwargs["mode"]
        return []

    monkeypatch.setattr(main, "run_ingestion_service", _fake_ingestion)
    monkeypatch.setattr(main, "run_trading_cycle", lambda events, **kwargs: {"events": 0, "features": 0, "signals": 0, "orders": 0, "fills": 0, "positions": {}, "cash": 0.0, "mode": kwargs["mode"]})

    metrics = main.run_cycle(
        cfg=cfg,
        logger=get_logger(level="INFO"),
        mode="paper",
        max_events=1,
        feature_audit_path=str(tmp_path / "audit.jsonl"),
    )
    assert called["ingestion_mode"] == "paper"
    assert metrics["events"] == 0


def test_run_feature_release_publish_action(monkeypatch, tmp_path):
    cfg = load_config("dev")
    gate_path = tmp_path / "gate.json"
    gate_path.write_text(json.dumps({"parity_mismatches": 0, "stale_serves": 0, "serving_latency_max": 0.01}), encoding="utf-8")
    args = SimpleNamespace(
        env="dev",
        feature_release_action="publish",
        feature_release_registry=str(tmp_path / "releases.json"),
        feature_release_name="default",
        feature_release_version="1.0.0",
        feature_release_target="paper",
        feature_release_gate_input=str(gate_path),
        release_gates=False,
        mode="dry",
    )
    monkeypatch.setattr(main, "parse_args", lambda: args)
    monkeypatch.setattr(main, "load_config", lambda env=None: cfg)
    monkeypatch.setattr(main, "run_cycle", lambda **kwargs: (_ for _ in ()).throw(AssertionError("run_cycle should not execute")))

    assert main.run() == 0
    assert Path(args.feature_release_registry).exists()


def test_run_feature_release_publish_requires_gate_input(monkeypatch, tmp_path):
    cfg = load_config("dev")
    args = SimpleNamespace(
        env="dev",
        feature_release_action="publish",
        feature_release_registry=str(tmp_path / "releases.json"),
        feature_release_name="default",
        feature_release_version="1.0.0",
        feature_release_target="paper",
        feature_release_gate_input=None,
        release_gates=False,
        mode="dry",
    )
    monkeypatch.setattr(main, "parse_args", lambda: args)
    monkeypatch.setattr(main, "load_config", lambda env=None: cfg)

    with pytest.raises(ValueError, match="gate-input"):
        main.run()
