import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from app import main
from app.config import load_config
from app.features.training_bundle_registry import TrainingBundleRecord, TrainingBundleRegistry
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
        feature_release_gates=False,
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
        feature_release_gates=False,
        mode="dry",
    )
    monkeypatch.setattr(main, "parse_args", lambda: args)
    monkeypatch.setattr(main, "load_config", lambda env=None: cfg)

    with pytest.raises(ValueError, match="gate-input"):
        main.run()


def test_run_requires_feature_contract_metadata_outside_dry(monkeypatch, tmp_path):
    cfg = load_config("dev")
    args = SimpleNamespace(
        env="dev",
        feature_release_action=None,
        release_gates=False,
        feature_release_gates=False,
        mode="live",
        max_events=1,
        duration=None,
        feature_audit_path=str(tmp_path / "features-audit.jsonl"),
        feature_dataset_id="",
        feature_schema_hash="",
        feature_training_bundle_id="",
        feature_consumer_name="",
        feature_consumer_kind="",
        feature_training_bundle_registry=None,
        feature_observability_output=None,
        fast_path=False,
        production_mode=False,
        trace_steps=False,
        ingest_max_buffer=10_000,
        ingest_dedup=True,
        ingest_batch_size=1,
        ingest_lag_warn=None,
        ingest_buffer_warn=None,
        ingest_backpressure_policy="pause",
        ingest_temporal_policy="accept",
        ingest_pipeline_version="v2",
        ingest_shadow_mode=False,
        ingest_shadow_block_on_diff=False,
        ingest_stream_types=("kline",),
        allow_live_fallback=False,
        error_policy=None,
    )
    monkeypatch.setattr(main, "parse_args", lambda: args)
    monkeypatch.setattr(main, "load_config", lambda env=None: cfg)
    monkeypatch.setattr(main, "_validate_operational_security", lambda *args, **kwargs: None)

    with pytest.raises(ValueError, match="consumer metadata"):
        main.run()


def test_run_wires_feature_contract_metadata_into_cycle(monkeypatch, tmp_path):
    cfg = load_config("dev")
    registry_dir = tmp_path / "training-bundles"
    TrainingBundleRegistry(registry_dir).register(
        TrainingBundleRecord(
            bundle_id="train-bundle-1",
            dataset_id="dataset-2024-01",
            feature_schema_hash="schema-v1",
            feature_set_name="legacy",
            feature_set_version="legacy",
        )
    )
    args = SimpleNamespace(
        env="dev",
        feature_release_action=None,
        release_gates=False,
        feature_release_gates=False,
        mode="live",
        max_events=1,
        duration=None,
        feature_audit_path=str(tmp_path / "features-audit.jsonl"),
        feature_dataset_id="dataset-2024-01",
        feature_schema_hash="schema-v1",
        feature_training_bundle_id="train-bundle-1",
        feature_consumer_name="paper-strategy",
        feature_consumer_kind="strategy",
        feature_training_bundle_registry=str(registry_dir),
        feature_observability_output=str(tmp_path / "feature_observability.json"),
        fast_path=False,
        production_mode=False,
        trace_steps=False,
        ingest_max_buffer=10_000,
        ingest_dedup=True,
        ingest_batch_size=1,
        ingest_lag_warn=None,
        ingest_buffer_warn=None,
        ingest_backpressure_policy="pause",
        ingest_temporal_policy="accept",
        ingest_pipeline_version="v2",
        ingest_shadow_mode=False,
        ingest_shadow_block_on_diff=False,
        ingest_stream_types=("kline",),
        allow_live_fallback=False,
        error_policy=None,
    )
    called = {}
    monkeypatch.setattr(main, "parse_args", lambda: args)
    monkeypatch.setattr(main, "load_config", lambda env=None: cfg)
    monkeypatch.setattr(main, "_validate_operational_security", lambda *args, **kwargs: None)

    def _fake_run_cycle(**kwargs):
        called["feature_consumer_metadata"] = kwargs["feature_consumer_metadata"]
        called["feature_observability_output"] = kwargs["feature_observability_output"]
        return {"events": 1, "features": 0, "signals": 0, "orders": 0, "fills": 0, "positions": {}, "cash": 0.0}

    monkeypatch.setattr(main, "run_cycle", _fake_run_cycle)

    assert main.run() == 0
    assert called["feature_consumer_metadata"]["training_bundle_id"] == "train-bundle-1"
    assert called["feature_observability_output"] == args.feature_observability_output


def test_run_executes_feature_release_gates(monkeypatch, tmp_path):
    cfg = load_config("dev")
    args = SimpleNamespace(
        env="dev",
        feature_release_action=None,
        release_gates=False,
        feature_release_gates=True,
        feature_release_gates_target="paper",
        feature_release_gates_parity_path=str(tmp_path / "parity.json"),
        feature_release_gates_benchmark_path=str(tmp_path / "benchmark.json"),
        feature_release_gates_observability_path=str(tmp_path / "observability.json"),
        feature_release_gates_contract_path=str(tmp_path / "contract.json"),
        feature_release_gates_online_backend="http",
        feature_release_gates_observability_sink="http",
        feature_release_gates_shadow_path=None,
        feature_release_gates_soak_path=None,
        feature_release_gates_concurrency_path=None,
        feature_release_gates_rollout_audit_path=None,
        feature_release_gates_output=str(tmp_path / "feature-gates.json"),
        mode="dry",
    )
    called = {}
    monkeypatch.setattr(main, "parse_args", lambda: args)
    monkeypatch.setattr(main, "load_config", lambda env=None: cfg)

    def _fake_feature_release_gates(**kwargs):
        called.update(kwargs)
        return SimpleNamespace(pass_ok=True, target="paper", overall_status="PASS")

    monkeypatch.setattr(main, "run_feature_release_gates", _fake_feature_release_gates)
    monkeypatch.setattr(main, "render_feature_release_gate_summary", lambda report: "feature gates ok")

    assert main.run() == 0
    assert called["target"] == "paper"
    assert called["output_path"] == args.feature_release_gates_output
