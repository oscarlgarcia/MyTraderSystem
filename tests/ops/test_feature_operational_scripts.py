from __future__ import annotations

import json
import importlib.util
from datetime import datetime, timedelta, timezone
from pathlib import Path
import sys
from types import SimpleNamespace

import pytest

from tests.features.http_test_support import feature_http_server


WORKSPACE = Path(__file__).resolve().parents[2]


def _load_script(name: str):
    path = WORKSPACE / "scripts" / f"{name}.py"
    scripts_dir = str(WORKSPACE / "scripts")
    if scripts_dir not in sys.path:
        sys.path.insert(0, scripts_dir)
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    assert spec is not None and spec.loader is not None
    spec.loader.exec_module(module)
    return module


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def test_feature_store_server_resolves_env_defaults(monkeypatch, tmp_path: Path):
    feature_store_server = _load_script("feature_store_server")
    captured: dict[str, object] = {}

    monkeypatch.setattr(
        feature_store_server,
        "load_config",
        lambda env: SimpleNamespace(
            feature_store_server_backend="local_sqlite",
            feature_store_server_path=tmp_path / "online.sqlite",
            feature_store_server_host="127.0.0.1",
            feature_store_server_port=8123,
        ),
    )
    monkeypatch.setattr(feature_store_server, "create_online_store", lambda config: captured.setdefault("config", config) or object())
    monkeypatch.setattr(
        feature_store_server,
        "create_feature_store_api",
        lambda online_store: captured.setdefault("store", online_store) or object(),
    )
    monkeypatch.setattr(
        feature_store_server,
        "uvicorn",
        SimpleNamespace(run=lambda app, host, port: captured.update({"app": app, "host": host, "port": port})),
    )
    monkeypatch.setattr("sys.argv", ["feature_store_server.py", "--env", "dev"])

    assert feature_store_server.main() == 0
    assert captured["config"].backend == "local_sqlite"
    assert Path(captured["config"].path) == tmp_path / "online.sqlite"
    assert captured["host"] == "127.0.0.1"
    assert captured["port"] == 8123


def test_feature_store_smoke_main_checks_healthz(monkeypatch):
    feature_store_smoke = _load_script("feature_store_smoke")
    monkeypatch.setattr(
        feature_store_smoke,
        "httpx",
        SimpleNamespace(
            get=lambda url, timeout: SimpleNamespace(
                raise_for_status=lambda: None,
                json=lambda: {"ok": True},
            )
        ),
    )
    monkeypatch.setattr("sys.argv", ["feature_store_smoke.py", "--base-url", "http://127.0.0.1:8011"])

    assert feature_store_smoke.main() == 0


def test_feature_bootstrap_release_inputs_creates_operational_artifacts(monkeypatch, tmp_path: Path):
    bootstrap = _load_script("feature_bootstrap_release_inputs")
    cfg = SimpleNamespace(
        feature_validation_dir=tmp_path,
        feature_offline_store_path=tmp_path / "offline.sqlite",
        feature_store_server_path=tmp_path / "online.sqlite",
        feature_training_bundle_registry_dir=tmp_path / "training-bundles",
    )
    monkeypatch.setattr(bootstrap, "load_config", lambda env: cfg)
    monkeypatch.setattr(
        "sys.argv",
        [
            "feature_bootstrap_release_inputs.py",
            "--env",
            "dev",
            "--target",
            "paper",
            "--feature-set-name",
            "legacy",
            "--feature-set-version",
            "legacy",
            "--symbol",
            "BTCUSDT",
            "--output-dir",
            str(tmp_path),
            "--event-count",
            "16",
        ],
    )

    assert bootstrap.main() == 0
    assert (tmp_path / "feature_parity_report.json").exists()
    assert (tmp_path / "feature_benchmark_report.json").exists()
    assert (tmp_path / "feature_contract_validation.json").exists()
    assert (tmp_path / "feature_observability.json").exists()
    assert (tmp_path / "feature_rollout_audit.json").exists()
    registry_dir = cfg.feature_training_bundle_registry_dir
    assert registry_dir.exists()
    assert any(registry_dir.iterdir())


def test_feature_release_evidence_main_generates_manifest(monkeypatch, tmp_path: Path):
    feature_evidence = _load_script("feature_release_evidence")
    with feature_http_server() as (server, handler):
        from app.common.dto import FeatureVector

        ts = datetime.now(timezone.utc) - timedelta(seconds=1)
        handler.store.upsert(
            FeatureVector(
                symbol="BTCUSDT",
                ts=ts,
                available_ts=ts,
                values={"price": 100.0},
                feature_set_name="legacy",
                feature_set_version="legacy",
                lineage_id="bundle-1",
            )
        )
        monkeypatch.setattr(
            "sys.argv",
            [
                "feature_release_evidence.py",
                "--primary-url",
                f"http://127.0.0.1:{server.server_port}",
                "--feature-set-name",
                "legacy",
                "--feature-set-version",
                "legacy",
                "--symbol",
                "BTCUSDT",
                "--soak-iterations",
                "5",
                "--concurrency-rounds",
                "2",
                "--concurrency-readers-per-round",
                "3",
                "--output-dir",
                str(tmp_path),
            ],
        )
        assert feature_evidence.main() == 0
        manifest = json.loads((tmp_path / "feature_release_evidence_manifest.json").read_text(encoding="utf-8"))
        assert manifest["pass_ok"] is True
        assert manifest["primary_backend"] == "http"
        assert (tmp_path / "feature_serving_soak.json").exists()
        assert (tmp_path / "feature_serving_concurrency.json").exists()


def test_feature_release_evidence_env_uses_config_defaults(monkeypatch, tmp_path: Path):
    feature_evidence = _load_script("feature_release_evidence")
    with feature_http_server() as (server, handler):
        from app.common.dto import FeatureVector

        ts = datetime.now(timezone.utc) - timedelta(seconds=1)
        handler.store.upsert(
            FeatureVector(
                symbol="BTCUSDT",
                ts=ts,
                available_ts=ts,
                values={"price": 100.0},
                feature_set_name="legacy",
                feature_set_version="legacy",
                lineage_id="bundle-1",
            )
        )
        monkeypatch.setattr(
            feature_evidence,
            "load_config",
            lambda env: SimpleNamespace(
                env=env,
                feature_online_store_url=f"http://127.0.0.1:{server.server_port}",
                feature_validation_dir=tmp_path,
            ),
        )
        monkeypatch.setattr(
            "sys.argv",
            [
                "feature_release_evidence.py",
                "--env",
                "dev",
                "--feature-set-name",
                "legacy",
                "--feature-set-version",
                "legacy",
                "--symbol",
                "BTCUSDT",
                "--soak-iterations",
                "5",
                "--concurrency-rounds",
                "2",
                "--concurrency-readers-per-round",
                "3",
            ],
        )
        assert feature_evidence.main() == 0
        assert (tmp_path / "feature_release_evidence_manifest.json").exists()


def test_feature_release_evidence_live_requires_shadow_url(monkeypatch, tmp_path: Path):
    feature_evidence = _load_script("feature_release_evidence")
    monkeypatch.setattr(
        "sys.argv",
        [
            "feature_release_evidence.py",
            "--primary-url",
            "http://127.0.0.1:9999",
            "--feature-set-name",
            "legacy",
            "--feature-set-version",
            "legacy",
            "--symbol",
            "BTCUSDT",
            "--target",
            "live",
            "--output-dir",
            str(tmp_path),
        ],
    )

    with pytest.raises(SystemExit, match="live feature evidence requires --shadow-url"):
        feature_evidence.main()


def test_feature_live_go_no_go_main_publishes_when_gates_pass(monkeypatch, tmp_path: Path):
    feature_go_no_go = _load_script("feature_live_go_no_go")
    now = datetime.now(timezone.utc).isoformat()
    parity = tmp_path / "parity.json"
    benchmark = tmp_path / "benchmark.json"
    observability = tmp_path / "observability.json"
    contract = tmp_path / "contract.json"
    gates_output = tmp_path / "feature_release_gates.json"
    _write_json(parity, {"generated_at": now, "pass_ok": True, "parity_mismatches": 0})
    _write_json(benchmark, {"generated_at": now, "threshold_pass_ok": True})
    _write_json(
        observability,
        {
            "generated_at": now,
            "metrics": {
                "serving_requests": 10,
                "invalid_serves": 0,
                "stale_serves": 0,
                "serving_latency_max": 0.01,
                "parity_mismatches": 0,
                "shadow_failures": 0,
                "contract_validation_failures": 0,
            },
        },
    )
    _write_json(contract, {"generated_at": now, "pass_ok": True})

    monkeypatch.setattr(
        "sys.argv",
        [
            "feature_live_go_no_go.py",
            "--target",
            "paper",
            "--registry-path",
            str(tmp_path / "feature_releases.json"),
            "--feature-set-name",
            "legacy",
            "--feature-set-version",
            "legacy",
            "--parity-path",
            str(parity),
            "--benchmark-path",
            str(benchmark),
            "--observability-path",
            str(observability),
            "--contract-path",
            str(contract),
            "--online-backend",
            "http",
            "--observability-sink",
            "http",
            "--gates-output",
            str(gates_output),
            "--publish",
        ],
    )
    assert feature_go_no_go.main() == 0
    registry = json.loads((tmp_path / "feature_releases.json").read_text(encoding="utf-8"))
    assert registry["legacy"]["active_version"] == "legacy"


def test_feature_live_go_no_go_env_uses_config_defaults(monkeypatch, tmp_path: Path):
    feature_go_no_go = _load_script("feature_live_go_no_go")
    now = datetime.now(timezone.utc).isoformat()
    parity = tmp_path / "parity.json"
    benchmark = tmp_path / "benchmark.json"
    observability = tmp_path / "observability.json"
    contract = tmp_path / "contract.json"
    gates_output = tmp_path / "feature_release_gates.json"
    registry_path = tmp_path / "feature_releases.json"
    _write_json(parity, {"generated_at": now, "pass_ok": True, "parity_mismatches": 0})
    _write_json(benchmark, {"generated_at": now, "threshold_pass_ok": True})
    _write_json(observability, {"generated_at": now, "metrics": {"serving_requests": 10, "invalid_serves": 0}})
    _write_json(contract, {"generated_at": now, "pass_ok": True})
    monkeypatch.setattr(
        feature_go_no_go,
        "load_config",
        lambda env: SimpleNamespace(
            feature_release_registry_path=registry_path,
            feature_release_online_backend="http",
            feature_observability_sink="http",
            feature_validation_dir=tmp_path,
        ),
    )
    monkeypatch.setattr(
        "sys.argv",
        [
            "feature_live_go_no_go.py",
            "--env",
            "dev",
            "--target",
            "paper",
            "--feature-set-name",
            "legacy",
            "--feature-set-version",
            "legacy",
            "--parity-path",
            str(parity),
            "--benchmark-path",
            str(benchmark),
            "--observability-path",
            str(observability),
            "--contract-path",
            str(contract),
            "--gates-output",
            str(gates_output),
            "--publish",
        ],
    )
    assert feature_go_no_go.main() == 0
    registry = json.loads(registry_path.read_text(encoding="utf-8"))
    assert registry["legacy"]["active_version"] == "legacy"


def test_feature_live_go_no_go_live_requires_evidence_manifest(monkeypatch, tmp_path: Path):
    feature_go_no_go = _load_script("feature_live_go_no_go")
    monkeypatch.setattr(
        "sys.argv",
        [
            "feature_live_go_no_go.py",
            "--target",
            "live",
            "--registry-path",
            str(tmp_path / "feature_releases.json"),
            "--feature-set-name",
            "legacy",
            "--feature-set-version",
            "legacy",
            "--parity-path",
            str(tmp_path / "parity.json"),
            "--benchmark-path",
            str(tmp_path / "benchmark.json"),
            "--observability-path",
            str(tmp_path / "observability.json"),
            "--contract-path",
            str(tmp_path / "contract.json"),
            "--online-backend",
            "http",
            "--observability-sink",
            "http",
            "--gates-output",
            str(tmp_path / "feature_release_gates.json"),
        ],
    )

    with pytest.raises(SystemExit, match="live go/no-go requires --evidence-manifest-path"):
        feature_go_no_go.main()


def test_feature_release_rollback_drill_restores_previous_version(monkeypatch, tmp_path: Path):
    rollback_drill = _load_script("feature_release_rollback_drill")
    registry_path = tmp_path / "feature_releases.json"
    registry_path.write_text(
        json.dumps({"legacy": {"active_version": "2.0.0", "previous_version": "1.0.0"}}, ensure_ascii=False),
        encoding="utf-8",
    )
    monkeypatch.setattr(
        "sys.argv",
        [
            "feature_release_rollback_drill.py",
            "--registry-path",
            str(registry_path),
            "--feature-set-name",
            "legacy",
            "--restore",
        ],
    )
    assert rollback_drill.main() == 0
    registry = json.loads(registry_path.read_text(encoding="utf-8"))
    assert registry["legacy"]["active_version"] == "2.0.0"


def test_feature_release_rollback_drill_env_uses_config_registry(monkeypatch, tmp_path: Path):
    rollback_drill = _load_script("feature_release_rollback_drill")
    registry_path = tmp_path / "feature_releases.json"
    registry_path.write_text(
        json.dumps({"legacy": {"active_version": "2.0.0", "previous_version": "1.0.0"}}, ensure_ascii=False),
        encoding="utf-8",
    )
    monkeypatch.setattr(
        rollback_drill,
        "load_config",
        lambda env: SimpleNamespace(feature_release_registry_path=registry_path),
    )
    monkeypatch.setattr(
        "sys.argv",
        [
            "feature_release_rollback_drill.py",
            "--env",
            "dev",
            "--feature-set-name",
            "legacy",
            "--restore",
        ],
    )
    assert rollback_drill.main() == 0


def test_feature_release_rollback_drill_live_restore_passes_live_readiness(monkeypatch, tmp_path: Path):
    rollback_drill = _load_script("feature_release_rollback_drill")
    registry_path = tmp_path / "feature_releases.json"
    registry_path.write_text(
        json.dumps({"legacy_live": {"active_version": "v2", "previous_version": "v1"}}, ensure_ascii=False),
        encoding="utf-8",
    )
    monkeypatch.setattr(
        "sys.argv",
        [
            "feature_release_rollback_drill.py",
            "--registry-path",
            str(registry_path),
            "--feature-set-name",
            "legacy_live",
            "--target",
            "live",
            "--restore",
        ],
    )

    assert rollback_drill.main() == 0
    registry = json.loads(registry_path.read_text(encoding="utf-8"))
    assert registry["legacy_live"]["active_version"] == "v2"
