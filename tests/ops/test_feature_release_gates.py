import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

from app.ops.feature_release_gates import render_feature_release_summary, run_feature_release_gates


NOW = datetime.now(timezone.utc)


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def test_feature_release_gates_paper_passes_with_clean_artifacts(tmp_path: Path):
    parity_path = tmp_path / "parity.json"
    benchmark_path = tmp_path / "benchmark.json"
    observability_path = tmp_path / "observability.json"
    contract_path = tmp_path / "contract.json"
    _write_json(parity_path, {"generated_at": NOW.isoformat(), "pass_ok": True, "parity_mismatches": 0})
    _write_json(benchmark_path, {"generated_at": NOW.isoformat(), "threshold_pass_ok": True})
    _write_json(
        observability_path,
        {
            "generated_at": NOW.isoformat(),
            "metrics": {
                "serving_requests": 10,
                "invalid_serves": 0,
                "stale_serves": 0,
                "serving_latency_max": 0.01,
                "parity_mismatches": 0,
                "shadow_failures": 0,
                "contract_validation_failures": 0,
            },
            "alerts": [],
        },
    )
    _write_json(contract_path, {"generated_at": NOW.isoformat(), "pass_ok": True})

    report = run_feature_release_gates(
        target="paper",
        parity_path=parity_path,
        benchmark_path=benchmark_path,
        observability_path=observability_path,
        contract_path=contract_path,
        online_backend="http",
        observability_sink="http",
        output_path=tmp_path / "feature-release-gates.json",
    )

    assert report.pass_ok is True
    assert report.gate_report.pass_ok is True
    assert report.live_readiness is None
    assert "Feature release gates: PASS (paper)" in render_feature_release_summary(report)


def test_feature_release_gates_live_requires_operational_artifacts(tmp_path: Path):
    parity_path = tmp_path / "parity.json"
    benchmark_path = tmp_path / "benchmark.json"
    observability_path = tmp_path / "observability.json"
    contract_path = tmp_path / "contract.json"
    _write_json(parity_path, {"generated_at": NOW.isoformat(), "pass_ok": True, "parity_mismatches": 0})
    _write_json(benchmark_path, {"generated_at": NOW.isoformat(), "threshold_pass_ok": True})
    _write_json(observability_path, {"generated_at": NOW.isoformat(), "metrics": {"serving_requests": 10}})
    _write_json(contract_path, {"generated_at": NOW.isoformat(), "pass_ok": True})

    with pytest.raises(ValueError, match="live feature release gates require shadow, soak, concurrency and rollout audit artifacts"):
        run_feature_release_gates(
            target="live",
            parity_path=parity_path,
            benchmark_path=benchmark_path,
            observability_path=observability_path,
            contract_path=contract_path,
            online_backend="http",
            observability_sink="http",
        )


def test_feature_release_gates_live_passes_with_shadow_soak_concurrency_and_rollout(tmp_path: Path):
    parity_path = tmp_path / "parity.json"
    benchmark_path = tmp_path / "benchmark.json"
    observability_path = tmp_path / "observability.json"
    contract_path = tmp_path / "contract.json"
    shadow_path = tmp_path / "shadow.jsonl"
    soak_path = tmp_path / "soak.json"
    concurrency_path = tmp_path / "concurrency.json"
    rollout_audit_path = tmp_path / "rollout.json"
    _write_json(parity_path, {"generated_at": NOW.isoformat(), "pass_ok": True, "parity_mismatches": 0})
    _write_json(benchmark_path, {"generated_at": NOW.isoformat(), "threshold_pass_ok": True})
    _write_json(
        observability_path,
        {
            "generated_at": NOW.isoformat(),
            "metrics": {
                "serving_requests": 10,
                "invalid_serves": 0,
                "stale_serves": 0,
                "serving_latency_max": 0.01,
                "parity_mismatches": 0,
                "shadow_failures": 0,
                "contract_validation_failures": 0,
            },
            "alerts": [],
        },
    )
    _write_json(contract_path, {"generated_at": NOW.isoformat(), "pass_ok": True})
    shadow_path.write_text(json.dumps({"timestamp": NOW.isoformat(), "pass_ok": True, "severity": "info"}) + "\n", encoding="utf-8")
    _write_json(soak_path, {"generated_at": NOW.isoformat(), "pass_ok": True, "max_latency_seconds": 0.1})
    _write_json(concurrency_path, {"generated_at": NOW.isoformat(), "pass_ok": True, "max_latency_seconds": 0.1})
    _write_json(rollout_audit_path, {"generated_at": NOW.isoformat(), "pass_ok": True})

    report = run_feature_release_gates(
        target="live",
        parity_path=parity_path,
        benchmark_path=benchmark_path,
        observability_path=observability_path,
        contract_path=contract_path,
        online_backend="http",
        observability_sink="http",
        shadow_path=shadow_path,
        soak_path=soak_path,
        concurrency_path=concurrency_path,
        rollout_audit_path=rollout_audit_path,
    )
    assert report.pass_ok is True
    assert report.live_readiness is not None
    assert report.live_readiness["pass_ok"] is True

