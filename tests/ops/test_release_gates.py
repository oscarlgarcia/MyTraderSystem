import json
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest import mock

import pytest

import app.ops.release_gates as release_gates
from app.ops.observability_contract import build_observability_contract_report
from app.ops.release_gates import render_release_gate_summary, run_release_gates


NOW = datetime.now(timezone.utc)


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def _write_shadow_comparison(path: Path, *, significant: bool) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "ts": NOW.isoformat(),
        "diffs": {"row_count": 0, "identity_count": 0},
        "significant": significant,
    }
    path.write_text(json.dumps(payload, ensure_ascii=False) + "\n", encoding="utf-8")


def _write_metadata_snapshot(base_dir: Path, *, env: str, mode: str = "runtime", material_drift: bool = False) -> None:
    path = base_dir / "metadata" / "instruments" / f"env={env}" / "venue=BINANCE" / "latest.json"
    _write_json(
        path,
        {
            "metadata_snapshot_mode": mode,
            "venue_snapshot_path": str(base_dir / "metadata" / "vendor" / "exchangeInfo.json"),
            "venue_snapshot_version": "runtime-v1",
            "fallback_reason": None if mode == "runtime" else "network down",
            "drift": {
                "has_drift": material_drift,
                "material": material_drift,
                "added_symbols": [],
                "removed_symbols": [],
                "changed_symbols": ["BTCUSDT"] if material_drift else [],
                "changed_fields_by_symbol": {"BTCUSDT": ["tick_size"]} if material_drift else {},
            },
        },
    )


def _rewrite_runtime_artifact_target(path: Path, *, target_profile: str) -> None:
    payload = json.loads(path.read_text(encoding="utf-8"))
    payload["target_profile"] = target_profile
    _write_json(path, payload)


def _write_release_artifacts(
    tmp_path: Path,
    *,
    stale_rest: bool = False,
    target: str = "paper",
    stream_type: str = "kline",
    phase: str = "final",
) -> tuple[Path, Path, Path, Path, Path, Path, Path, Path, Path]:
    rest_path = tmp_path / "rest.json"
    ws_path = tmp_path / "ws.json"
    benchmark_path = tmp_path / "benchmark.json"
    parity_path = tmp_path / "parity.json"
    soak_path = tmp_path / "soak.json"
    vendor_contracts_path = tmp_path / "vendor-contracts.json"
    failure_injection_path = tmp_path / "failure-injection.json"
    live_drill_path = tmp_path / "live-drill.json"
    operational_evidence_path = tmp_path / "operational-evidence.json"
    rest_generated_at = (NOW - timedelta(days=2)).isoformat() if stale_rest else NOW.isoformat()
    _write_json(
        rest_path,
        {
            "generated_at": rest_generated_at,
            "pass_ok": True,
            "diffs": {"row_count": 0},
            "comparison_reason": "semantic_match",
        },
    )
    _write_json(
        ws_path,
        {
            "report_generated_at": NOW.isoformat(),
            "target_profile": "paper",
            "pass_ok": True,
            "symbol": "BTCUSDT",
            "stream_type": "kline",
            "continuity": {
                "reconnects": 1,
                "duplicates": 0,
                "gaps": 0,
                "gap_irreparable": 0,
                "streams_degraded": [],
                "heartbeat_missed_total": 0,
                "exchange_receive_skew_seconds": 0.1,
                "receive_process_skew_seconds": 0.1,
                "processing_latency_seconds": 0.1,
            },
            "slo": {"target_profile": "paper"},
            "reconnects_observed": 1,
            "reconnects_target": 1,
            "comparison_reason": "continuity_ok",
        },
    )
    _write_json(
        benchmark_path,
        {
            "generated_at": NOW.isoformat(),
            "target_profile": "paper",
            "pass_ok": True,
            "required_high_cardinality_symbol_counts": [100],
            "slo": {"min_rows_per_second": 1.0},
            "synthetic_case": {"pass_ok": True, "rows_per_second": 10.0, "requested_symbol_count": 12},
            "replay_case": {"pass_ok": True, "rows_per_second": 10.0, "requested_symbol_count": 12},
            "concurrent_compaction_case": {"pass_ok": True, "rows_per_second": 10.0, "requested_symbol_count": 12},
            "shadow_scoped_case": {"pass_ok": True, "rows_per_second": 10.0, "requested_symbol_count": 4},
            "high_cardinality_cases": [
                {"name": "high_cardinality_100", "pass_ok": True, "rows_per_second": 10.0, "requested_symbol_count": 100}
            ],
        },
    )
    _write_json(
        parity_path,
        {
            "generated_at": NOW.isoformat(),
            "pass_ok": True,
            "order_match": True,
            "manifest_ok": True,
            "normalized_path": str(tmp_path / "normalized" / "bars" / "env=dev" / "venue=BINANCE" / "symbol=BTCUSDT" / "date=2024-01-01"),
            "symbol": "BTCUSDT",
            "stream_type": "kline",
            "manifest_missing_files": [],
            "manifest_mismatches": [],
        },
    )
    _write_json(
        soak_path,
        {
            "generated_at": NOW.isoformat(),
            "target_profile": "paper",
            "pass_ok": True,
            "stream_type": "kline",
            "max_allowed_gaps": 0,
            "max_gaps": 0,
            "max_allowed_duplicates": 0,
            "max_duplicates": 0,
            "max_allowed_gap_irreparable": 0,
            "max_gap_irreparable": 0,
            "max_allowed_heartbeat_missed_total": 0,
            "max_heartbeat_missed_total": 0,
            "max_allowed_exchange_receive_skew_seconds": 30.0,
            "max_exchange_receive_skew_seconds": 0.1,
            "max_allowed_receive_process_skew_seconds": 5.0,
            "max_receive_process_skew_seconds": 0.1,
            "max_allowed_processing_latency_seconds": 5.0,
            "max_processing_latency_seconds": 0.1,
            "max_allowed_compaction_failures": 0,
            "compaction_failures_total": 0,
            "max_streams_degraded": 0,
            "reconnects_observed": 1,
            "reconnects_target": 1,
        },
    )
    _write_json(
        vendor_contracts_path,
        {
            "generated_at": NOW.isoformat(),
            "pass_ok": True,
            "pytest_target": "tests/network/test_binance_contracts.py",
            "command": ["python", "-m", "pytest"],
            "duration_seconds": 1.0,
            "returncode": 0,
        },
    )
    _write_json(
        failure_injection_path,
        {
            "generated_at": NOW.isoformat(),
            "pass_ok": True,
            "pytest_target": "tests/ops/test_failure_injection.py",
            "critical_test_ids": [
                "tests/ops/test_failure_injection.py::test_failure_injection_release_gate_fails_with_stale_ws_artifact",
                "tests/ops/test_failure_injection.py::test_failure_injection_prod_rejects_fallback_metadata_snapshot",
                "tests/ops/test_failure_injection.py::test_failure_injection_release_gate_fails_with_manifest_mismatch",
            ],
            "command": ["python", "-m", "pytest"],
            "duration_seconds": 1.0,
            "returncode": 0,
        },
    )
    _write_json(
        live_drill_path,
        {
            "generated_at": NOW.isoformat(),
            "drill_executed": True,
            "promote_ready": True,
            "rollback_ready": True,
            "overall_status": "PASS",
        },
    )
    observability_contract = build_observability_contract_report(target=target)
    _write_json(
        operational_evidence_path,
        {
            "generated_at": NOW.isoformat(),
            "target": target,
            "phase": phase,
            "stream_types": [stream_type],
            "cadence_policy": {
                "runtime_artifact_max_age_seconds": 86400,
                "benchmark_and_replay_max_age_seconds": 604800,
                "runtime_artifact_expected_interval_seconds": 21600,
                "live_drill_expected_interval_seconds": 43200,
                "required_artifacts": ["replay_parity", "storage_benchmark", "vendor_contracts"],
            },
            "evidence_origin": "operational_runtime" if target == "live" else "paper_operational",
            "provenance": {
                "source": "readiness_orchestrator",
                "runner_id": f"tests:{target}:{stream_type}:{phase}",
                "trigger": f"tests_{target}_{phase}",
                "generated_by": "tests/ops/test_release_gates.py",
                "verification_scope": "external_operational_surfaces",
                "derived_in_process": False,
            },
            "excluded_feed_policy": {"book": "excluded"},
            "observability": {
                "pass_ok": True,
                "repo_runbooks": [
                    "docs/operations/ingestion_runbook.md",
                    "docs/operations/ingestion_promotion_runbook.md",
                ],
                "external_surfaces": [
                    {
                        "surface_id": surface.surface_id,
                        "kind": surface.kind,
                        "description": surface.description,
                        "repo_reference": surface.repo_reference,
                        "owner": surface.owner,
                        "surface_ref": surface.surface_ref,
                        "verification_mode": surface.verification_mode,
                        "verified_at": NOW.isoformat(),
                        "verification_ref": f"artifact://tests/{surface.surface_id}",
                        "pass_ok": True,
                    }
                    for surface in observability_contract.external_surfaces
                ],
            },
            "artifacts": [
                {"name": "replay_parity", "required": True, "pass_ok": True, "fresh": True},
                {"name": "storage_benchmark", "required": True, "pass_ok": True, "fresh": True},
                {"name": "vendor_contracts", "required": True, "pass_ok": True, "fresh": True},
            ],
            "pass_ok": True,
            "reasons": ["operational evidence fresh and aligned"],
        },
    )
    return (
        rest_path,
        ws_path,
        benchmark_path,
        parity_path,
        soak_path,
        vendor_contracts_path,
        failure_injection_path,
        live_drill_path,
        operational_evidence_path,
    )


def test_release_gates_paper_passes_with_clean_artifacts(tmp_path: Path):
    (
        rest_path,
        ws_path,
        benchmark_path,
        parity_path,
        soak_path,
        vendor_contracts_path,
        failure_injection_path,
        live_drill_path,
        operational_evidence_path,
    ) = _write_release_artifacts(tmp_path)
    _write_metadata_snapshot(tmp_path, env="dev", mode="runtime")

    report = run_release_gates(
        base_dir=tmp_path,
        env="dev",
        target="paper",
        stream_types=("kline",),
        output_path=tmp_path / "release-gates.json",
        rest_canary_path=rest_path,
        ws_canary_path=ws_path,
        replay_parity_path=parity_path,
        benchmark_path=benchmark_path,
        soak_path=soak_path,
        network_contracts_path=vendor_contracts_path,
        failure_injection_path=failure_injection_path,
        live_drill_path=live_drill_path,
        operational_evidence_path=operational_evidence_path,
    )

    assert report.pass_ok is True
    assert report.overall_status == "PASS"
    assert report.base_dir == str(tmp_path)
    blocks = {block.name: block for block in report.blocks}
    assert blocks["instrument_metadata"].status == "pass"
    assert blocks["storage_benchmark"].status == "pass"
    assert blocks["replay_parity"].status == "pass"
    assert blocks["paper_soak"].status == "pass"
    assert blocks["vendor_contracts"].status == "pass"
    assert blocks["operational_evidence"].status == "pass"
    assert blocks["operational_observability"].status == "pass"
    assert blocks["observability_contract"].status == "pass"
    assert blocks["observability_contract"].details["target"] == "paper"
    assert "exchange_receive_skew_seconds" in blocks["observability_contract"].details["required_metric_thresholds"]
    assert "invalid_timestamp_detected" in blocks["observability_contract"].details["required_alerts"]
    assert blocks["live_drill"].status == "pass"
    assert blocks["replay_parity"].details["path"] == str(parity_path)
    assert blocks["canary_ws"].details["path"] == str(ws_path)
    assert blocks["storage_benchmark"].details["path"] == str(benchmark_path)
    assert blocks["vendor_contracts"].details["path"] == str(vendor_contracts_path)
    written = json.loads((tmp_path / "release-gates.json").read_text(encoding="utf-8"))
    assert written["overall_status"] == "PASS"
    assert written["base_dir"] == str(tmp_path)
    assert "Release gates: PASS (paper)" in render_release_gate_summary(report)


def test_release_gates_paper_trade_passes_without_runtime_proxy_artifacts(tmp_path: Path):
    _write_metadata_snapshot(tmp_path, env="dev", mode="runtime")
    benchmark_path = tmp_path / "benchmark.json"
    parity_path = tmp_path / "parity.json"
    vendor_contracts_path = tmp_path / "vendor-contracts.json"
    operational_evidence_path = tmp_path / "operational-evidence.json"
    _write_json(
        benchmark_path,
        {
            "generated_at": NOW.isoformat(),
            "target_profile": "paper",
            "pass_ok": True,
            "required_high_cardinality_symbol_counts": [100],
            "slo": {"min_rows_per_second": 1.0},
            "synthetic_case": {"pass_ok": True, "rows_per_second": 10.0, "requested_symbol_count": 12},
            "replay_case": {"pass_ok": True, "rows_per_second": 10.0, "requested_symbol_count": 12},
            "concurrent_compaction_case": {"pass_ok": True, "rows_per_second": 10.0, "requested_symbol_count": 12},
            "shadow_scoped_case": {"pass_ok": True, "rows_per_second": 10.0, "requested_symbol_count": 4},
            "high_cardinality_cases": [
                {"name": "high_cardinality_100", "pass_ok": True, "rows_per_second": 10.0, "requested_symbol_count": 100}
            ],
        },
    )
    _write_json(
        parity_path,
        {
            "generated_at": NOW.isoformat(),
            "pass_ok": True,
            "order_match": True,
            "manifest_ok": True,
            "normalized_path": str(tmp_path / "normalized" / "trades" / "env=papercand"),
            "symbol": "BTCUSDT",
            "stream_type": "trade",
            "manifest_missing_files": [],
            "manifest_mismatches": [],
        },
    )
    _write_json(
        vendor_contracts_path,
        {
            "generated_at": NOW.isoformat(),
            "pass_ok": True,
            "pytest_target": "tests/network/test_binance_contracts.py",
            "command": ["python", "-m", "pytest"],
            "duration_seconds": 1.0,
            "returncode": 0,
        },
    )

    _write_json(
        operational_evidence_path,
        {
            "generated_at": NOW.isoformat(),
            "target": "paper",
            "phase": "final",
            "stream_types": ["trade"],
            "cadence_policy": {
                "runtime_artifact_max_age_seconds": 86400,
                "benchmark_and_replay_max_age_seconds": 604800,
                "runtime_artifact_expected_interval_seconds": 21600,
                "live_drill_expected_interval_seconds": 43200,
                "required_artifacts": ["replay_parity", "storage_benchmark", "vendor_contracts"],
            },
            "evidence_origin": "paper_operational",
            "provenance": {
                "source": "readiness_orchestrator",
                "runner_id": "tests:paper:trade:final",
                "trigger": "tests_paper_final",
                "generated_by": "tests/ops/test_release_gates.py",
                "verification_scope": "external_operational_surfaces",
                "derived_in_process": False,
            },
            "excluded_feed_policy": {"book": "excluded"},
            "observability": {
                "pass_ok": True,
                "repo_runbooks": [
                    "docs/operations/ingestion_runbook.md",
                    "docs/operations/ingestion_promotion_runbook.md",
                ],
                "external_surfaces": [
                    {
                        "surface_id": surface.surface_id,
                        "kind": surface.kind,
                        "description": surface.description,
                        "repo_reference": surface.repo_reference,
                        "owner": surface.owner,
                        "surface_ref": surface.surface_ref,
                        "verification_mode": surface.verification_mode,
                        "verified_at": NOW.isoformat(),
                        "verification_ref": f"artifact://tests/{surface.surface_id}",
                        "pass_ok": True,
                    }
                    for surface in build_observability_contract_report(target="paper").external_surfaces
                ],
            },
            "artifacts": [
                {"name": "replay_parity", "required": True, "pass_ok": True, "fresh": True},
                {"name": "storage_benchmark", "required": True, "pass_ok": True, "fresh": True},
                {"name": "vendor_contracts", "required": True, "pass_ok": True, "fresh": True},
            ],
            "pass_ok": True,
            "reasons": ["operational evidence fresh and aligned"],
        },
    )

    report = run_release_gates(
        base_dir=tmp_path,
        env="dev",
        target="paper",
        stream_types=("trade",),
        benchmark_path=benchmark_path,
        replay_parity_path=parity_path,
        network_contracts_path=vendor_contracts_path,
        rest_canary_path=tmp_path / "missing-rest.json",
        ws_canary_path=tmp_path / "missing-ws.json",
        soak_path=tmp_path / "missing-soak.json",
        failure_injection_path=tmp_path / "missing-failure.json",
        live_drill_path=tmp_path / "missing-live-drill.json",
        operational_evidence_path=operational_evidence_path,
    )

    assert report.pass_ok is True
    blocks = {block.name: block for block in report.blocks}
    assert blocks["support_matrix"].status == "pass"
    assert blocks["support_matrix"].details["feeds"]["trade"]["supports_paper"] is True
    assert blocks["support_matrix"].details["feeds"]["trade"]["paper_validation_basis"] == "replay_validated"
    assert blocks["evidence_contract"].status == "warn"
    assert blocks["canary_rest"].required is False
    assert blocks["canary_ws"].required is False
    assert blocks["paper_soak"].required is False


def test_release_gates_paper_book_is_rejected_by_support_matrix(tmp_path: Path):
    _write_metadata_snapshot(tmp_path, env="dev", mode="runtime")
    benchmark_path = tmp_path / "benchmark.json"
    parity_path = tmp_path / "parity.json"
    vendor_contracts_path = tmp_path / "vendor-contracts.json"
    operational_evidence_path = tmp_path / "operational-evidence.json"
    _write_json(
        benchmark_path,
        {
            "generated_at": NOW.isoformat(),
            "target_profile": "paper",
            "pass_ok": True,
            "required_high_cardinality_symbol_counts": [100],
            "slo": {"min_rows_per_second": 1.0},
            "synthetic_case": {"pass_ok": True, "rows_per_second": 10.0, "requested_symbol_count": 12},
            "replay_case": {"pass_ok": True, "rows_per_second": 10.0, "requested_symbol_count": 12},
            "concurrent_compaction_case": {"pass_ok": True, "rows_per_second": 10.0, "requested_symbol_count": 12},
            "shadow_scoped_case": {"pass_ok": True, "rows_per_second": 10.0, "requested_symbol_count": 4},
            "high_cardinality_cases": [
                {"name": "high_cardinality_100", "pass_ok": True, "rows_per_second": 10.0, "requested_symbol_count": 100}
            ],
        },
    )
    _write_json(
        parity_path,
        {
            "generated_at": NOW.isoformat(),
            "pass_ok": True,
            "order_match": True,
            "manifest_ok": True,
            "normalized_path": str(tmp_path / "normalized" / "book" / "env=papercand"),
            "symbol": "BTCUSDT",
            "stream_type": "book",
            "manifest_missing_files": [],
            "manifest_mismatches": [],
        },
    )
    _write_json(
        vendor_contracts_path,
        {
            "generated_at": NOW.isoformat(),
            "pass_ok": True,
            "pytest_target": "tests/network/test_binance_contracts.py",
            "command": ["python", "-m", "pytest"],
            "duration_seconds": 1.0,
            "returncode": 0,
        },
    )

    _write_json(
        operational_evidence_path,
        {
            "generated_at": NOW.isoformat(),
            "target": "paper",
            "phase": "final",
            "stream_types": ["book"],
            "cadence_policy": {
                "runtime_artifact_max_age_seconds": 86400,
                "benchmark_and_replay_max_age_seconds": 604800,
            },
            "evidence_origin": "paper_operational",
            "provenance": {
                "source": "readiness_orchestrator",
                "runner_id": "tests:paper:book:final",
                "trigger": "tests_paper_final",
                "generated_by": "tests/ops/test_release_gates.py",
                "verification_scope": "external_operational_surfaces",
                "derived_in_process": False,
            },
            "excluded_feed_policy": {"book": "excluded"},
            "observability": {
                "pass_ok": True,
                "repo_runbooks": [
                    "docs/operations/ingestion_runbook.md",
                    "docs/operations/ingestion_promotion_runbook.md",
                ],
                "external_surfaces": [
                    {
                        "surface_id": surface.surface_id,
                        "kind": surface.kind,
                        "description": surface.description,
                        "repo_reference": surface.repo_reference,
                        "owner": surface.owner,
                        "surface_ref": surface.surface_ref,
                        "verification_mode": surface.verification_mode,
                        "verified_at": NOW.isoformat(),
                        "verification_ref": f"artifact://tests/{surface.surface_id}",
                        "pass_ok": True,
                    }
                    for surface in build_observability_contract_report(target="paper").external_surfaces
                ],
            },
            "artifacts": [
                {"name": "replay_parity", "required": True, "pass_ok": True, "fresh": True},
                {"name": "storage_benchmark", "required": True, "pass_ok": True, "fresh": True},
                {"name": "vendor_contracts", "required": True, "pass_ok": True, "fresh": True},
            ],
            "pass_ok": True,
            "reasons": ["operational evidence fresh and aligned"],
        },
    )

    report = run_release_gates(
        base_dir=tmp_path,
        env="dev",
        target="paper",
        stream_types=("book",),
        benchmark_path=benchmark_path,
        replay_parity_path=parity_path,
        network_contracts_path=vendor_contracts_path,
        operational_evidence_path=operational_evidence_path,
    )

    block = next(block for block in report.blocks if block.name == "support_matrix")
    assert block.status == "fail"
    assert any("book does not support paper ingestion" in reason for reason in block.reasons)


def test_release_gates_fail_when_required_artifact_is_stale(tmp_path: Path):
    (
        rest_path,
        ws_path,
        benchmark_path,
        parity_path,
        soak_path,
        vendor_contracts_path,
        failure_injection_path,
        live_drill_path,
        operational_evidence_path,
    ) = _write_release_artifacts(tmp_path, stale_rest=True)
    _write_metadata_snapshot(tmp_path, env="dev", mode="runtime")

    report = run_release_gates(
        base_dir=tmp_path,
        env="dev",
        target="paper",
        stream_types=("kline",),
        rest_canary_path=rest_path,
        ws_canary_path=ws_path,
        replay_parity_path=parity_path,
        benchmark_path=benchmark_path,
        soak_path=soak_path,
        network_contracts_path=vendor_contracts_path,
        failure_injection_path=failure_injection_path,
        live_drill_path=live_drill_path,
    )

    assert report.pass_ok is False
    canary_block = next(block for block in report.blocks if block.name == "canary_rest")
    assert canary_block.status == "fail"
    assert any("artifact stale" in reason for reason in canary_block.reasons)


def test_release_gates_fail_when_explicit_operational_evidence_artifact_is_missing(tmp_path: Path):
    rest_path, ws_path, benchmark_path, parity_path, soak_path, vendor_contracts_path, failure_injection_path, live_drill_path, _ = _write_release_artifacts(tmp_path)
    _write_metadata_snapshot(tmp_path, env="dev", mode="runtime")

    report = run_release_gates(
        base_dir=tmp_path,
        env="dev",
        target="paper",
        stream_types=("kline",),
        rest_canary_path=rest_path,
        ws_canary_path=ws_path,
        replay_parity_path=parity_path,
        benchmark_path=benchmark_path,
        soak_path=soak_path,
        network_contracts_path=vendor_contracts_path,
        failure_injection_path=failure_injection_path,
        live_drill_path=live_drill_path,
        operational_evidence_path=tmp_path / "missing-operational-evidence.json",
    )

    block = next(block for block in report.blocks if block.name == "operational_evidence")
    assert block.status == "fail"
    assert any("missing artifact" in reason for reason in block.reasons)


def test_release_gates_fail_when_operational_evidence_is_only_derived_inline(tmp_path: Path):
    rest_path, ws_path, benchmark_path, parity_path, soak_path, vendor_contracts_path, failure_injection_path, live_drill_path, _ = _write_release_artifacts(tmp_path)
    _write_metadata_snapshot(tmp_path, env="dev", mode="runtime")

    report = run_release_gates(
        base_dir=tmp_path,
        env="dev",
        target="paper",
        stream_types=("kline",),
        rest_canary_path=rest_path,
        ws_canary_path=ws_path,
        replay_parity_path=parity_path,
        benchmark_path=benchmark_path,
        soak_path=soak_path,
        network_contracts_path=vendor_contracts_path,
        failure_injection_path=failure_injection_path,
        live_drill_path=live_drill_path,
    )

    block = next(block for block in report.blocks if block.name == "operational_evidence")
    assert block.status == "fail"
    assert any("inline derived evidence" in reason for reason in block.reasons)


def test_release_gates_fail_when_observability_surface_verification_is_incomplete(tmp_path: Path):
    rest_path, ws_path, benchmark_path, parity_path, soak_path, vendor_contracts_path, failure_injection_path, live_drill_path, operational_evidence_path = _write_release_artifacts(tmp_path)
    _write_metadata_snapshot(tmp_path, env="dev", mode="runtime")
    payload = json.loads(operational_evidence_path.read_text(encoding="utf-8"))
    payload["observability"]["external_surfaces"][0]["surface_ref"] = ""
    _write_json(operational_evidence_path, payload)

    report = run_release_gates(
        base_dir=tmp_path,
        env="dev",
        target="paper",
        stream_types=("kline",),
        rest_canary_path=rest_path,
        ws_canary_path=ws_path,
        replay_parity_path=parity_path,
        benchmark_path=benchmark_path,
        soak_path=soak_path,
        network_contracts_path=vendor_contracts_path,
        failure_injection_path=failure_injection_path,
        live_drill_path=live_drill_path,
        operational_evidence_path=operational_evidence_path,
    )

    block = next(block for block in report.blocks if block.name == "operational_observability")
    assert block.status == "fail"
    assert any("missing surface_ref" in reason for reason in block.reasons)


def test_release_gates_live_requires_runtime_metadata_and_live_drill(tmp_path: Path):
    rest_path, ws_path, benchmark_path, parity_path, soak_path, vendor_contracts_path, failure_injection_path, live_drill_path, operational_evidence_path = _write_release_artifacts(tmp_path)
    _rewrite_runtime_artifact_target(ws_path, target_profile="live")
    _rewrite_runtime_artifact_target(soak_path, target_profile="live")
    benchmark_payload = json.loads(benchmark_path.read_text(encoding="utf-8"))
    benchmark_payload["target_profile"] = "live"
    benchmark_payload["required_high_cardinality_symbol_counts"] = [100, 500]
    benchmark_payload["high_cardinality_cases"] = [
        {"name": "high_cardinality_100", "pass_ok": True, "rows_per_second": 10.0, "requested_symbol_count": 100},
        {"name": "high_cardinality_500", "pass_ok": True, "rows_per_second": 10.0, "requested_symbol_count": 500},
    ]
    _write_json(benchmark_path, benchmark_payload)
    _write_shadow_comparison(tmp_path / "shadow" / "env=dev" / "comparisons.jsonl", significant=False)
    _write_metadata_snapshot(tmp_path, env="dev", mode="fallback")

    report = run_release_gates(
        base_dir=tmp_path,
        env="dev",
        target="live",
        stream_types=("kline",),
        rest_canary_path=rest_path,
        ws_canary_path=ws_path,
        replay_parity_path=parity_path,
        benchmark_path=benchmark_path,
        soak_path=soak_path,
        network_contracts_path=vendor_contracts_path,
        failure_injection_path=failure_injection_path,
        live_drill_path=live_drill_path,
        operational_evidence_path=operational_evidence_path,
    )

    assert report.pass_ok is False
    metadata_block = next(block for block in report.blocks if block.name == "instrument_metadata")
    assert metadata_block.status == "fail"
    assert any("runtime instrument metadata snapshot" in reason for reason in metadata_block.reasons)


def test_release_gates_live_trade_passes_without_rest_canary(tmp_path: Path):
    _write_metadata_snapshot(tmp_path, env="dev", mode="runtime")
    benchmark_path = tmp_path / "benchmark.json"
    parity_path = tmp_path / "parity.json"
    ws_path = tmp_path / "ws.json"
    soak_path = tmp_path / "soak.json"
    vendor_contracts_path = tmp_path / "vendor-contracts.json"
    failure_injection_path = tmp_path / "failure-injection.json"
    live_drill_path = tmp_path / "live-drill.json"
    operational_evidence_path = tmp_path / "operational-evidence.json"
    _write_json(
        benchmark_path,
        {
            "generated_at": NOW.isoformat(),
            "target_profile": "live",
            "pass_ok": True,
            "required_high_cardinality_symbol_counts": [100, 500],
            "slo": {"min_rows_per_second": 1.0},
            "synthetic_case": {"pass_ok": True, "rows_per_second": 10.0, "requested_symbol_count": 12},
            "replay_case": {"pass_ok": True, "rows_per_second": 10.0, "requested_symbol_count": 12},
            "concurrent_compaction_case": {"pass_ok": True, "rows_per_second": 10.0, "requested_symbol_count": 12},
            "shadow_scoped_case": {"pass_ok": True, "rows_per_second": 10.0, "requested_symbol_count": 4},
            "high_cardinality_cases": [
                {"name": "high_cardinality_100", "pass_ok": True, "rows_per_second": 10.0, "requested_symbol_count": 100},
                {"name": "high_cardinality_500", "pass_ok": True, "rows_per_second": 10.0, "requested_symbol_count": 500},
            ],
        },
    )
    _write_json(
        parity_path,
        {
            "generated_at": NOW.isoformat(),
            "pass_ok": True,
            "order_match": True,
            "manifest_ok": True,
            "normalized_path": str(tmp_path / "normalized" / "trades" / "env=livecand"),
            "symbol": "BTCUSDT",
            "stream_type": "trade",
            "manifest_missing_files": [],
            "manifest_mismatches": [],
        },
    )
    _write_json(
        ws_path,
        {
            "report_generated_at": NOW.isoformat(),
            "target_profile": "live",
            "pass_ok": True,
            "symbol": "BTCUSDT",
            "stream_type": "trade",
            "continuity": {
                "reconnects": 1,
                "duplicates": 0,
                "gaps": 0,
                "gap_irreparable": 0,
                "streams_degraded": [],
                "heartbeat_missed_total": 0,
                "exchange_receive_skew_seconds": 0.1,
                "receive_process_skew_seconds": 0.1,
                "processing_latency_seconds": 0.1,
            },
            "slo": {"target_profile": "live"},
            "reconnects_observed": 1,
            "reconnects_target": 1,
            "comparison_reason": "continuity_ok",
        },
    )
    _write_json(
        soak_path,
        {
            "generated_at": NOW.isoformat(),
            "target_profile": "live",
            "pass_ok": True,
            "stream_type": "trade",
            "max_allowed_gaps": 0,
            "max_gaps": 0,
            "max_allowed_duplicates": 0,
            "max_duplicates": 0,
            "max_allowed_gap_irreparable": 0,
            "max_gap_irreparable": 0,
            "max_allowed_heartbeat_missed_total": 0,
            "max_heartbeat_missed_total": 0,
            "max_allowed_exchange_receive_skew_seconds": 30.0,
            "max_exchange_receive_skew_seconds": 0.1,
            "max_allowed_receive_process_skew_seconds": 5.0,
            "max_receive_process_skew_seconds": 0.1,
            "max_allowed_processing_latency_seconds": 5.0,
            "max_processing_latency_seconds": 0.1,
            "max_allowed_compaction_failures": 0,
            "compaction_failures_total": 0,
            "max_streams_degraded": 0,
            "reconnects_observed": 1,
            "reconnects_target": 1,
        },
    )
    _write_json(
        vendor_contracts_path,
        {
            "generated_at": NOW.isoformat(),
            "pass_ok": True,
            "pytest_target": "tests/network/test_binance_contracts.py",
            "command": ["python", "-m", "pytest"],
            "duration_seconds": 1.0,
            "returncode": 0,
        },
    )
    _write_json(
        failure_injection_path,
        {
            "generated_at": NOW.isoformat(),
            "pass_ok": True,
            "pytest_target": "tests/ops/test_failure_injection.py",
            "critical_test_ids": [
                "tests/ops/test_failure_injection.py::test_failure_injection_release_gate_fails_with_stale_ws_artifact",
                "tests/ops/test_failure_injection.py::test_failure_injection_prod_rejects_fallback_metadata_snapshot",
                "tests/ops/test_failure_injection.py::test_failure_injection_release_gate_fails_with_manifest_mismatch",
            ],
            "command": ["python", "-m", "pytest"],
            "duration_seconds": 1.0,
            "returncode": 0,
        },
    )
    _write_json(
        live_drill_path,
        {
            "generated_at": NOW.isoformat(),
            "drill_executed": True,
            "promote_ready": True,
            "rollback_ready": True,
            "overall_status": "PASS",
        },
    )
    _write_json(
        operational_evidence_path,
        {
            "generated_at": NOW.isoformat(),
            "target": "live",
            "phase": "final",
            "stream_types": ["trade"],
            "cadence_policy": {
                "runtime_artifact_max_age_seconds": 86400,
                "benchmark_and_replay_max_age_seconds": 604800,
                "runtime_artifact_expected_interval_seconds": 21600,
                "live_drill_expected_interval_seconds": 43200,
                "required_artifacts": [
                    "replay_parity",
                    "storage_benchmark",
                    "vendor_contracts",
                    "ws_canary",
                    "soak",
                    "failure_injection",
                    "live_drill",
                ],
            },
            "evidence_origin": "operational_runtime",
            "provenance": {
                "source": "readiness_orchestrator",
                "runner_id": "tests:live:trade:final",
                "trigger": "tests_live_final",
                "generated_by": "tests/ops/test_release_gates.py",
                "verification_scope": "external_operational_surfaces",
                "derived_in_process": False,
            },
            "excluded_feed_policy": {"book": "excluded"},
            "observability": {
                "pass_ok": True,
                "repo_runbooks": [
                    "docs/operations/ingestion_runbook.md",
                    "docs/operations/ingestion_promotion_runbook.md",
                    "docs/ops/live_cutover.md",
                ],
                "external_surfaces": [
                    {
                        "surface_id": surface.surface_id,
                        "kind": surface.kind,
                        "description": surface.description,
                        "repo_reference": surface.repo_reference,
                        "owner": surface.owner,
                        "surface_ref": surface.surface_ref,
                        "verification_mode": surface.verification_mode,
                        "verified_at": NOW.isoformat(),
                        "verification_ref": f"artifact://tests/{surface.surface_id}",
                        "pass_ok": True,
                    }
                    for surface in build_observability_contract_report(target="live").external_surfaces
                ],
            },
            "artifacts": [
                {"name": "replay_parity", "required": True, "pass_ok": True, "fresh": True},
                {"name": "storage_benchmark", "required": True, "pass_ok": True, "fresh": True},
                {"name": "vendor_contracts", "required": True, "pass_ok": True, "fresh": True},
                {"name": "ws_canary", "required": True, "pass_ok": True, "fresh": True},
                {"name": "soak", "required": True, "pass_ok": True, "fresh": True},
                {"name": "failure_injection", "required": True, "pass_ok": True, "fresh": True},
                {"name": "live_drill", "required": True, "pass_ok": True, "fresh": True},
            ],
            "pass_ok": True,
            "reasons": ["operational evidence fresh and aligned"],
        },
    )
    _write_shadow_comparison(tmp_path / "shadow" / "env=dev" / "comparisons.jsonl", significant=False)

    report = run_release_gates(
        base_dir=tmp_path,
        env="dev",
        target="live",
        stream_types=("trade",),
        rest_canary_path=tmp_path / "missing-rest.json",
        ws_canary_path=ws_path,
        replay_parity_path=parity_path,
        benchmark_path=benchmark_path,
        soak_path=soak_path,
        network_contracts_path=vendor_contracts_path,
        failure_injection_path=failure_injection_path,
        live_drill_path=live_drill_path,
        operational_evidence_path=operational_evidence_path,
    )

    assert report.pass_ok is True
    blocks = {block.name: block for block in report.blocks}
    assert blocks["evidence_contract"].status == "pass"
    assert blocks["canary_rest"].required is False
    assert blocks["canary_ws"].required is True
    assert blocks["support_matrix"].details["feeds"]["trade"]["supports_live"] is True


def test_release_gates_fail_when_vendor_contract_artifact_is_incomplete(tmp_path: Path):
    rest_path, ws_path, benchmark_path, parity_path, soak_path, vendor_contracts_path, failure_injection_path, live_drill_path, operational_evidence_path = _write_release_artifacts(tmp_path)
    _write_metadata_snapshot(tmp_path, env="dev", mode="runtime")
    _write_json(
        vendor_contracts_path,
        {
            "generated_at": NOW.isoformat(),
            "pass_ok": True,
            "command": [],
            "returncode": 0,
        },
    )

    report = run_release_gates(
        base_dir=tmp_path,
        env="dev",
        target="paper",
        stream_types=("kline",),
        rest_canary_path=rest_path,
        ws_canary_path=ws_path,
        replay_parity_path=parity_path,
        benchmark_path=benchmark_path,
        soak_path=soak_path,
        network_contracts_path=vendor_contracts_path,
        failure_injection_path=failure_injection_path,
        live_drill_path=live_drill_path,
        operational_evidence_path=operational_evidence_path,
    )

    block = next(block for block in report.blocks if block.name == "vendor_contracts")
    assert block.status == "fail"
    assert any("pytest_target" in reason or "command" in reason or "duration_seconds" in reason for reason in block.reasons)


def test_release_gates_fail_when_benchmark_target_profile_or_high_cardinality_cases_do_not_match(tmp_path: Path):
    rest_path, ws_path, benchmark_path, parity_path, soak_path, vendor_contracts_path, failure_injection_path, live_drill_path, operational_evidence_path = _write_release_artifacts(tmp_path)
    _rewrite_runtime_artifact_target(ws_path, target_profile="live")
    _rewrite_runtime_artifact_target(soak_path, target_profile="live")
    _write_metadata_snapshot(tmp_path, env="dev", mode="runtime")
    benchmark_payload = json.loads(benchmark_path.read_text(encoding="utf-8"))
    benchmark_payload["target_profile"] = "paper"
    benchmark_payload["required_high_cardinality_symbol_counts"] = [100, 500]
    benchmark_payload["high_cardinality_cases"] = [
        {"name": "high_cardinality_100", "pass_ok": True, "rows_per_second": 10.0, "requested_symbol_count": 100}
    ]
    _write_json(benchmark_path, benchmark_payload)

    report = run_release_gates(
        base_dir=tmp_path,
        env="dev",
        target="live",
        stream_types=("kline",),
        rest_canary_path=rest_path,
        ws_canary_path=ws_path,
        replay_parity_path=parity_path,
        benchmark_path=benchmark_path,
        soak_path=soak_path,
        network_contracts_path=vendor_contracts_path,
        failure_injection_path=failure_injection_path,
        live_drill_path=live_drill_path,
        operational_evidence_path=operational_evidence_path,
    )

    block = next(block for block in report.blocks if block.name == "storage_benchmark")
    assert block.status == "fail"
    assert any("target_profile" in reason or "high-cardinality" in reason for reason in block.reasons)


def test_release_gates_fail_when_soak_records_compaction_failures(tmp_path: Path):
    rest_path, ws_path, benchmark_path, parity_path, soak_path, vendor_contracts_path, failure_injection_path, live_drill_path, operational_evidence_path = _write_release_artifacts(tmp_path)
    _write_metadata_snapshot(tmp_path, env="dev", mode="runtime")
    _write_json(
        soak_path,
        {
            "generated_at": NOW.isoformat(),
            "pass_ok": True,
            "max_allowed_gaps": 0,
            "max_gaps": 0,
            "max_allowed_gap_irreparable": 0,
            "max_gap_irreparable": 0,
            "max_allowed_compaction_failures": 0,
            "compaction_failures_total": 1,
            "reconnects_observed": 1,
            "reconnects_target": 1,
        },
    )

    report = run_release_gates(
        base_dir=tmp_path,
        env="dev",
        target="paper",
        stream_types=("kline",),
        rest_canary_path=rest_path,
        ws_canary_path=ws_path,
        replay_parity_path=parity_path,
        benchmark_path=benchmark_path,
        soak_path=soak_path,
        network_contracts_path=vendor_contracts_path,
        failure_injection_path=failure_injection_path,
        live_drill_path=live_drill_path,
        operational_evidence_path=operational_evidence_path,
    )

    block = next(block for block in report.blocks if block.name == "paper_soak")
    assert block.status == "fail"
    assert any("compaction failures exceed soak threshold" in reason for reason in block.reasons)


def test_release_gates_fail_when_ws_canary_records_degraded_runtime(tmp_path: Path):
    rest_path, ws_path, benchmark_path, parity_path, soak_path, vendor_contracts_path, failure_injection_path, live_drill_path, operational_evidence_path = _write_release_artifacts(tmp_path)
    _write_metadata_snapshot(tmp_path, env="dev", mode="runtime")
    ws_payload = json.loads(ws_path.read_text(encoding="utf-8"))
    ws_payload["continuity"]["gaps"] = 1
    ws_payload["continuity"]["streams_degraded"] = ["BINANCE:BTCUSDT:kline"]
    _write_json(ws_path, ws_payload)

    report = run_release_gates(
        base_dir=tmp_path,
        env="dev",
        target="paper",
        stream_types=("kline",),
        rest_canary_path=rest_path,
        ws_canary_path=ws_path,
        replay_parity_path=parity_path,
        benchmark_path=benchmark_path,
        soak_path=soak_path,
        network_contracts_path=vendor_contracts_path,
        failure_injection_path=failure_injection_path,
        live_drill_path=live_drill_path,
        operational_evidence_path=operational_evidence_path,
    )

    block = next(block for block in report.blocks if block.name == "canary_ws")
    assert block.status == "fail"
    assert any("promotion threshold" in reason or "streams degraded" in reason for reason in block.reasons)


def test_release_gates_fail_when_soak_records_duplicate_runtime(tmp_path: Path):
    rest_path, ws_path, benchmark_path, parity_path, soak_path, vendor_contracts_path, failure_injection_path, live_drill_path, operational_evidence_path = _write_release_artifacts(tmp_path)
    _write_metadata_snapshot(tmp_path, env="dev", mode="runtime")
    soak_payload = json.loads(soak_path.read_text(encoding="utf-8"))
    soak_payload["max_duplicates"] = 1
    _write_json(soak_path, soak_payload)

    report = run_release_gates(
        base_dir=tmp_path,
        env="dev",
        target="paper",
        stream_types=("kline",),
        rest_canary_path=rest_path,
        ws_canary_path=ws_path,
        replay_parity_path=parity_path,
        benchmark_path=benchmark_path,
        soak_path=soak_path,
        network_contracts_path=vendor_contracts_path,
        failure_injection_path=failure_injection_path,
        live_drill_path=live_drill_path,
        operational_evidence_path=operational_evidence_path,
    )

    block = next(block for block in report.blocks if block.name == "paper_soak")
    assert block.status == "fail"
    assert any("duplicates exceed soak threshold" in reason for reason in block.reasons)


def test_release_gates_live_fail_when_failure_injection_artifact_is_missing(tmp_path: Path):
    rest_path, ws_path, benchmark_path, parity_path, soak_path, vendor_contracts_path, _failure_injection_path, live_drill_path, operational_evidence_path = _write_release_artifacts(tmp_path)
    _rewrite_runtime_artifact_target(ws_path, target_profile="live")
    _rewrite_runtime_artifact_target(soak_path, target_profile="live")
    benchmark_payload = json.loads(benchmark_path.read_text(encoding="utf-8"))
    benchmark_payload["target_profile"] = "live"
    benchmark_payload["required_high_cardinality_symbol_counts"] = [100, 500]
    benchmark_payload["high_cardinality_cases"] = [
        {"name": "high_cardinality_100", "pass_ok": True, "rows_per_second": 10.0, "requested_symbol_count": 100},
        {"name": "high_cardinality_500", "pass_ok": True, "rows_per_second": 10.0, "requested_symbol_count": 500},
    ]
    _write_json(benchmark_path, benchmark_payload)
    _write_metadata_snapshot(tmp_path, env="dev", mode="runtime")
    _write_shadow_comparison(tmp_path / "shadow" / "env=dev" / "comparisons.jsonl", significant=False)
    missing_failure_injection_path = tmp_path / "missing-failure-injection.json"

    report = run_release_gates(
        base_dir=tmp_path,
        env="dev",
        target="live",
        stream_types=("kline",),
        rest_canary_path=rest_path,
        ws_canary_path=ws_path,
        replay_parity_path=parity_path,
        benchmark_path=benchmark_path,
        soak_path=soak_path,
        network_contracts_path=vendor_contracts_path,
        failure_injection_path=missing_failure_injection_path,
        live_drill_path=live_drill_path,
        operational_evidence_path=operational_evidence_path,
    )

    block = next(block for block in report.blocks if block.name == "failure_injection")
    assert block.status == "fail"
    assert any("missing artifact" in reason for reason in block.reasons)


def test_release_gates_live_fail_when_live_drill_artifact_is_stale(tmp_path: Path):
    rest_path, ws_path, benchmark_path, parity_path, soak_path, vendor_contracts_path, failure_injection_path, live_drill_path, operational_evidence_path = _write_release_artifacts(tmp_path)
    _rewrite_runtime_artifact_target(ws_path, target_profile="live")
    _rewrite_runtime_artifact_target(soak_path, target_profile="live")
    _write_metadata_snapshot(tmp_path, env="dev", mode="runtime")
    _write_shadow_comparison(tmp_path / "shadow" / "env=dev" / "comparisons.jsonl", significant=False)
    _write_json(
        live_drill_path,
        {
            "generated_at": (NOW - timedelta(days=2)).isoformat(),
            "drill_executed": True,
            "promote_ready": True,
            "rollback_ready": True,
            "overall_status": "PASS",
        },
    )

    report = run_release_gates(
        base_dir=tmp_path,
        env="dev",
        target="live",
        stream_types=("kline",),
        rest_canary_path=rest_path,
        ws_canary_path=ws_path,
        replay_parity_path=parity_path,
        benchmark_path=benchmark_path,
        soak_path=soak_path,
        network_contracts_path=vendor_contracts_path,
        failure_injection_path=failure_injection_path,
        live_drill_path=live_drill_path,
        operational_evidence_path=operational_evidence_path,
    )

    block = next(block for block in report.blocks if block.name == "live_drill")
    assert block.status == "fail"
    assert any("artifact stale" in reason for reason in block.reasons)


def test_release_gates_live_fail_when_live_drill_not_pass(tmp_path: Path):
    rest_path, ws_path, benchmark_path, parity_path, soak_path, vendor_contracts_path, failure_injection_path, live_drill_path, operational_evidence_path = _write_release_artifacts(tmp_path)
    _rewrite_runtime_artifact_target(ws_path, target_profile="live")
    _rewrite_runtime_artifact_target(soak_path, target_profile="live")
    _write_metadata_snapshot(tmp_path, env="dev", mode="runtime")
    _write_shadow_comparison(tmp_path / "shadow" / "env=dev" / "comparisons.jsonl", significant=False)
    _write_json(
        live_drill_path,
        {
            "generated_at": NOW.isoformat(),
            "drill_executed": True,
            "promote_ready": False,
            "rollback_ready": True,
            "overall_status": "FAIL",
        },
    )

    report = run_release_gates(
        base_dir=tmp_path,
        env="dev",
        target="live",
        stream_types=("kline",),
        rest_canary_path=rest_path,
        ws_canary_path=ws_path,
        replay_parity_path=parity_path,
        benchmark_path=benchmark_path,
        soak_path=soak_path,
        network_contracts_path=vendor_contracts_path,
        failure_injection_path=failure_injection_path,
        live_drill_path=live_drill_path,
        operational_evidence_path=operational_evidence_path,
    )

    block = next(block for block in report.blocks if block.name == "live_drill")
    assert block.status == "fail"
    assert any("overall_status is not PASS" in reason for reason in block.reasons)


def test_release_gates_fail_when_observability_contract_is_incomplete(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    rest_path, ws_path, benchmark_path, parity_path, soak_path, vendor_contracts_path, failure_injection_path, live_drill_path, operational_evidence_path = _write_release_artifacts(tmp_path)
    _write_metadata_snapshot(tmp_path, env="dev", mode="runtime")

    fake_report = mock.Mock(
        target="paper",
        required_metrics=("exchange_receive_skew_seconds",),
        required_alerts=("invalid_timestamp_detected",),
        required_metric_thresholds={},
        alert_specs={},
        external_surfaces=(),
        missing_alerts=(),
        missing_metric_thresholds=("exchange_receive_skew_seconds",),
        invalid_alert_specs=(),
        pass_ok=False,
    )
    monkeypatch.setattr(release_gates, "build_observability_contract_report", lambda target="paper": fake_report)

    report = release_gates.run_release_gates(
        base_dir=tmp_path,
        env="dev",
        target="paper",
        stream_types=("kline",),
        rest_canary_path=rest_path,
        ws_canary_path=ws_path,
        replay_parity_path=parity_path,
        benchmark_path=benchmark_path,
        soak_path=soak_path,
        network_contracts_path=vendor_contracts_path,
        failure_injection_path=failure_injection_path,
        live_drill_path=live_drill_path,
        operational_evidence_path=operational_evidence_path,
    )

    block = next(block for block in report.blocks if block.name == "observability_contract")
    assert block.status == "fail"
    assert any("metric thresholds" in reason for reason in block.reasons)
