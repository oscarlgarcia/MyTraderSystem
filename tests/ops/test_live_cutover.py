from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
import subprocess
import sys
from pathlib import Path

from app.ops.live_cutover import render_live_cutover_summary, run_live_cutover_drill


def _write_json(path: Path, payload: dict[str, object]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")


def test_live_cutover_drill_writes_evidence_and_completed_checklist(tmp_path: Path):
    output_path = tmp_path / "live-drill.json"
    release_gate_path = tmp_path / "release-gates.json"
    rest_canary_path = tmp_path / "rest.json"
    ws_canary_path = tmp_path / "ws.json"
    benchmark_path = tmp_path / "benchmark.json"
    failure_injection_path = tmp_path / "failure-injection.json"
    rollback_path = tmp_path / "rollback.md"
    live_cutover_doc_path = tmp_path / "live_cutover.md"
    promotion_runbook_path = tmp_path / "promotion_runbook.md"

    now = datetime.now(timezone.utc).isoformat()
    _write_json(release_gate_path, {"generated_at": now, "target": "live", "overall_status": "PASS", "pass_ok": True})
    _write_json(rest_canary_path, {"generated_at": now, "pass_ok": True, "comparison_reason": "semantic_match"})
    _write_json(ws_canary_path, {"report_generated_at": now, "pass_ok": True, "comparison_reason": "continuity_ok"})
    _write_json(benchmark_path, {"generated_at": now, "pass_ok": True, "slo": {"min_rows_per_second": 1.0}})
    _write_json(
        failure_injection_path,
        {
            "generated_at": now,
            "pass_ok": True,
            "critical_test_ids": [
                "tests/ops/test_failure_injection.py::test_failure_injection_release_gate_fails_with_stale_ws_artifact",
                "tests/ops/test_failure_injection.py::test_failure_injection_prod_rejects_fallback_metadata_snapshot",
                "tests/ops/test_failure_injection.py::test_failure_injection_release_gate_fails_with_manifest_mismatch",
            ],
        },
    )
    rollback_path.write_text("# rollback\n", encoding="utf-8")
    live_cutover_doc_path.write_text("# cutover\n", encoding="utf-8")
    promotion_runbook_path.write_text("# promotion\n", encoding="utf-8")

    report = run_live_cutover_drill(
        base_dir=tmp_path,
        env="dev",
        output_path=output_path,
        release_gate_path=release_gate_path,
        rest_canary_path=rest_canary_path,
        ws_canary_path=ws_canary_path,
        benchmark_path=benchmark_path,
        failure_injection_path=failure_injection_path,
        rollback_checklist_path=rollback_path,
        live_cutover_doc_path=live_cutover_doc_path,
        promotion_runbook_path=promotion_runbook_path,
    )

    assert report.drill_executed is True
    assert report.checklist_completed is True
    assert report.promote_ready is True
    assert report.rollback_ready is True
    assert report.overall_status == "PASS"
    assert report.checklist_completed is True
    assert report.artifact_ttl_seconds == 86400
    assert output_path.exists()
    assert "Live cutover drill: PASS" in render_live_cutover_summary(report)


def test_live_cutover_drill_fails_when_required_artifact_is_red(tmp_path: Path):
    release_gate_path = tmp_path / "release-gates.json"
    rest_canary_path = tmp_path / "rest.json"
    ws_canary_path = tmp_path / "ws.json"
    benchmark_path = tmp_path / "benchmark.json"
    failure_injection_path = tmp_path / "failure-injection.json"
    rollback_path = tmp_path / "rollback.md"
    live_cutover_doc_path = tmp_path / "live_cutover.md"
    promotion_runbook_path = tmp_path / "promotion_runbook.md"

    now = datetime.now(timezone.utc).isoformat()
    _write_json(release_gate_path, {"generated_at": now, "target": "live", "overall_status": "FAIL", "pass_ok": False})
    _write_json(rest_canary_path, {"generated_at": now, "pass_ok": True, "comparison_reason": "semantic_match"})
    _write_json(ws_canary_path, {"report_generated_at": now, "pass_ok": False, "comparison_reason": "continuity_failed"})
    _write_json(benchmark_path, {"generated_at": now, "pass_ok": True, "slo": {"min_rows_per_second": 1.0}})
    _write_json(failure_injection_path, {"generated_at": now, "pass_ok": True, "critical_test_ids": ["a", "b", "c"]})
    rollback_path.write_text("# rollback\n", encoding="utf-8")
    live_cutover_doc_path.write_text("# cutover\n", encoding="utf-8")
    promotion_runbook_path.write_text("# promotion\n", encoding="utf-8")

    report = run_live_cutover_drill(
        base_dir=tmp_path,
        env="dev",
        release_gate_path=release_gate_path,
        rest_canary_path=rest_canary_path,
        ws_canary_path=ws_canary_path,
        benchmark_path=benchmark_path,
        failure_injection_path=failure_injection_path,
        rollback_checklist_path=rollback_path,
        live_cutover_doc_path=live_cutover_doc_path,
        promotion_runbook_path=promotion_runbook_path,
    )

    assert report.drill_executed is True
    assert report.checklist_completed is False
    assert report.promote_ready is False
    assert report.rollback_ready is True
    assert report.overall_status == "FAIL"


def test_live_cutover_drill_fails_when_failure_injection_or_release_gates_are_stale(tmp_path: Path):
    release_gate_path = tmp_path / "release-gates.json"
    rest_canary_path = tmp_path / "rest.json"
    ws_canary_path = tmp_path / "ws.json"
    benchmark_path = tmp_path / "benchmark.json"
    failure_injection_path = tmp_path / "failure-injection.json"
    rollback_path = tmp_path / "rollback.md"
    live_cutover_doc_path = tmp_path / "live_cutover.md"
    promotion_runbook_path = tmp_path / "promotion_runbook.md"

    stale = (datetime.now(timezone.utc) - timedelta(days=2)).isoformat()
    now = datetime.now(timezone.utc).isoformat()
    _write_json(release_gate_path, {"generated_at": stale, "target": "live", "overall_status": "PASS", "pass_ok": True})
    _write_json(rest_canary_path, {"generated_at": now, "pass_ok": True, "comparison_reason": "semantic_match"})
    _write_json(ws_canary_path, {"report_generated_at": now, "pass_ok": True, "comparison_reason": "continuity_ok"})
    _write_json(benchmark_path, {"generated_at": now, "pass_ok": True, "slo": {"min_rows_per_second": 1.0}})
    _write_json(failure_injection_path, {"generated_at": stale, "pass_ok": True, "critical_test_ids": ["a", "b", "c"]})
    rollback_path.write_text("# rollback\n", encoding="utf-8")
    live_cutover_doc_path.write_text("# cutover\n", encoding="utf-8")
    promotion_runbook_path.write_text("# promotion\n", encoding="utf-8")

    report = run_live_cutover_drill(
        base_dir=tmp_path,
        env="dev",
        release_gate_path=release_gate_path,
        rest_canary_path=rest_canary_path,
        ws_canary_path=ws_canary_path,
        benchmark_path=benchmark_path,
        failure_injection_path=failure_injection_path,
        rollback_checklist_path=rollback_path,
        live_cutover_doc_path=live_cutover_doc_path,
        promotion_runbook_path=promotion_runbook_path,
    )

    assert report.overall_status == "FAIL"
    names = {item.name: item for item in report.checklist}
    assert names["release_gate_pass"].passed is False
    assert names["failure_injection_pass"].passed is False


def test_live_cutover_script_help_runs():
    result = subprocess.run(
        [sys.executable, "scripts/ingestion_live_drill.py", "--help"],
        capture_output=True,
        text=True,
        check=False,
    )

    assert result.returncode == 0
    assert "--release-gates-path" in result.stdout
    assert "--failure-injection-path" in result.stdout
    assert "--live-cutover-doc-path" in result.stdout
    assert "--promotion-runbook-path" in result.stdout
