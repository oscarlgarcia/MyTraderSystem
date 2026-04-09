from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from pathlib import Path

from app.ops.operational_governance import build_operational_governance_report, write_operational_governance_report


def test_operational_governance_bootstrap_passes_for_pipeline_runner(tmp_path: Path):
    output_dir = tmp_path / "docs" / "validation"
    report = build_operational_governance_report(
        target="paper",
        output_dir=output_dir,
        runner_id="ingestion-paper-runner",
        trigger="scheduled_paper_cycle",
        provenance_source="ingestion_operational_cycle",
        execution_ref="paper-exec-001",
        channel="pipeline",
        schedule_name="ingestion-paper-cadence",
        job_id="paper-job-001",
        job_url="https://ops.example/paper-job-001",
    )

    assert report.pass_ok is True
    assert report.cadence_state == "bootstrap"
    path = output_dir / "ingestion_operational_governance_paper.json"
    write_operational_governance_report(path, report)
    payload = json.loads(path.read_text(encoding="utf-8"))
    assert payload["schedule_name"] == "ingestion-paper-cadence"
    assert payload["job_id"] == "paper-job-001"


def test_operational_governance_marks_manual_runs_as_non_promotable(tmp_path: Path):
    report = build_operational_governance_report(
        target="live",
        output_dir=tmp_path,
        runner_id="ingestion-live-runner",
        trigger="manual_debug",
        provenance_source="ingestion_operational_cycle",
        execution_ref="live-exec-manual",
        channel="manual",
        schedule_name="manual-live",
        job_id="manual-job",
        job_url="https://ops.example/manual-live",
    )

    assert report.pass_ok is False
    assert report.cadence_state == "manual"
    assert any("manual channel" in reason for reason in report.reasons)


def test_operational_governance_marks_stale_history_as_failing(tmp_path: Path):
    output_dir = tmp_path / "docs" / "validation"
    history_path = output_dir / "ingestion_operational_history_live.jsonl"
    output_dir.mkdir(parents=True, exist_ok=True)
    previous = {
        "generated_at": (datetime.now(timezone.utc) - timedelta(hours=12)).isoformat(),
        "target": "live",
        "runner_id": "ingestion-live-runner",
        "execution_ref": "prev-live-exec",
        "channel": "scheduled",
        "schedule_name": "ingestion-live-cadence",
        "job_id": "live-job-prev",
        "job_url": "https://ops.example/live-job-prev",
        "owner": "team-ingestion-oncall",
        "cadence_state": "healthy",
        "pass_ok": True,
        "overall_status": "PASS",
    }
    history_path.write_text(json.dumps(previous) + "\n", encoding="utf-8")

    report = build_operational_governance_report(
        target="live",
        output_dir=output_dir,
        runner_id="ingestion-live-runner",
        trigger="scheduled_live_cycle",
        provenance_source="ingestion_operational_cycle",
        execution_ref="live-exec-002",
        channel="scheduled",
        schedule_name="ingestion-live-cadence",
        job_id="live-job-002",
        job_url="https://ops.example/live-job-002",
        history_path=history_path,
    )

    assert report.pass_ok is False
    assert report.cadence_state == "stale"
    assert report.previous_execution_ref == "prev-live-exec"
