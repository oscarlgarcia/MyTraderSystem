from __future__ import annotations

import json
from pathlib import Path

import pytest

from app.ops.runner_context import (
    load_runner_context_from_env,
    load_runner_context_from_file,
    resolve_runner_context,
)


def test_load_runner_context_from_file(tmp_path: Path):
    path = tmp_path / "runner-context.json"
    path.write_text(
        json.dumps(
            {
                "execution_ref": "exec-file-001",
                "channel": "scheduled",
                "schedule_name": "ingestion-paper-cadence",
                "job_id": "job-file-001",
                "job_url": "https://ops.example/job-file-001",
                "owner": "team-ingestion",
            }
        ),
        encoding="utf-8",
    )

    context = load_runner_context_from_file(path)

    assert context.execution_ref == "exec-file-001"
    assert context.channel == "scheduled"
    assert context.schedule_name == "ingestion-paper-cadence"
    assert context.source == f"file:{path}"


def test_load_runner_context_from_env(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setenv("INGESTION_RUNNER_EXECUTION_REF", "exec-env-001")
    monkeypatch.setenv("INGESTION_RUNNER_CHANNEL", "pipeline")
    monkeypatch.setenv("INGESTION_RUNNER_SCHEDULE_NAME", "ingestion-live-cadence")
    monkeypatch.setenv("INGESTION_RUNNER_JOB_ID", "job-env-001")
    monkeypatch.setenv("INGESTION_RUNNER_JOB_URL", "https://ops.example/job-env-001")
    monkeypatch.setenv("INGESTION_RUNNER_OWNER", "team-ingestion-oncall")

    context = load_runner_context_from_env()

    assert context.execution_ref == "exec-env-001"
    assert context.channel == "pipeline"
    assert context.job_id == "job-env-001"
    assert context.source == "env:INGESTION_RUNNER_"


def test_resolve_runner_context_prioritizes_cli_over_file_and_env(tmp_path: Path, monkeypatch: pytest.MonkeyPatch):
    path = tmp_path / "runner-context.json"
    path.write_text(
        json.dumps(
            {
                "execution_ref": "exec-file-001",
                "channel": "scheduled",
                "schedule_name": "schedule-file",
                "job_id": "job-file-001",
                "job_url": "https://ops.example/job-file-001",
                "owner": "owner-file",
            }
        ),
        encoding="utf-8",
    )
    monkeypatch.setenv("INGESTION_RUNNER_EXECUTION_REF", "exec-env-001")
    monkeypatch.setenv("INGESTION_RUNNER_CHANNEL", "pipeline")
    monkeypatch.setenv("INGESTION_RUNNER_SCHEDULE_NAME", "schedule-env")
    monkeypatch.setenv("INGESTION_RUNNER_JOB_ID", "job-env-001")
    monkeypatch.setenv("INGESTION_RUNNER_JOB_URL", "https://ops.example/job-env-001")
    monkeypatch.setenv("INGESTION_RUNNER_OWNER", "owner-env")

    context = resolve_runner_context(
        execution_ref="exec-cli-001",
        channel="scheduled",
        schedule_name="schedule-cli",
        job_id="job-cli-001",
        job_url="https://ops.example/job-cli-001",
        owner="owner-cli",
        runner_context_path=path,
        runner_context_from_env=True,
    )

    assert context.execution_ref == "exec-cli-001"
    assert context.schedule_name == "schedule-cli"
    assert context.job_id == "job-cli-001"
    assert context.owner == "owner-cli"
    assert context.source == f"file:{path}"


def test_resolve_runner_context_raises_when_required_fields_are_missing(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.delenv("INGESTION_RUNNER_EXECUTION_REF", raising=False)

    with pytest.raises(ValueError, match="runner context missing required fields"):
        resolve_runner_context(runner_context_from_env=True)
