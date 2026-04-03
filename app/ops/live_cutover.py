from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Literal


ARTIFACT_TTL = timedelta(hours=24)


@dataclass(frozen=True, slots=True)
class CutoverChecklistItem:
    name: str
    required: bool
    passed: bool
    details: dict[str, object]


@dataclass(frozen=True, slots=True)
class CriticalAlertResponse:
    alert_type: str
    ack_deadline_minutes: int
    rollback_decision_deadline_minutes: int
    recommended_action: str


@dataclass(frozen=True, slots=True)
class LiveCutoverDrillReport:
    generated_at: str
    env: str
    target: Literal["live"]
    overall_status: Literal["PASS", "FAIL"]
    drill_executed: bool
    checklist_completed: bool
    promote_ready: bool
    rollback_ready: bool
    decision_deadlines: dict[str, int]
    artifact_ttl_seconds: int
    checklist: tuple[CutoverChecklistItem, ...]
    critical_alert_responses: tuple[CriticalAlertResponse, ...]


def write_live_cutover_report(path: Path, report: LiveCutoverDrillReport) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as handle:
        json.dump(asdict(report), handle, ensure_ascii=False, indent=2, default=str)
        handle.write("\n")
    return path


def render_live_cutover_summary(report: LiveCutoverDrillReport) -> str:
    lines = [f"Live cutover drill: {report.overall_status}"]
    for item in report.checklist:
        lines.append(
            f"- {item.name}: {'PASS' if item.passed else 'FAIL'} "
            f"[{'required' if item.required else 'informational'}]"
        )
    return "\n".join(lines)


def run_live_cutover_drill(
    *,
    base_dir: Path,
    env: str,
    output_path: Path | None = None,
    release_gate_path: Path | None = None,
    rest_canary_path: Path | None = None,
    ws_canary_path: Path | None = None,
    benchmark_path: Path | None = None,
    failure_injection_path: Path | None = None,
    rollback_checklist_path: Path | None = None,
    live_cutover_doc_path: Path | None = None,
    promotion_runbook_path: Path | None = None,
) -> LiveCutoverDrillReport:
    release_gate_path = Path(release_gate_path or "docs/validation/ingestion_release_gates.json")
    rest_canary_path = Path(rest_canary_path or "docs/validation/ingestion_canary_report.json")
    ws_canary_path = Path(ws_canary_path or "docs/validation/ingestion_ws_canary_report.json")
    benchmark_path = Path(benchmark_path or "docs/validation/ingestion_storage_benchmark.json")
    failure_injection_path = Path(failure_injection_path or "docs/validation/ingestion_failure_injection.json")
    rollback_checklist_path = Path(
        rollback_checklist_path or "docs/operations/ingestion_rollback_checklist.md"
    )
    live_cutover_doc_path = Path(live_cutover_doc_path or "docs/ops/live_cutover.md")
    promotion_runbook_path = Path(
        promotion_runbook_path or "docs/operations/ingestion_promotion_runbook.md"
    )

    deadlines = {
        "promote_decision_max_minutes": 15,
        "critical_alert_ack_max_minutes": 2,
        "rollback_decision_max_minutes": 5,
    }

    release_gate_payload = _load_json_if_exists(release_gate_path)
    rest_canary_payload = _load_json_if_exists(rest_canary_path)
    ws_canary_payload = _load_json_if_exists(ws_canary_path)
    benchmark_payload = _load_json_if_exists(benchmark_path)
    failure_injection_payload = _load_json_if_exists(failure_injection_path)

    checklist = (
        _check_release_gate(release_gate_path, release_gate_payload),
        _check_canary("canary_rest", rest_canary_path, rest_canary_payload),
        _check_canary("canary_ws", ws_canary_path, ws_canary_payload),
        _check_benchmark(benchmark_path, benchmark_payload),
        _check_failure_injection(failure_injection_path, failure_injection_payload),
        CutoverChecklistItem(
            name="rollback_checklist_present",
            required=True,
            passed=rollback_checklist_path.exists(),
            details={"path": str(rollback_checklist_path)},
        ),
        CutoverChecklistItem(
            name="live_cutover_runbook_present",
            required=True,
            passed=live_cutover_doc_path.exists(),
            details={"path": str(live_cutover_doc_path)},
        ),
        CutoverChecklistItem(
            name="promotion_runbook_present",
            required=True,
            passed=promotion_runbook_path.exists(),
            details={"path": str(promotion_runbook_path)},
        ),
        CutoverChecklistItem(
            name="decision_deadlines_defined",
            required=True,
            passed=all(value > 0 for value in deadlines.values()),
            details=deadlines,
        ),
    )

    promote_ready = all(item.passed for item in checklist if item.required)
    rollback_ready = (
        rollback_checklist_path.exists()
        and live_cutover_doc_path.exists()
        and promotion_runbook_path.exists()
        and deadlines["rollback_decision_max_minutes"] > 0
    )
    report = LiveCutoverDrillReport(
        generated_at=datetime.now(timezone.utc).isoformat(),
        env=env,
        target="live",
        overall_status="PASS" if promote_ready and rollback_ready else "FAIL",
        drill_executed=True,
        checklist_completed=all(item.passed for item in checklist if item.required),
        promote_ready=promote_ready,
        rollback_ready=rollback_ready,
        decision_deadlines=deadlines,
        artifact_ttl_seconds=int(ARTIFACT_TTL.total_seconds()),
        checklist=checklist,
        critical_alert_responses=_critical_alert_responses(),
    )
    if output_path is not None:
        write_live_cutover_report(Path(output_path), report)
    return report


def _load_json_if_exists(path: Path) -> dict[str, object] | None:
    if not path.exists():
        return None
    return json.loads(path.read_text(encoding="utf-8-sig"))


def _artifact_age_seconds(path: Path, payload: dict[str, object] | None) -> float | None:
    if payload is None:
        return None
    for key in ("generated_at", "report_generated_at", "fetched_at"):
        value = payload.get(key)
        if value in (None, ""):
            continue
        try:
            ts = datetime.fromisoformat(str(value))
            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=timezone.utc)
            return max(0.0, (datetime.now(timezone.utc) - ts.astimezone(timezone.utc)).total_seconds())
        except ValueError:
            continue
    return max(0.0, (datetime.now(timezone.utc) - datetime.fromtimestamp(path.stat().st_mtime, tz=timezone.utc)).total_seconds())


def _check_release_gate(path: Path, payload: dict[str, object] | None) -> CutoverChecklistItem:
    age_seconds = _artifact_age_seconds(path, payload)
    passed = bool(
        payload
        and payload.get("pass_ok") is True
        and payload.get("overall_status") == "PASS"
        and payload.get("target") == "live"
        and age_seconds is not None
        and age_seconds <= ARTIFACT_TTL.total_seconds()
    )
    return CutoverChecklistItem(
        name="release_gate_pass",
        required=True,
        passed=passed,
        details={
            "path": str(path),
            "overall_status": payload.get("overall_status") if payload else None,
            "pass_ok": payload.get("pass_ok") if payload else None,
            "target": payload.get("target") if payload else None,
            "artifact_age_seconds": age_seconds,
        },
    )


def _check_canary(name: str, path: Path, payload: dict[str, object] | None) -> CutoverChecklistItem:
    age_seconds = _artifact_age_seconds(path, payload)
    passed = bool(
        payload
        and payload.get("pass_ok") is True
        and age_seconds is not None
        and age_seconds <= ARTIFACT_TTL.total_seconds()
    )
    return CutoverChecklistItem(
        name=name,
        required=True,
        passed=passed,
        details={
            "path": str(path),
            "pass_ok": payload.get("pass_ok") if payload else None,
            "comparison_reason": payload.get("comparison_reason") if payload else None,
            "artifact_age_seconds": age_seconds,
        },
    )


def _check_benchmark(path: Path, payload: dict[str, object] | None) -> CutoverChecklistItem:
    age_seconds = _artifact_age_seconds(path, payload)
    passed = bool(
        payload
        and payload.get("pass_ok") is True
        and age_seconds is not None
        and age_seconds <= ARTIFACT_TTL.total_seconds()
    )
    return CutoverChecklistItem(
        name="storage_benchmark_pass",
        required=True,
        passed=passed,
        details={
            "path": str(path),
            "pass_ok": payload.get("pass_ok") if payload else None,
            "slo": payload.get("slo") if payload else None,
            "artifact_age_seconds": age_seconds,
        },
    )


def _check_failure_injection(path: Path, payload: dict[str, object] | None) -> CutoverChecklistItem:
    age_seconds = _artifact_age_seconds(path, payload)
    critical_test_ids = payload.get("critical_test_ids") if payload else None
    passed = bool(
        payload
        and payload.get("pass_ok") is True
        and isinstance(critical_test_ids, list)
        and len(critical_test_ids) >= 3
        and age_seconds is not None
        and age_seconds <= ARTIFACT_TTL.total_seconds()
    )
    return CutoverChecklistItem(
        name="failure_injection_pass",
        required=True,
        passed=passed,
        details={
            "path": str(path),
            "pass_ok": payload.get("pass_ok") if payload else None,
            "critical_test_ids": critical_test_ids,
            "artifact_age_seconds": age_seconds,
        },
    )


def _critical_alert_responses() -> tuple[CriticalAlertResponse, ...]:
    return (
        CriticalAlertResponse(
            alert_type="reconnect_storm",
            ack_deadline_minutes=2,
            rollback_decision_deadline_minutes=5,
            recommended_action="Confirm vendor connectivity, freeze promotion, inspect websocket instability before continuing.",
        ),
        CriticalAlertResponse(
            alert_type="gap_irreparable",
            ack_deadline_minutes=2,
            rollback_decision_deadline_minutes=5,
            recommended_action="Treat stream as degraded, abort live promotion and revert to previous pipeline version.",
        ),
        CriticalAlertResponse(
            alert_type="shadow_semantic_diff",
            ack_deadline_minutes=2,
            rollback_decision_deadline_minutes=5,
            recommended_action="Stop promotion immediately, inspect shadow diffs and keep candidate behind shadow mode only.",
        ),
        CriticalAlertResponse(
            alert_type="compaction_failure_detected",
            ack_deadline_minutes=2,
            rollback_decision_deadline_minutes=5,
            recommended_action="Freeze live cutover, inspect compaction failures and clear storage health before retrying.",
        ),
        CriticalAlertResponse(
            alert_type="provider_metadata_drift",
            ack_deadline_minutes=2,
            rollback_decision_deadline_minutes=5,
            recommended_action="Review venue metadata drift and confirm catalog changes before promoting live.",
        ),
    )
