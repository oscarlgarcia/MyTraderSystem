from __future__ import annotations

from pathlib import Path


WORKSPACE = Path(__file__).resolve().parents[2]
PROMOTION_RUNBOOK = WORKSPACE / "docs" / "operations" / "ingestion_promotion_runbook.md"
ROLLBACK_CHECKLIST = WORKSPACE / "docs" / "operations" / "ingestion_rollback_checklist.md"


def _read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def test_promotion_runbook_covers_live_prerequisites_and_waivers():
    content = _read(PROMOTION_RUNBOOK)

    required_snippets = (
        "## Prerequisites",
        "## Promotion Decision",
        "## Allowed Waivers",
        "## Forbidden Waivers",
        "## Promotion Procedure",
        "## Abort Conditions",
        "## Rollback Trigger",
        "## Artifact Freshness And Invalidation",
        "docs/validation/approved_ingestion_datasets.json",
        "docs/validation/ingestion_release_gates.json",
        "docs/validation/ingestion_live_drill_report.json",
        "docs/validation/ingestion_failure_injection.json",
        "strict",
        "stale artifact",
        "manifest mismatch",
        "material provider metadata drift",
    )

    for snippet in required_snippets:
        assert snippet in content


def test_rollback_checklist_covers_triggers_evidence_and_retry_rules():
    content = _read(ROLLBACK_CHECKLIST)

    required_snippets = (
        "## When Rollback Is Mandatory",
        "## Immediate Actions",
        "## Required Evidence To Preserve",
        "## Rollback Procedure",
        "## Retry Rules",
        "## Artifact Invalidation Rules",
        "## Exit Criteria",
        "gap_irreparable",
        "shadow_semantic_diff",
        "compaction_failure_detected",
        "provider_metadata_drift",
        "docs/validation/ingestion_release_gates.json",
        "docs/validation/ingestion_live_drill_report.json",
        "stale artifacts are invalidated and regenerated",
    )

    for snippet in required_snippets:
        assert snippet in content


def test_runbook_docs_do_not_contain_known_encoding_corruption():
    combined = _read(PROMOTION_RUNBOOK) + "\n" + _read(ROLLBACK_CHECKLIST)
    for bad_token in ("CuÃ¡ndo", "degradaciÃ³n", "crÃ­ticas", "promociÃ³n", "versiÃ³n"):
        assert bad_token not in combined
