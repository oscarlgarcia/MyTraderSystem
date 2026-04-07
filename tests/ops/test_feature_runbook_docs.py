from __future__ import annotations

from pathlib import Path


WORKSPACE = Path(__file__).resolve().parents[2]
PROMOTION_RUNBOOK = WORKSPACE / "docs" / "operations" / "feature_promotion_runbook.md"
ROLLBACK_CHECKLIST = WORKSPACE / "docs" / "operations" / "feature_rollback_checklist.md"


def _read(path: Path) -> str:
    return path.read_text(encoding="utf-8")


def test_feature_promotion_runbook_covers_live_prerequisites_and_gates():
    content = _read(PROMOTION_RUNBOOK)
    required_snippets = (
        "## Prerequisites",
        "## Promotion Decision",
        "## Promotion Procedure",
        "## Live Readiness Gates",
        "## Abort Conditions",
        "## Artifact Freshness And Invalidation",
        "docs/validation/feature_release_gates.json",
        "docs/validation/feature_observability.json",
        "docs/validation/feature_shadow_summary.json",
        "docs/validation/feature_serving_soak.json",
        "docs/validation/feature_serving_concurrency.json",
        "training bundle",
        "contract_validation",
    )
    for snippet in required_snippets:
        assert snippet in content


def test_feature_rollback_checklist_covers_triggers_evidence_and_retry_rules():
    content = _read(ROLLBACK_CHECKLIST)
    required_snippets = (
        "## When Rollback Is Mandatory",
        "## Immediate Actions",
        "## Required Evidence To Preserve",
        "## Rollback Procedure",
        "## Retry Rules",
        "shadow_failure_budget_exceeded",
        "training_serving_contract_not_validated",
        "feature_release_gates.json",
        "feature_shadow_summary.json",
        "feature_serving_soak.json",
    )
    for snippet in required_snippets:
        assert snippet in content
