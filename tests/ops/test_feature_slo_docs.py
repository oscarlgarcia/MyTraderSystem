from __future__ import annotations

from pathlib import Path


WORKSPACE = Path(__file__).resolve().parents[2]
SLO_DOC = WORKSPACE / "docs" / "operations" / "feature_serving_slos.md"


def test_feature_serving_slo_doc_covers_paper_live_and_rollback():
    content = SLO_DOC.read_text(encoding="utf-8")
    required_snippets = (
        "## Paper Baseline",
        "## Live Baseline",
        "## Rollback Triggers",
        "feature_serving_soak.json.pass_ok",
        "feature_serving_concurrency.json.pass_ok",
        "feature_release_gates.json.pass_ok",
        "live_readiness.pass_ok",
        "shadow failure budget",
        "invalid ratio budget",
    )
    for snippet in required_snippets:
        assert snippet in content
