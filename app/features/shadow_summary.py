from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path


@dataclass(frozen=True)
class FeatureShadowSummary:
    generated_at: str
    path: str
    total_reports: int
    failed_reports: int
    critical_failures: int
    high_failures: int
    medium_failures: int
    pass_ok: bool


def summarize_shadow_reports(
    path: str | Path,
    *,
    max_failed_reports: int = 0,
    max_critical_failures: int = 0,
) -> FeatureShadowSummary:
    source = Path(path)
    rows = [
        json.loads(line)
        for line in source.read_text(encoding="utf-8").splitlines()
        if line.strip()
    ]
    failed = [row for row in rows if not bool(row.get("pass_ok", False))]
    critical_failures = sum(1 for row in failed if row.get("severity") == "critical")
    high_failures = sum(1 for row in failed if row.get("severity") == "high")
    medium_failures = sum(1 for row in failed if row.get("severity") == "medium")
    pass_ok = len(failed) <= max_failed_reports and critical_failures <= max_critical_failures
    return FeatureShadowSummary(
        generated_at=datetime.now(timezone.utc).isoformat(),
        path=str(source),
        total_reports=len(rows),
        failed_reports=len(failed),
        critical_failures=critical_failures,
        high_failures=high_failures,
        medium_failures=medium_failures,
        pass_ok=pass_ok,
    )


def write_shadow_summary(path: str | Path, summary: FeatureShadowSummary) -> Path:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(asdict(summary), ensure_ascii=False, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return target

