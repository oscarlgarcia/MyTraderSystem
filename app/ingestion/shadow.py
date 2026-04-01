from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path


@dataclass(frozen=True, slots=True)
class ShadowSnapshot:
    pipeline_version: str
    events_persisted: int
    duplicates_total: int
    gaps_total: int
    processing_latency_seconds: float
    write_latency_seconds: float


@dataclass(frozen=True, slots=True)
class ShadowComparison:
    primary: ShadowSnapshot
    shadow: ShadowSnapshot
    diffs: dict[str, float]
    significant: bool


class ShadowPromotionError(RuntimeError):
    pass


def compare_shadow_snapshots(primary: ShadowSnapshot, shadow: ShadowSnapshot) -> ShadowComparison:
    diffs = {
        "events_persisted": float(primary.events_persisted - shadow.events_persisted),
        "duplicates_total": float(primary.duplicates_total - shadow.duplicates_total),
        "gaps_total": float(primary.gaps_total - shadow.gaps_total),
        "processing_latency_seconds": float(primary.processing_latency_seconds - shadow.processing_latency_seconds),
        "write_latency_seconds": float(primary.write_latency_seconds - shadow.write_latency_seconds),
    }
    significant = any(
        diffs[key] != 0.0
        for key in ("events_persisted", "duplicates_total", "gaps_total")
    )
    return ShadowComparison(
        primary=primary,
        shadow=shadow,
        diffs=diffs,
        significant=significant,
    )


def persist_shadow_comparison(base_dir: Path, *, env: str, comparison: ShadowComparison) -> Path:
    out_path = Path(base_dir) / "shadow" / f"env={env}" / "comparisons.jsonl"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "ts": datetime.now(timezone.utc).isoformat(),
        "primary": asdict(comparison.primary),
        "shadow": asdict(comparison.shadow),
        "diffs": comparison.diffs,
        "significant": comparison.significant,
    }
    with out_path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload, ensure_ascii=False, default=str) + "\n")
    return out_path


def assert_shadow_promotable(comparison: ShadowComparison, *, block_on_diff: bool) -> None:
    if block_on_diff and comparison.significant:
        raise ShadowPromotionError(
            "shadow comparison detected significant differences: "
            + ", ".join(f"{key}={value}" for key, value in comparison.diffs.items() if value != 0.0)
        )
