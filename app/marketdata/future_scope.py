from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass(frozen=True, slots=True)
class FutureFeedContract:
    feed_name: str
    status: str
    phase: str
    required_sources: tuple[str, ...]
    requires_snapshot: bool
    requires_replay: bool
    requires_recovery_strategy: bool
    requires_storage_schema: bool
    requires_delivery_contracts: bool
    requires_quality_scoring: bool
    notes: str


@dataclass(frozen=True, slots=True)
class FutureScopeRegistry:
    generated_at: str
    entries: tuple[FutureFeedContract, ...]


def future_scope_registry_path(base_dir: Path, env: str) -> Path:
    return Path(base_dir) / env / "catalog" / "future-scope.json"


def build_future_scope_registry() -> FutureScopeRegistry:
    entries = (
        FutureFeedContract(
            feed_name="book_delta",
            status="planned",
            phase="phase-4",
            required_sources=("websocket_incremental", "snapshot_bootstrap"),
            requires_snapshot=True,
            requires_replay=True,
            requires_recovery_strategy=True,
            requires_storage_schema=True,
            requires_delivery_contracts=True,
            requires_quality_scoring=True,
            notes="full depth requires sequence-aware resnapshot plus delta catch-up before promotion",
        ),
        FutureFeedContract(
            feed_name="book_bbo_contract",
            status="implemented",
            phase="phase-3",
            required_sources=("websocket_bookticker", "rest_snapshot"),
            requires_snapshot=True,
            requires_replay=True,
            requires_recovery_strategy=False,
            requires_storage_schema=True,
            requires_delivery_contracts=True,
            requires_quality_scoring=True,
            notes="top-of-book quotes are allowed as contractual expansion before full depth",
        ),
        FutureFeedContract(
            feed_name="funding_rate",
            status="planned",
            phase="phase-4",
            required_sources=("rest", "historical_backfill"),
            requires_snapshot=False,
            requires_replay=True,
            requires_recovery_strategy=False,
            requires_storage_schema=True,
            requires_delivery_contracts=True,
            requires_quality_scoring=True,
            notes="derivatives support requires explicit contract versioning and downstream serving contracts",
        ),
        FutureFeedContract(
            feed_name="open_interest",
            status="planned",
            phase="phase-4",
            required_sources=("rest", "historical_backfill"),
            requires_snapshot=False,
            requires_replay=True,
            requires_recovery_strategy=False,
            requires_storage_schema=True,
            requires_delivery_contracts=True,
            requires_quality_scoring=True,
            notes="open interest enters only with dataset catalog and quality scoring attached",
        ),
        FutureFeedContract(
            feed_name="liquidations",
            status="planned",
            phase="phase-4",
            required_sources=("websocket", "historical_backfill"),
            requires_snapshot=False,
            requires_replay=True,
            requires_recovery_strategy=False,
            requires_storage_schema=True,
            requires_delivery_contracts=True,
            requires_quality_scoring=True,
            notes="liquidation feeds require vendor-specific contract tests before promotion",
        ),
    )
    return FutureScopeRegistry(generated_at=_utc_now(), entries=entries)


def write_future_scope_registry(path: Path, registry: FutureScopeRegistry) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps({"generated_at": registry.generated_at, "entries": [asdict(item) for item in registry.entries]}, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    return path
