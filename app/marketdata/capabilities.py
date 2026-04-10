from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path

from app.marketdata.support_matrix import FEED_SUPPORT_MATRIX


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass(frozen=True, slots=True)
class VenueCapability:
    venue: str
    connector_version: str
    stream_type: str
    operational_tier: str
    supports_snapshot: bool
    supports_backfill: bool
    supports_replay: bool
    supports_paper: bool
    supports_live: bool
    recovery_capability: str


@dataclass(frozen=True, slots=True)
class VenueCapabilityRegistry:
    generated_at: str
    entries: tuple[VenueCapability, ...]


def capability_registry_path(base_dir: Path, env: str) -> Path:
    return Path(base_dir) / env / "catalog" / "venue-capabilities.json"


def build_venue_capability_registry() -> VenueCapabilityRegistry:
    entries = tuple(
        VenueCapability(
            venue="BINANCE",
            connector_version="binance.v1",
            stream_type=stream_type,
            operational_tier=support.operational_tier,
            supports_snapshot=support.supports_exact_recovery or support.supports_handoff,
            supports_backfill=stream_type in {"trade", "kline"},
            supports_replay=stream_type in {"trade", "kline"},
            supports_paper=support.supports_paper,
            supports_live=support.supports_live,
            recovery_capability=support.recovery_capability,
        )
        for stream_type, support in FEED_SUPPORT_MATRIX.items()
    )
    return VenueCapabilityRegistry(generated_at=_utc_now(), entries=entries)


def write_venue_capability_registry(path: Path, registry: VenueCapabilityRegistry) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps({"generated_at": registry.generated_at, "entries": [asdict(item) for item in registry.entries]}, ensure_ascii=False, indent=2),
        encoding="utf-8",
    )
    return path
