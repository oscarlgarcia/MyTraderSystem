from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path

from app.marketdata.support_matrix import FEED_SUPPORT_MATRIX


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass(frozen=True, slots=True)
class DeliveryContract:
    env: str
    stream_type: str
    venue: str
    snapshot_supported: bool
    replay_supported: bool
    research_supported: bool
    backtesting_supported: bool
    paper_supported: bool
    live_supported: bool
    freshness_expectation_seconds: float
    completeness_mode: str
    delivery_mode: str
    bootstrap_mode: str
    publication_mode: str
    contract_version: str = "v2"


@dataclass(frozen=True, slots=True)
class DeliveryContractRegistry:
    generated_at: str
    contracts: tuple[DeliveryContract, ...]


def delivery_contract_registry_path(base_dir: Path, env: str) -> Path:
    return Path(base_dir) / env / "catalog" / "delivery-contracts.json"


def build_delivery_contract_registry(*, env: str, venue: str = "BINANCE") -> DeliveryContractRegistry:
    contracts = []
    freshness_map = {"trade": 30.0, "kline": 90.0, "book": 5.0}
    for stream_type, support in FEED_SUPPORT_MATRIX.items():
        contracts.append(
            DeliveryContract(
                env=env,
                stream_type=stream_type,
                venue=venue,
                snapshot_supported=support.supports_handoff or support.supports_exact_recovery,
                replay_supported=support.supports_paper or support.supports_live,
                research_supported=True,
                backtesting_supported=True,
                paper_supported=support.supports_paper,
                live_supported=support.supports_live,
                freshness_expectation_seconds=freshness_map.get(stream_type, 30.0),
                completeness_mode=support.recovery_capability,
                delivery_mode="curated_snapshot_then_incremental",
                bootstrap_mode="snapshot_service",
                publication_mode="jsonl_bus_like",
            )
        )
    return DeliveryContractRegistry(generated_at=_utc_now(), contracts=tuple(contracts))


def write_delivery_contract_registry(path: Path, registry: DeliveryContractRegistry) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(
        json.dumps(
            {"generated_at": registry.generated_at, "contracts": [asdict(item) for item in registry.contracts]},
            ensure_ascii=False,
            indent=2,
        ),
        encoding="utf-8",
    )
    return path


def read_delivery_contract_registry(path: Path) -> DeliveryContractRegistry:
    resolved = Path(path)
    if not resolved.exists():
        return DeliveryContractRegistry(generated_at=_utc_now(), contracts=())
    payload = json.loads(resolved.read_text(encoding="utf-8"))
    return DeliveryContractRegistry(
        generated_at=str(payload.get("generated_at") or _utc_now()),
        contracts=tuple(DeliveryContract(**item) for item in payload.get("contracts", ())),
    )
