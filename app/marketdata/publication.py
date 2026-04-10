from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


@dataclass(frozen=True, slots=True)
class PublicationRecord:
    env: str
    venue: str
    stream_type: str
    symbol: str
    published_at: str
    payload: dict
    dataset_id: str | None = None
    dataset_version: str | None = None
    lineage_id: str | None = None
    delivery_contract_version: str | None = None


def publication_path(base_dir: Path, env: str, *, stream_type: str, venue: str = "BINANCE") -> Path:
    return Path(base_dir) / env / "publication" / f"venue={venue.upper()}" / f"stream_type={stream_type}" / "events.jsonl"


def publish_record(base_dir: Path, record: PublicationRecord) -> Path:
    path = publication_path(base_dir, record.env, stream_type=record.stream_type, venue=record.venue)
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(
            json.dumps(
                {
                    "published_at": record.published_at or _utc_now(),
                    "env": record.env,
                    "venue": record.venue,
                    "stream_type": record.stream_type,
                    "symbol": record.symbol,
                    "dataset_id": record.dataset_id,
                    "dataset_version": record.dataset_version,
                    "lineage_id": record.lineage_id,
                    "delivery_contract_version": record.delivery_contract_version,
                    "payload": record.payload,
                },
                ensure_ascii=False,
                default=str,
            )
        )
        handle.write("\n")
    return path
