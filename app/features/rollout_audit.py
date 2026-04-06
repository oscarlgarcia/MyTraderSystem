from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path


@dataclass(frozen=True)
class PersistedCanaryDecision:
    timestamp: str
    feature_set_name: str
    route: str
    version: str
    symbol: str
    entity_scope: str
    decision_ts: str
    status: str


class CanaryAuditStore:
    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)

    def append(
        self,
        *,
        feature_set_name: str,
        route: str,
        version: str,
        symbol: str,
        entity_scope: str,
        decision_ts: datetime,
        status: str,
    ) -> None:
        payload = PersistedCanaryDecision(
            timestamp=datetime.now(timezone.utc).isoformat(),
            feature_set_name=feature_set_name,
            route=route,
            version=version,
            symbol=symbol,
            entity_scope=entity_scope,
            decision_ts=decision_ts.isoformat(),
            status=status,
        )
        with self.path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(asdict(payload), ensure_ascii=False, sort_keys=True) + "\n")
