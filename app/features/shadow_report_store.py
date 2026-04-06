from __future__ import annotations

import json
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path


@dataclass(frozen=True)
class PersistedShadowReport:
    timestamp: str
    symbol: str
    decision_ts: str
    feature_set_name: str
    feature_set_version: str
    pass_ok: bool
    reason: str


class ShadowReportStore:
    def __init__(self, path: str | Path) -> None:
        self.path = Path(path)
        self.path.parent.mkdir(parents=True, exist_ok=True)

    def append(
        self,
        *,
        symbol: str,
        decision_ts: datetime,
        feature_set_name: str,
        feature_set_version: str,
        pass_ok: bool,
        reason: str,
    ) -> None:
        payload = PersistedShadowReport(
            timestamp=datetime.now(timezone.utc).isoformat(),
            symbol=symbol,
            decision_ts=decision_ts.isoformat(),
            feature_set_name=feature_set_name,
            feature_set_version=feature_set_version,
            pass_ok=pass_ok,
            reason=reason,
        )
        with self.path.open("a", encoding="utf-8") as handle:
            handle.write(json.dumps(asdict(payload), ensure_ascii=False, sort_keys=True) + "\n")
