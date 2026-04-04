from __future__ import annotations

from datetime import datetime
from typing import List

from app.common.dto import FeatureVector
from app.features.offline_store import MaterializationRunRecord, OfflineFeatureStore


class FeatureQueryService:
    def __init__(self, *, offline_store: OfflineFeatureStore) -> None:
        self.offline_store = offline_store

    def get_run(self, run_id: str) -> MaterializationRunRecord | None:
        return self.offline_store.get_materialization_run(run_id)

    def reconstruct_run(
        self,
        *,
        run_id: str,
        symbol: str | None = None,
        start_ts: datetime | None = None,
        end_ts: datetime | None = None,
    ) -> List[FeatureVector]:
        return self.offline_store.reconstruct_run(
            run_id=run_id,
            symbol=symbol,
            start_ts=start_ts,
            end_ts=end_ts,
        )
