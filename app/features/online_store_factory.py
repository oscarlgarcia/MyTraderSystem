from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from app.features.online_store import OnlineFeatureStore
from app.features.online_store_base import FeatureOnlineStore
from app.features.online_store_json import JsonOnlineFeatureStore
from app.features.online_store_memory import MemoryOnlineFeatureStore


@dataclass(frozen=True)
class OnlineStoreConfig:
    backend: str = "local_sqlite"
    path: str | Path | None = None
    history_max_rows_per_scope: int | None = None
    history_retention_seconds: float | None = None


def create_online_store(config: OnlineStoreConfig) -> FeatureOnlineStore:
    if config.backend == "memory":
        return MemoryOnlineFeatureStore()
    if config.backend == "local_sqlite":
        if config.path is None:
            raise ValueError("local_sqlite backend requires path")
        return OnlineFeatureStore(
            config.path,
            history_max_rows_per_scope=config.history_max_rows_per_scope,
            history_retention_seconds=config.history_retention_seconds,
        )
    if config.backend == "json_file":
        if config.path is None:
            raise ValueError("json_file backend requires path")
        return JsonOnlineFeatureStore(config.path)
    raise ValueError(f"unsupported online store backend: {config.backend}")
