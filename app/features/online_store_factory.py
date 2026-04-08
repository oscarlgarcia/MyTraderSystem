from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

from app.features.online_store import OnlineFeatureStore
from app.features.online_store_base import FeatureOnlineStore
from app.features.online_store_http import RemoteHttpOnlineFeatureStore
from app.features.online_store_json import JsonOnlineFeatureStore
from app.features.online_store_memory import MemoryOnlineFeatureStore

PRODUCTION_CANONICAL_ONLINE_BACKEND = "http"
LIVE_READY_ONLINE_BACKENDS = (PRODUCTION_CANONICAL_ONLINE_BACKEND,)


@dataclass(frozen=True)
class OnlineStoreConfig:
    backend: str = "local_sqlite"
    path: str | Path | None = None
    url: str | None = None
    timeout_seconds: float = 5.0
    history_max_rows_per_scope: int | None = None
    history_retention_seconds: float | None = None


def is_live_ready_online_backend(backend: str) -> bool:
    return str(backend).strip().lower() in LIVE_READY_ONLINE_BACKENDS


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
    if config.backend == "http":
        if not config.url:
            raise ValueError("http backend requires url")
        return RemoteHttpOnlineFeatureStore(config.url, timeout_seconds=float(config.timeout_seconds))
    raise ValueError(f"unsupported online store backend: {config.backend}")
