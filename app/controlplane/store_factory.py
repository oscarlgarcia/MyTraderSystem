from __future__ import annotations

from app.config import AppConfig
from app.controlplane.sqlite_store import SQLiteControlPlaneStore
from app.controlplane.store import ControlPlaneStore, PostgresControlPlaneStore


def create_control_plane_store(cfg: AppConfig) -> ControlPlaneStore:
    backend = str(cfg.control_plane_backend).strip().lower()
    if backend == "sqlite":
        return SQLiteControlPlaneStore(cfg.control_plane_db_path)
    if backend == "postgres":
        return PostgresControlPlaneStore(cfg.control_plane_db_url or "")
    raise ValueError(f"Unsupported control_plane_backend: {cfg.control_plane_backend}")
