from __future__ import annotations

from pathlib import Path


def _repo_root() -> Path:
    return Path(__file__).resolve().parents[2]


def test_storage_critical_paths_do_not_use_full_table_materialization():
    source = (_repo_root() / "app" / "ingestion" / "storage.py").read_text(encoding="utf-8")

    assert "table.to_pylist()" not in source
    assert "existing.to_pylist()" not in source
    assert "new.to_pylist()" not in source
    assert "tbl.to_pylist()" not in source


def test_shadow_snapshot_builder_does_not_use_full_table_materialization():
    source = (_repo_root() / "app" / "ingestion" / "shadow.py").read_text(encoding="utf-8")

    assert "table.to_pylist()" not in source
