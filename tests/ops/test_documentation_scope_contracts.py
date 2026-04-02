from __future__ import annotations

from pathlib import Path


REPO_ROOT = Path(__file__).resolve().parents[2]
ADR_PATH = REPO_ROOT / "docs" / "adr" / "ADR-0001-historical-market-data-scope.md"


def _read(relative_path: str) -> str:
    return (REPO_ROOT / relative_path).read_text(encoding="utf-8")


def test_historical_scope_adr_exists_and_is_approved() -> None:
    text = ADR_PATH.read_text(encoding="utf-8")

    assert "Estado: Aprobada" in text
    assert "bars-only (`kline`)" in text
    assert "`trade` historical backfill **no** forma parte del contrato soportado actual" in text


def test_public_docs_reference_the_historical_scope_adr() -> None:
    for relative_path in (
        "README.md",
        "docs/tech_spec.md",
        "docs/validation/ingestion_readiness.md",
    ):
        text = _read(relative_path)
        assert "ADR-0001-historical-market-data-scope.md" in text
        assert "bars-only (`kline`)" in text
