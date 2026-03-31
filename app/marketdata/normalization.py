"""
Normalization version policy for market data events and datasets.
"""

from __future__ import annotations

from typing import Final

NORMALIZER_VERSION: Final[str] = "v1"
SUPPORTED_NORMALIZER_VERSIONS: Final[frozenset[str]] = frozenset({NORMALIZER_VERSION})


def resolve_normalizer_version(version: str | None = None) -> str:
    resolved = version or NORMALIZER_VERSION
    if resolved not in SUPPORTED_NORMALIZER_VERSIONS:
        raise ValueError(f"unsupported normalizer_version: {resolved}")
    return resolved


def stamp_normalizer_version(metadata: dict[str, str], *, version: str | None = None) -> dict[str, str]:
    metadata.setdefault("normalizer_version", resolve_normalizer_version(version))
    return metadata
