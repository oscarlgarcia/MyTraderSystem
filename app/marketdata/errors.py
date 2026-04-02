"""
Typed market data-specific failures.
"""

from __future__ import annotations

from collections.abc import Iterable

from app.ingestion.errors import IngestionError


class SchemaDriftError(IngestionError):
    def __init__(
        self,
        *,
        vendor: str,
        stream_type: str,
        shape_hash: str,
        expected_shape_hash: str,
        unexpected_paths: Iterable[str] = (),
        missing_required_paths: Iterable[str] = (),
        kind_mismatches: Iterable[str] = (),
        drift_mode: str = "blocking",
    ) -> None:
        self.vendor = str(vendor).upper()
        self.stream_type = str(stream_type)
        self.shape_hash = str(shape_hash)
        self.expected_shape_hash = str(expected_shape_hash)
        self.unexpected_paths = tuple(sorted(str(path) for path in unexpected_paths))
        self.missing_required_paths = tuple(sorted(str(path) for path in missing_required_paths))
        self.kind_mismatches = tuple(sorted(str(path) for path in kind_mismatches))
        self.drift_mode = str(drift_mode)

        details: list[str] = []
        if self.unexpected_paths:
            details.append(f"unexpected={list(self.unexpected_paths)}")
        if self.missing_required_paths:
            details.append(f"missing={list(self.missing_required_paths)}")
        if self.kind_mismatches:
            details.append(f"kind_mismatch={list(self.kind_mismatches)}")
        detail_message = ", ".join(details) if details else "shape mismatch"
        super().__init__(
            "parse",
            "permanent",
            (
                f"schema drift detected for {self.vendor} {self.stream_type}: "
                f"{detail_message} "
                f"(shape_hash={self.shape_hash}, expected_shape_hash={self.expected_shape_hash})"
            ),
        )

    def as_context(self) -> dict[str, object]:
        return {
            "vendor": self.vendor,
            "stream_type": self.stream_type,
            "shape_hash": self.shape_hash,
            "expected_shape_hash": self.expected_shape_hash,
            "unexpected_paths": list(self.unexpected_paths),
            "missing_required_paths": list(self.missing_required_paths),
            "kind_mismatches": list(self.kind_mismatches),
            "drift_mode": self.drift_mode,
        }
