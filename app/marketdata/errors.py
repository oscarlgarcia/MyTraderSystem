"""
Typed market data-specific failures and machine-readable incident payloads.
"""

from __future__ import annotations

from collections.abc import Iterable
from datetime import datetime
from typing import Any

from app.ingestion.errors import IngestionError


class MarketdataIncidentError(IngestionError):
    def __init__(self, category: str, severity: str, message: str) -> None:
        super().__init__(category, severity, message)

    @property
    def error_type(self) -> str:
        return type(self).__name__

    def as_context(self) -> dict[str, object]:
        return {
            "error_type": self.error_type,
            "error_category": self.category,
            "error_severity": self.severity,
            "error_message": self.message,
        }


class SchemaDriftError(MarketdataIncidentError):
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
            **super().as_context(),
            "vendor": self.vendor,
            "stream_type": self.stream_type,
            "shape_hash": self.shape_hash,
            "expected_shape_hash": self.expected_shape_hash,
            "unexpected_paths": list(self.unexpected_paths),
            "missing_required_paths": list(self.missing_required_paths),
            "kind_mismatches": list(self.kind_mismatches),
            "drift_mode": self.drift_mode,
        }


class IrrecoverableGapError(MarketdataIncidentError):
    def __init__(
        self,
        *,
        stream_key: str,
        venue: str,
        symbol: str,
        stream_type: str,
        gap_detection_mode: str | None,
        gap_seconds: float,
        missing_count: int,
    ) -> None:
        self.stream_key = stream_key
        self.venue = str(venue).upper()
        self.symbol = str(symbol).upper()
        self.stream_type = str(stream_type)
        self.gap_detection_mode = gap_detection_mode
        self.gap_seconds = float(gap_seconds)
        self.missing_count = int(missing_count)
        super().__init__(
            "validation",
            "permanent",
            (
                f"irrecoverable gap detected for {self.stream_key} "
                f"(mode={self.gap_detection_mode}, gap_seconds={self.gap_seconds:.6f}, missing_count={self.missing_count})"
            ),
        )

    def as_context(self) -> dict[str, object]:
        return {
            **super().as_context(),
            "stream_key": self.stream_key,
            "venue": self.venue,
            "symbol": self.symbol,
            "stream_type": self.stream_type,
            "gap_detection_mode": self.gap_detection_mode,
            "gap_seconds": self.gap_seconds,
            "missing_count": self.missing_count,
        }


class RecoveryExactnessError(MarketdataIncidentError):
    def __init__(
        self,
        *,
        stream_key: str,
        venue: str,
        symbol: str,
        stream_type: str,
        requested_rows: int,
        received_rows: int,
        request_start_ts: datetime | None,
        request_end_ts: datetime | None,
        interval: str | None,
        gap_seconds: float,
        missing_count: int,
        missing_timestamps: Iterable[str] = (),
        unexpected_timestamps: Iterable[str] = (),
        duplicate_timestamps: Iterable[str] = (),
    ) -> None:
        self.stream_key = stream_key
        self.venue = str(venue).upper()
        self.symbol = str(symbol).upper()
        self.stream_type = str(stream_type)
        self.requested_rows = int(requested_rows)
        self.received_rows = int(received_rows)
        self.request_start_ts = request_start_ts
        self.request_end_ts = request_end_ts
        self.interval = interval
        self.gap_seconds = float(gap_seconds)
        self.missing_count = int(missing_count)
        self.missing_timestamps = tuple(str(value) for value in missing_timestamps)
        self.unexpected_timestamps = tuple(str(value) for value in unexpected_timestamps)
        self.duplicate_timestamps = tuple(str(value) for value in duplicate_timestamps)
        super().__init__(
            "validation",
            "permanent",
            (
                f"recovery exactness violation for {self.stream_key} "
                f"(requested_rows={self.requested_rows}, received_rows={self.received_rows})"
            ),
        )

    def as_context(self) -> dict[str, object]:
        return {
            **super().as_context(),
            "stream_key": self.stream_key,
            "venue": self.venue,
            "symbol": self.symbol,
            "stream_type": self.stream_type,
            "requested_rows": self.requested_rows,
            "received_rows": self.received_rows,
            "request_start_ts": self.request_start_ts.isoformat() if self.request_start_ts else None,
            "request_end_ts": self.request_end_ts.isoformat() if self.request_end_ts else None,
            "interval": self.interval,
            "gap_seconds": self.gap_seconds,
            "missing_count": self.missing_count,
            "missing_timestamps": list(self.missing_timestamps),
            "unexpected_timestamps": list(self.unexpected_timestamps),
            "duplicate_timestamps": list(self.duplicate_timestamps),
        }


class CheckpointMismatchError(MarketdataIncidentError):
    def __init__(
        self,
        *,
        stream_key: str,
        checkpoint_cursor_kind: str | None,
        checkpoint_cursor_value: str | None,
        checkpoint_last_event_ts: datetime | None,
        reason: str,
    ) -> None:
        self.stream_key = stream_key
        self.checkpoint_cursor_kind = checkpoint_cursor_kind
        self.checkpoint_cursor_value = checkpoint_cursor_value
        self.checkpoint_last_event_ts = checkpoint_last_event_ts
        self.reason = str(reason)
        super().__init__(
            "validation",
            "permanent",
            f"checkpoint mismatch for {self.stream_key}: {self.reason}",
        )

    def as_context(self) -> dict[str, object]:
        return {
            **super().as_context(),
            "stream_key": self.stream_key,
            "checkpoint_cursor_kind": self.checkpoint_cursor_kind,
            "checkpoint_cursor_value": self.checkpoint_cursor_value,
            "checkpoint_last_event_ts": self.checkpoint_last_event_ts.isoformat() if self.checkpoint_last_event_ts else None,
            "reason": self.reason,
        }


class VendorReplayStaleDataError(MarketdataIncidentError):
    def __init__(
        self,
        *,
        stream_key: str,
        venue: str,
        symbol: str,
        stream_type: str,
        previous_event_ts: datetime,
        current_event_ts: datetime,
        late_seconds: float,
    ) -> None:
        self.stream_key = stream_key
        self.venue = str(venue).upper()
        self.symbol = str(symbol).upper()
        self.stream_type = str(stream_type)
        self.previous_event_ts = previous_event_ts
        self.current_event_ts = current_event_ts
        self.late_seconds = float(late_seconds)
        super().__init__(
            "validation",
            "permanent",
            f"vendor replay stale data for {self.stream_key} (late_seconds={self.late_seconds:.6f})",
        )

    def as_context(self) -> dict[str, object]:
        return {
            **super().as_context(),
            "stream_key": self.stream_key,
            "venue": self.venue,
            "symbol": self.symbol,
            "stream_type": self.stream_type,
            "previous_event_ts": self.previous_event_ts.isoformat(),
            "current_event_ts": self.current_event_ts.isoformat(),
            "late_seconds": self.late_seconds,
        }


class ShadowPromotionError(MarketdataIncidentError):
    def __init__(self, *, diffs: dict[str, Any]) -> None:
        self.diffs = dict(diffs)
        super().__init__(
            "validation",
            "permanent",
            "shadow comparison detected significant differences",
        )

    def as_context(self) -> dict[str, object]:
        return {
            **super().as_context(),
            "diffs": self.diffs,
        }


class MarketdataAnomalyError(MarketdataIncidentError):
    def __init__(
        self,
        *,
        stream_key: str,
        venue: str,
        symbol: str,
        stream_type: str,
        anomaly_type: str,
        anomaly_severity: str,
        anomaly_action: str,
        previous_price: float | None = None,
        current_price: float | None = None,
        relative_jump: float | None = None,
        previous_volume: float | None = None,
        current_volume: float | None = None,
        volume_ratio: float | None = None,
        threshold: float | None = None,
    ) -> None:
        self.stream_key = str(stream_key)
        self.venue = str(venue).upper()
        self.symbol = str(symbol).upper()
        self.stream_type = str(stream_type)
        self.anomaly_type = str(anomaly_type)
        self.anomaly_severity = str(anomaly_severity)
        self.anomaly_action = str(anomaly_action)
        self.previous_price = None if previous_price is None else float(previous_price)
        self.current_price = None if current_price is None else float(current_price)
        self.relative_jump = None if relative_jump is None else float(relative_jump)
        self.previous_volume = None if previous_volume is None else float(previous_volume)
        self.current_volume = None if current_volume is None else float(current_volume)
        self.volume_ratio = None if volume_ratio is None else float(volume_ratio)
        self.threshold = None if threshold is None else float(threshold)
        super().__init__(
            "validation",
            "permanent",
            (
                f"marketdata anomaly for {self.stream_key}: "
                f"type={self.anomaly_type}, severity={self.anomaly_severity}, action={self.anomaly_action}"
            ),
        )

    def as_context(self) -> dict[str, object]:
        return {
            **super().as_context(),
            "stream_key": self.stream_key,
            "venue": self.venue,
            "symbol": self.symbol,
            "stream_type": self.stream_type,
            "anomaly_type": self.anomaly_type,
            "anomaly_severity": self.anomaly_severity,
            "anomaly_action": self.anomaly_action,
            "previous_price": self.previous_price,
            "current_price": self.current_price,
            "relative_jump": self.relative_jump,
            "previous_volume": self.previous_volume,
            "current_volume": self.current_volume,
            "volume_ratio": self.volume_ratio,
            "threshold": self.threshold,
        }
