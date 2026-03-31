"""
Typed ingestion failures and policy helpers.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from typing import Literal

import httpx

try:
    from websockets.exceptions import ConnectionClosed
except ImportError:  # pragma: no cover - optional in local tooling
    ConnectionClosed = ()  # type: ignore[assignment]


ErrorCategory = Literal["source", "parse", "validation", "sink"]
ErrorSeverity = Literal["transient", "permanent"]
ErrorPolicy = Literal["fail_fast", "allow_fallback", "degraded"]


@dataclass
class IngestionError(Exception):
    category: ErrorCategory
    severity: ErrorSeverity
    message: str

    def __str__(self) -> str:
        return self.message

    @property
    def retryable(self) -> bool:
        return self.category == "source" and self.severity == "transient"


def classify_error(exc: Exception, *, default_category: ErrorCategory = "source") -> IngestionError:
    if isinstance(exc, IngestionError):
        return exc
    if isinstance(exc, json.JSONDecodeError):
        return IngestionError("parse", "permanent", f"invalid JSON payload: {exc}")
    if isinstance(exc, KeyError):
        return IngestionError("parse", "permanent", f"missing payload field: {exc}")
    if isinstance(exc, ValueError):
        category: ErrorCategory = "validation" if default_category != "sink" else "sink"
        return IngestionError(category, "permanent", str(exc))
    if isinstance(exc, (TimeoutError, ConnectionError, httpx.TimeoutException, httpx.ConnectError)):
        return IngestionError(default_category, "transient", str(exc))
    if ConnectionClosed and isinstance(exc, ConnectionClosed):
        return IngestionError(default_category, "transient", str(exc))
    if isinstance(exc, httpx.HTTPStatusError):
        status = exc.response.status_code if exc.response is not None else None
        severity: ErrorSeverity = "transient" if status in {429, 500, 502, 503, 504} else "permanent"
        return IngestionError(default_category, severity, str(exc))
    if isinstance(exc, OSError):
        category = "sink" if default_category == "sink" else default_category
        return IngestionError(category, "permanent", str(exc))
    if isinstance(exc, RuntimeError) and default_category == "source":
        return IngestionError("source", "transient", str(exc))
    return IngestionError(default_category, "permanent", str(exc))


def classify_connector_error(exc: Exception) -> IngestionError:
    if isinstance(exc, OSError):
        return IngestionError("source", "transient", str(exc))
    return classify_error(exc, default_category="source")


def resolve_error_policy(policy: ErrorPolicy | None, *, allow_live_fallback: bool = False) -> ErrorPolicy:
    if policy:
        return policy
    if allow_live_fallback:
        return "allow_fallback"
    return "fail_fast"
