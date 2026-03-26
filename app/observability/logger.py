"""
Structured logging utilities.

No external dependencies; uses stdlib logging + json for formatting.
"""

from __future__ import annotations

import json
import logging
import sys
from contextvars import ContextVar
from datetime import datetime, timezone
from logging import Logger
from typing import Any, Dict, Optional

TRACE_ID: ContextVar[Optional[str]] = ContextVar("trace_id", default=None)


def set_trace_id(trace_id: str) -> None:
    TRACE_ID.set(trace_id)


def get_trace_id() -> Optional[str]:
    return TRACE_ID.get()


class JsonFormatter(logging.Formatter):
    """Minimal JSON formatter."""

def format(self, record: logging.LogRecord) -> str:  # type: ignore[override]
        payload: Dict[str, Any] = {
            "ts": datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "module": record.module,
            "message": record.getMessage(),
        }
        trace_id = get_trace_id()
        if trace_id:
            payload["trace_id"] = trace_id
        # Attach extras excluding built-ins
        forbidden = {"password", "secret", "token", "api_key"}
        builtin_keys = logging.LogRecord(None, None, "", 0, "", (), None).__dict__.keys()
        for key, value in record.__dict__.items():
            if key.startswith("_") or key in builtin_keys:
                continue
            if key in payload:
                continue
            if key.lower() in forbidden:
                continue
            payload[key] = value
        return json.dumps(payload, ensure_ascii=False)


def _base_handler() -> logging.Handler:
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonFormatter())
    return handler


def get_logger(name: str = "app", level: str = "INFO", log_file: Optional[str] = None) -> Logger:
    logger = logging.getLogger(name)
    logger.setLevel(level.upper())
    # Reset handlers to ensure consistent JSON formatting across repeated calls/tests.
    logger.handlers = []
    logger.addHandler(_base_handler())
    if log_file:
        file_handler = logging.FileHandler(log_file)
        file_handler.setFormatter(JsonFormatter())
        logger.addHandler(file_handler)
    logger.propagate = False
    return logger
