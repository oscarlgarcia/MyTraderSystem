"""
Structured logging utilities.

No external dependencies; uses stdlib logging + json for formatting.
"""

from __future__ import annotations

import json
import logging
import re
import sys
from contextvars import ContextVar
from datetime import datetime, timezone
from logging import Logger
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Any, Dict, Optional, TextIO

TRACE_ID: ContextVar[Optional[str]] = ContextVar("trace_id", default=None)
REDACTED = "[REDACTED]"
SENSITIVE_KEY_MARKERS = (
    "password",
    "secret",
    "token",
    "api_key",
    "apikey",
    "private_key",
    "authorization",
    "cookie",
    "session",
)
SENSITIVE_VALUE_PATTERNS = (
    re.compile(r"bearer\s+[a-z0-9._\-]+", re.IGNORECASE),
    re.compile(r"(token|password|api[_-]?key|secret)=([^&\\s]+)", re.IGNORECASE),
)


def set_trace_id(trace_id: str) -> None:
    TRACE_ID.set(trace_id)


def clear_trace_id() -> None:
    TRACE_ID.set(None)


def get_trace_id() -> Optional[str]:
    return TRACE_ID.get()


class JsonFormatter(logging.Formatter):
    """Minimal JSON formatter."""

    def format(self, record: logging.LogRecord) -> str:  # type: ignore[override]
        message = record.getMessage()
        payload: Dict[str, Any] = {
            "ts": datetime.fromtimestamp(record.created, tz=timezone.utc).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "module": record.module,
            "message": message,
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
            if key.lower() in forbidden or _is_sensitive_key(key):
                continue
            payload[key] = _sanitize_value(value, field_name=key)
        return json.dumps(payload, ensure_ascii=False, default=str)


def _is_sensitive_key(key: str) -> bool:
    lowered = key.lower()
    return any(marker in lowered for marker in SENSITIVE_KEY_MARKERS)


def _sanitize_value(value: Any, *, field_name: str | None = None) -> Any:
    if field_name and _is_sensitive_key(field_name):
        return REDACTED
    if isinstance(value, dict):
        return {str(key): _sanitize_value(item, field_name=str(key)) for key, item in value.items()}
    if isinstance(value, (list, tuple, set)):
        return [_sanitize_value(item) for item in value]
    if isinstance(value, str):
        stripped = value.strip()
        if stripped.startswith("{") or stripped.startswith("["):
            try:
                parsed = json.loads(value)
            except json.JSONDecodeError:
                pass
            else:
                return _sanitize_value(parsed, field_name=field_name)
        for pattern in SENSITIVE_VALUE_PATTERNS:
            if pattern.search(value):
                return REDACTED
    return value


def _base_handler(stream: Optional[TextIO] = None) -> logging.Handler:
    handler = logging.StreamHandler(stream or sys.stdout)
    handler.setFormatter(JsonFormatter())
    return handler


def get_logger(
    name: str = "app",
    level: str = "INFO",
    log_file: Optional[str] = None,
    stream: Optional[TextIO] = None,
    max_bytes: int = 5_000_000,
    backup_count: int = 3,
) -> Logger:
    logger = logging.getLogger(name)
    logger.setLevel(level.upper())
    # Reset handlers to ensure consistent JSON formatting across repeated calls/tests.
    logger.handlers = []
    logger.addHandler(_base_handler(stream=stream))
    if log_file:
        try:
            file_handler = RotatingFileHandler(
                log_file, maxBytes=max_bytes, backupCount=backup_count, encoding="utf-8"
            )
            file_handler.setFormatter(JsonFormatter())
            logger.addHandler(file_handler)
            _log_file_metrics(logger, file_handler)
        except OSError as exc:
            warning_logger = _base_handler(stream=sys.stdout)
            logger.addHandler(warning_logger)
            logger.warning(
                "No se pudo abrir log_file, usando stdout",
                extra={"log_file": log_file, "error": str(exc)},
            )
    logger.propagate = False
    return logger


def _log_file_metrics(logger: Logger, handler: RotatingFileHandler) -> None:
    try:
        path = handler.baseFilename
        size = Path(path).stat().st_size if Path(path).exists() else 0
        backups = list(Path(path).parent.glob(Path(path).name + "*"))
        record = logger.makeRecord(
            logger.name,
            logging.INFO,
            __file__,
            0,
            "log_file_metrics",
            args=(),
            exc_info=None,
            extra={
                "log_file": path,
                "log_file_size": size,
                "log_file_backups": len(backups),
            },
        )
        # Emit metrics only to non-file handlers to keep log files clean for application events.
        for handler in logger.handlers:
            if isinstance(handler, RotatingFileHandler):
                continue
            handler.handle(record)
    except Exception:
        # No romper inicialización por métricas de log.
        pass
