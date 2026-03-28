import json
import logging
import io

from app.observability.logger import JsonFormatter, get_logger, set_trace_id, clear_trace_id
from logging.handlers import RotatingFileHandler
import os


def test_json_formatter_includes_trace_id(monkeypatch, capsys):
    set_trace_id("trace-123")
    logger = get_logger(name="testlogger", level="INFO", stream=io.StringIO())
    logger.info("hello", extra={"env": "dev"})
    out = logger.handlers[0].stream.getvalue().strip()
    payload = json.loads(out)
    assert payload["trace_id"] == "trace-123"
    assert payload["env"] == "dev"
    assert payload["level"] == "INFO"
    assert payload["message"] == "hello"


def test_log_level_toggle(capsys):
    buffer = io.StringIO()
    logger = get_logger(name="leveltest", level="ERROR", stream=buffer)
    logger.info("should not appear")
    logger.error("will appear")
    out = [line for line in buffer.getvalue().splitlines() if line]
    assert len(out) == 1
    payload = json.loads(out[0])
    assert payload["level"] == "ERROR"


def test_formatter_excludes_sensitive_keys():
    formatter = JsonFormatter()
    record = logging.LogRecord(
        name="x",
        level=logging.INFO,
        pathname=__file__,
        lineno=1,
        msg="msg",
        args=(),
        exc_info=None,
    )
    record.password = "SECRET"
    record.api_key = "HIDDEN"
    record.TOKEN = "HIDDEN2"
    formatted = formatter.format(record)
    assert "password" not in formatted
    assert "api_key" not in formatted
    assert "TOKEN" not in formatted


def test_trace_id_absent_when_not_set():
    buffer = io.StringIO()
    logger = get_logger(name="notrace", level="INFO", stream=buffer)
    # Ensure ContextVar is default (None).
    clear_trace_id()
    logger.info("msg")
    payload = json.loads(buffer.getvalue().strip())
    assert "trace_id" not in payload


def test_clear_trace_id_removes_previous():
    buffer = io.StringIO()
    set_trace_id("abc")
    clear_trace_id()
    logger = get_logger(name="clear", level="INFO", stream=buffer)
    logger.info("msg")
    payload = json.loads(buffer.getvalue().strip())
    assert "trace_id" not in payload


def test_file_handler_writes_json(tmp_path):
    log_path = tmp_path / "out.log"
    logger = get_logger(name="filelog", level="INFO", log_file=str(log_path))
    logger.info("file message", extra={"env": "dev"})
    contents = log_path.read_text(encoding="utf-8").strip()
    assert contents, "file should contain log entry"
    payload = json.loads(contents)
    assert payload["message"] == "file message"
    assert payload["env"] == "dev"


def test_file_handler_is_rotating(tmp_path):
    log_path = tmp_path / "rot.log"
    logger = get_logger(name="rot", level="INFO", log_file=str(log_path), max_bytes=50, backup_count=2)
    # generar varias entradas para forzar rotación con límite pequeño
    for _ in range(20):
        logger.info("x" * 10)
    logger.handlers[1].flush()
    rotated = list(tmp_path.glob("rot.log*"))
    assert rotated, "expected rotated files"
    assert any(f.name.endswith(".1") or f.name.endswith(".2") for f in rotated)
    handler = logger.handlers[1]
    assert isinstance(handler, RotatingFileHandler)
    assert handler.maxBytes == 50
    assert handler.backupCount == 2


def test_logger_fallback_on_bad_path(tmp_path):
    bad_path = tmp_path / "nonexistent" / "log.log"  # directory missing
    logger = get_logger(name="badlog", level="INFO", log_file=str(bad_path))
    # solo stream handler debe existir si falla file handler
    assert any(isinstance(h, logging.StreamHandler) for h in logger.handlers)
    assert not any(isinstance(h, RotatingFileHandler) for h in logger.handlers)
