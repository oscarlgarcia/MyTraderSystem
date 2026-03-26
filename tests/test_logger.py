import json
import logging
import io

from app.observability.logger import JsonFormatter, get_logger, set_trace_id


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
    formatted = formatter.format(record)
    assert "password" not in formatted
    assert "api_key" not in formatted
