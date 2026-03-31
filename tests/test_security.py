import io
import json
from pathlib import Path

import pytest

from app.config import get_secret_env
from app.observability.logger import get_logger
from app.ingestion.storage import validate_output_path


def test_sensitive_fields_are_never_logged():
    buffer = io.StringIO()
    logger = get_logger(name="test.security.logger", level="INFO", stream=buffer)

    logger.info(
        "security check",
        extra={
            "api_key": "super-secret-key",
            "payload": {
                "password": "hunter2",
                "nested": {"authorization": "Bearer topsecret"},
                "note": "token=abc123",
            },
        },
    )

    payload = json.loads(buffer.getvalue().strip())
    rendered = buffer.getvalue()
    assert "super-secret-key" not in rendered
    assert "hunter2" not in rendered
    assert "topsecret" not in rendered
    assert "abc123" not in rendered
    assert "api_key" not in payload
    assert payload["payload"]["password"] == "[REDACTED]"
    assert payload["payload"]["nested"]["authorization"] == "[REDACTED]"
    assert payload["payload"]["note"] == "[REDACTED]"


def test_invalid_output_path_is_rejected(tmp_path):
    file_path = tmp_path / "not_a_dir"
    file_path.write_text("x", encoding="utf-8")

    with pytest.raises(ValueError, match="data_dir must be a directory"):
        validate_output_path(file_path)


def test_secret_env_helper_reads_prefixed_secret(monkeypatch):
    monkeypatch.setenv("APP_SECRET_BINANCE_KEY", "value")
    assert get_secret_env("binance_key") == "value"
