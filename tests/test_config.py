import os
from pathlib import Path

import pytest

from app.config import AppConfig, load_config


def test_load_dev_config():
    cfg = load_config("dev")
    assert isinstance(cfg, AppConfig)
    assert cfg.env == "dev"
    assert cfg.data_dir == Path("./data/dev")
    assert cfg.log_level == "INFO"


def test_missing_key_raises(tmp_path, monkeypatch):
    cfg_path = tmp_path / "config.broken.yaml"
    cfg_path.write_text('{"env": "broken"}', encoding="utf-8")
    monkeypatch.chdir(tmp_path)
    with pytest.raises(ValueError):
        load_config("broken")


def test_env_override(monkeypatch, tmp_path):
    cfg_path = tmp_path / "config.dev.yaml"
    cfg_path.write_text('{"env": "dev", "data_dir": "./data/dev", "log_level": "INFO"}', encoding="utf-8")
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("APP_DATA_DIR", "/tmp/custom")
    cfg = load_config("dev")
    assert cfg.data_dir == Path("/tmp/custom")


def test_app_env_respected(monkeypatch, tmp_path):
    cfg_path = tmp_path / "config.test.yaml"
    cfg_path.write_text('{"env": "test", "data_dir": "./data/test", "log_level": "WARNING"}', encoding="utf-8")
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("APP_ENV", "test")
    cfg = load_config()
    assert cfg.env == "test"


def test_invalid_log_level(monkeypatch, tmp_path):
    cfg_path = tmp_path / "config.dev.yaml"
    cfg_path.write_text('{"env": "dev", "data_dir": "./data/dev", "log_level": "VERBOSE"}', encoding="utf-8")
    monkeypatch.chdir(tmp_path)
    with pytest.raises(ValueError):
        load_config("dev")


def test_missing_file_raises(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    with pytest.raises(FileNotFoundError):
        load_config("dev")
