import os
from pathlib import Path

import pytest

from app.config import AppConfig, load_config, parse_args
from app.marketdata.instruments import resolve_instrument


def test_load_dev_config():
    cfg = load_config("dev")
    assert isinstance(cfg, AppConfig)
    assert cfg.env == "dev"
    assert cfg.data_dir == Path("./data/dev")
    assert cfg.log_level == "INFO"
    assert cfg.ws_base.startswith("wss://")
    assert cfg.rest_base.startswith("https://")
    assert cfg.symbols == ["BTCUSDT", "ETHUSDT"]
    assert cfg.feature_online_backend == "local_sqlite"
    assert cfg.feature_online_store_path.as_posix().endswith("data/dev/feature-store/online.sqlite")
    assert cfg.feature_online_store_url == "http://127.0.0.1:8011"
    assert cfg.feature_offline_store_path.as_posix().endswith("data/dev/feature-store/offline.sqlite")
    assert cfg.feature_store_server_backend == "local_sqlite"
    assert cfg.feature_store_server_path.as_posix().endswith("data/dev/feature-store/online.sqlite")
    assert cfg.feature_store_server_host == "127.0.0.1"
    assert cfg.feature_store_server_port == 8011
    assert cfg.feature_release_online_backend == "http"
    assert cfg.feature_observability_sink == "http"
    assert cfg.feature_validation_dir == Path("docs/validation")


def test_missing_key_raises(tmp_path, monkeypatch):
    cfg_path = tmp_path / "config.broken.yaml"
    cfg_path.write_text('{"env": "broken"}', encoding="utf-8")
    monkeypatch.chdir(tmp_path)
    with pytest.raises(ValueError):
        load_config("broken")


def test_env_override(monkeypatch, tmp_path):
    cfg_path = tmp_path / "config.dev.yaml"
    cfg_path.write_text(
        '{"env": "dev", "data_dir": "./data/dev", "log_level": "INFO", "ws_base": "wss://x", "rest_base": "https://x", "symbols": ["BTCUSDT"]}',
        encoding="utf-8",
    )
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("APP_DATA_DIR", "/tmp/custom")
    cfg = load_config("dev")
    assert cfg.data_dir == Path("/tmp/custom")


def test_app_env_respected(monkeypatch, tmp_path):
    cfg_path = tmp_path / "config.test.yaml"
    cfg_path.write_text(
        '{"env": "test", "data_dir": "./data/test", "log_level": "WARNING", "ws_base": "wss://x", "rest_base": "https://x", "symbols": ["BTCUSDT"]}',
        encoding="utf-8",
    )
    monkeypatch.chdir(tmp_path)
    monkeypatch.setenv("APP_ENV", "test")
    cfg = load_config()
    assert cfg.env == "test"


def test_invalid_log_level(monkeypatch, tmp_path):
    cfg_path = tmp_path / "config.dev.yaml"
    cfg_path.write_text(
        '{"env": "dev", "data_dir": "./data/dev", "log_level": "VERBOSE", "ws_base": "wss://x", "rest_base": "https://x", "symbols": ["BTCUSDT"]}',
        encoding="utf-8",
    )
    monkeypatch.chdir(tmp_path)
    with pytest.raises(ValueError):
        load_config("dev")


def test_missing_file_raises(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    with pytest.raises(FileNotFoundError):
        load_config("dev")


def test_invalid_endpoint(monkeypatch, tmp_path):
    cfg_path = tmp_path / "config.dev.yaml"
    cfg_path.write_text(
        '{"env": "dev", "data_dir": "./data/dev", "log_level": "INFO", "ws_base": "ftp://x", "rest_base": "https://x", "symbols": ["BTCUSDT"]}',
        encoding="utf-8",
    )
    monkeypatch.chdir(tmp_path)
    with pytest.raises(ValueError):
        load_config("dev")


def test_symbols_required_and_upper(monkeypatch, tmp_path):
    cfg_path = tmp_path / "config.dev.yaml"
    cfg_path.write_text(
        '{"env": "dev", "data_dir": "./data/dev", "log_level": "INFO", "ws_base": "wss://x", "rest_base": "https://x", "symbols": []}',
        encoding="utf-8",
    )
    monkeypatch.chdir(tmp_path)
    with pytest.raises(ValueError):
        load_config("dev")

    cfg_path.write_text(
        '{"env": "dev", "data_dir": "./data/dev", "log_level": "INFO", "ws_base": "wss://x", "rest_base": "https://x", "symbols": ["ethusdt"]}',
        encoding="utf-8",
    )
    cfg = load_config("dev")
    assert cfg.symbols == ["ETHUSDT"]


def test_load_config_registers_symbols_in_instrument_catalog(monkeypatch, tmp_path):
    cfg_path = tmp_path / "config.dev.yaml"
    cfg_path.write_text(
        '{"env": "dev", "data_dir": "./data/dev", "log_level": "INFO", "ws_base": "wss://x", "rest_base": "https://x", "symbols": ["solusdt"]}',
        encoding="utf-8",
    )
    monkeypatch.chdir(tmp_path)

    cfg = load_config("dev")

    instrument = resolve_instrument("SOLUSDT", venue="BINANCE")
    assert cfg.symbols == ["SOLUSDT"]
    assert instrument.base_asset == "SOL"
    assert instrument.quote_asset == "USDT"


def test_parse_args_accepts_pytest_flags():
    args = parse_args(["--env", "test", "-vv"])
    assert args.env == "test"


def test_load_prod_config():
    cfg = load_config("prod")
    assert isinstance(cfg, AppConfig)
    assert cfg.env == "prod"
    assert cfg.data_dir.is_absolute()
    assert cfg.feature_online_backend == "http"
    assert cfg.feature_online_store_url == "http://127.0.0.1:8011"
    assert cfg.feature_store_server_backend == "local_sqlite"
    assert cfg.feature_store_server_port == 8011
    assert cfg.feature_release_online_backend == "http"
    assert cfg.feature_observability_sink == "http"
    assert cfg.feature_validation_dir.is_absolute()
