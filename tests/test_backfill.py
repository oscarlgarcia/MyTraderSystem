import datetime as dt
from types import SimpleNamespace

import httpx
import pytest

from app.ingestion import backfill
from app.ingestion.storage import normalized_partition_path, read_parquet
from app.common.dto import MarketEvent


class DummyResponse:
    def __init__(self, status_code: int, json_data):
        self.status_code = status_code
        self._json = json_data

    def json(self):
        return self._json

    def raise_for_status(self):
        if self.status_code >= 400:
            raise httpx.HTTPStatusError("err", request=None, response=None)


def test_fetch_klines_paginates(monkeypatch):
    calls = []

    def fake_get(url, params=None, timeout=None):
        calls.append(params["startTime"])
        if params["startTime"] == 0:
            return DummyResponse(
                200,
                [
                    [0, "", "", "", "1", "10", 60000],
                    [60000, "", "", "", "2", "20", 120000],
                ],
            )
        return DummyResponse(
            200,
            [
                [120001, "", "", "", "3", "30", 180000],
            ],
        )

    client = SimpleNamespace(get=fake_get)
    rows = backfill.fetch_klines(client, "https://x", "BTCUSDT", 0, 200000, limit=2)
    assert len(rows) == 3
    assert calls == [0, 120001]


def test_invalid_date_raises():
    with pytest.raises(SystemExit):
        backfill.parse_args(["--env", "dev", "--symbol", "BTCUSDT", "--start", "bad", "--end", "2024-01-01T00:00:00+00:00"])


def test_backfill_dedup_flag():
    args = backfill.parse_args(
        [
            "--env",
            "dev",
            "--symbol",
            "BTCUSDT",
            "--start",
            "2024-01-01T00:00:00+00:00",
            "--end",
            "2024-01-01T00:01:00+00:00",
            "--dedup",
        ]
    )
    assert args.dedup is True


def test_429_retries_and_fails(monkeypatch):
    attempts = {"n": 0}

    def fake_get(url, params=None, timeout=None):
        attempts["n"] += 1
        return DummyResponse(429, [])

    client = SimpleNamespace(get=fake_get)
    with pytest.raises(httpx.HTTPStatusError):
        backfill.fetch_klines(client, "https://x", "BTCUSDT", 0, 1000, limit=1, retries_429=2)
    assert attempts["n"] == 3  # 1 intento inicial + 2 reintentos


def test_normalize_kline_row_utc():
    row = [0, "", "", "", "100", "5", 60_000]  # close time 60s
    ev = backfill.normalize_kline_row("BTCUSDT", row)
    assert ev.event_ts.tzinfo is not None
    assert ev.event_ts == dt.datetime.fromtimestamp(60, tz=dt.timezone.utc)


def test_backfill_writes_and_idempotent(monkeypatch, tmp_path):
    cfg = SimpleNamespace(env="dev", data_dir=tmp_path, log_level="INFO", rest_base="https://x")
    rows = [
        [1704067200000, "", "", "", "1", "10", 1704067260000],  # 2024-01-01 00:00:00 / close +60s
        [1704067260001, "", "", "", "2", "20", 1704067320000],  # +60s
    ]

    monkeypatch.setattr(backfill, "load_config", lambda env=None: cfg)
    monkeypatch.setattr(backfill, "fetch_klines", lambda **kwargs: rows)

    backfill.run(["--env", "dev", "--symbol", "BTCUSDT", "--start", "2024-01-01T00:00:00+00:00", "--end", "2024-01-01T01:00:00+00:00", "--dedup"])
    backfill.run(["--env", "dev", "--symbol", "BTCUSDT", "--start", "2024-01-01T00:00:00+00:00", "--end", "2024-01-01T01:00:00+00:00", "--dedup"])

    path = normalized_partition_path(tmp_path, "dev", source="kline", symbol="BTCUSDT", day="2024-01-01")
    assert path.exists()
    table = read_parquet(path)
    assert table.num_rows == 2  # idempotente
    # ordenado por event_ts
    ts_list = table.column("event_ts").to_pylist()
    assert ts_list == sorted(ts_list)


def test_backfill_dedup_drops_duplicates_and_logs(monkeypatch, tmp_path, capsys):
    cfg = SimpleNamespace(env="dev", data_dir=tmp_path, log_level="INFO", rest_base="https://x")
    rows = [
        [1704067200000, "", "", "", "1", "10", 1704067260000],
        [1704067200000, "", "", "", "1", "10", 1704067260000],
    ]

    monkeypatch.setattr(backfill, "load_config", lambda env=None: cfg)
    monkeypatch.setattr(backfill, "fetch_klines", lambda **kwargs: rows)

    backfill.run(
        [
            "--env",
            "dev",
            "--symbol",
            "BTCUSDT",
            "--start",
            "2024-01-01T00:00:00+00:00",
            "--end",
            "2024-01-01T00:10:00+00:00",
            "--dedup",
        ]
    )

    table = read_parquet(normalized_partition_path(tmp_path, "dev", source="kline", symbol="BTCUSDT", day="2024-01-01"))
    assert table.num_rows == 1
    assert "backfill duplicates dropped" in capsys.readouterr().out


def test_backfill_without_dedup_keeps_duplicates(monkeypatch, tmp_path):
    cfg = SimpleNamespace(env="dev", data_dir=tmp_path, log_level="INFO", rest_base="https://x")
    rows = [
        [1704067200000, "", "", "", "1", "10", 1704067260000],
        [1704067200000, "", "", "", "1", "10", 1704067260000],
    ]

    monkeypatch.setattr(backfill, "load_config", lambda env=None: cfg)
    monkeypatch.setattr(backfill, "fetch_klines", lambda **kwargs: rows)

    backfill.run(
        [
            "--env",
            "dev",
            "--symbol",
            "BTCUSDT",
            "--start",
            "2024-01-01T00:00:00+00:00",
            "--end",
            "2024-01-01T00:10:00+00:00",
        ]
    )

    table = read_parquet(normalized_partition_path(tmp_path, "dev", source="kline", symbol="BTCUSDT", day="2024-01-01"))
    assert table.num_rows == 2


def test_gap_detection(monkeypatch):
    events = [
        MarketEvent(symbol="BTCUSDT", event_ts=dt.datetime.fromtimestamp(0, tz=dt.timezone.utc), price=1.0, size=1.0, source="kline"),
        MarketEvent(symbol="BTCUSDT", event_ts=dt.datetime.fromtimestamp(180, tz=dt.timezone.utc), price=1.0, size=1.0, source="kline"),
    ]
    assert backfill._count_gaps(events, interval_ms=60_000) == 1


def test_dry_run_creates_no_files(monkeypatch, tmp_path):
    cfg = SimpleNamespace(env="dev", data_dir=tmp_path, log_level="INFO", rest_base="https://x")
    rows = [
        [1704067200000, "", "", "", "1", "10", 1704067260000],
    ]
    monkeypatch.setattr(backfill, "load_config", lambda env=None: cfg)
    monkeypatch.setattr(backfill, "fetch_klines", lambda **kwargs: rows)

    backfill.run(["--env", "dev", "--symbol", "BTCUSDT", "--start", "2024-01-01T00:00:00+00:00", "--end", "2024-01-01T00:10:00+00:00", "--dry-run"])
    files = list(tmp_path.glob("normalized/bars/env=dev/venue=*/symbol=BTCUSDT/date=*/data.parquet"))
    assert not files


def test_interval_no_soportado():
    with pytest.raises(ValueError):
        backfill.run(
            [
                "--env",
                "dev",
                "--symbol",
                "BTCUSDT",
                "--start",
                "2024-01-01T00:00:00+00:00",
                "--end",
                "2024-01-01T00:05:00+00:00",
                "--interval",
                "2m",  # no soportado
            ]
        )


def test_http_500_retries_then_fail(monkeypatch):
    attempts = {"n": 0}

    class DummyResponse:
        status_code = 500
        request = None

        def json(self):
            return []

        def raise_for_status(self):
            raise httpx.HTTPStatusError("err", request=None, response=None)

    def fake_get(url, params=None, timeout=None):
        attempts["n"] += 1
        return DummyResponse()

    client = SimpleNamespace(get=fake_get)
    with pytest.raises(httpx.HTTPStatusError):
        backfill.fetch_klines(client, "https://x", "BTCUSDT", 0, 1000, limit=1, retries_5xx=2)
    # 1 intento inicial + 2 reintentos = 3
    assert attempts["n"] == 3


def test_timeout_retries_then_fail(monkeypatch):
    attempts = {"n": 0}

    def fake_get(url, params=None, timeout=None):
        attempts["n"] += 1
        raise httpx.TimeoutException("timeout")

    client = SimpleNamespace(get=fake_get)
    with pytest.raises(httpx.HTTPStatusError):
        backfill.fetch_klines(client, "https://x", "BTCUSDT", 0, 1000, limit=1, retries_5xx=1)
    assert attempts["n"] == 2  # initial + retry


def test_gap_zero_when_consecutive():
    events = [
        backfill.normalize_kline_row(
            "BTCUSDT", [0, "", "", "", "1", "1", 60_000]
        ),
        backfill.normalize_kline_row(
            "BTCUSDT", [60_001, "", "", "", "1", "1", 120_000]
        ),
    ]
    assert backfill._count_gaps(events, interval_ms=60_000) == 0


def test_gap_custom_interval():
    events = [
        backfill.normalize_kline_row(
            "BTCUSDT", [0, "", "", "", "1", "1", 300_000]
        ),
        backfill.normalize_kline_row(
            "BTCUSDT", [900_001, "", "", "", "1", "1", 1_200_000]
        ),
    ]
    assert backfill._count_gaps(events, interval_ms=300_000) == 1
