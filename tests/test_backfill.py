import datetime as dt
import json
from types import SimpleNamespace

import httpx
import pytest

from app.ingestion import backfill
from app.ingestion.storage import normalized_partition_path, read_parquet
from app.marketdata.models import BarEvent, TradeEvent
from app.marketdata.replay import ReplaySource


class DummyResponse:
    def __init__(self, status_code: int, json_data):
        self.status_code = status_code
        self._json = json_data
        self.request = None

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


def test_fetch_trades_paginates_by_from_id():
    calls = []

    def fake_get(url, params=None, timeout=None):
        calls.append(dict(params))
        if "startTime" in params:
            return DummyResponse(
                200,
                [
                    {"a": 10, "p": "100", "q": "1", "f": 10, "l": 10, "T": 1000, "m": False, "M": True},
                    {"a": 11, "p": "101", "q": "1", "f": 11, "l": 11, "T": 1100, "m": True, "M": True},
                ],
            )
        return DummyResponse(
            200,
            [
                {"a": 12, "p": "102", "q": "1", "f": 12, "l": 12, "T": 1200, "m": False, "M": True},
                {"a": 13, "p": "103", "q": "1", "f": 13, "l": 13, "T": 2500, "m": False, "M": True},
            ],
        )

    client = SimpleNamespace(get=fake_get)
    rows = backfill.fetch_trades(client, "https://x", "BTCUSDT", 1000, 2000, limit=2)

    assert [row["a"] for row in rows] == [10, 11, 12]
    assert calls == [
        {"symbol": "BTCUSDT", "limit": 2, "startTime": 1000, "endTime": 2000},
        {"symbol": "BTCUSDT", "limit": 2, "fromId": 12},
    ]


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


def test_historical_backfill_scope_includes_kline_and_trade():
    assert backfill.HISTORICAL_BACKFILL_SCOPE == "bars-and-trades"
    assert backfill.SUPPORTED_HISTORICAL_BACKFILL_FEEDS == ("kline", "trade")
    assert backfill.supports_historical_backfill("kline") is True
    assert backfill.supports_historical_backfill("trade") is True


def test_trade_historical_backfill_is_supported_explicitly():
    backfill.assert_historical_backfill_support("trade")


def test_backfill_cli_help_declares_trade_support(capsys):
    with pytest.raises(SystemExit):
        backfill.parse_args(["--help"])

    out = capsys.readouterr().out
    assert "{kline,trade}" in out
    assert "aggTrades" in out


def test_429_retries_and_fails(monkeypatch):
    attempts = {"n": 0}

    def fake_get(url, params=None, timeout=None):
        attempts["n"] += 1
        return DummyResponse(429, [])

    client = SimpleNamespace(get=fake_get)
    with pytest.raises(httpx.HTTPStatusError):
        backfill.fetch_klines(client, "https://x", "BTCUSDT", 0, 1000, limit=1, retries_429=2)
    assert attempts["n"] == 3


def test_normalize_kline_row_utc():
    row = [0, "99", "101", "98", "100", "5", 60_000]
    ev = backfill.normalize_kline_row("BTCUSDT", row)
    assert isinstance(ev, BarEvent)
    assert ev.event_ts.tzinfo is not None
    assert ev.event_ts == dt.datetime.fromtimestamp(60, tz=dt.timezone.utc)
    assert ev.open == 99.0
    assert ev.high == 101.0
    assert ev.low == 98.0
    assert ev.close == 100.0
    assert ev.volume == 5.0
    assert ev.volume_kind == "quote"


def test_normalize_trade_row_utc_and_marks_historical_endpoint():
    row = {"a": 77, "p": "100.5", "q": "0.2", "f": 77, "l": 77, "T": 60_000, "m": False, "M": True}
    ev = backfill.normalize_trade_row("BTCUSDT", row)

    assert isinstance(ev, TradeEvent)
    assert ev.event_ts == dt.datetime.fromtimestamp(60, tz=dt.timezone.utc)
    assert ev.trade_id == "77"
    assert ev.source_id == "77"
    assert ev.price == 100.5
    assert ev.size == 0.2
    assert ev.metadata["historical_trade_endpoint"] == "aggTrades"
    assert ev.metadata["historical_trade_kind"] == "aggregate_trade"


def test_normalize_kline_row_prefers_quote_asset_volume_when_full_binance_row_is_available():
    row = [0, "99", "101", "98", "100", "5", 60_000, "250.5"]
    ev = backfill.normalize_kline_row("BTCUSDT", row)

    assert ev.volume == 250.5
    assert ev.volume_kind == "quote"


def test_backfill_kline_writes_and_idempotent(monkeypatch, tmp_path):
    cfg = SimpleNamespace(env="dev", data_dir=tmp_path, log_level="INFO", rest_base="https://x")
    rows = [
        [1704067200000, "0.9", "1.1", "0.8", "1", "10", 1704067260000],
        [1704067260001, "1.9", "2.1", "1.8", "2", "20", 1704067320000],
    ]

    monkeypatch.setattr(backfill, "load_config", lambda env=None: cfg)
    monkeypatch.setattr(backfill, "fetch_klines", lambda **kwargs: rows)

    backfill.run(["--env", "dev", "--symbol", "BTCUSDT", "--start", "2024-01-01T00:00:00+00:00", "--end", "2024-01-01T01:00:00+00:00", "--dedup"])
    backfill.run(["--env", "dev", "--symbol", "BTCUSDT", "--start", "2024-01-01T00:00:00+00:00", "--end", "2024-01-01T01:00:00+00:00", "--dedup"])

    path = normalized_partition_path(tmp_path, "dev", source="kline", symbol="BTCUSDT", day="2024-01-01")
    assert path.exists()
    table = read_parquet(path)
    assert table.num_rows == 2
    ts_list = table.column("event_ts").to_pylist()
    assert ts_list == sorted(ts_list)

    raw_files = list((tmp_path / "raw").glob("env=dev/venue=BINANCE/stream_type=kline/symbol=BTCUSDT/date=*/events.jsonl"))
    assert len(raw_files) == 1
    with raw_files[0].open("r", encoding="utf-8") as handle:
        raw_rows = [json.loads(line) for line in handle if line.strip()]
    assert [row["ingestion_seq"] for row in raw_rows] == [1, 2, 1, 2]
    assert len({row["run_id"] for row in raw_rows[:2]}) == 1
    assert len({row["run_id"] for row in raw_rows[2:]}) == 1
    assert raw_rows[0]["run_id"] != raw_rows[2]["run_id"]

    replayed = list(
        ReplaySource(
            base_dir=tmp_path / "raw",
            env="dev",
            symbol="BTCUSDT",
            stream_types=("kline",),
        ).stream()
    )
    assert len(replayed) == 4
    assert all(isinstance(event, BarEvent) for event in replayed)


def test_backfill_trade_writes_and_idempotent(monkeypatch, tmp_path):
    cfg = SimpleNamespace(env="dev", data_dir=tmp_path, log_level="INFO", rest_base="https://x")
    rows = [
        {"a": 101, "p": "100", "q": "1", "f": 101, "l": 101, "T": 1704067200000, "m": False, "M": True},
        {"a": 102, "p": "101", "q": "2", "f": 102, "l": 102, "T": 1704067201000, "m": True, "M": True},
    ]

    monkeypatch.setattr(backfill, "load_config", lambda env=None: cfg)
    monkeypatch.setattr(backfill, "fetch_trades", lambda **kwargs: rows)

    backfill.run([
        "--env", "dev", "--symbol", "BTCUSDT", "--feed-type", "trade", "--start", "2024-01-01T00:00:00+00:00", "--end", "2024-01-01T00:05:00+00:00", "--dedup"
    ])
    backfill.run([
        "--env", "dev", "--symbol", "BTCUSDT", "--feed-type", "trade", "--start", "2024-01-01T00:00:00+00:00", "--end", "2024-01-01T00:05:00+00:00", "--dedup"
    ])

    path = normalized_partition_path(tmp_path, "dev", source="trade", symbol="BTCUSDT", day="2024-01-01")
    assert path.exists()
    table = read_parquet(path)
    assert table.num_rows == 2
    rows_out = table.to_pylist()
    assert [row["trade_id"] for row in rows_out] == ["101", "102"]
    assert all(dict(row["metadata"])["historical_trade_endpoint"] == "aggTrades" for row in rows_out)

    raw_files = list((tmp_path / "raw").glob("env=dev/venue=BINANCE/stream_type=trade/symbol=BTCUSDT/date=*/events.jsonl"))
    assert len(raw_files) == 1
    with raw_files[0].open("r", encoding="utf-8") as handle:
        raw_rows = [json.loads(line) for line in handle if line.strip()]
    assert [row["payload"]["_backfill_endpoint"] for row in raw_rows] == ["aggTrades", "aggTrades", "aggTrades", "aggTrades"]

    replayed = list(
        ReplaySource(
            base_dir=tmp_path / "raw",
            env="dev",
            symbol="BTCUSDT",
            stream_types=("trade",),
        ).stream()
    )
    assert len(replayed) == 4
    assert all(isinstance(event, TradeEvent) for event in replayed)
    assert all(event.metadata["historical_trade_endpoint"] == "aggTrades" for event in replayed)


def test_backfill_dedup_drops_duplicates_and_logs(monkeypatch, tmp_path, capsys):
    cfg = SimpleNamespace(env="dev", data_dir=tmp_path, log_level="INFO", rest_base="https://x")
    rows = [
        [1704067200000, "1", "1", "1", "1", "10", 1704067260000],
        [1704067200000, "1", "1", "1", "1", "10", 1704067260000],
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
        [1704067200000, "1", "1", "1", "1", "10", 1704067260000],
        [1704067200000, "1", "1", "1", "1", "10", 1704067260000],
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


def test_gap_detection():
    events = [
        backfill.normalize_kline_row("BTCUSDT", [0, "1", "1", "1", "1", "1", 60_000]),
        backfill.normalize_kline_row("BTCUSDT", [120_000, "1", "1", "1", "1", "1", 180_000]),
    ]
    assert backfill._count_bar_gaps(events, interval_ms=60_000) == 1


def test_dry_run_creates_no_files(monkeypatch, tmp_path):
    cfg = SimpleNamespace(env="dev", data_dir=tmp_path, log_level="INFO", rest_base="https://x")
    rows = [
        [1704067200000, "1", "1", "1", "1", "10", 1704067260000],
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
                "2m",
            ]
        )


def test_http_500_retries_then_fail(monkeypatch):
    attempts = {"n": 0}

    class ErrorResponse:
        status_code = 500
        request = None

        def json(self):
            return []

        def raise_for_status(self):
            raise httpx.HTTPStatusError("err", request=None, response=None)

    def fake_get(url, params=None, timeout=None):
        attempts["n"] += 1
        return ErrorResponse()

    client = SimpleNamespace(get=fake_get)
    with pytest.raises(httpx.HTTPStatusError):
        backfill.fetch_klines(client, "https://x", "BTCUSDT", 0, 1000, limit=1, retries_5xx=2)
    assert attempts["n"] == 3


def test_timeout_retries_then_fail(monkeypatch):
    attempts = {"n": 0}

    def fake_get(url, params=None, timeout=None):
        attempts["n"] += 1
        raise httpx.TimeoutException("timeout")

    client = SimpleNamespace(get=fake_get)
    with pytest.raises(httpx.HTTPStatusError):
        backfill.fetch_klines(client, "https://x", "BTCUSDT", 0, 1000, limit=1, retries_5xx=1)
    assert attempts["n"] == 2


def test_gap_zero_when_consecutive():
    events = [
        backfill.normalize_kline_row("BTCUSDT", [0, "1", "1", "1", "1", "1", 60_000]),
        backfill.normalize_kline_row("BTCUSDT", [60_001, "1", "1", "1", "1", "1", 120_000]),
    ]
    assert backfill._count_bar_gaps(events, interval_ms=60_000) == 0


def test_gap_custom_interval():
    events = [
        backfill.normalize_kline_row("BTCUSDT", [0, "1", "1", "1", "1", "1", 300_000]),
        backfill.normalize_kline_row("BTCUSDT", [900_001, "1", "1", "1", "1", "1", 1_200_000]),
    ]
    assert backfill._count_bar_gaps(events, interval_ms=300_000) == 1
