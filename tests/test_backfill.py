import datetime as dt
from types import SimpleNamespace

import httpx
import pytest

from app.ingestion import backfill


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


def test_429_retries_and_fails(monkeypatch):
    attempts = {"n": 0}

    def fake_get(url, params=None, timeout=None):
        attempts["n"] += 1
        return DummyResponse(429, [])

    client = SimpleNamespace(get=fake_get)
    with pytest.raises(httpx.HTTPStatusError):
        backfill.fetch_klines(client, "https://x", "BTCUSDT", 0, 1000, limit=1, max_retries=2)
    assert attempts["n"] == 2


def test_normalize_kline_row_utc():
    row = [0, "", "", "", "100", "5", 60_000]  # close time 60s
    ev = backfill.normalize_kline_row("BTCUSDT", row)
    assert ev.event_ts.tzinfo is not None
    assert ev.event_ts == dt.datetime.fromtimestamp(60, tz=dt.timezone.utc)
