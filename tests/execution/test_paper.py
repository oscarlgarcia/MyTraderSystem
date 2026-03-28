from datetime import datetime, timezone

from app.common.dto import OrderIntent
from app.execution.paper import paper_execute


def test_paper_execute_fills_with_price():
    ts = datetime.now(tz=timezone.utc)
    intents = [OrderIntent(symbol="BTCUSDT", ts=ts, side="buy", quantity=1.0)]
    price_map = {"BTCUSDT": 100.0}
    reports = paper_execute(intents, price_by_symbol=price_map)
    assert reports[0].status == "filled"
    assert reports[0].avg_price == 100.0


def test_paper_execute_rejects_without_price():
    ts = datetime.now(tz=timezone.utc)
    intents = [OrderIntent(symbol="ETHUSDT", ts=ts, side="sell", quantity=1.0)]
    reports = paper_execute(intents, price_by_symbol={})
    assert reports[0].status == "rejected"
