from datetime import datetime, timezone

from app.common.dto import Signal
from app.risk.rules import apply_risk


def test_apply_risk_caps_notional_and_limits_symbols():
    ts = datetime.now(tz=timezone.utc)
    signals = [
        Signal(symbol="BTCUSDT", ts=ts, side="buy", size=10, confidence=0.9),
        Signal(symbol="ETHUSDT", ts=ts, side="sell", size=5, confidence=0.8),
        Signal(symbol="XRPUSDT", ts=ts, side="buy", size=1, confidence=0.7),
    ]
    price_map = {"BTCUSDT": 100, "ETHUSDT": 50, "XRPUSDT": 1}
    intents = apply_risk(signals, price_by_symbol=price_map, max_notional=200, max_symbols=2)
    assert len(intents) == 2
    btc = next(i for i in intents if i.symbol == "BTCUSDT")
    assert round(btc.quantity * price_map["BTCUSDT"], 2) <= 200
