from datetime import datetime, timezone
from app.common.dto import FeatureVector
from app.strategy.basic import generate_signals


def test_generate_signals_buy_sell_flat():
    base_ts = datetime.now(tz=timezone.utc)
    f_buy = FeatureVector(symbol="BTCUSDT", ts=base_ts, values={"price": 102, "sma_3": 100, "ret_1": 0.02})
    f_sell = FeatureVector(symbol="ETHUSDT", ts=base_ts, values={"price": 98, "sma_3": 100, "ret_1": -0.03})
    f_flat = FeatureVector(symbol="XRPUSDT", ts=base_ts, values={"price": 100, "sma_3": 100, "ret_1": 0.0})

    signals = generate_signals([f_buy, f_sell, f_flat])
    sides = {s.symbol: s.side for s in signals}
    assert sides["BTCUSDT"] == "buy"
    assert sides["ETHUSDT"] == "sell"
    assert sides["XRPUSDT"] == "flat"
