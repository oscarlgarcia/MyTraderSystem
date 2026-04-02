from app.marketdata.anomaly_checks import detect_price_jump



def test_detect_price_jump_returns_none_without_previous_price():
    assert detect_price_jump(previous_price=None, current_price=100.0) is None



def test_detect_price_jump_detects_large_relative_move():
    anomaly = detect_price_jump(previous_price=100.0, current_price=130.0, relative_jump_threshold=0.2)
    assert anomaly is not None
    assert anomaly.anomaly_type == "price_jump"
    assert anomaly.relative_jump >= 0.3
