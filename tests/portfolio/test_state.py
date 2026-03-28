from datetime import datetime, timezone

from app.common.dto import ExecutionReport, PortfolioState
from app.portfolio.state import update_portfolio


def test_update_portfolio_buy_and_sell():
    ts = datetime.now(tz=timezone.utc)
    reports = [
        ExecutionReport(
            symbol="BTCUSDT",
            ts=ts,
            status="filled",
            filled_qty=1.0,
            avg_price=100.0,
            client_order_id="paper-buy",
        ),
        ExecutionReport(
            symbol="BTCUSDT",
            ts=ts,
            status="filled",
            filled_qty=0.5,
            avg_price=110.0,
            client_order_id="paper-sell",
        ),
    ]
    state = update_portfolio(reports, prev_state=None, cash_start=1000.0)
    assert state.positions["BTCUSDT"] == 0.5
    assert state.cash >= 1000.0 - 100.0 + 55.0 - 1e-9
