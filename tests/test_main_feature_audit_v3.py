from datetime import datetime, timezone

import pytest

from app.common.dto import MarketEvent
from app.main import run_trading_cycle
from app.observability.logger import get_logger


def test_run_trading_cycle_requires_feature_audit_outside_dry_mode():
    event = MarketEvent(
        symbol="BTCUSDT",
        event_ts=datetime(2024, 1, 1, tzinfo=timezone.utc),
        available_ts=datetime(2024, 1, 1, 0, 0, 1, tzinfo=timezone.utc),
        price=100.0,
        size=1.0,
        source="trade",
    )
    with pytest.raises(ValueError, match="feature_audit_path"):
        run_trading_cycle([event], logger=get_logger(level="INFO"), mode="live")
