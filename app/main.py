"""
Entry point for the trading system (stub).

The goal of this module in Fase 1.1 is to verify that the toolchain
boots correctly. No domain logic or external IO is performed here.
"""

from __future__ import annotations

# Import packages to ensure availability and detect missing modules early.
# No side effects or heavy logic should be triggered here.
from app import common, execution, features, ingestion, observability, ops, portfolio, risk, strategy  # noqa: F401
from app.config import load_config, parse_args
from app.observability.logger import get_logger, set_trace_id


def run() -> int:
    """Basic bootstrap stub returning zero for success."""
    args = parse_args()
    config = load_config(args.env)
    logger = get_logger(level=config.log_level)
    # Demonstrate DTO usage in a minimal, non-I/O way.
    _ = common.TraceContext(trace_id="bootstrap")
    set_trace_id("bootstrap")
    logger.info("pipeline stub ok", extra={"env": config.env, "data_dir": str(config.data_dir)})
    return 0


if __name__ == "__main__":
    raise SystemExit(run())
