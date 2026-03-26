"""
Entry point for the trading system (stub).

The goal of this module in Fase 1.1 is to verify that the toolchain
boots correctly. No domain logic or external IO is performed here.
"""

from __future__ import annotations

# Import packages to ensure availability and detect missing modules early.
# No side effects or heavy logic should be triggered here.
from app import common, execution, features, ingestion, observability, ops, portfolio, risk, strategy  # noqa: F401


def run() -> int:
    """Basic bootstrap stub returning zero for success."""
    # Demonstrate DTO usage in a minimal, non-I/O way.
    _ = common.TraceContext(trace_id="bootstrap")
    print("pipeline stub ok")
    return 0


if __name__ == "__main__":
    raise SystemExit(run())
