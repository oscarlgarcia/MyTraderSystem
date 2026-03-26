"""
Entry point for `python -m app`.

Delegates to app.main.run to keep the executable path consistent.
"""

from __future__ import annotations

from app.main import run


if __name__ == "__main__":
    raise SystemExit(run())
