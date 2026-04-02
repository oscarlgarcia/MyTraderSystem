from __future__ import annotations

import argparse
import json
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run vendor network contract tests and persist a JSON artifact")
    parser.add_argument(
        "--output",
        default="docs/validation/ingestion_vendor_contracts.json",
    )
    parser.add_argument(
        "--pytest-target",
        default="tests/network/test_binance_contracts.py",
    )
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    command = [sys.executable, "-m", "pytest", args.pytest_target, "-q", "-m", "network"]
    result = subprocess.run(command, capture_output=True, text=True, check=False)
    payload = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "command": command,
        "returncode": result.returncode,
        "pass_ok": result.returncode == 0,
        "stdout": result.stdout,
        "stderr": result.stderr,
    }
    output = Path(args.output)
    output.parent.mkdir(parents=True, exist_ok=True)
    output.write_text(json.dumps(payload, ensure_ascii=False, indent=2), encoding="utf-8")
    return result.returncode


if __name__ == "__main__":
    raise SystemExit(main())
