import json
from app import main
from app.observability.logger import get_logger


def test_run_cycle_order():
    recorder = []
    main.run_cycle(recorder)
    assert recorder == ["ingestion", "features", "strategy", "risk", "execution", "portfolio"]


def test_run_returns_zero(monkeypatch, capsys):
    # Use a logger that writes to StringIO to capture output.
    outputs = []

    class _Stream:
        def write(self, msg):
            outputs.append(msg)
        def flush(self):
            pass

    stream = _Stream()
    monkeypatch.setattr(main, "get_logger", lambda level=None: get_logger(stream=stream, level="INFO"))

    rc = main.run()
    assert rc == 0
    log_lines = [l for l in outputs if l.strip()]
    assert log_lines, "expected at least one log line"
    payload = json.loads(log_lines[0])
    assert payload["message"] == "pipeline stub ok"
    assert payload["trace_id"]
    assert payload["steps"] == ["ingestion", "features", "strategy", "risk", "execution", "portfolio"]
    assert payload["env"] == "dev"
