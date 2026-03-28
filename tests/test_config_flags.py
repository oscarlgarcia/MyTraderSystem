from app.config import parse_args


def test_defaults_flags():
    args = parse_args([])
    assert args.mode == "dry"
    assert args.trace_steps is False
    assert args.features_after_ingest is False
    assert args.ingest_max_buffer == 10_000
    assert args.ingest_dedup is True


def test_features_after_ingest_flag():
    args = parse_args(["--features-after-ingest"])
    assert args.features_after_ingest is True


def test_trace_steps_flag():
    args = parse_args(["--trace-steps"])
    assert args.trace_steps is True
