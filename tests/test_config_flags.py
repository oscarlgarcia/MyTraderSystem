from app.config import parse_args


def test_defaults_flags():
    args = parse_args([])
    assert args.mode == "dry"
    assert args.trace_steps is False
    assert args.features_after_ingest is False
    assert args.ingest_max_buffer == 10_000
    assert args.ingest_batch_size == 1
    assert args.ingest_lag_warn is None
    assert args.ingest_buffer_warn is None
    assert args.ingest_dedup is True


def test_features_after_ingest_flag():
    args = parse_args(["--features-after-ingest"])
    assert args.features_after_ingest is True


def test_trace_steps_flag():
    args = parse_args(["--trace-steps"])
    assert args.trace_steps is True


def test_ingest_batch_size_flag():
    args = parse_args(["--ingest-batch-size", "4"])
    assert args.ingest_batch_size == 4


def test_fast_path_flag():
    args = parse_args(["--fast-path"])
    assert args.fast_path is True


def test_ingest_warn_flags():
    args = parse_args(["--ingest-lag-warn", "1.5", "--ingest-buffer-warn", "3"])
    assert args.ingest_lag_warn == 1.5
    assert args.ingest_buffer_warn == 3
