from app import main


def test_run_exits_zero():
    assert main.run() == 0

