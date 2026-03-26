def test_package_imports():
    # Import all top-level packages to ensure they are discoverable.
    import app.common  # noqa: F401
    import app.ingestion  # noqa: F401
    import app.features  # noqa: F401
    import app.strategy  # noqa: F401
    import app.risk  # noqa: F401
    import app.execution  # noqa: F401
    import app.portfolio  # noqa: F401
    import app.observability  # noqa: F401
    import app.ops  # noqa: F401


def test_python_m_app_runs():
    # Ensures `python -m app` executes without ImportError.
    import runpy

    result = runpy.run_module("app", run_name="__main__")
    assert "__name__" in result
