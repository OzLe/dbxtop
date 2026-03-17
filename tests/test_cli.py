"""CLI smoke tests for dbxtop."""

from dbxtop.cli import main


def test_main_is_callable():
    """Verify the CLI entry point is importable and callable."""
    assert callable(main)
