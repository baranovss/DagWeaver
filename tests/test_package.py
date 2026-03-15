"""Package-level tests."""

from dagweaver import __version__


def test_version():
    """Version is exposed."""
    assert __version__ == "0.1.0"
