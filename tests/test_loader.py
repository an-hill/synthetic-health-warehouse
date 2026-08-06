"""Smoke tests for the package layout."""

import importlib


def test_loader_package_imports() -> None:
    """The loader package is installed and importable."""
    assert importlib.import_module("loader") is not None
