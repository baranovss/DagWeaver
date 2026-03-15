"""Shared pytest fixtures/helpers for DagWeaver tests."""

from __future__ import annotations

import sys
from types import ModuleType

import pytest


@pytest.fixture
def fake_pkg() -> ModuleType:
    """Synthetic package module for package-scanning tests."""
    pkg = ModuleType("fake_pkg")
    pkg.__path__ = ["/virtual"]
    pkg.__name__ = "fake_pkg"
    return pkg


@pytest.fixture
def patch_single_module_scan(monkeypatch):
    """Patch weaves_builder package scanning to return a single module."""

    def _patch(module):
        monkeypatch.setattr(
            "dagweaver.builders.pkgutil.walk_packages",
            lambda *_args, **_kwargs: [("", "fake_pkg.any_module", False)],
        )
        monkeypatch.setattr(
            "dagweaver.builders.importlib.import_module",
            lambda _name: module,
        )

    return _patch


@pytest.fixture
def install_fake_airflow_python_operator(monkeypatch):
    """Install fake airflow modules exposing provided PythonOperator class."""

    def _install(operator_cls):
        airflow_mod = ModuleType("airflow")
        operators_mod = ModuleType("airflow.operators")
        python_mod = ModuleType("airflow.operators.python")
        python_mod.PythonOperator = operator_cls

        monkeypatch.setitem(sys.modules, "airflow", airflow_mod)
        monkeypatch.setitem(sys.modules, "airflow.operators", operators_mod)
        monkeypatch.setitem(sys.modules, "airflow.operators.python", python_mod)

    return _install
