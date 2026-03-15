"""Test DAG — scans weaves package, groups by group."""

from __future__ import annotations

from datetime import datetime

from airflow import DAG

import tests.weaver.weaves as weaves_pkg
from dagweaver.builders import build_dag, build_weaves

with DAG(
    dag_id="weaver_test_dag",
    start_date=datetime(2026, 1, 1),
    schedule=None,
    catchup=False,
) as dag:
    weaves = build_weaves(weaves_pkg)
    build_dag(dag, weaves)
