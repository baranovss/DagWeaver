# DagWeaver

Library for Apache Airflow DAG development. `@weave` decorator marks pipeline steps.

## Install

```bash
pip install dagweaver[airflow]
```

## Usage

**Weaves** — one `@weave` per file in `weaves/`:

```
my_project/
  weaves/
    raw_data.py      # @weave def raw_data(): ...
    processed.py     # @weave(upstream=["raw_data"]) def processed(data): ...
  dags/
    my_dag.py        # DAG definition (below)
```

`weaves/raw_data.py`:

```python
from dagweaver import weave

@weave
def raw_data():
    return "data"
```

`weaves/processed.py`:

```python
from dagweaver import weave

@weave(upstream=["raw_data"])
def processed(data):
    return data.upper()
```

`dags/my_dag.py`:

```python
from airflow import DAG
from dagweaver import build_weaves, build_dag
import my_project.weaves as weaves_pkg

with DAG("my_dag", start_date=..., schedule=None) as dag:
    weaves = build_weaves(weaves_pkg)
    build_dag(dag, weaves)
```

## Dev

```bash
pip install -e ".[dev]"
pytest
```
