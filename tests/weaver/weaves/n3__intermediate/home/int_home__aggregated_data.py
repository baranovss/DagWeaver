"""Aggregation step — depends on cleaned_data.

Operator chain: LatestOnlyOperator >> BranchPythonOperator >> PythonOperator
"""

from dagweaver import weave
from airflow.providers.standard.operators.latest_only import LatestOnlyOperator
from airflow.providers.standard.operators.python import BranchPythonOperator, PythonOperator


def _aggregate():
    return "aggregated_data_payload"


@weave(
    upstream=["stg_gdrive__row_data"],
    group=["n3__intermediate", "int_home"],
)
def int_home__aggregated_data(dag, ctx):
    filename = ctx.filename
    latest = LatestOnlyOperator(task_id=f"{filename}_latest_only", dag=dag)
    process = PythonOperator(task_id=filename, dag=dag, python_callable=_aggregate)
    branch = BranchPythonOperator(
        task_id=f"{filename}_branch", dag=dag,
        python_callable=lambda: process.task_id,
    )

    latest >> branch >> process

    return [latest, branch, process]
