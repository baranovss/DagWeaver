"""Cleaning step — depends on raw_data.

Operator chain: LatestOnlyOperator >> PythonOperator
"""

from dagweaver import weave
from airflow.providers.standard.operators.latest_only import LatestOnlyOperator
from airflow.providers.standard.operators.python import PythonOperator


def _clean():
    return "stg_gdrive__row_data_cleaned_payload"


@weave(
    upstream=["imp_gdrive__row_data"],
    group=["n2__staging", "stg_gdrive"],
)
def stg_gdrive__row_data(dag, ctx):
    filename = ctx.filename
    latest = LatestOnlyOperator(task_id=f"{filename}_latest_only", dag=dag)
    process = PythonOperator(task_id=filename, dag=dag, python_callable=_clean)

    latest >> process

    return [latest, process]
