"""Source step — no upstream dependencies.

Operator chain: LatestOnlyOperator >> PythonOperator
"""

from dagweaver import weave
from airflow.providers.standard.operators.latest_only import LatestOnlyOperator
from airflow.providers.standard.operators.python import PythonOperator


def _extract():
    return "raw_data_payload"


@weave(group=["n1__imports", "imp_gdrive"])
def imp_gdrive__row_data2(dag, ctx):
    filename = ctx.filename
    latest = LatestOnlyOperator(task_id=f"{filename}_latest_only", dag=dag)
    process = PythonOperator(task_id=filename, dag=dag, python_callable=_extract)

    latest >> process

    return [latest, process]
