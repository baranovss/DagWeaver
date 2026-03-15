"""Source step — no upstream dependencies.

Operator chain: LatestOnlyOperator >> PythonOperator
"""

from dagweaver import weave
from airflow.providers.standard.operators.latest_only import LatestOnlyOperator
from airflow.providers.standard.operators.python import PythonOperator


def _import(payload):
    return "imp_gdrive__row_data_payload + " + payload


@weave(
    group=["n1__imports", "imp_gdrive"],
    xcom={"payload": "imp_gdrive_base__rows"},
)
def imp_gdrive__row_data(dag, ctx, payload):
    filename = ctx.filename

    latest = LatestOnlyOperator(
        task_id=f"{filename}_latest_only",
        dag=dag,
    )

    process = PythonOperator(
        task_id=filename,
        dag=dag,
        python_callable=_import,
        op_kwargs={"payload": payload},
    )

    latest >> process

    return [latest, process]
