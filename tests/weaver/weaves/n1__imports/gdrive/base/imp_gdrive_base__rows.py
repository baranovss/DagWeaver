"""Source step — no upstream dependencies.

Operator chain: LatestOnlyOperator >> PythonOperator
"""

from dagweaver import weave
from airflow.providers.standard.operators.latest_only import LatestOnlyOperator
from airflow.providers.standard.operators.python import PythonOperator


def _import():
    return "imp_gdrive_base__rows_payload"


@weave(
    group=["n1__imports", "imp_gdrive", "base"]
)
def imp_gdrive_base__rows(dag, ctx):
    filename = ctx.filename
    
    latest = LatestOnlyOperator(
        task_id=f"{filename}_latest_only",
        dag=dag,
        doc_md="""\
## LatestOnly Guard

Пропускает **только последний** DAG run.

> Все предыдущие (backfill) запуски будут пропущены автоматически.

### Зачем?
- Предотвращает повторную загрузку исторических данных
- Экономит квоту Google Drive API

| Параметр | Значение |
|----------|----------|
| Стратегия | skip non-latest |
| Downstream | `imp_gdrive_base__rows` |
""",
    )

    process = PythonOperator(
        task_id=filename,
        dag=dag,
        python_callable=_import,
        doc_md="""\
## Import: Google Drive Base Rows

Загружает базовые строки из **Google Drive** spreadsheet.

### Источник
- Google Drive folder: `shared/analytics/raw/`
- Формат: CSV (`;` separator, UTF-8)

### Логика
1. Авторизация через сервисный аккаунт
2. Скачивание файла по `file_id`
3. Парсинг и базовая валидация схемы

### XCom output
Результат пушится в XCom и доступен downstream-шагам через:
```python
xcom={"payload": "imp_gdrive_base__rows"}
```

### Контакты
- **Owner**: `@data-team`
- **SLA**: 15 min

---
*Последнее обновление документации: 2026-03*
""",
        doc_yaml="""\
task: imp_gdrive_base__rows
layer: imports
source:
  type: google_drive
  folder: shared/analytics/raw/
  format: csv
  encoding: utf-8
  separator: ";"
owner: "@data-team"
sla_minutes: 15
xcom_output:
  key: return_value
  consumers:
    - imp_gdrive__row_data
schema:
  columns:
    - name: id
      type: integer
    - name: event_date
      type: date
    - name: payload
      type: string
tags:
  - gdrive
  - base
  - import
""",
    )

    latest >> process

    return [latest, process]
