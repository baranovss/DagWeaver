# feat: Ядро DagWeaver — weave, builders, lineage

## Что сделано

- **@weave** — декоратор для пометки шагов пайплайна
- **build_weaves** — обход пакета и сбор weaves из функций с @weave
- **build_dag** — построение Airflow DAG из weaves (TaskGroup, upstream, xcom)
- **SQL lineage** — `extract_sql_sources`, `extract_upstream_from_sql` для извлечения зависимостей из SQL
- **Ленивый импорт** — `build_dag`/`build_weaves` загружаются только при обращении (airflow опционален)
- **pyproject** — classifiers, urls, pytest.ini_options, ruff
- **README** — минимальное описание, структура weaves/dags
- **Тесты** — lineage, weaver-фикстуры, скрипты Airflow
