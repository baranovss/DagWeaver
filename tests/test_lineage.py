"""Tests for SQL lineage extraction."""

import pytest

from dagweaver.lineage import extract_sql_sources, extract_upstream_from_sql


@pytest.mark.parametrize(
    "sql, expected",
    [
        (
            "select * from ${catalog}.${schema}.imp_google_drive__marketplace_deals",
            ["imp_google_drive__marketplace_deals"],
        ),
        (
            "select t1.*, t2.col from ${schema}.imp_table t1 "
            "join ${catalog}.${schema}.imp_table2 t2 on t1.id = t2.id",
            ["imp_table", "imp_table2"],
        ),
        (
            "select * from hive.sbaranov.imp_table",
            [],
        ),
        (
            "select * from ${catalog}.${schema}.imp_table where ds = '${ds}'",
            ["imp_table"],
        ),
        (
            "select t1.*, t2.col from ${schema}.imp_table t1 "
            "join sbaranov.imp_table2 t2 on t1.id = t2.id",
            ["imp_table"],
        ),
        (
            "select t1.*, t2.col from hive.${schema}.imp_table t1 "
            "join sbaranov.imp_table2 t2 on t1.id = t2.id",
            ["imp_table"],
        ),
        (
            """SELECT
                    requirement_key
                FROM ${catalog}.${schema}.imp_mongo__submission_dto
                CROSS JOIN UNNEST(CAST(ARRAY[] AS array(row(key varchar, value json)))) AS t(requirement_key, requirement_value_json)""",
            ["imp_mongo__submission_dto"],
        ),
    ],
)
def test_extract_upstream_select_cases(sql, expected):
    assert extract_upstream_from_sql(sql) == expected


@pytest.mark.parametrize(
    "sql",
    [
        "update hive.sbaranov.some_table set col = 1 where id = 10",
        "delete from hive.sbaranov.some_table where id = 10",
        "create table hive.sbaranov.new_table as select * from ${catalog}.${schema}.imp_table",
        "insert into hive.sbaranov.some_table select * from ${catalog}.${schema}.imp_table",
        "drop table hive.sbaranov.some_table",
        "alter table hive.sbaranov.some_table add column x int",
    ],
)
def test_extract_upstream_non_select(sql):
    assert extract_upstream_from_sql(sql) == []


@pytest.mark.parametrize(
    "sql, expected",
    [
        (
            "select * from ${schema}.imp_table union all select * from ${schema}.imp_table2",
            ["imp_table", "imp_table2"],
        ),
        (
            "select * from (select * from ${catalog}.${schema}.imp_sub) sub",
            ["imp_sub"],
        ),
        (
            "with imp_table as (select 1) "
            "select * from imp_table join ${schema}.imp_table2 t on 1=1",
            ["imp_table2"],
        ),
        (
            "select * from catalog.schema.my_table",
            [],
        ),
    ],
)
def test_extract_upstream_corner_cases(sql, expected):
    assert extract_upstream_from_sql(sql) == expected


def test_extract_upstream_values_only():
    sql = "SELECT 1 FROM (VALUES (1, 2)) AS t(a, b)"
    assert extract_upstream_from_sql(sql) == []


def test_extract_upstream_with_custom_dialect():
    sql = "SELECT * FROM hive.${schema}.source_table"
    assert extract_upstream_from_sql(sql, dialect="spark") == ["source_table"]


@pytest.mark.parametrize(
    "sql",
    [
        "update hive.sbaranov.some_table set col = 1 where id = 10",
        "delete from hive.sbaranov.some_table where id = 10",
        "create table hive.sbaranov.new_table as select * from ${catalog}.${schema}.imp_table",
        "insert into hive.sbaranov.some_table select * from ${catalog}.${schema}.imp_table",
    ],
)
def test_extract_sql_sources_non_select(sql):
    assert extract_sql_sources(sql, "hive", "sbaranov") == []


@pytest.mark.parametrize(
    "sql, catalog, schema, expected",
    [
        (
            "select * from ${catalog}.${schema}.imp_table",
            "hive",
            "sbaranov",
            ["hive.sbaranov.imp_table"],
        ),
        (
            "select * from ${schema}.imp_table",
            "hive",
            "sbaranov",
            ["hive.sbaranov.imp_table"],
        ),
        (
            "select t1.*, t2.col from ${schema}.imp_table t1 join sbaranov.imp_table2 t2 on t1.id = t2.id",
            "hive",
            "sbaranov",
            [
                "hive.sbaranov.imp_table2",
                "hive.sbaranov.imp_table",
            ],
        ),
        (
            "WITH tmp AS (select * from hive.sbaranov.src) select * from tmp",
            "hive",
            "sbaranov",
            ["hive.sbaranov.src"],
        ),
    ],
)
def test_extract_sql_sources(sql, catalog, schema, expected):
    assert set(extract_sql_sources(sql, catalog, schema)) == set(expected)


@pytest.mark.parametrize(
    "sql, catalog, schema, expected",
    [
        (
            "select * from ${schema}.dup union select * from ${schema}.dup",
            "hive",
            "sbaranov",
            ["hive.sbaranov.dup"],
        ),
        (
            "select * from ${catalog}.sbaranov.imp_cat",
            "hive",
            "sbaranov",
            ["hive.sbaranov.imp_cat"],
        ),
        (
            "select * from (select * from hive.sbaranov.nested) n",
            "hive",
            "sbaranov",
            ["hive.sbaranov.nested"],
        ),
        (
            "with t as (select * from hive.sbaranov.src) select * from t join ${schema}.ext on 1=1",
            "hive",
            "sbaranov",
            ["hive.sbaranov.ext", "hive.sbaranov.src"],
        ),
        (
            "select * from catalog.schema.lit_table",
            "hive",
            "sbaranov",
            ["catalog.schema.lit_table"],
        ),
    ],
)
def test_extract_sql_sources_corner(sql, catalog, schema, expected):
    assert set(extract_sql_sources(sql, catalog, schema)) == set(expected)


@pytest.mark.parametrize(
    "sql",
    [
        "select * from ${schema}.imp_table",
        (
            "select t1.*, t2.col from ${schema}.imp_table t1 "
            "join ${catalog}.${schema}.imp_table2 t2 on t1.id = t2.id"
        ),
        (
            "with imp_table as (select 1) "
            "select * from imp_table join ${schema}.imp_table2 t on 1=1"
        ),
    ],
)
def test_extract_upstream_adapter_matches_core(sql):
    core_sources = extract_sql_sources(sql, "default", "default")
    original_lower = sql.lower()
    expected = sorted(
        {
            source.split(".")[-1]
            for source in core_sources
            if f"${{schema}}.{source.split('.')[-1].lower()}" in original_lower
        }
    )

    assert extract_upstream_from_sql(sql) == expected


def test_parse_error_returns_empty_lineage():
    bad_sql = "SELECT FROM ??? ???"

    assert extract_sql_sources(bad_sql, "hive", "sbaranov") == []
    assert extract_upstream_from_sql(bad_sql) == []
