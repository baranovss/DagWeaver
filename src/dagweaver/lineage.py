"""SQL lineage extraction for DagWeaver assets."""

from __future__ import annotations

import logging
import re
from typing import Optional

import sqlglot
from sqlglot import exp


def extract_upstream_from_sql(sql_query: str, dialect: str = "trino") -> list[str]:
    """Extract upstream asset names from an SQL query.

    Analyzes the query and returns table names (without catalog/schema)
    that qualify as upstream dependencies. Only tables whose schema
    part uses the ${schema} placeholder are considered, e.g.
    '${catalog}.${schema}.imp_table' -> 'imp_table'.

    Args:
        sql_query: Raw SQL string, possibly with ${catalog}, ${schema} placeholders.
        dialect: SQL dialect for sqlglot parsing (e.g. "trino", "spark", "postgres").

    Returns:
        Sorted list of upstream asset names.
    """
    if not sql_query or not sql_query.strip():
        return []

    sources_fqn = extract_sql_sources(
        sql_query,
        catalog="default",
        schema="default",
        dialect=dialect,
    )
    original_lower = sql_query.lower()

    upstream = {
        tbl.split(".")[-1]
        for tbl in sources_fqn
        if f"${{schema}}.{tbl.split('.')[-1].lower()}" in original_lower
    }

    return sorted(upstream)


def _sanitize_sql(sql_text: str) -> str:
    """Replace ${placeholder} with parseable markers for sqlglot."""
    text = sql_text.replace("${catalog}", "__CATALOG__").replace("${schema}", "__SCHEMA__")
    return re.sub(r"\$\{([a-zA-Z0-9_]+)\}", r"\1", text)


def _parse_sql(
    sql_text: str,
    dialect: str = "trino",
    *,
    sanitize: bool = True,
) -> Optional[exp.Expression]:
    """Parse SQL with sqlglot. Returns AST or None if parsing failed."""
    sql_to_parse = _sanitize_sql(sql_text) if sanitize else sql_text
    try:
        return sqlglot.parse_one(sql_to_parse, dialect=dialect, error_level="ignore")
    except Exception as exc:
        logging.warning("[dagweaver.lineage] Unable to parse SQL: %s", exc)
        return None


def _collect_sources_fqn(parsed: exp.Expression, catalog: str, schema: str) -> set[str]:
    """Walk parsed AST and return set of fully-qualified table names."""
    if isinstance(parsed, (exp.Insert, exp.Update, exp.Delete, exp.Create, exp.Drop, exp.Alter)):
        return set()

    cte_aliases = {cte.alias for cte in parsed.find_all(exp.CTE) if cte.alias}

    def table_to_fqn(table_expr: exp.Table) -> str:
        tbl_catalog = getattr(table_expr, "catalog", None) or catalog
        tbl_schema = getattr(table_expr, "db", None) or schema
        tbl_name = getattr(table_expr, "name", None)
        if not tbl_name:
            return ""

        tbl_catalog = str(tbl_catalog).replace("__CATALOG__", catalog).replace("__SCHEMA__", schema)
        tbl_schema = str(tbl_schema).replace("__CATALOG__", catalog).replace("__SCHEMA__", schema)
        tbl_name = str(tbl_name).replace("__CATALOG__", catalog).replace("__SCHEMA__", schema)
        return f"{tbl_catalog}.{tbl_schema}.{tbl_name}"

    sources: set[str] = set()
    for table in parsed.find_all(exp.Table):
        if table.name in cte_aliases and not table.db:
            continue
        fqn = table_to_fqn(table)
        if fqn:
            sources.add(fqn)

    return sources


def extract_sql_sources(
    sql_text: str,
    catalog: str,
    schema: str,
    *,
    dialect: str = "trino",
) -> list[str]:
    """Extract fully-qualified source tables from SQL.

    Args:
        sql_text: Raw SQL string (supports ${catalog}/${schema} placeholders).
        catalog: Default catalog to use when missing.
        schema: Default schema to use when missing.
        dialect: SQL dialect for sqlglot parsing.

    Returns:
        Sorted unique list of table names in `catalog.schema.table` format.
        Returns empty list for non-SELECT statements or unparsable SQL.
    """
    parsed = _parse_sql(sql_text, dialect=dialect, sanitize=True)
    if parsed is None:
        return []

    return sorted(_collect_sources_fqn(parsed, catalog, schema))
