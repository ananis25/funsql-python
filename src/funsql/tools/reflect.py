"""
This module implements schema introspection for the common SQL databases, so we 
don't have to construct the SQLCatalog by hand. 
"""

from typing import Optional

from ..common import S
from ..sqlcontext import SQLTable

__all__ = [
    "reflect_default",
    "reflect_sqlite",
    "reflect_mysql",
    "reflect_postgres",
    "make_sql_tables",
]


REFLECT_POSTGRES = """
SELECT
    n.nspname AS schema_,
    c.relname AS name_,
    a.attname AS column_
FROM
    pg_catalog.pg_namespace AS n
JOIN 
    pg_catalog.pg_class AS c 
    ON c.relnamespace = n.oid
JOIN
    pg_catalog.pg_attribute AS a
    ON a.attrelid = c.oid
WHERE
    n.nspname = {schema}
    AND 
    c.relkind IN ('r', 'v')
    AND
    a.attnum > 0
    AND
    NOT a.attisdropped
ORDER BY
    n.nspname, 
    c.relname, 
    a.attnum;
"""


REFLECT_SQLITE = """
SELECT 
    null AS schema_, 
    sm.name AS name_, 
    pti.name AS column_
FROM 
    sqlite_master AS sm
JOIN 
    pragma_table_info(sm.name) AS pti
    ON TRUE
WHERE 
    sm.type IN ('table', 'view') 
    AND 
    NOT sm.name LIKE 'sqlite_%'
ORDER BY 
    sm.name, 
    pti.cid;
"""


REFLECT_DEFAULT = """
SELECT
    c.table_schema AS schema_, 
    c.table_name AS name_,
    c.column_name AS column_
FROM 
    information_schema.columns AS c
WHERE
    c.table_schema = {schema}
ORDER BY
    c.table_schema,
    c.table_name,
    c.ordinal_position;
"""


def reflect_default(schema: str) -> str:
    return REFLECT_DEFAULT.format(schema=f"'{schema}'")


def reflect_postgres(schema: str = "public") -> str:
    return REFLECT_POSTGRES.format(schema=f"'{schema}'")


def reflect_sqlite() -> str:
    return REFLECT_SQLITE


def reflect_mysql(schema: Optional[str] = None) -> str:
    return REFLECT_DEFAULT.format(
        schema="DATABASE()" if schema is None else f"'{schema}'"
    )


def make_sql_tables(
    reflect_result: list[tuple[Optional[str], str, str]]
) -> list[SQLTable]:
    """Given a list of tuples of (schema, table, column), return a list of
    SQLTable objects.
    """
    tables_to_cols: dict[tuple[Optional[str], str], list[str]] = dict()
    for schema, table, column in reflect_result:
        if (schema, table) not in tables_to_cols:
            tables_to_cols[(schema, table)] = []
        tables_to_cols[(schema, table)].append(column)

    sql_tables: list[SQLTable] = []
    for (schema, table), columns in tables_to_cols.items():
        sql_tables.append(
            SQLTable(
                name=S(table),
                columns=[S(c) for c in columns],
                schema=S(schema) if schema is not None else None,
            )
        )
    return sql_tables
