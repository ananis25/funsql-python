"""
This module implements support for rendering to the common SQL variants. 
"""

from ..sqlcontext import SQLDialect
from ..clausedefs import VarStyle, LimitStyle
from ..render import dialect_postgres

__all__ = ["dialect_mysql", "dialect_postgres", "dialect_sqlite"]


def dialect_mysql() -> SQLDialect:
    return SQLDialect(
        name="mysql",
        var_style=VarStyle.POSITIONAL,
        var_prefix="?",
        id_quotes=("`", "`"),
        values_row_constructor="ROW",
        values_column_prefix="column_",
        values_column_index=0,
    )


def dialect_sqlite() -> SQLDialect:
    return SQLDialect(
        name="sqlite",
        var_style=VarStyle.NUMBERED,
        var_prefix="?",
        has_as_columns=False,
        has_datetime_types=False,
    )
