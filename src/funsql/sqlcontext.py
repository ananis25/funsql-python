"""
Constructs that provide the context to evaluate a query.
"""

from dataclasses import dataclass
from enum import Enum
from typing import Any, Iterator, Optional

from .common import S, Symbol
from .prettier import (
    Doc,
    QuoteContext,
    call_expr,
    assg_expr,
    list_expr,
    resolve,
    to_doc,
)

__all__ = [
    "SQLTable",
    "SQLCatalog",
    "ValuesTable",
    "VarStyle",
    "LimitStyle",
    "SQLDialect",
]


class SQLTable:
    name: Symbol
    columns: list[Symbol]
    schema: Optional[Symbol]

    def __init__(
        self, name: Symbol, columns: list[Symbol], schema: Optional[Symbol] = None
    ) -> None:
        self.name = S(name)
        self.columns = [S(col) for col in columns]
        self.schema = None if schema is None else S(schema)

    def __repr__(self) -> str:
        ctx = QuoteContext()
        return resolve(to_doc(self, ctx), 80)

    def pretty_repr(self, ctx: QuoteContext) -> Doc:
        name = "SQLTable"
        args = []

        args.append(str(self.name))
        if self.schema is not None:
            args.append(assg_expr("schema", str(self.schema)))
        if ctx.limit:
            args.append("...")
        else:
            args.append(
                assg_expr("columns", list_expr([str(col) for col in self.columns]))
            )
        return call_expr(name, args)


class ValuesTable:
    """To represent queries of the type - `FROM (VALUES ...) AS t(col1, col2, ...)`"""

    columns: tuple[Symbol]
    data: list[tuple]

    def __init__(self, columns: tuple[str], data: list[tuple]) -> None:
        for row in data:
            assert len(row) == len(columns)
        self.columns = tuple(S(c) for c in columns)
        self.data = data


class SQLCatalog:
    """SQL catalog to capture the structure of a table like object for constructing queries"""

    tables: dict[Symbol, SQLTable]
    dialect: "SQLDialect"

    def __init__(
        self, dialect: "SQLDialect", tables: Optional[dict[Symbol, SQLTable]] = None
    ) -> None:
        self.dialect = dialect
        self.tables = {} if tables is None else tables

    def __repr__(self) -> str:
        ctx = QuoteContext()
        return resolve(to_doc(self, ctx), 80)

    def pretty_repr(self, ctx: QuoteContext) -> Doc:
        name = "SQLCatalog"
        args = []

        args.append(assg_expr("dialect", str(self.dialect)))
        for t_name, table in self.tables.items():
            args.append(assg_expr(str(t_name), to_doc(table, ctx)))
        return call_expr(name, args)

    def get(self, key: Symbol) -> Optional[SQLTable]:
        """get the table with the given name"""
        return self.tables.get(key, None)

    def __getitem__(self, key: Symbol) -> SQLTable:
        """get the table with the given name"""
        return self.tables[key]

    def __len__(self) -> int:
        return len(self.tables)

    def __iter__(self) -> Iterator[tuple[Symbol, SQLTable]]:
        for name, table in self.tables.items():
            yield name, table


class VarStyle(Enum):
    NAMED = 1
    NUMBERED = 2
    POSITIONAL = 3


class LimitStyle(Enum):
    REGULAR = 1
    FETCH_FIRST_KIND = 2


@dataclass(repr=False)
class SQLDialect:
    name: str = "default"
    var_style: VarStyle = VarStyle.NAMED
    var_prefix: str = "?"
    id_quotes: tuple[str, str] = ('"', '"')
    has_bool_literals: bool = True
    limit_style: LimitStyle = LimitStyle.REGULAR
    has_recursive_annotation: bool = True
    has_as_columns: bool = True
    has_datetime_types: bool = True
    values_row_constructor: Optional[str] = None
    values_column_prefix: Optional[str] = "column"
    values_column_index: int = 1

    def __repr__(self) -> str:
        return f"SQLDialect(:{self.name})"
