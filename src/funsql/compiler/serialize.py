import datetime
import io
from contextlib import contextmanager
from functools import singledispatch
from typing import Any

from ..common import Symbol
from ..sqlcontext import SQLDialect


class SQLString:
    query: str
    variables: list[Symbol]

    def __init__(self, query: str, variables: list[Symbol]) -> None:
        self.query = query
        self.variables = variables

    def __repr__(self) -> str:
        query = f"query: \n{self.query}"
        if len(self.variables) == 0:
            return query
        else:
            variables = f"vars: {self.variables}"
            return f"{query}\n\n{variables}"


class SerializationContext:
    dialect: SQLDialect
    buffer: io.StringIO
    level: int
    nested: bool
    variables: list[Symbol]

    def __init__(self, dialect: SQLDialect) -> None:
        self.dialect = dialect
        self.buffer = io.StringIO()
        self.level = 0
        self.nested = False
        self.variables = []

    def write(self, data: str) -> None:
        self.buffer.write(data)

    def newline(self) -> None:
        self.buffer.write("\n")
        for _ in range(self.level):
            self.buffer.write("  ")

    @contextmanager
    def parens(self, space: bool = False):
        self.write(" (" if space else "(")
        yield
        self.write(") " if space else ")")

    def render(self) -> SQLString:
        return SQLString(self.buffer.getvalue(), self.variables.copy())


@singledispatch
def serialize(data: Any, ctx: SerializationContext) -> None:
    if hasattr(data, "_serialize"):
        data._serialize(ctx)
    else:
        raise NotImplementedError(
            f"Don't know how to serialize data of type: {type(data)}"
        )


@serialize.register
def _(data: int, ctx: SerializationContext) -> None:
    ctx.write(str(data))


@serialize.register
def _(data: float, ctx: SerializationContext) -> None:
    ctx.write(str(data))


@serialize.register
def _(data: bool, ctx: SerializationContext) -> None:
    if ctx.dialect.has_bool_literals:
        ctx.write("TRUE" if data else "FALSE")
    else:
        ctx.write("(1 = 1)" if data else "(1 = 0)")


@serialize.register
def _(data: datetime.datetime, ctx: SerializationContext) -> None:
    # TODO: Should we raise an exception when dealing with date/time values for the SQLite dialect?
    ctx.write(f"TIMESTAMP '{data.isoformat()}'")


@serialize.register
def _(data: datetime.date, ctx: SerializationContext) -> None:
    ctx.write(f"DATE '{data.strftime('%Y-%m-%d')}'")


@serialize.register
def _(data: datetime.time, ctx: SerializationContext) -> None:
    """Python resolves time values to count of hours, mins, seconds and microseconds;
    we serialize it to the resolution needed.
    """
    format_str = "%H:%M:%S" if data.microsecond == 0 else "%H:%M:%S.%f"
    ctx.write(f"TIME '{data.strftime(format_str)}'")


@serialize.register
def _(data: datetime.timedelta, ctx: SerializationContext) -> None:
    """Python resolves timedelta values to count of days, seconds, microseconds;
    we serialize it to the resolution needed.
    """
    ctx.write(f"INTERVAL ")
    if data.microseconds == 0:
        if data.seconds == 0:
            ctx.write(f"'{data.days}' DAY")
        else:
            seconds = data.days * 86400 + data.seconds
            ctx.write(f"'{seconds}' SECOND")
    else:
        ctx.write(f"'{data.total_seconds():.6f}' SECOND")


@serialize.register(type(None))
def _(data: None, ctx: SerializationContext) -> None:
    ctx.write("NULL")


@serialize.register
def _(data: str, ctx: SerializationContext) -> None:
    if "'" in data:
        data = data.replace("'", "''")  # tis the postgres way
    ctx.write(f"'{data}'")


@serialize.register
def _(data: Symbol, ctx: SerializationContext) -> None:
    s = str(data)
    lq, rq = ctx.dialect.id_quotes
    if rq in s:
        s = s.replace(rq, rq + rq)
    ctx.write(f"{lq}{s}{rq}")
