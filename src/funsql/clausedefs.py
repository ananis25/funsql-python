"""
Sections in this module
* utility functions used within this module
* clause definitions
* utility functions for external use
"""

from enum import Enum
from typing import Any, Optional, Union, Literal

from .common import Symbol, S
from .sqlcontext import LimitStyle, VarStyle
from .compiler.serialize import SerializationContext, serialize
from .clauses import SQLClause
from .prettier import (
    Doc,
    QuoteContext,
    call_expr,
    assg_expr,
    list_expr,
    pipe_expr,
    to_doc,
)

__all__ = [
    "AGG",
    "AS",
    "CASE",
    "FROM",
    "FUN",
    "GROUP",
    "HAVING",
    "ID",
    "JOIN",
    "KW",
    "LIMIT",
    "LIT",
    "NOTE",
    "OP",
    "ORDER",
    "Frame",
    "FrameMode",
    "FrameExclude",
    "FrameEdge",
    "FrameEdgeSide",
    "PARTITION",
    "SelectTop",
    "SELECT",
    "ValueOrder",
    "NullsOrder",
    "SORT",
    "UNION",
    "VALUES",
    "VAR",
    "WHERE",
    "WINDOW",
    "WITH",
    "qual",
    "alias",
]

# -----------------------------------------------------------
# Serialization methods for SQLclause objects
# -----------------------------------------------------------


@serialize.register(list)
def _(data: list[Any], ctx: SerializationContext, sep: Optional[str] = None) -> None:
    """Serialize a list of SQL clauses.

    NOTE: Doesn't leave a space before adding characters, do it in the caller.
    """
    sep = ", " if sep is None else f" {sep} "
    if len(data) > 0:
        for i, arg in enumerate(data):
            assert isinstance(arg, (Symbol, SQLClause))
            if i > 0:
                if isinstance(arg, KW):
                    ctx.write(" ")
                else:
                    ctx.write(sep)
            serialize(arg, ctx)


def serialize_lines(
    data: list[SQLClause], ctx: SerializationContext, sep: Optional[str] = None
) -> None:
    """Serialize a list of SQL clauses, one per line. For example, when listing
    multiple column names with a SELECT or ORDER clause.
    """
    if len(data) == 0:
        return
    if len(data) == 1:
        ctx.write(" ")
        serialize(data[0], ctx)
    else:
        ctx.level += 1
        for i, clause in enumerate(data):
            if i > 0:
                if sep is None:
                    ctx.write(", ")
                else:
                    ctx.write(f" {sep} ")
            ctx.newline()
            serialize(clause, ctx)
        ctx.level -= 1


# -----------------------------------------------------------
# utility functions local to this module
# -----------------------------------------------------------

# Type of inputs that need to be converted to a SQLClause
TYPE_INPUT_CLAUSE = Union[SQLClause, Symbol, Any]


def _cast_to_clause(arg: Union[SQLClause, Symbol, Any]) -> SQLClause:
    """Convert a value to a SQLClause."""
    if isinstance(arg, SQLClause):
        return arg
    elif isinstance(arg, Symbol):
        return ID(arg) if str(arg) != "*" else OP(arg)
    else:
        return LIT(arg)


def _cast_to_clause_skip_none(
    arg: Union[SQLClause, Symbol, Any]
) -> Union[SQLClause, None]:
    """Convert a value to a SQLClause, skipping None which otherwise gets
    treated as the NULL literal.
    """
    return None if arg is None else _cast_to_clause(arg)


def _rebase_clause(curr: Optional[SQLClause], pre: SQLClause) -> SQLClause:
    """Shorthand for a common pattern"""
    if curr is None:
        return pre
    else:
        return curr.rebase(pre)


# -----------------------------------------------------------
# SQL clause definitions
# -----------------------------------------------------------


class AGG(SQLClause):
    name: Symbol
    distinct: bool
    args: list[SQLClause]
    filter_: Optional[SQLClause]
    over: Optional[SQLClause]

    def __init__(
        self,
        name: Union[Symbol, str],
        *args: SQLClause,
        distinct: bool = False,
        filter_: Optional[SQLClause] = None,
        over: Optional[SQLClause] = None,
    ) -> None:
        self.name = S(name)
        self.args = [_cast_to_clause(arg) for arg in args]
        self.distinct = distinct
        self.filter_ = filter_
        self.over = _cast_to_clause_skip_none(over)

    def pretty_repr(self, ctx: Optional[QuoteContext]) -> "Doc":
        name = "AGG"
        args = []

        args.append(to_doc(str(self.name)))
        if self.distinct:
            args.append(assg_expr("distinct", to_doc(self.distinct)))
        for arg in self.args:
            args.append(to_doc(arg))
        if self.filter_ is not None:
            args.append(assg_expr("filter", to_doc(self.filter_)))
        if self.over is not None:
            args.append(assg_expr("over", to_doc(self.over)))

        return call_expr(name, args)

    def rebase(self, pre: SQLClause) -> "AGG":
        return self.__class__(
            self.name,
            *self.args,
            distinct=self.distinct,
            filter_=self.filter_,
            over=_rebase_clause(self.over, pre),
        )

    def _serialize(self, ctx: SerializationContext) -> None:
        has_filter = self.filter_ is not None
        has_over = self.over is not None

        if has_filter or has_over:
            ctx.write("(")

        ctx.write(str(self.name))
        with ctx.parens():
            if self.distinct:
                ctx.write("DISTINCT ")
            serialize(self.args, ctx)
        if has_filter:
            ctx.write(" FILTER ")
            with ctx.parens():
                ctx.write("WHERE ")
                serialize(self.filter_, ctx)
        if has_over:
            ctx.write(" OVER ")
            with ctx.parens():
                serialize(self.over, ctx)

        if has_filter or has_over:
            ctx.write(")")


class AS(SQLClause):
    name: Symbol
    columns: Optional[list[Symbol]]
    over: Optional[SQLClause]

    def __init__(
        self,
        name: Symbol,
        columns: Optional[list[Symbol]] = None,
        over: Optional[SQLClause] = None,
    ) -> None:
        self.name = name
        self.columns = None if columns is None else [S(c) for c in columns]
        self.over = _cast_to_clause_skip_none(over)

    def pretty_repr(self, ctx: Optional[QuoteContext]) -> "Doc":
        name = "AS"
        args: list["Doc"] = [str(self.name)]
        if self.columns is not None:
            args.append(
                assg_expr("columns", list_expr([to_doc(col) for col in self.columns]))
            )

        ex = call_expr(name, args)
        if self.over is not None:
            ex = pipe_expr(to_doc(self.over), ex)
        return ex

    def rebase(self, pre: SQLClause) -> "AS":
        return self.__class__(
            name=self.name, columns=self.columns, over=_rebase_clause(self.over, pre)
        )

    def _serialize(self, ctx: SerializationContext) -> None:
        if isinstance(self.over, PARTITION):
            assert (
                self.columns is None
            ), "PARTITION clause can't be aliased as a table with columns"
            serialize(self.name, ctx)
            ctx.write(" AS ")
            with ctx.parens():
                serialize(self.over, ctx)
        elif self.over is not None:
            serialize(self.over, ctx)
            ctx.write(" AS ")
            serialize(self.name, ctx)
            if self.columns is not None:
                with ctx.parens(space=True):
                    serialize(self.columns, ctx)


class CASE(SQLClause):
    args: list[SQLClause]

    def __init__(self, *args: SQLClause) -> None:
        self.args = [_cast_to_clause(arg) for arg in args]

    def pretty_repr(self, ctx: Optional[QuoteContext]) -> "Doc":
        name = "CASE"
        args: list["Doc"] = []

        if len(self.args) < 2:
            args.append(
                assg_expr("args", list_expr([to_doc(arg) for arg in self.args]))
            )
        else:
            args.extend([to_doc(arg) for arg in self.args])
        return call_expr(name, args)

    def _serialize(self, ctx: SerializationContext) -> None:
        with ctx.parens():
            ctx.write("CASE")
            nargs = len(self.args)
            for i, arg in enumerate(self.args):
                if i % 2 == 0:
                    ctx.write(" WHEN " if i < nargs - 1 else " ELSE ")
                else:
                    ctx.write(" THEN ")
                serialize(arg, ctx)
            ctx.write(" END")


class FROM(SQLClause):
    over: Optional[SQLClause] = None

    def __init__(self, over: Optional[Union[SQLClause, Symbol]] = None) -> None:
        self.over = _cast_to_clause_skip_none(over)

    def pretty_repr(self, ctx: Optional[QuoteContext]) -> "Doc":
        name = "FROM"
        ex = call_expr(name, [])

        if self.over is not None:
            ex = pipe_expr(to_doc(self.over), ex)
        return ex

    def rebase(self, pre: SQLClause) -> "FROM":
        return self.__class__(over=_rebase_clause(self.over, pre))

    def _serialize(self, ctx: SerializationContext) -> None:
        ctx.newline()
        ctx.write("FROM")
        if self.over is not None:
            ctx.write(" ")
            serialize(self.over, ctx)


class FUN(SQLClause):
    """A SQL Function"""

    name: Symbol
    args: list[SQLClause]

    def __init__(self, name: Union[Symbol, str], *args: SQLClause) -> None:
        self.name = S(name)
        self.args = [_cast_to_clause(arg) for arg in args]

    def pretty_repr(self, ctx: Optional[QuoteContext]) -> "Doc":
        name = "FUN"
        args = [to_doc(str(self.name))]
        for arg in self.args:
            args.append(to_doc(arg))

        ex = call_expr(name, args)
        return ex

    def _serialize(self, ctx: SerializationContext) -> None:
        ctx.write(str(self.name))
        with ctx.parens():
            serialize(self.args, ctx)


class GROUP(SQLClause):
    by: list[SQLClause]
    over: Optional[SQLClause]

    def __init__(self, *by: SQLClause, over: Optional[SQLClause] = None) -> None:
        self.by = [_cast_to_clause(arg) for arg in by]
        self.over = _cast_to_clause_skip_none(over)

    def pretty_repr(self, ctx: Optional[QuoteContext]) -> "Doc":
        ex = call_expr("GROUP", [to_doc(arg) for arg in self.by])
        if self.over is not None:
            ex = pipe_expr(to_doc(self.over), ex)
        return ex

    def rebase(self, pre: SQLClause) -> "GROUP":
        return self.__class__(*self.by, over=_rebase_clause(self.over, pre))

    def _serialize(self, ctx: SerializationContext) -> None:
        if self.over is not None:
            serialize(self.over, ctx)
        if len(self.by) > 0:
            ctx.newline()
            ctx.write("GROUP BY")
            serialize_lines(self.by, ctx)


class HAVING(SQLClause):
    condition: SQLClause
    over: Optional[SQLClause]

    def __init__(self, condition: SQLClause, over: Optional[SQLClause] = None) -> None:
        self.condition = condition
        self.over = _cast_to_clause_skip_none(over)

    def pretty_repr(self, ctx: Optional[QuoteContext]) -> "Doc":
        ex = call_expr("HAVING", [to_doc(self.condition)])
        if self.over is not None:
            ex = pipe_expr(to_doc(self.over), ex)
        return ex

    def rebase(self, pre: SQLClause) -> "HAVING":
        return self.__class__(
            condition=self.condition, over=_rebase_clause(self.over, pre)
        )

    def _serialize(self, ctx: SerializationContext) -> None:
        if self.over is not None:
            serialize(self.over, ctx)
        ctx.newline()
        ctx.write("HAVING")

        cond = self.condition
        if isinstance(cond, OP) and cond.name == "AND" and len(cond.args) >= 2:
            serialize_lines(cond.args, ctx, sep="AND")
        else:
            ctx.write(" ")
            serialize(cond, ctx)


class ID(SQLClause):
    name: Symbol
    over: Optional[SQLClause] = None

    def __init__(self, name: Symbol, over: Optional[SQLClause] = None) -> None:
        assert isinstance(
            name, Symbol
        ), f"Invalid type to initialize a Symbol: {type(name)}"
        self.name = name
        self.over = _cast_to_clause_skip_none(over)

    def pretty_repr(self, ctx: Optional[QuoteContext]) -> "Doc":
        ex = call_expr("ID", [to_doc(self.name)])
        if self.over is not None:
            ex = pipe_expr(to_doc(self.over), ex)
        return ex

    def rebase(self, pre: SQLClause) -> "ID":
        return self.__class__(name=self.name, over=_rebase_clause(self.over, pre))

    def _serialize(self, ctx: SerializationContext) -> None:
        if self.over is not None:
            serialize(self.over, ctx)
            ctx.write(".")
        serialize(self.name, ctx)


class JOIN(SQLClause):
    """Your regular SQL Join clause"""

    joinee: SQLClause
    on: SQLClause
    left: bool = False
    right: bool = False
    lateral: bool = False
    over: Optional[SQLClause] = None

    def __init__(
        self,
        joinee: SQLClause,
        on: SQLClause,
        left: bool = False,
        right: bool = False,
        lateral: bool = False,
        over: Optional[SQLClause] = None,
    ) -> None:
        self.joinee = _cast_to_clause(joinee)
        self.on = _cast_to_clause(on)
        self.left = left
        self.right = right
        self.lateral = lateral
        self.over = _cast_to_clause_skip_none(over)

    def pretty_repr(self, ctx: Optional[QuoteContext]) -> "Doc":
        name = "JOIN"
        args = []

        args.append(to_doc(self.joinee))
        args.append(to_doc(self.on))
        if self.left:
            args.append(assg_expr("left", to_doc(True)))
        if self.right:
            args.append(assg_expr("right", to_doc(True)))
        if self.lateral:
            args.append(assg_expr("lateral", to_doc(True)))

        ex = call_expr(name, args)
        if self.over is not None:
            ex = pipe_expr(to_doc(self.over), ex)
        return ex

    def rebase(self, pre: SQLClause) -> "JOIN":
        return self.__class__(
            joinee=self.joinee,
            on=self.on,
            left=self.left,
            right=self.right,
            lateral=self.lateral,
            over=_rebase_clause(self.over, pre),
        )

    def _serialize(self, ctx: SerializationContext) -> None:
        if self.over is not None:
            serialize(self.over, ctx)
        ctx.newline()

        is_cross = (
            (not self.left)
            and (not self.right)
            and (isinstance(self.on, LIT) and self.on.val == True)
        )
        if is_cross:
            ctx.write("CROSS JOIN ")
        elif self.left and self.right:
            ctx.write("FULL JOIN ")
        elif self.left:
            ctx.write("LEFT JOIN ")
        elif self.right:
            ctx.write("RIGHT JOIN ")
        else:
            ctx.write("INNER JOIN ")  # default JOIN type

        if self.lateral:
            ctx.write("LATERAL ")

        serialize(self.joinee, ctx)
        if not is_cross:
            ctx.write(" ON ")  # TODO: why is the filter on cross joins needed?
            serialize(self.on, ctx)


class KW(SQLClause):
    """Keyword argument of a function/operator"""

    name: Symbol
    over: Optional[SQLClause] = None

    def __init__(self, name: Symbol, over: Optional[SQLClause] = None) -> None:
        self.name = name
        self.over = _cast_to_clause_skip_none(over)

    def pretty_repr(self, ctx: Optional[QuoteContext]) -> "Doc":
        ex = call_expr("KW", [str(self.name)])
        if self.over is not None:
            ex = pipe_expr(to_doc(self.over), ex)
        return ex

    def rebase(self, pre: SQLClause) -> "KW":
        return self.__class__(name=self.name, over=_rebase_clause(self.over, pre))

    def _serialize(self, ctx: SerializationContext) -> None:
        ctx.write(str(self.name))
        if self.over is not None:
            ctx.write(" ")
            serialize(self.over, ctx)


class LIMIT(SQLClause):
    """
    NOTE: `with_ties` is not a supported construct in SQLite or MYSQL dialects
    """

    over: Optional[SQLClause]
    offset: Optional[int]
    limit: Optional[int]
    with_ties: bool

    def __init__(
        self,
        limit: Optional[int] = None,
        *,
        offset: Optional[int] = None,
        with_ties: bool = False,
        over: Optional[SQLClause] = None,
    ) -> None:
        """Can be initialized as LIMIT(:offset, :limit) or LIMIT(:limit)"""
        self.limit = limit
        self.offset = offset
        self.with_ties = with_ties
        self.over = _cast_to_clause_skip_none(over)

    def pretty_repr(self, ctx: Optional[QuoteContext]) -> "Doc":
        name = "LIMIT"
        args = []

        if self.limit is not None:
            args.append(to_doc(self.limit))
        if self.offset is not None:
            args.append(assg_expr("offset", to_doc(self.offset)))
        if self.with_ties:
            args.append(assg_expr("with_ties", to_doc(True)))

        ex = call_expr(name, args)
        if self.over is not None:
            ex = pipe_expr(to_doc(self.over), ex)
        return ex

    def rebase(self, pre: SQLClause) -> "LIMIT":
        return self.__class__(
            self.limit,
            offset=self.offset,
            over=_rebase_clause(self.over, pre),
            with_ties=self.with_ties,
        )

    def _serialize(self, ctx: SerializationContext) -> None:
        if self.over is not None:
            serialize(self.over, ctx)
        if self.offset is None and self.limit is None:
            return

        if ctx.dialect.limit_style == LimitStyle.MYSQL:
            ctx.newline()
            ctx.write("LIMIT ")
            if self.offset is not None:
                ctx.write(f"{self.offset}, ")
            ctx.write(
                f"{self.limit}" if self.limit is not None else "18446744073709551615"
            )
        elif ctx.dialect.limit_style == LimitStyle.SQLITE:
            ctx.newline()
            ctx.write("LIMIT ")
            ctx.write(f"{self.limit}" if self.limit is not None else "-1")
            if self.offset is not None:
                ctx.newline()
                ctx.write("OFFSET ")
                ctx.write(f"{self.offset}")
        else:
            if self.offset is not None:
                ctx.newline()
                ctx.write("OFFSET ")
                ctx.write(f"{self.offset}")
                ctx.write("ROW" if self.offset == 1 else "ROWS")
            if self.limit is not None:
                ctx.newline()
                ctx.write("FETCH FIRST " if self.offset is None else "FETCH NEXT ")
                ctx.write(f"{self.limit}")
                ctx.write(" ROW" if self.limit == 1 else " ROWS")
                ctx.write(" WITH TIES" if self.with_ties else " ONLY")


class LIT(SQLClause):
    val: Any

    def __init__(self, val: Any) -> None:
        self.val = val

    def pretty_repr(self, ctx: Optional[QuoteContext]) -> "Doc":
        val = to_doc(self.val)
        return f"LIT({val})"

    def _serialize(self, ctx: SerializationContext) -> None:
        serialize(self.val, ctx)


class NOTE(SQLClause):
    text: str
    postfix: bool
    over: Optional[SQLClause]

    def __init__(
        self, text: str, postfix: bool = False, over: Optional[SQLClause] = None
    ) -> None:
        self.text = text
        self.postfix = postfix
        self.over = _cast_to_clause_skip_none(over)

    def pretty_repr(self, ctx: Optional[QuoteContext]) -> "Doc":
        name = "NOTE"
        args = []

        args.append(to_doc(self.text))
        if self.postfix:
            args.append(assg_expr("postfix", to_doc(True)))

        ex = call_expr(name, args)
        if self.over is not None:
            ex = pipe_expr(to_doc(self.over), ex)
        return ex

    def rebase(self, pre: SQLClause) -> "NOTE":
        return self.__class__(
            text=self.text, over=_rebase_clause(self.over, pre), postfix=self.postfix
        )

    def _serialize(self, ctx: SerializationContext) -> None:
        if self.over is None:
            ctx.write(self.text)
        elif self.postfix:
            serialize(self.over, ctx)
            ctx.write(f" {self.text}")
        else:
            ctx.write(f"{self.text} ")
            serialize(self.over, ctx)


class OP(SQLClause):
    """An SQL operator"""

    name: Symbol
    args: list[SQLClause]

    def __init__(self, name: Union[Symbol, str], *args: TYPE_INPUT_CLAUSE) -> None:
        self.name = S(name)
        self.args = [_cast_to_clause(x) for x in args]

    def pretty_repr(self, ctx: Optional[QuoteContext]) -> "Doc":
        name = "OP"
        args = [to_doc(str(self.name))]
        for arg in self.args:
            args.append(to_doc(arg))

        ex = call_expr(name, args)
        return ex

    def _serialize(self, ctx: SerializationContext) -> None:
        name_str = str(self.name)
        if len(self.args) == 0:
            ctx.write(name_str)
        elif len(self.args) == 1:
            with ctx.parens():
                ctx.write(f"{name_str} ")
                serialize(self.args[0], ctx)
        else:
            with ctx.parens():
                serialize(self.args, ctx, sep=name_str)


class ORDER(SQLClause):
    """SQL ORDER BY clause"""

    by: list[SQLClause]
    over: Optional[SQLClause]

    def __init__(self, *by: SQLClause, over: Optional[SQLClause] = None) -> None:
        self.by = [_cast_to_clause(x) for x in by]
        self.over = _cast_to_clause_skip_none(over)

    def pretty_repr(self, ctx: Optional[QuoteContext]) -> "Doc":
        name = "ORDER"
        args = []
        for arg in self.by:
            args.append(to_doc(arg))

        ex = call_expr(name, args)
        if self.over is not None:
            ex = pipe_expr(to_doc(self.over), ex)
        return ex

    def rebase(self, pre: SQLClause) -> "ORDER":
        return self.__class__(*self.by, over=_rebase_clause(self.over, pre))

    def _serialize(self, ctx: SerializationContext) -> None:
        if self.over is not None:
            serialize(self.over, ctx)
        if len(self.by) > 0:
            ctx.newline()
            ctx.write("ORDER BY")
            serialize_lines(self.by, ctx)


class FrameMode(Enum):
    """Frame mode for window functions"""

    RANGE = "RANGE"
    ROWS = "ROWS"
    GROUPS = "GROUPS"


class FrameExclude(Enum):
    """Frame exclusion for window functions per row/group"""

    NO_OTHERS = "NO OTHERS"
    CURRENT_ROW = "CURRENT ROW"
    GROUP = "GROUP"
    TIES = "TIES"


class FrameEdgeSide(Enum):
    PRECEDING = "PRECEDING"
    CURRENT_ROW = "CURRENT ROW"
    FOLLOWING = "FOLLOWING"


class FrameEdge:
    """
    Corresponds to the `frame_start` or `frame_end` boundary in the partition clause.
    Specified as: `{val} {typ}`

    Args:
        typ: {FrameEdgeSide}
        val: None - unbounded, any other expr - {expr}
    """

    typ: FrameEdgeSide
    val: Union[None, Any]

    def __init__(self, typ: FrameEdgeSide, val: Union[None, Any] = None) -> None:
        self.typ = typ
        self.val = val

    def pretty_repr(self, ctx: Optional[QuoteContext]) -> "Doc":
        if self.typ == FrameEdgeSide.CURRENT_ROW:
            return "CURRENT_ROW"
        else:
            return call_expr(self.typ.value, [to_doc(self.val)])


class Frame:
    """Specify the partition fram for a window function. Expanded by the SQL query,
    it corresponds to:

    `{ mode } BETWEEN { start } AND { end } [ exclude ]`

    Args:
        mode: The frame mode
        start: The start boundary
        end: The end boundary

    NOTE: The rules for default values of (frame_start, frame_end) depend on if the
    order_by clause in the corresponding partition.
    - (None, 0) if order_by is present
    - (None, None) if not

    This confuses me, so we make both mandatory if you're using a partition frame.
    """

    mode: FrameMode
    start: FrameEdge
    end: FrameEdge
    exclude: Optional[FrameExclude] = None

    def __init__(
        self,
        mode: FrameMode,
        start: FrameEdge,
        end: FrameEdge,
        exclude: Optional[FrameExclude] = None,
    ) -> None:
        self.mode = mode
        self.start = start
        self.end = end
        self.exclude = exclude

    def pretty_repr(self, ctx: Optional[QuoteContext]) -> "Doc":
        if self.start is None and self.end is None and self.exclude is None:
            return self.mode.value

        list_ex = []
        list_ex.append(assg_expr("mode", self.mode.value))
        if self.start is not None:
            list_ex.append(assg_expr("start", to_doc(self.start)))
        if self.end is not None:
            list_ex.append(assg_expr("end", to_doc(self.end)))
        if self.exclude is not None:
            list_ex.append(assg_expr("exclude", self.exclude.value))

        return list_expr(list_ex)

    def _serialize_boundary(
        self, ctx: SerializationContext, boundary: FrameEdge
    ) -> None:
        if boundary.typ == FrameEdgeSide.CURRENT_ROW:
            ctx.write("CURRENT ROW")
        elif boundary.val is None:
            ctx.write(f"UNBOUNDED {boundary.typ.value}")
        else:
            serialize(boundary.val, ctx)
            ctx.write(f" {boundary.typ.value}")

    def _serialize(self, ctx: SerializationContext) -> None:
        ctx.write(f"{self.mode.value} ")

        ctx.write("BETWEEN ")
        self._serialize_boundary(ctx, self.start)
        ctx.write(" AND ")
        self._serialize_boundary(ctx, self.end)
        if self.exclude is not None:
            ctx.write(f" EXCLUDE {self.exclude.value}")


class PARTITION(SQLClause):
    """
    Look at the postgres and sqlite documentation to figure out the semantics
    - sqlite: https://www.sqlite.org/windowfunctions.html
    - postgres: https://www.postgresql.org/docs/14/sql-expressions.html#SYNTAX-WINDOW-FUNCTIONS
    """

    by: list[SQLClause]
    order_by: list[SQLClause]
    frame: Optional[Frame]
    over: Optional[SQLClause]

    def __init__(
        self,
        *by: SQLClause,
        order_by: Optional[list[SQLClause]] = None,
        frame: Optional[Frame] = None,
        over: Optional[SQLClause] = None,
    ) -> None:
        self.by = [_cast_to_clause(x) for x in by]
        self.order_by = (
            [] if order_by is None else [_cast_to_clause(x) for x in order_by]
        )
        self.frame = frame
        self.over = _cast_to_clause_skip_none(over)

    def pretty_repr(self, ctx: Optional[QuoteContext]) -> "Doc":
        name = "PARTITION"
        args = []
        for a in self.by:
            args.append(to_doc(a))
        if len(self.order_by) > 0:
            args.append(
                assg_expr("order_by", list_expr([to_doc(a) for a in self.order_by]))
            )
        if self.frame is not None:
            args.append(assg_expr("frame", to_doc(self.frame)))

        ex = call_expr(name, args)
        if self.over is not None:
            ex = pipe_expr(to_doc(self.over), ex)
        return ex

    def rebase(self, pre: "SQLClause") -> "PARTITION":
        return self.__class__(
            *self.by,
            order_by=self.order_by,
            frame=self.frame,
            over=_rebase_clause(self.over, pre),
        )

    def _serialize(self, ctx: SerializationContext) -> None:
        add_space: bool = False
        if self.over is not None:
            add_space = True
            serialize(self.over, ctx)
        if len(self.by) > 0:
            ctx.write(" " if add_space else "")
            add_space = True
            ctx.write("PARTITION BY ")
            serialize(self.by, ctx)
        if len(self.order_by) > 0:
            ctx.write(" " if add_space else "")
            add_space = True
            ctx.write("ORDER BY ")
            serialize(self.order_by, ctx)
        if self.frame is not None:
            ctx.write(" " if add_space else "")
            serialize(self.frame, ctx)


class SelectTop(SQLClause):
    """When selecting top n rows"""

    limit: int
    with_ties: bool

    def __init__(self, limit: int, with_ties: bool = False) -> None:
        self.limit = limit
        self.with_ties = with_ties

    def pretty_repr(self, ctx: Optional[QuoteContext]) -> "Doc":
        if not self.with_ties:
            return to_doc(self.limit)
        else:
            return list_expr(
                [
                    assg_expr("limit", to_doc(self.limit)),
                    assg_expr("with ties", to_doc(self.with_ties)),
                ]
            )


class SELECT(SQLClause):
    """SQL SELECT clause where it all comes together"""

    args: list[SQLClause]
    distinct: bool
    top: Optional[SelectTop]
    over: Optional[SQLClause]

    def __init__(
        self,
        *args: Union[SQLClause, Symbol, Any],
        distinct: bool = False,
        top: Optional[SelectTop] = None,
        over: Optional[SQLClause] = None,
    ) -> None:
        self.args = [_cast_to_clause(x) for x in args]
        self.distinct = distinct
        self.top = top
        self.over = _cast_to_clause_skip_none(over)

    def pretty_repr(self, ctx: Optional[QuoteContext]) -> "Doc":
        name = "SELECT"
        args = []

        if self.distinct:
            args.append(assg_expr("distinct", to_doc(self.distinct)))
        if self.top is not None:
            args.append(assg_expr("top", to_doc(self.top)))
        if len(self.args) == 0:
            args.append(assg_expr("args", to_doc("[]")))
        else:
            for arg in self.args:
                args.append(to_doc(arg))

        ex = call_expr(name, args)
        if self.over is not None:
            ex = pipe_expr(to_doc(self.over), ex)
        return ex

    def rebase(self, pre: SQLClause) -> "SELECT":
        return self.__class__(
            *self.args,
            distinct=self.distinct,
            top=self.top,
            over=_rebase_clause(self.over, pre),
        )

    def _serialize(self, ctx: SerializationContext) -> None:
        nested_orig = ctx.nested
        if nested_orig:
            ctx.level += 1
            ctx.write("(")
            ctx.newline()

        ctx.nested = True
        ctx.write("SELECT")
        if self.top is not None:
            ctx.write(f" TOP {self.top.limit}")
            if self.top.with_ties:
                ctx.write(" WITH TIES")

        if self.distinct:
            ctx.write(" DISTINCT")
        serialize_lines(self.args, ctx)
        if self.over is not None:
            serialize(self.over, ctx)

        ctx.nested = nested_orig
        if nested_orig:
            ctx.level -= 1
            ctx.newline()
            ctx.write(")")


class ValueOrder(Enum):
    ASC = "ASC"
    DESC = "DESC"


class NullsOrder(Enum):
    FIRST = "FIRST"
    LAST = "LAST"


class SORT(SQLClause):
    value: ValueOrder
    nulls: Optional[NullsOrder] = None
    over: Optional[SQLClause] = None

    def __init__(
        self,
        value: ValueOrder,
        nulls: Optional[NullsOrder] = None,
        over: Optional[SQLClause] = None,
    ) -> None:
        self.value = value
        self.nulls = nulls
        self.over = _cast_to_clause_skip_none(over)

    def pretty_repr(self, ctx: Optional[QuoteContext]) -> "Doc":
        name = "SORT"
        args = []

        args.append(self.value.value)
        if self.nulls is not None:
            args.append(assg_expr("nulls", self.nulls.value))

        ex = call_expr(name, args)
        if self.over is not None:
            ex = pipe_expr(to_doc(self.over), ex)
        return ex

    def rebase(self, pre: SQLClause) -> "SORT":
        return self.__class__(
            value=self.value, nulls=self.nulls, over=_rebase_clause(self.over, pre)
        )

    def _serialize(self, ctx: SerializationContext) -> None:
        if self.over is not None:
            serialize(self.over, ctx)
            ctx.write(" ")
        ctx.write(self.value.value)
        if self.nulls is not None:
            ctx.write(" NULLS " + self.nulls.value)


class UNION(SQLClause):
    args: list[SQLClause]
    all_: bool
    over: Optional[SQLClause]

    def __init__(
        self,
        *args: SQLClause,
        all_: bool = False,
        over: Optional[SQLClause] = None,
    ) -> None:
        self.args = [_cast_to_clause(x) for x in args]
        self.all_ = all_
        self.over = _cast_to_clause_skip_none(over)

    def pretty_repr(self, ctx: Optional[QuoteContext]) -> "Doc":
        name = "UNION"
        args = []

        if self.all_:
            args.append(assg_expr("all", to_doc(self.all_)))
        if len(self.args) == 0:
            args.append(assg_expr("args", "[]"))
        else:
            for arg in self.args:
                args.append(to_doc(arg))

        ex = call_expr(name, args)
        if self.over is not None:
            ex = pipe_expr(to_doc(self.over), ex)
        return ex

    def rebase(self, pre: SQLClause) -> "UNION":
        return self.__class__(
            *self.args, all_=self.all_, over=_rebase_clause(self.over, pre)
        )

    def _serialize(self, ctx: SerializationContext) -> None:
        nested_orig = ctx.nested
        if nested_orig:
            ctx.level += 1
            ctx.write("(")
            ctx.newline()

        ctx.nested = False
        if self.over is not None:
            serialize(self.over, ctx)
        for arg in self.args:
            ctx.newline()
            ctx.write("UNION" if not self.all_ else "UNION ALL")
            ctx.newline()
            serialize(arg, ctx)

        ctx.nested = nested_orig
        if nested_orig:
            ctx.level -= 1
            ctx.newline()
            ctx.write(")")


class VALUES(SQLClause):
    rows: list[Any]

    def __init__(self, rows: list[Union[Any, tuple]]) -> None:
        self.rows = rows

    def pretty_repr(self, ctx: Optional[QuoteContext]) -> "Doc":
        name = "VALUES"
        args = []

        make_tuple = lambda seq: ", ".join([to_doc(x) for x in seq])  # type: ignore
        if isinstance(self.rows[0], tuple):
            args.append(list_expr([f"({make_tuple(row)})" for row in self.rows]))
        else:
            args.append(make_tuple(self.rows))
        ex = call_expr(name, args)
        return ex

    def _serialize(self, ctx: SerializationContext) -> None:
        nested_orig = ctx.nested
        if nested_orig:
            ctx.level += 1
            ctx.write("(")
            ctx.newline()

        ctx.nested = True
        ctx.write("VALUES")
        l = len(self.rows)
        if l == 1:
            ctx.write(" ")
        elif l > 1:
            ctx.level += 1
            ctx.newline()

        row_prefix = ctx.dialect.values_row_constructor
        for i, row in enumerate(self.rows):
            if i > 0:
                ctx.write(",")
                ctx.newline()
            if isinstance(row, tuple):
                if row_prefix is not None:
                    ctx.write(row_prefix)

                with ctx.parens():
                    for j, val in enumerate(row):
                        if j > 0:
                            ctx.write(", ")
                        serialize(val, ctx)
            else:
                serialize(row, ctx)

        if l > 1:
            ctx.level -= 1
        ctx.nested = nested_orig
        if nested_orig:
            ctx.level -= 1
            ctx.newline()
            ctx.write(")")


class VAR(SQLClause):
    name: Symbol

    def __init__(self, name: Symbol) -> None:
        self.name = name

    def pretty_repr(self, ctx: Optional[QuoteContext]) -> "Doc":
        return call_expr("VAR", [str(self.name)])

    def _serialize(self, ctx: SerializationContext) -> None:
        ctx.write(ctx.dialect.var_prefix)
        style = ctx.dialect.var_style

        if style == VarStyle.POSITIONAL:
            ctx.variables.append(self.name)
        else:
            try:
                pos = ctx.variables.index(self.name)
            except ValueError:
                pos = len(ctx.variables)
                ctx.variables.append(self.name)
            if style == VarStyle.NAMED:
                ctx.write(str(self.name))
            elif style == VarStyle.NUMBERED:
                ctx.write(str(1 + pos))


class WHERE(SQLClause):
    condition: SQLClause
    over: Optional[SQLClause] = None

    def __init__(self, condition: SQLClause, over: Optional[SQLClause] = None) -> None:
        self.condition = _cast_to_clause(condition)
        self.over = _cast_to_clause_skip_none(over)

    def pretty_repr(self, ctx: Optional[QuoteContext]) -> "Doc":
        name = "WHERE"
        args = [to_doc(self.condition)]

        ex = call_expr(name, args)
        if self.over is not None:
            ex = pipe_expr(to_doc(self.over), ex)
        return ex

    def rebase(self, pre: SQLClause) -> "WHERE":
        return self.__class__(
            condition=self.condition,
            over=_rebase_clause(self.over, pre),
        )

    def _serialize(self, ctx: SerializationContext) -> None:
        if self.over is not None:
            serialize(self.over, ctx)

        ctx.newline()
        ctx.write("WHERE")
        cond = self.condition
        if isinstance(cond, OP) and cond.name == "AND" and len(cond.args) >= 2:
            serialize_lines(cond.args, ctx, sep="AND")
        else:
            ctx.write(" ")
            serialize(cond, ctx)


class WINDOW(SQLClause):
    args: list[SQLClause]
    over: Optional[SQLClause]

    def __init__(self, *args: SQLClause, over: Optional[SQLClause] = None) -> None:
        self.args = [_cast_to_clause(arg) for arg in args]
        self.over = _cast_to_clause_skip_none(over)

    def pretty_repr(self, ctx: Optional[QuoteContext]) -> "Doc":
        name = "WINDOW"
        args = []

        if len(self.args) == 0:
            args.append(assg_expr("args", "[]"))
        else:
            for arg in self.args:
                args.append(to_doc(arg))

        ex = call_expr(name, args)
        if self.over is not None:
            ex = pipe_expr(to_doc(self.over), ex)
        return ex

    def rebase(self, pre: SQLClause) -> "WINDOW":
        return self.__class__(*self.args, over=_rebase_clause(self.over, pre))

    def _serialize(self, ctx: SerializationContext) -> None:
        if self.over is not None:
            serialize(self.over, ctx)

        if len(self.args) > 0:
            ctx.newline()
            ctx.write("WINDOW")
            serialize_lines(self.args, ctx)


class WITH(SQLClause):
    args: list[SQLClause]
    recursive: bool
    over: Optional[SQLClause]

    def __init__(
        self,
        *args: SQLClause,
        recursive: bool = False,
        over: Optional[SQLClause] = None,
    ) -> None:
        self.args = list(args)
        self.over = _cast_to_clause_skip_none(over)
        self.recursive = recursive

    def pretty_repr(self, ctx: Optional[QuoteContext]) -> "Doc":
        name = "WITH"
        args = []

        if self.recursive:
            args.append(assg_expr("recursive", to_doc(True)))
        if len(self.args) == 0:
            args.append(assg_expr("args", "[]"))
        else:
            for arg in self.args:
                args.append(to_doc(arg))

        ex = call_expr(name, args)
        if self.over is not None:
            ex = pipe_expr(to_doc(self.over), ex)
        return ex

    def rebase(self, pre: SQLClause) -> "WITH":
        return self.__class__(
            *self.args,
            recursive=self.recursive,
            over=_rebase_clause(self.over, pre),
        )

    def _serialize(self, ctx: SerializationContext) -> None:
        if len(self.args) > 0:
            ctx.write("WITH ")
            if self.recursive and ctx.dialect.has_recursive_annotation:
                ctx.write("RECURSIVE ")

            for i, arg in enumerate(self.args):
                if i > 0:
                    ctx.write(", ")
                    ctx.newline()
                # NOTE: The `WITH` clause inverts order of the alias and the original clause for an `AS` expression
                if isinstance(arg, AS):
                    serialize(arg.name, ctx)
                    if arg.columns is not None:
                        with ctx.parens(space=True):
                            serialize(arg.columns, ctx)
                    ctx.write(" AS ")
                    # NOTE: reassigning `arg` to the parent of the AS clause
                    arg = arg.over

                nested_orig = ctx.nested
                ctx.nested = True
                serialize(arg, ctx)
                ctx.nested = nested_orig
            ctx.newline()

        if self.over is not None:
            serialize(self.over, ctx)


# -----------------------------------------------------------
# utility routines to export
# -----------------------------------------------------------


def qual(*_args: Union[Symbol, str]) -> "ID":
    """Qualify a table/column name, i.e.

    - `qual(schema, table) = ID(schema) >> ID(table)`
    - `qual(table, column) = ID(table) >> ID(column)`, etc.
    """
    args = [S(arg) for arg in _args]
    if len(args) == 2:
        return ID(args[0]) >> ID(args[1])
    elif len(args) == 3:
        return ID(args[0]) >> ID(args[1]) >> ID(args[2])
    else:
        raise NotImplementedError(
            "How do you qualify a SQL identifier with <2, >3 args?"
        )


def alias(curr: Union[SQLClause, Symbol, str], rename: Union[Symbol, str]) -> "AS":
    """Rename an identifier or SQL clause"""
    if not isinstance(curr, SQLClause):
        curr = ID(S(curr))
    return curr >> AS(S(rename))
