from functools import partial
from typing import Any, Optional, Union, Callable

from .common import Symbol, S, LITERAL_TYPES, register_union_type
from .sqlcontext import SQLTable, ValuesTable
from .nodes import *
from .clauses import SQLClause
from .clausedefs import (
    Frame,
    FrameMode,
    FrameEdgeSide,
    FrameEdge,
    FrameExclude,
    ValueOrder,
    NullsOrder,
)
from .prettier import (
    Doc,
    call_expr,
    assg_expr,
    list_expr,
    pipe_expr,
    to_doc,
    QuoteContext,
)

__all__ = [
    "Agg",
    "Append",
    "As",
    "Bind",
    "Define",
    "From",
    "Fun",
    "Get",
    "Group",
    "Iterate",
    "Join",
    "Limit",
    "Lit",
    "Order",
    "Partition",
    "Select",
    "Sort",
    "Var",
    "Where",
    "With",
    "WithExternal",
    "Asc",
    "Desc",
    "ValuesTable",
    "aka",
    "F",
]


# -----------------------------------------------------------
# utility functions local to this module
# -----------------------------------------------------------


class ExtendAttrs(type):
    """
    A SQLNode using this as a metaclass can support fluent syntax like
    `Fun.count(*args)` instead of `Fun("count", *args)`. Used by the `Fun`
    and `Agg` classes.

    NOTE: This is a frequence source of bugs, since the user provided key value
    might clash with an attribute of the class. Look out for it probably?
    """

    def __getattr__(cls, key: str) -> Callable:
        if key.startswith("_"):
            raise AttributeError(
                "fluent syntax isn't supported for strings starting with an underscore"
            )
        return partial(cls, key)


class ExtendAttrsFull(type):
    """
    Same as ExtendAttrs but actually initializes the class. Used by the
    `Get` and `Var` classes.
    """

    def __getattr__(cls, key: str) -> Callable:
        if key.startswith("_"):
            raise AttributeError(
                "fluent syntax isn't supported for strings starting with an underscore"
            )
        return cls(key)


def _rebase_node(curr: Optional[SQLNode], pre: SQLNode) -> SQLNode:
    """shorthand for a common pattern"""
    if curr is None:
        return pre
    else:
        return curr.rebase(pre)


def _cast_to_node(node: Union[SQLNode, Any]) -> SQLNode:
    """Convert a value to a SQLNode"""
    if isinstance(node, SQLNode):
        return node
    else:
        return Lit(node)


def _cast_to_node_skip_none(node: Union[SQLNode, Any]) -> Optional[SQLNode]:
    """Convert a value to a SQLNode, skipping None"""
    if node is None:
        return None
    else:
        return _cast_to_node(node)


# -----------------------------------------------------------
# Node definitions to export
# -----------------------------------------------------------


class Agg(SQLNode, metaclass=ExtendAttrs):
    _name: Symbol
    args: list[SQLNode]
    distinct: bool
    filter_: Optional[SQLNode]
    over: Optional[SQLNode]

    def __init__(
        self,
        name: Union[str, Symbol],
        *args: SQLNode,
        distinct: bool = False,
        filter_: Optional[SQLNode] = None,
        over: Optional[SQLNode] = None,
    ) -> None:
        super().__init__()
        self._name = S(name)
        self.args = [_cast_to_node(arg) for arg in args]
        self.distinct = distinct
        self.filter_ = _cast_to_node_skip_none(filter_)
        self.over = _cast_to_node_skip_none(over)

    def rebase(self, pre: SQLNode) -> "SQLNode":
        return self.__class__(
            self._name,
            *self.args,
            distinct=self.distinct,
            filter_=self.filter_,
            over=_rebase_node(self.over, pre),
        )

    @check_repr_context
    def pretty_repr(self, ctx: QuoteContext) -> "Doc":
        name = "Agg"
        args = []

        _name_str = str(self._name)
        if _name_str.isalpha():
            name = f"Agg.{_name_str}"  # Agg.Count
        else:
            name = f'Agg."{_name_str}"'  # edge cases

        if self.distinct:
            args.append(assg_expr("distinct", to_doc(self.distinct)))
        for arg in self.args:
            args.append(to_doc(arg, ctx))
        if self.filter_ is not None:
            args.append(assg_expr("filter", to_doc(self.filter_, ctx)))
        if self.over is not None:
            args.append(assg_expr("over", to_doc(self.over, ctx)))

        return call_expr(name, args)


class Append(TabularNode):
    """`Append` concatenates input datasets, analogous to `union` in SQL."""

    args: list[SQLNode]
    over: Optional[SQLNode]

    def __init__(self, *args: SQLNode, over: Optional[SQLNode] = None) -> None:
        super().__init__()
        self.args = [_cast_to_node(arg) for arg in args]
        self.over = _cast_to_node_skip_none(over)

    def rebase(self, pre: SQLNode) -> "SQLNode":
        return self.__class__(*self.args, over=_rebase_node(self.over, pre))

    @check_repr_context
    def pretty_repr(self, ctx: QuoteContext) -> "Doc":
        name = "Append"
        args = []

        if len(self.args) == 0:
            args.append(assg_expr("args", to_doc("[]")))
        else:
            for arg in self.args:
                args.append(to_doc(arg, ctx))

        ex = call_expr(name, args)
        if self.over is not None:
            ex = pipe_expr(to_doc(self.over, ctx), ex)
        return ex


class As(SQLNode):
    name: Symbol
    over: Optional[SQLNode]

    def __init__(
        self, name: Union[str, Symbol], over: Optional[SQLNode] = None
    ) -> None:
        super().__init__()
        self.name = S(name)
        self.over = _cast_to_node_skip_none(over)

    def rebase(self, pre: SQLNode) -> "SQLNode":
        return self.__class__(name=self.name, over=_rebase_node(self.over, pre))

    @check_repr_context
    def pretty_repr(self, ctx: QuoteContext) -> "Doc":
        name = "As"
        args: list["Doc"] = [str(self.name)]

        ex = call_expr(name, args)
        if self.over is not None:
            ex = pipe_expr(to_doc(self.over, ctx), ex)
        return ex


class Bind(SQLNode):
    """Binds a set of `Var` variables in the `over` node to values."""

    args: list[SQLNode]
    over: Optional[SQLNode]
    label_map: dict[Symbol, int]

    def __init__(
        self,
        *args: SQLNode,
        over: Optional[SQLNode] = None,
        label_map: Optional[dict[Symbol, int]] = None,
    ) -> None:
        super().__init__()
        self.args = [_cast_to_node(arg) for arg in args]
        self.over = _cast_to_node_skip_none(over)
        self.label_map = (
            label_map if label_map is not None else populate_label_map(self, self.args)
        )

    def rebase(self, pre: SQLNode) -> "SQLNode":
        return self.__class__(*self.args, over=_rebase_node(self.over, pre))

    @check_repr_context
    def pretty_repr(self, ctx: QuoteContext) -> "Doc":
        name = "Bind"
        args = []
        if len(self.args) == 0:
            args.append(assg_expr("args", to_doc("[]")))
        else:
            for arg in self.args:
                args.append(to_doc(arg, ctx))

        ex = call_expr(name, args)
        if self.over is not None:
            ex = pipe_expr(to_doc(self.over, ctx), ex)
        return ex


class Define(TabularNode):
    """Define node add or replaces columns in the output"""

    args: list[SQLNode]
    over: Optional[SQLNode]
    label_map: dict[Symbol, int]

    def __init__(
        self,
        *args: SQLNode,
        over: Optional[SQLNode] = None,
        label_map: Optional[dict[Symbol, int]] = None,
    ) -> None:
        super().__init__()
        self.args = [_cast_to_node(arg) for arg in args]
        self.over = _cast_to_node_skip_none(over)
        self.label_map = (
            label_map if label_map is not None else populate_label_map(self, self.args)
        )

    def rebase(self, pre: SQLNode) -> "SQLNode":
        return self.__class__(
            *self.args, over=_rebase_node(self.over, pre), label_map=self.label_map
        )

    @check_repr_context
    def pretty_repr(self, ctx: QuoteContext) -> "Doc":
        name = "Define"
        args = [to_doc(arg, ctx) for arg in self.args]

        ex = call_expr(name, args)
        if self.over is not None:
            ex = pipe_expr(to_doc(self.over, ctx), ex)
        return ex


class From(TabularNode):
    source: Union[Symbol, SQLTable, ValuesTable, None]

    def __init__(
        self, source: Union[Symbol, SQLTable, ValuesTable, None] = None
    ) -> None:
        super().__init__()
        if isinstance(source, (SQLTable, ValuesTable)) or source is None:
            self.source = source
        else:
            self.source = S(source)

    @check_repr_context
    def pretty_repr(self, ctx: QuoteContext) -> "Doc":
        name = "From"
        args = []

        if isinstance(self.source, SQLTable):
            alias = ctx.vars_.get(self.source, None)
            if alias is None:
                args.append(to_doc(self.source, QuoteContext(limit=True)))
            else:
                args.append(str(alias))
        elif isinstance(self.source, Symbol):
            args.append(str(self.source))
        elif isinstance(self.source, ValuesTable):
            if ctx.limit:
                args.append("...")
            else:
                for col in self.source.columns:
                    args.append(assg_expr(str(col), "[...]"))

        return call_expr(name, args)


class Fun(SQLNode, metaclass=ExtendAttrs):
    _name: Symbol
    args: list[SQLNode]

    def __init__(self, name: Union[str, Symbol], *args: SQLNode) -> None:
        super().__init__()
        self._name = S(name)
        self.args = [_cast_to_node(arg) for arg in args]

    @check_repr_context
    def pretty_repr(self, ctx: QuoteContext) -> "Doc":
        name = "Fun"
        args = []

        _name_str = str(self._name)
        if _name_str.isalpha():
            name = f"Fun.{_name_str}"  # Fun.exists
        else:
            name = f'Fun."{_name_str}"'  # Fun.">"

        for arg in self.args:
            args.append(to_doc(arg, ctx))
        return call_expr(name, args)


class Get(SQLNode, metaclass=ExtendAttrsFull):
    """`Get` node creates a column/table reference."""

    _name: Symbol
    over: Optional[SQLNode]

    def __init__(
        self, name: Union[str, Symbol], over: Optional[SQLNode] = None
    ) -> None:
        super().__init__()
        self._name = S(name)
        self.over = _cast_to_node_skip_none(over)

    def __getattr__(self, name: str) -> "Get":
        return self.__class__(name=S(name), over=self)

    def rebase(self, pre: SQLNode) -> "SQLNode":
        return self.__class__(name=self._name, over=_rebase_node(self.over, pre))

    @check_repr_context
    def pretty_repr(self, ctx: QuoteContext) -> "Doc":
        path = [self._name]
        over = self.over

        while over is not None and isinstance(over, Get) and over not in ctx.vars_:
            path.append(over._name)
            over = over.over
        if over is not None and over in ctx.vars_:
            path.append(ctx.vars_[over])
            over = None
        else:
            path.append(S("Get"))

        ex = ".".join(str(p) for p in reversed(path))
        if over is not None:
            ex = pipe_expr(to_doc(over, ctx), ex)
        return ex


class Group(TabularNode):
    by: list[SQLNode]
    over: Optional[SQLNode]
    label_map: dict[Symbol, int]

    def __init__(
        self,
        *by: SQLNode,
        over: Optional[SQLNode] = None,
        label_map: Optional[dict[Symbol, int]] = None,
    ) -> None:
        super().__init__()
        self.by = [_cast_to_node(arg) for arg in by]
        self.over = _cast_to_node_skip_none(over)
        self.label_map = (
            label_map if label_map is not None else populate_label_map(self, self.by)
        )

    def rebase(self, pre: SQLNode) -> "SQLNode":
        return self.__class__(
            *self.by, over=_rebase_node(self.over, pre), label_map=self.label_map
        )

    @check_repr_context
    def pretty_repr(self, ctx: QuoteContext) -> "Doc":
        name = "Group"
        args = [to_doc(arg, ctx) for arg in self.by]

        ex = call_expr(name, args)
        if self.over is not None:
            ex = pipe_expr(to_doc(self.over, ctx), ex)
        return ex


class Iterate(TabularNode):
    """Iterate generates the concatenated output of an iterated query"""

    iterator: SQLNode
    over: Optional[SQLNode]

    def __init__(self, iterator: SQLNode, over: Optional[SQLNode] = None) -> None:
        super().__init__()
        self.iterator = _cast_to_node(iterator)
        self.over = _cast_to_node_skip_none(over)

    def rebase(self, pre: SQLNode) -> "SQLNode":
        return self.__class__(iterator=self.iterator, over=_rebase_node(self.over, pre))

    @check_repr_context
    def pretty_repr(self, ctx: QuoteContext) -> "Doc":
        name = "Iterate"
        args = [to_doc(self.iterator, ctx)]

        ex = call_expr(name, args)
        if self.over is not None:
            ex = pipe_expr(to_doc(self.over, ctx), ex)
        return ex


class Join(TabularNode):
    """Join generates the output of a join operation between two tables.

    NOTE: `skip` set to True leaves out the right side of the join if it doesn't contribute
    any column refs downstream. It isn't really desirable though, like the Join condition
    could have a dependence on it.
    """

    joinee: SQLNode
    on: SQLNode
    left: bool
    right: bool
    skip: bool
    over: Optional[SQLNode]

    def __init__(
        self,
        joinee: SQLNode,
        on: SQLNode,
        left: bool = False,
        right: bool = False,
        skip: bool = False,
        over: Optional[SQLNode] = None,
    ) -> None:
        super().__init__()
        self.joinee = _cast_to_node(joinee)
        self.on = _cast_to_node(on)
        self.left = left
        self.right = right
        self.skip = skip
        self.over = _cast_to_node_skip_none(over)

    def rebase(self, pre: SQLNode) -> "SQLNode":
        return self.__class__(
            joinee=self.joinee,
            on=self.on,
            left=self.left,
            right=self.right,
            skip=self.skip,
            over=_rebase_node(self.over, pre),
        )

    @check_repr_context
    def pretty_repr(self, ctx: QuoteContext) -> "Doc":
        name = "Join"
        args = []

        if ctx.limit:
            args.append("...")
        else:
            args.append(to_doc(self.joinee, ctx))
            args.append(to_doc(self.on, ctx))
            if self.left:
                args.append(assg_expr("left", "True"))
            if self.right:
                args.append(assg_expr("right", "True"))
            if self.skip:
                args.append(assg_expr("skip", "True"))

        ex = call_expr(name, args)
        if self.over is not None:
            ex = pipe_expr(to_doc(self.over, ctx), ex)
        return ex


class Limit(TabularNode):
    limit: Optional[int]
    offset: Optional[int]
    over: Optional[SQLNode]

    def __init__(
        self,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        over: Optional[SQLNode] = None,
    ) -> None:
        super().__init__()
        self.limit = limit
        self.offset = offset
        self.over = over

    def rebase(self, pre: SQLNode) -> "SQLNode":
        return self.__class__(
            self.limit, offset=self.offset, over=_rebase_node(self.over, pre)
        )

    @check_repr_context
    def pretty_repr(self, ctx: QuoteContext) -> "Doc":
        name = "Limit"
        args = []

        if self.limit is not None:
            args.append(to_doc(self.limit))
        if self.offset is not None:
            args.append(assg_expr("offset", to_doc(self.offset)))

        ex = call_expr(name, args)
        if self.over is not None:
            ex = pipe_expr(to_doc(self.over, ctx), ex)
        return ex


class Lit(SQLNode):
    val: Any

    def __init__(self, val: Any) -> None:
        super().__init__()
        assert isinstance(
            val, LITERAL_TYPES
        ), f"Unexpected object of type: {type(val)} being cast as a literal"
        self.val = val

    @check_repr_context
    def pretty_repr(self, ctx: QuoteContext) -> "Doc":
        val = to_doc(self.val)
        return f"Lit({val})"


class Order(TabularNode):
    by: list[SQLNode]
    over: Optional[SQLNode]

    def __init__(
        self,
        *by: SQLNode,
        over: Optional[SQLNode] = None,
    ) -> None:
        super().__init__()
        self.by = [_cast_to_node(arg) for arg in by]
        self.over = _cast_to_node_skip_none(over)

    def rebase(self, pre: SQLNode) -> "SQLNode":
        return self.__class__(*self.by, over=_rebase_node(self.over, pre))

    @check_repr_context
    def pretty_repr(self, ctx: QuoteContext) -> "Doc":
        name = "Order"
        args = []

        if len(self.by) == 0:
            args.append(assg_expr("by", to_doc("[]")))
        else:
            for arg in self.by:
                args.append(to_doc(arg, ctx))

        ex = call_expr(name, args)
        if self.over is not None:
            ex = pipe_expr(to_doc(self.over, ctx), ex)
        return ex


class Partition(TabularNode):
    by: list[SQLNode]
    order_by: list[SQLNode]
    frame: Optional[Frame]
    over: Optional[SQLNode]

    def __init__(
        self,
        *by: SQLNode,
        order_by: Optional[list[SQLNode]] = None,
        frame: Optional[Frame] = None,
        over: Optional[SQLNode] = None,
    ) -> None:
        super().__init__()
        self.by = [_cast_to_node(arg) for arg in by]
        self.order_by = (
            [] if order_by is None else [_cast_to_node(arg) for arg in order_by]
        )
        self.frame = frame
        self.over = _cast_to_node_skip_none(over)

    def rebase(self, pre: SQLNode) -> "SQLNode":
        return self.__class__(
            *self.by,
            order_by=self.order_by,
            frame=self.frame,
            over=_rebase_node(self.over, pre),
        )

    @check_repr_context
    def pretty_repr(self, ctx: QuoteContext) -> "Doc":
        name = "Partition"
        args = []

        for a in self.by:
            args.append(to_doc(a, ctx))
        if len(self.order_by) > 0:
            args.append(
                assg_expr(
                    "order_by", list_expr([to_doc(a, ctx) for a in self.order_by])
                )
            )
        if self.frame is not None:
            args.append(assg_expr("frame", to_doc(self.frame)))

        ex = call_expr(name, args)
        if self.over is not None:
            ex = pipe_expr(to_doc(self.over, ctx), ex)
        return ex


class Select(TabularNode):
    args: list[SQLNode]
    over: Optional[SQLNode]
    label_map: dict[Symbol, int]

    def __init__(
        self,
        *args: SQLNode,
        over: Optional[SQLNode] = None,
        label_map: Optional[dict[Symbol, int]] = None,
    ) -> None:
        super().__init__()
        self.args = [_cast_to_node(arg) for arg in args]
        self.over = _cast_to_node_skip_none(over)
        self.label_map = (
            label_map if label_map is not None else populate_label_map(self, self.args)
        )

    def rebase(self, pre: SQLNode) -> "SQLNode":
        return self.__class__(
            *self.args,
            label_map=self.label_map,
            over=_rebase_node(self.over, pre),
        )

    @check_repr_context
    def pretty_repr(self, ctx: QuoteContext) -> "Doc":
        name = "Select"
        args = []

        if len(self.args) == 0:
            args.append(assg_expr("args", to_doc("[]")))
        else:
            for arg in self.args:
                args.append(to_doc(arg, ctx))

        ex = call_expr(name, args)
        if self.over is not None:
            ex = pipe_expr(to_doc(self.over, ctx), ex)
        return ex


class Sort(SQLNode):
    value: ValueOrder
    nulls: Optional[NullsOrder]
    over: Optional[SQLNode]

    def __init__(
        self,
        value: ValueOrder,
        nulls: Optional[NullsOrder] = None,
        over: Optional[SQLNode] = None,
    ) -> None:
        super().__init__()
        self.value = value
        self.nulls = nulls
        self.over = _cast_to_node_skip_none(over)

    def rebase(self, pre: SQLNode) -> "SQLNode":
        return self.__class__(
            value=self.value, nulls=self.nulls, over=_rebase_node(self.over, pre)
        )

    @check_repr_context
    def pretty_repr(self, ctx: QuoteContext) -> "Doc":
        name = "Sort"
        args = []

        args.append(self.value.value)
        if self.nulls is not None:
            args.append(assg_expr("nulls", self.nulls.value))

        ex = call_expr(name, args)
        if self.over is not None:
            ex = pipe_expr(to_doc(self.over, ctx), ex)
        return ex


class Var(SQLNode, metaclass=ExtendAttrsFull):
    _name: Symbol

    def __init__(self, name: Union[str, Symbol]) -> None:
        super().__init__()
        self._name = S(name)

    @check_repr_context
    def pretty_repr(self, ctx: QuoteContext) -> "Doc":
        return f"Var.{self._name}"


class Where(TabularNode):
    """Filter the input with the given condition"""

    condition: SQLNode
    over: Optional[SQLNode]

    def __init__(self, condition: SQLNode, over: Optional[SQLNode] = None) -> None:
        super().__init__()
        self.condition = _cast_to_node(condition)
        self.over = _cast_to_node_skip_none(over)

    def rebase(self, pre: SQLNode) -> "SQLNode":
        return self.__class__(
            condition=self.condition, over=_rebase_node(self.over, pre)
        )

    @check_repr_context
    def pretty_repr(self, ctx: QuoteContext) -> "Doc":
        name = "Where"
        args = [to_doc(self.condition, ctx)]
        ex = call_expr(name, args)
        if self.over is not None:
            ex = pipe_expr(to_doc(self.over, ctx), ex)
        return ex


class WithExternal(TabularNode):
    args: list[SQLNode]
    schema: Optional[Symbol]
    handler: Callable[[SQLTable, SQLClause], None]
    label_map: dict[Symbol, int]
    over: Optional[SQLNode]

    def __init__(
        self,
        *args: SQLNode,
        schema: Optional[Symbol] = None,
        handler: Any = None,
        label_map: Optional[dict[Symbol, int]] = None,
        over: Optional[SQLNode] = None,
    ) -> None:
        super().__init__()
        self.args = [_cast_to_node(arg) for arg in args]
        self.schema = schema
        self.handler = handler
        self.over = _cast_to_node_skip_none(over)
        self.label_map = (
            label_map if label_map is not None else populate_label_map(self, self.args)
        )

    def rebase(self, pre: SQLNode) -> "SQLNode":
        # NOTE: hmm, not passing over label_map
        return self.__class__(
            *self.args,
            schema=self.schema,
            handler=self.handler,
            over=_rebase_node(self.over, pre),
        )

    @check_repr_context
    def pretty_repr(self, ctx: QuoteContext) -> "Doc":
        name = "WithExternal"
        args = []

        if len(self.args) == 0:
            args.append(assg_expr("args", to_doc("[]")))
        else:
            for a in self.args:
                args.append(to_doc(a, ctx))
        if self.schema is not None:
            args.append(assg_expr("schema", str(self.schema)))
        if self.handler is not None:
            args.append(assg_expr("handler", self.handler.__name__))

        ex = call_expr(name, args)
        if self.over is not None:
            ex = pipe_expr(to_doc(self.over, ctx), ex)
        return ex


class With(TabularNode):
    args: list[SQLNode]
    materialized: Optional[bool]
    label_map: dict[Symbol, int]
    over: Optional[SQLNode]

    def __init__(
        self,
        *args: SQLNode,
        materialized: Optional[bool] = None,
        label_map: Optional[dict[Symbol, int]] = None,
        over: Optional[SQLNode] = None,
    ) -> None:
        super().__init__()
        self.args = [_cast_to_node(arg) for arg in args]
        self.materialized = materialized
        self.over = _cast_to_node_skip_none(over)
        self.label_map = (
            label_map if label_map is not None else populate_label_map(self, self.args)
        )

    def rebase(self, pre: SQLNode) -> "SQLNode":
        return self.__class__(
            *self.args,
            materialized=self.materialized,
            over=_rebase_node(self.over, pre),
        )

    @check_repr_context
    def pretty_repr(self, ctx: QuoteContext) -> "Doc":
        name = "With"
        args = []

        if len(self.args) == 0:
            args.append(assg_expr("args", to_doc("[]")))
        else:
            for a in self.args:
                args.append(to_doc(a, ctx))
        if self.materialized is not None:
            args.append(assg_expr("materialized", to_doc(self.materialized)))

        ex = call_expr(name, args)
        if self.over is not None:
            ex = pipe_expr(to_doc(self.over, ctx), ex)
        return ex


# -----------------------------------------------------------
# utility functions to export
# -----------------------------------------------------------


def Asc(nulls: Optional[NullsOrder] = None) -> Sort:
    """Shorthand to create a Sort node with ASC direction"""
    return Sort(ValueOrder.ASC, nulls=nulls)


def Desc(nulls: Optional[NullsOrder] = None) -> Sort:
    """Shorthand to create a Sort node with DESC direction"""
    return Sort(ValueOrder.DESC, nulls=nulls)


def aka(*args, **kwargs) -> SQLNode:
    """Shorthand to create an alias for a node. To be used as:

    >>> aka(Get.person, S.person_id)
    >>> aka(100, "count")
    >>> aka(count=100)
    >>> aka(sum=Agg.sum(Get.population))
    etc.
    """
    assert (
        len(args) == 0 or len(kwargs) == 0
    ), "only one of args or kwargs should be passed"

    if len(kwargs) == 1:
        name, node_like = next(iter(kwargs.items()))
    elif len(args) == 2:
        node_like, name = args
    else:
        raise Exception("Invalid arguments passed to the aliasing routine `aka`")
    return As(name=S(name), over=_cast_to_node(node_like))


class F:
    """Shorthand to pass arguments to window frame objects"""

    ROWS = FrameMode.ROWS
    RANGE = FrameMode.RANGE
    GROUPS = FrameMode.GROUPS
    EXCL_CURR = FrameExclude.CURRENT_ROW
    EXCL_GROUP = FrameExclude.GROUP
    EXCL_TIES = FrameExclude.TIES

    @staticmethod
    def curr_row() -> FrameEdge:
        return FrameEdge(FrameEdgeSide.CURRENT_ROW)

    @staticmethod
    def pre(val: Union[None, Any] = None) -> FrameEdge:
        return FrameEdge(FrameEdgeSide.PRECEDING, val)

    @staticmethod
    def follow(val: Union[None, Any] = None) -> FrameEdge:
        return FrameEdge(FrameEdgeSide.FOLLOWING, val)


# -----------------------------------------------------------
# label method implemented for nodes
# -----------------------------------------------------------


@register_union_type(label)
def _(node: Union[Agg, Fun, Get]) -> Symbol:
    return node._name


@label.register
def _(node: As) -> Symbol:
    return node.name


@label.register
def _(node: Append) -> Symbol:
    lbl = label(node.over)
    if all(label(arg) == lbl for arg in node.args):
        return lbl
    else:
        return S("union")


@label.register
def _(node: From) -> Symbol:
    if isinstance(node.source, SQLTable):
        return node.source.name
    elif isinstance(node.source, Symbol):
        return node.source
    elif isinstance(node.source, dict):
        return S("values")
    else:
        return label(None)


@label.register
def _(node: Lit) -> Symbol:
    return label(None)


@register_union_type(label)
def _(node: Union[Bind, Define, Group, Iterate, Join, Limit, Order]) -> Symbol:
    return label(node.over)


@register_union_type(label)
def _(node: Union[Partition, Select, Sort, Var, Where, With, WithExternal]) -> Symbol:
    return label(node.over)
