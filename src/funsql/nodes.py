import io
from enum import Enum
from functools import lru_cache, singledispatch, wraps
from typing import Any, Iterator, Optional, Type, Union, get_type_hints

from .common import OrderedSet, S, Symbol, LITERAL_TYPES, LITERAL_TYPE_SIG
from .prettier import (
    Doc,
    QuoteContext,
    assg_expr,
    get_screen_width,
    full_query_expr,
    highlight_expr,
    resolve,
    to_doc,
)
from .sqlcontext import SQLTable

__all__ = [
    "SQLNode",
    "NODE_MATERIAL",
    "TabularNode",
    "populate_label_map",
    "label",
    "check_repr_context",
    "ErrDuplicateLabel",
    "ErrIllFormed",
    "ErrRefUndefinedHandle",
    "ErrRefAmbiguousHandle",
    "ErrRefUndefinedName",
    "ErrRefAmbiguousName",
    "ErrRefUnexpectedRowType",
    "ErrRefUnexpectedScalarType",
    "ErrRefUnexpectedAgg",
    "ErrRefAmbiguousAgg",
    "ErrRefUndefinedTable",
    "ErrRefInvalidTable",
]


# -----------------------------------------------------------
# methods exported by this module
# -----------------------------------------------------------


@singledispatch
def label(node: Union["SQLNode", None]) -> Symbol:
    """Returns a default alias for the node; to use when rendering to SQL.
    The implementation for subclasses of SQLNodes is defined in `nodedefs.py`.
    """
    if node is None:
        return S("_")
    else:
        raise NotImplementedError(
            f"label isn't implemented for the SQLNode type - {type(node)}"
        )


def populate_label_map(node: "SQLNode", args: list["SQLNode"]) -> dict[Symbol, int]:
    """Validate that labels of args are unique, for Select and other nodes
    that declare new column references, and cache them.
    """
    label_map = dict[Symbol, int]()
    for i, arg in enumerate(args):
        name = label(arg)
        if name in label_map:
            raise ErrDuplicateLabel(name=name, path=[arg, node])
        label_map[name] = i
    return label_map


# -----------------------------------------------------------
# Definition of SQLNode objects
# -----------------------------------------------------------


class SQLNode:
    """A tabular or scalar operation that can be expressed as a SQL query"""

    highlight: bool  # highlight node when printing the query tree?

    def __init__(self) -> None:
        self.highlight = False

    def __repr__(self) -> str:
        """From the top level node, the tree is traversed and `linearized` to
        produce a representation with exactly one node defined on each line.
        The actual string representation for each node is implemented in the
        `pretty_repr` method.
        """

        # got too long; delegated to another function so the class definition is compact
        return repr_node(self, limit=False)

    def pretty_repr(self, ctx: QuoteContext, full: bool = False) -> "Doc":
        """Implemented by each SQLNode subclass. Returns a Doc object representing
        the SQLNode.
        """
        raise NotImplementedError(
            f"pretty_repr method isn't implemented on the SQLNode class - {type(self)}"
        )

    def rebase(self, pre: "SQLNode") -> "SQLNode":
        raise NotImplementedError(
            f"rebase isn't implemented for the SQLNode class - {type(self)}"
        )

    def __rshift__(self, other: "SQLNode") -> "SQLNode":
        """Used to compose SQL nodes"""
        if isinstance(other, SQLNode):
            return other.rebase(self)
        else:
            raise NotImplementedError(
                f">> isn't a valid operation with a SQLNode and {type(other)}"
            )


class TabularNode(SQLNode):
    """A node that can be used as a SQL subquery"""

    def __init__(self) -> None:
        super().__init__()


# For convenience, user facing SQLNode objects take python literal types as inputs
# and cast them internally to SQLNodes. This represents the allowed types.
NODE_MATERIAL = Union[SQLNode, LITERAL_TYPE_SIG]

# -----------------------------------------------------------
# pretty printing for SQLNode objects
# -----------------------------------------------------------


def _get_origin(typ: Any) -> Any:
    # HACK: looks very fragile, but `get_origin`, `get_args` are not present in python <3.8
    return typ.__origin__ if hasattr(typ, "__origin__") else None


def _get_args(typ: Any) -> tuple:
    return typ.__args__ if hasattr(typ, "__args__") else tuple()


def typ_as_str(data: Any) -> str:
    return data.__class__.__name__


@lru_cache
def get_node_refs(node_typ: Type[SQLNode]) -> tuple[tuple[str], tuple[str]]:
    """Compute attributes on a SQLNode class that point to other SQLNodes. We use
    it to implement visitor pattern for traversing the node tree. Alternatively, we
    could ask each SQLNode to specify them, but that'd be tedious.
    """

    assert issubclass(node_typ, SQLNode)

    maybe_nodes, listof_nodes = [], []
    for attr, typ in get_type_hints(node_typ).items():
        if typ == SQLNode:
            maybe_nodes.append(attr)

        container = _get_origin(typ)
        if container == Union:
            args = _get_args(typ)
            if len(args) == 2 and SQLNode in args and type(None) in args:
                maybe_nodes.append(attr)
        elif container == list and SQLNode in _get_args(typ):
            listof_nodes.append(attr)

    # we want the parent node to be visited first, which the `over` attribute points to
    if "over" in maybe_nodes:
        maybe_nodes.remove("over")
        maybe_nodes.insert(0, "over")
    return (tuple(maybe_nodes), tuple(listof_nodes))


def visit(node: SQLNode, visiting: set[SQLNode]) -> Iterator[SQLNode]:
    """recursively visit attributes that are SQLNodes."""
    if node in visiting:
        return
    visiting.add(node)

    maybe_nodes, listof_nodes = get_node_refs(type(node))
    for attr in maybe_nodes:
        _value = getattr(node, attr)
        if _value is not None:
            yield from visit(_value, visiting)
    for attr in listof_nodes:
        for _value in getattr(node, attr):
            yield from visit(_value, visiting)

    yield node
    visiting.remove(node)


def repr_node(node: SQLNode, limit: bool = False) -> str:
    """Returns a string representation of the SQLNode, with the full tree of
    nodes, linearized. That is, each table reference, tabular node, or a
    repeated node is put on a separate line. While, any references to them are
    replaced by an alias.
    """
    if limit:
        ctx = QuoteContext(limit=True)
        return resolve(to_doc(node, ctx), get_screen_width())

    tables_seen = OrderedSet[SQLTable]()
    nodes_seen = OrderedSet[SQLNode]()
    nodes_toplevel = set[SQLNode]()

    visited: set[SQLNode] = set()
    for ref in visit(node, visited):
        # HACK: we really want to do an isinstance check, but that introduces a cyclic dep.
        if typ_as_str(ref) == "From":
            if isinstance(ref.source, SQLTable):  # type: ignore
                tables_seen.add(ref.source)  # type: ignore
        elif typ_as_str(ref) == "FromTable":
            tables_seen.add(ref.table)  # type: ignore

        if isinstance(ref, TabularNode) or ref in nodes_seen:
            nodes_toplevel.add(ref)
        nodes_seen.add(ref)

    ctx = QuoteContext()
    defs = []
    if len(nodes_toplevel) >= 2 or (
        len(nodes_toplevel) == 1 and node not in nodes_toplevel
    ):
        # start by defining all the table references
        _ctx_limit = QuoteContext(limit=True)
        for tab in tables_seen:
            name = tab.name
            defs.append(assg_expr(str(name), to_doc(tab, _ctx_limit)))
            ctx.vars_[tab] = name

        # add aliases for top level nodes
        q_idx = 0
        for n in nodes_seen:
            if n in nodes_toplevel:
                q_idx += 1
                name = S(f"q{q_idx}")
                ctx.vars_[n] = name

        # add definitions for all top level nodes; child references will be replaced with aliases
        q_idx = 0
        for n in nodes_seen:
            if n in nodes_toplevel:
                q_idx += 1
                name = f"q{q_idx}"
                defs.append(assg_expr(str(name), n.pretty_repr(ctx, full=True)))

    ex = to_doc(node, ctx)
    if len(defs) > 0:
        ex = full_query_expr([*defs, ex])
    return resolve(ex, 80)


def check_repr_context(repr_func):
    """Decorator for the `pretty_repr` method on SQLNode objects. Looks into the
    Quote context to decide if the full expression needs to be printed or an alias
    with do.
    """

    @wraps(repr_func)
    def wrapped(node: SQLNode, ctx: QuoteContext, full: bool = False) -> Doc:
        if ctx.limit:
            return "..."
        else:
            var = ctx.vars_.get(node, None)
            if var is None or full:
                expr = repr_func(node, ctx)
                if node.highlight:
                    return highlight_expr(expr)
                else:
                    return expr
            else:
                return str(var)

    return wrapped


# -----------------------------------------------------------
# Errors to raise when parsing the SQL node tree
# -----------------------------------------------------------


class ErrFunSQL(BaseException):
    name: Optional[Symbol]
    path: list[SQLNode]

    def __init__(
        self,
        name: Union[str, Symbol, None] = None,
        path: Optional[list[SQLNode]] = None,
    ) -> None:
        self.name = S(name) if name is not None else None
        self.path = path if path is not None else []

    def _custom_msg(self) -> str:
        raise NotImplementedError("Custom message should be implemented in subclasses")

    def __str__(self) -> str:
        buf = io.StringIO()
        buf.write(self._custom_msg() + "\n")

        if len(self.path) > 0:
            self.path[0].highlight = True  # highlight the error site
            buf.write(f"\n{self.path[-1]}\n")
        elif isinstance(self, ErrIllFormed):
            buf.write("Top level node\n")
        else:
            buf.write("\n")
        return buf.getvalue()


class ErrDuplicateLabel(ErrFunSQL):
    def _custom_msg(self) -> str:
        return f"FunSQL.ErrDuplicateLabel: {self.name} is used more than once in:"


class ErrIllFormed(ErrFunSQL):
    def _custom_msg(self) -> str:
        return "FunSQL.ErrIllFormed: ill-formed query in:"


class ErrRefUndefinedHandle(ErrFunSQL):
    def _custom_msg(self) -> str:
        return f"FunSQL.ErrReference: node bound reference failed to resolve in:"


class ErrRefAmbiguousHandle(ErrFunSQL):
    def _custom_msg(self) -> str:
        return f"FunSQL.ErrReference: node bound reference is ambiguous in:"


class ErrRefUndefinedName(ErrFunSQL):
    def _custom_msg(self) -> str:
        return f"FunSQL.ErrReference: name {self.name} is undefined in:"


class ErrRefAmbiguousName(ErrFunSQL):
    def _custom_msg(self) -> str:
        return f"FunSQL.ErrReference: name {self.name} is ambiguous in:"


class ErrRefUnexpectedRowType(ErrFunSQL):
    def _custom_msg(self) -> str:
        return f"FunSQL.ErrReference: incomplete reference {self.name} in:"


class ErrRefUnexpectedScalarType(ErrFunSQL):
    def _custom_msg(self) -> str:
        return f"FunSQL.ErrReference: unexpected reference after {self.name} in:"


class ErrRefUnexpectedAgg(ErrFunSQL):
    def _custom_msg(self) -> str:
        return f"FunSQL.ErrReference: aggregate expression allowed only inside a Group or Partition in:"


class ErrRefAmbiguousAgg(ErrFunSQL):
    def _custom_msg(self) -> str:
        return f"FunSQL.ErrReference: ambiguous aggregate expression in:"


class ErrRefUndefinedTable(ErrFunSQL):
    def _custom_msg(self) -> str:
        return f"FunSQL.ErrReference: table reference {self.name} is undefined in:"


class ErrRefInvalidTable(ErrFunSQL):
    def _custom_msg(self) -> str:
        return f"FunSQL.ErrReference: table reference {self.name} required label using `As` in:"
