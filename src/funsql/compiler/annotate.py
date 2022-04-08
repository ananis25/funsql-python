"""
This module implements the annotation pass over the SQLNode expression. 
Tabular nodes are boxed, `Get` references are reversed, and some nodes 
are replaced with or split into new nodes to make validating reference easier. 
"""

from contextlib import contextmanager
from functools import singledispatch
from typing import Any, Optional, Mapping, Union, overload

from ..common import Symbol, register_union_type
from ..nodedefs import *
from ..nodes import *
from ..prettier import (
    Doc,
    QuoteContext,
    call_expr,
    assg_expr,
    list_expr,
    pipe_expr,
    to_doc,
)
from ..sqlcontext import SQLCatalog, SQLTable
from .types import *


__all__ = [
    "Box",
    "NameBound",
    "HandleBound",
    "FromNothing",
    "FromTable",
    "FromReference",
    "FromValues",
    "IntBind",
    "IntIterate",
    "Knot",
    "IntJoin",
    "AnnotateContext",
    "box_type",
    "annotate",
]


def _rebase_node(curr: Optional[SQLNode], pre: SQLNode) -> SQLNode:
    """shorthand for a common pattern"""
    return pre if curr is None else curr.rebase(pre)


# -----------------------------------------------------------
# Auxiliary node definitions
# -----------------------------------------------------------


class Box(TabularNode):
    """Represents a SQL query node with SELECT args undetermined. A box
    node wraps a regular SQLNode object, and is a container for information
    like, the column/table references available at the node which can be
    accessed downstream, pointers to other nodes referred by it, etc.

    Attributes:
    - typ: encapsulated the types of references available at this node
    - handle: Filled in during the `resolve` step, if the node is used as a handle
    - refs: list of SQLNode objects referred by this node
    - over: original SQLNode object boxed by this node
    """

    typ: BoxType
    handle: int
    refs: list[SQLNode]
    over: Optional[SQLNode]

    def __init__(
        self,
        typ: BoxType = EMPTY_BOX,
        handle: int = 0,
        refs: Optional[list[SQLNode]] = None,
        over: Optional[SQLNode] = None,
    ) -> None:
        super().__init__()
        self.typ = typ
        self.handle = handle
        self.refs = refs if refs is not None else []
        self.over = over

    def rebase(self, pre: TabularNode) -> "Box":
        return Box(self.typ, self.handle, self.refs, _rebase_node(self.over, pre))

    @check_repr_context
    def pretty_repr(self, ctx: QuoteContext) -> Doc:
        name = "Box"
        args = []

        if ctx.limit:
            args.append("...")
        else:
            if self.typ != EMPTY_BOX:
                args.append(assg_expr("type", to_doc(self.typ)))
            if self.handle > 0:
                args.append(assg_expr("handle", str(self.handle)))
            if len(self.refs) > 0:
                args.append(
                    assg_expr("refs", list_expr([to_doc(r, ctx) for r in self.refs]))
                )

        ex = call_expr(name, args)
        if self.over is not None:
            ex = pipe_expr(to_doc(self.over, ctx), ex)
        return ex


def box_type(node: Union[None, SQLNode, Box]) -> BoxType:
    return node.typ if isinstance(node, Box) else EMPTY_BOX


class NameBound(SQLNode):
    """Represents a hierarchical Get node, obtained by inverting a regular
    Get node. The node `Get.a.b` is transformed as:

    `Get(over = Get(S.a), name = S.b) => NameBound(over = Get(S.b), name = S.a)`

    TODO: what happens with Get nodes that are not hierarchical?
    """

    over: SQLNode
    name: Symbol

    def __init__(self, over: SQLNode, name: Symbol) -> None:
        super().__init__()
        self.over = over
        self.name = name

    @check_repr_context
    def pretty_repr(self, ctx: QuoteContext) -> Doc:
        name = "NameBound"
        args = []

        args.append(assg_expr("over", to_doc(self.over, ctx)))
        args.append(assg_expr("name", str(self.name)))
        return call_expr(name, args)


class HandleBound(SQLNode):
    """Represents an identifier bound to a regular SQL node, obtained by inverting a
    bound reference. The node `q.a` is tranformed as:

    `Get(over = q, name = S.a) => HandleBound(over = Get(S.a), handle = get_handle(q))`
    """

    over: SQLNode
    handle: int

    def __init__(self, over: SQLNode, handle: int) -> None:
        super().__init__()
        self.over = over
        self.handle = handle

    @check_repr_context
    def pretty_repr(self, ctx: QuoteContext) -> Doc:
        name = "HandleBound"
        args = []

        args.append(assg_expr("over", to_doc(self.over, ctx)))
        args.append(assg_expr("handle", str(self.handle)))
        return call_expr(name, args)


# A From node is specialized to one of:
# - FromNothing
# - FromTable
# - FromReference
# - FromValues


class FromNothing(TabularNode):
    """FromNothing is a stand-in for a From node without a reference."""

    def __init__(self) -> None:
        super().__init__()

    @check_repr_context
    def pretty_repr(self, ctx: QuoteContext) -> Doc:
        return call_expr("FromNothing", [])


class FromTable(TabularNode):
    """FromTable is a stand-in for a From node with a table as source."""

    table: SQLTable

    def __init__(self, table: SQLTable) -> None:
        super().__init__()
        self.table = table

    @check_repr_context
    def pretty_repr(self, ctx: QuoteContext) -> Doc:
        name = "FromTable"
        args = []

        alias = ctx.vars_.get(self.table, None)
        if alias is not None:
            args.append(str(alias))
        else:
            args.append(to_doc(self.table, QuoteContext(limit=True)))
        return call_expr(name, args)


class FromReference(TabularNode):
    """FromReference is a stand-in for a From node referencing a source through an alias."""

    over: SQLNode
    name: Symbol

    def __init__(self, over: SQLNode, name: Symbol) -> None:
        super().__init__()
        self.over = over
        self.name = name

    @check_repr_context
    def pretty_repr(self, ctx: QuoteContext) -> Doc:
        name = "FromReference"
        args = []

        args.append(assg_expr("name", str(self.name)))
        args.append(assg_expr("over", to_doc(self.over, ctx)))
        return call_expr(name, args)


class FromValues(TabularNode):
    """FromValues is a stand-in for a From node with a list of values as source."""

    source: ValuesTable

    def __init__(self, source: ValuesTable) -> None:
        super().__init__()
        self.source = source

    @check_repr_context
    def pretty_repr(self, ctx: QuoteContext) -> Doc:
        name = "FromValues"
        args = []

        for col in self.source.columns:
            args.append(assg_expr(str(col), "[...]"))
        return call_expr(name, args)


class IntBind(SQLNode):
    """Substitute for a Bind node after the annotate step."""

    args: list[SQLNode]
    over: Optional[SQLNode]
    label_map: dict[Symbol, int]
    owned: bool  # is the outer query for this node resolved?

    def __init__(
        self,
        args: list[SQLNode],
        owned: bool = False,
        label_map: Optional[dict[Symbol, int]] = None,
        over: Optional[SQLNode] = None,
    ) -> None:
        super().__init__()
        self.args = list(args)
        self.owned = owned
        self.over = over
        self.label_map = (
            populate_label_map(self, self.args) if label_map is None else label_map
        )

    @check_repr_context
    def pretty_repr(self, ctx: QuoteContext) -> Doc:
        name = "IntBind"
        args = []

        if len(self.args) == 0:
            args.append(assg_expr("args", to_doc("[]")))
        else:
            for arg in self.args:
                args.append(to_doc(arg, ctx))
        args.append(assg_expr("owned", str(self.owned)))

        ex = call_expr(name, args)
        if self.over is not None:
            ex = pipe_expr(to_doc(self.over, ctx), ex)
        return ex

    def rebase(self, pre: SQLNode) -> SQLNode:
        return IntBind(
            args=self.args,
            owned=self.owned,
            label_map=self.label_map,
            over=_rebase_node(self.over, pre),
        )


class Knot(TabularNode):
    """An Iterate node (recursive UNION ALL) is split into a Knot and an IntIterate node."""

    name: Symbol  # label of the iterator node
    box: Box  # reference to the Box node wrapping this
    iterator: SQLNode
    iterator_boxes: list[Box]
    over: Optional[SQLNode]

    def __init__(
        self,
        iterator: SQLNode,
        box: Box,
        name: Optional[Symbol] = None,
        iterator_boxes: Optional[list[Box]] = None,
        over: Optional[SQLNode] = None,
    ):
        super().__init__()
        self.iterator = iterator
        self.box = box
        self.name = label(iterator) if name is None else name
        self.iterator_boxes = [] if iterator_boxes is None else iterator_boxes
        self.over = over

    @check_repr_context
    def pretty_repr(self, ctx: QuoteContext) -> Doc:
        name = "Knot"
        args = []

        if ctx.limit:
            args.append("...")
        else:
            args.append(to_doc(self.iterator, ctx))
        args.append(assg_expr("name", str(self.name)))
        if ctx.limit:
            args.append("...")
        else:
            args.append(to_doc(self.iterator, ctx))
            boxes = list_expr([to_doc(b, ctx) for b in self.iterator_boxes])
            args.append(assg_expr("iterator_boxes", boxes))

        ex = call_expr(name, args)
        if self.over is not None:
            ex = pipe_expr(to_doc(self.over, ctx), ex)
        return ex

    def rebase(self, pre: SQLNode) -> SQLNode:
        return Knot(
            iterator=self.iterator,
            box=self.box,
            name=self.name,
            iterator_boxes=self.iterator_boxes,
            over=_rebase_node(self.over, pre),
        )


class IntIterate(TabularNode):
    """An Iterate node (recursive UNION ALL) is split into a Knot and an IntIterate node."""

    name: Symbol  # label of the parent to Iterate node
    iterator_name: Symbol  # label of the iterator
    over: Optional[SQLNode]

    def __init__(
        self, name: Symbol, iterator_name: Symbol, over: Optional[SQLNode] = None
    ):
        super().__init__()
        self.name = name
        self.iterator_name = iterator_name
        self.over = over

    @check_repr_context
    def pretty_repr(self, ctx: QuoteContext) -> Doc:
        name = "IntIterate"
        args = []
        args.append(assg_expr("name", str(self.name)))
        args.append(assg_expr("iterator_name", str(self.iterator_name)))

        ex = call_expr(name, args)
        if self.over is not None:
            ex = pipe_expr(to_doc(self.over, ctx), ex)
        return ex

    def rebase(self, pre: SQLNode) -> SQLNode:
        return IntIterate(
            name=self.name,
            iterator_name=self.iterator_name,
            over=_rebase_node(self.over, pre),
        )


class IntJoin(TabularNode):
    joinee: SQLNode
    on: SQLNode
    left: bool
    right: bool
    skip: bool
    typ: BoxType
    lateral: list[SQLNode]
    over: Optional[SQLNode]

    def __init__(
        self,
        joinee: SQLNode,
        on: SQLNode,
        left: bool = False,
        right: bool = False,
        skip: bool = False,
        typ: BoxType = EMPTY_BOX,
        lateral: Optional[list[SQLNode]] = None,
        over: Optional[SQLNode] = None,
    ):
        super().__init__()
        self.joinee = joinee
        self.on = on
        self.left = left
        self.right = right
        self.skip = skip
        self.typ = typ
        self.lateral = [] if lateral is None else lateral
        self.over = over

    @check_repr_context
    def pretty_repr(self, ctx: QuoteContext) -> Doc:
        name = "IntJoin"
        args = []

        if ctx.limit:
            args.append("...")
        else:
            args.append(to_doc(self.joinee, ctx))
            args.append(to_doc(self.on, ctx))
            if self.left:
                args.append(assg_expr("left", str(self.left)))
            if self.right:
                args.append(assg_expr("right", str(self.right)))
            if self.skip:
                args.append(assg_expr("skip", str(self.skip)))
            lateral_args = list_expr([to_doc(x, ctx) for x in self.lateral])
            if len(self.lateral) > 0:
                args.append(assg_expr("lateral", lateral_args))

        ex = call_expr(name, args)
        if self.over is not None:
            ex = pipe_expr(to_doc(self.over, ctx), ex)
        return ex

    def rebase(self, pre: SQLNode) -> SQLNode:
        return IntJoin(
            joinee=self.joinee,
            on=self.on,
            left=self.left,
            right=self.right,
            skip=self.skip,
            typ=self.typ,
            lateral=self.lateral,
            over=_rebase_node(self.over, pre),
        )


# -----------------------------------------------------------
# label method implemented for auxiliary nodes
# -----------------------------------------------------------


@register_union_type(label)
def _(node: Union[IntJoin, IntBind, HandleBound, NameBound]) -> Symbol:
    return label(node.over)


@register_union_type(label)
def _(node: Union[IntIterate, Knot]) -> Symbol:
    return node.name


@label.register
def _(node: Box) -> Symbol:
    return node.typ.name


# -----------------------------------------------------------
# Implements the Annotation context and annotate methods for
# each node
# -----------------------------------------------------------

NEGATIVE_INT = -1000  # placeholder negative value as standin for missing index


class PathMap:
    """Maps a node in the annotated graph to a path in the original node graph,
    so errors can be highlighted.

    Attributes:
        - paths: keeps a list of all nodes in the original graph as they are annotated
        - origins: tracks an index to the original node for each annotated/box node
    """

    paths: list[tuple[SQLNode, int]]
    origins: dict[Any, int]

    def __init__(self) -> None:
        self.paths = []
        self.origins = dict()

    def get_path(self, idx_or_node: Union[int, SQLNode]) -> list[SQLNode]:
        idx = (
            idx_or_node
            if isinstance(idx_or_node, int)
            else self.origins.get(idx_or_node, NEGATIVE_INT)
        )
        path = []
        # recursively fetch all nodes till the root
        while idx >= 0:
            node, idx = self.paths[idx]
            path.append(node)
        return path


class AnnotateContext:
    """Container for the data collected during the annotation step

    Attributes:
        - catalog: SQL context during the compile pass
        - path_map: PathMap
        - current_path: stack like variable, tracks all nodes from the root to the current node
        - handles: keeps an index of all original nodes used for bound references
        - boxes: list of all box nodes created during the annotate pass
        - cte_nodes: list of all CTE nodes encountered during the annotate pass
    """

    catalog: SQLCatalog
    path_map: PathMap
    current_path: list[int]
    handles: dict[SQLNode, int]
    boxes: list[Box]
    cte_nodes: dict[Symbol, SQLNode]

    def __init__(self, catalog: SQLCatalog) -> None:
        self.catalog = catalog
        self.path_map = PathMap()
        self.current_path = [NEGATIVE_INT]
        self.handles = dict()
        self.boxes = []
        self.cte_nodes = dict()

    @contextmanager
    def extend_cte_nodes(self, more_nodes: Mapping[Symbol, SQLNode]):
        """Contex manager for extending the container of cte_nodes with new labels,
        and restoring the previous value on exit.
        """
        old_nodes = self.cte_nodes
        self.cte_nodes = {**old_nodes, **more_nodes}
        yield
        self.cte_nodes = old_nodes

    @contextmanager
    def extend_path(self, node: SQLNode):
        """Context manager for growing the current path when annotating a new node,
        and shrinking it back after.
        """
        self.path_map.paths.append((node, self.current_path[-1]))
        self.current_path.append(len(self.path_map.paths) - 1)
        yield
        self.current_path.pop()

    def mark_origin(self, node: SQLNode) -> None:
        self.path_map.origins[node] = self.current_path[-1]

    def get_path(self, node: Optional[SQLNode] = None) -> list[SQLNode]:
        dest = self.current_path[-1] if node is None else node
        return self.path_map.get_path(dest)

    def make_handle(self, node: SQLNode) -> int:
        if node not in self.handles:
            self.handles[node] = len(self.handles)
        return self.handles[node]

    def get_handle(self, node: Optional[SQLNode]) -> int:
        """
        Args:
            A boxed node
        Returns:
            If the original node was used as a handle, return its index in the handles list.
        """
        handle = NEGATIVE_INT
        if node is not None:
            idx = self.path_map.origins.get(node, NEGATIVE_INT)
            if idx >= 0:
                origin_node = self.path_map.paths[idx][0]
                handle = self.handles.get(origin_node, NEGATIVE_INT)
        return handle


# -----------------------------------------------------------
# Rewriting the node graph
# -----------------------------------------------------------


@overload
def annotate(node: None, ctx: AnnotateContext) -> Box:
    ...


@overload
def annotate(node: SQLNode, ctx: AnnotateContext) -> Box:
    ...


@overload
def annotate(node: list[SQLNode], ctx: AnnotateContext) -> list[Box]:
    ...


def annotate(
    node: Union[None, SQLNode, list[SQLNode]], ctx: AnnotateContext
) -> Union[Box, list[Box]]:
    """Annotate a tabular node, by delegating to the type specific annotation
    routine first and then wrapping the output in a box node.
    """
    if isinstance(node, list):
        assert all(isinstance(x, SQLNode) for x in node), "not all nodes are SQLNodes"
        return [annotate(x, ctx) for x in node]

    elif node is None:
        box = Box(over=None)
        ctx.boxes.append(box)
        ctx.mark_origin(box)
        return box

    else:
        # TODO: this isn't necessary, just to help me debug errors early
        if not isinstance(node, (TabularNode, As, Bind)):
            raise ErrIllFormed(path=ctx.get_path())
        with ctx.extend_path(node):
            node_p = annotate_node(node, ctx)
            ctx.mark_origin(node_p)

            box = Box(over=node_p)
            ctx.boxes.append(box)
            ctx.mark_origin(box)
        return box


@overload
def annotate_scalar(node: None, ctx: AnnotateContext) -> None:
    ...


@overload
def annotate_scalar(node: SQLNode, ctx: AnnotateContext) -> SQLNode:
    ...


@overload
def annotate_scalar(node: list[SQLNode], ctx: AnnotateContext) -> list[SQLNode]:
    ...


def annotate_scalar(
    node: Union[SQLNode, None, list[SQLNode]], ctx: AnnotateContext
) -> Union[SQLNode, None, list[SQLNode]]:
    """Annotate a scalar node, by delegating to the type specific annotation routine.

    NOTE: generic typing using TypeVar refuses to cooperate, so we have to take the
    longer `overload` route to type the function correctly.
    """
    if isinstance(node, list):
        assert all(isinstance(x, SQLNode) for x in node), "not all nodes are SQLNodes"
        return [annotate_scalar(x, ctx) for x in node]

    elif node is None:
        return None

    elif isinstance(node, TabularNode):
        return annotate(node, ctx)

    else:
        with ctx.extend_path(node):
            node_p = annotate_node_scalar(node, ctx)
            ctx.mark_origin(node_p)
            return node_p


@singledispatch
def annotate_node(node: SQLNode, ctx: AnnotateContext) -> SQLNode:
    raise NotImplementedError(
        f"annotate_node is not implemented for type: {type(node)}"
    )


@singledispatch
def annotate_node_scalar(node: SQLNode, ctx: AnnotateContext) -> SQLNode:
    raise NotImplementedError(
        f"annotate_node_scalar is not implemented for type: {type(node)}"
    )


def rebind(
    parent: Optional[SQLNode], node_isolated: SQLNode, ctx: AnnotateContext
) -> SQLNode:
    """Traverse up the hierarchy and reverse the Get/Agg nodes. Takes as input
    a scalar node split into an isolated copy of it, and its parent node.

    Args:
        parent: The parent node
        node_isolated: The node isolated (i.e. with parent set to None)
        ctx: Annotation context

    Returns:
        The rebinded node
    """

    while isinstance(parent, Get):
        ctx.mark_origin(node_isolated)
        node_isolated = NameBound(over=node_isolated, name=parent._name)
        parent = parent.over
    if parent is not None:
        handle = ctx.make_handle(parent)
        ctx.mark_origin(node_isolated)
        node_isolated = HandleBound(over=node_isolated, handle=handle)
    return node_isolated


@annotate_node_scalar.register
def _(node: Agg, ctx: AnnotateContext) -> SQLNode:
    # TODO: how critical is it to annotate these attributes in order? For each type of node?
    args_p = annotate_scalar(node.args, ctx)
    filter_p = annotate_scalar(node.filter_, ctx)
    node_p = Agg(node._name, *args_p, distinct=node.distinct, filter_=filter_p)
    return rebind(node.over, node_p, ctx)


@annotate_node.register
def _(node: Append, ctx: AnnotateContext) -> SQLNode:
    over_p = annotate(node.over, ctx)
    args_p = annotate(node.args, ctx)
    return Append(*args_p, over=over_p)


@annotate_node.register
def _(node: As, ctx: AnnotateContext) -> SQLNode:
    over_p = annotate(node.over, ctx)
    return As(name=node.name, over=over_p)


@annotate_node_scalar.register
def _(node: As, ctx: AnnotateContext) -> SQLNode:
    over_p = annotate_scalar(node.over, ctx)
    return As(name=node.name, over=over_p)


@annotate_node.register
@annotate_node_scalar.register
def _(node: Bind, ctx: AnnotateContext) -> SQLNode:
    over_p = annotate(node.over, ctx)
    args_p = annotate_scalar(node.args, ctx)
    return IntBind(args=args_p, over=over_p, label_map=node.label_map)


@annotate_node.register
def _(node: Define, ctx: AnnotateContext) -> SQLNode:
    over_p = annotate(node.over, ctx)
    args_p = annotate_scalar(node.args, ctx)
    return Define(*args_p, over=over_p, label_map=node.label_map)


@annotate_node.register
def _(node: From, ctx: AnnotateContext) -> SQLNode:
    source = node.source
    if isinstance(source, SQLTable):
        # refers to a regular table object, cool
        return FromTable(table=source)
    elif isinstance(source, Symbol):
        # if it is one of the CTE nodes, use that
        over = ctx.cte_nodes.get(source, None)
        if over is not None:
            return FromReference(name=source, over=over)
        else:
            # look into the full table catalog for a table with this name
            table = ctx.catalog.get(source)
            if table is not None:
                return FromTable(table=table)
            else:
                # couldn't resolve the symbol to a known node, sad
                raise ErrReference(
                    ErrType.UNDEFINED_TABLE_REF, name=source, path=ctx.get_path()
                )
    elif isinstance(source, ValuesTable):
        # A `VALUES` query constructed in the query
        return FromValues(source)
    else:
        return FromNothing()


@annotate_node_scalar.register
def _(node: Fun, ctx: AnnotateContext) -> SQLNode:
    """Annotate all the arguments to the function node"""
    args_p = annotate_scalar(node.args, ctx)
    return Fun(node._name, *args_p)


@annotate_node_scalar.register
def _(node: Get, ctx: AnnotateContext) -> SQLNode:
    return rebind(node.over, Get(name=node._name), ctx)


@annotate_node.register
def _(node: Group, ctx: AnnotateContext) -> SQLNode:
    over_p = annotate(node.over, ctx)
    by_p = annotate_scalar(node.by, ctx)
    return Group(*by_p, over=over_p, label_map=node.label_map)


@annotate_node.register
def _(node: Iterate, ctx: AnnotateContext) -> SQLNode:
    """
    * Iterate base >> Iterate (Iterate loop)
    gets translated into:
    * Iterate base >> Knot (iterator = Iterate loop) >> IntIterate,
    Knot (Iterate loop) >> Iterate loop
    """
    over_p = annotate(node.over, ctx)
    knot_box = Box()
    knot = Knot(iterator=node.iterator, box=knot_box, over=over_p)
    ctx.mark_origin(knot)
    knot_box.over = knot
    ctx.boxes.append(knot_box)
    over_p = knot_box
    ctx.mark_origin(over_p)

    with ctx.extend_cte_nodes({knot.name: over_p}):
        start_at = len(ctx.boxes)
        iterator_p = annotate(node.iterator, ctx)
    knot.iterator = iterator_p
    knot.iterator_boxes = ctx.boxes[start_at:]
    return IntIterate(name=label(node.over), iterator_name=knot.name, over=over_p)


@annotate_node.register
def _(node: Join, ctx: AnnotateContext) -> SQLNode:
    over_p = annotate(node.over, ctx)
    joinee_p = annotate(node.joinee, ctx)
    on_p = annotate_scalar(node.on, ctx)
    return IntJoin(
        joinee=joinee_p,
        on=on_p,
        left=node.left,
        right=node.right,
        skip=node.skip,
        over=over_p,
    )


@annotate_node.register
def _(node: Limit, ctx: AnnotateContext) -> SQLNode:
    over_p = annotate(node.over, ctx)
    return Limit(limit=node.limit, offset=node.offset, over=over_p)


@annotate_node_scalar.register
def _(node: Lit, ctx: AnnotateContext) -> SQLNode:
    return node


@annotate_node.register
def _(node: Order, ctx: AnnotateContext) -> SQLNode:
    over_p = annotate(node.over, ctx)
    by_p = annotate_scalar(node.by, ctx)
    return Order(*by_p, over=over_p)


@annotate_node.register
def _(node: Partition, ctx: AnnotateContext) -> SQLNode:
    over_p = annotate(node.over, ctx)
    by_p = annotate_scalar(node.by, ctx)
    order_by_p = annotate_scalar(node.order_by, ctx)
    return Partition(*by_p, order_by=order_by_p, over=over_p, frame=node.frame)


@annotate_node.register
def _(node: Select, ctx: AnnotateContext) -> SQLNode:
    """
    NOTE: the `label_map` value doesn't need to be updated since it just maps
    the column names to an index in the `args`.
    """
    over_p = annotate(node.over, ctx)
    args_p = annotate_scalar(node.args, ctx)
    return Select(*args_p, over=over_p, label_map=node.label_map)


@annotate_node_scalar.register
def _(node: Sort, ctx: AnnotateContext) -> SQLNode:
    over_p = annotate_scalar(node.over, ctx)
    return Sort(value=node.value, nulls=node.nulls, over=over_p)


@annotate_node_scalar.register
def _(node: Var, ctx: AnnotateContext) -> SQLNode:
    return node


@annotate_node.register
def _(node: Where, ctx: AnnotateContext) -> SQLNode:
    over_p = annotate(node.over, ctx)
    condition_p = annotate_scalar(node.condition, ctx)
    return Where(condition=condition_p, over=over_p)


@annotate_node.register
def _(node: With, ctx: AnnotateContext) -> SQLNode:
    args_p = annotate(node.args, ctx)
    more_ctes = {name: args_p[val] for name, val in node.label_map.items()}
    with ctx.extend_cte_nodes(more_ctes):
        over_p = annotate(node.over, ctx)

    return With(
        *args_p, materialized=node.materialized, label_map=node.label_map, over=over_p
    )


@annotate_node.register
def _(node: WithExternal, ctx: AnnotateContext) -> SQLNode:
    args_p = annotate(node.args, ctx)
    more_ctes = {name: args_p[val] for name, val in node.label_map.items()}
    with ctx.extend_cte_nodes(more_ctes):
        over_p = annotate(node.over, ctx)

    return WithExternal(
        *args_p,
        schema=node.schema,
        handler=node.handler,
        label_map=node.label_map,
        over=over_p,
    )
