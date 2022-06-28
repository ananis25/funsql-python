"""
This module implements the `resolve` pass over the annotated SQLNode 
expression. We derive the names and types of references available at 
all the nodes, by walking the node tree till the source. Only the table/column 
references _available_ at a node can then be referred by the nodes 
downstream of it. 
"""

from typing import Any, Optional, Union
from functools import singledispatch

from .types import *
from .annotate import *
from ..common import register_union_type
from ..nodes import *
from ..nodedefs import *

__all__ = ["resolve_toplevel"]


def resolve_toplevel(ctx: AnnotateContext) -> None:
    resolve_boxes(ctx.boxes, ctx)


def resolve_boxes(boxes: list[Box], ctx: AnnotateContext) -> None:
    for box in boxes:
        if box.over is not None:
            handle_idx = ctx.get_handle(box.over)
            typ = resolve(box.over, ctx)
            typ = typ.add_handle(handle_idx)
            box.handle = handle_idx
            box.typ = typ


@singledispatch
def resolve(data: Union[TabularNode, As], ctx: AnnotateContext) -> BoxType:
    raise NotImplementedError(f"Don't know how to resolve node of type {type(data)}")


@resolve.register
def _(node: Append, ctx: AnnotateContext) -> BoxType:
    """Iterate over the nodes included in the Union operator. Only
    the references available at all the nodes can be used downstream.
    """
    t = box_type(node.over)
    for arg in node.args:
        t = intersect(t, box_type(arg))
    assert isinstance(t, BoxType)
    return t


@register_union_type(resolve)
def _(node: Union[As, Knot], ctx: AnnotateContext) -> BoxType:
    """As/Knot nodes add an indirection to the column references. For ex, considering
    the query: `From(Table[col1, col2]) >> As(table_alias)`,
    * From(...): has references to col1 and col2
    * As(...): has references to table_alias.col1 and table_alias.col2
    """
    t = box_type(node.over)
    fields: FieldTypeMap = dict()
    fields[node.name] = t.row

    row = RowType(fields)
    return BoxType(node.name, row, t.handle_map)


@resolve.register
def _(node: Define, ctx: AnnotateContext) -> BoxType:
    """Add the column references defined in this node. If the parent node
    has a reference with the same name, it gets overriden.
    """
    t = box_type(node.over)
    fields: FieldTypeMap = dict()
    for f, ft in t.row.fields.items():
        if f not in node.label_map:
            fields[f] = ft
    for f in node.label_map:
        fields[f] = UnitType.Scalar

    row = RowType(fields, group=t.row.group)
    return BoxType(t.name, row, t.handle_map)


@resolve.register
def _(node: FromNothing, ctx: AnnotateContext) -> BoxType:
    return EMPTY_BOX


@resolve.register
def _(node: FromReference, ctx: AnnotateContext) -> BoxType:
    """Check that the parent node had {node.name} as a valid reference. Raises
    an error if not, else copy over the set of references.
    """
    t = box_type(node.over)
    ft = t.row.fields.get(node.name, None)
    if not isinstance(ft, RowType):
        raise ErrRefInvalidTable(name=node.name, path=ctx.get_path(node.over))
    return BoxType(node.name, ft)


@resolve.register
def _(node: FromTable, ctx: AnnotateContext) -> BoxType:
    """Copy over all the column references from the source table."""
    fields: FieldTypeMap = dict()
    for f in node.table.columns:
        fields[f] = UnitType.Scalar
    row = RowType(fields)
    return BoxType(node.table.name, row)


@resolve.register
def _(node: FromValues, ctx: AnnotateContext) -> BoxType:
    """All columns provided by the table of values can be referenced."""
    fields: FieldTypeMap = dict()
    for f in node.source.columns:
        fields[f] = UnitType.Scalar
    row = RowType(fields)
    return BoxType(S("values"), row)


@resolve.register
def _(node: Group, ctx: AnnotateContext) -> BoxType:
    """The columns we are grouping by are available as references, while the ones which
    can be aggregated over are moved to the `group` attribute.
    """
    t = box_type(node.over)
    fields: FieldTypeMap = dict()
    for f in node.label_map:
        fields[f] = UnitType.Scalar
    row = RowType(fields, group=t.row)
    return BoxType(t.name, row)


@register_union_type(resolve)
def _(
    node: Union[IntBind, Limit, Order, Where, With, WithExternal], ctx: AnnotateContext
) -> BoxType:
    """The references available at the parent node can be used here too."""
    return box_type(node.over)


def resolve_knot(node: Box, ctx: AnnotateContext) -> None:
    assert isinstance(node.over, Knot), "expected a Knot node as parent"
    knot = node.over
    iterator_typ = box_type(knot.iterator)

    # TODO: why are we doing a fixed point convergence here?
    while not is_subset(node.typ.row, iterator_typ.row):  # type: ignore
        node.typ = intersect(node.typ, iterator_typ)  # type: ignore
        resolve_boxes(knot.iterator_boxes, ctx)
        iterator_typ = box_type(knot.iterator)
    assert isinstance(node.typ, BoxType)
    node.typ = node.typ.add_handle(node.handle)


@resolve.register
def _(node: IntIterate, ctx: AnnotateContext) -> BoxType:
    """Infer the same references as available at the Knot node"""
    assert isinstance(node.over, Box)
    resolve_knot(node.over, ctx)

    t = box_type(node.over)
    row = t.row.fields[node.iterator_name]
    assert isinstance(row, RowType), "expected a RowType"
    return BoxType(node.name, row)


@resolve.register
def _(node: IntJoin, ctx: AnnotateContext) -> BoxType:
    """All references available on either side of the Join nodes are available."""
    lt = box_type(node.over)
    rt = box_type(node.joinee)
    t: BoxType = union(lt, rt)  # type: ignore
    node.typ = t
    return t


@resolve.register
def _(node: Partition, ctx: AnnotateContext) -> BoxType:
    """All the references from the parent node are available both as regualar
    columns, and to aggregate over.

    NOTE: This is different from Group node, where you can't access columns not
    used to group, only aggregate over them.
    """
    t = box_type(node.over)
    row = RowType(t.row.fields, group=t.row)
    return BoxType(t.name, row, t.handle_map)


@resolve.register
def _(node: Select, ctx: AnnotateContext) -> BoxType:
    """All the references included in the select node are available downstream."""
    t = box_type(node.over)
    fields: FieldTypeMap = dict()
    for f in node.label_map:
        fields[f] = UnitType.Scalar
    row = RowType(fields)
    return BoxType(t.name, row)
