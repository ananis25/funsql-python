"""
The `link` step resolves the columns provided by each tabular node. 

* We start with the leaf `Box` node, which typically wraps a Select node, and add 
the columns used in the output to its `refs` list. 
* Then, we propagate upwards. 
  * Each tabular node wrapped in a `Box` node reads the `refs` from its box, and 
  assigns them to one of its parent `Box` nodes. For nodes like `Fun` or `Where` 
  which might depend on other columns besides the ones selected, the other columns 
  are similarly resolved. 
  * These box nodes further link references through their parent tabular nodes. 
"""

from typing import Any, Optional, Union, TypeVar, Literal
from functools import singledispatch

from .types import *
from .annotate import *
from ..common import register_union_type
from ..nodes import *
from ..nodedefs import *

__all__ = ["link_toplevel"]


# -----------------------------------------------------------
# utilities to help link references
# -----------------------------------------------------------


def gather(node: Union[None, SQLNode, list[SQLNode]], refs: list[SQLNode]) -> None:
    """gather node references in the `refs` container; to pass to the children nodes"""
    if isinstance(node, list):
        for n in node:
            assert isinstance(n, SQLNode), f"expected a SQLNode, got {type(n)}"
            gather(n, refs)
    elif isinstance(node, (As, Box, Sort)):
        gather(node.over, refs)
    elif isinstance(node, IntBind):
        gather(node.over, refs)
        gather(node.args, refs)
        node.owned = True
    elif isinstance(node, Fun):
        gather(node.args, refs)
    elif isinstance(node, (Agg, Get, HandleBound, NameBound)):
        refs.append(node)
    else:
        pass


def validate(t: SQLType, ref: SQLNode, ctx: AnnotateContext) -> None:
    """validate references"""
    if isinstance(t, RowType):
        validate_rowtype(t, ref, ctx)
    elif isinstance(t, BoxType):
        validate_boxtype(t, ref, ctx)
    else:
        raise NotImplementedError(f"validate method not implemented for: {type(t)}")


def validate_boxtype(t: BoxType, ref: SQLNode, ctx: AnnotateContext) -> None:
    if isinstance(ref, HandleBound):
        handle = ref.handle  # index to the tabular node used as handle
        over = ref.over  # TODO: must this be a Get node?
        if handle in t.handle_map:
            ht = t.handle_map[handle]
            if ht == UnitType.Ambiguous:
                raise ErrRefAmbiguousHandle(path=ctx.get_path(ref))
            else:
                validate(ht, over, ctx)
        else:
            raise ErrRefUndefinedHandle(path=ctx.get_path(ref))

    else:
        validate(t.row, ref, ctx)


def validate_rowtype(t: RowType, ref: SQLNode, ctx: AnnotateContext) -> None:
    """verify that the type container has an entry for the node referenced"""
    while isinstance(ref, NameBound):
        name = ref.name
        ft = t.fields.get(name, UnitType.Empty)
        if not isinstance(ft, RowType):
            if ft == UnitType.Empty:
                raise ErrRefUndefinedName(name=name, path=ctx.get_path(ref))
            elif ft == UnitType.Scalar:
                raise ErrRefUnexpectedScalarType(name=name, path=ctx.get_path(ref))
            elif ft == UnitType.Ambiguous:
                raise ErrRefAmbiguousName(name=name, path=ctx.get_path(ref))
            else:
                raise Exception(f"unexpected field type: {type(ft)}")
        t = ft
        ref = ref.over

    if isinstance(ref, Get) and ref.over is None:
        name = ref.name
        ft = t.fields.get(name, UnitType.Empty)
        if not ft == UnitType.Scalar:
            if ft == UnitType.Empty:
                raise ErrRefUndefinedName(name=name, path=ctx.get_path(ref))
            elif ft == UnitType.Ambiguous:
                raise ErrRefAmbiguousName(name=name, path=ctx.get_path(ref))
            elif isinstance(ft, RowType):
                raise ErrRefUnexpectedRowType(name=name, path=ctx.get_path(ref))
            else:
                raise Exception(f"unexpected field type: {type(ft)}")

    elif isinstance(ref, Agg) and ref.over is None:
        name = ref.name
        if not isinstance(t.group, RowType):
            if t.group == UnitType.Empty:
                raise ErrRefUnexpectedAgg(name=name, path=ctx.get_path(ref))
            elif t.group == UnitType.Ambiguous:
                raise ErrRefAmbiguousAgg(name=name, path=ctx.get_path(ref))
            else:
                raise Exception(f"unexpected group type: {type(t.group)}")

    else:
        raise Exception(f"unexpected reference: {type(ref)}")


def gather_n_validate(
    node: Union[SQLNode, list[SQLNode]],
    refs: list[SQLNode],
    t: BoxType,
    ctx: AnnotateContext,
) -> None:
    """gather node references and validate them"""
    start_at = len(refs)
    gather(node, refs)
    for ref in refs[start_at:]:
        validate(t, ref, ctx)


T = TypeVar("T", BoxType, RowType)


def route(lt: T, rt: T, ref: SQLNode) -> Literal[-1, 1]:
    """For all the references at a Join node, determine if a reference was sourced
    from the left side node of the join or the right.

    Returns:
      * -1 if the reference was sourced from the left side node
      * 1 if the reference was sourced from the right side node
    """

    if isinstance(lt, BoxType) and isinstance(rt, BoxType):
        if isinstance(ref, HandleBound):
            typ = lt.handle_map.get(ref.handle, UnitType.Empty)
            return 1 if typ == UnitType.Empty else -1
        else:
            return route(lt.row, rt.row, ref)

    elif isinstance(lt, RowType) and isinstance(rt, RowType):
        while isinstance(ref, NameBound):
            lt_p = lt.fields.get(ref.name, UnitType.Empty)
            if lt_p == UnitType.Empty:
                return 1
            rt_p = rt.fields.get(ref.name, UnitType.Empty)
            if rt_p == UnitType.Empty:
                return -1
            assert isinstance(lt_p, RowType) and isinstance(rt_p, RowType)
            lt = lt_p
            rt = rt_p
            ref = ref.over

        if isinstance(ref, Get):
            return -1 if ref.name in lt.fields else 1
        elif isinstance(ref, Agg):
            return -1 if isinstance(lt.group, RowType) else 1
        else:
            raise Exception(f"unexpected ref node of type: {type(ref)}")

    else:
        raise Exception(f"unexpected nodes of type: {type(lt)}")


# -----------------------------------------------------------
# linking implemented
# -----------------------------------------------------------


def link_toplevel(ctx: AnnotateContext) -> None:
    root_box = ctx.boxes[-1]
    for f, ft in root_box.typ.row.fields.items():
        if ft == UnitType.Scalar:
            root_box.refs.append(Get(f))
    link_boxes(list(reversed(ctx.boxes)), ctx)


def link_boxes(boxes: list[Box], ctx: AnnotateContext) -> None:
    for box in boxes:
        if box.over is not None:
            refs_p: list[SQLNode] = []
            for ref in box.refs:
                refs_p.append(
                    ref.over
                    if isinstance(ref, HandleBound) and ref.handle == box.handle
                    else ref
                )
            link(box.over, refs_p, ctx)


def check_box(node: Optional[SQLNode]) -> Box:
    assert isinstance(node, Box), f"expected node of type: Box, got: {type(node)}"
    return node


@singledispatch
def link(node: SQLNode, refs: list[SQLNode], ctx: AnnotateContext) -> None:
    raise NotImplementedError(f"link method not implemented for: {type(node)}")


@link.register
def _(node: Append, refs: list[SQLNode], ctx: AnnotateContext) -> None:
    box = check_box(node.over)
    box.refs.extend(refs)
    for arg in node.args:
        assert isinstance(arg, Box), "expected `args` of type: Row"
        box = arg
        box.refs.extend(refs)


@link.register
def _(node: As, refs: list[SQLNode], ctx: AnnotateContext) -> None:
    box = check_box(node.over)
    for ref in refs:
        if isinstance(ref, NameBound):
            assert ref.name == node.name
            box.refs.append(ref.over)
        elif isinstance(ref, HandleBound):
            box.refs.append(ref)
        else:
            raise Exception(f"unexpected ref node of type: {type(ref)}")


@link.register
def _(node: Define, refs: list[SQLNode], ctx: AnnotateContext) -> None:
    box = check_box(node.over)
    seen: set[Symbol] = set()
    for ref in refs:
        if isinstance(ref, Get) and ref.over is None and ref.name in node.label_map:
            if ref.name not in seen:
                seen.add(ref.name)
                col = node.args[node.label_map[ref.name]]
                gather_n_validate(col, box.refs, box.typ, ctx)
        else:
            box.refs.append(ref)


@register_union_type(link)
def _(
    node: Union[FromNothing, FromTable, FromValues],
    refs: list[SQLNode],
    ctx: AnnotateContext,
) -> None:
    pass


@link.register
def _(node: FromReference, refs: list[SQLNode], ctx: AnnotateContext) -> None:
    box = check_box(node.over)
    for ref in refs:
        box.refs.append(NameBound(over=ref, name=node.name))


@link.register
def _(node: Group, refs: list[SQLNode], ctx: AnnotateContext) -> None:
    box = check_box(node.over)
    gather_n_validate(node.by, box.refs, box.typ, ctx)
    for ref in refs:
        if isinstance(ref, Agg) and ref.over is None:
            gather_n_validate(ref.args, box.refs, box.typ, ctx)
            if ref.filter_ is not None:
                gather_n_validate(ref.filter_, box.refs, box.typ, ctx)


@register_union_type(link)
def _(node: Union[Limit, With], refs: list[SQLNode], ctx: AnnotateContext) -> None:
    box = check_box(node.over)
    box.refs.extend(refs)


@link.register
def _(node: IntBind, refs: list[SQLNode], ctx: AnnotateContext) -> None:
    if not node.owned:
        gather_n_validate(node.args, [], EMPTY_BOX, ctx)
    box = check_box(node.over)
    box.refs.extend(refs)


@link.register
def _(node: IntIterate, refs: list[SQLNode], ctx: AnnotateContext) -> None:
    box = check_box(node.over)
    for ref in refs:
        box.refs.append(NameBound(over=ref, name=node.iterator_name))


@link.register
def _(node: IntJoin, refs: list[SQLNode], ctx: AnnotateContext) -> None:
    lbox = check_box(node.over)
    rbox = check_box(node.joinee)
    lrefs, rrefs = [], []

    for ref in refs:
        turn = route(lbox.typ, rbox.typ, ref)
        if turn < 0:
            lrefs.append(ref)
        else:
            rrefs.append(ref)
    if len(rrefs) != 0:
        node.skip = False
    if node.skip:
        lbox.refs.extend(lrefs)
        return

    gather_n_validate(node.joinee, node.lateral, lbox.typ, ctx)
    lbox.refs.extend(node.lateral)
    refs_p = []
    gather_n_validate(node.on, refs_p, node.typ, ctx)
    for ref in refs_p:
        turn = route(lbox.typ, rbox.typ, ref)
        if turn < 0:
            lbox.refs.append(ref)
        else:
            rbox.refs.append(ref)
    lbox.refs.extend(lrefs)
    rbox.refs.extend(rrefs)


@link.register
def _(node: Knot, refs: list[SQLNode], ctx: AnnotateContext) -> None:
    box = check_box(node.over)
    iterator_box = check_box(node.iterator)

    refs = []
    seen = set()
    while True:
        repeat = False
        for ref in node.box.refs:
            assert isinstance(ref, NameBound), "expected `ref` of type: NameBound"
            assert ref.name == node.name
            if ref.over not in seen:
                refs.append(ref)
                seen.add(ref.over)
                repeat = True

        node.box.refs.clear()
        node.box.refs.extend(refs)
        if not repeat:
            break
        for ibox in node.iterator_boxes:
            ibox.refs.clear()
        iterator_box.refs.extend(refs)
        link_boxes(list(reversed(node.iterator_boxes)), ctx)

    for ref in refs:
        assert isinstance(ref, NameBound), "expected `ref` of type: NameBound"
        box.refs.append(ref.over)


@link.register
def _(node: Order, refs: list[SQLNode], ctx: AnnotateContext) -> None:
    box = check_box(node.over)
    box.refs.extend(refs)
    gather_n_validate(node.by, box.refs, box.typ, ctx)


@link.register
def _(node: Partition, refs: list[SQLNode], ctx: AnnotateContext) -> None:
    box = check_box(node.over)
    for ref in refs:
        if isinstance(ref, Agg) and ref.over is None:
            gather_n_validate(ref.args, box.refs, box.typ, ctx)
            if ref.filter_ is not None:
                gather_n_validate(ref.filter_, box.refs, box.typ, ctx)
        else:
            box.refs.append(ref)
    gather_n_validate(node.by, box.refs, box.typ, ctx)
    gather_n_validate(node.order_by, box.refs, box.typ, ctx)


@link.register
def _(node: Select, refs: list[SQLNode], ctx: AnnotateContext) -> None:
    """"""
    box = check_box(node.over)
    gather_n_validate(node.args, box.refs, box.typ, ctx)


@link.register
def _(node: Where, refs: list[SQLNode], ctx: AnnotateContext) -> None:
    box = check_box(node.over)
    box.refs.extend(refs)
    gather_n_validate(node.condition, box.refs, box.typ, ctx)
