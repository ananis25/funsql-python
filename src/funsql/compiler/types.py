"""
This module defines types for all the table/column references across 
the FunSQL node objects. Before rendering, the compiler resolves the 
names and types of all references _available_ at a node; which it can 
provide to the nodes downstream. 
"""

from enum import Enum
from typing import Optional, Union, ClassVar, overload

from ..common import S, Symbol
from ..prettier import (
    Doc,
    QuoteContext,
    call_expr,
    assg_expr,
    list_expr,
    annotate_expr,
    to_doc,
)


NEGATIVE_INT = -1000  # placeholder negative value as stand-in for missing index

SQLType = Union["UnitType", "RowType", "BoxType"]


class UnitType(Enum):
    Empty = 1  # placeholder type assigned to a reference while we deduce it
    Scalar = 2  # regular type assigned to a column reference
    Ambiguous = 3  # type assigned to references we are unsure about

    def pretty_repr(self, ctx: QuoteContext) -> "Doc":
        return f"{self.name}Type()"


FieldTypeMap = dict["Symbol", SQLType]
HandleTypeMap = dict[int, SQLType]


class RowType:
    """Container for a set of fields mapped to their corresponding types, say
    the column references available at a tabular node.
    """

    fields: "FieldTypeMap"
    group: SQLType

    def __init__(
        self, fields: Optional[FieldTypeMap] = None, group: Optional[SQLType] = None
    ) -> None:
        self.fields = fields if fields is not None else dict()
        self.group = group if group is not None else UnitType.Empty

    def pretty_repr(self, ctx: QuoteContext) -> "Doc":
        name = "RowType"
        args = []

        for field, val in self.fields.items():
            args.append(annotate_expr(str(field), to_doc(val)))
        if not self.group == UnitType.Empty:
            args.append(assg_expr("group", to_doc(self.group)))
        return call_expr(name, args)


class BoxType:
    """Type assigned to a Box node (wrapping a tabular node). It tracks the set of
    column references available at the tabular node, and handles.
    """

    name: Symbol
    row: RowType
    handle_map: HandleTypeMap

    def __init__(
        self, name: Symbol, row: "RowType", handle_map: Optional["HandleTypeMap"] = None
    ):
        self.name = name
        self.row = row
        self.handle_map = handle_map if handle_map is not None else dict()

    def is_empty(self) -> bool:
        return self.name.data == "_" and len(self.row.fields) == 0

    def pretty_repr(self, ctx: QuoteContext) -> "Doc":
        name = "BoxType"
        args = []

        args.append(to_doc(self.name))
        for field, val in self.row.fields.items():
            args.append(annotate_expr(str(field), to_doc(val, ctx)))
        if not self.row.group == UnitType.Empty:
            args.append(assg_expr("group", to_doc(self.row.group, ctx)))
        if len(self.handle_map) > 0:
            embedded_handles = list_expr([str(k) for k in self.handle_map])
            args.append(assg_expr("handles_incl", embedded_handles))
            # NOTE: printing the namespace of the handle nodes too makes output string too large
            # for h in sorted(self.handle_map.keys()):
            #     args.append(annotate_expr(str(h), to_doc(self.handle_map[h], ctx)))

        return call_expr(name, args)

    def add_handle(self, handle: int) -> "BoxType":
        # missing handle means a negative int input
        if handle < 0:
            return self
        else:
            return self.__class__(
                self.name, self.row, {**self.handle_map, handle: self.row}
            )


EMPTY_BOX_TYPE = lambda: BoxType(S("_"), RowType())


def intersect(t1: SQLType, t2: SQLType) -> SQLType:
    """Used to deduce the references available at an Append node.

    The union of multiple tables results in a single table, so you'd expect
    only the references available in each of them; so an `intersect` operation.
    """
    if t1 == UnitType.Ambiguous and t2 == UnitType.Ambiguous:
        return UnitType.Ambiguous
    elif t1 == UnitType.Scalar and t2 == UnitType.Scalar:
        return UnitType.Scalar

    elif isinstance(t1, RowType) and isinstance(t2, RowType):
        if t1 == t2:
            return t1
        common_fields: FieldTypeMap = dict()
        for field in set(t1.fields).intersection(set(t2.fields)):
            t = intersect(t1.fields[field], t2.fields[field])
            if not t == UnitType.Empty:
                common_fields[field] = t
        group = intersect(t1.group, t2.group)
        return RowType(common_fields, group)

    elif isinstance(t1, BoxType) and isinstance(t2, BoxType):
        if t1 == t2:
            return t1
        common_handles: HandleTypeMap = dict()
        for key in set(t1.handle_map).intersection(set(t2.handle_map)):
            t = intersect(t1.handle_map[key], t2.handle_map[key])
            if not t == UnitType.Empty:
                common_handles[key] = t
        name = t1.name if t1.name == t2.name else S("union")
        return BoxType(name, intersect(t1.row, t2.row), common_handles)  # type: ignore

    else:
        return UnitType.Empty


def union(t1: SQLType, t2: SQLType) -> SQLType:
    """Used to deduce the box type (read: references available) at a Join
    node, using the box types for the nodes being joined.

    You'd expect the nodes downstream of a Join to access references on either
    of the two sides of the Join, so a `union` makes sense.
    """
    if t1 == UnitType.Empty and t2 == UnitType.Empty:
        return UnitType.Empty
    elif t1 == UnitType.Empty and not t2 == UnitType.Empty:
        return t2
    elif not t1 == UnitType.Empty and t2 == UnitType.Empty:
        return t1
    elif t1 == UnitType.Scalar and t2 == UnitType.Scalar:
        return UnitType.Scalar

    elif isinstance(t1, RowType) and isinstance(t2, RowType):
        combined_fields: FieldTypeMap = dict()
        t1_fields = set(t1.fields)
        t2_fields = set(t2.fields)

        for ref in t1_fields.intersection(t2_fields):
            ref_typ_t1 = t1.fields[ref]
            ref_typ_t2 = t2.fields[ref]
            if isinstance(ref_typ_t1, RowType) and isinstance(ref_typ_t2, RowType):
                combined_fields[ref] = union(ref_typ_t1, ref_typ_t2)
            else:
                combined_fields[ref] = UnitType.Ambiguous
        for ref in t1_fields.difference(t2_fields):
            combined_fields[ref] = t1.fields[ref]
        for ref in t2_fields.difference(t1_fields):
            combined_fields[ref] = t2.fields[ref]

        group = (
            t1.group
            if t2.group == UnitType.Empty
            else t2.group
            if t1.group == UnitType.Empty
            else UnitType.Ambiguous
        )
        return RowType(combined_fields, group)

    elif isinstance(t1, BoxType) and isinstance(t2, BoxType):
        combined_handles: HandleTypeMap = dict()
        t1_handles = set(t1.handle_map)
        t2_handles = set(t2.handle_map)

        for field in t1_handles.intersection(t2_handles):
            combined_handles[field] = UnitType.Ambiguous
        for field in t1_handles.difference(t2_handles):
            combined_handles[field] = t1.handle_map[field]
        for field in t2_handles.difference(t1_handles):
            combined_handles[field] = t2.handle_map[field]
        return BoxType(t1.name, union(t1.row, t2.row), combined_handles)  # type: ignore

    else:
        return UnitType.Ambiguous


def is_subset(t1: SQLType, t2: SQLType) -> bool:
    """Check whether t1 is a subset of t2"""
    if isinstance(t1, RowType) and isinstance(t2, RowType):
        if t1 == t2:
            return True
        for field in t1.fields.keys():
            if field not in t2.fields:
                return False
            if not is_subset(t1.fields[field], t2.fields[field]):
                return False
        return True

    elif isinstance(t1, BoxType) and isinstance(t2, BoxType):
        if t1 == t2:
            return True
        if t1.name != t2.name:
            return False
        if not is_subset(t1.row, t2.row):
            return False
        for field in t1.handle_map.keys():
            if field not in t2.handle_map:
                return False
            if not is_subset(t1.handle_map[field], t2.handle_map[field]):
                return False
        return True

    else:
        # for the non-container SQLType instances, just compare the data types
        return type(t1) == type(t2)
