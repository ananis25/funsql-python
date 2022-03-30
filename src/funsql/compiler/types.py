"""
This module defines types for all the table/column references across 
the FunSQL node objects. Before rendering, the compiler resolves the 
names and types of all references _available_ at a node; which it can 
provide to the nodes downstream. 
"""

from typing import Optional, Union, ClassVar, overload

from ..common import S, Symbol
from ..prettier import Doc, QuoteContext, call_expr, assg_expr, annotate_expr, to_doc


class SQLType:
    pass


class EmptyType(SQLType):
    """Placeholder type assigned to a reference while we deduce it."""

    _instance: ClassVar[Optional["EmptyType"]] = None

    def __new__(cls):
        """This could be abstracted out, but I don't know how to get it past pylance."""
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def pretty_repr(self, ctx: QuoteContext) -> "Doc":
        return "EmptyType()"


class ScalarType(SQLType):
    """The regular type assigned to a column reference"""

    _instance: ClassVar[Optional["ScalarType"]] = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def pretty_repr(self, ctx: QuoteContext) -> "Doc":
        return "ScalarType()"


class AmbiguousType(SQLType):
    """Type assigned to references we are unsure about, like when the query
    is constructed incorrectly.
    """

    _instance: ClassVar[Optional["AmbiguousType"]] = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance

    def pretty_repr(self, ctx: QuoteContext) -> "Doc":
        return "AmbiguousType()"


# container mapping available references at a tabular node to their types
FieldTypeMap = dict["Symbol", Union["ScalarType", "RowType", "AmbiguousType"]]
# type assigned to a container for references you can aggregate over, at a Group node
GroupType = Union["EmptyType", "RowType", "AmbiguousType"]
HandleTypeMap = dict[int, Union["RowType", "AmbiguousType"]]


class RowType(SQLType):
    """Container for a set of fields mapped to their corresponding types, say
    the column references available at a tabular node.
    """

    fields: "FieldTypeMap"
    group: "GroupType"

    def __init__(
        self, fields: Optional[FieldTypeMap] = None, group: Optional[GroupType] = None
    ) -> None:
        self.fields = fields if fields is not None else dict()
        self.group = group if group is not None else EmptyType()

    def pretty_repr(self, ctx: QuoteContext) -> "Doc":
        name = "RowType"
        args = []

        for field, val in self.fields.items():
            args.append(annotate_expr(str(field), to_doc(val)))
        if not isinstance(self.group, EmptyType):
            args.append(assg_expr("group", to_doc(self.group)))
        return call_expr(name, args)


class BoxType(SQLType):
    """Type assigned to a Box node (wrapping a tabular node). It tracks the set of
    column references available at the tabular node, and handles.

    TODO: what are handles?
    """

    name: Symbol
    row: "RowType"
    handle_map: HandleTypeMap

    def __init__(
        self, name: Symbol, row: "RowType", handle_map: Optional["HandleTypeMap"] = None
    ):
        self.name = name
        self.row = row
        self.handle_map = handle_map if handle_map is not None else dict()

    def pretty_repr(self, ctx: QuoteContext) -> "Doc":
        name = "BoxType"
        args = []

        args.append(to_doc(self.name))
        for field, val in self.row.fields.items():
            args.append(annotate_expr(str(field), to_doc(val, ctx)))
        if not isinstance(self.row.group, EmptyType):
            args.append(assg_expr("group", to_doc(self.row.group, ctx)))
        for h in sorted(self.handle_map.keys()):
            args.append(annotate_expr(str(h), to_doc(self.handle_map[h], ctx)))

        return call_expr(name, args)

    @classmethod
    def from_fields(
        cls,
        name: Symbol,
        fields: Union["FieldTypeMap", "HandleTypeMap"],
        group: Optional["GroupType"] = None,
    ) -> "BoxType":
        if group is None:
            group = EmptyType()

        field_map = dict()
        handle_map = dict()
        for k, v in fields.items():
            if isinstance(k, Symbol):
                field_map[k] = v
            elif isinstance(k, int):
                handle_map[k] = v
        return BoxType(name, RowType(field_map, group), handle_map)

    def add_handle(self, handle: int) -> "BoxType":
        # missing handle means a negative int input
        if handle < 0:
            return self
        else:
            return self.__class__(
                self.name, self.row, {**self.handle_map, handle: self.row}
            )


EMPTY_BOX = BoxType(S("_"), RowType())


@overload
def intersect(t1: AmbiguousType, t2: AmbiguousType) -> AmbiguousType:
    ...


@overload
def intersect(t1: ScalarType, t2: ScalarType) -> ScalarType:
    ...


@overload
def intersect(t1: RowType, t2: RowType) -> RowType:
    ...


@overload
def intersect(t1: BoxType, t2: BoxType) -> BoxType:
    ...


@overload
def intersect(t1: SQLType, t2: SQLType) -> EmptyType:
    ...


def intersect(t1: SQLType, t2: SQLType) -> SQLType:
    """Used to deduce the references available at an Append node.

    The union of multiple tables results in a single table, so you'd expect
    only the references available in each of them; so an `intersect` operation.
    """
    if isinstance(t1, AmbiguousType) or isinstance(t2, AmbiguousType):
        return AmbiguousType()

    elif isinstance(t1, ScalarType) and isinstance(t2, ScalarType):
        return ScalarType()

    elif isinstance(t1, RowType) and isinstance(t2, RowType):
        if t1 == t2:
            return t1
        fields: FieldTypeMap = dict()
        for field in t1.fields.keys():
            if field in t2.fields:
                t = intersect(t1.fields[field], t2.fields[field])
                if not isinstance(t, EmptyType):
                    fields[field] = t
        group = intersect(t1.group, t2.group)
        return RowType(fields, group)

    elif isinstance(t1, BoxType) and isinstance(t2, BoxType):
        if t1 == t2:
            return t1
        new_handles: HandleTypeMap = dict()
        for key in t1.handle_map:
            if key in t2.handle_map:
                t = intersect(t1.handle_map[key], t2.handle_map[key])
                if not isinstance(t, EmptyType):
                    new_handles[key] = t
        name = t1.name if t1.name == t2.name else S("union")
        return BoxType(name, intersect(t1.row, t2.row), new_handles)

    else:
        return EmptyType()


@overload
def union(t1: RowType, t2: RowType) -> RowType:
    ...


@overload
def union(t1: BoxType, t2: BoxType) -> BoxType:
    ...


def union(t1: SQLType, t2: SQLType) -> SQLType:
    """Used to deduce the box type (read: references available) at a Join
    node, using the box types for the nodes being joined.

    You'd expect the nodes downstream of a Join to access references on either
    of the two sides of the Join, so a `union` makes sense.
    """
    if isinstance(t1, EmptyType) and isinstance(t2, EmptyType):
        return EmptyType()

    elif isinstance(t1, EmptyType) and not isinstance(t2, EmptyType):
        return t2
    elif not isinstance(t1, EmptyType) and isinstance(t2, EmptyType):
        return t1

    elif isinstance(t1, ScalarType) and isinstance(t2, ScalarType):
        return ScalarType()

    elif isinstance(t1, RowType) and isinstance(t2, RowType):
        fields = dict()
        for ref, ref_typ_t1 in t1.fields.items():
            if ref in t2.fields:
                ref_typ_t2 = t2.fields[ref]
                if isinstance(ref_typ_t1, RowType) and isinstance(ref_typ_t2, RowType):
                    fields[ref] = union(ref_typ_t1, ref_typ_t2)
                else:
                    fields[ref] = AmbiguousType()
            else:
                fields[ref] = ref_typ_t1
        for ref, ref_typ_t2 in t2.fields.items():
            if ref not in t1.fields:
                fields[ref] = ref_typ_t2

        group = (
            t1.group
            if isinstance(t2.group, EmptyType)
            else t2.group
            if isinstance(t1.group, EmptyType)
            else AmbiguousType()
        )
        return RowType(fields, group)

    elif isinstance(t1, BoxType) and isinstance(t2, BoxType):
        handle_map = dict()
        for field in t1.handle_map:
            if field not in t2.handle_map:
                handle_map[field] = t1.handle_map[field]
            else:
                handle_map[field] = AmbiguousType()
        for field in t2.handle_map:
            if field not in t1.handle_map:
                handle_map[field] = t2.handle_map[field]
        return BoxType(t1.name, union(t1.row, t2.row), handle_map)

    else:
        return AmbiguousType()


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
