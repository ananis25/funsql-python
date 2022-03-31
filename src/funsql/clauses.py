from functools import lru_cache
from typing import Any, Generic, Union, Optional, Type, TypeVar, get_type_hints

from .common import Symbol
from .prettier import Doc, QuoteContext, resolve, to_doc

__all__ = ["SQLClause"]


def _get_origin(typ: Any) -> Any:
    # HACK: looks very fragile, but `get_origin`, `get_args` are not present in python <3.8
    return typ.__origin__ if hasattr(typ, "__origin__") else None


def _get_args(typ: Any) -> tuple:
    return typ.__args__ if hasattr(typ, "__args__") else tuple()


@lru_cache
def _get_refs_for_clause(
    object_typ: Type["SQLClause"],
) -> tuple[list[str], list[str]]:
    """Compute list of attributes on a SQLClause class that are scalars vs sequences."""

    assert issubclass(object_typ, SQLClause)

    scalar_attrs, sequence_attrs = [], []
    for attr, typ in get_type_hints(object_typ).items():
        container = _get_origin(typ)
        if container == list:
            sequence_attrs.append(attr)
        elif container == Union:
            args = _get_args(typ)
            assert (
                len(args) == 2 and type(None) in args
            ), "TODO: accomodate union types other than optional"
            arg = [x for x in args if x != type(None)][0]
            if _get_origin(arg) == list:
                sequence_attrs.append(attr)
            else:
                scalar_attrs.append(attr)
        else:
            scalar_attrs.append(attr)
    return scalar_attrs, sequence_attrs


T = TypeVar("T", bound="SQLClause")


class SQLClause(Generic[T]):
    """Base class for a SQL clause, that translates directly to an SQL expression"""

    def __repr__(self) -> str:
        return resolve(to_doc(self), 80)

    def pretty_repr(self, ctx: Optional[QuoteContext]) -> Doc:
        raise NotImplementedError(
            f"pretty_repr method isn't implemented on the SQLClause class - {type(self)}"
        )

    def rebase(self: T, *args, **kwargs) -> T:
        raise NotImplementedError(
            f"rebase method isn't implemented on the SQLClause class - {type(self)}"
        )

    def __rshift__(self, other):
        """
        We should implement __rrshift__ instead but reflected operators in python
        don't work for objects of the same type, so this indirection.
        [link](https://docs.python.org/3/reference/datamodel.html?highlight=radd#id11)
        """
        if isinstance(other, SQLClause):
            return other.rebase(self)
        else:
            raise NotImplementedError(
                f">> isn't a valid operation with a SQLClause and {type(other)}"
            )

    def __rrshift__(self, other: Symbol) -> "SQLClause":
        if isinstance(other, Symbol):
            return self.rebase(other)
        else:
            raise NotImplementedError(
                f">> isn't a valid operation with a SQLClause and {type(other)}"
            )

    def _key(self):
        """We want to implement hashing for SQLClause objects, since they are compared
        to deduplicate clauses in the rendered SQL.

        TODO: This is fairly expensive, and increases render time by like 50pc. Should we
        instead call a custom function when clause equality is really evaluated, instead
        of implementing `__eq__`?

        Credits: The pattern is lifted off a stackoverflow answer I can't find the link to.
        """
        scalar_attrs, sequence_attrs = _get_refs_for_clause(self.__class__)
        vals = []
        for attr in scalar_attrs:
            vals.append(getattr(self, attr))
        for attr in sequence_attrs:
            val = getattr(self, attr)
            vals.append(tuple(val) if val is not None else None)
        return tuple(vals)

    def __hash__(self) -> int:
        return hash(self._key())

    def __eq__(self, other: Any) -> bool:
        if type(self) is not type(other):
            return NotImplemented
        return self._key() == other._key()
