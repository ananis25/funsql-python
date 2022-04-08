"""
This module hosts utilities imported across modules. 
"""

from typing import Callable, ClassVar, Union, TypeVar, Generic, get_type_hints
import datetime

__all__ = ["register_union_type", "Symbol", "S", "OrderedSet", "LITERAL_TYPES", "LITERAL_TYPE_SIG"]


# -----------------------------------------------------------
# data structures used across the package
# -----------------------------------------------------------


class SymbolHelper(type):
    """
    Customize attribute access to double up as a constructor.
    So, `Symbol.foo` is the same as `Symbol("foo")`.
    """

    def __getattr__(cls, key: str) -> "Symbol":
        if key.startswith("_"):
            raise AttributeError(
                "fluent syntax isn't supported for strings starting with an underscore"
            )
        return cls(key)


class Symbol(metaclass=SymbolHelper):
    """
    Use Symbol objects to refer to identifiers, function names, etc.
    Returns the same copy for each type of symbol.
    """

    _instances: ClassVar[dict[str, "Symbol"]] = {}
    data: str

    def __new__(cls, data: Union[str, "Symbol"]):
        if isinstance(data, Symbol):
            return data

        assert isinstance(data, str)
        if data not in cls._instances:
            cls._instances[data] = super().__new__(cls)
        return cls._instances[data]

    def __init__(self, data: Union[str, "Symbol"]):
        if isinstance(data, str):  # else the `data` attribute is already set
            self.data = data

    def __str__(self) -> str:
        return self.data

    def __repr__(self) -> str:
        return self.data

    def __hash__(self) -> int:
        return hash(self.data)


# Shorthand for the Symbol class
S = Symbol

_T = TypeVar("_T")


class OrderedSet(Generic[_T]):
    """We need an ordered set at a bunch of places"""

    _map: dict[_T, bool]

    def __init__(self):
        self._map = {}

    def add(self, item: _T):
        self._map[item] = True

    def pop(self):
        return self._map.popitem()[0]

    def __contains__(self, item: _T):
        return item in self._map

    def __iter__(self):
        return iter(self._map)

    def __len__(self):
        return len(self._map)

    def __repr__(self):
        return repr(self._map)


LITERAL_TYPES = (
    int,
    float,
    str,
    bool,
    type(None),
    datetime.date,
    datetime.time,
    datetime.datetime,
    datetime.timedelta,
)
# unpacking list in a type object subscript only possible since python 3.11
LITERAL_TYPE_SIG = Union[
    int,
    float,
    str,
    bool,
    None,
    datetime.date,
    datetime.time,
    datetime.datetime,
    datetime.timedelta,
]

# -----------------------------------------------------------
# utility routines
# -----------------------------------------------------------


def register_union_type(target_fn: Callable) -> Callable:
    """The singledispatch.register decorator doesn't work for functions that have a
    union type as the first argument. This decorator is a workaround for that.

    credits: Python bug tracker [link](https://bugs.python.org/issue46014)
    """

    def decorator(func):
        arg_type = list(get_type_hints(func).values())[0]
        assert arg_type.__origin__ is Union
        for typ in arg_type.__args__:
            func = target_fn.register(typ)(func)
        return func

    return decorator
