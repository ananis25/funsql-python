"""
This module implements primitives to render FunSQL objects into a stream 
of tokens for pretty printing. The pretty printing algorithm used is the 
one first implemented by Derek C. Oppen. 
"""


import datetime
import os
from functools import singledispatch
from contextlib import contextmanager
from typing import Union, Optional, Any, Callable, Generator

from .common import Symbol, register_union_type
from .prettyprint import *


Doc = Union[Token, list["Doc"]]

# -----------------------------------------------------------
# utility routines to create/merge `Doc` objects
# -----------------------------------------------------------


def join_docs(sep: Callable[[], Doc], docs: list[Doc]) -> list[Doc]:
    """Join a sequence of tokens with a separator"""
    list_vals = []
    for i, doc in enumerate(docs):
        if i != 0:
            list_vals.append(sep())
        list_vals.append(doc)
    return list_vals


def assg_expr(key: str, val: "Doc") -> Doc:
    return [key, " = ", val]


def annotate_expr(key: str, val: "Doc") -> Doc:
    return [key, ": ", val]


def pipe_expr(left: "Doc", right: "Doc") -> Doc:
    return [Begin(ns_indent=True), left, " >>", Break(1), right, End()]


def highlight_expr(doc: "Doc") -> "Doc":
    highlight = ["^^", Break(blanks=2), doc, Break(blanks=2), "^^"]
    return [Begin(ns_indent=True), *highlight, End()]


def block_expr(args: list[Doc]) -> list[Doc]:
    assert isinstance(args, list), f"Expected list, got {type(args)}"
    sep = lambda: [",", Break(blanks=1)]
    return [Begin(ns_indent=True), join_docs(sep, args), End()]


def call_expr(name: str, args: list[Doc]) -> Doc:
    return [name, "(", block_expr(args), ")"]


def list_expr(args: list["Doc"]) -> Doc:
    return ["[", block_expr(args), "]"]


def full_query_expr(args: list["Doc"]) -> Doc:
    assert isinstance(args, list), f"Expected list, got {type(args)}"
    sep = lambda: [",", Break(blanks=10000)]
    return [
        Begin(),
        "let ",
        Begin(ns_indent=True),
        join_docs(sep, args),
        End(),
        Break(blanks=10000),
        "end",
        End(),
    ]


# -----------------------------------------------------------
# convert standard python datatypes to `Doc` objects
# -----------------------------------------------------------


@singledispatch
def to_doc(val: Any, ctx: Optional["QuoteContext"] = None) -> Doc:
    if not hasattr(val, "pretty_repr"):
        raise TypeError(f"Unsupported literal type for SQL expression: {val}")
    return val.pretty_repr(ctx)


@register_union_type(to_doc)
def _(val: Union[int, float, bool]) -> str:
    return str(val)


@to_doc.register
def _(val: str) -> str:
    return f'"{val}"'


@to_doc.register
def _(val: Symbol) -> str:
    return str(val)


@to_doc.register
def _(val: datetime.date) -> str:
    return f"DATE '{val.strftime('%Y-%m-%d')}'"


@to_doc.register
def _(val: datetime.time) -> str:
    return f"TIME '{val.strftime('%H:%M:%S')}'"


@to_doc.register
def _(val: datetime.datetime) -> str:
    return f"TIMESTAMP '{val.isoformat()}'"


@to_doc.register
def _(val: datetime.timedelta) -> str:
    return f"INTERVAL '{val.total_seconds()} SECONDS'"


@to_doc.register
def _(val: None) -> str:
    return "NULL"


# -----------------------------------------------------------
# print out a `Doc` object
# -----------------------------------------------------------


def flatten(doc: list["Doc"]) -> Generator[Token, None, None]:
    """Flatten a Doc object into a sequence of tokens"""
    for item in doc:
        if isinstance(item, list):
            yield from flatten(item)
        else:
            yield item


def get_screen_width() -> int:
    """Get the current screen width"""
    try:
        sz = os.get_terminal_size().columns
        return int(sz * 5 / 6)
    except:
        return 80


def resolve(doc: "Doc", width: int) -> str:
    """Resolve a Doc object into a string"""
    printer = Printer(width)
    if not isinstance(doc, list):
        doc = [doc]
    for tok in flatten(doc):
        printer.scan(tok)
    printer.eof()
    return printer.getvalue()


class QuoteContext:
    """Auxiliary information for pretty printing"""

    limit: bool  # indicate if the child objects don't need to be expanded
    vars_: dict[Any, Symbol]  # substitute node/clause objects with equivalent symbols

    def __init__(self, limit: bool = False, vars_: Optional[dict[Any, Symbol]] = None):
        self.limit = limit
        self.vars_ = vars_ if vars_ is not None else {}

    @contextmanager
    def set(self, **kwargs):
        """Temporarily set the QuoteContext attributes"""
        prev = {k: getattr(self, k) for k in kwargs}
        for k, v in kwargs.items():
            setattr(self, k, v)

        try:
            yield
        finally:
            for k, v in prev.items():
                setattr(self, k, v)
