from enum import Enum
from typing import Optional, Union

from .nodes import SQLNode
from .clauses import SQLClause
from .sqlcontext import *
from .compiler.annotate import annotate, AnnotateContext
from .compiler.resolve import *
from .compiler.link import *
from .compiler.translate import *
from .compiler.serialize import serialize, SerializationContext, SQLString

# -----------------------------------------------------------
# utility functions to generate SQL context for specific dialects
# -----------------------------------------------------------


def dialect_default() -> SQLDialect:
    return dialect_sqlite()


def dialect_sqlite() -> SQLDialect:
    return SQLDialect(
        name="sqlite",
        var_style=VarStyle.NUMBERED,
        var_prefix="?",
        limit_style=LimitStyle.SQLITE,
        has_as_columns=False,
    )


# -----------------------------------------------------------
# render routines
# -----------------------------------------------------------


class RenderDepth(Enum):
    """Ordered enumeration to control the compiler output.

    Reference: https://docs.python.org/3/library/enum.html#orderedenum
    """

    ANNOTATE = 1
    RESOLVE = 2
    LINK = 3
    TRANSLATE = 4
    SERIALIZE = 5

    def __lt__(self, other):
        if isinstance(other, RenderDepth):
            return self.value < other.value
        return NotImplemented

    def __le__(self, other):
        if isinstance(other, RenderDepth):
            return self.value <= other.value
        return NotImplemented

    def __gt__(self, other):
        if isinstance(other, RenderDepth):
            return self.value > other.value
        return NotImplemented

    def __ge__(self, other):
        if isinstance(other, RenderDepth):
            return self.value >= other.value
        return NotImplemented


def render(
    node: SQLNode,
    depth: RenderDepth = RenderDepth.SERIALIZE,
    catalog: Optional[SQLCatalog] = None,
) -> Union[SQLNode, SQLClause, SQLString]:
    """Render the SQL node expression to a SQLString object.

    NOTE: calling the `render` method might mutate the SQLNode object. Work with
    a fresh object on each call.
    """
    assert isinstance(node, SQLNode)
    if catalog is None:
        catalog = SQLCatalog(dialect=dialect_default())

    ann_ctx = AnnotateContext(catalog=catalog)
    node_annotated = annotate(node, ann_ctx)
    if not depth > RenderDepth.ANNOTATE:
        return node_annotated

    resolve_toplevel(ann_ctx)
    if not depth > RenderDepth.RESOLVE:
        return node_annotated

    link_toplevel(ann_ctx)
    if not depth > RenderDepth.LINK:
        return node_annotated

    translate_ctx = TranslateContext(ann_ctx)
    output_clause = translate_toplevel(node_annotated, translate_ctx)
    if not depth > RenderDepth.TRANSLATE:
        return output_clause

    serialize_ctx = SerializationContext(dialect=catalog.dialect)
    serialize(output_clause, serialize_ctx)
    return serialize_ctx.render()


def render_clause(clause: SQLClause, dialect: SQLDialect) -> SQLString:
    """Render the SQL clause to a SQLString object."""
    assert isinstance(clause, SQLClause)
    assert isinstance(dialect, SQLDialect)
    serialize_ctx = SerializationContext(dialect=dialect)
    serialize(clause, serialize_ctx)
    return serialize_ctx.render()
