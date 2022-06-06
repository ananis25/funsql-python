"""
This module implements the compiler pass to translate the tree of SQLnode 
objects to a tree of SQLClause objects, which is almost like the actual 
lexical structure of the SQL query. The SQLClause expression can then be 
serialized into the SQL query string. 
"""

from contextlib import contextmanager
from functools import singledispatch
from typing import Optional, Union, overload

from ..clausedefs import *
from ..clauses import *
from ..nodedefs import *
from ..nodes import *
from ..sqlcontext import *
from .annotate import *
from .types import *

__all__ = ["translate_toplevel", "TranslateContext"]

NODE_TO_CLAUSE = dict[SQLNode, SQLClause]
NODE_TO_ALIAS = dict[SQLNode, Symbol]
ALIAS_TO_CLAUSE = dict[Symbol, SQLClause]


class Assemblage:
    """Represents a partially constructed SQL query"""

    clause: Optional[SQLClause]  # An SQL subquery, with(out) SELECT args
    cols: ALIAS_TO_CLAUSE  # TODO: SELECT args if required. Umm what?
    repl: NODE_TO_ALIAS  # mapping referenced nodes to column aliases

    def __init__(
        self,
        clause: Optional[SQLClause],
        cols: Optional[ALIAS_TO_CLAUSE] = None,
        repl: Optional[NODE_TO_ALIAS] = None,
    ) -> None:
        self.clause = clause
        self.cols = cols if cols is not None else dict()
        self.repl = repl if repl is not None else dict()

    def get_complete_select(self) -> SQLClause:
        """Construct the SELECT clause to a partially assembled query and return.
        NOTE: the clause attribute is not modified in place.
        """
        clause = self.clause
        if not isinstance(clause, (SELECT, UNION)):
            args = cols_to_select_args(self.cols)
            clause = SELECT(*args, over=clause)
        return clause

    def get_make_subs(self, alias: Optional[Symbol]) -> NODE_TO_CLAUSE:
        """Build and return a node-to-clause mapping assuming the assemblage will be
        extended/completed, if the input `alias` arg is None/not None.
        TODO: what?
        """
        subs: NODE_TO_CLAUSE = dict()
        if alias is None:
            for ref, name in self.repl.items():
                subs[ref] = self.cols[name]
        else:
            cache: ALIAS_TO_CLAUSE = dict()
            for ref, name in self.repl.items():
                if name not in cache:
                    cache[name] = ID(name=name, over=ID(name=alias))
                subs[ref] = cache[name]
        return subs

    def unwrap_repl(self) -> "Assemblage":
        repl_p: NODE_TO_ALIAS = dict()
        for ref, name in self.repl.items():
            assert isinstance(ref, NameBound)
            repl_p[ref.over] = name
        return Assemblage(self.clause, cols=self.cols, repl=repl_p)


def cols_to_select_args(cols: ALIAS_TO_CLAUSE) -> list[SQLClause]:
    """Pack the arguments to a SELECT clause; goes over all the columns
    and assigns them aliases.
    """
    args = []
    for name, clause in cols.items():
        # skip if the column is already a legit SELECT arg
        if not (isinstance(clause, ID) and clause.name == name):
            clause = AS(name=name, over=clause)
        args.append(clause)
    if len(args) == 0:
        args.append(LIT(None))
    return args


def translates_to_repl_n_cols(
    translates: list[tuple[SQLNode, SQLClause]]
) -> tuple[NODE_TO_ALIAS, ALIAS_TO_CLAUSE]:
    """Build a map of nodes to aliases and SELECT columns"""
    repl: NODE_TO_ALIAS = dict()
    cols: ALIAS_TO_CLAUSE = dict()

    renames: dict[tuple[Symbol, SQLClause], Symbol] = dict()
    counter: dict[Symbol, int] = dict()
    for ref, clause in translates:
        name = label(ref)
        counter[name] = counter.get(name, 0) + 1
        k = counter[name]

        # TODO: this is a trouble site, until we implement eq/hash for *all* SQLClause objects
        if k > 1 and (name, clause) in renames:
            # filters out duplicate columns
            name_p = renames[(name, clause)]
            repl[ref] = S(name_p)
        else:
            name_p = S(f"{name}_{k}") if k > 1 else name
            repl[ref] = name_p
            cols[name_p] = clause
            renames[(name, clause)] = name_p

    return repl, cols


def refs_to_repl_n_cols(refs: list[SQLNode]) -> tuple[NODE_TO_ALIAS, ALIAS_TO_CLAUSE]:
    """Build a map of nodes to aliases and implicit SELECT columns for a UNION query"""
    repl: NODE_TO_ALIAS = dict()
    cols: ALIAS_TO_CLAUSE = dict()

    # TODO: make sure if we really want a sequence like - q, q_2, q_3, ...? Or something else.
    counter: dict[Symbol, int] = dict()
    for ref in refs:
        name = label(ref)
        counter[name] = counter.get(name, 0) + 1
        k = counter[name]

        name_p = S(f"{name}_{k}") if k > 1 else name
        repl[ref] = name_p
        cols[name_p] = ID(name=name_p)

    return repl, cols


class CTEAssemblage:
    asmb: Assemblage
    name: Symbol
    schema: Optional[Symbol]
    materialized: Optional[bool]
    external: bool

    def __init__(
        self,
        asmb: Assemblage,
        name: Symbol,
        schema: Optional[Symbol] = None,
        materialized: Optional[bool] = None,
        external: bool = False,
    ) -> None:
        self.asmb = asmb
        self.name = name
        self.schema = schema
        self.materialized = materialized
        self.external = external


class TranslateContext:
    dialect: SQLDialect
    aliases: dict[Symbol, int]
    cte_map: dict[SQLNode, CTEAssemblage]
    recursive: bool
    vars_: ALIAS_TO_CLAUSE
    subs: NODE_TO_CLAUSE

    def __init__(self, ctx: AnnotateContext) -> None:
        self.dialect = ctx.catalog.dialect
        self.aliases = dict()
        self.cte_map = dict()
        self.recursive = False
        self.vars_ = dict()
        self.subs = dict()

    @contextmanager
    def substitute_vars_n_subs(
        self,
        *,
        vars_: Optional[ALIAS_TO_CLAUSE] = None,
        subs: Optional[NODE_TO_CLAUSE] = None,
    ):
        prev_vars_ = self.vars_
        prev_subs = self.subs

        try:
            if vars_ is not None:
                self.vars_ = vars_
            if subs is not None:
                self.subs = subs
            yield
        finally:
            self.vars_ = prev_vars_
            self.subs = prev_subs

    def allocate_alias(self, node_or_alias: Union[SQLNode, Symbol]) -> Symbol:
        """allocate a new alias"""
        if isinstance(node_or_alias, SQLNode):
            assert isinstance(node_or_alias, Box)
            alias = node_or_alias.typ.name
        else:
            alias = node_or_alias

        n = self.aliases.get(alias, 0) + 1
        self.aliases[alias] = n
        return S(f"{alias}_{n}")


def translate_toplevel(node: SQLNode, ctx: TranslateContext) -> SQLClause:
    """Translate the top-level SQLNode to produce a SQLClause representing the full query."""
    clause = translate(node, ctx)
    with_args: list[SQLClause] = []

    # The CTE nodes encountered are collected in the translation `context`
    for cte_asmb in ctx.cte_map.values():
        if cte_asmb.external:
            continue

        cols = [S(name) for name in cte_asmb.asmb.cols]
        if len(cols) == 0:
            cols.append(S("_"))

        over = cte_asmb.asmb.get_complete_select()
        if cte_asmb.materialized is not None:
            over = NOTE(
                "MATERIALIZED" if cte_asmb.materialized else "NOT MATERIALIZED",
                over=over,
            )
        arg = AS(name=cte_asmb.name, columns=cols, over=over)
        with_args.append(arg)

    if len(with_args) > 0:
        clause = WITH(*with_args, over=clause, recursive=ctx.recursive)
    return clause


# -----------------------------------------------------------
# translating scalar nodes
# -----------------------------------------------------------


@overload
def translate(node: SQLNode, ctx: TranslateContext) -> SQLClause:
    ...


@overload
def translate(node: None, ctx: TranslateContext) -> None:
    ...


@overload
def translate(node: list[SQLNode], ctx: TranslateContext) -> list[SQLClause]:
    ...


def translate(
    node: Union[None, SQLNode, list[SQLNode]], ctx: TranslateContext
) -> Union[None, SQLClause, list[SQLClause]]:
    """translate a SQLNode"""
    if node is None:
        return None
    elif isinstance(node, list):
        return [translate(n, ctx) for n in node]

    assert isinstance(node, SQLNode), f"expected SQLNode, got: {type(node)}"
    if node in ctx.subs:
        return ctx.subs[node]
    else:
        return translate_node(node, ctx)


@singledispatch
def translate_node(node: SQLNode, ctx: TranslateContext) -> Optional[SQLClause]:
    """specific translations for each subtype of SQLNode"""
    raise NotImplementedError(
        f"Translation isn't implemented for node of type: {type(node)}"
    )


@translate_node.register
def _(node: Agg, ctx: TranslateContext) -> SQLClause:
    if str(node.name).upper() == "COUNT":
        args = translate(node.args, ctx) if len(node.args) > 0 else [OP(S("*"))]
        filter_ = translate(node.filter_, ctx)
        return AGG(S("COUNT"), *args, distinct=node.distinct, filter_=filter_)
    else:
        args = translate(node.args, ctx)
        filter_ = translate(node.filter_, ctx)
        return AGG(
            S(str(node.name).upper()),
            *args,
            distinct=node.distinct,
            filter_=filter_,
        )


@translate_node.register
def _(node: As, ctx: TranslateContext) -> Optional[SQLClause]:
    return translate(node.over, ctx)


@translate_node.register
def _(node: IntBind, ctx: TranslateContext) -> Optional[SQLClause]:
    vars_p = ctx.vars_.copy()
    for name, i in node.label_map.items():
        vars_p[name] = translate(node.args[i], ctx)

    with ctx.substitute_vars_n_subs(vars_=vars_p):
        return translate(node.over, ctx)


@translate_node.register
def _(node: Box, ctx: TranslateContext) -> SQLClause:
    base: Assemblage = assemble(node, ctx)
    return base.get_complete_select()


_FUNC_REPLACE = {
    "==": "=",
    "!=": "<>",
}


@translate_node.register
def _(node: Fun, ctx: TranslateContext) -> SQLClause:
    fn = str(node.name).upper()

    if fn in ("NOT", "LIKE", "EXISTS", "=", "==", "!="):
        name = S(_FUNC_REPLACE.get(fn, fn))
        args = translate(node.args, ctx)
        return OP(name, *args)

    elif fn in ("AND", "OR"):
        default = True if fn == "AND" else False
        args = translate(node.args, ctx)
        if len(args) == 0:
            return LIT(default)
        elif len(args) == 1:
            return args[0]
        elif len(args) == 2 and isinstance(args[0], Lit) and args[0].val == default:
            return args[1]
        elif isinstance(args[0], OP) and str(args[0].name) == fn:
            # merge nested AND/OR
            return OP(args[0].name, *args[0].args, *args[1:])
        else:
            return OP(S(fn), *args)

    elif fn in ("IN", "NOT IN"):
        default = False if fn == "IN" else True
        if len(node.args) <= 1:
            return LIT(default)
        else:
            args = translate(node.args, ctx)
            if len(args) == 2 and isinstance(args[1], (SELECT, UNION)):
                return OP(S(fn), args[0], args[1])
            else:
                return OP(S(fn), args[0], FUN(S("_"), *args[1:]))

    elif fn in ("IS NULL", "IS NOT NULL"):
        name = S("IS" if fn == "IS NULL" else "IS NOT")
        return OP(name, *translate(node.args, ctx), None)

    elif fn == "CASE":
        return CASE(*translate(node.args, ctx))

    elif fn == "CAST":
        args = translate(node.args, ctx)
        if len(args) == 2 and isinstance(args[1], LIT) and isinstance(args[1].val, str):
            return FUN(S("CAST"), args[0], KW(S("AS"), OP(S(args[1].val))))
        else:
            return FUN(S("CAST"), *args)

    elif fn == "EXTRACT":
        args = translate(node.args, ctx)
        if len(args) == 2 and isinstance(args[0], LIT) and isinstance(args[0].val, str):
            return FUN(S("EXTRACT"), OP(S(args[0].val)), KW(S("FROM"), args[1]))
        else:
            return FUN(S("EXTRACT"), *args)

    elif fn in ("BETWEEN", "NOT BETWEEN"):
        if len(node.args) == 3:
            args = translate(node.args, ctx)
            return OP(S(fn), args[0], args[1], KW(S("AND"), over=args[2]))
        else:
            pass  # handled by the default logic

    elif fn in ("CURRENT_DATE", "CURRENT_TIMESTAMP"):
        if len(node.args) == 0:
            return OP(S(fn))
        else:
            pass  # handled by the default logic

    args = translate(node.args, ctx)
    if fn.isalpha():
        return FUN(S(fn), *args)
    else:
        return OP(S(fn), *args)


@translate_node.register
def _(node: Lit, ctx: TranslateContext) -> SQLClause:
    return LIT(node.val)


@translate_node.register
def _(node: Sort, ctx: TranslateContext) -> SQLClause:
    return SORT(value=node.value, nulls=node.nulls, over=translate(node.over, ctx))


@translate_node.register
def _(node: Var, ctx: TranslateContext) -> SQLClause:
    if node.name in ctx.vars_:
        return ctx.vars_[node.name]
    else:
        return VAR(node.name)


# -----------------------------------------------------------
# translating subquery nodes
# -----------------------------------------------------------


@singledispatch
def assemble(node: SQLNode, refs: list[SQLNode], ctx: TranslateContext) -> Assemblage:
    raise NotImplementedError(f"unhandled node of type: {type(node)}")


@assemble.register
def _(node: Box, ctx: TranslateContext) -> Assemblage:
    refs_p: list[SQLNode] = []
    for ref in node.refs:
        if isinstance(ref, HandleBound) and ref.handle == node.handle:
            refs_p.append(ref.over)
        else:
            refs_p.append(ref)
    base = assemble(node.over, refs_p, ctx)

    repl_p: NODE_TO_ALIAS = dict()
    for ref in node.refs:
        if isinstance(ref, HandleBound) and ref.handle == node.handle:
            repl_p[ref] = base.repl[ref.over]
        else:
            repl_p[ref] = base.repl[ref]
    return Assemblage(base.clause, base.cols, repl_p)


@assemble.register
def _(node: None, refs: list[SQLNode], ctx: TranslateContext) -> Assemblage:
    assert len(refs) == 0
    return Assemblage(None)


def aligned_columns(
    refs: list[SQLNode], repl: NODE_TO_ALIAS, args: list[SQLClause]
) -> bool:
    if len(refs) != len(args):
        return False
    for ref, arg in zip(refs, args):
        if not (isinstance(arg, (ID, AS)) and arg.name == repl[ref]):
            return False
    return True


@assemble.register
def _(node: Append, refs: list[SQLNode], ctx: TranslateContext) -> Assemblage:
    """no idea what is happening here"""
    assert node.over is not None
    base = assemble(node.over, ctx)
    branches = [(node.over, base)]
    for arg in node.args:
        branches.append((arg, assemble(arg, ctx)))

    dups: dict[SQLNode, SQLNode] = dict()
    seen: dict[Symbol, SQLNode] = dict()
    for ref in refs:
        name = base.repl[ref]
        if name in seen:
            other_ref = seen[name]
            if all(asmb.repl[ref] == asmb.repl[other_ref] for arg, asmb in branches):
                dups[ref] = seen[name]
        else:
            seen[name] = ref

    urefs = [ref for ref in refs if ref not in dups]
    repl, dummy_cols = refs_to_repl_n_cols(urefs)
    for ref, uref in dups.items():
        repl[ref] = repl[uref]

    cs: list[SQLClause] = []
    for arg, asmb in branches:
        if isinstance(asmb.clause, SELECT) and aligned_columns(
            urefs, repl, asmb.clause.args
        ):
            cs.append(asmb.clause)
            continue

        if not isinstance(asmb.clause, (SELECT, UNION)):
            alias = None
            tail = asmb.clause
        else:
            alias = ctx.allocate_alias(arg)
            tail = FROM(AS(over=asmb.get_complete_select(), name=alias))
        subs = asmb.get_make_subs(alias)
        cols: ALIAS_TO_CLAUSE = dict()
        for ref in urefs:
            name = repl[ref]
            cols[name] = subs[ref]
        clause = SELECT(*cols_to_select_args(cols), over=tail)
        cs.append(clause)

    clause = UNION(*cs[1:], all_=True, over=cs[0])
    return Assemblage(clause, cols=dummy_cols, repl=repl)


@assemble.register
def _(node: As, refs: list[SQLNode], ctx: TranslateContext) -> Assemblage:
    assert node.over is not None
    base = assemble(node.over, ctx)
    repl_p: NODE_TO_ALIAS = dict()
    for ref in refs:
        if isinstance(ref, NameBound):
            repl_p[ref] = base.repl[ref.over]
        else:
            repl_p[ref] = base.repl[ref]

    return Assemblage(base.clause, cols=base.cols, repl=repl_p)


@assemble.register
def _(node: Define, refs: list[SQLNode], ctx: TranslateContext) -> Assemblage:
    assert node.over is not None
    base = assemble(node.over, ctx)
    if not any(isinstance(ref, Get) and ref.name in node.label_map for ref in refs):
        return base

    if not isinstance(base.clause, (SELECT, UNION)):
        base_alias = None
        clause = base.clause
    else:
        base_alias = ctx.allocate_alias(node.over)
        clause = FROM(AS(name=base_alias, over=base.get_complete_select()))

    subs = base.get_make_subs(base_alias)
    translates: list[tuple[SQLNode, SQLClause]] = []
    tr_cache: ALIAS_TO_CLAUSE = dict()
    for ref in refs:
        if isinstance(ref, Get) and ref.over is None and ref.name in node.label_map:
            name = ref.name
            if name not in tr_cache:
                _define = node.args[node.label_map[name]]
                with ctx.substitute_vars_n_subs(subs=subs):
                    tr_cache[name] = translate(_define, ctx)
            col = tr_cache[name]
            translates.append((ref, col))
        else:
            translates.append((ref, subs[ref]))

    repl, cols = translates_to_repl_n_cols(translates)
    return Assemblage(clause, cols=cols, repl=repl)


@assemble.register
def _(node: FromNothing, refs: list[SQLNode], ctx: TranslateContext) -> Assemblage:
    return assemble(None, refs, ctx)


@assemble.register
def _(node: FromReference, refs: list[SQLNode], ctx: TranslateContext) -> Assemblage:
    cte_asmb = ctx.cte_map[node.over]
    asmb = cte_asmb.asmb.unwrap_repl()

    alias = ctx.allocate_alias(node.name)
    schema = None if cte_asmb.schema is None else ID(name=cte_asmb.schema)
    table = ID(name=cte_asmb.name, over=schema)
    clause = FROM(AS(name=alias, over=table))

    subs = asmb.get_make_subs(alias)
    translates = [(ref, subs[ref]) for ref in refs]
    repl, cols = translates_to_repl_n_cols(translates)
    return Assemblage(clause, cols=cols, repl=repl)


@assemble.register
def _(node: FromTable, refs: list[SQLNode], ctx: TranslateContext) -> Assemblage:
    seen: set[Symbol] = set()
    for ref in refs:
        assert (
            isinstance(ref, Get) and ref.over is None and ref.name in node.table.columns
        )
        seen.add(ref.name)
    alias = ctx.allocate_alias(node.table.name)

    schema = None if node.table.schema is None else ID(name=node.table.schema)
    table = ID(name=node.table.name, over=schema)
    clause = FROM(AS(name=alias, over=table))

    cols: ALIAS_TO_CLAUSE = dict()
    for col in node.table.columns:
        if col in seen:
            cols[col] = ID(name=col, over=ID(name=alias))

    repl: NODE_TO_ALIAS = dict()
    for ref in refs:
        if isinstance(ref, Get) and ref.over is None:
            repl[ref] = ref.name
    return Assemblage(clause, cols=cols, repl=repl)


@assemble.register
def _(node: FromValues, refs: list[SQLNode], ctx: TranslateContext) -> Assemblage:
    columns = node.source.columns
    column_set = set(columns)
    seen: set[Symbol] = set()
    for ref in refs:
        assert isinstance(ref, Get) and ref.over is None and ref.name in column_set
        seen.add(ref.name)

    rows: list[tuple]
    column_aliases: list[Symbol]
    # render the VALUES clause based on whether all/some/no columns are selected
    if len(seen) == len(columns):
        rows = node.source.data
        column_aliases = list(columns)
    elif len(seen) > 0:
        _indices = [i for i, c in enumerate(columns) if c in seen]
        rows = [tuple(row[i] for i in _indices) for row in node.source.data]
        column_aliases = [col for col in columns if col in seen]
    else:
        rows = [(None,) * len(node.source.data)]
        column_aliases = [S("_")]

    clause: SQLClause
    alias = ctx.allocate_alias(S("values"))
    cols: ALIAS_TO_CLAUSE = dict()
    if len(rows) == 0:
        clause = WHERE(LIT(False))
        for col in columns:
            if col in seen:
                cols[col] = LIT(None)
    elif not ctx.dialect.has_as_columns:
        # sqlite doesn't support column aliases for a VALUES clause
        col_prefix = ctx.dialect.values_column_prefix
        col_index = ctx.dialect.values_column_index
        assert col_prefix is not None
        clause = FROM(AS(alias, over=VALUES(rows)))
        parent = ID(name=alias)
        for col in columns:
            if col in seen:
                name = S(f"{col_prefix}{col_index}")
                cols[col] = ID(name=name, over=parent)
                col_index += 1
    else:
        clause = FROM(AS(alias, columns=column_aliases, over=VALUES(rows)))
        parent = ID(name=alias)
        for col in columns:
            if col in seen:
                cols[col] = ID(name=col, over=parent)

    repl: NODE_TO_ALIAS = dict()
    for ref in refs:
        if isinstance(ref, Get) and ref.over is None:
            repl[ref] = ref.name
    return Assemblage(clause, cols=cols, repl=repl)


@assemble.register
def _(node: Group, refs: list[SQLNode], ctx: TranslateContext) -> Assemblage:
    has_aggs = any(isinstance(ref, Agg) for ref in refs)
    if len(node.by) == 0 and not has_aggs:
        return assemble(None, refs, ctx)

    base = assemble(node.over, ctx)
    if base.clause is None or isinstance(base.clause, (FROM, JOIN, WHERE)):
        base_alias = None
        tail = base.clause
    else:
        assert node.over is not None
        base_alias = ctx.allocate_alias(node.over)
        tail = FROM(AS(name=base_alias, over=base.get_complete_select()))

    subs = base.get_make_subs(base_alias)
    with ctx.substitute_vars_n_subs(subs=subs):
        by = translate(node.by, ctx)
    translates: list[tuple[SQLNode, SQLClause]] = []
    for ref in refs:
        if isinstance(ref, Get) and ref.over is None:
            assert ref.name in node.label_map
            translates.append((ref, by[node.label_map[ref.name]]))
        elif isinstance(ref, Agg) and ref.over is None:
            with ctx.substitute_vars_n_subs(subs=subs):
                translates.append((ref, translate(ref, ctx)))

    repl, cols = translates_to_repl_n_cols(translates)
    assert len(cols) > 0
    if has_aggs:
        clause = GROUP(*by, over=tail)
    else:
        args = cols_to_select_args(cols)
        clause = SELECT(*args, distinct=True, over=tail)
        for name in cols:
            cols[name] = ID(name)
    return Assemblage(clause, cols=cols, repl=repl)


@assemble.register
def _(node: IntBind, refs: list[SQLNode], ctx: TranslateContext) -> Assemblage:
    vars_p = ctx.vars_.copy()
    for name, i in node.label_map.items():
        vars_p[name] = translate(node.args[i], ctx)
    with ctx.substitute_vars_n_subs(vars_=vars_p):
        return assemble(node.over, ctx)


@assemble.register
def _(node: IntIterate, refs: list[SQLNode], ctx: TranslateContext) -> Assemblage:
    with ctx.substitute_vars_n_subs(vars_=dict()):
        asmb_p = assemble(node.over, ctx)
    base = asmb_p.unwrap_repl()
    assert isinstance(base.clause, FROM)

    subs = base.get_make_subs(None)
    translates = []
    for ref in refs:
        translates.append((ref, subs[ref]))
    repl, cols = translates_to_repl_n_cols(translates)
    return Assemblage(base.clause, cols=cols, repl=repl)


@assemble.register
def _(node: IntJoin, refs: list[SQLNode], ctx: TranslateContext) -> Assemblage:
    assert node.over is not None
    left = assemble(node.over, ctx)
    if node.skip:
        return left
    if isinstance(left.clause, (FROM, JOIN)):
        left_alias = None
        tail = left.clause
    else:
        left_alias = ctx.allocate_alias(node.over)
        tail = FROM(AS(name=left_alias, over=left.get_complete_select()))

    subs = left.get_make_subs(left_alias)
    if len(node.lateral) > 0:
        with ctx.substitute_vars_n_subs(subs=subs):
            right = assemble(node.joinee, ctx)
    else:
        right = assemble(node.joinee, ctx)

    rclause = right.clause
    cond_1 = (
        isinstance(rclause, FROM)
        and isinstance(rclause.over, AS)
        and rclause.over.columns is None
        and isinstance(rclause.over.over, ID)
        and rclause.over.over.over is None
    )
    cond_2 = (
        isinstance(rclause, FROM)
        and isinstance(rclause.over, ID)
        and rclause.over.over is None
    )

    if cond_1 or cond_2:
        assert isinstance(rclause, FROM)  # make the typechecker happy
        joinee = rclause.over
        for ref, name in right.repl.items():
            subs[ref] = right.cols[name]
    else:
        right_alias = ctx.allocate_alias(node.joinee)
        joinee = AS(name=right_alias, over=right.get_complete_select())
        _cache: ALIAS_TO_CLAUSE = dict()
        for ref, name in right.repl.items():
            if name not in _cache:
                _cache[name] = ID(name=name, over=ID(right_alias))
            subs[ref] = _cache[name]

    with ctx.substitute_vars_n_subs(subs=subs):
        on = translate(node.on, ctx)
    assert joinee is not None
    clause = JOIN(
        joinee=joinee,
        on=on,
        left=node.left,
        right=node.right,
        lateral=len(node.lateral) > 0,
        over=tail,
    )
    translates = [(ref, subs[ref]) for ref in refs]
    repl, cols = translates_to_repl_n_cols(translates)

    return Assemblage(clause, cols=cols, repl=repl)


@assemble.register
def _(node: Knot, refs: list[SQLNode], ctx: TranslateContext) -> Assemblage:
    left = assemble(node.over, ctx)
    repl: NODE_TO_ALIAS = dict()
    seen: set[Symbol] = set()
    dups: set[SQLNode] = set()
    for ref in refs:
        assert isinstance(ref, NameBound)
        name = left.repl[ref.over]
        repl[ref] = name
        if name in seen:
            dups.add(ref.over)
        else:
            seen.add(name)

    temp_union = Assemblage(left.clause, cols=left.cols, repl=repl)
    union_alias = ctx.allocate_alias(node.name)
    ctx.cte_map[node.box] = CTEAssemblage(temp_union, name=union_alias)

    asmb = assemble(node.iterator, ctx)
    right = asmb.unwrap_repl()
    urefs = []
    for ref in refs:
        assert isinstance(ref, NameBound)
        if ref.over not in dups:
            urefs.append(ref.over)

    cs: list[SQLClause] = []
    assert node.over is not None
    _pairs: list[tuple[SQLNode, Assemblage]] = [
        (node.over, left),
        (node.iterator, right),
    ]
    for arg, asmb in _pairs:
        if isinstance(asmb.clause, SELECT) and aligned_columns(
            urefs, left.repl, asmb.clause.args
        ):
            cs.append(asmb.clause)
            continue
        elif not isinstance(asmb.clause, (SELECT, UNION)):
            alias = None
            tail = asmb.clause
        else:
            alias = ctx.allocate_alias(arg)
            tail = FROM(AS(name=alias, over=asmb.get_complete_select()))

        subs = asmb.get_make_subs(alias)
        cols: ALIAS_TO_CLAUSE = dict()
        for ref in urefs:
            name = left.repl[ref]
            cols[name] = subs[ref]
        cs.append(SELECT(*cols_to_select_args(cols), over=tail))

    union_clause = UNION(*cs[1:], all_=True, over=cs[0])
    cols: ALIAS_TO_CLAUSE = dict()
    for ref in urefs:
        name = left.repl[ref]
        cols[name] = ID(name)
    union = Assemblage(union_clause, cols=cols, repl=repl)
    ctx.cte_map[node.box] = CTEAssemblage(union, name=union_alias)
    ctx.recursive = True

    clause = FROM(over=ID(union_alias))
    subs = union.get_make_subs(union_alias)

    translates = [(ref, subs[ref]) for ref in refs]
    repl, cols = translates_to_repl_n_cols(translates)
    return Assemblage(clause, cols=cols, repl=repl)


@assemble.register
def _(node: Limit, refs: list[SQLNode], ctx: TranslateContext) -> Assemblage:
    assert node.over is not None
    base = assemble(node.over, ctx)
    if node.offset is None and node.limit is None:
        return base
    if base.clause is None or isinstance(
        base.clause, (FROM, JOIN, WHERE, GROUP, HAVING, ORDER)
    ):
        base_alias = None
        tail = base.clause
    else:
        base_alias = ctx.allocate_alias(node.over)
        tail = FROM(AS(name=base_alias, over=base.get_complete_select()))

    clause = LIMIT(node.limit, offset=node.offset, over=tail)
    subs = base.get_make_subs(base_alias)
    translates = [(ref, subs[ref]) for ref in refs]
    repl, cols = translates_to_repl_n_cols(translates)
    return Assemblage(clause, cols=cols, repl=repl)


@assemble.register
def _(node: Order, refs: list[SQLNode], ctx: TranslateContext) -> Assemblage:
    assert node.over is not None
    base = assemble(node.over, ctx)
    if len(node.by) == 0:
        return base

    if base.clause is None or isinstance(
        base.clause, (FROM, JOIN, WHERE, GROUP, HAVING)
    ):
        base_alias = None
        tail = base.clause
    else:
        base_alias = ctx.allocate_alias(node.over)
        tail = FROM(AS(name=base_alias, over=base.get_complete_select()))

    subs = base.get_make_subs(base_alias)
    with ctx.substitute_vars_n_subs(subs=subs):
        by = translate(node.by, ctx)
    clause = ORDER(*by, over=tail)
    translates = [(ref, subs[ref]) for ref in refs]
    repl, cols = translates_to_repl_n_cols(translates)
    return Assemblage(clause, cols=cols, repl=repl)


@assemble.register
def _(node: Partition, refs: list[SQLNode], ctx: TranslateContext) -> Assemblage:
    assert node.over is not None
    base = assemble(node.over, ctx)
    if not any(isinstance(ref, Agg) for ref in refs):
        return base

    if base.clause is None or isinstance(
        base.clause, (FROM, JOIN, WHERE, GROUP, HAVING)
    ):
        base_alias = None
        tail = base.clause
    else:
        base_alias = ctx.allocate_alias(node.over)
        tail = FROM(AS(name=base_alias, over=base.get_complete_select()))

    subs = base.get_make_subs(base_alias)
    clause = WINDOW(over=tail)
    with ctx.substitute_vars_n_subs(subs=subs):
        by = translate(node.by, ctx)
        order_by = translate(node.order_by, ctx)

        partition = PARTITION(*by, order_by=order_by, frame=node.frame)
        translates: list[tuple[SQLNode, SQLClause]] = []
        for ref in refs:
            if isinstance(ref, Agg) and ref.over is None:
                translates.append((ref, partition >> translate(ref, ctx)))
            else:
                translates.append((ref, subs[ref]))

    repl, cols = translates_to_repl_n_cols(translates)
    return Assemblage(clause, cols=cols, repl=repl)


@assemble.register
def _(node: Select, refs: list[SQLNode], ctx: TranslateContext) -> Assemblage:
    assert node.over is not None
    base = assemble(node.over, ctx)
    if not isinstance(base.clause, (SELECT, UNION)):
        base_alias = None
        tail = base.clause
    else:
        base_alias = ctx.allocate_alias(node.over)
        tail = FROM(AS(name=base_alias, over=base.get_complete_select()))

    subs = base.get_make_subs(base_alias)
    cols: ALIAS_TO_CLAUSE = dict()
    with ctx.substitute_vars_n_subs(subs=subs):
        for name, i in node.label_map.items():
            cols[name] = translate(node.args[i], ctx)
    clause = SELECT(*cols_to_select_args(cols), over=tail)

    cols = {name: ID(name) for name in cols}
    repl: NODE_TO_ALIAS = dict()
    for ref in refs:
        assert isinstance(ref, Get) and ref.over is None
        repl[ref] = ref.name
    return Assemblage(clause, cols=cols, repl=repl)


def merge_conditions(cond_1: SQLClause, cond_2: SQLClause) -> OP:
    is_and_cond_1 = isinstance(cond_1, OP) and str(cond_1.name) == "AND"
    is_and_cond_2 = isinstance(cond_2, OP) and str(cond_2.name) == "AND"

    # The typechecker can't tell cond_1 and cond_2 are actually `OP` objects, the
    # `isinstance` check doesn't carry through.
    if is_and_cond_1 and is_and_cond_2:
        return OP(S("AND"), *cond_1.args, *cond_2.args)  # type: ignore
    elif is_and_cond_1 and not is_and_cond_2:
        return OP(S("AND"), *cond_1.args, cond_2)  # type: ignore
    elif not is_and_cond_1 and is_and_cond_2:
        return OP(S("AND"), cond_1, *cond_2.args)  # type: ignore
    else:
        return OP(S("AND"), cond_1, cond_2)


@assemble.register
def _(node: Where, refs: list[SQLNode], ctx: TranslateContext) -> Assemblage:
    assert node.over is not None
    base = assemble(node.over, ctx)
    if (
        base.clause is None
        or isinstance(base.clause, (FROM, JOIN, WHERE, HAVING))
        or (isinstance(base.clause, GROUP) and len(base.clause.by) > 0)
    ):
        subs = base.get_make_subs(None)
        with ctx.substitute_vars_n_subs(subs=subs):
            cond = translate(node.condition, ctx)
        if isinstance(cond, LIT) and cond.val == True:
            return base
        if isinstance(base.clause, WHERE):
            cond = merge_conditions(base.clause.condition, cond)
            clause = WHERE(cond, over=base.clause.over)
        elif isinstance(base.clause, GROUP):
            clause = HAVING(cond, over=base.clause)
        elif isinstance(base.clause, HAVING):
            cond = merge_conditions(base.clause.condition, cond)
            clause = HAVING(cond, over=base.clause.over)
        else:
            clause = WHERE(cond, over=base.clause)
    else:
        base_alias = ctx.allocate_alias(node.over)
        tail = FROM(AS(name=base_alias, over=base.get_complete_select()))
        subs = base.get_make_subs(base_alias)
        with ctx.substitute_vars_n_subs(subs=subs):
            cond = translate(node.condition, ctx)
        if isinstance(cond, LIT) and cond.val == True:
            return base
        clause = WHERE(cond, over=tail)

    translates = [(ref, subs[ref]) for ref in refs]
    repl, cols = translates_to_repl_n_cols(translates)
    return Assemblage(clause, cols=cols, repl=repl)


@assemble.register
def _(node: With, refs: list[SQLNode], ctx: TranslateContext) -> Assemblage:
    for arg in node.args:
        asmb = assemble(arg, ctx)
        alias = ctx.allocate_alias(arg)
        ctx.cte_map[arg] = CTEAssemblage(
            asmb, name=alias, materialized=node.materialized
        )
    return assemble(node.over, ctx)


@assemble.register
def _(node: WithExternal, refs: list[SQLNode], ctx: TranslateContext) -> Assemblage:
    for arg in node.args:
        asmb = assemble(arg, ctx)
        assert isinstance(arg, Box)
        table_name = arg.typ.name
        table_cols = [c for c in asmb.cols]
        if len(table_cols) == 0:
            table_cols.append(S("_"))

        table = SQLTable(name=table_name, schema=node.schema, columns=table_cols)
        if node.handler is not None:
            node.handler(table, asmb.get_complete_select())
        ctx.cte_map[arg] = CTEAssemblage(asmb, name=table.name, external=True)
    return assemble(node.over, ctx)
