"""
This module implements node objects which represent operations on tabular 
datasets and can be combined together to assemble SQL queries. In the `render` 
step, FunSQL compiles the assembled node tree into a SQL query. 

NOTE [DEV]:
1. Writing the __init__ method gets repetitive, but dataclasses don't help with star-arg inputs. 
Also, we can't hint at what input types we allow, annotations only show what it gets coerced to. 
2. Can abstract out the `rebase` and `pretty print` method but there are a lot of edge cases, so 
lets just keep it simple. The compiler is the beast part of the code. 
"""

from functools import partial
from typing import Any, Optional, Union, Callable

from .common import Symbol, S, LITERAL_TYPES, register_union_type
from .sqlcontext import SQLTable, ValuesTable
from .nodes import *
from .clauses import SQLClause
from .clausedefs import (
    Frame,
    FrameMode,
    FrameEdgeSide,
    FrameEdge,
    FrameExclude,
    ValueOrder,
    NullsOrder,
)
from .prettier import (
    Doc,
    call_expr,
    assg_expr,
    list_expr,
    pipe_expr,
    to_doc,
    QuoteContext,
)

__all__ = [
    "Agg",
    "Append",
    "As",
    "Bind",
    "Define",
    "From",
    "Fun",
    "Get",
    "Group",
    "Iterate",
    "Join",
    "Limit",
    "Lit",
    "Order",
    "Partition",
    "Select",
    "Sort",
    "Var",
    "Where",
    "With",
    "Asc",
    "Desc",
    "ValuesTable",
    "aka",
    "F",
]


# -----------------------------------------------------------
# utility functions local to this module
# -----------------------------------------------------------

# These are internal
RESERVED_WORDS = {
    "name",
    "over",
    "args",
    "filter_",
    "distinct",
}


class ExtendAttrs(type):
    """
    A SQLNode using this as a metaclass can support fluent syntax like
    `Fun.count(*args)` instead of `Fun("count", *args)`. Used by the `Fun`
    and `Agg` classes.

    NOTE: This is a frequence source of bugs, since the user provided key value
    might clash with an attribute of the class. Look out for it probably?
    """

    def __getattr__(cls, key: str) -> Callable:
        if key in RESERVED_WORDS:
            raise AttributeError(
                "fluent syntax isn't supported for reserved words, use the call expression syntax instead"
            )
        return partial(cls, key)


class ExtendAttrsFull(type):
    """
    Same as ExtendAttrs but actually initializes the class. Used by the
    `Get` and `Var` classes.
    """

    def __getattr__(cls, key: str) -> Any:
        if key in RESERVED_WORDS:
            raise AttributeError(
                "fluent syntax isn't supported for reserved words, use the call expression syntax instead"
            )
        return cls(key)


def _rebase_node(curr: Optional[SQLNode], pre: SQLNode) -> SQLNode:
    """shorthand for a common pattern"""
    if curr is None:
        return pre
    else:
        return curr.rebase(pre)


def _cast_to_node(node: Union[SQLNode, Any]) -> SQLNode:
    """Convert a value to a SQLNode"""
    if isinstance(node, SQLNode):
        return node
    else:
        return Lit(node)


def _cast_to_node_skip_none(node: Union[SQLNode, Any]) -> Optional[SQLNode]:
    """Convert a value to a SQLNode, skipping None"""
    if node is None:
        return None
    else:
        return _cast_to_node(node)


# -----------------------------------------------------------
# Node definitions to export
# -----------------------------------------------------------


class Agg(SQLNode, metaclass=ExtendAttrs):
    """
    `Agg` is used to create an aggregate expression over a column. It should be applied
    only within the output of nodes of type:
    * `Group`: gets translated to a column aggregate for the `GROUP BY` operation
    * `Partition`: gets translated to a `WINDOW` function.

    Specific database engines may support additional aggregate functions besides the ones
    that are common - count, sum, max, min, etc. FunSQL is unaware of what is supported, and
    renders all of them as regular function expressions.

    Args:
        name: name of the aggregate function
        *args: arguments to the aggregate function
        distinct: whether to use only distinct values from the column for aggregation
        filter_: rows to filter out from the dataset before aggregation

    Examples:
    ```python
    # To fetch the number of students in each class
    >>> q = From(Students) >> Group(Get.class_id) >> Select(Get.class_id, Agg.count())

    # Compute average playing time of players in the Arsenal first team
    >>> q = (
            From(Players)
            >> Where(eq(Get.team_id, "Arsenal"))
            >> Group(Get.player_id)
            >> Select(Get.player_id, Agg.avg(Get.playing_time)
        )

    # Add a column indicating the rank of each building by height in its city
    >>> q = (
            From(Buildings)
            >> Partition(Get.city_id, order_by=[Get.height])
            >> Select(Get.city_id, Get.building_id, Get.height, Agg.row_number()
        )
    """

    name: Symbol
    args: list[SQLNode]
    distinct: bool
    filter_: Optional[SQLNode]
    over: Optional[SQLNode]

    def __init__(
        self,
        name: Union[str, Symbol],
        *args: NODE_MATERIAL,
        distinct: bool = False,
        filter_: Optional[NODE_MATERIAL] = None,
        over: Optional[SQLNode] = None,
    ) -> None:
        super().__init__()
        self.name = S(name)
        self.args = [_cast_to_node(arg) for arg in args]
        self.distinct = distinct
        self.filter_ = _cast_to_node_skip_none(filter_)
        self.over = _cast_to_node_skip_none(over)

    def rebase(self, pre: SQLNode) -> "SQLNode":
        return self.__class__(
            self.name,
            *self.args,
            distinct=self.distinct,
            filter_=self.filter_,
            over=_rebase_node(self.over, pre),
        )

    @check_repr_context
    def pretty_repr(self, ctx: QuoteContext) -> "Doc":
        name = "Agg"
        args = []

        _name_str = str(self.name)
        if _name_str.isalpha():
            name = f"Agg.{_name_str}"  # Agg.Count
        else:
            name = f'Agg."{_name_str}"'  # edge cases

        if self.distinct:
            args.append(assg_expr("distinct", to_doc(self.distinct)))
        for arg in self.args:
            args.append(to_doc(arg, ctx))
        if self.filter_ is not None:
            args.append(assg_expr("filter", to_doc(self.filter_, ctx)))
        if self.over is not None:
            args.append(assg_expr("over", to_doc(self.over, ctx)))

        return call_expr(name, args)


class Append(TabularNode):
    """
    `Append` concatenates input datasets, analogous to a `UNION ALL` operation in SQL.
    Only the columns present in all the input datasets are kept.

    Args:
        *args: input datasets to take a union over

    Examples:
    ```python

    # Collect the birthdays of everyone in the School
    >>> q = (
        From(Students)
        >> Select(Get.date_of_birth)
        >> Append(
            From(Teachers) >> Select(Get.date_of_birth),
            From(Staff) >> Select(Get.date_of_birth)
        )
    )
    ```
    """

    args: list[SQLNode]
    over: Optional[SQLNode]

    def __init__(self, *args: NODE_MATERIAL, over: Optional[SQLNode] = None) -> None:
        super().__init__()
        self.args = [_cast_to_node(arg) for arg in args]
        self.over = _cast_to_node_skip_none(over)

    def rebase(self, pre: SQLNode) -> "SQLNode":
        return self.__class__(*self.args, over=_rebase_node(self.over, pre))

    @check_repr_context
    def pretty_repr(self, ctx: QuoteContext) -> "Doc":
        name = "Append"
        args = []

        if len(self.args) == 0:
            args.append(assg_expr("args", to_doc("[]")))
        else:
            for arg in self.args:
                args.append(to_doc(arg, ctx))

        ex = call_expr(name, args)
        if self.over is not None:
            ex = pipe_expr(to_doc(self.over, ctx), ex)
        return ex


class As(SQLNode):
    """
    `As` node is similar but not quite like the `AS` clause in SQL. When preceded by a:
        * scalar expression - it specifies the name of the column to refer to it
        * tabular node - it nests the namespace for the subquery behind the assigned name

    When used in a tabular context, the alias provided by `As` is used by FunSQL to identify
    column references correctly. It might not end up in the rendered SQL query if not necessary.

    Args:
        name: name assigned to the preceding node
        over: scalar/subquery being assigned the name

    Examples:
    ```python
    # rename the id column when reading from a table
    >>> q = From(Students) >> Select(Get.id >> As("student_id"))

    # nest the location table behind the alias "location" to disambiguate between the `id` column
    # of the `Students` table and the `id` column of the `Location` table.
    >>> q = (
            From(Students)
            >> Join(Location >> As("location"), eq(Get.location.id, Get.city_id))
            >> Select(Get.id, Get.name, Get.location.name)
        )
    ```
    """

    name: Symbol
    over: Optional[SQLNode]

    def __init__(
        self, name: Union[str, Symbol], over: Optional[NODE_MATERIAL] = None
    ) -> None:
        super().__init__()
        self.name = S(name)
        self.over = _cast_to_node_skip_none(over)

    def rebase(self, pre: SQLNode) -> "SQLNode":
        return self.__class__(name=self.name, over=_rebase_node(self.over, pre))

    @check_repr_context
    def pretty_repr(self, ctx: QuoteContext) -> "Doc":
        name = "As"
        args: list["Doc"] = [str(self.name)]

        ex = call_expr(name, args)
        if self.over is not None:
            ex = pipe_expr(to_doc(self.over, ctx), ex)
        return ex


class Bind(SQLNode):
    """
    `Bind` evaulates the parent subquery with parameters; that is maps `Var` variables in
    the parent node to the supplied values.

    Bind is especially useful for correlated query cases. When a subquery with `Bind` applied is
    used as a scalar expression, it gets rendered as a correlated subquery. While when `Bind` is
    applied to the `joinee` branch of a Join node, it gets rendered as a lateral join.

    Args:
        *args: nodes in the form, `value >> As(variable_name)`

    Examples:
    ```python
    # fetch the number of times a person visited their healthcare provider
    >>> q1 = From(Visits) >> Where(eq(Get.patient_id, Var.PERSON_ID)) >> Group() >> Select(Agg.count())
    # This gives us a `prepared statement` in SQL, which can be executed with different values for the `PERSON_ID` parameter.

    # Alternatively, `Bind` lets us supply the parameter to create the full query directly.
    >>> q = q1 >> Bind(Var.PERSON_ID, 1000)

    # By binding column references instead of explicity values, we can create correlated queries.
    # For example, to fetch patients with at least one visit,
    >>> visited = lambda x: From(Visits) >> Where(eq(Get.patient_id, Var.PERSON_ID)) >> Bind(x >> As("PERSON_ID"))
    >>> q = From(Person) >> Where(Fun.exists(visits(Get.person_id))

    # To fetch patients along with their last visit date, we use `Bind` in a tabular context.
    >>> q = (
            From(person)
            >> Join(From(Visits)
                    >> Where(eq(Get.patient_id, Var.PERSON_ID))
                    >> Order(Get.visit_date >> Desc())
                    >> Limit(1)
                    >> Bind(Get.person_id >> As("PERSON_ID"))
                    >> As("last_visit"),
                    on=True,
                    left=True
                )
            >> Select(Get.person_id, Get.name, Get.last_visit.visit_date)
        )
    ```
    """

    args: list[SQLNode]
    over: Optional[SQLNode]
    label_map: dict[Symbol, int]

    def __init__(self, *args: NODE_MATERIAL, over: Optional[SQLNode] = None) -> None:
        super().__init__()
        self.args = [_cast_to_node(arg) for arg in args]
        self.over = _cast_to_node_skip_none(over)
        self.label_map = populate_label_map(self, self.args)

    def rebase(self, pre: SQLNode) -> "SQLNode":
        return self.__class__(*self.args, over=_rebase_node(self.over, pre))

    @check_repr_context
    def pretty_repr(self, ctx: QuoteContext) -> "Doc":
        name = "Bind"
        args = []
        if len(self.args) == 0:
            args.append(assg_expr("args", to_doc("[]")))
        else:
            for arg in self.args:
                args.append(to_doc(arg, ctx))

        ex = call_expr(name, args)
        if self.over is not None:
            ex = pipe_expr(to_doc(self.over, ctx), ex)
        return ex


class Define(TabularNode):
    """
    Define node creates or replaces columns in the parent subquery.

    Args:
        *args: nodes in the form, `column reference >> As(column_name)`

    Examples:
    ```python
    # Define a new column listing the age of each person
    >>> q = From(Person) >> Define(Fun("-", Fun.now(), Get.date_of_birth) >> As("age")) >> Select(Get.name, Get.age)

    # Compute the discounted price, and after adding taxes. Define nodes make query writing modular.
    >>> q = (
            From(Cart)
            >> Join(From(Product) >> As("product"), on=eq(Get.product_id, Get.product.id), left=True)
            >> Define(Fun("-", 1, Get.discount) >> As("discount_factor"))
            >> Define(Fun("*", Get.product.price, Get.discount_factor) >> As("discounted_price"))
            >> Define(Fun("*", Get.discounted_price, Get.product.tax_rate) >> As("tax_amount"))
            >> Define(Fun("+", Get.discounted_price, Get.tax_amount) >> As("total_price"))
        )
    ```
    """

    args: list[SQLNode]
    over: Optional[SQLNode]
    label_map: dict[Symbol, int]

    def __init__(self, *args: NODE_MATERIAL, over: Optional[SQLNode] = None) -> None:
        super().__init__()
        self.args = [_cast_to_node(arg) for arg in args]
        self.over = _cast_to_node_skip_none(over)
        self.label_map = populate_label_map(self, self.args)

    def rebase(self, pre: SQLNode) -> "SQLNode":
        return self.__class__(*self.args, over=_rebase_node(self.over, pre))

    @check_repr_context
    def pretty_repr(self, ctx: QuoteContext) -> "Doc":
        name = "Define"
        args = [to_doc(arg, ctx) for arg in self.args]

        ex = call_expr(name, args)
        if self.over is not None:
            ex = pipe_expr(to_doc(self.over, ctx), ex)
        return ex


class From(TabularNode):
    """
    `From` refers to the contents of a database table; present at the root of query objects.
    The source of a `From` node can be of the type:
    * `SQLTable`: corresponds to a table in the database. Downstream nodes can refer to its columns, and build expressions from them.
    * `Symbol`: refers to a table provided at render time, or a CTE subquery constructed using a `With` node.
    * `ValuesTable`: refers to a list of tuples of values, gets rendered as a VALUES clause in the final SQL.
    * None: represents a subquery with one row and no columns.

    Args:
        source: table, symbol or a Values table.

    Examples:

    # To select from the table with user address details
    >>> Addresses = SQLTable(S.addresses, columns=["id", "user_id", "address", "city"])
    >>> q = From(Addresses) >> Select(Get.user_id, Get.city)

    # To filter out the discounted products in the catalog
    >>> q = (
            From(Products)
            >> Where(Fun.IN(Get.id, From(S.discounted_items) >> Select(Get.product_id)))
            >> With(From(Discounts) >> Where(Fun(">", Get.discount, 0)) >> As("discounted_items"))
        )
    """

    source: Union[Symbol, SQLTable, ValuesTable, None]

    def __init__(
        self, source: Union[Symbol, SQLTable, ValuesTable, None] = None
    ) -> None:
        super().__init__()
        if isinstance(source, (SQLTable, ValuesTable)) or source is None:
            self.source = source
        else:
            self.source = S(source)

    @check_repr_context
    def pretty_repr(self, ctx: QuoteContext) -> "Doc":
        name = "From"
        args = []

        if isinstance(self.source, SQLTable):
            alias = ctx.vars_.get(self.source, None)
            if alias is None:
                args.append(to_doc(self.source, QuoteContext(limit=True)))
            else:
                args.append(str(alias))
        elif isinstance(self.source, Symbol):
            args.append(str(self.source))
        elif isinstance(self.source, ValuesTable):
            if ctx.limit:
                args.append("...")
            else:
                for col in self.source.columns:
                    args.append(assg_expr(str(col), "[...]"))

        return call_expr(name, args)


class Fun(SQLNode, metaclass=ExtendAttrs):
    """
    `Fun` is used to apply a SQL function or operator.

    FunSQL is unaware of which functions/operators are actually supported by each
    database engine. By default, `Fun` node is translated to the function call syntax
    by default, if the `name` argument is a valid python identifier, else it is rendered
    as an operator. For the commonly used function names below, FunSQL specifies the
    rendering explicitly.

    |---------------------------------- |---------------------------------------|
    | `Fun` node                        | SQL syntax                            |
    |---------------------------------- |---------------------------------------|
    | `Fun("=", x, y)`                  | `x = y`                               |
    | `Fun."!="(x, y)`                  | `x <> y`                              |
    | `Fun.and(p₁, p₂, …)`              | `p₁ AND p₂ AND …`                     |
    | `Fun.between(x, y, z)`            | `x BETWEEN y AND z`                   |
    | `Fun.case(p, x, …)`               | `CASE WHEN p THEN x … END`            |
    | `Fun.cast(x, {TYPE})`             | `CAST(x AS {TYPE})`                   |
    | `Fun.current_date()`              | `CURRENT_DATE`                        |
    | `Fun.current_timestamp()`         | `CURRENT_TIMESTAMP`                   |
    | `Fun.exists(q)`                   | `EXISTS q`                            |
    | `Fun.extract("FIELD", x)`         | `EXTRACT(FIELD FROM x)`               |
    | `Fun.in(x, q)`                    | `x IN q`                              |
    | `Fun.in(x, y₁, y₂, …)`            | `x IN (y₁, y₂, …)`                    |
    | `Fun("is not null", x)`           | `x IS NOT NULL`                       |
    | `Fun("is null", x)`               | `x IS NULL`                           |
    | `Fun.like(x, y)`                  | `x LIKE y`                            |
    | `Fun.not(p)`                      | `NOT p`                               |
    | `Fun("not between", x, y, z)`     | `x NOT BETWEEN y AND z`               |
    | `Fun("not in", x, q)`             | `x NOT IN q`                          |
    | `Fun("not in", x, y₁, y₂, …)`     | `x NOT IN (y₁, y₂, …)`                |
    | `Fun.or(p₁, p₂, …)`               | `p₁ OR p₂ OR …`                       |
    |---------------------------------- |---------------------------------------|


    Args:
        name: name of the function or operator.
        *args: arguments to the function or operator.

    Examples:
    ```python
    # filter out users named "John"
    >>> q = From(Users) >> Where(Fun.like(Get.name, "%John%"))

    # specify the Join condition for two tables
    >>> q = From(Users) >> Join(Orders >> As(S.orders), Fun("=", Get.id, Get.orders.user_id))
    ```
    """

    name: Symbol
    args: list[SQLNode]

    def __init__(self, name: Union[str, Symbol], *args: NODE_MATERIAL) -> None:
        super().__init__()
        self.name = S(name)
        self.args = [_cast_to_node(arg) for arg in args]

    @check_repr_context
    def pretty_repr(self, ctx: QuoteContext) -> "Doc":
        name = "Fun"
        args = []

        _name_str = str(self.name)
        if _name_str.isalpha():
            name = f"Fun.{_name_str}"  # Fun.exists
        else:
            name = f'Fun."{_name_str}"'  # Fun.">"

        for arg in self.args:
            args.append(to_doc(arg, ctx))
        return call_expr(name, args)


class Get(SQLNode, metaclass=ExtendAttrsFull):
    """
    `Get` node creates reference to a column in the input dataset/subquery.

    To disambiguate between columns with the same name (like the join output of
    two tables), one of the tables is wrapped in an `As` node. To access wrapped
    columns, the chained syntax for `Get is used: `Get.A.B.C` refers to a column
    wrapped as `... >> As(S.B) >> As(S.A)`. The chained Get node, can also be created
    as: `Get.A >> Get.B >> Get.C`.

    Args:
        name: name of the column reference
        over: name wrapping the dataset which has the column

    Examples:
    ```python
    # get the name of the user
    >>> q = From(Users) >> Select(Get.name)

    # get the name of the user and the num of orders they made
    >>> q = (
            From(Users)
            >> Join(Orders >> As(S.orders), on=eq(Get.id, Get.orders.user_id))
            >> Select(Get.name, Get.orders.count())
        )

    # dereferencing a wrapped column
    >>> q = From(Users) >> As("A") >> As("B") >> As("C") >> Select(Get.C.B.A.name)
    ```
    """

    name: Symbol
    over: Optional[SQLNode]

    def __init__(
        self, name: Union[str, Symbol], over: Optional[SQLNode] = None
    ) -> None:
        super().__init__()
        self.name = S(name)
        self.over = _cast_to_node_skip_none(over)

    def __getattr__(self, name: str) -> "Get":
        return self.__class__(name=S(name), over=self)

    def rebase(self, pre: SQLNode) -> "SQLNode":
        return self.__class__(name=self.name, over=_rebase_node(self.over, pre))

    @check_repr_context
    def pretty_repr(self, ctx: QuoteContext) -> "Doc":
        path = [self.name]
        over = self.over

        while over is not None and isinstance(over, Get) and over not in ctx.vars_:
            path.append(over.name)
            over = over.over
        if over is not None and over in ctx.vars_:
            path.append(ctx.vars_[over])
            over = None
        else:
            path.append(S("Get"))

        ex = ".".join(str(p) for p in reversed(path))
        if over is not None:
            ex = pipe_expr(to_doc(over, ctx), ex)
        return ex


class Group(TabularNode):
    """
    The `Group` node is akin to the SQL `GROUP BY` clause. It partitions the input dataset
    by the grouping key (set of columns specified). and creates a new dataset with exactly
    one row for each distinct value of the grouping key.
    More columns can be added to the grouped  dataset by creating aggregations using the
    `Agg` node. An aggregation computes a unique value for each group, summarizing it as specified.
    If no aggregations are defined, `Group` renders just the dataset with the grouping keys.

    NOTE: `Group` requires unique names for all the columns in the grouping key. So, the downstream
    nodes can refer to these columns directly, without needing a hierarchical `Get` node.

    args:
        *by: column references to group by

    Examples:
    ```python
    # count the total numbrt of users
    >>> q = From(Users) >> Group() >> Select(Agg.count())

    # count the total number of users by city
    >>> q = From(Users) >> Group(Get.city) >> Select(Get.city, Agg.count())

    # all the distinct cities users come from
    >>> q = From(Users) >> Group(Get.city)
    ```
    """

    by: list[SQLNode]
    over: Optional[SQLNode]
    label_map: dict[Symbol, int]

    def __init__(self, *by: NODE_MATERIAL, over: Optional[SQLNode] = None) -> None:
        super().__init__()
        self.by = [_cast_to_node(arg) for arg in by]
        self.over = _cast_to_node_skip_none(over)
        self.label_map = populate_label_map(self, self.by)

    def rebase(self, pre: SQLNode) -> "SQLNode":
        return self.__class__(*self.by, over=_rebase_node(self.over, pre))

    @check_repr_context
    def pretty_repr(self, ctx: QuoteContext) -> "Doc":
        name = "Group"
        args = [to_doc(arg, ctx) for arg in self.by]

        ex = call_expr(name, args)
        if self.over is not None:
            ex = pipe_expr(to_doc(self.over, ctx), ex)
        return ex


class Iterate(TabularNode):
    """
    Iterate is used to generate a recursive CTE. The Postgresql [docs](https://www.postgresql.org/docs/current/queries-with.html#QUERIES-WITH-RECURSIVE)
    do a good job explaining it. FunSQL translates the Iterate node roughly as:

    ```sql
    WITH RECURSIVE `iterator` AS (
        SELECT ...
        FROM `over`
        UNION ALL
        SELECT ...
        FROM `iterator`
    )
    SELECT ...
    FROM `iterator`
    ```

    Args:
        iterator: query that is executed repeatedly
        over: query that provides the initial value

    Examples:
    ```python
    # calculating the factorial function
    >>> q = (
            Define(1 >> As(S.n), 1 >> As(S.fact))
            >> Iterate(
                From(S.factorial)
                >> Define(Fun("+", Get.n, 1) >> As(S.n), Fun("*", Get.n, Get.fact) >> As(S.fact))
                >> Where(Fun("<", Get.n, 10))
                >> As(S.factorial)
            )
        )
    ```
    """

    iterator: SQLNode
    over: Optional[SQLNode]

    def __init__(self, iterator: SQLNode, over: Optional[SQLNode] = None) -> None:
        super().__init__()
        self.iterator = _cast_to_node(iterator)
        self.over = _cast_to_node_skip_none(over)

    def rebase(self, pre: SQLNode) -> "SQLNode":
        return self.__class__(iterator=self.iterator, over=_rebase_node(self.over, pre))

    @check_repr_context
    def pretty_repr(self, ctx: QuoteContext) -> "Doc":
        name = "Iterate"
        args = [to_doc(self.iterator, ctx)]

        ex = call_expr(name, args)
        if self.over is not None:
            ex = pipe_expr(to_doc(self.over, ctx), ex)
        return ex


class Join(TabularNode):
    """
    Join represents the output of a SQL join operation between two tables. The type of join is
    deduced from the input args:
    * INNER JOIN - default
    * LEFT JOIN - `left` is True
    * RIGHT JOIN - `right` is True
    * FULL JOIN - `left` is True, and `right` is True
    * CROSS JOIN - `on` is set to True

    FunSQL renders a lateral join when a variable in the `joinee` branch is binded to a column
    reference in the `over` subquery. Refer to the `Bind` node for an example.

    If the two sides of the join might have the same column names, disambiguate them by wrapping one
    of the tables using the `As` node.

    Args:
        joinee: table on right side of the join
        on: join condition
        left: if it is a left join
        right: if it is a right join
        skip: if True, the Join operation isn't rendered when right side table is not referenced downstream
        over: table on left side of the join

    NOTE: `skip` set to True leaves out the right side of the join if it doesn't contribute any column
    refs downstream. It isn't really desirable though, like in case of an inner join, the right table still
    affects the output through the join condition, so omitting it yields a different query.

    Examples:
    ```python
    # fetch the maximum order value for each user
    >>> q = (
            From(User)
            >> Join(From(Orders) >> As(S.orders), on=Fun("=", Get.id, Get.orders.user_id))
            >> Select(Get.id, Agg.max(Get.orders.value))
        )
    ```
    """

    joinee: SQLNode
    on: SQLNode
    left: bool
    right: bool
    skip: bool
    over: Optional[SQLNode]

    def __init__(
        self,
        joinee: SQLNode,
        on: NODE_MATERIAL,
        left: bool = False,
        right: bool = False,
        skip: bool = False,
        over: Optional[SQLNode] = None,
    ) -> None:
        super().__init__()
        self.joinee = _cast_to_node(joinee)
        self.on = _cast_to_node(on)
        self.left = left
        self.right = right
        self.skip = skip
        self.over = _cast_to_node_skip_none(over)

    def rebase(self, pre: SQLNode) -> "SQLNode":
        return self.__class__(
            joinee=self.joinee,
            on=self.on,
            left=self.left,
            right=self.right,
            skip=self.skip,
            over=_rebase_node(self.over, pre),
        )

    @check_repr_context
    def pretty_repr(self, ctx: QuoteContext) -> "Doc":
        name = "Join"
        args = []

        if ctx.limit:
            args.append("...")
        else:
            args.append(to_doc(self.joinee, ctx))
            args.append(to_doc(self.on, ctx))
            if self.left:
                args.append(assg_expr("left", "True"))
            if self.right:
                args.append(assg_expr("right", "True"))
            if self.skip:
                args.append(assg_expr("skip", "True"))

        ex = call_expr(name, args)
        if self.over is not None:
            ex = pipe_expr(to_doc(self.over, ctx), ex)
        return ex


class Limit(TabularNode):
    """
    `Limit` limits the number of rows returned by a subquery. It is generally used in
    conjunction with the `Order` node, since the query output might not be deterministic
    otherwise.

    Args:
        limit: number of rows to return
        offset: number of rows to skip from the first position

    Examples:
    ```python
    # fetch any 10 user records from the database
    >>> q = From(Users) >> Select(Get.id, Get.name) >> Limit(10)

    # fetch the first 100 users by the date they joined, skipping the first 10
    >>> q = From(Users) >> Order(Get.date_joined) >> Limit(100, offset=10) >> Select(Get.id, Get.name)
    ```
    """

    limit: Optional[int]
    offset: Optional[int]
    over: Optional[SQLNode]

    def __init__(
        self,
        limit: Optional[int] = None,
        offset: Optional[int] = None,
        over: Optional[SQLNode] = None,
    ) -> None:
        super().__init__()
        self.limit = limit
        self.offset = offset
        self.over = over

    def rebase(self, pre: SQLNode) -> "SQLNode":
        return self.__class__(
            self.limit, offset=self.offset, over=_rebase_node(self.over, pre)
        )

    @check_repr_context
    def pretty_repr(self, ctx: QuoteContext) -> "Doc":
        name = "Limit"
        args = []

        if self.limit is not None:
            args.append(to_doc(self.limit))
        if self.offset is not None:
            args.append(assg_expr("offset", to_doc(self.offset)))

        ex = call_expr(name, args)
        if self.over is not None:
            ex = pipe_expr(to_doc(self.over, ctx), ex)
        return ex


class Lit(SQLNode):
    """
    `Lit` node represents a SQL literal value. FunSQL automatically casts values of the
    regular python datatypes to a `Lit` node, when a node value is expected.

    The set of literal types supported by FunSQl include:
    * int
    * float
    * str
    * bool
    * None - translated to a NULL literal
    * datetime.date
    * datetime.time
    * datetime.datetime - translated to a TIMESTAMP
    * datetime.timedelta - translated to an INTERVAL value

    NOTE: Some SQL engines might not support all the literal types listed here, for example,
    SQLite works with date/time values using functions rather than specific data types. In
    these cases, FunSQL will raise an error when serializing the query.

    Args:
        val: literal value

    Examples:
    ```python
    >>> q = Select(1, Lit(2), "hello", Lit("world"), None, Lit(None))
    ```
    """

    val: Any

    def __init__(self, val: Any) -> None:
        super().__init__()
        assert isinstance(
            val, LITERAL_TYPES
        ), f"Unexpected object of type: {type(val)} being cast as a literal"
        self.val = val

    @check_repr_context
    def pretty_repr(self, ctx: QuoteContext) -> "Doc":
        val = to_doc(self.val)
        return f"Lit({val})"


class Order(TabularNode):
    """
    `Order` sorts the results of the input subquery by the `sort key`, specified
    as a set of columns. It is translated to an `ORDER BY` clause in the final SQL
    query.

    Each column reference can be passed into a `Sort` node to specify the sort order
    for it - ascending/descending.

    Args:
        *by: column reference to sort by

    Examples:
    ```python
    # fetch students with their birthdays early in the year
    >>> q = (
            From(Students)
            >> Order(Fun.extract("MONTH", Get.date_of_birth))
            >> Limit(10)
            >> Select(Get.name, Get.date_of_birth)
        )

    # sort error logs by their severity level and reported time, critical ones first
    >>> q = (
            From(ErrorLogs)
            >> Order(Get.severity_level >> Desc(), Get.timestamp)
            >> Limit(10)
        )
    ```
    """

    by: list[SQLNode]
    over: Optional[SQLNode]

    def __init__(
        self,
        *by: SQLNode,
        over: Optional[SQLNode] = None,
    ) -> None:
        super().__init__()
        self.by = [_cast_to_node(arg) for arg in by]
        self.over = _cast_to_node_skip_none(over)

    def rebase(self, pre: SQLNode) -> "SQLNode":
        return self.__class__(*self.by, over=_rebase_node(self.over, pre))

    @check_repr_context
    def pretty_repr(self, ctx: QuoteContext) -> "Doc":
        name = "Order"
        args = []

        if len(self.by) == 0:
            args.append(assg_expr("by", to_doc("[]")))
        else:
            for arg in self.by:
                args.append(to_doc(arg, ctx))

        ex = call_expr(name, args)
        if self.over is not None:
            ex = pipe_expr(to_doc(self.over, ctx), ex)
        return ex


class Partition(TabularNode):
    """
    A `Partition` node bunches together the rows of the input subquery by the `partition key`,
    which is a set of columns; not unlike a Group node. However, instead of splitting the dataset,
    each partition provides a `frame` for the rows in it. Aggregate columns can then be computed
    for the partition, the aggregation for each row is only done over the rows related to it, via
    sharing the same partition `frame`.

    Args:
        *by: column reference to partition by
        order_by: columns to order the rows in each frame by
        frame: argument to further customize the extent of rows related to a row

    Examples:
    ```python
    # calculate country totals with state subtotals already calculated
    >>> q = (
            From(Sales)
            >> Group(Get.country, Get.state)
            >> Select(Get.country, Get.state, Agg.sum(Get.amount) >> As("state_total"))
            >> Partition(Get.country)
            >> Select(Get.country, Get.state, Agg.sum(Get.state_total) >> As("country_total"))
        )

    # calculate the moving average of customer tickets each day
    >>> q = (
            From(Feedback)
            >> Group(Get.date)
            >> Select(Get.date, Agg.avg(Get.tickets) >> As("tickets_avg"))
            >> Partition(order_by=[Get.date],
                         frame=Frame(F.ROWS, F.pre(3), F.follow(3)))
            >> Select(Get.date, Agg.avg(Get.tickets_avg) >> As("tickets_moving_avg"))
        )
    ```
    """

    by: list[SQLNode]
    order_by: list[SQLNode]
    frame: Optional[Frame]
    over: Optional[SQLNode]

    def __init__(
        self,
        *by: SQLNode,
        order_by: Optional[list[SQLNode]] = None,
        frame: Optional[Frame] = None,
        over: Optional[SQLNode] = None,
    ) -> None:
        super().__init__()
        self.by = [_cast_to_node(arg) for arg in by]
        self.order_by = (
            [] if order_by is None else [_cast_to_node(arg) for arg in order_by]
        )
        self.frame = frame
        self.over = _cast_to_node_skip_none(over)

    def rebase(self, pre: SQLNode) -> "SQLNode":
        return self.__class__(
            *self.by,
            order_by=self.order_by,
            frame=self.frame,
            over=_rebase_node(self.over, pre),
        )

    @check_repr_context
    def pretty_repr(self, ctx: QuoteContext) -> "Doc":
        name = "Partition"
        args = []

        for a in self.by:
            args.append(to_doc(a, ctx))
        if len(self.order_by) > 0:
            order_args = list_expr([to_doc(a, ctx) for a in self.order_by])
            args.append(assg_expr("order_by", order_args))
        if self.frame is not None:
            args.append(assg_expr("frame", to_doc(self.frame)))

        ex = call_expr(name, args)
        if self.over is not None:
            ex = pipe_expr(to_doc(self.over, ctx), ex)
        return ex


class Select(TabularNode):
    """
    `Select` defines an ordered set of columns to keep from the input subquery. Each
    column reference must have a unique alias, set explicity using the `As` node or inferred
    by FunSQL.

    Args:
        *args: column references to select

    Examples:
    ```python
    # get the number of flights out of each airport
    >>> q = From(Flights) >> Group(Get.origin) >> Select(Get.origin, Agg.count() >> As("count"))

    # get list of students and their ages
    >>> q = From(Students) >> Select(Get.name, Fun("-", Get.date_of_birth, Fun.now()) >> As("age"))
    ```
    """

    args: list[SQLNode]
    over: Optional[SQLNode]
    label_map: dict[Symbol, int]

    def __init__(
        self,
        *args: NODE_MATERIAL,
        over: Optional[SQLNode] = None,
    ) -> None:
        super().__init__()
        self.args = [_cast_to_node(arg) for arg in args]
        self.over = _cast_to_node_skip_none(over)
        self.label_map = populate_label_map(self, self.args)

    def rebase(self, pre: SQLNode) -> "SQLNode":
        return self.__class__(*self.args, over=_rebase_node(self.over, pre))

    @check_repr_context
    def pretty_repr(self, ctx: QuoteContext) -> "Doc":
        name = "Select"
        args = []

        if len(self.args) == 0:
            args.append(assg_expr("args", to_doc("[]")))
        else:
            for arg in self.args:
                args.append(to_doc(arg, ctx))

        ex = call_expr(name, args)
        if self.over is not None:
            ex = pipe_expr(to_doc(self.over, ctx), ex)
        return ex


class Sort(SQLNode):
    """
    `Sort` node indicates the sorting order for the values in a column, when sorting a
    dataset on it. It is used with the `Order` or `Partition` nodes.

    `Asc` and `Desc` are functions to create instances of `Sort` node directly.

    Args:
        value: sort order ascending/descending?
        nulls: are the null values placed first/last?

    Examples:
    ```python
    # sort patients in decreasing order by their age, alphabetically
    >>> q = From(Patients) >> Order(Get.date_of_birth >> Asc(), Get.name) >> Limit(100)
    ```
    """

    value: ValueOrder
    nulls: Optional[NullsOrder]
    over: Optional[SQLNode]

    def __init__(
        self,
        value: ValueOrder,
        nulls: Optional[NullsOrder] = None,
        over: Optional[SQLNode] = None,
    ) -> None:
        super().__init__()
        self.value = value
        self.nulls = nulls
        self.over = _cast_to_node_skip_none(over)

    def rebase(self, pre: SQLNode) -> "SQLNode":
        return self.__class__(
            value=self.value, nulls=self.nulls, over=_rebase_node(self.over, pre)
        )

    @check_repr_context
    def pretty_repr(self, ctx: QuoteContext) -> "Doc":
        name = "Sort"
        args = []

        args.append(self.value.value)
        if self.nulls is not None:
            args.append(assg_expr("nulls", self.nulls.value))

        ex = call_expr(name, args)
        if self.over is not None:
            ex = pipe_expr(to_doc(self.over, ctx), ex)
        return ex


class Var(SQLNode, metaclass=ExtendAttrsFull):
    """
    `Var` node creates a placeholder for a query parameter. The value for the placeholder
    variable can also be provided using a `Bind` node, either as a SQL literal, or as a
    column reference, in which case it gets rendered as a correlated query/lateral join.
    Refer to the `Bind` node for details.

    Examples:
    ```python
    # get the capital for a country
    >>> q = From(Countries) >> Where(eq(Get.name, Var.COUNTRY_NAME)) >> Select(Get.name, Get.capital_name)
    ```
    """

    name: Symbol

    def __init__(self, name: Union[str, Symbol]) -> None:
        super().__init__()
        self.name = S(name)

    @check_repr_context
    def pretty_repr(self, ctx: QuoteContext) -> "Doc":
        return f"Var.{self.name}"


class Where(TabularNode):
    """
    Filter the input dataset with the given condition, returning a subset of the rows in it.

    Args:
        condition: an input/derived column of boolean type, rows where it is false are filtered out

    Examples:
    ```python
    # fetch names of students playing basketball
    >>> q = (
            From(Students)
            >> Join(From(Activities) >> As("activity"), on=eq(Get.student_id, Get.id))
            >> Where(Fun("=", Get.activity.name, "basketball"))
            >> Select(Get.name)
        )
    ```
    """

    condition: SQLNode
    over: Optional[SQLNode]

    def __init__(
        self, condition: NODE_MATERIAL, over: Optional[SQLNode] = None
    ) -> None:
        super().__init__()
        self.condition = _cast_to_node(condition)
        self.over = _cast_to_node_skip_none(over)

    def rebase(self, pre: SQLNode) -> "SQLNode":
        return self.__class__(
            condition=self.condition, over=_rebase_node(self.over, pre)
        )

    @check_repr_context
    def pretty_repr(self, ctx: QuoteContext) -> "Doc":
        name = "Where"
        args = [to_doc(self.condition, ctx)]
        ex = call_expr(name, args)
        if self.over is not None:
            ex = pipe_expr(to_doc(self.over, ctx), ex)
        return ex


class With(TabularNode):
    """
    `With` node is used to create a CTE(common table expression). For larger queries,
    abstracting out a subquery that is used multiple times is helpful.

    The subquery wrapped using a `With` node can be accessed in its *parent* node, specified
    using the `over` argument. Each subquery must be aliased using an `As` node, so it can be
    referenced by the alias.

    Args:
        args: subqueries to be wrapped in a CTE, multiple can be specified.

    Examples:
    ```python
    # fetch the names of students playing basketball
    >>> q = (
            From(Students)
            >> Where(Fun.in(Get.id, From(S.bb_players) >> Select(Get.student_id)))
            >> With(From("Activities") >> Where(Fun("=", Get.name, "basketball")) >> As("bb_players"))
        )
    """

    args: list[SQLNode]
    materialized: Optional[bool]
    label_map: dict[Symbol, int]
    over: Optional[SQLNode]

    def __init__(
        self,
        *args: SQLNode,
        materialized: Optional[bool] = None,
        over: Optional[SQLNode] = None,
    ) -> None:
        super().__init__()
        self.args = [_cast_to_node(arg) for arg in args]
        self.materialized = materialized
        self.over = _cast_to_node_skip_none(over)
        self.label_map = populate_label_map(self, self.args)

    def rebase(self, pre: SQLNode) -> "SQLNode":
        return self.__class__(
            *self.args,
            materialized=self.materialized,
            over=_rebase_node(self.over, pre),
        )

    @check_repr_context
    def pretty_repr(self, ctx: QuoteContext) -> "Doc":
        name = "With"
        args = []

        if len(self.args) == 0:
            args.append(assg_expr("args", to_doc("[]")))
        else:
            for a in self.args:
                args.append(to_doc(a, ctx))
        if self.materialized is not None:
            args.append(assg_expr("materialized", to_doc(self.materialized)))

        ex = call_expr(name, args)
        if self.over is not None:
            ex = pipe_expr(to_doc(self.over, ctx), ex)
        return ex


# -----------------------------------------------------------
# utility functions to export
# -----------------------------------------------------------


def Asc(nulls: Optional[NullsOrder] = None) -> Sort:
    """Shorthand to create a Sort node with ASC direction"""
    return Sort(ValueOrder.ASC, nulls=nulls)


def Desc(nulls: Optional[NullsOrder] = None) -> Sort:
    """Shorthand to create a Sort node with DESC direction"""
    return Sort(ValueOrder.DESC, nulls=nulls)


def aka(*args, **kwargs) -> SQLNode:
    """Shorthand to create an alias for a node. To be used as:

    >>> aka(Get.person, S.person_id)
    >>> aka(100, "count")
    >>> aka(count=100)
    >>> aka(sum=Agg.sum(Get.population))
    etc.
    """
    assert (
        len(args) == 0 or len(kwargs) == 0
    ), "only one of args or kwargs should be passed"

    if len(kwargs) == 1:
        name, node_like = next(iter(kwargs.items()))
    elif len(args) == 2:
        node_like, name = args
    else:
        raise Exception("Invalid arguments passed to the aliasing routine `aka`")
    return As(name=S(name), over=_cast_to_node(node_like))


def eq(left: SQLNode, right: SQLNode) -> SQLNode:
    """Shorthand to create a Function node checking equality"""
    return Fun("=", left, right)


def LeftJoin(
    joinee: SQLNode, on: NODE_MATERIAL, over: Optional[SQLNode] = None
) -> "Join":
    """Shorthand to create a left join node"""
    return Join(joinee=joinee, on=on, left=True, over=over)


class F:
    """Shorthand to pass arguments to window frame objects"""

    ROWS = FrameMode.ROWS
    RANGE = FrameMode.RANGE
    GROUPS = FrameMode.GROUPS
    EXCL_CURR = FrameExclude.CURRENT_ROW
    EXCL_GROUP = FrameExclude.GROUP
    EXCL_TIES = FrameExclude.TIES

    @staticmethod
    def curr_row() -> FrameEdge:
        return FrameEdge(FrameEdgeSide.CURRENT_ROW)

    @staticmethod
    def pre(val: Union[None, Any] = None) -> FrameEdge:
        return FrameEdge(FrameEdgeSide.PRECEDING, val)

    @staticmethod
    def follow(val: Union[None, Any] = None) -> FrameEdge:
        return FrameEdge(FrameEdgeSide.FOLLOWING, val)


# -----------------------------------------------------------
# label method implemented for nodes
# -----------------------------------------------------------


@register_union_type(label)
def _(node: Union[Agg, Fun, Get]) -> Symbol:
    return node.name


@label.register
def _(node: As) -> Symbol:
    return node.name


@label.register
def _(node: Append) -> Symbol:
    lbl = label(node.over)
    if all(label(arg) == lbl for arg in node.args):
        return lbl
    else:
        return S("union")


@label.register
def _(node: From) -> Symbol:
    if isinstance(node.source, SQLTable):
        return node.source.name
    elif isinstance(node.source, Symbol):
        return node.source
    elif isinstance(node.source, dict):
        return S("values")
    else:
        return label(None)


@label.register
def _(node: Lit) -> Symbol:
    return label(None)


@register_union_type(label)
def _(node: Union[Bind, Define, Group, Iterate, Join, Limit, Order]) -> Symbol:
    return label(node.over)


@register_union_type(label)
def _(node: Union[Partition, Select, Sort, Var, Where, With]) -> Symbol:
    return label(node.over)
