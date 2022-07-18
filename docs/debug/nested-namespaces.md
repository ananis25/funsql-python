## query

This query illustrates how namespaces work in FunSQL. Each time a tabular node is piped into an `As` node, its namespace is nested inside a field named with the alias provided by `As`. 

```python
>>> from funsql import *

>>> table = SQLTable("table", columns=["col_1", "col_2"])
>>> q = From(table) >> As("A") >> As("B") >> Select(Get.B.A.col_1)
```

## Annotate pass

```python
>>> render(q, RenderDepth.ANNOTATE)

let table = SQLTable(table, ...),
    q1 = FromTable(table),
    q2 = q1 >> Box(),
    q3 = q2 >> As(A) >> Box(),
    q4 = q3 >> As(B) >> Box(),
    q5 = q4 >>
         Select(NameBound(over = NameBound(over = Get.col_1, name = A),
                          name = B)),
    q6 = q5 >> Box(),
    q6
end
```

### Resolve pass
```python
>>> render(q, RenderDepth.RESOLVE)

let table = SQLTable(table, ...),
    q1 = FromTable(table),
    q2 = q1 >>
         Box(type = BoxType(table, col_1: ScalarType(), col_2: ScalarType())),
    q3 = q2 >> As(A) >>
         Box(type = BoxType(A,
                            A: RowType(col_1: ScalarType(),
                                       col_2: ScalarType()))),
    q4 = q3 >> As(B) >>
         Box(type = BoxType(B,
                            B: RowType(A: RowType(col_1: ScalarType(),
                                                  col_2: ScalarType())))),
    q5 = q4 >>
         Select(NameBound(over = NameBound(over = Get.col_1, name = A),
                          name = B)),
    q6 = q5 >> Box(type = BoxType(B, col_1: ScalarType())),
    q6
end
```

### Link pass
```python
>>> render(q, RenderDepth.LINK)

let table = SQLTable(table, ...),
    q1 = FromTable(table),
    q2 = Get.col_1,
    q3 = q1 >>
         Box(type = BoxType(table, col_1: ScalarType(), col_2: ScalarType()),
             refs = [q2]),
    q4 = NameBound(over = q2, name = A),
    q5 = q3 >> As(A) >>
         Box(type = BoxType(A,
                            A: RowType(col_1: ScalarType(),
                                       col_2: ScalarType())),
             refs = [q4]),
    q6 = NameBound(over = q4, name = B),
    q7 = q5 >> As(B) >>
         Box(type = BoxType(B,
                            B: RowType(A: RowType(col_1: ScalarType(),
                                                  col_2: ScalarType()))),
             refs = [q6]),
    q8 = q7 >> Select(q6),
    q9 = q8 >> Box(type = BoxType(B, col_1: ScalarType()), refs = [Get.col_1]),
    q9
end
```

### Translate pass
```python
>>> render(q, RenderDepth.TRANSLATE)

ID(table) >> AS(table_1) >> FROM() >> SELECT(ID(table_1) >> ID(col_1))
```

### Serialization
```python
>>> render(q, RenderDepth.SERIALIZE)

SELECT "table_1"."col_1"
FROM "table" AS "table_1"
```