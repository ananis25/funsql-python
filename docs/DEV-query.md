### query
```python
>>> from funsql import *

>>> table = SQLTable("table", columns=["col_1", "col_2"])
>>> q = From(table) >> As("A") >> As("B") >> Select(Get.B.A.col_1)
```

### Annotate pass
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