## Query compilation

[INCOMPLETE]

The FunSQL library makes multiple passes over the input node graph, to compile it down to a valid SQL query. 

### Annotate
* Tabular node objects represent a SQL subquery (something that can go into the `FROM` clause of a `SELECT` statement). With each tabular node, a `Box` node is attached which carries metadata relevant to the subquery. 
If the node graph is, A -> B -> C, node B is boxed by rewriting the graph as, A -> B -> Box_B -> C. 
The `typ` attribute on the box node is a namespace for all the references available at this node. The "type" system in FunSQL includes three types of fields. 
* scalar type - regular column reference
* group - list of column references over which an aggregation can be computed. Constructed for `group` and `partition` nodes. 
* a field pointing to another namespace. Nested namespaces are accessed through hierarchical nodes. 

* Unbound references. The column references are resolved based on which node they are being accessed in. 
* FunSQL also support bound nodes, where instead of accessing a column through the ordered list of namespaces, you can access it through the node that hosts the namespace too. Bound references. 

* Reversing column references. 
 * For illustration, consider the contrived node graph: `From(table) >> As("A") >> As("B") >> Select(Get.B.A.col_1)`. When boxing the `Select` node, we encounter the hierarchical node (`Get.B.A.col_1`), which can also be expressed as, `Get(name = col_1, over=Get.B.A)`. 
 * To assert this is a legitimate reference, we need to traverse up the query graph and check if the node preceding the `Select` has a namespace named `B`, that has a namespace named `A` nested inside, which in turn hosts `col_1`. However, on recursing the reference, `Get.B >> Get.A >> Get.col_1`, we see `col_1` first, `A` next and `B` in the end. 
 * Therefore, we replace this `Get` node with a set of nodes in the inverted hierarchy, `Get.col_1 >> NameBound(A) >> NameBound(B)`. `NameBound` is an intermediate node that can be validated by just checking if a namespace with the corresponding _name_ is present in the current node. Validating that the parent to a `NameBound` node is a valid reference is now delegated up the graph to the parent of the corresponding subquery node (from `Select` to the `As` node preceding it). 

 * Refactoring some nodes
  * From - specialized into FromReference, FromTable or a FromValues node based on the source. Column references are generally rooted at a `From` node, so this is needed to deduce which references are valid. 
  * Bind
  * Join - cast to an `IntJoin` node. No major difference, just adds a couple more attributes useful to the compiler when resolving references provided by the joined subuery. 
  * Iterate

### Resolve

### Link

### Translate
Node graph is converted to a graph of SQLClause objects, which is a stand in for the lexical structure of the final SQL. 

### Serialize
* Serialization dialect - render differently for each target database

