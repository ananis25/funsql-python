## Contents

* `usage-guide.ipynb` - Introduces the verbs available in FunSQL, and how to assmeble queries using them. 

* `compiler-internals.md` - Scattered notes on how the FunSQL compiler works. 

### Tests

These notebooks document the full FunSQL API. 

* `tests/using-nodes.ipynb` - This is the user facing API, and adds more detail about each FunSQL node.

* `tests/using-clauses.ipynb` - FunSQL compiles the tree of SQL nodes to something close to the lexical structure of SQL, called clause objects.  These directly translate to SQL text, only abstracting over spaces and dialect specific punctuation.  When projects like [Substrait](https://substrait.io/) are further along, might be a good idea to use that as a backend instead. 

### Debugging

This directory stores the compiler output at different stages for some typical queries. 