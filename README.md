# funsql-python

[![PyPI](https://img.shields.io/pypi/v/funsql-python.svg)](https://pypi.org/project/funsql-python/)
[![Changelog](https://img.shields.io/github/v/release/ananis25/funsql-python?include_prereleases&label=changelog)](https://github.com/ananis25/funsql-python/releases)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](https://github.com/ananis25/funsql-python/blob/main/LICENSE)

`funsql` is a python library to write SQL queries in a way that is more composable. This implementation follows closely the original Julia library [FunSQL.jl](https://github.com/MechanicalRabbit/FunSQL.jl/). Thanks to the original authors who have been refining the idea for some time! 

While I try improve the documentation here, here is material from the parent project which motivates the library. The python API is pretty similar to the original Julia library, so after reading the original docs, you are good to go. 
1. [Why FunSQL?](https://mechanicalrabbit.github.io/FunSQL.jl/stable/guide/#Why-FunSQL?)
2. A [presentation](https://www.youtube.com/watch?v=rGWwmuvRUYk) from JuliaCon


## Usage

The `docs` directory has examples on how to use the library.
* `using-nodes.ipynb` - This is the user facing API, and shows how to use FunSQL to construct SQL queries. 
* `using-clauses.ipynb` - FunSQL compiles the tree of SQL nodes to something close to the lexical structure of SQL, called clause objects. These directly translate to SQL text, only abstracting over spaces and dialect specific punctuation. When projects like [Substrait](https://substrait.io/) are further along, might be a good idea to use that as a backend instead. 

The `examples` directory has more examples of queries written using FunSQL. 

## Concept

FunSQL tracks the shape of the data as SQL operations are applied to it, and uses it to assert if a particular pipeline of operations yield a valid query. [TODO]

## More notes
* Q. Supported SQL subset? 

    **Ans**. Window functions, correlated/lateral join queries, CTEs. are all supported. Aggregation queries like Cube/Rollup, Grouping Sets, etc. haven't been implemented yet. 
    FunSQL is oblivious to the specific UDF/aggregate functions supported by database engines, if they fit the `Fun` node syntax, FunSQL can include it in the output SQL query.

* Q. Supported database engines? 

    **Ans**. FunSQL is not a database connector and only produces the SQL query string. Currently, it can produce queries in the Sqlite/Postgres dialect. Maybe MySQL, but I have never used it. 

    As noted above, FunSQL models the shape of the data, and its namespace through different tabular operations. After resolving column references, and verifying the query is legitimate, FunSQL compiles the input tree of SQL nodes to a tree of SQL clause objects. These directly translate to SQL text, only abstracting over spaces and dialect specific punctuation. 

    However, SQL dialects are plenty and projects like [Apache Calcite](https://calcite.apache.org/) already exist, that can write to different SQL dialects. A better idea is to compile the FunSQL query treee to the relational node structure `Calcite` works with. That would let us support most of the popular database engines.
    
    The only blocker is that `Calcite` is a Java library; I have never written Java, and don't know how to compile it to a native extension that is usable from python without installing a JVM. When projects like [Substrait](https://substrait.io/) are further along, it might be a good idea to use that as a backend instead. 

* Q. Supported languages? 
    
    **Ans**. This repository implements a python library, while the original implementation of FunSQL is in Julia. The core idea of tracking column references and data shape is not a lot of code and easy enough to port. Once we can integrate with the Substrait/Calcite projects, I intend to write a Rust implementation, so individual language bindings are even shorter. 

* Q. Similar projects? 

    **Ans**. There are multiple libraries/languages that make writing SQL easier. 

    * Pipeline DSLs: [dplyr](https://github.com/tidyverse/dplyr), [prql](https://github.com/prql/prql), [ibis](https://github.com/ibis-project/ibis). 
    
        These query languages define a set of `verbs`, each representing a table operation and let us define analytics queries incrementally. The FunSQL Julia library can be used similarly, with the distinction that the `query verbs` are closer to their SQL counterparts. However, the analytical DSLs are generally more concise to query data. Further, the python FunSQL implementation doesn't sugar the syntax at all and is clunky to directly write queries in. 
        
        The benefit of FunSQL is that query fragments are regular objects in the host language (Julia/Python), and can be manipulated or composed freely. This makes writing your own query DSLs on top of it, or extending it to support new syntactic features easy! Though I would think projects like `prql` and `ibis` can probably be used similarly by working with their internal compiler implementations. 

    * ORMs: [SQLAlchemy](https://www.sqlalchemy.org/). 
    
        ORMs simplify interaction with databases by letting us define language constructs like python classes mapping to database tables, and then writing queries by calling methods on them. I would expect the SQLAlchemy core library can be used to build queries incrementally, but haven't delved into it much. 

    * Query Builders: [PyPika](https://github.com/kayak/pypika). 
    
        Pypika converts a data structure assembled in python to a SQL query string, and shares the scope of FunSQL. However, it is a thin wrapper around SQL expressions and doesn't model the semantics of SQL operations, resulting in incorrect output. 

            ```py
            from pypika import Query, Table        
            c = Table("customers")
            q1 = Query.from_(c).limit(100).where(c.city == "Mumbai").select(c.name)
            q2 = Query.from_(c).where(c.city == "Mumbai").limit(100).select(c.name)

            print(str(q1)) 
            # SELECT "name" FROM "customers" WHERE "city"='Mumbai' LIMIT 100
            print(str(q2))
            # SELECT "name" FROM "customers" WHERE "city"='Mumbai' LIMIT 100
            ```

    * Other projects: [Malloy](https://github.com/looker-open-source/malloy) is a super cool project that models relational data and queries against it, using a single language. Queries are constructed as resuable fragments that can be composed/nested arbitrarily, and get compiled to SQL at execution time. 

        FunSQL operators are similar in that they can be arbitrarily composed, though it doesn't implement the NEST operator yet. It should be possible to use FunSQL for implementing a watered down version of Malloy in the language of your choice, though Malloy is pretty comprehensive (database connectors, built in graphing, tracking lineage) and you should use it. 

## Installation

Install this library using `pip`:

    $ pip install funsql-python

## Development

To contribute to this library, checkout the code in a new virtual enviroment. 

Now install the dependencies and test dependencies:

    pip install -e '.[test]'

To run the tests:

    pytest
