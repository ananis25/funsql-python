# funsql-python

[![PyPI](https://img.shields.io/pypi/v/funsql-python.svg)](https://pypi.org/project/funsql-python/)
[![Changelog](https://img.shields.io/github/v/release/ananis25/funsql-python?include_prereleases&label=changelog)](https://github.com/ananis25/funsql-python/releases)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](https://github.com/ananis25/funsql-python/blob/main/LICENSE)

`funsql` is a python library to write SQL queries in a way that is more composable. 

This implementation follows closely the original Julia library [FunSQL.jl](https://github.com/MechanicalRabbit/FunSQL.jl/). Thanks to the original authors who have been refining the idea for some time! While I try improve the documentation here, here is material from the parent project which motivates the library. 
1. [Why FunSQL?](https://mechanicalrabbit.github.io/FunSQL.jl/stable/guide/#Why-FunSQL?)
2. A [presentation](https://www.youtube.com/watch?v=rGWwmuvRUYk) from JuliaCon

The API is pretty similar to the original Julia library, so you are good to go. 

## Concept

* Track the shape of the data through operations, and the namespace. 

### More notes
Q. How much of SQL syntax is supported? 
A. Window functions, correlated/lateral join queries, CTEs. are all supported. Aggregation queries like Cube/Rollup, Grouping Sets, etc. haven't been implemented yet. 
    FunSQL is oblivious to the specific UDF/aggregate functions supported by database engines, if they fit the `Fun` node syntax, FunSQL can include it in the output SQL query.

Q. Which databases can FunSQL work with? 
A. Currently, Sqlite/Postgres. Maybe MYSQL, but I have never used it. 

    As noted above, FunSQL models the shape of the data, and its namespace through different tabular operations. After resolving column references, and verifying the query is legitimate, FunSQL compiles the input tree of SQL nodes to a tree of SQL clause objects. These directly translate to SQL text, only abstracting over spaces and dialect specific punctuation. 

    However, SQL dialects are plenty and projects like [Apache Calcite](https://calcite.apache.org/) already exist, that can write to different SQL dialects. A better idea is to compile the FunSQL query treee to the relational node structure `Calcite` works with. That would let us support most of the popular database engines.
    
    The only blocker is that `Calcite` is a Java library; I have never written Java, and don't know how to compile it to a native extension that is usable from python without installing a JVM. When projects like [Substrait](https://substrait.io/) are further along, might be a good idea to use that as a backend instead. 


## Installation

Install this library using `pip`:

    $ pip install funsql-python

## Usage

The `docs` directory has examples on how to use the library.
* `using-nodes.ipynb` - This is the user facing API, and shows how to use FunSQL to construct SQL queries. 
* `using-clauses.ipynb` - FunSQL compiles the tree of SQL nodes to something close to the lexical structure of SQL, called clause objects. These directly translate to SQL text, only abstracting over spaces and dialect specific punctuation. When projects like [Substrait](https://substrait.io/) are further along, might be a good idea to use that as a backend instead. 

The `examples` directory has more examples of queries written using FunSQL. 

## Development

To contribute to this library, checkout the code in a new virtual enviroment. 

Now install the dependencies and test dependencies:

    pip install -e '.[test]'

To run the tests:

    pytest
