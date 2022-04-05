# funsql-python

[![PyPI](https://img.shields.io/pypi/v/funsql-python.svg)](https://pypi.org/project/funsql-python/)
[![Changelog](https://img.shields.io/github/v/release/ananis25/funsql-python?include_prereleases&label=changelog)](https://github.com/ananis25/funsql-python/releases)
[![License](https://img.shields.io/badge/license-Apache%202.0-blue.svg)](https://github.com/ananis25/funsql-python/blob/main/LICENSE)

`funsql` is a python library to write SQL queries in a way that is more composable. 

The initial implementation is mostly a line by line port of the Julia library [FunSQL.jl](https://github.com/MechanicalRabbit/FunSQL.jl/). Thanks to the original authors who have been refining the idea for some time! While I try improve the documentation here, go over to the parent repository which motivates the project. 

The `docs` directory has examples on how to use the library.
* `using-nodes.ipynb` - shows how to use FunSQL to construct SQL queries. This is the user facing API. 
* `development/using-clauses.ipynb` - FunSQL represents the SQL syntax using clause objects. This notebook shows how they get compiled to SQL strings. 

The repository [funsql-examples](https://github.com/ananis25/funsql-examples) contains more examples of queries written using FunSQL. 

## Installation

Install this library using `pip`:

    $ pip install funsql-python

## Usage

Usage instructions go here.

## Development

To contribute to this library, checkout the code in a new virtual enviroment. 

Now install the dependencies and test dependencies:

    pip install -e '.[test]'

To run the tests:

    pytest
