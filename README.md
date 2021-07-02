# GeeseDB
Graph Engine for Exploration and Search

GeeseDB is a Python toolkit for solving information retrieval research problems that leverage graphs as data structures. It aims to simplify information retrieval research by allowing researchers to easily formulate graph queries through a graph query language. GeeseDB is built on top of [DuckDB](http://duckdb.org/), an embedded column-store relational database designed for analytical workloads.

GeeseDB is available as an easy to install Python package. In only a few lines of code users can create a first stage retrieval ranking using BM25. Queries read and write Numpy arrays and Pandas dataframes, at zero or negligible data transformation cost (dependent on base datatype). Therefore, results of a first-stage ranker expressed in GeeseDB can be used in various stages in the ranking process, enabling all the power of Python machine learning libraries with minimal overhead. Also, because data representation and processing are strictly separated, GeeseDB forms an ideal basis for reproducible IR research.

## Package Installation

Install latest version of GeeseDB via [PyPI](https://pypi.org/project/geesedb/):

```
pip install geesedb==0.0.2
```

GeeseDB depends on a couple of packages that can also be installed using `pip`. It is also possible to install the development installation of GeeseDB using `pip`:

```
pip install git+https://github.com/informagi/GeeseDB.git
```

If you are planning to contribute to the package it is possible to clone the package, and install it using `pip` in editable version:
```
git clone git@github.com:informagi/GeeseDB.git && cd GeeseDB && pip install -e .
```

You can run our tests to confirm if everything is working as intended (in the repository folder):
```
pytest
```
