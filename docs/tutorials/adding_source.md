# Adding a source

## Add a source
To add a source, create a new file in `viadot/sources`. The source must inherit from the `Source` or `SQL` class and have credentials implemented using [Pydantic](https://medium.com/mlearning-ai/improve-your-data-models-with-pydantic-f9f10ca66f26). You can also take a look at any existing source like [Databricks](https://github.com/dyvenia/viadot/blob/2.0/viadot/sources/databricks.py) as a reference. 

## Add unit tests
To add a unit tests, create a new file in `viadot/tests/unit`. The tests should cover each of the functionalities of the source.

