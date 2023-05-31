# Adding a source
## Add a source
Let's assume that your goal is to create a data source that retrieves data from a PostgresSQL database. The first thing you need to do is create a class that inherits from the `Source` or `SQL` class and implement credentials using [Pydantic](https://medium.com/mlearning-ai/improve-your-data-models-with-pydantic-f9f10ca66f26) models.

```python
# inside source/postgresql.py file

from pydantic import BaseModel

class PostgreSQLCredentials(BaseModel):
    host: str
    port: int = 5432
    database: str
    user: str
    password: str

class PostgreSQL(SQL):
    
    def __init__(
        self,
        credentials: PostgreSQLCredentials = None,
        config_key: str = None,
        *args,
        **kwargs,
    ):
```

Credentials can now be provided directly as the `credentials` parameter or by using the `config_key`.

The `config_key` parameter  is a specific key name for the credentials stored locally on our computer in either the `config.yml` or `config.json` file. By default, these files are searched in the directory `$HOME/.config/viadot`. 

When you are done with the source, remember to import it in the `__init__.py` file. 

```python
# inside source/__init__.py

from .postgresql import PostgreSQL
```

And that's all you need to know to create your own sources in the viadot repository. You can also take a look at any existing source like [Databricks](https://github.com/dyvenia/viadot/blob/2.0/viadot/sources/databricks.py) as a reference. 

## Add unit tests
To add a unit tests, create a new file in `viadot/tests/unit`. All tests for a given source should be in one file and use the [pytest](https://docs.pytest.org/en/7.3.x/) framework. Your PR will be accepted only if the test coverage is greater than or equal to 80%.

