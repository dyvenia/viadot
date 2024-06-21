# Adding a source

## Adding general source
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

The `config_key` parameter  is a specific key name for the credentials stored locally on our computer in either the `config.yml` or `config.json` file. By default, these files are searched in the directory `~/.config/viadot`. 

Remember to expose methods `to_df()` and `to_json()` which are abstract methods required in the implementation of child classes. Add the decoration function `add_viadot_metadata_columns` to the `to_df()` method. This decorator adds two columns (`_viadot_source`, `_viadot_downloaded_at_utc`) to our DataFrame.

```python
# Further part of PostgreSQL class.

    @add_viadot_metadata_columns
    def to_df(
        self,
        query: str,
        con: pyodbc.Connection = None,
        if_empty: Literal["warn", "skip", "fail"] = "warn",
    ) -> pd.DataFrame:
        """Creates DataFrame form SQL query."""
```

When you are done with the source, remember to import it in the `__init__.py` file. 

```python
# inside source/__init__.py

from .postgresql import PostgreSQL
```

And that's all you need to know to create your own sources in the viadot repository. You can also take a look at any existing source like [MiniIO](https://github.com/dyvenia/viadot/blob/2.0/src/viadot/sources/minio.py) as a reference. 

## Adding optional source

In the previous example, we added a source that is general and can be used by all, regardless of whether the cloud provider can use it. Viadot also contains cloud-specific sources, which are optional, and we have to handle that case when optional packages are not installed. We have to put packages that are not always installed into try-except block, as below:

```python
import os
from typing import Any, Dict, List

import pandas as pd

try:
    from adlfs import AzureBlobFileSystem, AzureDatalakeFileSystem
except ModuleNotFoundError:
    raise ImportError("Missing required modules to use AzureDataLake source.")

from viadot.config import get_source_credentials
from viadot.exceptions import CredentialError
from viadot.sources.base import Source
from viadot.utils import add_viadot_metadata_columns


class AzureDataLake(Source):
    """
    A class for pulling data from the Azure Data Lakes (gen1 and gen2).
```

The second step is to add if statements to the `__init__.py` file:

```python
from importlib.util import find_spec

from .cloud_for_customers import CloudForCustomers
from .exchange_rates import ExchangeRates
from .genesys import Genesys
from .sharepoint import Sharepoint
from .trino_source import Trino

__all__ = [
    "CloudForCustomers",
    "ExchangeRates",
    "Genesys",
    "Sharepoint",
    "Trino",
]

if find_spec("adlfs"):
    from viadot.sources.azure_data_lake import AzureDataLake  # noqa

    __all__.extend(["AzureDataLake"])
```

## Adding tests
To add unit tests for your source, create a new file, `viadot/tests/unit/test_<your_source_name>.py`, and add your tests inside it.

## Adding tests for optional sources

In case we want to add tests to optional sources like `AzureDataLake` we need to add if statements as below,  which will skip the tests if optional packages are not installed in the environment.

```python
import pandas as pd
import pytest

try:
    from viadot.sources import AzureDataLake

    _adls_installed = True
except ImportError:
    _adls_installed = False

if not _adls_installed:
    pytest.skip("AzureDataLake source not installed", allow_module_level=True)

```