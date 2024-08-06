# Creating a source connector

The first thing you need to do is create a class that inherits from the `SQL` class. You should also specify a [pydantic](https://medium.com/mlearning-ai/improve-your-data-models-with-pydantic-f9f10ca66f26) model for the source's credentials:

```python
# sources/postgresql.py

"""PostgreSQL connector."""

from viadot.sources.base import SQL
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
        credentials: PostgreSQLCredentials | None = None,
        config_key: str | None = None,
        *args,
        **kwargs,
    ):
        ...
```

Credentials can now be provided directly as the `credentials` parameter or by [using the config key](../user_guide/config_key.md).

!!! warning "viadot metadata - hardcoded schemas workaround"

    The addition of viadot metadata columns (currently, `_viadot_source` and `_viadot_downloaded_at_utc`) should be done in the base class's `to_df()` method. However, due to some production uses of viadot relying on hardcoded DataFrame schemas (and not being able to either pin the `viadot` version or fix the hardcoding), this cannot currently be done. As a workaround, you need to implement the `to_df()` method in your source and add the columns yourself.

    Below is a an example for our Postgres connector. Since we can reuse the parent class's `to_df()` method, we're simply wrapping it with the `add_viadot_metadata_columns` decorator:

    ```python hl_lines="27-35"
    # sources/postgresql.py

    """PostgreSQL connector."""

    from viadot.sources.base import SQL
    from viadot.utils import add_viadot_metadata_columns
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
            credentials: PostgreSQLCredentials | None = None,
            config_key: str | None = None,
            *args,
            **kwargs,
        ):
            ...

        @add_viadot_metadata_columns
        def to_df(
            self,
            query: str,
            con: pyodbc.Connection | None = None,
            if_empty: Literal["warn", "skip", "fail"] = "warn",
        ) -> pd.DataFrame:
            """Execute a query and return the result as a pandas DataFrame."""
            super().to_df()
    ```

    For more information, see [this issue](https://github.com/dyvenia/viadot/issues/737).

Now, we also need to add a way to pass the credentials to the parent class:

```python hl_lines="7 32-36"
# sources/postgresql.py

"""PostgreSQL connector."""

from viadot.sources.base import SQL
from viadot.utils import add_viadot_metadata_columns
from viadot.config import get_source_credentials
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
        credentials: PostgreSQLCredentials | None = None,
        config_key: str | None = None,
        *args,
        **kwargs,
    ):
        """A PostgreSQL connector.

        Args:
            credentials (PostgreSQLCredentials, optional): Database credentials.
            config_key (str, optional): The key in the viadot config holding relevant credentials.
        """
        raw_creds = credentials or get_source_credentials(config_key) or {}
        validated_creds = PostgreSQLCredentials(**raw_creds).dict(
            by_alias=True
        )
        super().__init__(*args, credentials=validated_creds, **kwargs)

    @add_viadot_metadata_columns
    def to_df(
        self,
        query: str,
        con: pyodbc.Connection | None = None,
        if_empty: Literal["warn", "skip", "fail"] = "warn",
    ) -> pd.DataFrame:
        """Execute a query and return the result as a pandas DataFrame."""
        super().to_df()
```

Once you're done with the source, remember to import it in `sources/__init__.py`, so that it can be imported with `from viadot.sources import PostgreSQL` (instead of `from viadot.sources.postgresql import PostgreSQL`):

```python
# sources/__init__.py

from .postgresql import PostgreSQL

__all__ = [
    ...,
    "PostgreSQL"
]
```

## Sources using optional dependencies

In case your source uses an [optional dependency](https://github.com/dyvenia/viadot/blob/2.0/pyproject.toml), you need to escape the import. In the example below, our source uses the optional `adlfs` package (part of the `viadot-azure` extra):

```python hl_lines="5-8"
# sources/azure_data_lake.py

from viadot.sources.base import Source

try:
    from adlfs import AzureBlobFileSystem, AzureDatalakeFileSystem
except ModuleNotFoundError:
    raise ImportError("Missing required modules to use AzureDataLake source.")


class AzureDataLake(Source):
    ...
```

The import in `sources/__init__.py` also needs to be guarded:

```python hl_lines="13-16"
# sources/__init__.py

from importlib.util import find_spec

from .cloud_for_customers import CloudForCustomers
...

__all__ = [
    "CloudForCustomers",
    ...
]

if find_spec("adlfs"):
    from viadot.sources.azure_data_lake import AzureDataLake  # noqa

    __all__.extend(["AzureDataLake"])
```

## Adding tests

Make sure to add tests for your source!

### Unit

You can think of unit tests as tests which do not require internet connection or connectivity to the actual data source or destination. All unit tests are executed automatically on each PR to `viadot`'s default branch.

A common practice to ensure above requirements are met is to mock the external systems. For example, if we wish to create a unit test for our `Sharepoint` source which will test the `to_df()` method, which in turn depends on the `_download_excel()` method, we must first mock the `_download_excel()` method so that it doesn't actually try to download any data. Below is an example of how you can accomplish this:

```python
# tests/unit/test_sharepoint.py

import pandas as pd
from viadot.sources import Sharepoint

TEST_CREDENTIALS = {"site": "test", "username": "test2", "password": "test"}

class SharepointMock(Sharepoint):
    def _download_excel(self, url=None):
        """Returns a test DataFrame instead of calling a Sharepoint server."""
        return pd.ExcelFile(Path("tests/unit/test_file.xlsx"))

def test_sharepoint():
    s = SharepointMock(credentials=TEST_CREDENTIALS)
    df = s.to_df(url="test")

    assert not df.empty
```

### Integration

Integration tests connect to the actual systems. For these tests, you will need to set up your viadot config with proper credentials. For example, to test a `Sharepoint` source, our config could look like this:

```yaml
# ~/.config/viadot/config.yaml
version: 1

sources:
  - sharepoint_dev:
      class: Sharepoint
      credentials:
        site: "site.sharepoint.com"
        username: "test_username"
        password: "test_password"
```

Then, in our integration tests, we can use the `Sharepoint` source with the `sharepoint_dev` config key:

```python
# tests/integration/test_sharepoint.py

import pytest
...

@pytest.fixture
def sharepoint():
    from viadot.sources import Sharepoint

    return Sharepoint(config_key="sharepoint_dev")
```

!!! info

    For more information on viadot config, see [this page](../user_guide/config_key.md).

### Optional dependencies

Same as with the source, make sure to escape the imports of optional dependencies:

```python
import pytest
...

try:
    from viadot.sources import AzureDataLake

    _adls_installed = True
except ImportError:
    _adls_installed = False

if not _adls_installed:
    pytest.skip("AzureDataLake source not installed", allow_module_level=True)
```

## Conclusion

And that's all you need to know to create your own `viadot` connectors!

If you need inspiration, take a look at some of the [existing sources](https://github.com/dyvenia/viadot/blob/2.0/src/viadot/sources/).
