
Let's assume that we've finished adding our source `PostgreSQL`. We want to use it in our flow `postgresql_to_adls`. This flow will take a table from the PostgreSQL database using our previously defined source and upload it to the Azure Data Lake.
We will have to create a task that will take our data from the PostgreSQL database and put it into flow, where the task sending data to the Azure Data Lake already exists.


## Adding prefect task for general source

We store tasks associated with a single source in a single file named like source. All tasks should be defined in the path `viadot/src/viadot/orchestration/prefect/tasks/`.
So in our example, we create a file `postgresql.py` in the previously specified directory, and inside this file we define the logic.

```python
#tasks/postgresql.py

from viadot.sources import PostgreSQL
from prefect import task

from prefect_viadot.exceptions import MissingSourceCredentialsError
from prefect_viadot.utils import get_credentials

@task(retries=3, retry_delay_seconds=10, timeout_seconds=60 * 60)
def postgresql_to_df(credentials_key: str | None = None, credentials_secret: str | None = None, ...):
    if not (credentials_secret or config_key):
        raise MissingSourceCredentialsError
        
    credentials = credentials or get_credentials(credentials_secret)
    postgres = PostgreSQL(credentials=credentials, config_key=config_key)
    return postgres.to_df(...)
    
As you can see, there are a few standards when it comes to implementing a task:
- it should typically be a thin wrapper over a `viadot` function or method (ie. it doesn't contain any logic, only calls the `viadot` funcion)
- if the source requires credentials, we should allow specifying them via a config key or a secret store (`config_key` and `credentials_secret` params)
- we should validate the credentials and raise MissingSourceCredentialsError if they're not specified

When you are done with the task, remember to import it in the `__init__.py` file.

```python
# tasks/__init__.py

from .postgresql import postgresql_to_df  # noqa: F401
```

At the end, add integration tests for the specified task in `viadot/tests/orchestration/prefect_tests/integration/`. Your PR will be accepted only if the test coverage is greater than or equal to 80%.


## Adding prefect task for optional source

In the previous example, we added a task that is for general sources. Viadot also contains cloud-specific sources, which are optional, and we have to handle that case when creating prefect tasks for it. This is an example of how to handle the import of an optional source.

```python
#tasks/adls.py

"""Tasks for interacting with Azure Data Lake (gen2)."""

import contextlib
from typing import Any

import pandas as pd
from viadot.orchestration.prefect.exceptions import MissingSourceCredentialsError
from viadot.orchestration.prefect.utils import get_credentials

from prefect import task

with contextlib.suppress(ImportError):
    from viadot.sources import AzureDataLake

```

When adding a new integration test for our task, we have to also handle import and skip test if package is not installed in our environment.

```python
# prefect_tests/integration/test_adls.py

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

## Add prefect flow

The last step of our development is to define the completed flow. We store flow in the `viadot/src/viadot/orchestration/prefect/flows/` directory, where each Python file is one flow template.
The name of the file should explain from where to where we send data, so in our example, `postgresql_to_adls.py`, which means we're sending data from the PostgreSQL database to Azure Data Lake.

```python
from viadot.orchestration.prefect.tasks import df_to_adls, postgresql_to_df

from prefect import flow

@flow(
    name="extract--postgresql--adls",
    description="Extract data from PostgreSQL database and load it into Azure Data Lake.",
    retries=1,
    retry_delay_seconds=60,
)
def postgresql_to_adls()
    # Logic of our flow.
```

At the end, add tests for the specified flow in `viadot/tests/integration/orchestration/flows/test_<your_flow_name>.py`.