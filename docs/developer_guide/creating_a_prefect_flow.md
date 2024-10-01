# Creating jobs

Let's assume that we've finished our `PostgreSQL` source. We now want to use it to automatically download data from PostgreSQL on a schedule.

!!! note

    Job scheduling is left outside of the scope of this guide - we only focus on creating the job itself.

We create our ingestion job by utilizing [Prefect](https://www.prefect.io/) as our orchestration tool. We will create a Prefect flow, `postgresql_to_adls`. This flow will utilize our new connector to download a PostgreSQL table into a pandas `DataFrame`, and then upload the data to Azure Data Lake. The flow will consist of two tasks:

- `postgresql_to_df` - downloads a PostgreSQL table into a pandas `DataFrame`
- `df_to_adls` - uploads a pandas `DataFrame` to Azure Data Lake

## Creating a task

Below is an example task:

```python
# orchestration/prefect/tasks/postgresql.py

from viadot.sources import PostgreSQL
from prefect import task

from prefect_viadot.exceptions import MissingSourceCredentialsError
from prefect_viadot.utils import get_credentials

import pandas as pd


@task(retries=3, retry_delay_seconds=10, timeout_seconds=60 * 60)
def postgresql_to_df(config_key: str | None = None, credentials_secret: str | None = None, ...) -> pd.DataFrame:
    if not (credentials_secret or config_key):
        raise MissingSourceCredentialsError

    credentials = get_credentials(credentials_secret) if credentials_secret else None

    postgres = PostgreSQL(credentials=credentials, config_key=config_key)
    return postgres.to_df(...)
```

!!! info "Best practices"

    1. The task should be a thin wrapper over a `viadot` function or method (ie. it shouldn't contain any logic but simply use relevant `viadot` functions and classes).
    2. The task **MUST NOT** allow specifying the credentials directly. Credentials for the source must be passed via config key and/or secret. This is because Prefect stores the values of task parameters and sometimes even logs them in the UI, which means that passing the credentials directly creates a security risk.
    3. Validate the credentials and raise `MissingSourceCredentialsError` if needed.
    4. Utilize retries and timeouts to make the task and the entire system more robust.

When you are done with the task, remember to import it in `tasks/__init__.py`, so that it can be imported with `from viadot.orchestration.prefect.tasks import postgresql_to_df` (instead of `from viadot.orchestration.prefect.tasks.postgresql_to_df import postgresql_to_df`):

```python
# orchestration/prefect/tasks/__init__.py

from .postgresql import postgresql_to_df  # noqa: F401
```

!!! note "Tests"

    Note that since the orchestration layer is only a thin wrapper around `viadot` sources, we don't require unit or integration tests for tasks or flows. Instead, add all your unit and integration tests at the source connector level.

## Tasks using optional dependencies

Similar to sources, in case your task uses an optional dependency, it has to be escaped:

```python
# orchestration/prefect/tasks/adls.py

"""Tasks for interacting with Azure Data Lake (gen2)."""

import contextlib
...

with contextlib.suppress(ImportError):
    from viadot.sources import AzureDataLake

...
```

In case you're adding task/flow tests, remember to also escape the imports with `viadot.utils.skip_test_on_missing_extra()`!

## Creating a Prefect flow

Once the tasks are ready, the last step of our development is to define the flow:

```python
# orchestration/prefect/flows/postgresql_to_adls.py

from viadot.orchestration.prefect.tasks import df_to_adls, postgresql_to_df

from prefect import flow

@flow(
    name="extract--postgresql--adls",
    description="Extract data from PostgreSQL database and load it into Azure Data Lake.",
    retries=1,
    retry_delay_seconds=60,
    timeout_seconds=60*60,
)
def postgresql_to_adls(
    adls_path: str,
    adls_credentials_secret: str | None,
    adls_config_key: str | None,
    overwrite: bool = False,
    postgresql_config_key: str | None,
    postgresql_credentials_secret: str | None,
    sql_query: str | None,
    ) -> None:

    df = postgresql_to_df(
        credentials_secret=postgresql_credentials_secret,
        config_key=postgresql_config_key,
        sql_query=sql_query,
    )
    return df_to_adls(
        df=df,
        path=adls_path,
        credentials_secret=adls_credentials_secret,
        config_key=adls_config_key,
        overwrite=overwrite,
    )

```

!!! info "Best practices"

    1. The flow should be a thin wrapper over the tasks, and should contain minimal logic. If your flow is getting too complex, it means that you're probably working around the limitations for `viadot`. Instead of adding workarounds in the flow, simply add the missing functionality to the connector you're using. This will make the functionality easier to test. It will also make it reusable across different orchestrators (eg. Airflow).
    2. Utilize retries and timeouts to make the flow and the entire system more robust*.

    *if you do use retries, make sure the flow is [idempotent](https://airbyte.com/data-engineering-resources/idempotency-in-data-pipelines)

When you are done with the flow, remember to import it in the init file, so that it can be imported with `from viadot.orchestration.prefect.flows import postgresql_to_adls` (instead of `from viadot.orchestration.prefect.flows.postgresql_to_adls import postgresql_to_adls`):

```python
# orchestration/prefect/flows/__init__.py

...
from .postgresql_to_adls import postgresql_to_adls

__all__ = [
    ...,
    "postgresql_to_adls"
]
```

## Adding docs

To allow MkDocs to autogenerate and display documentation for your tasks and flows in [reference docs](../references/orchestration/prefect/tasks.md), add relevant entries in the reference docs (`docs/references/orchestration/prefect`). For example:

Task:

```markdown
# docs/references/orchestration/prefect/tasks.md

...

::: viadot.orchestration.prefect.tasks.postgresql_to_df
```

Flow:

```markdown
# docs/references/orchestration/prefect/flows.md

...

::: viadot.orchestration.prefect.flows.postgresql_to_adls
```
