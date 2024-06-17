
Let's assume that we've finished adding our source `PostgresSQL`. We want to use it in our flow `postgressql_to_adls`. This flow will take a table from the PostgreSQL database using our previously defined source and upload it to the Azure Data Lake.
We will have to create a task that will take our data from the PostgreSQL database and put it into flow, where the task sending data to the Azure Data Lake already exists.


## Add prefect task

We store tasks associated with a single source in a single file named like source. All tasks should be defined in the path `viadot/src/viadot/orchestration/prefect/tasks/`.
So in our example, we create a file `postgresql.py` in the previously specified directory, and inside this file we define the logic.

```python
#tasks/postgresql.py

from viadot.sources import PostgreSQL
from prefect import task

@task(retries=3, retry_delay_seconds=10, timeout_seconds=60 * 60)
def postgresql_to_df():
    # Logic of our task. 
```

When you are done with the task, remember to import it in the `__init__.py` file.

```python
# tasks/__init__.py

from .postgresql import postgresql_to_df  # noqa: F401
```

At the end, add integration tests for the specified task in `viadot/tests/orchestration/prefect_tests/integration/`. Your PR will be accepted only if the test coverage is greater than or equal to 80%.


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

At the end, add tests for the specified flow in `viadot/tests/orchestration/prefect_tests/flows`. Your PR will be accepted only if the test coverage is greater than or equal to 80%.