"""Tasks for interacting with Databricks."""

import contextlib
from typing import Literal

import pandas as pd
from prefect import task


with contextlib.suppress(ImportError):
    from viadot.sources import Databricks

from viadot.orchestration.prefect.exceptions import MissingSourceCredentialsError
from viadot.orchestration.prefect.utils import get_credentials


@task(retries=3, retry_delay_seconds=10, timeout_seconds=60 * 60)
def df_to_databricks(
    df: pd.DataFrame,
    table: str,
    schema: str | None = None,
    if_exists: Literal["replace", "skip", "fail"] = "fail",
    if_empty: Literal["warn", "skip", "fail"] = "warn",
    credentials_secret: str | None = None,
    config_key: str | None = None,
) -> None:
    """Insert a pandas `DataFrame` into a Delta table.

    Args:
        df (pd.DataFrame): A pandas `DataFrame` with the data
            to be inserted into the table.
        table (str): The name of the target table.
        schema (str, optional): The name of the target schema.
        if_exists (str, Optional): What to do if the table already exists.
            One of 'replace', 'skip', and 'fail'.
        if_empty (str, optional): What to do if the input `DataFrame` is empty.
            Defaults to 'warn'.
        credentials_secret (str, optional): The name of the secret storing
            the credentials. Defaults to None.
            More info on: https://docs.prefect.io/concepts/blocks/
        config_key (str, optional): The key in the viadot config holding relevant
            credentials. Defaults to None.

    Example:
        ```python
        from prefect_viadot.tasks df_to_databricks
        from prefect import flow
        import pandas as pd

        @flow
        def insert_df_into_databricks():
            list = [{"id":"1", "name":"Joe"}]
            df = pd.DataFrame(list)
            insert = df_to_databricks(
                df=df,
                schema="prefect_viadot_test"
                table="test",
                if_exists="replace"
            )
            return insert

        insert_df_into_databricks()
        ```
    """
    if not (credentials_secret or config_key):
        raise MissingSourceCredentialsError

    credentials = get_credentials(credentials_secret)
    databricks = Databricks(
        credentials=credentials,
        config_key=config_key,
    )
    if schema and not databricks._check_if_schema_exists(schema):
        databricks.create_schema(schema)
    databricks.create_table_from_pandas(
        df=df, schema=schema, table=table, if_exists=if_exists, if_empty=if_empty
    )
