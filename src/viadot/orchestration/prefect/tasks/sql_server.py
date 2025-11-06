"""Tasks for interacting with SQLServer."""

from typing import Any, Literal

import pandas as pd
from prefect import task
from prefect.logging import get_run_logger

from viadot.config import get_source_credentials
from viadot.orchestration.prefect.exceptions import MissingSourceCredentialsError
from viadot.orchestration.prefect.utils import get_credentials
from viadot.sources.base import Record
from viadot.sources.sql_server import SQLServer


@task(retries=3, retry_delay_seconds=10, timeout_seconds=60 * 60 * 3)
def create_sql_server_table(
    schema: str,
    table: str,
    if_exists: Literal["fail", "replace", "skip", "delete"] = "fail",
    dtypes: dict[str, Any] | None = None,
    credentials_secret: str | None = None,
    config_key: str | None = None,
) -> None:
    """A task for creating table in SQL Server.

    Args:
        schema (str): Destination schema.
        table (str): Destination table.
        if_exists (Literal, optional): What to do if the table already exists.
        dtypes (dict[str, Any], optional): Data types to enforce.
        credentials_secret (str, optional): The name of the secret storing
            the credentials. Defaults to None.
            More info on: https://docs.prefect.io/concepts/blocks/
        config_key (str, optional): The key in the viadot config holding relevant
            credentials. Defaults to None.
    """
    if not (credentials_secret or config_key):
        raise MissingSourceCredentialsError

    logger = get_run_logger()

    credentials = get_credentials(credentials_secret) or get_source_credentials(
        config_key
    )
    sql_server = SQLServer(credentials=credentials)

    fqn = f"{schema}.{table}" if schema is not None else table
    created = sql_server.create_table(
        schema=schema, table=table, dtypes=dtypes, if_exists=if_exists
    )
    if created:
        logger.info(f"Successfully created table {fqn}.")
    else:
        logger.info(
            f"Table {fqn} has not been created as if_exists is set to {if_exists}."
        )


@task(retries=3, retry_delay_seconds=10, timeout_seconds=60 * 60 * 3)
def sql_server_to_df(
    query: str,
    credentials_secret: str | None = None,
    config_key: str | None = None,
    tests: dict[str, Any] | None = None,
) -> pd.DataFrame:
    """Load the result of a SQL Server Database query into a pandas DataFrame.

    Args:
        query (str, required): The query to execute on the SQL Server database.
            If the query doesn't start with "SELECT" returns an empty DataFrame.
        credentials_secret (str, optional): The name of the secret storing
            the credentials. Defaults to None.
            More info on: https://docs.prefect.io/concepts/blocks/
        config_key (str, optional): The key in the viadot config holding relevant
            credentials. Defaults to None.

    Returns:
        pd.Dataframe: The resulting data as a pandas DataFrame.
    """
    if not (credentials_secret or config_key):
        raise MissingSourceCredentialsError

    logger = get_run_logger()

    credentials = get_source_credentials(config_key) or get_credentials(
        credentials_secret
    )
    sql_server = SQLServer(credentials=credentials)
    df = sql_server.to_df(query=query, tests=tests)
    nrows = df.shape[0]
    ncols = df.shape[1]

    logger.info(
        f"Successfully downloaded {nrows} rows and {ncols} columns of data to a DataFrame."
    )
    return df


@task(retries=3, retry_delay_seconds=10, timeout_seconds=60 * 60 * 3)
def sql_server_query(
    query: str,
    credentials_secret: str | None = None,
    config_key: str | None = None,
) -> list[Record] | bool:
    """Execute a query on SQL Server.

    Args:
        query (str, required): The query to execute on the SQL Server database.
        credentials_secret (str, optional): The name of the secret storing
            the credentials. Defaults to None.
            More info on: https://docs.prefect.io/concepts/blocks/
        config_key (str, optional): The key in the viadot config holding relevant
            credentials. Defaults to None.
    """
    if not (credentials_secret or config_key):
        raise MissingSourceCredentialsError

    logger = get_run_logger()

    credentials = get_source_credentials(config_key) or get_credentials(
        credentials_secret
    )
    sql_server = SQLServer(credentials=credentials)
    result = sql_server.run(query)

    logger.info("Successfully ran the query.")
    return result
