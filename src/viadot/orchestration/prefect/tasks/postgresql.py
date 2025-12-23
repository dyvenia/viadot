"""Tasks for interacting with PostgreSQL."""

from typing import Any, Literal

import pandas as pd
from prefect import task
from prefect.logging import get_run_logger

from viadot.config import get_source_credentials
from viadot.orchestration.prefect.exceptions import MissingSourceCredentialsError
from viadot.orchestration.prefect.utils import get_credentials
from viadot.sources.base import Record
from viadot.sources.postgres import PostgreSQL


@task(retries=3, retry_delay_seconds=10, timeout_seconds=60 * 60 * 3)
def create_postgresql_table(
    schema: str,
    table: str,
    if_exists: Literal["fail", "replace", "skip", "delete"] = "fail",
    dtypes: dict[str, Any] | None = None,
    credentials_secret: str | None = None,
    config_key: str | None = None,
) -> None:
    """A task for creating a table in PostgreSQL.

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
    postgresql = PostgreSQL(
        credentials=credentials,
    )

    fqn = f"{schema}.{table}" if schema is not None else table
    created = postgresql.create_table(
        schema=schema, table=table, dtypes=dtypes, if_exists=if_exists
    )
    if created:
        logger.info(f"Successfully created table {fqn}.")
    else:
        logger.info(
            f"Table {fqn} has not been created as if_exists is set to {if_exists}."
        )


@task(retries=3, retry_delay_seconds=10, timeout_seconds=60 * 60 * 3)
def postgresql_to_df(
    query: str,
    credentials_secret: str | None = None,
    postgres_host: str = "localhost",
    postgres_port: int = 5432,
    postgres_db_name: str = "postgres",
    postgres_sslmode: str = "require",
    config_key: str | None = None,
    tests: dict[str, Any] | None = None,
) -> pd.DataFrame:
    """Load the result of a PostgreSQL Database query into a pandas DataFrame.

    Args:
        query (str, required): The query to execute on the PostgreSQL database.
            If the query doesn't start with "SELECT" returns an empty DataFrame.
        credentials_secret (str, optional): The name of the secret storing
            the credentials. Defaults to None.
            More info on: https://docs.prefect.io/concepts/blocks/
        postgres_host (str, optional): The host of the PostgreSQL database.
            Defaults to "localhost".
        postgres_port (int, optional): The port of the PostgreSQL database.
            Defaults to 5432.
        postgres_db_name (str, optional): The name of the PostgreSQL database.
            Defaults to "postgres".
        postgres_sslmode (str, optional): The SSL mode to use for the
            PostgreSQL database. Defaults to "require".
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
    postgresql = PostgreSQL(
        credentials=credentials,
        postgres_host=postgres_host,
        postgres_port=postgres_port,
        postgres_db_name=postgres_db_name,
        postgres_sslmode=postgres_sslmode,
    )

    df = postgresql.to_df(query=query, tests=tests)
    nrows = df.shape[0]
    ncols = df.shape[1]

    logger.info(
        f"Successfully downloaded {nrows} rows and {ncols} columns of data to a DataFrame."
    )
    return df


@task(retries=3, retry_delay_seconds=10, timeout_seconds=60 * 60 * 3)
def postgresql_query(
    query: str,
    credentials_secret: str | None = None,
    postgres_host: str = "localhost",
    postgres_port: int = 5432,
    postgres_db_name: str = "postgres",
    postgres_sslmode: str = "require",
    config_key: str | None = None,
) -> list[Record] | bool:
    """Execute a query on PostgreSQL.

    Args:
        query (str, required): The query to execute on the PostgreSQL database.
        credentials_secret (str, optional): The name of the secret storing
            the credentials. Defaults to None.
            More info on: https://docs.prefect.io/concepts/blocks/
        postgres_host (str, optional): The host of the PostgreSQL database.
            Defaults to "localhost".
        postgres_port (int, optional): The port of the PostgreSQL database.
            Defaults to 5432.
        postgres_db_name (str, optional): The name of the PostgreSQL database.
            Defaults to "postgres".
        postgres_sslmode (str, optional): The SSL mode to use for the
            PostgreSQL database. Defaults to "require".
        config_key (str, optional): The key in the viadot config holding relevant
            credentials. Defaults to None.
    """
    if not (credentials_secret or config_key):
        raise MissingSourceCredentialsError

    logger = get_run_logger()

    credentials = get_source_credentials(config_key) or get_credentials(
        credentials_secret
    )
    postgresql = PostgreSQL(
        credentials=credentials,
        postgres_host=postgres_host,
        postgres_port=postgres_port,
        postgres_db_name=postgres_db_name,
        postgres_sslmode=postgres_sslmode,
    )
    result = postgresql.run(query)

    logger.info("Successfully ran the query.")
    return result
