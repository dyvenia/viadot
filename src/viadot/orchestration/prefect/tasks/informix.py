"""Tasks for interacting with Informix Informix via JDBC."""

import contextlib
from typing import Any

import pandas as pd
from prefect import task
from prefect.logging import get_run_logger

from viadot.config import get_source_credentials
from viadot.orchestration.prefect.exceptions import MissingSourceCredentialsError
from viadot.orchestration.prefect.utils import get_credentials
from viadot.sources.base import Record


with contextlib.suppress(ImportError):
    from viadot.sources.informix import Informix


@task(retries=3, retry_delay_seconds=10, timeout_seconds=60 * 60 * 3)
def informix_to_df(
    query: str,
    credentials_secret: str | None = None,
    config_key: str | None = None,
    tests: dict[str, Any] | None = None,
) -> pd.DataFrame:
    """Load the result of a Informix Informix query into a pandas DataFrame.

    Args:
        query (str): The query to execute.
        credentials_secret (str): The credentials secret.
        config_key (str): The configuration key.
        tests (dict[str, Any]): The tests to run on the data.

    Returns:
        pd.DataFrame: The result of the query.

    Raises:
        MissingSourceCredentialsError: If the credentials secret
            or configuration key is not provided.
    """
    if not (credentials_secret or config_key):
        raise MissingSourceCredentialsError

    logger = get_run_logger()

    credentials = get_source_credentials(config_key) or get_credentials(
        credentials_secret
    )

    informix = Informix(credentials=credentials)
    df = informix.to_df(query=query, tests=tests)
    nrows = df.shape[0]
    ncols = df.shape[1]

    logger.info(
        f"Successfully downloaded {nrows} rows and {ncols} columns of data to a DataFrame."
    )
    return df


@task(retries=3, retry_delay_seconds=10, timeout_seconds=60 * 60 * 3)
def informix_query(
    query: str,
    credentials_secret: str | None = None,
    config_key: str | None = None,
) -> list[Record] | bool:
    """Execute a query on Informix Informix.

    Args:
        query (str): The query to execute.
        credentials_secret (str): The credentials secret.
        config_key (str): The configuration key.

    Returns:
        list[Record] | bool: The result of the query.
    """
    if not (credentials_secret or config_key):
        raise MissingSourceCredentialsError

    logger = get_run_logger()

    credentials = get_source_credentials(config_key) or get_credentials(
        credentials_secret
    )
    informix = Informix(credentials=credentials)
    result = informix.run(query)

    logger.info("Successfully ran the query.")
    return result
