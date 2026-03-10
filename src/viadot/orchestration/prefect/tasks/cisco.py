"""Tasks for interacting with Cisco Informix via JDBC."""

from typing import Any

import pandas as pd
from prefect import task
from prefect.logging import get_run_logger

from viadot.config import get_source_credentials
from viadot.orchestration.prefect.exceptions import MissingSourceCredentialsError
from viadot.orchestration.prefect.utils import get_credentials
from viadot.sources.base import Record
from viadot.sources.cisco import Cisco


@task(retries=3, retry_delay_seconds=10, timeout_seconds=60 * 60 * 3)
def cisco_to_df(
    query: str,
    credentials_secret: str | None = None,
    config_key: str | None = None,
    tests: dict[str, Any] | None = None,
) -> pd.DataFrame:
    """Load the result of a Cisco Informix query into a pandas DataFrame.

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

    cisco = Cisco(credentials=credentials)
    df = cisco.to_df(query=query, tests=tests)
    nrows = df.shape[0]
    ncols = df.shape[1]

    logger.info(
        f"Successfully downloaded {nrows} rows and {ncols} columns of data to a DataFrame."
    )
    return df


@task(retries=3, retry_delay_seconds=10, timeout_seconds=60 * 60 * 3)
def cisco_query(
    query: str,
    credentials_secret: str | None = None,
    config_key: str | None = None,
) -> list[Record] | bool:
    """Execute a query on Cisco Informix.

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
    cisco = Cisco(credentials=credentials)
    result = cisco.run(query)

    logger.info("Successfully ran the query.")
    return result
