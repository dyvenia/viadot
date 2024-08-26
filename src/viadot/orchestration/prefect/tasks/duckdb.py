"""Tasks for running query in DuckDB."""

from typing import Any, Literal

from prefect import task
from prefect.logging import get_run_logger

from viadot.config import get_source_credentials
from viadot.orchestration.prefect.exceptions import MissingSourceCredentialsError
from viadot.orchestration.prefect.utils import get_credentials
from viadot.sources import DuckDB
from viadot.sources.base import Record


@task(retries=3, retry_delay_seconds=10, timeout_seconds=60 * 60)
def duckdb_query(
    query: str,
    fetch_type: Literal["record", "dataframe"] = "record",
    # Specifying credentials in a dictionary is not recommended in viadot tasks,
    # but in this case credentials can include only database name.
    credentials: dict[str, Any] | None = None,
    credentials_secret: str | None = None,
    config_key: str | None = None,
) -> list[Record] | bool:
    """Run query on a DuckDB database.

    Args:
        query (str, required): The query to execute on the DuckDB database.
        fetch_type (str, optional): In which form the data should be returned.
            Defaults to "record".
        credentials (dict[str, Any], optional): Credentials to the Database. Defaults to
            None.
        credentials_secret (str, optional): The name of the secret storing credentials
            to the database. Defaults to None. More info on:
            https://docs.prefect.io/concepts/blocks/
        config_key (str, optional): The key in the viadot config holding relevant
            credentials to the database. Defaults to None.
    """
    if not (credentials or credentials_secret or config_key):
        raise MissingSourceCredentialsError

    logger = get_run_logger()

    credentials = (
        credentials
        or get_source_credentials(config_key)
        or get_credentials(credentials_secret)
    )
    duckdb = DuckDB(credentials=credentials)
    result = duckdb.run_query(query=query, fetch_type=fetch_type)
    logger.info("Query has been executed successfully.")
    return result
