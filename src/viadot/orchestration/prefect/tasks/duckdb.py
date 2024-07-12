"""Tasks for running query in DuckDB."""

from typing import Any, Literal

from prefect import task
from prefect.logging import get_run_logger


from viadot.sources import DuckDB
from viadot.orchestration.prefect.exceptions import MissingSourceCredentialsError
from viadot.orchestration.prefect.utils import get_credentials
from viadot.config import get_source_credentials


@task(retries=3, retry_delay_seconds=10, timeout_seconds=60 * 60)
def duckdb_query(
    query: str,
    fetch_type: Literal["record", "dataframe"] = "record",
    credentials_secret: str | None = None,
    config_key: str | None = None,
    credentials: dict[str, Any] | None = None,
):
    """Run query on a DuckDB database.

    Args:
        query (str, required): The query to execute on the DuckDB database.
        fetch_type (str, optional): In which for the data should be returned.
            Defaults to "record".
        credentials_secret (str, optional): The name of the secret storing the credentials
            to the DuckDB. Defaults to None.
            More info on: https://docs.prefect.io/concepts/blocks/
        credentials (dict[str, Any], optional): Credentials to the DuckDB. Defaults to None.
        config_key (str, optional): The key in the viadot config holding relevant credentials
            to the DuckDB. Defaults to None.

    """

    if not (credentials_secret or credentials or config_key):
        raise MissingSourceCredentialsError

    logger = get_run_logger()

    credentials = (
        credentials
        or get_source_credentials(config_key)
        or get_credentials(credentials_secret)
    )
    duckdb = DuckDB(credentials=credentials)
    result = duckdb.run_query(query=query, fetch_type=fetch_type)
    logger.info("Query has been ru successfully.")
    return result
