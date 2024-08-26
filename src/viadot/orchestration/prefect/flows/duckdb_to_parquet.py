"""Flow for extracting data from the DuckDB to a Parquet file."""

from typing import Any, Literal

from prefect import flow

from viadot.orchestration.prefect.tasks import duckdb_query
from viadot.orchestration.prefect.tasks.task_utils import df_to_parquet


@flow(
    name="extract--duckdb--parquet",
    description="Extract data from DuckDB and save it to Parquet file.",
    retries=1,
    retry_delay_seconds=60,
    timeout_seconds=2 * 60 * 60,
)
def duckdb_to_parquet(
    query: str,
    path: str,
    if_exists: Literal["append", "replace", "skip"] = "replace",
    duckdb_credentials_secret: str | None = None,
    # Specifying credentials in a dictionary is not recommended in the viadot flows,
    # but in this case credentials can include only database name.
    duckdb_credentials: dict[str, Any] | None = None,
    duckdb_config_key: str | None = None,
) -> None:
    """Download a table from DuckDB and save it to a Parquet file.

    Args:
        query (str, required): The query to execute on the DuckDB database. If the query
            doesn't start with "SELECT", returns an empty DataFrame.
        path (str): Path where to save a Parquet file which will be created while
            executing flow.
        if_exists (Literal, optional): What to do if the file exists. Defaults to
            "replace".
        duckdb_credentials_secret (str, optional): The name of the secret storing
            the credentials to the DuckDB. Defaults to None.
            More info on: https://docs.prefect.io/concepts/blocks/
        duckdb_credentials (dict[str, Any], optional): Credentials to the DuckDB.
            Defaults to None.
        duckdb_config_key (str, optional): The key in the viadot config holding relevant
            credentials to the DuckDB. Defaults to None.

    """
    df = duckdb_query(
        query=query,
        fetch_type="dataframe",
        credentials=duckdb_credentials,
        config_key=duckdb_config_key,
        credentials_secret=duckdb_credentials_secret,
    )
    return df_to_parquet(
        df=df,
        path=path,
        if_exists=if_exists,
    )
