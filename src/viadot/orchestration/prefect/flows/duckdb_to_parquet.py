"""Flow for extracting data from the DuckDB to a Parquet file."""

from typing import Any, Literal

from viadot.orchestration.prefect.tasks import duckdb_query
from viadot.orchestration.prefect.tasks.task_utils import df_to_parquet

from prefect import flow


@flow(
    name="extract--duckdb--parquet",
    description="Extract data from DuckDB and save it to Parquet file.",
    retries=1,
    retry_delay_seconds=60,
)
def duckdb_to_parquet(  # noqa: PLR0913, PLR0917
    query: str,
    path: str,
    if_exists: Literal["append", "replace", "skip"] = "replace",
    duckdb_credentials_secret: str | None = None,
    # Specifing credentials in a dictionary is not recomended in the viadot flows,
    # but in this case credantials can include only database name.
    duckdb_credentials: dict[str, Any] | None = None,
    duckdb_config_key: str | None = None,
) -> None:
    """Download a table from DuckDB and save it to Parquet file.

    Args:
        query (str, required): The query to execute on the SQL Server database.
            If the qery doesn't start with "SELECT" returns an empty DataFrame.
        path (str): Path where to save a Parquet file which will be created while
            executing flow.
        if_exists (Literal, optional): What to do if Parquet exists. Defaults to "replace".
        duckdb_credentials_secret (str, optional): The name of the secret storing
            the credentials to the DuckDB. Defaults to None.
            More info on: https://docs.prefect.io/concepts/blocks/
        duckdb_credentials (dict[str, Any], optional): Credentials to the DuckDB.
            Defaults to None.
        duckdb_config_key (str, optional): The key in the viadot config holding relevant
            credentials to the DuckDB. Defaults to None.
        sql_server_credentials_secret (str, optional): The name of the secret storing
            the credentialsto the SQLServer. Defaults to None.
            More info on: https://docs.prefect.io/concepts/blocks/
        sql_server_config_key (str, optional): The key in the viadot config holding relevant
            credentials to the SQLServer. Defaults to None.

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
