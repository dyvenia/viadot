"""Flow for extracting data from the DuckDB into SQLServer."""

from typing import Literal, Any

from viadot.orchestration.prefect.tasks import (
    duckdb_query,
    create_sql_server_table,
    bcp,
)
from viadot.orchestration.prefect.tasks.task_utils import df_to_csv, df_get_data_types_task

from prefect import flow


@flow(
    name="extract--duckdb--sql_server",
    description="Extract data from DuckDB and save it in the SQLServer",
    retries=1,
    retry_delay_seconds=60,
)
def duckdb_to_sql_server(  # noqa: PLR0913, PLR0917
    query: str,
    local_path: str,
    db_table: str,
    db_schema: str,
    if_exists: Literal["fail", "replace", "skip", "delete"] = "replace",
    dtypes: dict[str, Any] = None,
    chunksize: int = 5000,
    error_log_file_path: str = "./log_file.log",
    on_error: Literal["skip", "fail"] = "skip",
    duckdb_credentials_secret: str | None = None,
    # Specifing credentials in a dictionary is not recomended in the viadot flows, 
    # but in this case credantials can include only database name.
    duckdb_credentials: dict[str, Any] | None = None,
    duckdb_config_key: str | None = None,
    sql_server_credentials_secret: str | None = None,
    sql_server_config_key: str | None = None,
) -> None:
    """Download a table from DuckDB and upload it to the SQLServer.

    Args:
        query (str, required): The query to execute on the SQL Server database.
            If the qery doesn't start with "SELECT" returns an empty DataFrame.
        local_path (str): Path where to save a Parquet file which will be created while
            executing flow.
        db_table (str, optional): Destination table. Defaults to None.
        db_schema (str, optional): Destination schema. Defaults to None.
        if_exists (Literal, optional): What to do if the table exists. Defaults to "replace".
        dtypes (dict, optional): The data types to be enforced for the resulting table. 
            By default, infered from the DataFrame. Defaults to None.
        chunksize (int, optional): Size of a chunck to use in the bcp function. 
            Defaults to 5000.
        error_log_file_path (string, optional): Full path of an error file. Defaults
            to "./log_file.log".
        on_error (str, optional): What to do in case of a bcp error. Defaults to "skip".
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
    if dtypes is None:
        dtypes = df_get_data_types_task(df)

    table = create_sql_server_table(
        table=db_table,
        schema=db_schema,
        dtypes=dtypes,
        if_exists=if_exists,
        credentials_secret=sql_server_credentials_secret,
        config_key=sql_server_config_key,
    )
    csv = df_to_csv(df=df, path=local_path)

    return bcp(
        path=local_path,
        schema=db_schema,
        table=db_table,
        chunksize=chunksize,
        error_log_file_path=error_log_file_path,
        on_error=on_error,
    )
