"""Flow for downloading data from SQLServer and saveing it to a Parquet file."""

from typing import Literal

from prefect import flow

from viadot.orchestration.prefect.tasks import sql_server_to_df
from viadot.orchestration.prefect.tasks.task_utils import df_to_parquet


@flow(
    name="extract--sql_server--parquet",
    description="Extract data from SQLServer and save it to a Parquet file.",
    retries=1,
    retry_delay_seconds=60,
)
def sql_server_to_parquet(
    query: str,
    path: str,
    if_exists: Literal["append", "replace", "skip"] = "replace",
    sql_server_credentials_secret: str | None = None,
    sql_server_config_key: str | None = None,
) -> None:
    """Download a file from SQLServer and save it to a Parquet file.

    Args:
        query (str, required): The query to execute on the SQL Server database.
            If the qery doesn't start with "SELECT" returns an empty DataFrame.
        path (str): Path where to save a Parquet file which will be created while
            executing flow.
        if_exists (Literal, optional): What to do if Parquet exists.
            Defaults to "replace".
        sql_server_credentials_secret (str, optional): The name of the secret storing
            the credentialsto the SQLServer. Defaults to None.
            More info on: https://docs.prefect.io/concepts/blocks/
        sql_server_config_key (str, optional): The key in the viadot config
            holding relevant credentials to the SQLServer. Defaults to None.
    """
    df = sql_server_to_df(
        query=query,
        config_key=sql_server_config_key,
        credentials_secret=sql_server_credentials_secret,
    )

    return df_to_parquet(
        df=df,
        path=path,
        if_exists=if_exists,
    )
