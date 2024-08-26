"""Flows for downloading data from SQLServer and uploading it to MinIO."""

from typing import Literal

from prefect import flow

from viadot.orchestration.prefect.tasks import df_to_minio, sql_server_to_df


@flow(
    name="extract--sql_server--minio",
    description="Extract data from SQLServer and load it into MinIO.",
    retries=1,
    retry_delay_seconds=60,
)
def sql_server_to_minio(
    query: str,
    path: str,
    if_exists: Literal["error", "delete_matching", "overwrite_or_ignore"] = "error",
    basename_template: str | None = None,
    sql_server_credentials_secret: str | None = None,
    sql_server_config_key: str | None = None,
    minio_credentials_secret: str | None = None,
    minio_config_key: str | None = None,
) -> None:
    """Download a file from SQLServer and upload it to MinIO.

    Args:
        query (str, required): The query to execute on the SQL Server database.
            If the query doesn't start with "SELECT" returns an empty DataFrame.
        path (str): Path to the MinIO file/folder.
        basename_template (str, optional): A template string used to generate
            base names of written data files. The token '{i}' will be replaced with
            an automatically incremented integer. Defaults to None.
        if_exists (Literal["error", "delete_matching", "overwrite_or_ignore"],
            optional). What to do if the dataset already exists. Defaults to "error".
        sql_server_credentials_secret (str, optional): The name of the secret storing
            the credentials to the SQLServer. Defaults to None.
            More info on: https://docs.prefect.io/concepts/blocks/
        sql_server_config_key (str, optional): The key in the viadot config holding
            relevant credentials to the SQLServer. Defaults to None.
        minio_credentials_secret (str, optional): The name of the secret storing
            the credentials to the MinIO. Defaults to None.
            More info on: https://docs.prefect.io/concepts/blocks/
        minio_config_key (str, optional): The key in the viadot config holding relevant
            credentials to the MinIO. Defaults to None.
    """
    df = sql_server_to_df(
        query=query,
        config_key=sql_server_config_key,
        credentials_secret=sql_server_credentials_secret,
    )

    return df_to_minio(
        df=df,
        path=path,
        if_exists=if_exists,
        basename_template=basename_template,
        config_key=minio_config_key,
        credentials_secret=minio_credentials_secret,
    )
