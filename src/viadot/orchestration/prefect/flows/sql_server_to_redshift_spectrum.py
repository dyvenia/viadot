"""Flows for downloading data from SQLServer and uploading it to Redshift Spectrum."""

from typing import Literal

from prefect import flow

from viadot.orchestration.prefect.tasks import df_to_redshift_spectrum, sql_server_to_df


@flow(
    name="extract--sql_server--redshift_spectrum",
    description="Extract data from SQLServer and load it into AWS Redshift Spectrum.",
    retries=1,
    retry_delay_seconds=60,
)
def sql_server_to_redshift_spectrum(  # noqa: PLR0913
    query: str,
    to_path: str,
    schema_name: str,
    table: str,
    sep: str = ",",
    extension: str = ".parquet",
    if_exists: Literal["overwrite", "append"] = "overwrite",
    partition_cols: list[str] | None = None,
    index: bool = False,
    compression: str | None = None,
    aws_config_key: str | None = None,
    credentials_secret: str | None = None,
    sql_server_credentials_secret: str | None = None,
    sql_server_config_key: str | None = None,
) -> None:
    """Flow to load data from a SQL Server database to Redshift Spectrum.

    Args:
        query (str): The SQL query to execute on the SQL Server database.
        to_path (str): The path where the extracted data will be stored.
        schema_name (str): The name of the schema in Redshift Spectrum.
        table (str): The name of the table in Redshift Spectrum.
        sep (str, optional): The separator used in the output file. Defaults to ",".
        extension (str, optional): The file extension to use. Defaults to ".parquet".
        if_exists (Literal["overwrite", "append"], optional): Action to take if
            the table already exists. Defaults to "overwrite".
        partition_cols (list[str] | None, optional): Columns to partition the data by.
            Defaults to None.
        index (bool, optional): Whether to include the index in the output file.
            Defaults to False.
        compression (str | None, optional): Compression type to use for the output file.
            Defaults to None.
        aws_config_key (str | None, optional): AWS configuration key. Defaults to None.
        credentials_secret (str | None, optional): Name of the secret storing
            AWS credentials. Defaults to None.
        sql_server_credentials_secret (str | None, optional): Name of the secret storing
            SQL Server credentials. Defaults to None.
        sql_server_config_key (str | None, optional): Key in the configuration for
            SQL Server credentials. Defaults to None.

    Returns:
        None
    """
    df = sql_server_to_df(
        query=query,
        config_key=sql_server_config_key,
        credentials_secret=sql_server_credentials_secret,
    )

    df_to_redshift_spectrum(
        df=df,
        to_path=to_path,
        schema_name=schema_name,
        table=table,
        extension=extension,
        if_exists=if_exists,
        partition_cols=partition_cols,
        index=index,
        compression=compression,
        sep=sep,
        config_key=aws_config_key,
        credentials_secret=credentials_secret,
    )
