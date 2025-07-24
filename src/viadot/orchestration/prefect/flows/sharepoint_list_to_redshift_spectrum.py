"""Flows for loading data from Sharepoint to Redshift Spectrum."""

from typing import Any, Literal

from prefect import flow

from viadot.orchestration.prefect.tasks import (
    df_to_redshift_spectrum,
    sharepoint_list_to_df,
)


@flow(
    name="extract--sharepoint--list--redshift_spectrum",
    description="Extract data from Sharepoint Lists and load it into AWS Redshift Spectrum.",
    retries=1,
    retry_delay_seconds=60,
)
def sharepoint_list_to_redshift_spectrum(  # noqa: PLR0913
    to_path: str,
    schema_name: str,
    table: str,
    list_site: str,
    list_name: str,
    sep: str = ",",
    extension: str = ".parquet",
    if_exists: Literal["overwrite", "append"] = "overwrite",
    default_protocol: str | None = "https://",
    partition_cols: list[str] | None = None,
    index: bool = False,
    compression: str | None = None,
    aws_config_key: str | None = None,
    credentials_secret: str | None = None,
    sharepoint_credentials_secret: str | None = None,
    sharepoint_config_key: str | None = None,
    query: str | None = None,
    select: list[str] | None = None,
    tests: dict[str, Any] | None = None,
) -> None:
    """Flow to load data from a SharePoint list to Redshift Spectrum.

    Args:
        to_path (str): The path where the data will be stored.
        schema_name (str): The name of the schema in Redshift Spectrum.
        table (str): The name of the table in Redshift Spectrum.
        list_site (str):  The Sharepoint site on which the list is stored.
        list_name (str): The name of the SharePoint list.
        sep (str, optional): The separator used in the file. Defaults to ",".
        extension (str, optional): The file extension to use. Defaults to ".parquet".
        default_protocol (str, optional): The default protocol to use for
                SharePoint URLs.Defaults to "https://".
        if_exists (Literal["overwrite", "append"], optional): Action if the table
            exists.Defaults to "overwrite".
        partition_cols (list[str] | None, optional): Columns to partition the data by.
            Defaults to None.
        index (bool, optional): Whether to include the index in the output.
            Defaults to False.
        compression (str | None, optional): Compression type to use. Defaults to None.
        aws_config_key (str | None, optional): AWS configuration key. Defaults to None.
        credentials_secret (str | None, optional): Name of the secret storing
            AWS credentials. Defaults to None.
        sharepoint_credentials_secret (str | None, optional): Name of the secret
            storing SharePoint credentials. Defaults to None.
        sharepoint_config_key (str | None, optional): Key in the config for
            SharePoint  credentials. Defaults to None.
        query (str | None, optional): Query to filter items in the SharePoint list.
            Defaults to None.
        select (list[str] | None, optional): Fields to include from the SharePoint list.
            Defaults to None.
        tests (dict[str, Any] | None, optional): Tests to validate the DataFrame.
            Defaults to None.

    Returns:
        None
    """
    # Tasks
    df = sharepoint_list_to_df(
        list_site=list_site,
        list_name=list_name,
        default_protocol=default_protocol,
        query=query,
        select=select,
        credentials_secret=sharepoint_credentials_secret,
        config_key=sharepoint_config_key,
        tests=tests,
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
