"""Flows for downloading data from EntraID and uploading it to AWS Redshift Spectrum."""

from typing import Literal

from prefect import flow

from viadot.orchestration.prefect.tasks import (
    df_to_redshift_spectrum,
    entraid_to_df,
)


@flow(
    name="extract--entraid--redshift_spectrum",
    description="Extract data from EntraID and load it into AWS Redshift Spectrum.",
    retries=1,
    retry_delay_seconds=60,
)
def entraid_to_redshift_spectrum(  # noqa: PLR0913
    to_path: str,
    schema_name: str,
    table: str,
    extension: str = ".parquet",
    if_exists: Literal["overwrite", "append"] = "overwrite",
    partition_cols: list[str] | None = None,
    index: bool = False,
    compression: str | None = None,
    sep: str = ",",
    aws_config_key: str | None = None,
    credentials_secret: str | None = None,
    sharepoint_credentials_secret: str | None = None,
    sharepoint_config_key: str | None = None,
) -> None:
    """Extract data from SharePoint and load it into AWS Redshift Spectrum.

    This function downloads data either from SharePoint file or the whole directory and
    uploads it to AWS Redshift Spectrum.

    Modes:
    It downloads data from EntraID and creates a table from it.


    Args:
        to_path (str): Path to a S3 folder where the table will be located. Defaults to
            None.
        schema_name (str): AWS Glue catalog database name.
        table (str): AWS Glue catalog table name.
        partition_cols (list[str]): List of column names that will be used to create
            partitions. Only takes effect if dataset=True.
        extension (str): Required file type. Accepted file formats are 'csv' and
            'parquet'.
        if_exists (str, optional): 'overwrite' to recreate any possible existing table
            or 'append' to keep any possible existing table. Defaults to overwrite.
        partition_cols (list[str], optional): List of column names that will be used to
            create partitions. Only takes effect if dataset=True. Defaults to None.
        index (bool, optional): Write row names (index). Defaults to False.
        compression (str, optional): Compression style (None, snappy, gzip, zstd).
        sep (str, optional): Field delimiter for the output file. Defaults to ','.
        aws_config_key (str, optional): The key in the viadot config holding relevant
            credentials. Defaults to None.
        credentials_secret (str, optional): The name of a secret block in Prefect
            that stores AWS credentials. Defaults to None.
        sharepoint_credentials_secret (str, optional): The name of the secret storing
            EntraID credentials. Defaults to None.
            More info on: https://docs.prefect.io/concepts/blocks/
        entraid_config_key (str, optional): The key in the viadot config holding
            relevant credentials. Defaults to None.
    """
    df = entraid_to_df(
        credentials_secret=sharepoint_credentials_secret,
        config_key=sharepoint_config_key,
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
