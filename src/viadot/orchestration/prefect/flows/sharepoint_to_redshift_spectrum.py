"""Flows for downloading data from Sharepoint and uploading it to AWS Redshift Spectrum."""  # noqa: W505

from typing import Any, Literal

from viadot.orchestration.prefect.tasks import df_to_redshift_spectrum, sharepoint_to_df

from prefect import flow


@flow(
    name="extract--sharepoint--redshift_spectrum",
    description="Extract data from Sharepoint and load it into AWS Redshift Spectrum.",
    retries=1,
    retry_delay_seconds=60,
)
def sharepoint_to_redshift_spectrum(  # noqa: PLR0913, PLR0917
    sharepoint_url: str,
    to_path: str,
    schema_name: str,
    table: str,
    extension: str = ".parquet",
    if_exists: Literal["overwrite", "append"] = "overwrite",
    partition_cols: list[str] | None = None,
    index: bool = False,
    compression: str | None = None,
    sep: str = ",",
    description: str | None = None,
    aws_credentials: dict[str, Any] | None = None,
    aws_config_key: str | None = None,
    sheet_name: str | list[str | int] | int | None = None,
    columns: str | list[str] | list[int] | None = None,
    sharepoint_credentials_secret: str | None = None,
    sharepoint_config_key: str | None = None,
    sharepoint_credentials: dict[str, Any] | None = None,
) -> None:
    """Download a pandas `DataFrame` from Sharepoint and upload it to Redshift Spectrum.

    Args:
        sharepoint_url (str): The URL to the file.
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
        description (str, optional): AWS Glue catalog table description. Defaults to
            None.
        aws_credentials (dict[str, Any], optional): Credentials to the AWS Redshift
            Spectrum. Defaults to None.
        aws_config_key (str, optional): The key in the viadot config holding relevant
            credentials. Defaults to None.
        sheet_name (str | list | int, optional): Strings are used for sheet names.
            Integers are used in zero-indexed sheet positions (chart sheets do not count
            as a sheet position). Lists of strings/integers are used to request multiple
            sheets. Specify None to get all worksheets. Defaults to None.
        columns (str | list[str] | list[int], optional): Which columns to ingest.
            Defaults to None.
        sharepoint_credentials_secret (str, optional): The name of the secret storing
            Sharepoint credentials. Defaults to None.
            More info on: https://docs.prefect.io/concepts/blocks/
        sharepoint_config_key (str, optional): The key in the viadot config holding
            relevant credentials. Defaults to None.
        sharepoint_credentials (dict, optional): Credentials to Sharepoint. Defaults to
            None.
    """
    df = sharepoint_to_df(
        url=sharepoint_url,
        sheet_name=sheet_name,
        columns=columns,
        credentials_secret=sharepoint_credentials_secret,
        config_key=sharepoint_config_key,
        credentials=sharepoint_credentials,
    )

    return df_to_redshift_spectrum(
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
        description=description,
        credentials=aws_credentials,
        config_key=aws_config_key,
    )
