"""Task for uploading pandas DataFrame to AWS Redshift Spectrum."""

import contextlib
from typing import Any, Literal

import pandas as pd
from prefect import task
from prefect.logging import get_run_logger

with contextlib.suppress(ImportError):
    from viadot.sources import RedshiftSpectrum


@task(retries=3, retry_delay_seconds=10, timeout_seconds=60 * 60)
def df_to_redshift_spectrum(  # noqa: PLR0913, PLR0917
    df: pd.DataFrame,
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
    credentials: dict[str, Any] | None = None,
    config_key: str | None = None,
    **kwargs: dict[str, Any] | None,
) -> None:
    """Task to upload a pandas `DataFrame` to a csv or parquet file.

    Args:
        df (pd.DataFrame): The Pandas DataFrame to ingest into Redshift Spectrum.
        to_path (str): Path to a S3 folder where the table will be located. If needed,
            a bottom-level directory named f"{table}" is automatically created, so
            that files are always located in a folder named the same as the table.
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
        sep (str, optional): Field delimiter for the output file. Applies only to '.csv'
            extension. Defaults to ','.
        description (str, optional): AWS Glue catalog table description.
        credentials (dict[str, Any], optional): Credentials to the AWS Redshift
            Spectrum. Defaults to None.
        config_key (str, optional): The key in the viadot config holding relevant
            credentials. Defaults to None.
        kwargs: The parameters to pass in awswrangler to_parquet/to_csv function.
    """
    rs = RedshiftSpectrum(credentials=credentials, config_key=config_key)

    rs.from_df(
        df=df,
        to_path=to_path,
        schema=schema_name,
        table=table,
        extension=extension,
        if_exists=if_exists,
        partition_cols=partition_cols,
        index=index,
        compression=compression,
        sep=sep,
        description=description,
        **kwargs,
    )

    logger = get_run_logger()
    logger.info("Data has been uploaded successfully.")
