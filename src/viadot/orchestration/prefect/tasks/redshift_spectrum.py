"""Task for uploading pandas DataFrame to AWS Redshift Spectrum."""

import contextlib
from typing import Any, Literal

import pandas as pd
from prefect import task
from prefect.logging import get_run_logger

from viadot.config import get_source_credentials
from viadot.orchestration.prefect.exceptions import MissingSourceCredentialsError
from viadot.orchestration.prefect.utils import get_credentials


with contextlib.suppress(ImportError):
    from viadot.sources import RedshiftSpectrum


@task(retries=3, retry_delay_seconds=10, timeout_seconds=60 * 60)
def df_to_redshift_spectrum(  # noqa: PLR0913
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
    config_key: str | None = None,
    credentials_secret: str | None = None,
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
        config_key (str, optional): The key in the viadot config holding relevant
            credentials. Defaults to None.
        credentials_secret (str, optional): The name of a secret block in Prefect
            that stores AWS credentials. Defaults to None.
        kwargs: The parameters to pass in awswrangler to_parquet/to_csv function.
    """
    if not (credentials_secret or config_key):
        raise MissingSourceCredentialsError

    credentials = get_source_credentials(config_key) or get_credentials(
        credentials_secret
    )

    # Convert columns containing only null values to string to avoid errors
    # during new table creation
    null_columns = df.isna().all()
    null_cols_list = null_columns[null_columns].index.tolist()
    df[null_cols_list] = df[null_cols_list].astype("string")

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
        **kwargs,
    )

    logger = get_run_logger()
    logger.info("Data has been uploaded successfully.")
