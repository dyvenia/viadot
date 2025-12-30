"""Download data from ECB API and load into Redshift Spectrum."""

from typing import Any, Literal

from prefect import flow
from prefect.task_runners import ConcurrentTaskRunner

from viadot.orchestration.prefect.tasks import df_to_redshift_spectrum, ecb_to_df


@flow(
    name="ECB extraction to Redshift Spectrum",
    description="Extract exchange rates data from ECB API and load into Redshift Spectrum.",
    retries=1,
    retry_delay_seconds=60,
    task_runner=ConcurrentTaskRunner,
)
def ecb_to_redshift_spectrum(  # noqa: PLR0913
    url: str,
    to_path: str,
    schema_name: str,
    table: str,
    if_empty: Literal["warn", "skip", "fail"] = "warn",
    tests: dict[str, Any] | None = None,
    extension: str = ".parquet",
    if_exists: Literal["overwrite", "append"] = "overwrite",
    partition_cols: list[str] | None = None,
    compression: Literal["snappy", "gzip", "zstd"] | None = None,
    aws_sep: str = ",",
    credentials_secret: str | None = None,
    aws_config_key: str | None = None,
) -> None:
    """Flow for downloading exchange rates data from ECB API to Redshift Spectrum.

    Args:
        url (str): The URL of the ECB API endpoint.
        to_path (str): Path to a S3 folder where the table will be located.
        schema_name (str): AWS Glue catalog database name.
        table (str): AWS Glue catalog table name.
        if_empty (Literal["warn", "skip", "fail"], optional): What to do if the
            query returns no data. Defaults to "warn".
        tests (dict[str, Any], optional): A dictionary with optional list of tests
            to verify the output dataframe. If defined, triggers the `validate`
            function from viadot.utils. Defaults to None.
        extension (str): Required file type. Accepted file formats are 'csv' and
            'parquet'. Defaults to ".parquet".
        if_exists (Literal["overwrite", "append"]): Whether to overwrite or append to
            existing data. Defaults to "overwrite".
        partition_cols (list[str], optional): List of column names that will be used to
            create partitions. Defaults to None.
        compression (Literal["snappy", "gzip", "zstd"], optional): Compression style
            (None, snappy, gzip, zstd). Defaults to None.
        aws_sep (str, optional): Field delimiter for the output file. Defaults to ','.
        credentials_secret (str, optional): The name of a secret block in Prefect
            that stores AWS credentials. Defaults to None.
        aws_config_key (str, optional): The key in the viadot config holding relevant
            AWS credentials. Defaults to None.

    Examples:
        ecb_to_redshift_spectrum(
            url="https://www.ecb.europa.eu/stats/eurofxref/eurofxref-daily.xml",
            to_path="s3://your-bucket/ecb/",
            schema_name="staging",
            table="ecb_exchange_rates",
            tests={
                "not_null": ["time", "currency", "rate"],
            },
            partition_cols=["time"],
            compression="snappy",
            if_exists="overwrite",
        )
    """
    data_frame = ecb_to_df(
        url=url,
        if_empty=if_empty,
        tests=tests,
    )

    return df_to_redshift_spectrum(
        df=data_frame,
        to_path=to_path,
        schema_name=schema_name,
        table=table,
        extension=extension,
        if_exists=if_exists,
        partition_cols=partition_cols,
        compression=compression,
        sep=aws_sep,
        config_key=aws_config_key,
        credentials_secret=credentials_secret,
    )
