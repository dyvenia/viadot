"""Flows for downloading data from Informix and uploading it to Redshift Spectrum."""

from typing import Any, Literal

from prefect import flow

from viadot.orchestration.prefect.tasks import df_to_redshift_spectrum, informix_to_df
from viadot.orchestration.prefect.utils import (
    DynamicDateHandler,
    with_flow_timeout_param,
)


@flow(
    name="extract--informix--redshift_spectrum",
    description="Extract data from Informix and load it into AWS Redshift Spectrum.",
    retries=1,
    retry_delay_seconds=60,
)
@with_flow_timeout_param()
def informix_to_redshift_spectrum(  # noqa: PLR0913
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
    dynamic_date_symbols: list[str] = ["<<", ">>"],  # noqa: B006
    dynamic_date_format: str = "%Y%m%d",
    dynamic_date_timezone: str = "UTC",
    tests: dict[str, Any] | None = None,
    config_key: str | None = None,
    credentials_secret: str | None = None,
    informix_credentials_secret: str | None = None,
    informix_config_key: str | None = None,
) -> None:
    """Flow to load data from a Informix source to Redshift Spectrum.

    Args:
        query (str): The query to execute.
        to_path (str): The path to the destination file.
        schema_name (str): The schema name.
        table (str): The table name.
        sep (str): The separator for the CSV file.
        extension (str): The extension for the file.
        if_exists (Literal["overwrite", "append"]): The action to take if
            the table already exists.
        partition_cols (list[str]): The columns to partition the data by.
        index (bool): Whether to include the index in the output file.
        compression (str): The compression type to use for the output file.
        dynamic_date_symbols (list[str]): The symbols used for dynamic date handling.
        dynamic_date_format (str): The format used for dynamic date parsing.
        dynamic_date_timezone (str): The timezone used for dynamic date processing.
        tests (dict[str, Any]): The tests to run on the data.
        config_key (str): The configuration key.
        credentials_secret (str): The credentials secret.
        informix_credentials_secret (str): The Informix credentials secret.
        informix_config_key (str): The Informix configuration key.
    """
    ddh = DynamicDateHandler(
        dynamic_date_symbols, dynamic_date_format, dynamic_date_timezone
    )
    query = ddh.process_dates(query)  # type: ignore

    df = informix_to_df(
        query=query,
        config_key=informix_config_key,
        credentials_secret=informix_credentials_secret,
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
        config_key=config_key,
        credentials_secret=credentials_secret,
    )
