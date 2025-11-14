"""Flows for downloading data from SQLServer and uploading it to Redshift Spectrum."""

from typing import Any, Literal

from prefect import flow

from viadot.orchestration.prefect.tasks import df_to_redshift_spectrum, sql_server_to_df
from viadot.orchestration.prefect.utils import DynamicDateHandler


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
    dynamic_date_symbols: list[str] = ["<<", ">>"],  # noqa: B006
    dynamic_date_format: str = "%Y%m%d",
    dynamic_date_timezone: str = "UTC",
    tests: dict[str, Any] | None = None,
    config_key: str | None = None,
    credentials_secret: str | None = None,
    sql_server_credentials_secret: str | None = None,
    sql_server_config_key: str | None = None,
) -> None:
    """Flow to load data from a SQL Server database to Redshift Spectrum.

    Args:
        query (str): The SQL query to execute on the SQL Server database. Dynamic date
            pattern is supported (SELECT * FROM table WHERE date='<<yesterday>>')
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
        dynamic_date_symbols (list[str], optional): Symbols used for dynamic date
            handling. Defaults to ["<<", ">>"].
        dynamic_date_format (str, optional): Format used for dynamic date parsing.
            Defaults to "%Y%m%d".
        dynamic_date_timezone (str, optional): Timezone used for dynamic date
            processing. Defaults to "UTC".
        tests (dict[str, Any], optional): A dictionary with optional list of tests
            to verify the output dataframe. If defined, triggers the `validate`
            function from utils. Defaults to None.
            Available tests:
                - `column_size`: dict{column: size}
                - `column_unique_values`: list[columns]
                - `column_list_to_match`: list[columns]
                - `dataset_row_count`: dict: {'min': number, 'max', number}
                - `column_match_regex`: dict: {column: 'regex'}
                - `column_sum`: dict: {column: {'min': number, 'max': number}}
            Defaults to: None
        config_key (str | None, optional): AWS configuration key. Defaults to None.
        credentials_secret (str | None, optional): Name of the secret storing
            AWS credentials. Defaults to None.
        sql_server_credentials_secret (str | None, optional): Name of the secret storing
            SQL Server credentials. Defaults to None.
        sql_server_config_key (str | None, optional): Key in the configuration for
            SQL Server credentials. Defaults to None.

    Returns:
        None
    """
    ddh = DynamicDateHandler(
        dynamic_date_symbols, dynamic_date_format, dynamic_date_timezone
    )
    query = ddh.process_dates(query)  # type: ignore

    df = sql_server_to_df(
        query=query,
        config_key=sql_server_config_key,
        credentials_secret=sql_server_credentials_secret,
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
