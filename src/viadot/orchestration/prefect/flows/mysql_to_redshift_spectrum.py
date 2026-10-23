"""Flows for downloading data from MySQL and uploading it to Redshift Spectrum."""

from typing import Any, Literal

from prefect import flow

from viadot.orchestration.prefect.tasks import df_to_redshift_spectrum, mysql_to_df
from viadot.orchestration.prefect.utils import (
    with_flow_timeout_param,
    with_state_tracking_and_downstream_triggering,
)


@flow(
    name="extract--mysql--redshift_spectrum",
    description="Extract data from MySQL and load it into AWS Redshift Spectrum.",
    retries=1,
    retry_delay_seconds=60,
)
@with_flow_timeout_param()
@with_state_tracking_and_downstream_triggering(node_name_param="table")
def mysql_to_redshift_spectrum(  # noqa: PLR0913
    query: str,
    to_path: str,
    schema_name: str,
    table: str,
    extension: str = ".parquet",
    if_exists: Literal["overwrite", "append"] = "overwrite",
    partition_cols: list[str] | None = None,
    index: bool = False,
    compression: str | None = None,
    tests: dict[str, Any] | None = None,
    config_key: str | None = None,
    credentials_secret: str | None = None,
    mysql_credentials_secret: str | None = None,
    mysql_config_key: str | None = None,
) -> None:
    """Flow to load data from MySQL to Redshift Spectrum.

    Args:
        query (str): The SQL query to execute on the MySQL database.
        to_path (str): The path where the extracted data will be stored.
        schema_name (str): The name of the schema in Redshift Spectrum.
        table (str): The name of the table in Redshift Spectrum.
        extension (str, optional): The file extension to use. Defaults to ".parquet".
        if_exists (Literal["overwrite", "append"], optional): Action to take if
            the table already exists. Defaults to "overwrite".
        partition_cols (list[str] | None, optional): Columns to partition the data by.
            Defaults to None.
        index (bool, optional): Whether to include the index in the output file.
            Defaults to False.
        compression (str | None, optional): Compression type to use for the output file.
            Defaults to None.
        tests (dict[str, Any], optional): A dictionary with optional list of tests
            to verify the output dataframe. Defaults to None.
        config_key (str | None, optional): AWS configuration key. Defaults to None.
        credentials_secret (str | None, optional): Name of the secret storing
            AWS credentials. Defaults to None.
        mysql_credentials_secret (str | None, optional): Name of the Prefect secret
            storing the MySQL credentials. Defaults to None.
        mysql_config_key (str | None, optional): Key in the viadot config holding
            the MySQL credentials. Defaults to None.

    Note:
        State tracking and downstream node triggering parameters are injected by the
        ``with_state_tracking_and_downstream_triggering`` decorator. See its docstring
        for details on available state and trigger parameters.

    Returns:
        None
    """
    df = mysql_to_df(
        query=query,
        config_key=mysql_config_key,
        credentials_secret=mysql_credentials_secret,
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
        config_key=config_key,
        credentials_secret=credentials_secret,
    )
