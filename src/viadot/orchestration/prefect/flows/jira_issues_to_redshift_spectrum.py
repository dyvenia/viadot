"""Flow for fetching Jira issues and loading them into Redshift Spectrum."""

from typing import Literal

import pandas as pd
from prefect import flow
from prefect.logging import get_run_logger

from viadot.orchestration.prefect.tasks import (
    df_to_redshift_spectrum,
    jira_issues_to_df,
)


@flow(
    name="jira_to_redshift_spectrum",
    description="Fetch Jira issues via JQL and load them into Redshift Spectrum.",
)
def jira_to_redshift_spectrum_flow(  # noqa: PLR0913
    jql: str,
    fields: list[str],
    to_path: str | None = None,
    schema_name: str | None = None,
    table: str | None = None,
    extension: str = ".parquet",
    if_exists: Literal["overwrite", "append", "skip"] = "overwrite",
    partition_cols: list[str] | None = None,
    compression: str | None = None,
    aws_sep: str = ",",
    aws_config_key: str | None = None,
    credentials_secret: str | None = None,
) -> pd.DataFrame:
    """Fetch Jira issues and load them into Redshift Spectrum.

    Args:
        jql (str): JQL query, e.g. 'project = MYPROJ AND status = Open'.
        fields (list[str]): Human-readable Jira field names to fetch.
        to_path (str): S3 destination path, e.g. 's3://my-bucket/jira/issues/'.
        schema_name (str): Redshift Spectrum schema name.
        table (str): Target table name.
        extension (str): File extension for the output files. Defaults to '.parquet'.
        if_exists (str): Behaviour when the table already exists.
            One of 'overwrite', 'append', 'skip'. Defaults to 'overwrite'.
        partition_cols (list[str] | None): Columns to partition the output by.
        compression (str | None): Compression codec, e.g. 'snappy'.
        aws_sep (str): CSV separator (used when extension is '.csv'). Defaults to ','.
        aws_config_key (str | None): viadot config key for AWS credentials.
        credentials_secret (str | None): Name of the AWS secret in the secrets manager.

    Returns:
        pd.DataFrame: The fetched DataFrame (for testing / downstream use).
    """
    logger = get_run_logger()
    df = jira_issues_to_df(
        jql=jql,
        fields=fields,
        credentials_secret=credentials_secret,
    )
    logger.info(f"Fetched {len(df)} rows from Jira.")
    logger.info("Loading data into Redshift Spectrum.")
    df_to_redshift_spectrum(
        df=df,
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
