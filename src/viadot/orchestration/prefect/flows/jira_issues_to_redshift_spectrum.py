"""Flow for fetching Jira issues and loading them into Redshift Spectrum."""

from typing import Literal

import pandas as pd
from prefect import flow
from prefect.logging import get_run_logger

from viadot.orchestration.prefect.tasks import (
    df_to_redshift_spectrum,
    jira_issues_to_df,
)
from viadot.orchestration.prefect.utils import (
    with_flow_timeout_param,
    with_state_tracking_and_downstream_triggering,
)


@flow(
    name="jira_to_redshift_spectrum",
    description="Fetch Jira issues via JQL and load them into Redshift Spectrum.",
)
@with_flow_timeout_param()
@with_state_tracking_and_downstream_triggering(node_name_param="table")
def jira_to_redshift_spectrum_flow(  # noqa: PLR0913
    jql: str,
    fields: list[str] | None = None,
    technical_fields: list[str] | None = None,
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
    jira_credentials_secret: str | None = None,
    custom_field_mapping: dict[str, str] | None = None,
) -> pd.DataFrame:
    """Fetch Jira issues and load them into Redshift Spectrum.

    Args:
        jql (str): JQL query, e.g. 'project = MYPROJ AND status = Open'.
        fields (list[str]): Human-readable Jira field names to fetch.
        technical_fields: Raw Jira field ids (e.g. "summary",
                "customfield_16187"). If provided, issues are fetched directly
                and returned without any name resolution. Column names keep the
                raw ids and values are returned exactly as Jira sends them.
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
        jira_credentials_secret (str | None): Name of the AWS secret containing
            Jira credentials.
        custom_field_mapping (dict[str, str] | None): Optional mapping of custom
            field names to their corresponding Jira field IDs, e.g.

    Returns:
        pd.DataFrame: The fetched DataFrame (for testing / downstream use).
    """
    logger = get_run_logger()
    df = jira_issues_to_df(
        jql=jql,
        fields=fields,
        technical_fields=technical_fields,
        credentials_secret=jira_credentials_secret,
        custom_field_mapping=custom_field_mapping,
    )
    logger.info(f"Fetched {len(df)} rows from Jira.")

    logger.info(
        "Cleaning complex data types (dicts/lists) to prevent Parquet schema validation errors."
    )
    for col in df.columns:
        if df[col].apply(lambda x: isinstance(x, dict | list)).any():
            df[col] = df[col].astype(str)

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
