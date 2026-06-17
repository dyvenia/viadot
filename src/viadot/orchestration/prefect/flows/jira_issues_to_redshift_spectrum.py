"""Flow for fetching Jira issues and loading them into Redshift Spectrum."""

from typing import Literal

import pandas as pd
from prefect import flow, task
from prefect.logging import get_run_logger

from viadot.orchestration.prefect.tasks import (
    df_to_redshift_spectrum,
    jira_issues_to_df,
)
from viadot.orchestration.prefect.utils import (
    with_flow_timeout_param,
    with_state_tracking_and_downstream_triggering,
)


@task
def log_df_schema(df: pd.DataFrame, name: str = "log_df_schema") -> pd.DataFrame:
    """Log the shape and dtypes of a DataFrame, and identify any nested columns.

    Args:
        df (pd.DataFrame): The DataFrame to log.
        name (str): Name to use in the log messages. Defaults to "log_df_schema".

    Returns:
        pd.DataFrame: The same DataFrame that was passed in.
    """
    logger = get_run_logger()
    logger.info(f"{name} shape={df.shape}")
    logger.info(f"{name} dtypes:\n{df.dtypes.to_string()}")
    nested = [
        c for c in df.columns if df[c].map(lambda v: isinstance(v, dict | list)).any()
    ]
    logger.warning(f"{name} nested columns (dict/list): {nested}")
    return df


@flow(
    name="jira_issues_to_redshift_spectrum",
    description="Fetch Jira issues via JQL and load them into Redshift Spectrum.",
)
@with_flow_timeout_param()
@with_state_tracking_and_downstream_triggering(node_name_param="table")
def jira_issues_to_redshift_spectrum_flow(  # noqa: PLR0913
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
) -> None:
    """Fetch Jira issues and load them into Redshift Spectrum.

    Args:
        jql (str): JQL query, e.g. 'project = MYPROJ AND status = Open'.
        fields (list[str] | None, optional): Human-readable Jira field names to fetch.
                Defaults to None.
        technical_fields (list[str] | None, optional): Raw Jira field ids
                (e.g. "summary", "customfield_16187"). If provided, issues are fetched
                directly and returned without any name resolution. Column names keep the
                raw ids and values are returned exactly as Jira sends them.
                Defaults to None.
        to_path (str | None, optional): S3 destination path, e.g. 's3://my-bucket/jira/issues/'.
                Defaults to None.
        schema_name (str | None, optional): Redshift Spectrum schema name.
                Defaults to None.
        table (str | None, optional): Target table name. Defaults to None.
        extension (str): File extension for the output files. Defaults to '.parquet'.
        if_exists (Literal["overwrite", "append", "skip"], optional): Behaviour
                when the table already exists. Defaults to 'overwrite'.
        partition_cols (list[str] | None): Columns to partition the output by.
                Defaults to None.
        compression (str | None, optional): Compression codec, e.g. 'snappy'.
                Defaults to None.
        aws_sep (str, optional): CSV separator (used when extension is '.csv').
                Defaults to ','.
        aws_config_key (str, optional): The key in the viadot config holding relevant
            AWS credentials. Defaults to None.
        credentials_secret (str | None, optional): Name of the AWS secret
                in the secrets manager. Defaults to None.
        jira_credentials_secret (str | None, optional): Name of the AWS secret containing
                Jira credentials. Defaults to None.
        custom_field_mapping (dict[str, str] | None, optional): Optional mapping of custom
                field names to their corresponding Jira field IDs, e.g.
                Defaults to None.

    Returns:
        None: This flow does not return any value.
                It fetches Jira issues and loads them into Redshift Spectrum.
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
    df = log_df_schema(df)
    logger.info(
        "Cleaning complex data types (dicts/lists) to prevent Parquet schema validation errors."
    )
    for col in df.columns:
        if df[col].apply(lambda x: isinstance(x, dict | list)).any():
            df[col] = df[col].astype(str)

    complex_type_cols = [
        col
        for col in df.columns
        if df[col].apply(lambda x: isinstance(x, dict | list)).any()
    ]

    if complex_type_cols:
        logger.info(
            f"Columns with complex data types (dicts/lists) that were converted to strings: {complex_type_cols}"
        )
        for col in complex_type_cols:
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
