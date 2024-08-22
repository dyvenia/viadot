"""Flows for pulling data from/into Sharepoint."""

import contextlib
from typing import Literal

from prefect import flow

from viadot.orchestration.prefect.tasks import df_to_databricks, sharepoint_to_df


@flow(
    name="extract--sharepoint--databricks",
    description="Extract data from Sharepoint and load it into Databricks.",
    retries=1,
    retry_delay_seconds=60,
)
def sharepoint_to_databricks(
    sharepoint_url: str,
    databricks_table: str,
    databricks_schema: str | None = None,
    if_exists: Literal["replace", "skip", "fail"] = "fail",
    sheet_name: str | list | int | None = None,
    columns: str | list[str] | list[int] | None = None,
    sharepoint_credentials_secret: str | None = None,
    sharepoint_config_key: str | None = None,
    databricks_credentials_secret: str | None = None,
    databricks_config_key: str | None = None,
) -> None:
    """Download a file from Sharepoint and upload it to Azure Data Lake.

    Args:
        sharepoint_url (str): The URL to the file.
        databricks_table (str): The name of the target table.
        databricks_schema (str, optional): The name of the target schema.
        if_exists (str, Optional): What to do if the table already exists.
            One of 'replace', 'skip', and 'fail'.
        columns (str | list[str] | list[int], optional): Which columns to ingest.
            Defaults to None.
        sheet_name (str | list | int, optional): Strings are used for sheet names.
            Integers are used in zero-indexed sheet positions
            (chart sheets do not count as a sheet position). Lists of strings/integers
            are used to request multiple sheets. Specify None to get all worksheets.
            Defaults to None.
        sharepoint_credentials_secret (str, optional): The name of the Azure Key Vault
            secret storing relevant credentials. Defaults to None.
        sharepoint_config_key (str, optional): The key in the viadot config holding
            relevant credentials. Defaults to None.
        databricks_credentials_secret (str, optional): The name of the Azure Key Vault
            secret storing relevant credentials. Defaults to None.
        databricks_config_key (str, optional): The key in the viadot config holding
            relevant credentials. Defaults to None.
    """
    # Workaround Prefect converting this parameter to string due to multiple
    # supported input types -- pandas relies on the data type to choose relevant
    # implementation.
    if sheet_name is not None:
        with contextlib.suppress(ValueError):
            sheet_name = int(sheet_name)

    df = sharepoint_to_df(
        url=sharepoint_url,
        credentials_secret=sharepoint_credentials_secret,
        config_key=sharepoint_config_key,
        sheet_name=sheet_name,
        columns=columns,
    )
    return df_to_databricks(
        df=df,
        schema=databricks_schema,
        table=databricks_table,
        if_exists=if_exists,
        credentials_secret=databricks_credentials_secret,
        config_key=databricks_config_key,
    )
