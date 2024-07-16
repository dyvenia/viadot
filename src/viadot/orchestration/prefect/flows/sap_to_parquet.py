"""Flows for downloading data from SAP to Parquet file."""

from typing import Any, Literal

from viadot.orchestration.prefect.tasks import sap_rfc_to_df
from viadot.orchestration.prefect.tasks.task_utils import df_to_parquet

from prefect import flow

@flow(
    name="extract--sap--parquet",
    description="Extract data from SAP and load it into Parquet file",
    retries=0,
    # retry_delay_seconds=10,
)
def sap_to_parquet(  
    path: str,
    if_exists: Literal["append", "replace", "skip"] = "replace",
    query: str | None = None,
    func: str | None = None,
    sap_sep: str | None = None,
    rfc_total_col_width_character_limit: int = 400,
    rfc_unique_id: list[str] | None = None,
    sap_credentials_secret: str | None = None,
    sap_credentials: dict[str, Any] | None = None,
    sap_config_key: str = "SAP",
    alternative_version: bool = False,
    replacement: str = "-",
) -> None:
    """Download a pandas `DataFrame` from SAP and upload it to AWS Redshift Spectrum.

    Args:
        path (str): Path to Parquet file, where the data will be located.
            Defaults to None.
        if_exists (Literal["append", "replace", "skip"], optional): What to do if the table exists. Defaults to "replace"
        query (str): The query to be executed with pyRFC.
        sap_sep (str, optional): The separator to use when reading query results.
            If not provided, multiple options are automatically tried.
            Defaults to None.
        func (str, optional): SAP RFC function to use. Defaults to None.
        rfc_total_col_width_character_limit (int, optional): Number of characters by
            which query will be split in chunks in case of too many columns for RFC
            function. According to SAP documentation, the limit is 512 characters.
            However, we observed SAP raising an exception even on a slightly lower
            number of characters, so we add a safety margin. Defaults to 400.
        rfc_unique_id  (list[str], optional): Reference columns to merge chunks Data
            Frames. These columns must to be unique. If no columns are provided, all
                data frame columns will by concatenated. Defaults to None.
        sap_credentials_secret (str, optional): The name of the Prefect Secret that stores
            SAP credentials. Defaults to None.
        sap_credentials (dict[str, Any], optional): Credentials to SAP.
            Defaults to None.
        sap_config_key (str, optional): The key in the viadot config holding relevant
            credentials. Defaults to "SAP".
        alternative_version (bool, optional): Enable the use version 2 in source.
            Defaults to False.
        replacement (str, optional): In case of sep is on a columns, set up a new
            character to replace inside the string to avoid flow breakdowns.
            Defaults to "-".
    """
    df = sap_rfc_to_df(
        query=query,
        sep=sap_sep,
        func=func,
        replacement=replacement,
        rfc_total_col_width_character_limit=rfc_total_col_width_character_limit,
        rfc_unique_id=rfc_unique_id,
        config_key=sap_config_key,
        credentials_secret=sap_credentials_secret,
        credentials = sap_credentials,
        alternative_version=alternative_version
    )

    return df_to_parquet(
        df=df,
        path=path,
        if_exists=if_exists,
    )

