"""Task for downloading data from Hubspot API to a pandas DataFrame."""

from typing import Any

import pandas as pd
from prefect import get_run_logger, task

from viadot.config import get_source_credentials
from viadot.orchestration.prefect.exceptions import MissingSourceCredentialsError
from viadot.orchestration.prefect.utils import get_credentials
from viadot.sources import Hubspot


@task(retries=3, log_prints=True, retry_delay_seconds=10, timeout_seconds=60 * 60)
def hubspot_to_df(  # NOQA: PLR0913
    endpoint: str | None = None,
    api_method: str | None = None,
    campaign_ids: list[str] | None = None,
    contact_type: str = "influencedContacts",
    config_key: str | None = None,
    credentials_secret: str | None = None,
    credentials: dict[str, Any] | None = None,
    filters: list[dict[str, Any]] | None = None,
    properties: list[Any] | None = None,
    nrows: int = 1000,
    drop_empty_columns: bool = False,
) -> pd.DataFrame:
    """Task to download data from Hubspot API to a pandas DataFrame.

    Args:
        endpoint (str): API endpoint for an individual request.
        api_method (str, optional): The method to use to get the data from the API.
        campaign_ids (list[str], optional): List of campaign IDs to get the metrics for.
        config_key (Optional[str], optional): The key in the viadot config holding
            relevant credentials. Defaults to None.
        credentials_secret (Optional[str], optional): The name of the Azure Key
            Vault secret where credentials are stored. Defaults to None.
        credentials (dict[str, Any], optional): Credentials to Hubspot.
            If provided, this value has priority over `config_key`
            and `credentials_secret`. Defaults to None.
        filters (Optional[List[Dict[str, Any]]], optional): Filters defined for the API
            body in specific order. Defaults to None.
        properties (Optional[List[Any]], optional): List of user-defined columns to be
            pulled from the API. Defaults to None.
        nrows (int, optional): Max number of rows to pull during execution.
            Defaults to 1000.
        drop_empty_columns (bool, optional): If True, removes columns that are 100%
            empty and known technical metadata columns (identity-profiles, merge-audits,
            vid-offset). Defaults to False.


    Examples:
        data_frame = hubspot_to_df(
            config_key=config_key,
            credentials_secret=credentials_secret,
            endpoint=endpoint,
            filters=filters,
            properties=properties,
            nrows=nrows,
        )

    Raises:
        MissingSourceCredentialsError: If no credentials have been provided.

    Returns:
        pd.DataFrame: The response data as a pandas DataFrame.
    """
    if not (credentials or config_key or credentials_secret):
        raise MissingSourceCredentialsError

    credentials = (
        credentials
        or get_source_credentials(config_key)
        or get_credentials(credentials_secret)
    )

    hubspot = Hubspot(
        credentials=credentials,
        config_key=config_key,
    )

    data = hubspot.call_api(
        method=api_method,
        endpoint=endpoint,
        campaign_ids=campaign_ids,
        contact_type=contact_type,
        filters=filters,
        properties=properties,
        nrows=nrows,
    )

    df = hubspot.to_df(data=data)

    if drop_empty_columns:
        logger = get_run_logger()
        initial_cols = len(df.columns)
        df = df.dropna(axis=1, how="all")
        exclude_keywords = ["identity-profiles", "merge-audits", "vid-offset"]
        cols_to_drop = [
            col for col in df.columns if any(key in col for key in exclude_keywords)
        ]
        df = df.drop(columns=cols_to_drop)
        logger.info(
            f"Cleanup complete: removed {initial_cols - len(df.columns)} completely "
            "empty or technical columns."
        )
        logger.info(f"Final column count: {len(df.columns)}")

    return df
